import {
  EventId,
  GLEAN_DEFAULT_MODEL,
  type GleanSettings,
  type ProviderRuntimeEvent,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
  RuntimeRequestId,
  type ThreadId,
  TurnId,
} from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import * as fs from "node:fs";
import * as os from "node:os";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import { collectUint8StreamText } from "../../stream/collectUint8StreamText.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { ProviderSession, ProviderTurnStartResult } from "@t3tools/contracts";

const PROVIDER = ProviderDriverKind.make("glean");

interface GleanSessionState {
  chatId: string | null;
  abortController: AbortController | null;
  pendingArtifactInfo: {
    id: string;
    version: number;
    trackingToken: string;
    questions: Array<{ question: string; ids: string[] }>;
  } | null;
}

export interface GleanAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: unknown;
}

const readStreamAsText = <E>(stream: Stream.Stream<Uint8Array, E>): Effect.Effect<string, E> =>
  collectUint8StreamText({ stream }).pipe(Effect.map((result) => result.text));

export function makeGleanAdapter(config: GleanSettings, options?: GleanAdapterOptions) {
  return Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const runtimeEvents = yield* Queue.unbounded<ProviderRuntimeEvent>();
    const sessions = new Map<ThreadId, GleanSessionState>();

    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        for (const state of sessions.values()) {
          if (state.abortController) {
            state.abortController.abort();
          }
        }
        sessions.clear();
      }).pipe(Effect.ensuring(Queue.shutdown(runtimeEvents))),
    );

    const nowIso = Effect.map(DateTime.now, DateTime.formatIso);

    const generateUUID = crypto.randomUUIDv4.pipe(Effect.orDie);

    const binaryPath = config.binaryPath || "glean";

    const resolveCliEnv = (): Record<string, string> => {
      const base = config.apiToken ? { GLEAN_API_TOKEN: config.apiToken } : {};
      return { ...(options?.environment ?? {}), ...base, GLEAN_SERVER_URL: config.serverUrl };
    };

    const runGleanCli = (args: Array<string>) =>
      Effect.gen(function* () {
        const childEnv = resolveCliEnv();
        const command = ChildProcess.make(binaryPath, args, { env: childEnv });
        const child = yield* spawner.spawn(command).pipe(
          Effect.mapError(
            (cause) =>
              new ProviderAdapterRequestError({
                provider: PROVIDER,
                method: `glean ${args[0] ?? "chat"}`,
                detail: "Failed to spawn glean CLI.",
                cause,
              }),
          ),
        );
        const [stdout, stderr] = yield* Effect.all(
          [readStreamAsText(child.stdout), readStreamAsText(child.stderr)],
          { concurrency: "unbounded" },
        );
        const exitCode = yield* child.exitCode.pipe(Effect.orDie, Effect.map(Number));
        if (exitCode !== 0) {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: `glean ${args[0] ?? "chat"}`,
            detail: stderr.trim() || `glean CLI exited with code ${exitCode}.`,
          });
        }
        return { stdout: stdout.trim(), stderr: stderr.trim() };
      }).pipe(Effect.scoped);

    const buildEventBase = (input: {
      threadId: ThreadId;
      turnId?: TurnId | undefined;
      itemId?: string | undefined;
      raw?: { source: "glean.ndjson"; payload: unknown } | undefined;
      createdAt?: string | undefined;
    }) =>
      Effect.all({
        eventId: generateUUID.pipe(Effect.map(EventId.make)),
        createdAt: input.createdAt === undefined ? nowIso : Effect.succeed(input.createdAt),
      }).pipe(
        Effect.map(({ eventId, createdAt }) => ({
          eventId,
          provider: PROVIDER,
          threadId: input.threadId,
          createdAt,
          ...(input.turnId ? { turnId: input.turnId } : {}),
          ...(input.itemId ? { itemId: RuntimeItemId.make(input.itemId) } : {}),
          ...(input.raw !== undefined ? { raw: input.raw } : {}),
        })),
      );

    const emit = (event: ProviderRuntimeEvent) =>
      Queue.offer(runtimeEvents, event).pipe(Effect.asVoid);

    const startSession: ProviderAdapterShape<ProviderAdapterError>["startSession"] = (input) =>
      Effect.gen(function* () {
        const existing = sessions.get(input.threadId);
        if (existing) {
          const existingSession: ProviderSession = {
            provider: PROVIDER,
            providerInstanceId: options?.instanceId ?? ProviderInstanceId.make("glean"),
            status: "ready",
            runtimeMode: input.runtimeMode,
            threadId: input.threadId,
            createdAt: yield* nowIso,
            updatedAt: yield* nowIso,
          };
          return existingSession;
        }

        sessions.set(input.threadId, {
          chatId: null,
          abortController: null,
          pendingArtifactInfo: null,
        });

        const createdAt = yield* nowIso;
        const session: ProviderSession = {
          provider: PROVIDER,
          providerInstanceId: options?.instanceId ?? ProviderInstanceId.make("glean"),
          status: "ready",
          runtimeMode: input.runtimeMode,
          threadId: input.threadId,
          createdAt,
          updatedAt: createdAt,
        };

        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId })),
          type: "session.state.changed",
          payload: {
            state: "starting",
          },
        });

        return session;
      });

    const sendTurn: ProviderAdapterShape<ProviderAdapterError>["sendTurn"] = (input) =>
      Effect.gen(function* () {
        const state = sessions.get(input.threadId);
        if (!state) {
          return yield* new ProviderAdapterSessionNotFoundError({
            provider: PROVIDER,
            threadId: input.threadId,
          });
        }

        const text = input.input?.trim();
        if (!text || text.length === 0) {
          return yield* new ProviderAdapterValidationError({
            provider: PROVIDER,
            operation: "sendTurn",
            issue: "Glean turns require text input.",
          });
        }

        const uuid = yield* generateUUID;
        const turnId = TurnId.make(`glean-turn-${uuid}`);

        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
          type: "turn.started",
          payload: {},
        });

        const args = state.chatId
          ? ["chat", "--raw", "--save", "--resume", state.chatId, text]
          : ["chat", "--raw", "--save", text];

        const responseExit = yield* Effect.exit(runGleanCli(args));

        if (responseExit._tag === "Failure") {
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
            type: "runtime.error",
            payload: {
              message: "Glean CLI request failed.",
              class: "provider_error",
            },
          });
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
            type: "turn.completed",
            payload: {
              state: "failed",
            },
          });
          const result: ProviderTurnStartResult = { threadId: input.threadId, turnId };
          return result;
        }

        const { stdout: rawOutput } = responseExit.value;
        const lines = rawOutput.split("\n").filter((l) => l.trim().length > 0);
        let turnCompleted = false;
        const accumulatedText = new Map<string, string>();

        for (const line of lines) {
          let parsed: Record<string, unknown>;
          try {
            parsed = JSON.parse(line);
          } catch {
            continue;
          }

          const chatId = typeof parsed.chatId === "string" ? parsed.chatId : undefined;
          if (chatId && !state.chatId) {
            state.chatId = chatId;
          }

          if (typeof parsed.chat === "object" && parsed.chat !== null) {
            const chat = parsed.chat as Record<string, unknown>;
            if (typeof chat.name === "string") {
              yield* emit({
                ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
                type: "thread.metadata.updated",
                payload: { name: chat.name },
              });
            }
            continue;
          }

          const messages = Array.isArray(parsed.messages)
            ? (parsed.messages as Array<Record<string, unknown>>)
            : [];
          for (const msg of messages) {
            if (msg.messageType === "CONTROL") {
              turnCompleted = true;
              yield* emit({
                ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
                type: "turn.completed",
                payload: { state: "completed", stopReason: "stop" },
              });
              continue;
            }

            if (msg.messageType === "ARTIFACT_PAPER" || msg.messageType === "ARTIFACT_MESSAGE") {
              const info = msg.artifactInfo as Record<string, unknown> | undefined;
              const artifactId = typeof info?.id === "string" ? info.id : "unknown";
              const artifactFragments = Array.isArray(msg.fragments)
                ? (msg.fragments as Array<Record<string, unknown>>)
                : [];
              let artifactName: string | undefined;
              let artifactContent: string | undefined;
              for (const frag of artifactFragments) {
                const artifact = frag.artifact as Record<string, unknown> | undefined;
                if (typeof artifact?.name === "string" && !artifactName) {
                  artifactName = artifact.name;
                }
                if (typeof artifact?.resolvedContent === "string" && !artifactContent) {
                  artifactContent = artifact.resolvedContent;
                }
              }
              if (artifactContent && artifactContent.length > 0) {
                const header = artifactName ? `**${artifactName}**\n\n` : "";
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                    itemId: `glean-artifact-${artifactId}`,
                  })),
                  type: "content.delta",
                  payload: {
                    streamKind: "assistant_text",
                    delta: `\n${header}${artifactContent}\n`,
                  },
                });
              }
              yield* emit({
                ...(yield* buildEventBase({
                  threadId: input.threadId,
                  turnId,
                  itemId: `glean-artifact-${artifactId}`,
                })),
                type: "item.completed",
                payload: {
                  itemType: "artifact_reference",
                  status: "completed",
                  title:
                    artifactName ??
                    (msg.messageType === "ARTIFACT_PAPER" ? "Glean Canvas" : "Glean Artifact"),
                  data: msg.artifactInfo ?? {},
                },
              });
              continue;
            }

            if (msg.messageType === "ARTIFACT_USER_QUESTIONS") {
              const requestId = typeof msg.messageId === "string" ? msg.messageId : undefined;
              const fragments = Array.isArray(msg.fragments)
                ? (msg.fragments as Array<Record<string, unknown>>)
                : [];
              const allQuestions: Array<{
                id: string;
                header: string;
                question: string;
                options: Array<{ label: string; description: string; value?: string }>;
              }> = [];
              for (const fragment of fragments) {
                const artifact = fragment.artifact as Record<string, unknown> | undefined;
                const cqc = artifact?.clarifyingQuestionsContent as
                  | Record<string, unknown>
                  | undefined;
                const questions = cqc?.questions;
                if (!Array.isArray(questions)) continue;
                for (const q of questions as Array<Record<string, unknown>>) {
                  const question = typeof q.question === "string" ? q.question : "";
                  const options = Array.isArray(q.options)
                    ? (q.options as Array<string>).map((opt) => ({
                        label: opt,
                        description: "",
                      }))
                    : [];
                  if (question.length === 0 || options.length === 0) continue;
                  allQuestions.push({
                    id: `glean-q-${question.substring(0, 30)}`,
                    header: question.substring(0, 80),
                    question,
                    options,
                  });
                }
              }
              if (allQuestions.length > 0 && requestId) {
                const artifactId =
                  typeof msg.artifactInfo === "object" && msg.artifactInfo !== null
                    ? ((msg.artifactInfo as Record<string, unknown>).id as string | undefined)
                    : undefined;
                const artifactVersion =
                  typeof msg.artifactInfo === "object" && msg.artifactInfo !== null
                    ? ((msg.artifactInfo as Record<string, unknown>).version as number | undefined)
                    : undefined;
                const artifactTrackingToken =
                  typeof msg.artifactInfo === "object" && msg.artifactInfo !== null
                    ? ((msg.artifactInfo as Record<string, unknown>).trackingToken as
                        | string
                        | undefined)
                    : undefined;
                if (artifactId && artifactVersion && artifactTrackingToken) {
                  state.pendingArtifactInfo = {
                    id: artifactId,
                    version: artifactVersion,
                    trackingToken: artifactTrackingToken,
                    questions: allQuestions.map((q) => ({ question: q.question, ids: [q.id] })),
                  };
                }
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                  })),
                  requestId: RuntimeRequestId.make(requestId),
                  type: "user-input.requested",
                  payload: {
                    questions: allQuestions,
                    responseMode: "message" as const,
                  },
                });
              }
              continue;
            }

            if (msg.messageType !== "CONTENT") continue;

            const messageId = typeof msg.messageId === "string" ? msg.messageId : undefined;
            const fragments = Array.isArray(msg.fragments)
              ? (msg.fragments as Array<Record<string, unknown>>)
              : [];
            for (const fragment of fragments) {
              const delta = typeof fragment.text === "string" ? fragment.text : "";
              if (delta.length === 0) continue;

              const key = messageId ?? "default";
              const existing = accumulatedText.get(key) ?? "";
              accumulatedText.set(key, existing + delta);

              yield* emit({
                ...(yield* buildEventBase({
                  threadId: input.threadId,
                  turnId,
                  itemId: RuntimeItemId.make(`glean-msg-${key}`),
                })),
                type: "content.delta",
                payload: { streamKind: "assistant_text", delta },
              });
            }
          }
        }

        if (!turnCompleted) {
          const fullText = [...accumulatedText.values()].join("");
          if (fullText.length > 0) {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: input.threadId,
                turnId,
                itemId: RuntimeItemId.make(`glean-msg-${uuid}`),
              })),
              type: "content.delta",
              payload: { streamKind: "assistant_text", delta: fullText },
            });
          }
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
            type: "turn.completed",
            payload: { state: "completed", stopReason: "stop" },
          });
        }

        state.abortController = null;

        const result: ProviderTurnStartResult = { threadId: input.threadId, turnId };
        return result;
      });

    const interruptTurn: ProviderAdapterShape<ProviderAdapterError>["interruptTurn"] = (
      threadId,
      _turnId,
    ) =>
      Effect.gen(function* () {
        const state = sessions.get(threadId);
        if (!state) {
          return;
        }
        if (state.abortController) {
          state.abortController.abort();
          state.abortController = null;
        }
        yield* emit({
          ...(yield* buildEventBase({ threadId })),
          type: "turn.aborted",
          payload: {
            reason: "Interrupted by user.",
          },
        });
      });

    const respondToRequest: ProviderAdapterShape<ProviderAdapterError>["respondToRequest"] = () =>
      Effect.void;

    const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] = (
      threadId,
      _requestId,
      answers,
    ) =>
      Effect.gen(function* () {
        const state = sessions.get(threadId);
        if (!state || !state.pendingArtifactInfo) return;

        const artifact = state.pendingArtifactInfo;
        state.pendingArtifactInfo = null;

        const responses: Array<{ question: string; answers: string[] }> = [];
        for (const q of artifact.questions) {
          const answer = answers[q.ids[0] ?? ""];
          const answerText =
            typeof answer === "string"
              ? [answer]
              : Array.isArray(answer)
                ? (answer as string[])
                : [];
          if (answerText.length > 0) {
            responses.push({ question: q.question, answers: answerText });
          }
        }
        if (responses.length === 0) return;

        const jsonBody = {
          messages: [
            {
              author: "USER",
              messageType: "CONTENT",
              fragments: [{ text: "Answering Glean clarification questions" }],
              artifactInfo: {
                id: artifact.id,
                version: artifact.version,
                trackingToken: artifact.trackingToken,
                action: {
                  clarifyingQuestionResponses: { responses },
                },
              },
            },
          ],
          saveChat: true,
          stream: true,
          chatId: state.chatId ?? undefined,
          agentConfig: { agent: "AUTO", mode: "DEFAULT" },
        };

        const jsonString = JSON.stringify(jsonBody);
        const responseExit = yield* Effect.exit(
          runGleanCli(["chat", "--json", jsonString, "dummy"]),
        );

        if (responseExit._tag === "Success") {
          const { stdout } = responseExit.value;
          if (stdout.length > 0) {
            yield* emit({
              ...(yield* buildEventBase({ threadId })),
              type: "content.delta",
              payload: {
                streamKind: "assistant_text" as const,
                delta: stdout,
              },
            });
          }
        }
      });

    const stopSession: ProviderAdapterShape<ProviderAdapterError>["stopSession"] = (threadId) =>
      Effect.gen(function* () {
        const state = sessions.get(threadId);
        if (!state) {
          return;
        }
        if (state.abortController) {
          state.abortController.abort();
        }
        sessions.delete(threadId);
        yield* emit({
          ...(yield* buildEventBase({ threadId })),
          type: "session.exited",
          payload: {
            reason: "Session stopped.",
            recoverable: false,
            exitKind: "graceful" as const,
          },
        });
      });

    const listSessions: ProviderAdapterShape<ProviderAdapterError>["listSessions"] = () =>
      Effect.gen(function* () {
        const results: Array<ProviderSession> = [];
        const now = yield* nowIso;
        for (const [threadId] of sessions) {
          results.push({
            provider: PROVIDER,
            threadId,
            status: "running",
            runtimeMode: "full-access",
            createdAt: now,
            updatedAt: now,
          });
        }
        return results;
      });

    const hasSession: ProviderAdapterShape<ProviderAdapterError>["hasSession"] = (threadId) =>
      Effect.sync(() => sessions.has(threadId));

    const readThread: ProviderAdapterShape<ProviderAdapterError>["readThread"] = (threadId) =>
      Effect.gen(function* () {
        const state = sessions.get(threadId);
        if (!state || !state.chatId) {
          return { threadId, turns: [] };
        }

        const syncExit = yield* Effect.exit(
          runGleanCli(["chat", "sync", "--full", "--chat-id", state.chatId]),
        );
        if (syncExit._tag === "Failure") {
          return { threadId, turns: [] };
        }

        const turns: Array<{ id: TurnId; items: ReadonlyArray<unknown> }> = [];
        const sessionPath = `${os.homedir()}/.glean/sessions/${state.chatId}.jsonl`;
        if (!fs.existsSync(sessionPath)) {
          return { threadId, turns };
        }

        const content = fs.readFileSync(sessionPath, "utf8");
        const lines = content.split("\n").filter((l) => l.trim().length > 0);
        let turnIndex = 0;
        const currentItems: Array<unknown> = [];
        let lastAuthor: string | null = null;

        for (const line of lines) {
          let parsed: Record<string, unknown>;
          try {
            parsed = JSON.parse(line);
          } catch {
            continue;
          }
          const messages = Array.isArray(parsed.messages)
            ? (parsed.messages as Array<Record<string, unknown>>)
            : [];
          for (const msg of messages) {
            const author = typeof msg.author === "string" ? msg.author : "";
            const mtype = typeof msg.messageType === "string" ? msg.messageType : "";
            if (author !== lastAuthor && currentItems.length > 0) {
              turnIndex++;
              turns.push({
                id: TurnId.make(`glean-history-${turnIndex}`),
                items: [...currentItems],
              });
              currentItems.length = 0;
            }
            lastAuthor = author;
            const fragments = Array.isArray(msg.fragments)
              ? (msg.fragments as Array<Record<string, unknown>>)
              : [];
            const texts: string[] = fragments
              .map((f) => (typeof f.text === "string" ? f.text : ""))
              .filter((t) => t.length > 0);
            const hasArtifact = typeof msg.artifactInfo === "object" && msg.artifactInfo !== null;
            if (texts.length > 0 || hasArtifact) {
              currentItems.push({ ...msg, fragments: texts.length > 0 ? fragments : undefined });
            }
          }
        }
        if (currentItems.length > 0) {
          turnIndex++;
          turns.push({ id: TurnId.make(`glean-history-${turnIndex}`), items: [...currentItems] });
        }

        return { threadId, turns };
      });

    const rollbackThread: ProviderAdapterShape<ProviderAdapterError>["rollbackThread"] = (
      _threadId,
      _numTurns,
    ) =>
      new ProviderAdapterValidationError({
        provider: PROVIDER,
        operation: "rollbackThread",
        issue: "Glean does not support conversation rollback.",
      });

    const stopAll: ProviderAdapterShape<ProviderAdapterError>["stopAll"] = () =>
      Effect.sync(() => {
        for (const state of sessions.values()) {
          if (state.abortController) {
            state.abortController.abort();
          }
        }
        sessions.clear();
      });

    return {
      provider: PROVIDER,
      capabilities: {
        sessionModelSwitch: "unsupported",
      },
      startSession,
      sendTurn,
      interruptTurn,
      respondToRequest,
      respondToUserInput,
      stopSession,
      listSessions,
      hasSession,
      readThread,
      rollbackThread,
      stopAll,
      get streamEvents() {
        return Stream.fromQueue(runtimeEvents);
      },
    } satisfies ProviderAdapterShape<ProviderAdapterError>;
  });
}
