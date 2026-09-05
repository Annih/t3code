import {
  EventId,
  GLEAN_DEFAULT_MODEL,
  type GleanSettings,
  type ProviderRuntimeEvent,
  ProviderDriverKind,
  ProviderInstanceId,
  RuntimeItemId,
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

        sessions.set(input.threadId, { chatId: null, abortController: null });

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

        const args = ["chat", "--save"];
        if (state.chatId) {
          args.push("--resume", state.chatId);
        }
        args.push(text);

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

        const { stdout: responseText, stderr: errorText } = responseExit.value;
        const displayText =
          responseText.length > 0 ? responseText : errorText.length > 0 ? errorText : "";

        if (displayText.length > 0) {
          const msgItemId = RuntimeItemId.make(`glean-msg-${uuid}`);
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId, turnId, itemId: msgItemId })),
            type: "item.started",
            payload: {
              itemType: "assistant_message",
              status: "inProgress",
              title: "Assistant message",
            },
          });
          yield* emit({
            ...(yield* buildEventBase({
              threadId: input.threadId,
              turnId,
              itemId: msgItemId,
              raw: { source: "glean.ndjson" as const, payload: responseText },
            })),
            type: "content.delta",
            payload: {
              streamKind: "assistant_text" as const,
              delta: displayText,
            },
          });
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId, turnId, itemId: msgItemId })),
            type: "item.completed",
            payload: {
              itemType: "assistant_message",
              status: "completed",
              title: "Assistant message",
              detail: displayText,
            },
          });
        }

        state.abortController = null;

        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
          type: "turn.completed",
          payload: {
            state: "completed",
            stopReason: "stop",
          },
        });

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

          if (!state.chatId) {
            const sessionDir = `${os.homedir()}/.glean/sessions`;
            try {
              const files = fs.readdirSync(sessionDir).filter((f: string) => f.endsWith(".jsonl"));
              if (files.length > 0) {
                const latest = files
                  .map((f: string) => ({
                    name: f,
                    mtime: fs.statSync(`${sessionDir}/${f}`).mtimeMs,
                  }))
                  .sort((a, b) => b.mtime - a.mtime)[0];
                if (latest) {
                  state.chatId = latest.name.replace(/\.jsonl$/, "");
                }
              }
            } catch {
              // ignore — sessions directory may not exist
            }
          }
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

    const respondToUserInput: ProviderAdapterShape<ProviderAdapterError>["respondToUserInput"] =
      () => Effect.void;

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
          runGleanCli(["chat", "sync", "--chat-id", state.chatId]),
        );
        if (syncExit._tag === "Failure") {
          return { threadId, turns: [] };
        }

        return { threadId, turns: [] };
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
