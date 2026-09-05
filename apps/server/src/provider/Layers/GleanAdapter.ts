import {
  EventId,
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
import { HttpClient, HttpClientRequest } from "effect/unstable/http";

import {
  ProviderAdapterRequestError,
  ProviderAdapterSessionNotFoundError,
  ProviderAdapterValidationError,
  type ProviderAdapterError,
} from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import type { ProviderSession, ProviderTurnStartResult } from "@t3tools/contracts";

const PROVIDER = ProviderDriverKind.make("glean");

interface GleanSessionState {
  chatId: string | null;
  abortController: AbortController | null;
}

interface GleanChatMessage {
  messageId?: string;
  author?: string;
  messageType?: string;
  hasMoreFragments?: boolean;
  fragments?: Array<{
    text?: string;
    structuredResults?: Array<{
      title?: string;
      url?: string;
      datasource?: string;
    }>;
  }>;
  artifactInfo?: {
    id?: string;
    version?: number;
    trackingToken?: string;
  };
}

interface GleanChatResponse {
  chatId?: string;
  messageType?: string;
  messages?: Array<GleanChatMessage>;
  stepId?: string;
  isStepComplete?: boolean;
  backendTimeMillis?: number;
}

function resolveToken(config: GleanSettings): string | null {
  if (config.apiToken && config.apiToken.trim().length > 0) {
    return config.apiToken.trim();
  }
  return null;
}

function buildRequest(
  url: string,
  body: Record<string, unknown>,
  token: string | null,
): HttpClientRequest.HttpClientRequest {
  let request = HttpClientRequest.post(url).pipe(
    HttpClientRequest.bodyJsonUnsafe(body),
    HttpClientRequest.setHeader("Content-Type", "application/json"),
  );
  if (token) {
    request = request.pipe(HttpClientRequest.setHeader("Authorization", `Bearer ${token}`));
  }
  return request;
}

function makeRawEntry(parsedLine: unknown) {
  return {
    source: "glean.ndjson" as const,
    payload: parsedLine,
  };
}

function makeAssistantMessageItemId(messageId: string): string {
  return `glean-msg-${messageId}`;
}

function makeUpdateItemId(stepId: string | undefined, index: number): string {
  return `glean-update-${stepId ?? index}`;
}

function makeArtifactItemId(artifactInfo: { id?: string }): string {
  return `glean-artifact-${artifactInfo.id ?? "unknown"}`;
}

function makeCitationItemId(datasource: string, title: string): string {
  return `glean-cite-${datasource}-${title}`;
}

export interface GleanAdapterOptions {
  readonly instanceId?: ProviderInstanceId;
  readonly environment?: NodeJS.ProcessEnv;
  readonly nativeEventLogger?: unknown;
}

export function makeGleanAdapter(config: GleanSettings, options?: GleanAdapterOptions) {
  return Effect.gen(function* () {
    const crypto = yield* Crypto.Crypto;
    const httpClientService = yield* HttpClient.HttpClient;
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
        const serverUrl = config.serverUrl.trim();
        const chatUrl = `${serverUrl}/rest/api/v1/chat`;
        const token = resolveToken(config);

        const body: Record<string, unknown> = {
          messages: [
            {
              author: "USER",
              fragments: [{ text }],
            },
          ],
          saveChat: true,
          agentConfig: {
            agent: "general",
            mode: "DEFAULT",
          },
        };

        if (state.chatId) {
          body.chatId = state.chatId;
        }

        yield* emit({
          ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
          type: "turn.started",
          payload: {},
        });

        const request = buildRequest(chatUrl, body, token);
        const responseExit = yield* Effect.exit(httpClientService.execute(request));

        if (responseExit._tag === "Failure") {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "POST /rest/api/v1/chat",
            detail: "Glean chat request failed.",
            cause: responseExit.cause,
          });
        }

        const response = responseExit.value;
        let turnCompleted = false;
        let receivedChatId: string | null = null;

        const textExit = yield* Effect.exit(response.text);
        if (textExit._tag === "Failure") {
          return yield* new ProviderAdapterRequestError({
            provider: PROVIDER,
            method: "POST /rest/api/v1/chat",
            detail: "Failed to read Glean chat response body.",
            cause: textExit.cause,
          });
        }

        const lines = textExit.value
          .split("\n")
          .map((l: string) => l.trim())
          .filter((l: string) => l.length > 0);

        for (let lineIdx = 0; lineIdx < lines.length; lineIdx++) {
          const rawLine = lines[lineIdx];
          if (!rawLine) continue;
          const line = rawLine;

          let parsedLine: GleanChatResponse;
          try {
            parsedLine = JSON.parse(line);
          } catch {
            continue;
          }

          if (parsedLine.chatId && !receivedChatId) {
            receivedChatId = parsedLine.chatId;
          }

          const messageType = parsedLine.messageType ?? "";
          const messages = parsedLine.messages ?? [];

          if (messageType === "CONTENT") {
            for (const msg of messages) {
              if (msg.author === "GLEAN_AI") {
                const messageId = msg.messageId ?? "unknown";
                const rawEntry = makeRawEntry(parsedLine);

                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                    itemId: makeAssistantMessageItemId(messageId),
                    raw: rawEntry,
                  })),
                  type: "item.started",
                  payload: {
                    itemType: "assistant_message",
                    status: "inProgress",
                    title: "Assistant message",
                  },
                });

                for (const fragment of msg.fragments ?? []) {
                  if (fragment.text) {
                    yield* emit({
                      ...(yield* buildEventBase({
                        threadId: input.threadId,
                        turnId,
                        itemId: makeAssistantMessageItemId(messageId),
                        raw: rawEntry,
                      })),
                      type: "content.delta",
                      payload: {
                        streamKind: "assistant_text" as const,
                        delta: fragment.text,
                      },
                    });
                  }

                  if (fragment.structuredResults) {
                    for (const result of fragment.structuredResults) {
                      if (result.datasource && result.title) {
                        const citeItemId = makeCitationItemId(result.datasource, result.title);
                        const citeRaw = makeRawEntry(parsedLine);
                        yield* emit({
                          ...(yield* buildEventBase({
                            threadId: input.threadId,
                            turnId,
                            itemId: citeItemId,
                            raw: citeRaw,
                          })),
                          type: "item.started",
                          payload: {
                            itemType: "citation" as const,
                            status: "inProgress",
                            title: result.title,
                          },
                        });
                        yield* emit({
                          ...(yield* buildEventBase({
                            threadId: input.threadId,
                            turnId,
                            itemId: citeItemId,
                            raw: citeRaw,
                          })),
                          type: "item.completed",
                          payload: {
                            itemType: "citation" as const,
                            status: "completed",
                            title: result.title,
                            detail: result.url,
                            data: {
                              title: result.title,
                              url: result.url,
                              datasource: result.datasource,
                            },
                          },
                        });
                      }
                    }
                  }
                }

                if (msg.hasMoreFragments === false) {
                  const allText = (msg.fragments ?? [])
                    .map((f: { text?: string }) => f.text ?? "")
                    .join("");
                  yield* emit({
                    ...(yield* buildEventBase({
                      threadId: input.threadId,
                      turnId,
                      itemId: makeAssistantMessageItemId(messageId),
                      raw: makeRawEntry(parsedLine),
                    })),
                    type: "item.completed",
                    payload: {
                      itemType: "assistant_message",
                      status: "completed",
                      title: "Assistant message",
                      detail: allText,
                    },
                  });
                }
              }
            }
          } else if (messageType === "UPDATE") {
            for (const msg of messages) {
              const fragments = msg.fragments ?? [];
              for (const fragment of fragments) {
                if (fragment.text) {
                  const updateItemId = makeUpdateItemId(parsedLine.stepId, lineIdx);
                  const updateRaw = makeRawEntry(parsedLine);
                  yield* emit({
                    ...(yield* buildEventBase({
                      threadId: input.threadId,
                      turnId,
                      itemId: updateItemId,
                      raw: updateRaw,
                    })),
                    type: "item.started",
                    payload: {
                      itemType: "reasoning",
                      status: "inProgress",
                      title: fragment.text,
                    },
                  });
                  yield* emit({
                    ...(yield* buildEventBase({
                      threadId: input.threadId,
                      turnId,
                      itemId: updateItemId,
                      raw: updateRaw,
                    })),
                    type: "item.completed",
                    payload: {
                      itemType: "reasoning",
                      status: "completed",
                      title: fragment.text,
                    },
                  });
                }
              }
            }
          } else if (messageType === "ARTIFACT_MESSAGE") {
            for (const msg of messages) {
              if (msg.artifactInfo) {
                const artifactItemId = makeArtifactItemId(msg.artifactInfo);
                const artifactRaw = makeRawEntry(parsedLine);
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                    itemId: artifactItemId,
                    raw: artifactRaw,
                  })),
                  type: "item.started",
                  payload: {
                    itemType: "artifact_reference" as const,
                    status: "inProgress",
                    title: "Artifact",
                    data: msg.artifactInfo,
                  },
                });
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                    itemId: artifactItemId,
                    raw: artifactRaw,
                  })),
                  type: "item.completed",
                  payload: {
                    itemType: "artifact_reference" as const,
                    status: "completed",
                    title: "Artifact",
                    data: msg.artifactInfo,
                  },
                });
              }
            }
          } else if (messageType === "CONTROL") {
            for (const msg of messages) {
              const msgType = (msg as { messageType?: string }).messageType ?? "";
              if (msg.author === "AGL" && msgType === "CONTROL_FINISH") {
                turnCompleted = true;
                yield* emit({
                  ...(yield* buildEventBase({
                    threadId: input.threadId,
                    turnId,
                    raw: makeRawEntry(parsedLine),
                  })),
                  type: "turn.completed",
                  payload: {
                    state: "completed" as const,
                    stopReason: "stop",
                  },
                });
              }
            }
          } else if (messageType === "ERROR") {
            yield* emit({
              ...(yield* buildEventBase({
                threadId: input.threadId,
                turnId,
                raw: makeRawEntry(parsedLine),
              })),
              type: "runtime.error",
              payload: {
                message: "Glean returned an error.",
                class: "provider_error",
                detail: parsedLine,
              },
            });
          }
        }

        if (!turnCompleted) {
          yield* emit({
            ...(yield* buildEventBase({ threadId: input.threadId, turnId })),
            type: "turn.completed",
            payload: {
              state: "completed" as const,
              stopReason: "stop",
            },
          });
        }

        if (receivedChatId) {
          state.chatId = receivedChatId;
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

        const serverUrl = config.serverUrl.trim();
        const getChatUrl = `${serverUrl}/rest/api/v1/getchat`;
        const token = resolveToken(config);

        const request = buildRequest(getChatUrl, { id: state.chatId }, token);
        const responseExit = yield* Effect.exit(httpClientService.execute(request));

        if (responseExit._tag === "Failure") {
          return { threadId, turns: [] };
        }

        const response = responseExit.value;
        const jsonExit = yield* Effect.exit(response.json);

        if (jsonExit._tag === "Failure") {
          return { threadId, turns: [] };
        }

        const data = jsonExit.value as {
          messages?: Array<{
            messageId?: string;
            fragments?: Array<{ text?: string }>;
            author?: string;
          }>;
        };

        if (!data || !Array.isArray(data.messages)) {
          return { threadId, turns: [] };
        }

        const uuid = yield* generateUUID;

        const turns = data.messages
          .filter((msg: { author?: string }) => msg.author === "GLEAN_AI")
          .map((msg: { messageId?: string }) => ({
            id: TurnId.make(msg.messageId ?? uuid),
            items: [msg],
          }));

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
