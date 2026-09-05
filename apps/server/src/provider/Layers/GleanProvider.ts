import { GLEAN_DEFAULT_MODEL, type GleanSettings } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import { HttpClient, HttpClientRequest } from "effect/unstable/http";
import * as os from "node:os";
import * as nodeCrypto from "node:crypto";

import { buildServerProvider, type ServerProviderDraft } from "../providerSnapshot.ts";

function sha256First16(input: string): string {
  return nodeCrypto.createHash("sha256").update(input).digest("hex").substring(0, 16);
}

const GLEAN_PRESENTATION = {
  displayName: "Glean",
  showInteractionModeToggle: true,
} as const;

const GLEAN_DEFAULT_MODELS = [
  {
    slug: GLEAN_DEFAULT_MODEL,
    name: "Glean Assistant",
    isDefault: true,
    isCustom: false,
    capabilities: null,
  },
];

function extractHost(serverUrl: string): string | null {
  const trimmed = serverUrl.trim();
  if (trimmed.length === 0) return null;
  try {
    const url = new URL(trimmed);
    return url.host;
  } catch {
    return trimmed.replace(/^https?:\/\//, "").split("/")[0] ?? null;
  }
}

const resolveToken = Effect.fn("resolveToken")(function* (config: GleanSettings) {
  if (config.apiToken && config.apiToken.trim().length > 0) {
    return config.apiToken.trim();
  }
  if (config.authType === "oauth") {
    const host = extractHost(config.serverUrl);
    if (!host) return null;
    const home = os.homedir();
    const hash = sha256First16(host);
    const tokenPath = `${home}/.local/state/glean-cli/${hash}/tokens.json`;
    const fileSystem = yield* FileSystem.FileSystem;
    const exists = yield* fileSystem.exists(tokenPath).pipe(Effect.orElseSucceed(() => false));
    if (!exists) return null;
    const content = yield* fileSystem
      .readFileString(tokenPath)
      .pipe(Effect.orElseSucceed(() => ""));
    if (content.length === 0) return null;
    try {
      const parsed = JSON.parse(content);
      return typeof parsed.access_token === "string" ? parsed.access_token : null;
    } catch {
      return null;
    }
  }
  return null;
});

export const makePendingGleanProvider = (
  gleanSettings: GleanSettings,
): Effect.Effect<ServerProviderDraft> =>
  Effect.gen(function* () {
    const checkedAt = yield* Effect.map(DateTime.now, DateTime.formatIso);

    if (!gleanSettings.enabled) {
      return buildServerProvider({
        presentation: GLEAN_PRESENTATION,
        enabled: false,
        checkedAt,
        models: GLEAN_DEFAULT_MODELS,
        probe: {
          installed: false,
          version: null,
          status: "warning",
          auth: { status: "unknown" },
          message:
            gleanSettings.serverUrl.trim().length > 0
              ? "Glean is disabled in T3 Code settings. A server URL is configured."
              : "Glean is disabled in T3 Code settings.",
        },
      });
    }

    return buildServerProvider({
      presentation: GLEAN_PRESENTATION,
      enabled: true,
      checkedAt,
      models: GLEAN_DEFAULT_MODELS,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Glean provider status has not been checked in this session yet.",
      },
    });
  });

export const checkGleanProviderStatus = Effect.fn("checkGleanProviderStatus")(function* (
  config: GleanSettings,
  _cwd: string,
  _env: NodeJS.ProcessEnv,
): Effect.fn.Return<ServerProviderDraft, never, FileSystem.FileSystem | HttpClient.HttpClient> {
  const checkedAt = DateTime.formatIso(yield* DateTime.now);
  const serverUrl = config.serverUrl.trim();

  if (serverUrl.length === 0) {
    return buildServerProvider({
      presentation: GLEAN_PRESENTATION,
      enabled: config.enabled,
      checkedAt,
      models: GLEAN_DEFAULT_MODELS,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Glean server URL is not configured.",
      },
    });
  }

  if (!config.enabled) {
    return buildServerProvider({
      presentation: GLEAN_PRESENTATION,
      enabled: false,
      checkedAt,
      models: GLEAN_DEFAULT_MODELS,
      probe: {
        installed: false,
        version: null,
        status: "warning",
        auth: { status: "unknown" },
        message: "Glean is disabled in T3 Code settings.",
      },
    });
  }

  const token = yield* resolveToken(config);
  const searchUrl = `${serverUrl}/rest/api/v1/search`;

  let request = HttpClientRequest.post(searchUrl).pipe(
    HttpClientRequest.bodyJsonUnsafe({ query: "", pageSize: 1 }),
    HttpClientRequest.setHeader("Content-Type", "application/json"),
  );
  if (token) {
    request = request.pipe(HttpClientRequest.setHeader("Authorization", `Bearer ${token}`));
  }

  const httpClient = yield* HttpClient.HttpClient;
  const probeExit = yield* Effect.exit(httpClient.execute(request));

  if (probeExit._tag === "Success") {
    return buildServerProvider({
      presentation: GLEAN_PRESENTATION,
      enabled: true,
      checkedAt,
      models: GLEAN_DEFAULT_MODELS,
      probe: {
        installed: true,
        version: null,
        status: "ready",
        auth: { status: "authenticated" },
      },
    });
  }

  const causeStr = String(probeExit.cause);

  if (causeStr.includes("401") || causeStr.includes("403")) {
    return buildServerProvider({
      presentation: GLEAN_PRESENTATION,
      enabled: true,
      checkedAt,
      models: GLEAN_DEFAULT_MODELS,
      probe: {
        installed: true,
        version: null,
        status: "error",
        auth: { status: "unauthenticated" },
        message: "Glean authentication failed. Check your API token or OAuth credentials.",
      },
    });
  }

  return buildServerProvider({
    presentation: GLEAN_PRESENTATION,
    enabled: true,
    checkedAt,
    models: GLEAN_DEFAULT_MODELS,
    probe: {
      installed: false,
      version: null,
      status: "error",
      auth: { status: "unknown" },
      message: `Couldn't reach the Glean server at ${serverUrl}. Check the URL and your network connection.`,
    },
  });
});
