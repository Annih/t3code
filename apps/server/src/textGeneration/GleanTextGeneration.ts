import * as Effect from "effect/Effect";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/unstable/process";
import { type GleanSettings, TextGenerationError } from "@t3tools/contracts";

import {
  buildBranchNamePrompt,
  buildCommitMessagePrompt,
  buildPrContentPrompt,
  buildThreadTitlePrompt,
} from "./TextGenerationPrompts.ts";
import {
  normalizeCliError,
  sanitizeCommitSubject,
  sanitizePrTitle,
  sanitizeThreadTitle,
} from "./TextGenerationUtils.ts";
import * as TextGeneration from "./TextGeneration.ts";

const readStreamAsString = <E>(
  operation: string,
  stream: Stream.Stream<Uint8Array, E>,
): Effect.Effect<string, TextGenerationError> =>
  stream.pipe(
    Stream.decodeText(),
    Stream.runFold(
      () => "",
      (acc, chunk) => acc + chunk,
    ),
    Effect.mapError((cause) =>
      normalizeCliError("glean", operation, cause, "Failed to collect process output"),
    ),
  );

const firstNonEmptyLine = (text: string): string =>
  text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .find((line) => line.length > 0) ?? "";

export const makeGleanTextGeneration = Effect.fn("makeGleanTextGeneration")(function* (
  config: GleanSettings,
  env: NodeJS.ProcessEnv,
) {
  const commandSpawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const binaryPath = config.binaryPath || "glean";

  const runGleanOneshot = Effect.fn("runGleanOneshot")(function* (prompt: string) {
    const command = ChildProcess.make(binaryPath, ["chat", "--no-save", prompt], {
      env,
    });

    const onExit = Effect.gen(function* () {
      const child = yield* commandSpawner
        .spawn(command)
        .pipe(
          Effect.mapError((cause) =>
            normalizeCliError("glean", "glean oneshot", cause, "Failed to spawn Glean CLI process"),
          ),
        );

      const [stdout, stderr, exitCode] = yield* Effect.all(
        [
          readStreamAsString("glean oneshot", child.stdout),
          readStreamAsString("glean oneshot", child.stderr),
          child.exitCode.pipe(
            Effect.mapError((cause) =>
              normalizeCliError(
                "glean",
                "glean oneshot",
                cause,
                "Failed to read Glean CLI exit code",
              ),
            ),
          ),
        ],
        { concurrency: "unbounded" },
      );

      if (exitCode !== 0) {
        const stderrDetail = stderr.trim();
        const stdoutDetail = stdout.trim();
        const detail = stderrDetail.length > 0 ? stderrDetail : stdoutDetail;
        return yield* new TextGenerationError({
          operation: "glean oneshot",
          detail:
            detail.length > 0
              ? `Glean CLI command failed: ${detail}`
              : `Glean CLI command failed with code ${exitCode}.`,
        });
      }

      return stdout;
    });

    return yield* Effect.scoped(onExit);
  });

  const generateCommitMessage: TextGeneration.TextGeneration["Service"]["generateCommitMessage"] =
    Effect.fn("GleanTextGeneration.generateCommitMessage")(function* (input) {
      const { prompt } = buildCommitMessagePrompt({
        branch: input.branch,
        stagedSummary: input.stagedSummary,
        stagedPatch: input.stagedPatch,
        includeBranch: input.includeBranch === true,
        policy: input.policy,
      });

      const response = yield* runGleanOneshot(prompt).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation: "generateCommitMessage",
              detail: "Glean CLI request failed.",
              cause,
            }),
        ),
      );

      const lines = response.split(/\r?\n/).map((line) => line.trim());
      const subject = lines[0]?.trim() ?? "";
      const body = lines.slice(1).join("\n").trim();

      return {
        subject: sanitizeCommitSubject(subject),
        body,
        ...(input.includeBranch === true ? { branch: "" } : {}),
      };
    });

  const generatePrContent: TextGeneration.TextGeneration["Service"]["generatePrContent"] =
    Effect.fn("GleanTextGeneration.generatePrContent")(function* (input) {
      const { prompt } = buildPrContentPrompt({
        baseBranch: input.baseBranch,
        headBranch: input.headBranch,
        commitSummary: input.commitSummary,
        diffSummary: input.diffSummary,
        diffPatch: input.diffPatch,
        policy: input.policy,
        changeRequestTemplate: input.changeRequestTemplate,
      });

      const response = yield* runGleanOneshot(prompt).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation: "generatePrContent",
              detail: "Glean CLI request failed.",
              cause,
            }),
        ),
      );

      const lines = response.split(/\r?\n/).map((line) => line.trim());
      const title = lines[0]?.trim() ?? "";
      const body = lines.slice(1).join("\n").trim();

      return {
        title: sanitizePrTitle(title),
        body,
      };
    });

  const generateBranchName: TextGeneration.TextGeneration["Service"]["generateBranchName"] =
    Effect.fn("GleanTextGeneration.generateBranchName")(function* (input) {
      const { prompt } = buildBranchNamePrompt({
        message: input.message,
        attachments: input.attachments,
      });

      const response = yield* runGleanOneshot(prompt).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation: "generateBranchName",
              detail: "Glean CLI request failed.",
              cause,
            }),
        ),
      );

      return {
        branch: firstNonEmptyLine(response),
      };
    });

  const generateThreadTitle: TextGeneration.TextGeneration["Service"]["generateThreadTitle"] =
    Effect.fn("GleanTextGeneration.generateThreadTitle")(function* (input) {
      const { prompt } = buildThreadTitlePrompt({
        message: input.message,
        previousTitle: input.previousTitle,
        attachments: input.attachments,
      });

      const response = yield* runGleanOneshot(prompt).pipe(
        Effect.mapError(
          (cause) =>
            new TextGenerationError({
              operation: "generateThreadTitle",
              detail: "Glean CLI request failed.",
              cause,
            }),
        ),
      );

      return {
        title: sanitizeThreadTitle(firstNonEmptyLine(response)),
      };
    });

  return {
    generateCommitMessage,
    generatePrContent,
    generateBranchName,
    generateThreadTitle,
  } satisfies TextGeneration.TextGeneration["Service"];
});
