/**
 * The editor's working model of a pipeline.
 *
 * A `PipelineDraft` is a `PipelineDefinition` minus the fields the control
 * plane owns (`version`, `builtin`, `project_id`): those are displayed but
 * never authored, so keeping them out of the draft keeps the YAML pane honest
 * — everything you see in it is something you can change.
 */
import { z } from "zod";

import { FAN_OUT_PATHS, type JsonObject, type JsonValue, type PipelineDefinition } from "@/lib/api/types";

/** `^[a-zA-Z0-9._-]+$`, the uid pattern the control plane enforces. */
export const UID_PATTERN = /^[a-zA-Z0-9._-]+$/;

/** Recursive JSON value, for plugin `config:` blocks. */
export const jsonValueSchema: z.ZodType<JsonValue, JsonValue> = z.lazy(() =>
  z.union([
    z.string(),
    z.number(),
    z.boolean(),
    z.null(),
    z.array(jsonValueSchema),
    z.record(z.string(), jsonValueSchema),
  ]),
);

const configSchema = z.record(z.string(), jsonValueSchema);

const backoffSchema = z.enum(["exponential", "linear", "none"]);
const fanOutSchema = z.enum(FAN_OUT_PATHS);

/** Retry policy, always fully materialised inside a draft. */
export const retryDraftSchema = z.object({
  max_attempts: z.number().int().min(1).max(100),
  backoff: backoffSchema,
  initial_interval_secs: z.number().int().min(0).max(3600),
});

/** One step, as the editor holds it. */
export const stepDraftSchema = z.object({
  id: z.string().min(1, "step id is required").regex(UID_PATTERN, "use letters, digits, . _ -"),
  plugin: z.string().min(1, "pick a plugin"),
  depends_on: z.array(z.string()),
  fan_out: fanOutSchema.optional(),
  config: configSchema,
  timeout_secs: z.number().int().min(1).max(86_400).optional(),
  retry: retryDraftSchema.optional(),
});

/** Auto-routing trigger, always present in a draft (possibly all-empty). */
export const triggerDraftSchema = z.object({
  content_types: z.array(z.string()),
  filename_pattern: z.string(),
  index_pattern: z.string(),
});

/** The whole editable pipeline. */
export const pipelineDraftSchema = z.object({
  uid: z
    .string()
    .min(1, "uid is required")
    .regex(UID_PATTERN, "use letters, digits, . _ - only"),
  name: z.string(),
  description: z.string(),
  trigger: triggerDraftSchema,
  steps: z.array(stepDraftSchema).min(1, "a pipeline needs at least one step"),
});

export type RetryDraft = z.infer<typeof retryDraftSchema>;
export type StepDraft = z.infer<typeof stepDraftSchema>;
export type TriggerDraft = z.infer<typeof triggerDraftSchema>;
export type PipelineDraft = z.infer<typeof pipelineDraftSchema>;

// ---------------------------------------------------------------------------
// Wire form: what a hand-written YAML/JSON document may look like
// ---------------------------------------------------------------------------

/**
 * The tolerant shape accepted from the YAML pane: optional keys, partial retry
 * blocks, `null` instead of an omitted value. `normalizeDraft` turns it into a
 * fully materialised `PipelineDraft`.
 */
export const pipelineWireSchema = z.object({
  uid: z.string(),
  name: z.string().nullish(),
  description: z.string().nullish(),
  trigger: z
    .object({
      content_types: z.array(z.string()).nullish(),
      filename_pattern: z.string().nullish(),
      index_pattern: z.string().nullish(),
    })
    .nullish(),
  steps: z
    .array(
      z.object({
        id: z.string(),
        plugin: z.string(),
        depends_on: z.array(z.string()).nullish(),
        fan_out: z.string().nullish(),
        config: configSchema.nullish(),
        timeout_secs: z.number().int().nullish(),
        retry: z
          .object({
            max_attempts: z.number().int().nullish(),
            backoff: backoffSchema.nullish(),
            initial_interval_secs: z.number().int().nullish(),
          })
          .nullish(),
      }),
    )
    .nullish(),
});

export type PipelineWire = z.infer<typeof pipelineWireSchema>;

/** Server-side defaults for a step retry policy (`RetryConfig::default`). */
export const DEFAULT_RETRY: RetryDraft = {
  max_attempts: 3,
  backoff: "exponential",
  initial_interval_secs: 1,
};

/** Start-to-close timeout the worker applies when a step omits one. */
export const DEFAULT_TIMEOUT_SECS = 300;

function isFanOut(value: string): value is PipelineDraft["steps"][number]["fan_out"] & string {
  return (FAN_OUT_PATHS as readonly string[]).includes(value);
}

/** Fill in every optional key so the editor never deals with `undefined`. */
export function normalizeDraft(wire: PipelineWire): PipelineDraft {
  return {
    uid: wire.uid,
    name: wire.name ?? "",
    description: wire.description ?? "",
    trigger: {
      content_types: wire.trigger?.content_types ?? [],
      filename_pattern: wire.trigger?.filename_pattern ?? "",
      index_pattern: wire.trigger?.index_pattern ?? "",
    },
    steps: (wire.steps ?? []).map((step) => ({
      id: step.id,
      plugin: step.plugin,
      depends_on: step.depends_on ?? [],
      // An unsupported fan_out survives normalization as `undefined` here but is
      // reported by the wire parser, so it is never silently dropped.
      ...(step.fan_out && isFanOut(step.fan_out) ? { fan_out: step.fan_out } : {}),
      config: (step.config ?? {}) as JsonObject,
      ...(step.timeout_secs != null ? { timeout_secs: step.timeout_secs } : {}),
      ...(step.retry
        ? {
            retry: {
              max_attempts: step.retry.max_attempts ?? DEFAULT_RETRY.max_attempts,
              backoff: step.retry.backoff ?? DEFAULT_RETRY.backoff,
              initial_interval_secs:
                step.retry.initial_interval_secs ?? DEFAULT_RETRY.initial_interval_secs,
            },
          }
        : {}),
    })),
  };
}

/** An empty draft for `/pipelines/new`. */
export function emptyDraft(): PipelineDraft {
  return {
    uid: "",
    name: "",
    description: "",
    trigger: { content_types: [], filename_pattern: "", index_pattern: "" },
    steps: [],
  };
}

/** Read a stored pipeline into the editor. */
export function pipelineToDraft(pipeline: PipelineDefinition): PipelineDraft {
  return normalizeDraft({
    uid: pipeline.uid,
    name: pipeline.name ?? "",
    description: pipeline.description ?? "",
    trigger: pipeline.trigger ?? null,
    steps: pipeline.steps.map((step) => ({
      id: step.id,
      plugin: step.plugin,
      depends_on: step.depends_on ?? null,
      fan_out: step.fan_out ?? null,
      config: (step.config ?? {}) as JsonObject,
      timeout_secs: step.timeout_secs ?? null,
      retry: step.retry ?? null,
    })),
  });
}

/**
 * Turn the draft into the body of `POST /pipelines`. Empty optionals are
 * dropped so the stored definition stays as small as what was authored.
 */
export function draftToPipeline(draft: PipelineDraft): PipelineDefinition {
  const trigger = {
    ...(draft.trigger.content_types.length > 0
      ? { content_types: draft.trigger.content_types }
      : {}),
    ...(draft.trigger.filename_pattern.trim()
      ? { filename_pattern: draft.trigger.filename_pattern.trim() }
      : {}),
    ...(draft.trigger.index_pattern.trim()
      ? { index_pattern: draft.trigger.index_pattern.trim() }
      : {}),
  };

  return {
    uid: draft.uid.trim(),
    ...(draft.name.trim() ? { name: draft.name.trim() } : {}),
    ...(draft.description.trim() ? { description: draft.description.trim() } : {}),
    ...(Object.keys(trigger).length > 0 ? { trigger } : {}),
    steps: draft.steps.map((step) => ({
      id: step.id,
      plugin: step.plugin,
      ...(step.depends_on.length > 0 ? { depends_on: step.depends_on } : {}),
      ...(step.fan_out ? { fan_out: step.fan_out } : {}),
      ...(Object.keys(step.config).length > 0 ? { config: step.config } : {}),
      ...(step.timeout_secs !== undefined ? { timeout_secs: step.timeout_secs } : {}),
      ...(step.retry ? { retry: step.retry } : {}),
    })),
  };
}

/** Duplicate a pipeline under a new uid, dropping every server-owned field. */
export function cloneDraft(draft: PipelineDraft): PipelineDraft {
  const base = draft.uid.replace(/^builtin\./, "");
  return {
    ...draft,
    uid: `${base}-copy`,
    name: draft.name ? `${draft.name} (copy)` : "",
  };
}
