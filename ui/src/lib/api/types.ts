/**
 * Wire types for the meili-ingest gateway API.
 *
 * These mirror `crates/plugin-sdk/src/types.rs` and `docs/openapi.yaml`
 * one-for-one, in snake_case exactly as the JSON uses. Fields that Rust
 * declares as `Option<T>` with `skip_serializing_if = "Option::is_none"`
 * are optional here; fields the OpenAPI spec types as `[T, "null"]` are
 * optional *and* nullable.
 */

/**
 * Any value that can appear in a plugin `config:` block or a JSON Schema.
 *
 * The recursive halves are named interfaces on purpose: TypeScript caches
 * instantiations of a named type, which is what stops react-hook-form's
 * `DeepPartial` / `Path` helpers from recursing forever over a config block.
 */
export type JsonValue = string | number | boolean | null | JsonArray | JsonObject;

/**
 * A JSON array. Declared as an interface, not `JsonValue[]`: TypeScript caches
 * instantiations of named types, and without that cache react-hook-form's
 * `DeepPartial` / `Path` helpers hit "type instantiation is excessively deep"
 * on a step's config block.
 */
// eslint-disable-next-line @typescript-eslint/no-empty-object-type -- see above
export interface JsonArray extends Array<JsonValue> {}

/** A JSON object, and the shape of a plugin `config:` block. */
export interface JsonObject {
  [key: string]: JsonValue;
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/** `JobStatus` — status of a job or of a single step. */
export type JobStatus =
  | "queued"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled";

/** `InputKind` — input variants a plugin declares it accepts. */
export type InputKind = "bytes" | "ref" | "documents" | "many" | "empty";

/** `OutputKind` — the single output variant a plugin produces. */
export type OutputKind =
  | "bytes"
  | "ref"
  | "documents"
  | "many"
  | "indexed"
  | "empty";

/** `PluginKind` — how a plugin is executed. */
export type PluginKind = "builtin" | "wasm" | "grpc";

/** `Backoff` — retry backoff strategy. */
export type Backoff = "exponential" | "linear" | "none";

/** The only `fan_out` JSONPaths the workflow engine understands. */
export const FAN_OUT_PATHS = ["$.documents", "$.many", "$"] as const;
/** `fan_out` value of a step. */
export type FanOutPath = (typeof FAN_OUT_PATHS)[number];

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

/** `RetryConfig` — retry policy for a step. */
export interface RetryConfig {
  /** Maximum attempts including the first one. Default 3. */
  max_attempts: number;
  /** Backoff strategy. Default `exponential`. */
  backoff: Backoff;
  /** Initial interval in seconds. Default 1. */
  initial_interval_secs: number;
}

/** `StepDefinition` — one node of the pipeline DAG. */
export interface StepDefinition {
  /** Unique step id within the pipeline. */
  id: string;
  /** Plugin name, matching `PluginManifest.name`. */
  plugin: string;
  /** Steps this one waits for. Omitted on a non-first step = the previous step. */
  depends_on?: string[];
  /** Split the upstream output into parallel activities. */
  fan_out?: FanOutPath;
  /** Plugin config block, validated against the plugin's `config_schema`. */
  config?: JsonObject;
  /** Start-to-close timeout in seconds (server default 300). */
  timeout_secs?: number;
  /** Retry policy (server default: 3 attempts, exponential, 1s). */
  retry?: RetryConfig;
}

/** `PipelineTrigger` — when a pipeline is auto-selected by `POST /ingest`. */
export interface PipelineTrigger {
  /** MIME types, exact or `type/*`. */
  content_types?: string[];
  /** Glob on the filename (`*`, `?`), case-insensitive. */
  filename_pattern?: string;
  /** Index to write to when this pipeline is selected. */
  index_pattern?: string;
}

/** `PipelineDefinition` — a full pipeline, authored as YAML or JSON. */
export interface PipelineDefinition {
  /** Unique id. `builtin.*` is reserved by the control plane. */
  uid: string;
  /** Display name (defaults to `uid`). */
  name?: string;
  /** Free-form description. */
  description?: string;
  /** Bumped by the control plane on every update. */
  version?: number;
  /** Auto-routing trigger. */
  trigger?: PipelineTrigger;
  /** Steps (a DAG); at least one. */
  steps: StepDefinition[];
  /** Read-only: true for pipelines compiled into the control plane. */
  builtin?: boolean;
  /** Read-only: tenant scope. Absent = global. */
  project_id?: string;
}

// ---------------------------------------------------------------------------
// Plugins
// ---------------------------------------------------------------------------

/**
 * The subset of JSON Schema that plugin `config_schema`s use. Anything the
 * schema-driven form does not understand falls back to a raw JSON editor,
 * so this type stays deliberately permissive.
 */
export interface JsonSchema {
  /** `"string"`, or a union such as `["integer", "null"]`. */
  type?: string | string[];
  /** Object properties, in declaration order. */
  properties?: Record<string, JsonSchema>;
  /** Names of required properties. */
  required?: string[];
  /** Closed enumeration of allowed values. */
  enum?: JsonValue[];
  /** Default applied by the plugin when the key is absent. */
  default?: JsonValue;
  /** Helper text shown under the field. */
  description?: string;
  /**
   * The value is supplied by the system, not the author.
   *
   * `meili_indexer` marks the tenant's `host`, `api_key` and `index` this way: the
   * workflow injects them from the MeiliContext at run time, so an editor must not
   * render inputs for them (one of them is a secret).
   */
  readOnly?: boolean;
  /** Inclusive numeric lower bound. */
  minimum?: number;
  /** Inclusive numeric upper bound. */
  maximum?: number;
  /** String length bounds. */
  minLength?: number;
  /** String length bounds. */
  maxLength?: number;
  /** Item schema for `type: "array"`. */
  items?: JsonSchema;
  /** Whether unlisted keys are accepted. */
  additionalProperties?: boolean | JsonSchema;
  /** Keywords this UI does not model (`$schema`, `oneOf`, ...). */
  [keyword: string]: unknown;
}

/** `PluginManifest` — static description of a plugin, from `GET /plugins`. */
export interface PluginManifest {
  /** Unique snake_case name referenced by `steps[].plugin`. */
  name: string;
  /** Semver string. */
  version: string;
  /** Human description. */
  description?: string;
  /** Input variants accepted. */
  accepts?: InputKind[];
  /** Output variant produced. */
  produces: OutputKind;
  /** JSON Schema of the step `config:` block. */
  config_schema?: JsonSchema;
  /** Execution kind. */
  kind?: PluginKind;
  /** MIME types the plugin is designed for (informational). */
  content_types?: string[];
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/** `ActionCategory` — where an action sits in the shape of a pipeline. */
export type ActionCategory = "fetch" | "extract" | "transform" | "enrich" | "index";

/** The five categories in pipeline order, which is the order the grid renders. */
export const ACTION_CATEGORIES: ActionCategory[] = [
  "fetch",
  "extract",
  "transform",
  "enrich",
  "index",
];

/** `WorkflowCategory` — what a workflow is for. */
export type WorkflowCategory = "documents" | "data" | "media" | "web";

/** The four workflow categories, in grid order. */
export const WORKFLOW_CATEGORIES: WorkflowCategory[] = ["documents", "data", "media", "web"];

/**
 * `ActionEntry` — authored copy for one plugin.
 *
 * Deliberately separate from `PluginManifest`: that type is implemented by
 * third-party WASM and gRPC plugin authors and carries no product copy.
 */
export interface ActionEntry {
  /** Joins to `PluginManifest.name`. */
  plugin: string;
  title: string;
  category: ActionCategory;
  summary: string;
  use_cases: string[];
  /** A copyable YAML step block. */
  example_step: string;
  /** Fallback for when no worker has registered a manifest. */
  accepts: InputKind[];
  /** Fallback for when no worker has registered a manifest. */
  produces: OutputKind;
}

/** `WorkflowEntry` — authored copy for one workflow. */
export interface WorkflowEntry {
  /** `builtin.pdf`, or a curated template uid. */
  uid: string;
  title: string;
  category: WorkflowCategory;
  summary: string;
  when_to_use: string;
  /**
   * Present only for curated templates that are not deployed as pipelines.
   * Absent for `builtin.*`, whose definition comes from `GET /pipelines`.
   */
  definition?: PipelineDefinition;
}

/** `Catalog` — the body of `GET /catalog`. */
export interface Catalog {
  actions: ActionEntry[];
  workflows: WorkflowEntry[];
}

// ---------------------------------------------------------------------------
// Jobs
// ---------------------------------------------------------------------------

/** `UsageUnits` — billable work a step performed. Every field is additive. */
export interface UsageUnits {
  llm_input_tokens: number;
  llm_output_tokens: number;
  llm_requests: number;
  audio_seconds: number;
  pages: number;
  images: number;
  external_requests: number;
}

/** `StepResult` — per-step outcome recorded in the workflow progress. */
export interface StepResult {
  step_id: string;
  plugin: string;
  status: JobStatus;
  /** Documents produced (0 for binary outputs). */
  document_count?: number;
  /** Parallel branches (1 unless fanned out). */
  branches?: number;
  /** Error message when `status` is `failed`. */
  error?: string;
  duration_ms?: number;
  input_bytes?: number;
  usage?: UsageUnits;
}

/** `WorkflowProgress` — snapshot returned by the workflow's `progress` query. */
export interface WorkflowProgress {
  status: JobStatus;
  current_step?: string;
  completed_steps: number;
  total_steps: number;
  steps?: StepResult[];
  error?: string;
  cancel_requested?: boolean;
}

/** `IngestResponse` — 202 body of `POST /ingest`. */
export interface IngestResponse {
  job_id: string;
  /** `uid` of the pipeline that will run. */
  pipeline_used: string;
  /** Fully resolved Meilisearch index name. */
  target_index: string;
  status: JobStatus;
}

// The body of `GET /jobs/{job_id}` is typed as `JobDetail` in `./jobs`, next to the
// hooks that consume it. It is deliberately not duplicated here: `progress` is null
// whenever the gateway answers from its cached row rather than from Temporal, and a
// second definition of the same endpoint drifted from that within a day.

// ---------------------------------------------------------------------------
// Errors and validation
// ---------------------------------------------------------------------------

/**
 * Every gateway error body. The thrown counterpart is the `ApiError` class in
 * `./client`, which carries the HTTP `status` alongside these two fields.
 */
export interface ApiErrorBody {
  /** Human-readable message. */
  error: string;
  /** Stable snake_case code (`not_found`, `forbidden`, `invalid_pipeline`, ...). */
  code: string;
}

/** 200 body of `POST /pipelines/validate`. */
export interface ValidatePipelineOk {
  valid: true;
  /** Topological execution order of the steps. */
  order: string[];
}

/**
 * Outcome of the authoritative server-side validation.
 *
 * `unsupported` means the gateway does not expose `POST /pipelines/validate`
 * yet — the UI degrades to client-side validation only.
 */
export type ValidationOutcome =
  | { kind: "ok"; order: string[] }
  | { kind: "invalid"; error: string; code: string }
  | { kind: "unsupported" };
