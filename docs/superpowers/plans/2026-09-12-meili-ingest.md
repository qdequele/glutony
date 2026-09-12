# meili-ingest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build `meili-ingest`, a 100% Rust, Kubernetes-native, multi-tenant ingestion pipeline for Meilisearch (HTTP gateway → Temporal workflow → plugin activities → Meilisearch), as described in `SPEC.md`.

**Architecture:** An axum gateway resolves a `MeiliContext` from Envoy headers/env, detects MIME, asks the control plane (axum + Postgres) which pipeline to run, stages large payloads in a blob store, and starts a Temporal `PipelineWorkflow`. Worker pools poll typed task queues and run one `execute_step` activity per pipeline step through a `PluginRegistry`. Only the `meili_indexer` plugin talks to Meilisearch, using the `MeiliContext` merged into its config by the workflow.

**Tech Stack:** Rust 2024 edition, axum 0.8, tokio, temporalio-sdk 1.0 (+ temporalio-client/common/macros/workflow), sqlx 0.9 (Postgres), object_store 0.14, meilisearch-sdk 0.33, reqwest 0.13, infer, pdf-extract, docx-rs, calamine, scraper, pulldown-cmark, csv, extism, tonic 0.14.

**Spec:** `SPEC.md` (root). Read it entirely first. This plan resolves the spec's ambiguities; where they differ, this plan wins.

## Global Constraints

- Workspace root `Cargo.toml` already pins every dependency under `[workspace.dependencies]`. Crates use `x.workspace = true`. Do not add versions inline unless the crate is missing from the workspace list (then add it there).
- Edition 2024, `rust-version = 1.92`. `unsafe` forbidden.
- `thiserror` in library crates, `anyhow` in binaries (`main.rs`).
- **No `unwrap()`/`expect()` in non-test code.** Use `?` or explicit handling.
- All public types derive `Debug, Clone, Serialize, Deserialize` (and `PartialEq` when cheap).
- Env vars are read only in `from_env()` constructors or `main.rs`.
- Structured tracing: `tracing::info!(job_id = %id, plugin = %name, "...")`. Never log `api_key` (use `MeiliContext::redacted()`).
- Plugins call `ctx.heartbeat(..)` every ~10 items and `ctx.check_cancelled()?` in loops.
- Unit tests in `#[cfg(test)]`. Every crate must pass `cargo test -p <crate>` and `cargo clippy -p <crate> -- -D warnings`.
- Commit messages: conventional commits (`feat:`, `fix:`, `chore:`). **No `Co-Authored-By` lines.**
- The shared contract is `crates/plugin-sdk` (already implemented, do not change its public API without updating this plan). Read `crates/plugin-sdk/src/types.rs` before anything else.

## Decisions that resolve spec ambiguities

1. **Temporal payload limit (2 MB)**: uploads and step outputs can be large. New crate `crates/blob` wraps `object_store`. The gateway inlines bytes ≤ `INLINE_MAX_BYTES` (default 1 MiB) as `PluginInput::Bytes`, else stages them and passes `PluginInput::Ref(ContentRef::Staged{..})`. The worker activity resolves every `PluginInput::Ref` (Url / S3 / Staged) into `Bytes` before calling a plugin, and spills outputs whose `approx_size() > PAYLOAD_SPILL_BYTES` (default 1 MiB) back to the store as `PluginOutput::Ref(ContentRef::Staged{ mime: "application/vnd.meili-ingest.output+json" })`, which the next step hydrates. Plugins never see refs.
2. **Workflow type name** is `PipelineWorkflow`; workflow id is `ingest-<job_id>`; the workflow itself always runs on task queue `workers-general`; activities are routed with `ActivityOptions.task_queue` per SPEC §8.3.
3. **Gateway ↔ worker coupling**: the gateway does *not* link the worker crate. It starts the workflow untyped: `client.start_workflow(UntypedWorkflow::new("PipelineWorkflow"), RawValue::from_value(&input, &PayloadConverter::default()), opts)`, and reads progress with `UntypedQuery::new("progress")` returning `RawValue` → `to_value::<WorkflowProgress>()`. Cancel = `UntypedSignal::new("cancel")` + `handle.cancel()`.
4. **Fan-out**: only `$.documents` (one branch per document), `$.many` (one branch per element of a `Many` output) and `$` (no split) are supported. Validated at pipeline creation by `PipelineDefinition::validate()`.
5. **Job persistence**: Temporal is the source of truth. The gateway writes a row to the control plane's `jobs` table when it starts a job and updates it whenever `GET /jobs/:id` is served (write-through cache). `GET /jobs/:id` = Temporal `describe` + `progress` query, falling back to the cached row when Temporal says not found.
6. **Envoy trust**: if env `ENVOY_TRUSTED_HEADER` is set, `X-Meili-*` headers are honoured only when the request carries `X-Meili-Envoy-Secret: <that value>`; otherwise they are ignored and the request is treated as standalone. If unset, headers are trusted (dev mode) — logged once at startup as a warning.
7. **Plugin manifests in the control plane**: workers `POST /internal/plugins` their manifests at boot (best effort, upsert by name into table `plugins`); `GET /plugins` on the gateway proxies the control plane. The control plane also carries a static list of built-in plugin names for validation of user pipelines (unknown plugin → 422).
8. **Tenant-scoped pipelines**: resolver filters `project_id = ctx.project_id OR project_id IS NULL`; tenant-scoped user pipelines beat global user pipelines, which beat built-ins.
9. **YAML**: `POST /pipelines` accepts `application/x-yaml`/`text/yaml` (parsed with `serde_yaml` in the gateway) or JSON; the control plane only speaks JSON.
10. **`pptx_extractor`, `whisper_transcriber`, `video_audio_extractor`** are *not* implemented in this repo. The corresponding built-in pipelines exist, and the worker fails such steps with a clear `NonRetryable("plugin X is not registered on this worker; deploy a gRPC plugin")`.

## Cross-crate interfaces

### Plugin crates (`crates/plugins/<x>`, package `meili-ingest-plugin-<x>`)
Every plugin crate exports exactly:
```rust
pub const NAME: &str = "<plugin_name>";
pub struct <Camel>Plugin { .. }          // e.g. PdfExtractorPlugin
impl <Camel>Plugin { pub fn new() -> Self }  // + impl Default
impl Plugin for <Camel>Plugin { .. }
```
Env-backed plugins (`llm_enricher`, `image_captioner`) additionally: `pub fn from_env() -> Result<Self, PluginError>` (reads `LLM_API_KEY`, `LLM_BASE_URL` default `https://api.openai.com/v1`, `LLM_MODEL` default `gpt-4o-mini`) and `pub fn with_client(base_url, api_key, model, http: reqwest::Client) -> Self`.

| crate | NAME | type | accepts → produces |
|---|---|---|---|
| pdf | `pdf_extractor` | `PdfExtractorPlugin` | Bytes → Documents (one per page when `per_page: true` (default), else one) |
| docx | `docx_extractor` | `DocxExtractorPlugin` | Bytes → Documents (one document; paragraphs joined by `\n\n`; tables as tab-separated rows) |
| xlsx | `xlsx_extractor` | `XlsxExtractorPlugin` | Bytes → Documents (config `mode: "row"` (default) → one doc per row with header→cell `fields`, `meta.section = sheet`; `mode: "sheet"` → one doc per sheet) |
| html | `html_extractor` | `HtmlExtractorPlugin` | Bytes → Documents (one; title from `<title>`, text from `<body>` minus script/style/nav/footer; `fields.links` optional via `extract_links`) |
| markdown | `markdown_extractor` | `MarkdownExtractorPlugin` | Bytes → Documents (config `split_on_headings: true` (default) → one doc per top-level heading section, `meta.section` = heading; else one) |
| csv | `csv_parser` | `CsvParserPlugin` | Bytes → Documents (one per row; header → `fields`; `content` = values joined by space; config `delimiter`, `has_headers` (default true), `id_column`) |
| json | `json_flattener` | `JsonFlattenerPlugin` | Bytes **or** Documents → Documents (top-level array → one doc per element; object with `documents`/`items`/`data` array → those; else one doc). Nested objects are flattened to `a.b.c` keys in `fields`; `content` = concatenated string values; id from `id_field` (default `id`) or generated |
| chunker | `chunker` | `ChunkerPlugin` | Documents → Documents. Config `strategy: sentence|fixed|paragraph` (default sentence), `chunk_size: 512`, `overlap: 64` (chars), `min_chunk_size: 32`. Sets `meta.chunk_index`, `meta.chunk_total`, `meta.parent_id`; chunk id = `<parent_id>_<index>` |
| meili-indexer | `meili_indexer` | `MeiliIndexerPlugin` | Documents/Many → Indexed. Deserializes `IndexerConfig` from config (see plugin-sdk); uses `meilisearch-sdk`; creates index with `primary_key` when missing and `auto_create_index`; batches; waits for tasks when `wait_for_completion`; returns `IndexReport`. Never reads env. |
| llm-enricher | `llm_enricher` | `LlmEnricherPlugin` | Documents/Many → Documents. OpenAI-compatible `/chat/completions` with `response_format: {type: json_object}`; config `model`, `prompt`, `max_concurrent` (default 8), `temperature`, `fields_to_merge` (default: all keys of the JSON reply merged into `fields`; `title`/`summary`/`keywords`/`language` also mapped onto `title`/`fields.summary`/`fields.keywords`/`meta.language`). Uses `futures::stream::buffer_unordered`. Test with `wiremock`. |
| image-captioner | `image_captioner` | `ImageCaptionerPlugin` | Bytes → Documents (one doc; base64 data-URL to the vision chat endpoint; `content` = caption; config `model` default `gpt-4o-mini`, `prompt`, `detail`). Test with `wiremock`. |

Documents produced by extractors set `meta.source`/`meta.filename`/`meta.mime` from the input `Blob` and use ids derived from the filename (`sanitize_id`) + page/row suffix.

### `crates/blob` (package `meili-ingest-blob`)
```rust
pub struct BlobStore { .. }               // Clone, Debug
#[derive(thiserror::Error)] pub enum BlobError { NotFound(String), Store(#[from] object_store::Error), Http(String), Url(String), Serde(#[from] serde_json::Error), Unsupported(String) }
impl BlobStore {
    pub fn from_env() -> anyhow::Result<Self>;              // BLOB_STORE_URL, default "file://./blobs" (created if missing)
    pub fn from_url(url: &str) -> anyhow::Result<Self>;     // file://, s3://, gs://, az://, memory://
    pub fn memory() -> Self;                                // for tests
    pub fn staged_key(job_id: uuid::Uuid, name: &str) -> String;  // "jobs/<job_id>/<name>"
    pub async fn put(&self, key: &str, bytes: bytes::Bytes) -> Result<(), BlobError>;
    pub async fn get(&self, key: &str) -> Result<bytes::Bytes, BlobError>;
    pub async fn delete(&self, key: &str) -> Result<(), BlobError>;
    /// Url → reqwest GET (mime from Content-Type unless hint), S3 → object_store::parse_url, Staged → self.get
    pub async fn fetch_ref(&self, r: &ContentRef, http: &reqwest::Client) -> Result<Blob, BlobError>;
    /// Ref → Bytes, recursing through Many; Bytes/Documents unchanged. Also hydrates spilled outputs (see below).
    pub async fn resolve_input(&self, input: PluginInput, http: &reqwest::Client) -> Result<PluginInput, BlobError>;
    /// If output.approx_size() > threshold: serialize to JSON, put at staged_key(job_id, "<step_id>[-<branch>].json"), return PluginOutput::Ref(Staged{ mime: SPILLED_OUTPUT_MIME }).
    pub async fn spill_output(&self, job_id: uuid::Uuid, step_id: &str, branch: Option<usize>, output: PluginOutput, threshold: usize) -> Result<PluginOutput, BlobError>;
    /// Ref(Staged{mime == SPILLED_OUTPUT_MIME}) → the original PluginOutput; other variants unchanged (recursing Many).
    pub async fn hydrate_output(&self, output: PluginOutput) -> Result<PluginOutput, BlobError>;
    /// Gateway helper: inline when ≤ inline_max, else stage. Returns the PluginInput to put into the workflow.
    pub async fn stage_upload(&self, job_id: uuid::Uuid, blob: Blob, inline_max: usize) -> Result<PluginInput, BlobError>;
}
pub const SPILLED_OUTPUT_MIME: &str = "application/vnd.meili-ingest.output+json";
```

### `crates/router` (package `meili-ingest-router`)
```rust
pub struct PipelineRouter { pipelines: Vec<PipelineDefinition> }
pub struct RouteRequest<'a> { pub mime: &'a str, pub filename: Option<&'a str>, pub project_id: Option<&'a str> }
pub struct RouteMatch<'a> { pub pipeline: &'a PipelineDefinition, pub index_pattern: Option<&'a str> }
impl PipelineRouter {
    pub fn new(pipelines: Vec<PipelineDefinition>) -> Self;
    /// Priority: tenant-scoped user > global user > builtin. Within a tier: trigger with matching filename_pattern > mime-only. Ties: first in list.
    pub fn resolve(&self, req: RouteRequest<'_>) -> Option<RouteMatch<'_>>;
    pub fn by_uid(&self, uid: &str, project_id: Option<&str>) -> Option<&PipelineDefinition>;  // tenant pipeline shadows global with same uid
    pub fn all(&self, project_id: Option<&str>) -> Vec<&PipelineDefinition>;
}
pub fn mime_to_default_index(mime: &str) -> &'static str;   // SPEC §11
pub fn detect_mime(data: &[u8], filename: Option<&str>, content_type_hint: Option<&str>) -> String; // SPEC §10 chain; hint only used when everything else says octet-stream (and hint is not octet-stream/multipart)
pub fn plugin_task_queue(plugin: &str) -> &'static str;   // SPEC §8.3
```
The full built-in pipeline table (SPEC §9) lives in the router crate as `pub fn builtin_pipelines() -> Vec<PipelineDefinition>` so both the control plane and tests share it (`crates/control-plane/src/builtin_pipelines.rs` re-exports it).

### Control plane HTTP API (internal, `BIND` default `0.0.0.0:9000`, JSON only)
```
GET    /health                              → {"status":"ok"}
GET    /pipelines?project_id=<id>           → PipelineDefinition[]   (builtins + global user + tenant user)
POST   /pipelines                           body PipelineDefinition (normalize+validate; unknown plugin → 422; uid starting with "builtin." → 403) → 201 PipelineDefinition (version bumped on update, upsert by (uid, project_id))
GET    /pipelines/{uid}?project_id=         → PipelineDefinition | 404
DELETE /pipelines/{uid}?project_id=         → 204 | 403 builtin | 404
GET    /plugins                             → PluginManifest[]
POST   /internal/plugins                    body PluginManifest[] → 204 (upsert)
POST   /internal/resolve                    body {mime, filename?, project_id?, pipeline?} → 200 {pipeline: PipelineDefinition, index_pattern?: string} | 404 {"error": "no pipeline matches ..."}
POST   /internal/jobs                       body JobRecord → 201
PATCH  /internal/jobs/{job_id}              body JobUpdate {status?, current_step?, error?, index_name?} → 200 JobRecord | 404
GET    /internal/jobs/{job_id}              → JobRecord | 404
```
`JobRecord { job_id: Uuid, workflow_id: String, pipeline_uid: String, project_id: Option<String>, index_name: Option<String>, status: JobStatus, current_step: Option<String>, error: Option<String>, started_at: DateTime<Utc>, updated_at: DateTime<Utc> }`. Errors: `{"error": "<message>", "code": "<snake_case>"}`.
Migrations in `/migrations/*.sql` (sqlx migrate, embedded with `sqlx::migrate!("../../migrations")`) — the two tables from SPEC §12 plus `plugins(name TEXT PK, manifest JSONB, updated_at)`. `pipelines` PK becomes `(uid, COALESCE(project_id,''))` via a unique index.

### Gateway HTTP API (`BIND` default `0.0.0.0:8080`) — SPEC §4 plus `GET /health`
Response of ingest: `{ "job_id", "pipeline_used", "target_index", "status": "queued" }`. Batch: `{ "jobs": [ ...same... ] }`. Errors: `{"error": "...", "code": "..."}`, 400 for missing context, 404 unknown pipeline, 413 too large, 415 no pipeline matches MIME, 422 invalid pipeline, 502 control plane / Temporal unreachable.
`GET /jobs/{id}` → `{ job_id, status, current_step, progress: WorkflowProgress, pipeline_used, target_index, error? }`.

### Worker (`crates/worker`) — see `docs/superpowers/plans/temporal-api-probe.rs` for the compile-verified Temporal patterns.
- `workflow.rs`: `#[workflow] pub struct PipelineWorkflow { progress: WorkflowProgress }`, `#[run] async fn run(ctx: &mut WorkflowContext<Self>, input: PipelineWorkflowInput) -> WorkflowResult<PipelineWorkflowOutput>`, `#[signal] fn cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: ())`, `#[query] fn progress(&self, _ctx: &WorkflowContextView) -> WorkflowProgress`, `#[query] fn current_step(&self, _ctx: &WorkflowContextView) -> String`. Steps run in `validate()` order; steps whose deps are all done and that are mutually independent may be joined with `join_all`. Input resolution: no deps → workflow input; one dep → that output; several deps → `PluginInput::Many`. Fan-out: `$.documents` → one `execute_step` per document (`PluginInput::Documents(vec![doc])`), `$.many` → one per element; results collected into `PluginOutput::Many`. Inject `MeiliContext` into config when `plugin == INDEXER_PLUGIN` via `inject_meili_context`. Activity options: `task_queue(plugin_task_queue(plugin))`, `start_to_close = effective_timeout_secs`, `heartbeat_timeout = 60s`, `retry_policy` from `effective_retry()` (Linear → backoff 1.0, None → max_attempts 1 or coefficient 1 with zero interval). Failures → `ApplicationFailure::non_retryable(...)` into `WorkflowTermination`, after recording status in progress. Cancel signal → stop scheduling new steps, finish with `JobStatus::Cancelled`. **Never** use tokio primitives in workflow code.
- `activity.rs`: `pub struct StepActivities { registry: Arc<PluginRegistry>, blob: BlobStore, http: reqwest::Client, spill_threshold: usize }`, `#[activities] impl StepActivities { #[activity] pub async fn execute_step(self: Arc<Self>, ctx: ActivityContext, input: StepActivityInput) -> Result<StepActivityOutput, ActivityError> }` — resolves input via blob, builds the SDK `ActivityContext` (heartbeat channel forwarder task + cancellation flag set from `ctx.cancelled()`), dispatches to the plugin, maps `PluginError` (`is_retryable()` false → `ApplicationFailure::non_retryable`), spills output.
- `registry.rs`: `pub struct PluginRegistry`, `pub fn builtin() -> Self` (registers all 11 plugins; env-backed ones via `from_env()` and if that fails they are registered as *unavailable* so the error message is clear), `pub fn get(&self, name) -> Option<Arc<dyn Plugin>>`, `pub fn manifests(&self) -> Vec<PluginManifest>`, `pub fn register(&mut self, Arc<dyn Plugin>)`.
- `main.rs`: env `TEMPORAL_URL`, `TEMPORAL_NAMESPACE`, `TASK_QUEUE`, `CONTROL_PLANE_URL` (to post manifests), `BLOB_STORE_URL`, `PAYLOAD_SPILL_BYTES`; registers `PipelineWorkflow` + `StepActivities`; JSON tracing.

## Tasks

- [ ] **Task 1 – blob + router crates** (`crates/blob`, `crates/router`) with unit tests (memory store; MIME detection for pdf/png/zip-docx/utf8/octet; routing precedence; builtin table has 12 pipelines and every `builtin.*` validates).
- [ ] **Task 2 – extractor plugins** pdf, docx, xlsx, html, markdown (tests use small fixtures generated in-test where possible: build a DOCX/XLSX with the writer side of `docx-rs`/`rust_xlsxwriter`? no — prefer tiny checked-in fixtures under `crates/plugins/<x>/tests/fixtures/`, created by a script that is committed too).
- [ ] **Task 3 – data plugins** csv, json, chunker, meili-indexer (indexer tested against `wiremock` for the Meilisearch HTTP API: index create, documents add, task polling).
- [ ] **Task 4 – LLM plugins + plugin-runtime** llm-enricher, image-captioner (`wiremock`), `crates/plugin-runtime` (`WasmPlugin` via extism implementing `Plugin` by calling an exported `execute` function with JSON `{input, config}`; `GrpcPlugin` via tonic client for `proto/plugin.proto` service `Plugin { rpc Manifest(Empty) returns (ManifestResponse); rpc Execute(stream ExecuteRequest) returns (ExecuteResponse); }` — keep unary `Execute(ExecuteRequest)` for simplicity), `plugin-sdk` `wasm` feature docs.
- [ ] **Task 5 – gateway** (`crates/gateway`), all routes, `context.rs` exhaustive tests (SPEC §3.2 order, Envoy secret, query override, 400 when nothing), `extract.rs` tests (multipart/json url/s3/documents/items/raw), handler tests with a mocked control plane (`wiremock`) and a `TemporalStarter` trait so tests don't need a server.
- [ ] **Task 6 – control plane** (`crates/control-plane`), migrations, CRUD, resolver, jobs, plugins; sqlx tests gated behind `DATABASE_URL` (`#[ignore]` otherwise); pure logic (validation, precedence) unit-tested without DB.
- [ ] **Task 7 – worker** (after 2–4), workflow/activity/registry/main; unit-test DAG scheduling helpers (`ready_steps`, `resolve_input`, `fan_out_branches`) and `PluginError → ActivityError` mapping without Temporal.
- [ ] **Task 8 – infra & docs**: `Dockerfile` (multi-stage, one image, `--bin` selectable), `Dockerfile.dev` (cargo-watch), `compose.yaml` with `develop.watch` (postgres, temporal dev server, meilisearch, control-plane, gateway, worker-general, worker-llm), `k8s/*.yaml` (gateway, control-plane, workers + KEDA ScaledObjects on Temporal queue metrics via prometheus trigger, temporal reference), `config/pipelines/*.yaml` (video-ingest, pdf-with-enrichment), `docs/` Mintlify site (`mint.json`, quickstart, concepts, multi-tenancy, pipelines, plugins/authoring, deployment, `openapi.yaml` for the gateway), `README.md`, `.github/workflows/ci.yml` (fmt, clippy, test).
- [ ] **Task 9 – integration**: `cargo build --workspace`, `cargo test --workspace`, `cargo clippy --workspace -- -D warnings`, then an end-to-end run with `temporal server start-dev`, Meilisearch and Postgres in Docker: ingest a PDF via multipart and confirm documents land in the index.
