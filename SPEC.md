# meili-ingest — Implementation Spec

> **This document is the single source of truth for Claude Code.**
> Read it entirely before writing a single line. Every architectural decision
> is explained here with its rationale. When something is ambiguous, prefer
> the approach described here over general intuition.

---

## 1. What this project is

`meili-ingest` is an open-source, Kubernetes-native ingestion pipeline system
for Meilisearch. It exposes an HTTP gateway that accepts arbitrary content
(PDF, Word, Excel, video, images, HTML, JSON, CSV, …), detects what it is,
runs it through a configurable processing pipeline (extraction, chunking, OCR,
transcription, LLM enrichment, …), and pushes the result to a Meilisearch index.

**Hard constraints:**
- 100% Rust
- Open-source (MIT)
- Kubernetes-native (Deployments, KEDA scaling, Jobs for GPU workloads)
- Plugin system so the community can extend it without forking

---

## 2. High-level architecture

```
                        ┌──────────────────────────────────────────────┐
                        │              Meilisearch Cloud               │
                        │                                              │
  User request          │   Envoy (glutony)                            │
  xxx.us-west           │   - extracts tenant context from request     │
  .meilisearch.com  ───►│   - injects X-Meili-* headers               │
  /ingest               │   - forwards to meili-ingest gateway         │
                        │              │                               │
                        └──────────────┼───────────────────────────────┘
                                       │  (enriched request)
                        ┌──────────────▼───────────────────────────────┐
                        │         meili-ingest GATEWAY (axum)          │
                        │                                              │
                        │  POST /ingest                → auto-route    │
                        │  POST /ingest/pipeline/:name → explicit      │
                        │  GET  /jobs/:id              → status        │
                        │  CRUD /pipelines             → mgmt          │
                        └──────────────┬───────────────────────────────┘
                                       │  starts Temporal workflow
                        ┌──────────────▼───────────────────────────────┐
                        │              Temporal Server                 │
                        └──────────────┬───────────────────────────────┘
                                       │  dispatches activities
                     ┌─────────────────┼──────────────────┐
                     ▼                 ▼                  ▼
              workers-general    workers-llm        workers-gpu
              (pdf,docx,xlsx,    (llm_enricher)     (whisper,ocr)
               chunker,indexer)
                     │                 │                  │
                     └─────────────────┼──────────────────┘
                                       │
                        ┌──────────────▼───────────────────────────────┐
                        │            Meilisearch instance              │
                        │         (tenant-specific, resolved           │
                        │          from X-Meili-Host header)           │
                        └──────────────────────────────────────────────┘
```

---

## 3. The Envoy / glutony integration (multi-tenancy)

This is the critical design decision for Meilisearch Cloud deployment.

### Problem

meili-ingest needs to serve **multiple Meilisearch projects** (tenants) from a
single shared deployment. Each project has:
- Its own Meilisearch host (e.g. `xxx.us-west.meilisearch.io`)
- Its own API key
- Potentially its own set of custom pipelines

We do not want every tenant request to carry full Meilisearch credentials in
the body — that's a security and DX problem.

### Solution: Envoy header injection (project glutony)

Meilisearch Cloud's Envoy gateway (codename: glutony) sits in front of all
traffic. When a request arrives at `xxx.us-west.meilisearch.com/ingest`, Envoy:

1. Extracts the **project ID** from the subdomain (`xxx`)
2. Looks up the project's Meilisearch host and API key from its internal store
3. Optionally extracts an **index name** from the URL path if present
4. Injects the following headers before forwarding to meili-ingest:

```
X-Meili-Project-Id:   xxx                          # tenant identifier
X-Meili-Host:         https://xxx.us-west.meilisearch.io  # full Meilisearch URL
X-Meili-Api-Key:      <master_or_ingest_key>       # Meilisearch API key
X-Meili-Index:        <index_name>                 # optional, from path or query
X-Meili-Region:       us-west                      # for routing / observability
```

meili-ingest **trusts these headers** when the request comes from Envoy
(identified by a shared secret or mTLS — TBD with the glutony team).
When running standalone (self-hosted), these headers are absent and the
caller must provide credentials via standard means (see §3.3).

### 3.1 MeiliContext — the tenant context struct

Every operation in meili-ingest is scoped to a `MeiliContext`. This struct is
resolved once at the gateway layer and then **carried through the entire
pipeline as part of the Temporal workflow input**. Workers never read
environment variables for Meilisearch credentials — they always use the
`MeiliContext` from the workflow input.

```rust
/// Resolved once at the gateway, carried immutably through the whole pipeline.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MeiliContext {
    /// Tenant / project identifier (from X-Meili-Project-Id or config)
    pub project_id:  Option<String>,
    /// Full Meilisearch host URL (e.g. https://xxx.us-west.meilisearch.io)
    pub host:        String,
    /// Meilisearch API key with write access
    pub api_key:     String,
    /// Target index — may be overridden later by routing logic
    pub index:       Option<String>,
    /// Region tag for observability
    pub region:      Option<String>,
}
```

### 3.2 Context resolution order (gateway)

```
1. X-Meili-Host header (Envoy-injected)           → host
2. X-Meili-Api-Key header (Envoy-injected)        → api_key
3. X-Meili-Project-Id header (Envoy-injected)     → project_id
4. X-Meili-Index header (Envoy-injected)          → index (starting point)
5. ?index= query param                            → index (overrides header)
6. Authorization: Bearer <key> header             → api_key (self-hosted fallback)
7. MEILI_URL / MEILI_API_KEY env vars             → host / api_key (self-hosted fallback)
```

If neither Envoy headers nor env vars are present → 400 Bad Request.

### 3.3 Self-hosted / standalone mode

When running without Envoy (self-hosted on-prem or local dev):

```bash
# Option A: env vars (simplest)
MEILI_URL=http://localhost:7700 MEILI_API_KEY=masterKey ./meili-gateway

# Option B: per-request Authorization header
curl -H "Authorization: Bearer masterKey" \
     -H "X-Meili-Host: http://localhost:7700" \
     -F file=@doc.pdf \
     http://localhost:8080/ingest

# Option C: config file
meili_host: http://localhost:7700
meili_api_key: masterKey
```

### 3.4 Index name resolution (full chain)

```
X-Meili-Index header
  → ?index= query param overrides it
  → Pipeline trigger's index_pattern (if set) overrides it
  → MIME-based default (video/* → "videos", image/* → "images", etc.)
  → Global default_index config value ("documents")
```

The final resolved index name is written into `MeiliContext.index` before
the workflow starts. The `meili_indexer` plugin reads it from there — it never
has to figure out the index itself.

---

## 4. Two-tier ingestion API

### Tier 1: `POST /ingest` — auto-routing, batteries-included

Zero configuration. Drop any content in, the system does the right thing.

**Request formats:**

```bash
# Multipart file upload (most common)
curl -F "file=@report.pdf" http://.../ingest
curl -F "file=@report.pdf" -F "index=contracts" http://.../ingest

# JSON with URL reference (gateway fetches it inline for small files)
curl -d '{"url":"https://example.com/doc.pdf"}' \
     -H "Content-Type: application/json" http://.../ingest

# JSON with S3 reference (worker streams it — for large files)
curl -d '{"s3":"s3://my-bucket/large-video.mp4"}' \
     -H "Content-Type: application/json" http://.../ingest

# Inline JSON documents (direct indexing, no extraction needed)
curl -d '{"documents":[{"id":"1","title":"Hello"}]}' \
     -H "Content-Type: application/json" http://.../ingest

# Raw body (Content-Type + magic bytes determine handling)
curl --data-binary @document.pdf \
     -H "Content-Type: application/pdf" http://.../ingest

# Batch (starts one job per item)
curl -d '{"items":[{"url":"..."},{"s3":"..."}]}' \
     -H "Content-Type: application/json" http://.../ingest/batch
```

**Response:**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "pipeline_used": "builtin.pdf",
  "target_index": "documents",
  "status": "queued"
}
```

### Tier 2: `POST /ingest/pipeline/:name` — explicit pipeline

Full control, for complex orchestration.

```bash
# Explicit pipeline — full video processing with LLM enrichment
curl -F "file=@keynote.mp4" http://.../ingest/pipeline/video-ingest-enriched

# With index override
curl -F "file=@keynote.mp4" \
     http://.../ingest/pipeline/video-ingest-enriched?index=keynotes
```

**Response:** same as Tier 1, `pipeline_used` = `:name`.

### Other endpoints

```
GET  /jobs/:job_id              Job status + current step + progress
POST /jobs/:job_id/cancel       Cancel a running job (Temporal signal)

GET  /pipelines                 List all pipelines (built-in + user-defined)
POST /pipelines                 Create or update a pipeline (YAML or JSON body)
GET  /pipelines/:name           Get a pipeline definition
DELETE /pipelines/:name         Delete a user pipeline (built-ins cannot be deleted)

GET  /plugins                   List registered plugins with their manifests
```

---

## 5. Pipeline definition format

Pipelines are YAML or JSON. They are stored in Postgres by the control plane
and cached in memory.

```yaml
uid: my-pdf-with-enrichment
name: "PDF with LLM enrichment"
version: 1

# Optional: auto-select this pipeline when MIME/filename matches
trigger:
  content_types: [application/pdf]
  filename_pattern: "contract_*.pdf"   # glob, optional

steps:
  # Sequential by default (no depends_on = runs after previous step)
  - id: extract
    plugin: pdf_extractor
    config:
      per_page: true
      extract_images: false
    timeout_secs: 120
    retry:
      max_attempts: 3
      backoff: exponential

  - id: chunk
    plugin: chunker
    depends_on: [extract]
    config:
      strategy: sentence
      chunk_size: 512
      overlap: 64

  # Fan-out: one LLM call per chunk, in parallel
  - id: enrich
    plugin: llm_enricher
    depends_on: [chunk]
    fan_out: "$.documents"    # JSONPath into previous step output
    config:
      model: gpt-4o-mini
      max_concurrent: 20
      prompt: |
        Extract as JSON: title, summary, keywords, language.

  # Fan-in is automatic when depends_on references a fan_out step
  - id: index
    plugin: meili_indexer
    depends_on: [enrich]
    # No index config here — resolved from MeiliContext at runtime
```

### DAG execution rules

- Steps with no `depends_on` run immediately (in parallel if multiple)
- Steps with `depends_on: [a, b]` wait for ALL listed steps to complete
- `fan_out` on a step splits its input into N parallel activities; the next
  step that `depends_on` it receives all N outputs merged as `PluginInput::Many`
- Cycles are rejected at pipeline creation time (Kahn's algorithm)

---

## 6. Cargo workspace layout

```
meili-ingest/
├── Cargo.toml                          # workspace
│
├── crates/
│   ├── gateway/                        # axum HTTP server
│   │   src/
│   │     main.rs                       # router + server boot
│   │     state.rs                      # AppState (Temporal client, control plane client)
│   │     context.rs                    # MeiliContext resolution from headers/env
│   │     extract.rs                    # IngestPayload parser (multipart/json/raw)
│   │     error.rs                      # GatewayError → HTTP response
│   │     handlers/
│   │       ingest.rs                   # POST /ingest + /ingest/batch
│   │       pipeline.rs                 # POST /ingest/pipeline/:name
│   │       jobs.rs                     # GET /jobs/:id, POST /jobs/:id/cancel
│   │       pipelines.rs               # CRUD /pipelines
│   │       plugins.rs                  # GET /plugins
│   │
│   ├── control-plane/                  # pipeline registry + routing API
│   │   src/
│   │     main.rs                       # axum HTTP server (internal port)
│   │     db.rs                         # sqlx Postgres pool + migrations
│   │     pipelines.rs                  # CRUD for user-defined pipelines
│   │     builtin_pipelines.rs          # built-in pipeline definitions (Rust constants)
│   │     resolver.rs                   # POST /internal/resolve endpoint
│   │     plugins.rs                    # plugin manifest registry
│   │
│   ├── worker/                         # Temporal workflow + activity runner
│   │   src/
│   │     main.rs                       # Worker binary boot, plugin registration
│   │     workflow.rs                   # PipelineWorkflow (Temporal #[workflow])
│   │     activity.rs                   # execute_step activity (dispatches to plugins)
│   │     registry.rs                   # PluginRegistry (name → Arc<dyn Plugin>)
│   │
│   ├── router/                         # pipeline resolution logic (used by control-plane)
│   │   src/lib.rs                      # PipelineRouter: MIME + filename → pipeline
│   │
│   ├── queue/                          # (reserved for future Redis/Kafka abstraction)
│   │
│   ├── plugin-sdk/                     # THE crate published to crates.io
│   │   src/
│   │     lib.rs                        # Plugin trait + re-exports
│   │     types.rs                      # PluginInput/Output, Document, PipelineDefinition, MeiliContext
│   │     context.rs                    # ActivityContext (heartbeat, cancellation)
│   │     error.rs                      # PluginError
│   │
│   ├── plugin-runtime/                 # WASM (extism/wasmtime) + gRPC dispatch
│   │   src/lib.rs
│   │
│   └── plugins/                        # built-in plugins (also serve as examples)
│       ├── pdf/                        # pdf_extractor
│       ├── docx/                       # docx_extractor
│       ├── xlsx/                       # xlsx_extractor
│       ├── html/                       # html_extractor
│       ├── markdown/                   # markdown_extractor
│       ├── csv/                        # csv_parser
│       ├── json/                       # json_flattener
│       ├── chunker/                    # chunker (sentence/fixed/paragraph)
│       ├── meili-indexer/              # meili_indexer (reads MeiliContext)
│       ├── llm-enricher/               # llm_enricher (OpenAI-compatible API)
│       └── image-captioner/            # image_captioner (VLM via OpenAI API)
│
├── config/
│   └── pipelines/                      # example pipeline YAML files
│       ├── video-ingest.yaml
│       └── pdf-with-enrichment.yaml
│
├── k8s/
│   ├── workers.yaml                    # Deployments + KEDA ScaledObjects
│   ├── gateway.yaml
│   ├── control-plane.yaml
│   └── temporal.yaml                   # (reference only — use Temporal Cloud in prod)
│
└── SPEC.md                             # this file
```

---

## 7. Plugin system

### 7.1 The Plugin trait (plugin-sdk)

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn manifest(&self) -> PluginManifest;

    async fn execute(
        &self,
        ctx:    &ActivityContext,   // heartbeat + cancellation
        input:  PluginInput,
        config: serde_json::Value,  // from pipeline YAML `config:` block
    ) -> Result<PluginOutput, PluginError>;
}
```

### 7.2 Plugin kinds

| Kind | How it runs | Use case |
|---|---|---|
| **Built-in** | Compiled into the worker binary | PDF, DOCX, chunker, indexer |
| **WASM** | Loaded at runtime via `extism` + `wasmtime` | Lightweight community plugins, any language |
| **gRPC container** | Separate pod, called via tonic | GPU-heavy: Whisper, OCR, embedding |

For WASM plugins, the `plugin-sdk` crate has a `wasm` feature that exposes
`extism-pdk` helpers. Plugin authors compile to `wasm32-wasi` and ship a `.wasm`
file. The plugin runtime loads it via `extism`.

For gRPC plugins, the repo ships a `.proto` file (`plugin.proto`) that external
containers implement. The `plugin-runtime` crate dispatches to them over gRPC.

### 7.3 Writing a built-in plugin (the pattern to follow)

See `crates/plugins/llm-enricher/src/lib.rs` — it is the canonical example.
Key things to replicate:
- Implement `Plugin` trait
- Declare `manifest()` with proper `accepts` and `config_schema` (JSON Schema)
- Call `ctx.heartbeat(...)` periodically in long loops
- Check `ctx.is_cancelled()` and return `PluginError::NonRetryable("Cancelled")`
- Use `buffer_unordered(N)` for fan-out within a single plugin
- Declare `from_env()` constructor that reads credentials from env vars

### 7.4 MeiliContext in the indexer plugin

The `meili_indexer` plugin is the **only** plugin that touches Meilisearch.
It must:
1. Read `MeiliContext` from `config` (it is serialized there by the workflow)
2. Use `ctx.host` and `ctx.api_key` to create the Meilisearch client
3. Use `ctx.index` as the target index (it has already been resolved)
4. Never read `MEILI_URL` or `MEILI_API_KEY` env vars — those are only for
   the gateway's standalone fallback

```rust
// How MeiliContext flows into the indexer:
// 1. Gateway resolves MeiliContext from headers/env
// 2. Gateway passes it inside PipelineWorkflowInput
// 3. Workflow serializes it into each StepDefinition's config when building
//    the StepActivityInput for the indexer step
// 4. Indexer plugin deserializes it from config

#[derive(Deserialize)]
struct IndexerConfig {
    #[serde(flatten)]
    meili:             MeiliContext,
    primary_key:       String,
    auto_create_index: bool,
    batch_size:        usize,
}
```

---

## 8. Temporal workflow design

### 8.1 One workflow = one ingest job

```rust
#[workflow]
impl PipelineWorkflow {
    #[init] pub fn new() -> Self { Self {} }

    #[run]
    pub async fn run(&self, ctx: WfContext, input: PipelineWorkflowInput) 
        -> WorkflowResult<PipelineWorkflowOutput> { ... }

    #[signal] pub async fn cancel(&self) { ... }
    #[query]  pub fn current_step(&self) -> String { ... }
    #[query]  pub fn progress(&self) -> WorkflowProgress { ... }
}

pub struct PipelineWorkflowInput {
    pub job_id:   Uuid,
    pub pipeline: PipelineDefinition,
    pub input:    PluginInput,
    pub context:  MeiliContext,         // tenant context, carried end-to-end
}
```

### 8.2 DAG execution in the workflow

```
1. topological_sort(steps) → execution order respecting depends_on
2. For each step in order:
   a. resolve_input(step, previous_outputs, initial_input)
   b. if fan_out set → spawn N parallel activities → join_all → PluginOutput::Many
   c. else           → single activity
   d. inject MeiliContext into config if plugin == "meili_indexer"
3. Return PipelineWorkflowOutput { job_id, status, step_outputs }
```

### 8.3 Task queue routing

The workflow dispatches activities to **typed task queues** based on plugin name.
This allows specialized worker pools (GPU, LLM, general) to each poll only
their own queue.

```rust
fn plugin_task_queue(plugin: &str) -> &'static str {
    match plugin {
        "whisper_transcriber" | "ocr" | "video_audio_extractor" => "workers-gpu",
        "llm_enricher" | "image_captioner"                      => "workers-llm",
        "s3_downloader"                                          => "workers-io",
        _                                                        => "workers-general",
    }
}
```

### 8.4 Important Temporal constraints

- **Do NOT use `tokio::spawn`, `tokio::select!`, or `futures::select!` inside
  workflow code.** Use `temporalio_sdk::workflows::{select!, join!, join_all}`
  instead. The SDK has a nondeterminism detector that will panic if you use
  raw tokio primitives.
- Workflow code must be **deterministic** — same input always produces same
  execution history. No `rand`, no `SystemTime::now()`, no network calls.
  All side effects go in Activities.
- Use `ctx.activity(...).await` to run activities. Use `join_all(handles)` for
  parallel fan-out.

---

## 9. Built-in pipelines

These are compiled into the control-plane binary in `builtin_pipelines.rs`.
They are returned alongside user pipelines by the router.
User pipelines always take precedence (same MIME trigger → user wins).

| Pipeline UID | Triggers on | Steps |
|---|---|---|
| `builtin.pdf` | `application/pdf` | pdf_extractor → chunker → meili_indexer |
| `builtin.word` | `application/vnd...wordprocessingml...`, `application/msword` | docx_extractor → chunker → meili_indexer |
| `builtin.excel` | `application/vnd...spreadsheetml...`, `application/vnd.ms-excel` | xlsx_extractor → meili_indexer |
| `builtin.powerpoint` | `application/vnd...presentationml...` | pptx_extractor → meili_indexer |
| `builtin.html` | `text/html` | html_extractor → chunker → meili_indexer |
| `builtin.text` | `text/plain` | chunker → meili_indexer |
| `builtin.markdown` | `text/markdown`, `text/x-markdown` | markdown_extractor → meili_indexer |
| `builtin.csv` | `text/csv` | csv_parser → meili_indexer |
| `builtin.json` | `application/json` | json_flattener → meili_indexer |
| `builtin.image` | `image/jpeg`, `image/png`, `image/webp`, `image/gif` | image_captioner → meili_indexer |
| `builtin.audio` | `audio/mpeg`, `audio/wav`, `audio/ogg`, `audio/mp4` | whisper_transcriber → meili_indexer |
| `builtin.video` | `video/mp4`, `video/quicktime`, `video/webm` | video_audio_extractor → whisper_transcriber → meili_indexer |

Default chunker config: `strategy: sentence, chunk_size: 512, overlap: 64`
Default retry: `max_attempts: 3, backoff: exponential`

---

## 10. MIME detection chain

In `crates/gateway/src/extract.rs`, `detect_mime(data, filename)` runs:

1. **Magic bytes** via the `infer` crate — most reliable, checks file signature
2. **File extension** from the `filename` field — fallback for text formats
3. **UTF-8 sniff** of first 512 bytes — if valid UTF-8 → `text/plain`
4. **Default** → `application/octet-stream`

The Content-Type header is intentionally low-priority — callers often set it
wrong. Magic bytes don't lie.

---

## 11. Index name default mapping

When no index is specified anywhere in the request or pipeline:

```rust
fn mime_to_default_index(mime: &str) -> &str {
    match mime {
        m if m.starts_with("video/") => "videos",
        m if m.starts_with("audio/") => "audio",
        m if m.starts_with("image/") => "images",
        "text/html"                  => "pages",
        "text/csv"                   => "datasets",
        _                            => "documents",   // also configurable via DEFAULT_INDEX env
    }
}
```

---

## 12. Database schema (Postgres, via sqlx)

```sql
-- User-defined pipeline definitions
CREATE TABLE pipelines (
    uid          TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT,
    version      INT NOT NULL DEFAULT 1,
    definition   JSONB NOT NULL,          -- full PipelineDefinition as JSON
    project_id   TEXT,                    -- NULL = global; set = scoped to one tenant
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Job tracking (mirrors Temporal state, for quick status queries)
CREATE TABLE jobs (
    job_id        UUID PRIMARY KEY,
    workflow_id   TEXT NOT NULL,
    pipeline_uid  TEXT NOT NULL,
    project_id    TEXT,
    index_name    TEXT,
    status        TEXT NOT NULL DEFAULT 'queued',
    current_step  TEXT,
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## 13. Environment variables

### Gateway
| Variable | Default | Description |
|---|---|---|
| `BIND` | `0.0.0.0:8080` | Listen address |
| `TEMPORAL_URL` | `http://temporal-frontend:7233` | Temporal server |
| `TEMPORAL_NAMESPACE` | `default` | Temporal namespace |
| `CONTROL_PLANE_URL` | `http://meili-control-plane:9000` | Internal control plane |
| `MEILI_URL` | — | Standalone fallback Meilisearch host |
| `MEILI_API_KEY` | — | Standalone fallback API key |
| `DEFAULT_INDEX` | `documents` | Fallback index name |
| `MAX_UPLOAD_MB` | `500` | Max multipart upload size |
| `ENVOY_TRUSTED_HEADER` | — | Shared secret to trust X-Meili-* headers (optional) |

### Worker
| Variable | Default | Description |
|---|---|---|
| `TEMPORAL_URL` | `http://temporal-frontend:7233` | |
| `TASK_QUEUE` | `workers-general` | Which queue this worker polls |
| `LLM_API_KEY` | — | Required for workers-llm pool |
| `LLM_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible endpoint |

### Control Plane
| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | — | Postgres connection string |
| `BIND` | `0.0.0.0:9000` | Internal listen address |

---

## 14. What is already scaffolded

The following files exist and should be used as-is or built upon:

- `Cargo.toml` — workspace with all dependency versions pinned
- `crates/plugin-sdk/src/lib.rs` — `Plugin` trait
- `crates/plugin-sdk/src/types.rs` — all shared types including `MeiliContext`,
  `PipelineDefinition`, `PluginInput/Output`, `Document`
- `crates/plugin-sdk/src/context.rs` — `ActivityContext`
- `crates/plugin-sdk/src/error.rs` — `PluginError`
- `crates/worker/src/workflow.rs` — `PipelineWorkflow` (needs `MeiliContext`
  injection into indexer step — see §7.4)
- `crates/worker/src/activity.rs` — `execute_step` activity
- `crates/worker/src/registry.rs` — `PluginRegistry` builder
- `crates/worker/src/main.rs` — worker binary boot
- `crates/gateway/src/extract.rs` — `IngestPayload` parser
- `crates/gateway/src/state.rs` — `AppState`, `ControlPlaneClient`, `GatewayConfig`
- `crates/gateway/src/handlers/ingest.rs` — auto-ingest handler (partial)
- `crates/control-plane/src/builtin_pipelines.rs` — all 12 built-in pipelines
- `crates/router/src/lib.rs` — `PipelineRouter` with full test suite
- `crates/plugins/llm-enricher/src/lib.rs` — complete plugin example
- `config/pipelines/video-ingest.yaml` — complex pipeline example
- `k8s/workers.yaml` — worker Deployments + KEDA ScaledObjects

---

## 15. What needs to be built (prioritised)

### P0 — Core path (must work end-to-end first)

1. **`crates/gateway/src/context.rs`** — `MeiliContext::from_request(headers, env)`
   Implements the resolution order from §3.2. This is the most critical piece
   for multi-tenancy. Must handle both Envoy and standalone modes.

2. **`crates/gateway/src/main.rs`** — axum router + server boot.
   Wire up all routes from §4. Register `AppState` as axum state.
   Implement multipart extraction properly (axum extractor pattern).

3. **`crates/gateway/src/handlers/ingest.rs`** — complete the auto-ingest handler.
   Currently partial. Needs `MeiliContext` plumbed in, proper multipart
   handling via axum `Multipart` extractor, and the workflow start call.

4. **`crates/gateway/src/handlers/pipeline.rs`** — explicit pipeline handler.
   Simpler than auto-ingest: load pipeline by name, validate, start workflow.

5. **`crates/worker/src/workflow.rs`** — inject `MeiliContext` into the
   `meili_indexer` step config (see §7.4). The workflow receives it in
   `PipelineWorkflowInput` and must pass it into every `StepActivityInput`
   where `plugin == "meili_indexer"`.

6. **`crates/plugins/meili-indexer/src/lib.rs`** — the indexer plugin.
   Reads `MeiliContext` from config, batches documents, pushes to Meilisearch
   via the official `meilisearch-sdk` crate, waits for the task to complete.

7. **`crates/plugins/chunker/src/lib.rs`** — text chunker.
   Strategies: `sentence` (split on `.!?`), `fixed` (N chars), `paragraph`
   (split on `\n\n`). Adds `chunk_index` and `chunk_total` to `DocumentMeta`.

8. **`crates/plugins/pdf/src/lib.rs`** — PDF extractor.
   Use `lopdf` or `pdf-extract` crate. Per-page Documents. Preserve page number
   in `DocumentMeta.page`.

### P1 — Full built-in pipeline coverage

9.  `crates/plugins/docx/src/lib.rs` — use `docx-rs`
10. `crates/plugins/xlsx/src/lib.rs` — use `calamine`
11. `crates/plugins/html/src/lib.rs` — use `scraper`
12. `crates/plugins/csv/src/lib.rs` — use `csv` crate
13. `crates/plugins/json/src/lib.rs` — flatten nested JSON into Documents

### P2 — Control plane + job tracking

14. `crates/control-plane/src/main.rs` — HTTP server + routes
15. `crates/control-plane/src/db.rs` — sqlx pool, migrations
16. `crates/control-plane/src/pipelines.rs` — CRUD + validation (cycle detection)
17. `crates/control-plane/src/resolver.rs` — `POST /internal/resolve` endpoint
18. `crates/gateway/src/handlers/jobs.rs` — job status via Temporal query
19. `crates/gateway/src/handlers/pipelines.rs` — proxy to control plane

### P3 — Media plugins (need external services / GPU)

20. `crates/plugins/image-captioner/src/lib.rs` — calls OpenAI vision API
21. `whisper_transcriber` — gRPC plugin (separate container, not in this repo)
22. `video_audio_extractor` — ffmpeg-based, gRPC plugin

### P4 — WASM plugin runtime + gRPC plugin dispatch

23. `crates/plugin-runtime/src/wasm.rs` — extism host runtime
24. `crates/plugin-runtime/src/grpc.rs` — tonic client for heavy plugins
25. `proto/plugin.proto` — gRPC service definition

---

## 16. Key crates to use

| Purpose | Crate |
|---|---|
| HTTP server | `axum 0.7` |
| Middleware | `tower`, `tower-http` |
| Async runtime | `tokio` (full features) |
| Serialization | `serde`, `serde_json`, `serde_yaml` |
| Database | `sqlx` (postgres, runtime-tokio) |
| MIME detection | `infer` |
| PDF extraction | `pdf-extract` or `lopdf` |
| DOCX extraction | `docx-rs` |
| XLSX extraction | `calamine` |
| HTML parsing | `scraper` |
| HTTP client | `reqwest` (json + stream features) |
| Meilisearch | `meilisearch-sdk` (official) |
| Temporal | `temporalio-sdk` (Public Preview, use `#[workflow]` + `#[activity]`) |
| WASM host | `extism`, `wasmtime` |
| gRPC | `tonic`, `prost` |
| Error handling | `thiserror` (library crates), `anyhow` (binary crates) |
| Tracing | `tracing`, `tracing-subscriber` (json feature) |
| UUID | `uuid` (v4 + serde features) |

---

## 17. Code style and conventions

- Use `thiserror` in library crates, `anyhow` in binary crates (`main.rs`)
- All public types must derive `Debug`, `Clone`, `Serialize`, `Deserialize`
- All `Plugin` impls: `ctx.heartbeat(...)` every ~10 items in loops
- Tracing: use structured fields — `tracing::info!(job_id = %id, plugin = %name, "...")`
- No `unwrap()` in non-test code — use `?` or explicit error handling
- Env var reads: only in `from_env()` constructors or `main.rs`
- Tests: unit tests in `#[cfg(test)]` modules, prefer testing the router
  and context resolution logic exhaustively

---

## 18. Open questions / decisions deferred

1. **ENVOY_TRUSTED_HEADER**: How exactly does the gateway verify a request
   comes from Envoy and not a spoofed client? Options: shared secret header,
   mTLS, IP allowlist. Coordinate with glutony team. For now: if the header
   is absent, fall back to standalone mode without erroring.

2. **Tenant-scoped pipelines**: Should user-defined pipelines be scoped to a
   `project_id`? The DB schema has the column. The resolver should filter by
   `project_id = context.project_id OR project_id IS NULL`. Not yet implemented.

3. **Job persistence**: Should the `jobs` table be the source of truth for
   status, or should the gateway always query Temporal directly? Recommendation:
   use Temporal as source of truth, write to `jobs` table as a denormalized
   cache for fast queries without hitting Temporal's gRPC API on every poll.

4. **Plugin versioning**: The `PluginManifest` has a `version` field. The
   pipeline definition references plugins by name only. Should it be able to
   pin a version? Deferred — start without versioning.

5. **`pptx_extractor`**: No obvious Rust crate for PPTX. Options: call
   LibreOffice headless via subprocess, use a gRPC plugin. Deferred to P3.
