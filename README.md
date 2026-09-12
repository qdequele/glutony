# meili-ingest

**Drop any content in, get a searchable Meilisearch index out.**

`meili-ingest` is an open-source, Kubernetes-native ingestion pipeline for
[Meilisearch](https://www.meilisearch.com). It exposes an HTTP gateway that
accepts arbitrary content (PDF, Word, Excel, HTML, Markdown, CSV, JSON, images,
audio, video, …), detects what it is, runs it through a configurable DAG of
processing steps (extraction, chunking, OCR, transcription, LLM enrichment, …)
orchestrated by [Temporal](https://temporal.io), and pushes the result into a
Meilisearch index.

- **100% Rust** — axum gateway, Temporal workers, sqlx control plane.
- **Kubernetes-native** — Deployments, KEDA autoscaling per task queue, GPU pools.
- **Multi-tenant** — one deployment serves every Meilisearch Cloud project via Envoy-injected headers; also runs standalone.
- **Extensible** — built-in, WASM (extism) and gRPC plugins share one `Plugin` trait (`meili-ingest-plugin-sdk`).

Documentation: [`docs/`](docs/) (Mintlify) — start with `docs/quickstart.mdx`.

## Architecture

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

Three binaries, one image (`ghcr.io/meilisearch/meili-ingest`):

| Binary | Role |
|---|---|
| `meili-ingest-gateway` | Public HTTP API. Resolves the tenant context, detects MIME, picks a pipeline, stages large payloads, starts the Temporal workflow. |
| `meili-ingest-control-plane` | Internal API + Postgres. Pipeline registry (built-in + user), routing, plugin manifests, job cache. |
| `meili-ingest-worker` | Temporal worker. Runs `PipelineWorkflow` and one `execute_step` activity per step through the plugin registry. Polls one task queue (`TASK_QUEUE`). |

## 5-minute quickstart

Prerequisites: Docker with Compose v2.22+ (OrbStack recommended on macOS).

```bash
git clone https://github.com/meilisearch/meili-ingest
cd meili-ingest

# Build, start Postgres + Temporal + Meilisearch + all meili-ingest services,
# and hot-reload on source changes. First build takes a few minutes.
docker compose watch
```

Services (OrbStack DNS names; `localhost:<port>` works too):

| Service | URL |
|---|---|
| Gateway | http://gateway.meili-ingest.orb.local:8080 |
| Temporal UI | http://temporal.meili-ingest.orb.local:8233 |
| Meilisearch | http://meilisearch.meili-ingest.orb.local:7700 (key `masterKey`) |

Ingest something:

```bash
export INGEST=http://localhost:8080

# Multipart file upload (most common)
curl -F "file=@report.pdf" $INGEST/ingest
curl -F "file=@report.pdf" -F "index=contracts" $INGEST/ingest

# JSON with URL reference (gateway fetches it inline for small files)
curl -d '{"url":"https://example.com/doc.pdf"}' \
     -H "Content-Type: application/json" $INGEST/ingest

# JSON with S3 reference (worker streams it — for large files)
curl -d '{"s3":"s3://my-bucket/large-video.mp4"}' \
     -H "Content-Type: application/json" $INGEST/ingest

# Inline JSON documents (direct indexing, no extraction needed)
curl -d '{"documents":[{"id":"1","title":"Hello"}]}' \
     -H "Content-Type: application/json" $INGEST/ingest

# Raw body (Content-Type + magic bytes determine handling)
curl --data-binary @document.pdf \
     -H "Content-Type: application/pdf" $INGEST/ingest

# Batch (starts one job per item)
curl -d '{"items":[{"url":"..."},{"s3":"..."}]}' \
     -H "Content-Type: application/json" $INGEST/ingest/batch
```

Response:

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "pipeline_used": "builtin.pdf",
  "target_index": "documents",
  "status": "queued"
}
```

Follow the job, then search:

```bash
curl $INGEST/jobs/550e8400-e29b-41d4-a716-446655440000
curl -H "Authorization: Bearer masterKey" \
     "http://localhost:7700/indexes/documents/search?q=hello"
```

The local stack points the gateway at the bundled Meilisearch through
`MEILI_URL` / `MEILI_API_KEY` (standalone mode), so no headers are needed.

## Two-tier API

**Tier 1 — `POST /ingest`: auto-routing.** Zero configuration. The gateway
detects the MIME type (magic bytes → extension → UTF-8 sniff), asks the control
plane which pipeline matches (user pipelines beat built-ins), resolves the
target index and starts the job. Twelve built-in pipelines cover PDF, Word,
Excel, PowerPoint, HTML, text, Markdown, CSV, JSON, images, audio and video.

**Tier 2 — `POST /ingest/pipeline/:name`: explicit pipeline.** Full control:
run a named pipeline (built-in or your own) regardless of content type, with an
optional `?index=` override.

Pipelines are YAML/JSON DAGs of plugin steps with `depends_on`, `fan_out`
(one parallel activity per document), per-step timeouts and retry policies:

```yaml
uid: my-pdf-with-enrichment
trigger:
  content_types: [application/pdf]
  filename_pattern: "contract_*.pdf"
steps:
  - id: extract
    plugin: pdf_extractor
  - id: chunk
    plugin: chunker
    config: { strategy: sentence, chunk_size: 512, overlap: 64 }
  - id: enrich
    plugin: llm_enricher
    fan_out: "$.documents"
    config: { model: gpt-4o-mini, prompt: "Extract as JSON: title, summary, keywords, language." }
  - id: index
    plugin: meili_indexer
```

```bash
curl -X POST --data-binary @config/pipelines/pdf-with-enrichment.yaml \
     -H 'Content-Type: application/x-yaml' $INGEST/pipelines
```

Other endpoints: `GET /jobs/:id`, `POST /jobs/:id/cancel`, `GET|POST /pipelines`,
`GET|DELETE /pipelines/:uid`, `GET /plugins`, `GET /health`. Full OpenAPI spec in
[`docs/openapi.yaml`](docs/openapi.yaml).

## Multi-tenancy

Every request is scoped to a `MeiliContext` (`project_id`, `host`, `api_key`,
`index`, `region`) resolved **once** at the gateway and carried immutably
through the Temporal workflow. Workers never read Meilisearch credentials from
the environment; only the `meili_indexer` plugin touches Meilisearch, using the
context the workflow injects into its config.

Resolution order: `X-Meili-Host` / `X-Meili-Api-Key` / `X-Meili-Project-Id` /
`X-Meili-Index` headers (injected by Meilisearch Cloud's Envoy, trusted only when
`X-Meili-Envoy-Secret` matches `ENVOY_TRUSTED_HEADER`) → `?index=` →
`Authorization: Bearer` → `MEILI_URL` / `MEILI_API_KEY` env vars → `400`.

User pipelines can be global or scoped to a `project_id`; tenant pipelines
shadow global ones, which shadow built-ins.

## Plugins

A plugin implements one trait:

```rust
#[async_trait]
pub trait Plugin: Send + Sync + 'static {
    fn manifest(&self) -> PluginManifest;
    async fn execute(&self, ctx: &ActivityContext, input: PluginInput, config: serde_json::Value)
        -> Result<PluginOutput, PluginError>;
}
```

| Kind | Runs as | Good for |
|---|---|---|
| Built-in | compiled into the worker (`crates/plugins/*`) | fast, pure-Rust steps |
| WASM | `.wasm` loaded at runtime via extism | community plugins in any language |
| gRPC | separate container implementing `proto/plugin.proto` | GPU / heavy runtimes (Whisper, OCR, ffmpeg) |

`crates/plugins/llm-enricher` is the canonical example. See
`docs/plugins/authoring-builtin.mdx`, `authoring-wasm.mdx`, `authoring-grpc.mdx`.

## Deployment

- **Local / dev:** `docker compose watch` (see [`compose.yaml`](compose.yaml)).
- **Kubernetes:** `kubectl apply -k k8s/` — gateway (HPA + PDB), control plane,
  three worker pools with KEDA `ScaledObject`s on Temporal queue backlog, and a
  reference Temporal deployment (use Temporal Cloud in production).
- **Meilisearch Cloud:** deploy behind Envoy with `ENVOY_TRUSTED_HEADER` set;
  see `docs/deployment/meilisearch-cloud.mdx`.

Environment variables are documented in `docs/deployment/environment-variables.mdx`.

## Development

```bash
cargo build --workspace
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all --check
```

Requires Rust 1.94+ (the floor comes from `sqlx-postgres`) and `protoc`. DB-backed control-plane tests run when
`DATABASE_URL` is set.

For a full-stack check, `scripts/e2e.sh` starts Postgres, Meilisearch and a
Temporal dev server, runs the gateway, control plane and a worker from your
working tree, ingests a PDF, inline JSON documents and raw text, then asserts
the documents are searchable and tears everything down.

```bash
./scripts/e2e.sh
```

It needs Docker, the `temporal` CLI, `jq` and `python3`.

## License

MIT — see [LICENSE](LICENSE).
