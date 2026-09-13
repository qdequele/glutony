# Scheduled Sources — Design

**Status:** approved, pending implementation plan
**Date:** 2026-09-13
**Scope:** v1 of a cron-driven ingestion subsystem: a *source* fetches content from a
location on a schedule and runs it through an existing pipeline.

## Problem

`meili-ingest` today is entirely request-driven. Every job starts because someone called
`POST /ingest`. URL fetching already works — `IngestPayload::Url` becomes
`ContentRef::Url`, and `BlobStore::fetch_ref` GETs it at worker time
(`crates/blob/src/lib.rs`) — but nothing re-runs it. Keeping an index in sync with an
upstream feed, a nightly export or a public dataset means running your own cron against
the gateway.

This design adds the missing half: a persisted *source* that Temporal drives on a cron.

## Motivating example

The TMDB daily ID export, which the design is validated against throughout:

```
https://files.tmdb.org/p/exports/movie_ids_09_13_2026.json.gz
```

Gzipped NDJSON, ~600k lines, a **new file each day** named after that day's date,
published around 08:00 UTC. It exercises date-templating, transparent decompression,
streaming of a payload far past `INLINE_MAX_BYTES`, and the item-vs-document
distinction — all covered below.

## Decisions

These resolve the design's forks. Where later sections appear to differ, this list wins.

1. **Scheduling is v1; connectors are v2.** The scheduling subsystem ships complete over
   a single connector (`url`). The `SourceConnector` trait and the `Resolution::Items`
   vector exist from day one so `zip`, `bucket` and `api` connectors drop in without
   touching the scheduler, the schema or the workflow.
2. **A source is its own entity**, not a field on `PipelineTrigger`. Several sources may
   share one pipeline, and pausing a source must not touch the pipeline.
3. **Temporal Schedules**, not a Postgres polling loop. `temporalio-client` 1.0 ships
   `create_schedule` / `pause` / `unpause` / `trigger` / `backfill` / `update`
   (`schedules.rs`). Building the equivalent by hand would mean re-implementing catchup
   windows, overlap policy and cron DST handling, plus leader election across control
   plane replicas — which has no background-worker role today.
4. **The Schedule starts a thin `SourceRunWorkflow`**, never `PipelineWorkflow`
   directly. A Schedule serialises a *fixed* workflow input at creation time; starting
   `PipelineWorkflow` directly would freeze both the `job_id` and the pipeline
   definition, and leave nowhere to put the conditional fetch or the future fan-out.
5. **Conditional fetch, upsert, no deletes.** Each run sends `If-None-Match` /
   `If-Modified-Since`. A `304`, or a body whose hash matches the last run, ends the run
   before any job is created. Documents are upserted by id as today; documents that
   disappear upstream stay in the index. Mirror-sync deletion is a non-goal (see below).
6. **Two secrets per source, sealed at rest**: the fetch credential, and a
   `MeiliContext` (host + write key). The second is unavoidable — a cron tick has no
   incoming request, so there is no Envoy to inject `X-Meili-*` headers. Both are
   captured from the creating request and never returned by the API.
7. **`Resolution::Items` are source items, not documents.** One TMDB export is *one*
   item that yields ~600k documents. `MAX_FAN_OUT_BRANCHES` (500,
   `crates/worker/src/workflow.rs`) therefore never applies to a source's document
   count. Reading it the other way would cap every source at 500 documents.
8. **The SSRF guard applies to source fetches only in v1.** Extending it to ad-hoc
   `POST /ingest {"url": …}` is arguably correct but silently breaks anyone ingesting
   from an internal host today. Deliberately deferred.
9. **Template rendering uses the Temporal *scheduled* time**, not wall-clock. Retries of
   a run must resolve the same URL, and a backfill of 2026-09-01 must fetch that day's
   file, not today's. A manual `POST /sources/{uid}/run` has no scheduled time and uses
   the trigger instant.
10. **Cron validation is delegated to Temporal.** `create_schedule` rejects a malformed
    expression; the gateway surfaces that as `422` rather than shipping a second cron
    parser that could disagree with the one actually firing. This costs a round trip on
    create and is worth it.

### New dependencies

None of these are in the workspace today; all are added to `[workspace.dependencies]`:

| Crate | Why |
|---|---|
| `chacha20poly1305` | Sealing `fetch_auth` and `meili_ctx`. No crypto crate exists in the tree. |
| `blake3` | Hashing the streamed body for change detection when the server sends no `ETag`. |
| `flate2` | Transparent gzip. The existing `zip` dep is used only by the docx/pptx parsers. |
| `chrono-tz` | Rendering date templates in the source's timezone. `chrono` alone is UTC/offset only. |

## Data model

Migration `0002_sources.sql`.

```sql
CREATE TABLE IF NOT EXISTS sources (
    id           UUID PRIMARY KEY,
    uid          TEXT NOT NULL,            -- handle, unique per project
    name         TEXT NOT NULL,
    description  TEXT,
    project_id   TEXT,                     -- NULL = global / self-hosted
    pipeline_uid TEXT NOT NULL,
    location     JSONB NOT NULL,           -- tagged union, see below
    cron         TEXT NOT NULL,
    timezone     TEXT NOT NULL DEFAULT 'UTC',
    paused       BOOLEAN NOT NULL DEFAULT false,
    index_name   TEXT,
    fetch_auth   BYTEA,                    -- sealed; NULL = unauthenticated
    meili_ctx    BYTEA NOT NULL,           -- sealed: host + api_key + region
    last_etag      TEXT,
    last_modified  TEXT,
    last_hash      BYTEA,                  -- blake3 of the last fetched body
    last_run_at    TIMESTAMPTZ,
    last_status    TEXT,
    last_error     TEXT,
    schedule_id  TEXT NOT NULL,            -- Temporal schedule id
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS sources_uid_project
    ON sources (uid, COALESCE(project_id, ''));

CREATE TABLE IF NOT EXISTS source_runs (
    run_id      UUID PRIMARY KEY,
    source_id   UUID NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT NOT NULL,             -- unchanged | ingested | failed
    items       INT NOT NULL DEFAULT 0,
    job_ids     UUID[] NOT NULL DEFAULT '{}',
    error       TEXT
);

CREATE INDEX IF NOT EXISTS source_runs_source_started
    ON source_runs (source_id, started_at DESC);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_id UUID;
CREATE INDEX IF NOT EXISTS jobs_source ON jobs (source_id, started_at DESC);
```

`source_runs` duplicates information Temporal already holds, for the same reason `jobs`
does: workflow history is retention-limited and cannot be filtered per tenant without a
visibility query per request. It is a cache; Temporal remains the source of truth.

### `location` is a tagged union

v1 only ever writes `kind: "url"`. The other variants are reserved so v2 needs no
migration:

```jsonc
{ "kind": "url",    "url": "https://…/movie_ids_{{ date:%m_%d_%Y }}.json.gz",
                    "method": "GET", "headers": { "Accept": "application/json" } }
// `headers` here are non-secret and readable back via the API. Anything sensitive
// (bearer token, basic auth, an API-key header) goes in the sealed `fetch_auth`
// column instead and is merged over these at fetch time.
{ "kind": "zip",    "url": "…", "include": "**/*.json" }          // v2
{ "kind": "bucket", "uri": "s3://bucket/prefix/", "include": "**/*.pdf" }  // v2
{ "kind": "api",    "url": "…", "items_path": "$.results",
                    "pagination": { … } }                          // v2
```

### URL templating

Rendered against the run's **scheduled** time (Decision 9), UTC unless the source sets a
timezone:

| Token | Expands to |
|---|---|
| `{{ date:FMT }}` | `strftime(FMT)` of the scheduled time — `%m_%d_%Y`, `%Y-%m-%d`, … |
| `{{ date-1d:FMT }}` | Same, offset by a duration (`-1d`, `-1h`, `+2d`) |
| `{{ timestamp }}` | Unix seconds of the scheduled time |

The offset form matters for TMDB: a schedule running at 00:30 UTC must fetch the
*previous* day's export, because the current day's file does not exist until ~08:00.

Unknown tokens are a validation error at source-create time, not a silent passthrough.

## Connector interface

The v2 carve-out. Lives in a new crate `crates/source`.

```rust
pub trait SourceConnector: Send + Sync {
    fn kind(&self) -> &'static str;

    /// Resolve a location into zero or more *source items*, honouring the
    /// incremental state from the previous run.
    async fn resolve(
        &self,
        loc: &Location,
        state: &IncrementalState,
        rt: &ResolveRuntime,   // http client, blob store, guard, scheduled time
    ) -> Result<Resolution, SourceError>;
}

pub enum Resolution {
    /// Upstream is byte-identical to the previous run. No job, no usage.
    Unchanged,
    Items {
        items: Vec<PluginInput>,   // always Ref(ContentRef::Staged { .. })
        state: IncrementalState,
    },
}

pub struct IncrementalState {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub hash: Option<[u8; 32]>,
}
```

v1 ships `UrlConnector` only. `Items` being a `Vec` from the start is the whole point:
when `BucketConnector` later returns 400 objects, the scheduler is unchanged.

### `UrlConnector` behaviour

1. Render the URL template against the scheduled time.
2. Run the SSRF guard (below). Reject before any socket is opened.
3. `GET` with `If-None-Match` / `If-Modified-Since` from `state`. `304` → `Unchanged`.
4. **Stream** the body to the blob store — never `response.bytes()` into a `Vec`.
   `fetch_ref` buffers today (`crates/blob/src/lib.rs`), which is fine for a 1 MiB
   upload and not fine for a 50 MB export fetched by several sources at once. Hash with
   blake3 while streaming; enforce a byte cap during the stream, not after.
5. If the streamed hash equals `state.hash`, discard the staged object → `Unchanged`.
   (Covers servers that send no `ETag`, which is the common case for static exports.)
6. **Decompress transparently** when the body is gzip — detected by magic bytes
   (`1f 8b`), with the `.gz` extension only as a filename hint. Requires adding `flate2`
   to the workspace; nothing in the tree decompresses today (`zip` is a workspace dep
   but only the docx/pptx parsers use it, because Office files are zips).
   The MIME reported for the item is that of the *decompressed* content, so
   `movie_ids_…json.gz` routes as `application/x-ndjson` and the existing json plugin
   parses it line-by-line — already supported (`crates/plugins/json/src/lib.rs`).
7. Return one item.

## `SourceRunWorkflow`

A second workflow type registered alongside `PipelineWorkflow`
(`crates/worker/src/main.rs`, one line). Input is deliberately tiny — `{ source_id,
project_id }` — so the Schedule's frozen payload stays valid across every edit to the
source or its pipeline.

```
1. load_source(source_id)        activity → definition, decrypted secrets, state,
                                            the current pipeline definition
2. resolve_source(…)             activity → Resolution
3. Resolution::Unchanged         → record_run(unchanged); return.  No job. No usage.
4. Resolution::Items             → for each item, start a PipelineWorkflow child with a
                                   fresh job_id and the source's MeiliContext
5. record_run(outcome, state)    activity → persist etag/last_modified/hash, job ids
```

Because the pipeline is read in step 1 at run time, editing a pipeline takes effect on
the next tick with no schedule rewrite.

**Overlap policy** is `ScheduleOverlapPolicy::Skip`: a tick that fires while the previous
run is still in flight is dropped. A daily TMDB import that takes 40 minutes must never
have two copies racing into the same index.

**Usage.** `events_for_job` emits rows per *finished job*
(`crates/usage/src/lib.rs`), so an `Unchanged` run bills nothing — no job is ever
created. This is what makes an aggressive cron safe on a metered tenant.

## Security

### Secrets at rest

No crypto crate exists in the workspace today. Add `chacha20poly1305` (RustCrypto).

- Key from `SOURCE_SECRET_KEY`, 32 bytes base64.
- Sealed value is `nonce ‖ ciphertext`, nonce random per write.
- **If the key is unset, the whole `/sources` API returns `501 Not Implemented`.**
  Storing a tenant's Meilisearch write key in plaintext because an env var was missed is
  not an acceptable degraded mode.
- Rotation is out of scope for v1; the sealed blob carries a version byte so a rotation
  scheme can be added without a migration.

### Redaction

`GET /sources/{uid}` renders secrets as a kind plus a mask, never a value:

```json
{ "auth": { "kind": "bearer", "token": "****" },
  "meili": { "host": "https://x.us-west.meilisearch.io", "api_key": "****" } }
```

A `PATCH` that omits `auth` leaves the stored secret untouched; a `PATCH` sending
`"auth": null` clears it. Tests assert no response body and no log line ever contains a
decrypted value — the existing `MeiliContext::redacted()` convention extended to sources.

### SSRF guard

Tenant-supplied URLs are fetched from inside the cluster, where `169.254.169.254`,
`meili-control-plane:9000` and `temporal-frontend:7233` are all reachable.

- `https` only.
- Resolve DNS explicitly and reject loopback, private (RFC1918), link-local, CGNAT
  (100.64/10), ULA (fc00::/7), unspecified and multicast addresses. Check **every**
  resolved address, not just the first.
- Redirects followed manually with the guard re-applied at each hop, capped at 5. An
  allowed host redirecting to `127.0.0.1` is the obvious bypass.
- Byte cap enforced during streaming.
- Guard applies to source fetches only in v1 (Decision 8).

Pinning the resolved address to defeat DNS rebinding between the check and the connect
is noted as a known residual risk, not solved in v1.

## API

Gateway routes, mirroring the existing `/pipelines` shape
(`crates/gateway/src/lib.rs`):

| Method | Route | Notes |
|---|---|---|
| `POST` | `/sources` | Captures + seals `MeiliContext` from request headers. Validates cron, template tokens and pipeline existence. Creates the Temporal Schedule. |
| `GET` | `/sources` | Tenant-scoped list, secrets redacted. |
| `GET` | `/sources/{uid}` | Includes `next_run_at` from Temporal `describe`. |
| `PATCH` | `/sources/{uid}` | Updates the Schedule when cron/timezone change. |
| `DELETE` | `/sources/{uid}` | Deletes the Schedule, then the row. |
| `POST` | `/sources/{uid}/pause` / `/unpause` | Temporal pause/unpause. |
| `POST` | `/sources/{uid}/run` | `trigger` — run now, off-schedule. |
| `GET` | `/sources/{uid}/runs` | Paginated `source_runs`. |

Create is not atomic across Postgres and Temporal. The row is written first with
`paused = true`, the Schedule is created, then the row is unpaused. A crash between the
two leaves a paused source with no schedule, which is visible and repairable, rather
than a schedule firing against a row that does not exist.

**A source's pipeline may be deleted underneath it.** `DELETE /pipelines/{uid}` returns
`409` when any source references it, listing the source uids. Forcing the delete is not
offered in v1 — the alternative, letting every tick fail at `load_source`, turns one
explicit error into a recurring silent one. `load_source` still handles the missing-
pipeline case (row deleted directly in SQL, say) by recording a `failed` run with a
clear message rather than retrying forever.

## UI

`ui/src/app/sources/`, co-located components per repo convention, mirroring the existing
`pipelines` page: list with status and next run, create/edit form (react-hook-form + zod
+ shadcn, lucide icons), run history table linking each run to its jobs, and
pause/resume/run-now controls. Secrets are write-only inputs showing `****` when set.

## Docs

- `docs/concepts/sources.mdx` — concepts, cron syntax, templating table, the TMDB
  walkthrough end to end.
- `docs/openapi.yaml` — the eight routes above.
- `README.md` — one line in the feature list.

## Testing

**Unit**
- Template rendering: each token, offsets, unknown token rejected, DST boundary in a
  non-UTC timezone.
- Malformed cron surfaces as `422` (Temporal's rejection mapped, per Decision 10).
- SSRF classifier: table-driven across loopback / RFC1918 / link-local / CGNAT / ULA /
  public, v4 and v6, plus the redirect-to-private case.
- Seal/open roundtrip; wrong key fails; redaction never emits a decrypted value.
- `UrlConnector` against wiremock: `304` → `Unchanged`; changed body → one item;
  identical body with no `ETag` → `Unchanged` via hash; gzip body decompressed and typed
  as `application/x-ndjson`; oversized body rejected mid-stream; non-2xx surfaced.

**Workflow** (Temporal test env)
- `Unchanged` starts zero children and writes one `unchanged` run row.
- `Items` of length N starts N children with distinct job ids.
- A failing resolve records `failed` with the error and does not advance the ETag.
- Overlap: a second trigger while running is skipped.

**Integration** (`scripts/e2e.sh`)
- Create a source against a local fixture server → `POST /run` → job appears, documents
  land in Meilisearch → second `POST /run` → `unchanged`, no new job.

## Non-goals for v1

Explicitly out of scope, listed so the plan does not quietly grow:

- `zip`, `bucket` and `api` connectors (the trait exists; the impls do not).
- Mirror-sync deletion of documents that vanished upstream.
- Per-item incremental state (only whole-source ETag/hash in v1).
- Crawling / link-following.
- Webhook or event-driven triggers.
- Secret rotation.
- Extending the SSRF guard to ad-hoc `POST /ingest` URLs.

## Open risk

The `meili_ctx` secret is a long-lived, write-capable Meilisearch key held at rest. That
is inherent to request-less execution and was accepted deliberately. If Meilisearch Cloud
later exposes an internal mint-a-scoped-key authority, sources should migrate to
storing only `project_id` and minting per run.
