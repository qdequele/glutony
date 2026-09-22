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
6. **The destination lives on the pipeline, not the source.** *(Revised 2026-09-23;
   this replaced "two secrets per source".)* A cron tick has no incoming request, so no
   Envoy to inject `X-Meili-*` headers. Rather than have each source capture and seal a
   Meilisearch key, the pipeline's `meili_indexer` step names a **Meilisearch
   connection** (Decision 11) that pins host + key. A source stores exactly one secret —
   its fetch credential — and `POST /sources` refuses (`422`) a pipeline whose indexer
   step names no connection.
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
11. **Meilisearch connections are a named, tenant-scoped entity** holding a host and a
    sealed API key. Pipelines reference them by name, so a key lives in exactly one
    place: rotating it updates every pipeline at once, and pipeline JSON never contains a
    secret and stays safe to view, export and share.
12. **A connection on the indexer step always wins over the request context** for
    `host`/`api_key`. A step without one behaves exactly as today. This inverts
    `inject_meili_context` (`crates/plugin-sdk/src/types.rs`), which currently
    *always* overwrites `host`/`api_key` with the request's.
13. **The connection's key is opened inside the indexer activity, never in the
    workflow.** The workflow passes only the connection name; the key is decrypted in
    memory just before the plugin runs, so it never enters Temporal history. That is
    stricter than today's request path, where the injected `api_key` is recorded in the
    activity input.
14. **Connection hosts follow a deployment policy, strict by default.**
    `MEILI_CONNECTION_HOSTS` is `public` (default), `any`, or a comma list of
    `host[:port]`. A connection's host is a tenant-supplied URL written to from inside
    the cluster — the fetch-side SSRF exposure, on the write side — but self-hosted
    users routinely run Meilisearch on a private address, so the policy is one line of
    config rather than hard-coded.
15. **Deleting a connection is never blocked**, matching the pipeline rule. Pipelines
    still referencing it fail at run time with `connection "<uid>" not found`, and the
    pipeline editor flags the dangling reference. Nothing is archived: the pipeline is
    intact, only its destination is gone.

### New dependencies

None of these are in the workspace today; all are added to `[workspace.dependencies]`:

| Crate | Why |
|---|---|
| `chacha20poly1305` | Sealing a source's `fetch_auth` and a connection's `api_key`. No crypto crate exists in the tree. |
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
    last_etag      TEXT,
    last_modified  TEXT,
    last_hash      TEXT,                   -- hex blake3 of the last fetched body
    last_run_at    TIMESTAMPTZ,
    last_status    TEXT,
    last_error     TEXT,
    schedule_id  TEXT NOT NULL,            -- Temporal schedule id
    archived_at  TIMESTAMPTZ,              -- set when the source's pipeline is deleted
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS sources_uid_project
    ON sources (uid, COALESCE(project_id, ''));

CREATE TABLE IF NOT EXISTS meili_connections (
    id           UUID PRIMARY KEY,
    uid          TEXT NOT NULL,            -- what an indexer step names
    name         TEXT NOT NULL,
    project_id   TEXT,                     -- NULL = global / self-hosted
    host         TEXT NOT NULL,            -- not secret; returned by the API
    api_key      BYTEA NOT NULL,           -- sealed; never returned
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS meili_connections_uid_project
    ON meili_connections (uid, COALESCE(project_id, ''));

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

`0002_sources.sql` is revised in place for the connection redesign rather than followed
by a `0003` that drops `meili_ctx`: the branch is unmerged, so no deployed database has
applied it. A local dev database that ran the earlier version must drop `sources`,
`source_runs`, `jobs.source_id` and the `_sqlx_migrations` row for version 2 once.

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

## Meilisearch connections and the indexer step

### The indexer step names its destination

```yaml
- id: index
  plugin: meili_indexer
  config:
    connection: prod-movies     # pins host + api_key (Decision 12)
    index: movies               # optional; pins the index too
    batch_size: 1000            # existing: max documents per request
    max_batch_bytes: 52428800   # new: max serialized bytes per request
```

**Index resolution** when a connection is set: step `index` → pipeline trigger
`index_pattern` → the request's index, when there is a request → deployment default.
Without a connection, today's chain is unchanged.

**Batching.** `batch_size` (default 1000) stays the document-count cap. `max_batch_bytes`
(default 50 MiB, under Meilisearch's 100 MB default `http_payload_size_limit`) is measured
on the serialized JSON; a batch is cut at whichever limit is reached first. A single
document larger than `max_batch_bytes` is a **non-retryable** error naming its id — the
alternative is a Meilisearch `413` that the retry policy would repeat forever. Document
count alone is the wrong knob: 1000 TMDB id rows are ~60 KB, 1000 chunked PDFs with
embedded text can exceed the payload limit.

### Where the key is opened

```
PipelineWorkflow (deterministic)      execute_step activity (I/O)
─────────────────────────────         ─────────────────────────────────────────
step config: { connection:            1. GET /internal/connections/{uid}?project_id
  "prod-movies", index: … }  ───────▶ 2. open api_key with SecretKey (in memory)
                                      3. re-check host against MEILI_CONNECTION_HOSTS
                                      4. merge host/api_key into the config
                                      5. call meili_indexer
```

The workflow's `step_config` (`crates/worker/src/workflow.rs`) stops calling
`inject_meili_context` for `host`/`api_key` when a `connection` is present, so the
activity input — and therefore Temporal history — carries only the name.

### `MeiliContext` becomes optional at the start of a pipeline

`host` and `api_key` on the workflow input become `Option`. `POST /ingest` against a
pipeline whose indexer pins a connection no longer requires `X-Meili-*` headers or
standalone credentials, where today it returns `400 MissingContext`. A pipeline whose
indexer has no connection still requires them, with the same error as today.

### Connection validation

On create and on any `PATCH` touching `host` or `api_key`, the gateway:

1. applies the `MEILI_CONNECTION_HOSTS` policy to `host` (Decision 14);
2. calls `GET {host}/health`;
3. calls `GET {host}/indexes?limit=1` with the key, so a wrong key fails with `422` at
   save time rather than at 3am inside a cron run.

The dev `compose.yaml` sets `MEILI_CONNECTION_HOSTS=meilisearch:7700`.

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
        auth: Option<&FetchAuth>,
        state: &IncrementalState,
        rt: &ResolveRuntime,   // http client, guard, scheduled time, timezone
    ) -> Result<Resolution, SourceError>;
}

pub enum Resolution {
    /// Upstream is byte-identical to the previous run. No job, no usage.
    Unchanged,
    Items {
        items: Vec<ResolvedItem>,  // bytes + mime + filename
        state: IncrementalState,
    },
}

pub struct IncrementalState {
    pub etag: Option<String>,
    pub last_modified: Option<String>,
    pub hash: Option<String>,      // hex, so it survives JSON and Temporal payloads
}
```

A `ResolvedItem` carries bytes, not a staged ref: staging into the blob store is the
worker activity's job, which is what keeps every connector test free of an object store.
The activity stages each item and hands the workflow a `ContentRef::Staged`, so bytes
never enter workflow history.

v1 ships `UrlConnector` only. `Items` being a `Vec` from the start is the whole point:
when `BucketConnector` later returns 400 objects, the scheduler is unchanged.

### `UrlConnector` behaviour

1. Render the URL template against the scheduled time.
2. Run the SSRF guard (below). Reject before any socket is opened.
3. `GET` with `If-None-Match` / `If-Modified-Since` from `state`. `304` → `Unchanged`.
4. **Stream** the body chunk by chunk, hashing with blake3 as it arrives and enforcing
   the byte cap *during* the stream, not after — so an oversized body is abandoned
   early rather than downloaded in full.
   *As built:* chunks accumulate in memory up to `UrlGuard::max_bytes` (512 MiB), and
   the worker activity stages the result into the blob store afterwards. Streaming
   straight into the blob store is still the target — several sources fetching 50 MB
   exports at once should not hold them all in RAM — but it needs gzip decompression to
   become streaming too, and is deferred to a follow-up.
5. If the hash equals `state.hash` → `Unchanged`, nothing staged. (Covers servers that
   send no `ETag`, which is the common case for static exports.)
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
1. load_source(source_id)        activity → definition, decrypted fetch_auth, state,
                                            the current pipeline definition
2. resolve_source(…)             activity → Resolution
3. Resolution::Unchanged         → record_run(unchanged); return.  No job. No usage.
4. Resolution::Items             → for each item, start a PipelineWorkflow child with a
                                   fresh job_id and a MeiliContext carrying only
                                   project_id + index; host/api_key come from the
                                   indexer step's connection (Decision 13)
5. record_run(outcome, state)    activity → persist etag/last_modified/hash, job ids
```

`load_source` re-checks that the pipeline's indexer still names a connection. If it was
edited to drop one since the source was created, the run is recorded `failed` with that
reason rather than starting a child that would fail on missing context.

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

Two secrets exist, one per entity: a source's `fetch_auth`, and a connection's
`api_key`.

- Key from `SOURCE_SECRET_KEY`, 32 bytes base64. One key seals both.
- Sealed value is `version ‖ nonce ‖ ciphertext`, nonce random per write.
- **If the key is unset, the `/sources` and `/connections` APIs return
  `501 Not Implemented`**, and the worker fails any step naming a connection with a
  non-retryable error. Storing a Meilisearch write key in plaintext because an env var
  was missed is not an acceptable degraded mode.
- The control plane only ever moves sealed bytes. Decryption happens in the gateway
  (to validate a connection on save) and in the worker activity (to use it) — never in
  the control plane, which keeps its blast radius small.
- Rotation is out of scope for v1; the version byte lets a rotation scheme be added
  without a migration.

### Redaction

Secrets render as a kind plus a mask, never a value:

```json
GET /sources/tmdb        { "auth": { "kind": "bearer", "token": "****" }, … }
GET /connections/prod    { "host": "https://x.meilisearch.io", "api_key": "****", … }
```

A `PATCH` that omits the secret leaves it untouched; `"auth": null` clears a source's
credential. A connection's `api_key` cannot be cleared, only replaced — a connection
without a key is meaningless. Tests assert no response body and no log line ever
contains a decrypted value — the existing `MeiliContext::redacted()` convention,
extended to both entities.

### Connection host policy

`MEILI_CONNECTION_HOSTS` (Decision 14):

| Value | Accepts |
|---|---|
| `public` *(default)* | `https` only; every resolved address public — the source-fetch guard, reused |
| `any` | any `http`/`https` host |
| `meilisearch:7700,meili.internal` | exactly these `host[:port]` entries, `http` or `https`; nothing else |

Checked on save and again in the activity before each use, because DNS can change in
between. `public` is the default so a multi-tenant deployment is safe without
configuration; a self-hosted operator writing to a private instance sets one line.

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
| `POST` | `/sources` | Validates cron, template tokens, pipeline existence, and that the pipeline's indexer names a connection (`422` otherwise). Seals `fetch_auth`. Creates the Temporal Schedule. |
| `GET` | `/sources` | Tenant-scoped list, secrets redacted. |
| `GET` | `/sources/{uid}` | Includes `next_run_at` from Temporal `describe`. |
| `PATCH` | `/sources/{uid}` | Updates the Schedule when cron/timezone change. |
| `DELETE` | `/sources/{uid}` | Deletes the Schedule, then the row. |
| `POST` | `/sources/{uid}/pause` / `/unpause` | Temporal pause/unpause. |
| `POST` | `/sources/{uid}/run` | `trigger` — run now, off-schedule. |
| `GET` | `/sources/{uid}/runs` | Paginated `source_runs`. |
| `POST` | `/connections` | Validates host policy, health and key (see *Connection validation*). Seals `api_key`. |
| `GET` | `/connections` | Tenant-scoped list, `api_key` masked. |
| `GET` | `/connections/{uid}` | Includes `used_by`: the pipeline uids referencing it. |
| `PATCH` | `/connections/{uid}` | Re-validates when `host` or `api_key` change; omitting `api_key` keeps it. |
| `DELETE` | `/connections/{uid}` | Never blocked (Decision 15). |

`POST /ingest` changes only in the permissive direction: a pipeline whose indexer pins a
connection accepts requests with no Meilisearch context (see *`MeiliContext` becomes
optional*).

Create is not atomic across Postgres and Temporal. The row is written first with
`paused = true`, the Schedule is created, then the row is unpaused. A crash between the
two leaves a paused source with no schedule, which is visible and repairable, rather
than a schedule firing against a row that does not exist.

**A source's pipeline may be deleted underneath it.** `DELETE /pipelines/{uid}` is
**never blocked** — its behaviour is unchanged from today. Deleting a pipeline cascades
to *archive* every source that references it: the Temporal Schedule is deleted so nothing
fires again, and the row is stamped `archived_at` with its sealed `fetch_auth`
retained.

Archive rather than cascade-delete because the source row holds credentials a tenant
supplied by hand; destroying them as a side effect of an unrelated pipeline delete is
not recoverable, whereas an archived source can be repointed at a new pipeline and
unarchived. Archived sources are excluded from `GET /sources` unless
`?include_archived=true`, and never fire.

`load_source` still handles a genuinely missing pipeline (a row deleted directly in SQL,
say) by recording a `failed` run with a clear message rather than retrying forever.

## UI

`ui/src/app/sources/`, co-located components per repo convention, mirroring the existing
`pipelines` page: list with status and next run, create/edit form (react-hook-form + zod
+ shadcn, lucide icons), run history table linking each run to its jobs, and
pause/resume/run-now controls. Secrets are write-only inputs showing `****` when set.

`ui/src/app/connections/` lists and edits connections, showing `used_by` so a user sees
what a delete will break. The pipeline editor's `meili_indexer` form gains a connection
picker plus `batch_size` / `max_batch_bytes` fields, and flags a dangling connection
reference.

## Docs

- `docs/concepts/sources.mdx` — concepts, cron syntax, templating table, the TMDB
  walkthrough end to end.
- `docs/concepts/connections.mdx` — connections, precedence over the request context,
  batching, and `MEILI_CONNECTION_HOSTS`.
- `docs/openapi.yaml` — the thirteen routes above, plus the `POST /ingest` change.
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
- `inject_meili_context`: a step with `connection` keeps its own `host`/`api_key`; a
  step without one is overwritten exactly as today.
- Batching: cut on count, cut on bytes, whichever first; a single oversized document is
  a non-retryable error naming its id.
- Host policy: `public` rejects `http://meilisearch:7700` and accepts a public https
  host; an allowlist accepts only its entries; `any` accepts both.
- The indexer activity's input, as serialized for Temporal, never contains the
  connection's key.

**Workflow** (Temporal test env)
- `Unchanged` starts zero children and writes one `unchanged` run row.
- `Items` of length N starts N children with distinct job ids.
- A failing resolve records `failed` with the error and does not advance the ETag.
- Overlap: a second trigger while running is skipped.

**Integration** (`scripts/e2e.sh`)
- Create a connection to the dev Meilisearch, a pipeline whose indexer names it, and a
  source against a local fixture server → `POST /run` → job appears, documents land in
  Meilisearch → second `POST /run` → `unchanged`, no new job.
- `POST /ingest` with no `X-Meili-*` headers against that pipeline succeeds.

## Non-goals for v1

Explicitly out of scope, listed so the plan does not quietly grow:

- `zip`, `bucket` and `api` connectors (the trait exists; the impls do not).
- Mirror-sync deletion of documents that vanished upstream.
- Per-item incremental state (only whole-source ETag/hash in v1).
- Crawling / link-following.
- Webhook or event-driven triggers.
- Secret rotation.
- Extending the SSRF guard to ad-hoc `POST /ingest` URLs.
- Streaming a source fetch straight into the blob store (see `UrlConnector` step 4).
- Pinning connections on steps other than `meili_indexer` — it is the only plugin that
  talks to Meilisearch (`INDEXER_PLUGIN`).

## Open risks

**A connection's key is a long-lived, write-capable Meilisearch key held at rest.** That
is inherent to request-less execution and is accepted deliberately. The connection
redesign concentrates the risk rather than removing it: there is now one key per
destination instead of one per source, which makes it easier to rotate and audit. If
Meilisearch Cloud later exposes an internal mint-a-scoped-key authority, a connection
should hold only a project reference and mint a scoped key per run.

**Nothing stops a tenant pinning a destination they do not own**, provided they hold its
key. That is equivalent to calling that Meilisearch directly with the same key, so it is
not an escalation — but usage is metered against the *pipeline's* tenant, not the
destination's owner.

**DNS rebinding** between the host-policy check and the actual connect is unsolved on
both the fetch and the write path (see *SSRF guard*).
