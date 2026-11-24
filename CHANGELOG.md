# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow SemVer.
Releases are tagged `vX.Y.Z` and build `ghcr.io/qdequele/glutony:X.Y.Z`.

## [Unreleased]

### Added
- Lab events tell more about how a job ended, for the Lab's emails (the vendored
  `lab-events.schema.json` follows the Lab's). `job.failed` carries a `reason` code
  (`invalid_config`, `invalid_input`, `insufficient_credits`,
  `gateway_budget_exhausted`, `no_gateway`, `destination_auth`,
  `destination_unreachable`, `destination_rejected`, `internal`), the failed `step`,
  the `index_uid` and the `pipeline`. Every job event carries `pipeline` and, for the
  jobs of a scheduled source run or the files of an archive, a `group` naming the run.
  A scheduled source of a Lab account that starts failing or runs again after failing
  sends `source.status_changed` (with a `reason` such as `source_auth`,
  `source_not_found` or `source_unreachable`, and the failed runs in a row).
  `POST /internal/source-runs` answers the change in `status_change`.
  `PipelineWorkflowInput` (plugin SDK) gains an optional `group`. See
  [Meilisearch Lab](docs/deployment/meilisearch-lab.mdx).
- Archive uploads: a zip, tar, `.tar.gz` / `.tgz` or gzipped file sent to any ingest
  route (file, URL or object ref) starts an archive job that ingests every file inside
  it as its own job, each routed by its own type and name (or through the pipeline
  forced with `?pipeline=` / `/ingest/pipeline/{name}`). Nested archives are opened up
  to three levels; folders, `__MACOSX` and `.DS_Store` are dropped; unsafe paths,
  links, empty, oversized and unroutable files are skipped and reported. The response
  reads `pipeline_used: "archive"` and `target_index: "*"` (or the one shared index),
  `GET /jobs/{id}` reports per-file counts under `progress.archive`, and
  `GET /jobs?parent_job_id=` lists the file jobs. Bounded by `ARCHIVE_MAX_FILES`,
  `ARCHIVE_MAX_FILE_MB`, `ARCHIVE_MAX_UNPACKED_MB` and `ARCHIVE_MAX_DEPTH` on the
  workers. A pipeline whose trigger names the archive type itself still gets the
  archive whole. See [Archives](docs/concepts/archives.mdx).
- Usage analytics can live in a self-hosted ClickHouse instead of Tinybird: set
  `USAGE_CLICKHOUSE_URL` (plus `_DATABASE`, `_TABLE`, `_USER`, `_PASSWORD`) on the
  workers and the gateway. The workers create the table (same columns, engine and TTL
  as the Tinybird data source) and insert the same rows; `GET /usage` reads them back
  deduplicated, with the columns of `tenant_usage` and no hourly lag. Setting both
  stores is refused at boot.
- `WASM_PLUGIN_TIMEOUT_SECS` and `WASM_PLUGIN_MAX_MEMORY_MB` cap how long one WASM
  plugin call may run and how much memory an instance may allocate. Both are off unless
  set; a call that hits either fails the step without a retry.
- `image_captioner` takes `max_bytes` (default 20 MiB, OpenAI's image limit): a larger
  image fails as invalid input before it is encoded or sent, instead of costing several
  base64 copies of itself in worker memory and then being refused by the provider. The
  request also holds one base64 copy of the image instead of three.
- Built-in plugins can checkpoint their progress (`ActivityContext::record_progress`)
  and read the previous attempt's checkpoint on a retry
  (`ActivityContext::previous_progress`). The checkpoint travels in the Temporal
  heartbeat details. No plugin uses it yet.
- Bytes fetched over HTTP (URL ingests and scheduled URL sources) keep the `charset`
  declared in their `Content-Type` as `Blob.charset`, also through staging, while `mime`
  stays the bare type. No plugin decodes with it yet.
- `MAX_INFLIGHT_UPLOAD_MB` (default twice `MAX_UPLOAD_MB`, `0` to disable) bounds the
  request body the ingest routes hold in memory at once. A request that does not fit
  within 2 s gets `503` with `code: overloaded` and `Retry-After`. One maximum-size
  upload is always admitted when the gateway is idle.
- `POST /sources/{uid}/reset-state` forgets a source's saved `ETag`, `Last-Modified` and
  content hash, so its next run fetches and ingests in full even when upstream has not
  changed. Use it after editing the steps of the source's pipeline, which does not reset
  the state on its own.
- `JOBS_RETENTION_DAYS` (control plane, off by default): an hourly pass deletes finished
  jobs and source runs older than that many days. Running jobs, and archive jobs whose
  file jobs remain, are kept.
- `ARCHIVE_INFLIGHT_MB` (worker): the most decompressed archive content held in memory
  at once, waiting to be staged.
- `meili_indexer` takes an opt-in `prune: true`. Once the additions succeed, it deletes
  the documents that share a `_meta.parent_id` with a document of the step but were not
  sent by it, such as the stale chunks of a document that got shorter. It needs
  `wait_for_completion`, a key allowed to delete documents, and `_meta.parent_id` plus
  the primary key in the index's `filterableAttributes`. It cannot be combined with a
  `fan_out` other than `$` on the indexer step (the pipeline is rejected), and a parent
  whose chunks are not all in the step (per `meta.chunk_total`) is skipped. Off by
  default.
- `json_flattener` takes a `root` option: `"none"` reads the payload as is (a record
  with an `items` or `data` field stays one document), and a JSON Pointer such as
  `"/data/orders"` names the array of records, in the payload or in each NDJSON line.
  Unset keeps the wrapper-key rule.

### Fixed
- `job.completed` sends the Meilisearch index the job wrote to as `index_uid`, not the
  pipeline uid (now in `pipeline`); a job that names no index still sends the uid.
- A cancelled job sends `job.cancelled` instead of a `job.failed` reading
  `cancelled`, with the same lifecycle id.
- A job whose pipeline does not validate when its workflow starts is reported: its
  row turns `failed` and the Lab gets `job.failed` with `reason: invalid_config`.
  It ended without any status write-back or Lab event before.
- A scheduled source's fetch error keeps its kind in the run's error
  (`retryable: fetch: GET …`, `retryable: dns: …`).
- A batch item's `index` and `pipeline` are honoured. They were dropped without a
  word, so `{"items": [{"url": …, "index": "pdfs"}]}` wrote into the default index
  (`documents`, `datasets`, …) instead; the Lab's `pipelines_ingest_batch` tool
  offers both per item. An item's own value wins over the batch's; the index of an
  `/indexes/{index_uid}/…` path still wins over both.
- A URL whose name says nothing about its type (`https://example.com/`,
  `https://api.example.com/users`) can be ingested: it was refused with
  `unsupported_media_type` (`application/octet-stream`). The gateway now reads a
  `content_type` hint on `url` and `s3` refs (JSON key or multipart field, also per
  batch item), and without one asks the URL's server (`HEAD`, falling back to a `GET`
  whose body is not read, each hop under `SOURCE_FETCH_HOSTS`).
- `jev_enricher` works on the Lab's hosted Gateway again: with `LAB_AI_GATEWAY` on it
  posted to the Gateway's `/v1/systemone`, which LUMEN removed in 0.7.0
  (`404 LM-1003`), so every Jev step of a Lab team failed. It now calls
  `/v1/decisions`, which takes the same TypeSafe body and answers in it. A direct
  TypeSafe API (`TYPESAFE_BASE_URL`) is still called on `/systemone`.
- A URL or object ref ending in `.avro`, `.msgpack` / `.mpk` (or another extension
  the router maps itself, such as `.m4a` → `audio/mp4`) routes to its built-in
  pipeline. The gateway guessed a ref's type with `mime_guess` alone, which knows
  neither, so `builtin.avro` and `builtin.msgpack` were unreachable by URL (`415`);
  it now uses the router's extension table, the one uploads go through.
- URL ingests and scheduled sources send a `User-Agent`
  (`meili-ingest/<version> (+https://github.com/qdequele/glutony)`). Without one,
  Wikimedia and several CDNs answer `403`, which failed the job's first step.
- Documents fetched from two places whose last path segment matches no longer
  overwrite each other. Generated ids (the ones made when the content has no id of
  its own) were built from the filename alone, which for a URL ingest or a
  scheduled source is the URL's last segment: `https://a.example/export/data.json`
  and `https://b.example/v2/data.json` both wrote `data_0`, `data_1`, ... into a
  shared index, and `https://httpbin.org/json` wrote `json_0`. Fetched content now
  adds a 12-character hash of its origin: `data_<hash>_0`. The origin is the URL
  without its query string for URL ingests, the URI for object refs, and the
  source's project and `uid` for scheduled sources, so re-ingesting the same URL
  or re-running a source (templated URL included) keeps replacing the same
  documents. Uploads and documents with their own id (`id_field`, `id_column`)
  keep the ids they had.
  **Upgrading**: the next run of a source, or ingest of a URL, whose documents
  have generated ids writes them under the new ids and leaves the ones written
  before in the index. Delete those (their ids have no hash: `data_0`, `report_p3`)
  or empty the index and run the source again.
- `meili_indexer` no longer writes an empty `_meta: {}` into documents that carry no
  metadata (JSON records, for instance): `_meta` is sent only when it has a value.
  Documents already indexed keep their empty `_meta` until they are re-ingested.
- The gateway's `TINYBIRD_READ_TOKEN` (and `TINYBIRD_USAGE_PIPE`), which `GET /usage`
  needs, is now documented in the environment variables reference and the usage page,
  passed to the gateway by the Kubernetes manifests (`k8s/gateway.yaml`, optional key of
  `meili-ingest-secrets`) and by `compose.yaml`. Without it the Lab's Pipelines Usage
  page reports "no usage metering configured".
- A release tag that was not made by merging a `chore(release): vX.Y.Z` PR no longer
  publishes anything: `release.yml` first checks the tagged tree
  (`scripts/prepare-release.sh --verify-tag`: the crate version is the tag's, every
  version copy agrees, and `CHANGELOG.md` has the tag's section) before it builds or
  pushes an image, moves `latest`, creates the release or notifies the Lab. `v0.2.0`,
  created from the GitHub release form on a `0.1.0` tree, would have stopped there.
- Inline JSON documents with a non-integer numeric `id` (`1.5`, `1e3`) get a sanitised
  id (`1_5`, `1000_0`) instead of failing at the indexer after the job was accepted. An
  empty `id` gets a generated one, like a missing or `null` id, instead of a misleading
  400.
- A body whose `Content-Length` exceeds `MAX_UPLOAD_MB` now gets the JSON `413
  payload_too_large` error, and a missing or malformed query parameter on `GET /usage`,
  `GET /sources` and `GET /sources/{uid}/runs` gets a JSON `400 bad_request`, instead of
  plain-text bodies.
- A JSON `documents` body (bare array, `{"documents": [...]}` or a batch item) larger
  than `INLINE_MAX_BYTES` is staged in the blob store like a file upload. It went into
  the Temporal workflow input whatever its size, so a body over Temporal's 2 MB payload
  limit failed with `502 cannot start workflow`. The first step still receives the same
  documents.
- A source update or pause/resume whose control-plane write fails puts the Temporal
  schedule back on the stored cron, timezone and paused state. Before, the schedule kept
  firing on a cron that `GET /sources/{uid}` did not show.
- `GET /jobs/{id}` reports a failed job's `error` and last `current_step` when the
  workflow cannot answer the progress query (no worker polling, for example during a
  deploy). It returned `status: failed` with no reason.
- `GET /usage` answers `502` when Tinybird fails with a long error body containing
  multi-byte characters. Truncating that body could panic and drop the connection.
- An index named by `?index=`, the `index` field, `X-Meili-Index` or a batch item's
  `index` must be a valid Meilisearch index uid when the job writes to it. A malformed
  one is refused with `400 bad_request` (failing only that item in a batch) instead of a
  queued job that fails at the indexer. Before, only the `/indexes/{index_uid}/...` path
  was checked.
- `POST /ingest/pipeline/{name}` refuses a body `pipeline` field or `?pipeline=` that
  names another pipeline, with `400 bad_request`, as it already did for a batch item. It
  ran the route's pipeline without a word.
- An upload staged in the blob store (above `INLINE_MAX_BYTES`, or any archive) is
  removed when its workflow cannot be started. It stayed in the bucket with nothing
  referencing it, and each client retry staged another copy.
- A pipeline can no longer be saved with the uid `validate`. It answers `422`, because
  `/pipelines/validate` is the validation route and such a pipeline could not be fetched
  or deleted.
- Deleting a global pipeline now also archives the tenant sources that ran on it, and
  removes their schedules. Before, these sources stayed active and failed on every run.
  A tenant with its own pipeline of the same `uid` keeps its sources.
- The job list no longer shows a finished job as queued or running. A late or repeated
  status write can no longer move a job back, and the worker now waits briefly for a job
  row that the gateway has not inserted yet.
- Uploads sent right after a pipeline create, update or delete no longer risk being
  routed with the previous pipeline list for a further 5 seconds on the same control
  plane.
- `glutony_lab_events_delivered_total` no longer counts events twice when marking them
  delivered fails. `glutony_lab_events_failed_total` now counts events for every reason,
  where some reasons counted batches before. Review any alert thresholds on the
  `connect`, `timeout`, `auth`, `status` and `malformed` reasons.
- Large fanned-out jobs no longer fail or hang: a fan-out's branch list and its merged
  branch outputs are now kept under `PAYLOAD_SPILL_BYTES` as a whole, so a document
  whose chunks total more than 2 MiB no longer exceeds Temporal's payload limit. A
  step's document count in `GET /jobs/{id}` and in usage now includes outputs that were
  moved to the blob store.
- A `$.documents` fan-out over the output of another fanned-out step now reads the
  branches that were spilled to the blob store instead of dropping them. A `$.documents`
  fan-out over an output that is not documents (raw bytes) now fails the step instead of
  succeeding with nothing indexed.
- Usage of a failed or cancelled job now includes what was already spent: steps that ran
  alongside the failed one, and the branches of a fanned-out step that completed before
  another branch failed. Failed jobs can therefore bill slightly more step time than
  before.
- Each worker request to the control plane now gives up after 15 s and is retried,
  instead of waiting forever on a control plane that accepted the connection and went
  silent. This also stops a hung control plane from blocking worker start-up.
- Archive uploads: bytes decompressed for an entry that ends up skipped now count toward
  `ARCHIVE_MAX_UNPACKED_MB`, so an archive of entries that each expand past
  `ARCHIVE_MAX_FILE_MB` (or that understate their size) is refused instead of
  decompressed in full. A zip entry larger than the size it declares is skipped as
  `unreadable: larger than its declared size`.
- Archive uploads: a `.tar.gz` / `.tgz` whose tarball is larger than
  `ARCHIVE_MAX_FILE_MB` is now read entry by entry, like a zip, instead of failing with
  `the file expands to more than … bytes`; only its oversized entries are skipped.
- Archive uploads: a cancelled archive job now records `cancelled` in its job row (the
  jobs list showed it `running` with step `unpack`).
- Archive uploads: an archive job cancelled while it unpacks or routes its files, or
  cancelled from Temporal without `POST /jobs/{id}/cancel`, now ends `cancelled` instead
  of `failed`, and the worker stops reading the archive.
- Editing a source's `location`, `timezone`, `pipeline_uid`, `index_name` or `auth` now
  forgets its saved `ETag`, `Last-Modified` and content hash, so the next run ingests
  into the new destination. Before, the run was recorded `unchanged` and the new index
  stayed empty until the upstream file changed. An edit to the pipeline's own steps
  still keeps the state.
- An invalid date format (`{{ date:%Q }}`, a trailing `%`) or an out-of-range offset in
  a source URL template now returns `422` on `POST /sources` and `PATCH /sources/{uid}`.
  Before, it panicked the handler and the client's connection was dropped.
- Typing a large single-line `.json` source file served without a usable `Content-Type`
  no longer parses the whole document into memory on the worker's async threads, and
  decompression runs off those threads.
- A retried source fetch no longer leaves a phantom `queued` job in the jobs list: a
  retry reuses the job id, staged blob and job row of the attempt it replaces. Worker
  calls to the control plane now time out after 20 s instead of holding the attempt.
- A source fetch is no longer cut off after 300 s while data is still arriving. It now
  has 10 minutes in total (redirects and body included) and is abandoned early only when
  the body stops arriving for 60 s, so large exports on slower links can be ingested.
- A `meili_indexer` step whose `index` is blank or `null` now uses the request's index
  (header, query or default) instead of failing with `no target index resolved` after
  every upstream step has run.
- Files with very long paths (for example deeply nested archive members) no longer give
  every page, row or record the same document id. A generated id's stem over 400 bytes
  is now cut and ends with a short hash of the whole path. Shorter stems are unchanged.
- The worker now decides whether to move a step output to blob storage based on its real
  serialized size, metadata and JSON escaping included. Outputs that serialized past
  Temporal's 2 MB payload limit no longer fail the step. Metered `input_bytes` are
  unchanged.
- A WASM plugin call that never returns no longer wedges the plugin and leaks worker
  threads: the call is interrupted when its step is cancelled or abandoned, returns as
  cancelled, and the next call gets a fresh instance.
- A gRPC plugin that starts after the worker (a sidecar, for example) is no longer lost
  until the worker restarts: the worker retries it for about 30 seconds at boot.
  `GetManifest` now times out after 10 seconds instead of hanging worker startup.
- An input too large for one gRPC message (64 MiB, about 48 MiB of raw bytes once
  base64-encoded) now fails the step at once, without retries, with an error naming the
  limit. Before, it was retried with an opaque h2 error. The encoding no longer blocks
  the async runtime.
- MIME detection lets the file extension refine signatures that only name a container or
  guess from the first characters: a `README.md` starting with `<div align="center">` or
  `<!-- -->` now runs `builtin.markdown` instead of `builtin.html`, a `.txt` starting
  with `#!` is plain text, an `.html`/`.svg` starting with `<?xml` keeps its type, and
  an `.xls` written without a known root CLSID (xlwt, Apache POI) routes to
  `builtin.excel` instead of being refused with 415.
- Newline-delimited JSON is detected and routed to `builtin.json`: `.ndjson`/`.jsonl`
  files and bodies with one JSON object per line are `application/x-ndjson` (also
  accepted as `application/jsonl`, `application/x-jsonlines`, `application/jsonlines`,
  `application/ndjson`) and give one document per line, instead of being chunked as
  plain text.
- Pipeline triggers match alias spellings of a content type: a pipeline on `audio/mp3`,
  `audio/x-wav`, `image/jpg`, `application/x-parquet` or `application/x-zip-compressed`
  now fires for uploads detected as `audio/mpeg`, `audio/wav`, `image/jpeg`,
  `application/vnd.apache.parquet` or `application/zip` instead of being skipped in
  favour of the built-in.
- Large uploads (over 1 MiB) and archive entries are type-detected off the gateway's and
  worker's async threads, so a big JSON upload no longer stalls other requests while it
  is sniffed.
- `GET /catalog` no longer says the Audio and Video workflows chunk the transcript: they
  index it as the transcriber emits it.
- Uploads, archive entries and source items with long non-Latin filenames no longer fail
  to stage (a 500 on the default `file://` store). The filename stays out of the storage
  key and is still passed to the pipeline.
- Usage: a job cancelled before its `meili_indexer` step no longer reports, or bills to
  the Lab, the documents of the last step that ran. `documents_indexed` and the Lab
  `documents` unit now count what the indexer steps wrote.
- Usage on self-hosted ClickHouse: if the usage table is dropped while workers run, it
  is created again. A table from an older release gets the missing columns (`ALTER TABLE
  ... ADD COLUMN IF NOT EXISTS`, which needs the `ALTER ADD COLUMN` grant). Before,
  usage rows were rejected until a restart or a manual fix.
- Usage on self-hosted ClickHouse: `GET /usage` now counts days in UTC, as Tinybird
  does, even when the ClickHouse server uses another time zone.
- `llm_enricher`, `jev_enricher` and `image_captioner` retry a rate-limited (`429`),
  failing (`5xx`) or dropped request on their own, up to 4 attempts with backoff and
  jitter, honouring `Retry-After` up to 20 s and waiting at most 30 s per request,
  before the step's retry policy takes over. One rate-limited document no longer fails
  the whole batch and re-sends every document. A timed-out request and a Lab Gateway
  `401` still go straight to the step's retry; failed attempts are not metered. See
  [Jobs](docs/concepts/jobs.mdx).
- `whisper_transcriber` measures a WAV upload without a provider duration from its
  sample rate, channels and bits per sample, and from every byte after the `data`
  header. It used the header's informational bytes-per-second field and declared data
  length, so a forged header could bill a long transcription as almost zero seconds. A
  header whose block align does not match counts the request only.
- `llm_enricher` ignores a `title` or `language` in the model's reply that is not a
  string (for example `null`). It was written into `fields` and replaced the document's
  own title in the index, even under `merge_strategy: merge`.
- `image_captioner` with `json: true` (the default) and a custom `prompt` that does not
  mention JSON no longer fails every image with HTTP 400 from OpenAI: such a prompt gets
  "Reply with a single JSON object." appended. Prompts that mention JSON, and `json:
  false`, are sent unchanged.
- `video_audio_extractor` now decodes, resamples and encodes in one streaming pass, so
  its memory follows the input file and the WAV it writes rather than the decoded audio.
  A long video no longer runs a worker out of memory. The output is unchanged.
- `html_extractor` no longer crashes the worker on deeply nested HTML: the text walk is
  iterative instead of recursing once per nesting level.
- `html_extractor` takes the `<h1>` title fallback and the `<main>`/`<article>` content
  from the page itself, not from removed boilerplate such as a site `<header>` or an
  `<aside>` teaser.
- `html_extractor` decodes pages in the charset their byte order mark or `<meta
  charset>` declares (for example windows-1252, ISO-8859-1 or Shift_JIS), instead of
  indexing them as replacement characters. Valid UTF-8 is still read as UTF-8.
- `document_script` with `on_error: keep` now emits the document untouched when the
  script's result cannot be read back, instead of a half-rewritten one.
- `document_script` keeps integers above 9223372036854775807 in `fields` as integers
  when the script does not touch them, instead of turning them into floats.
- `document_script` no longer fails large documents with `Length of string too large`:
  the string, array and map caps now limit what a script adds on top of the incoming
  document.
- A spreadsheet with a few cells far apart (a value in `A1` and one in `XFD1048576`, or
  one stray value on the last row) no longer kills the worker: `xlsx_extractor` reads
  `.xlsx`, `.xlsm` and `.xlsb` sheets cell by cell, so memory follows the cells a sheet
  holds rather than the rectangle between its outermost cells. `.xls` and `.ods` sheets
  are still loaded whole by `calamine`.
- Large spreadsheets no longer block the worker's async runtime while they are parsed,
  so other activities' heartbeats and cancellations keep running.
- `xlsx_extractor` row mode no longer drops a column whose header repeats an earlier
  one: later duplicates get the first free `_2`, `_3`, … suffix (`Name | Notes | Notes`
  → `Name`, `Notes`, `Notes_2`). The first column with a header keeps its name.
- `pdf_extractor` with `max_pages` stops reading at the cap: pages past it are no longer
  rendered, so a capped step on a long PDF is fast and is not broken by an unreadable
  later page. `max_pages` is now documented.
- `pptx_extractor` attaches speaker notes to the right slide. Notes pages are numbered
  on their own, so when an early slide had no notes, every later slide got its
  neighbour's notes. Notes now follow the slide's relationship part. Decks already
  indexed keep the old notes until they are re-ingested.
- Chunk ids stay within Meilisearch's 511-byte limit: when `<parent_id>_<index>` would
  be longer, the parent id is shortened and followed by a short hash of it, instead of
  producing an id Meilisearch rejects and failing the whole batch. `meta.parent_id`
  keeps the full parent id.
- Uploaded files and members of uploaded archives whose folder path and name come to
  more than 400 characters no longer give every page or section the same id, where each
  overwrote the previous one. The id stem is shortened and ends with a short hash of the
  full path. Ids of such long paths change, so re-ingesting one leaves its old documents
  behind; shorter names keep their ids.
- Avro decimals inside a named type that the schema uses a second time (by name, through
  a `["null", "Money"]` union, or as array or map items) keep their scale. They were
  indexed unscaled, so `2.80` came out as `280`.
- The CSV delimiter is sniffed from the first records, ignoring quoted fields, as the
  one that gives the most consistent number of fields. Before, a semicolon, tab or pipe
  file whose header had as many commas as delimiters (a quoted `"Last, First"` or an
  `Address, City` column) was split on commas and every row landed in one mangled
  column.
- `json_flattener` keeps a document's own `content` when it gets documents (inline JSON
  documents, or the output of an earlier step) and `content_fields` is not set. Before,
  any other string field, such as an `author`, replaced the body.
- The OpenAPI contract describes what the gateway returns: - A batch entry can be an
  `{error, code}` object. - `/ingest/pipeline/{name}` and
  `/indexes/{index_uid}/ingest/pipeline/{name}` accept an `items` batch and answer
  `{jobs: [...]}`. - On `GET /jobs/{id}`, `progress` can be `null` and `pipeline_used`
  can be absent. - `POST /sources` answers `422` (not `404`) for an unknown pipeline. -
  `?pipeline=`, `?filename=`, the `404` on `/ingest` for an unknown `?pipeline=`, and
  the `version`, `tenant_id` and `features` fields of `/health` are declared.
- Docs: - The deployment docs no longer tell operators to use Temporal Cloud with mTLS
  or an API key. The gateway and workers connect to Temporal without TLS or
  authentication, so Temporal Cloud is not supported yet. - The Kubernetes verify step
  sends `X-Meili-Envoy-Secret`. - The "production image locally" example sets
  `CONTROL_PLANE_TOKEN` and shares a blob volume. - `CORS_ALLOW_ORIGINS`, `LOG_FORMAT`
  and `TINYBIRD_TIMEOUT_SECS` are documented.
- The credit pre-check's 10 s pause after a failed Lab call no longer covers every
  account when the failure is about one account. An answer the gateway cannot read, or a
  `4xx` other than `401`, `403` and `429`, pauses lookups for that account only; the
  other accounts were refused with `503 lab_unavailable` during that window. An
  unreachable Lab, `401`, `403`, `429` and any `5xx` still pause every account.
- AI steps on the team's Gateway no longer wait for the Lab's timeout on every lookup
  while the Lab hangs. After a failed call the control plane does not ask the Lab for an
  account's Gateway target again for 10 s, with `?refresh=true` too: lookups in that
  window get the cached target or `503 lab_unavailable` straight away. A refused key
  (`?rejected_key_id=`) is still reported to the Lab.
- WASM plugins now see the real job id, step id and attempt: `ctx.job_id()`,
  `ctx.step_id()` and `ctx.attempt()` used to return the nil UUID, `"test"` and 1. The
  `execute` envelope carries `job_id`, `step_id` and `attempt` as additive fields. Usage
  recorded inside a guest is still not billed. The WASM authoring guide now gives the
  `{"ok"|"error"}` reply the worker actually expects.
- A step with several `depends_on` no longer receives a merged input larger than
  Temporal's payload limit: the outputs it merges now share `PAYLOAD_SPILL_BYTES`, like
  the branches of a fanned-out step.
- Lab events that never got an answer from the Lab (unreachable, `401`, `5xx`,
  unreadable answer) are retried for 7 days instead of being dropped after 24 h, so an
  outage or a credential problem shorter than that loses no usage event. An event the
  Lab skipped (a `200` that does not accept it) is still dropped, 24 h after its first
  skip.
- With `LAB_AI_GATEWAY=true`, an AI step (`llm_enricher`, `image_captioner`,
  `jev_enricher`) of a Lab account no longer falls back to the worker's own provider key
  when the control plane serves no Gateway target (for example a replica without
  `LAB_INSTANCE_*`). The step fails retryably and never runs on the worker's key.
- Spilled step outputs no longer overwrite each other when step ids look alike (a
  fan-out step `enrich` and a step `enrich-0`, or a step `x-fanout` and the fan-out of
  `x`). Each spilled object now has its own key under `jobs/<job>/steps/<step>/`. Jobs
  in flight keep reading their old keys.
- `s3_downloader` steps now run on `workers-general` instead of `workers-io`, a queue
  the Kubernetes manifests never poll, so such a job no longer stays `running` forever.
- Archive uploads can no longer run a worker out of memory: a worker expands one archive
  at a time and holds at most `ARCHIVE_INFLIGHT_MB` (new, default 512, never below
  `ARCHIVE_MAX_FILE_MB`) of its decompressed files at once. Other archive jobs wait
  their turn. The docs now state the real peak memory (archive + budget + one file per
  nested level).
- Staged uploads, source items, archive files and spilled step outputs are no longer
  kept forever: workers delete everything a job left under `jobs/<job_id>/`
  `BLOB_TTL_DAYS` (new, control plane, default 7; `0` keeps them) after the job ended.
  For an archive, this waits until all its file jobs have ended. Workers on S3 now need
  `s3:ListBucket` and `s3:DeleteObject` on `jobs/`. A bucket lifecycle rule on `jobs/`
  is documented as the backstop.
- A retried `meili_indexer` step no longer sends again the batches its previous attempt
  already enqueued. After a task wait timeout or a 5xx on a later batch, it waits on
  those Meilisearch tasks and only sends the rest. It falls back to sending everything
  when the documents, the destination or the tasks no longer match.
- When `llm_enricher` or `jev_enricher` fails on one document and Temporal retries the
  step, the retry resumes from the documents already enriched instead of sending the
  whole batch to the provider again. Their results travel in the step's heartbeat
  details, capped at 256 KiB, and documents past the cap are redone. Failed attempts are
  not billed, and a resumed document is not billed twice.
- `video_audio_extractor` no longer reports `audio_seconds`. `whisper_transcriber`
  downstream reports the seconds it was billed for, so a video's audio was counted twice
  in the usage rollups, the tenant usage endpoint and the Lab usage event. Usage already
  recorded for video jobs stays doubled.
- `whisper_transcriber` splits an uncompressed WAV larger than `max_bytes` into windows
  that fit and sends them one after the other, so the built-in `video` pipeline now
  transcribes videos longer than about 13 min 39 s. The transcript reads as one request:
  the same document id, segment timestamps offset to the whole recording,
  `chunk_index`/`chunk_total` counted across windows, and `audio_seconds` for the whole
  length (one external request per window). Compressed audio over `max_bytes` is still
  rejected.
- `parquet_parser` writes `DECIMAL` columns wider than 15 digits as exact strings
  (`"12345678901234567890.1234567890"`), as `avro_parser` does, instead of rounding them
  through a float. Narrower decimals stay numbers. Wide decimal ids no longer collapse
  into one.
- `csv_parser` keeps cells that look like identifiers as strings: a leading `+`
  (`+33612345678`), integers too wide for 64 bits and exponent notation (`1E5`) are no
  longer turned into different numbers.
- `csv_parser` detects the file's character encoding (byte order mark, UTF-8, the
  server's declared charset, else windows-1252) and transcodes it, so an Excel export in
  windows-1252 no longer fails the whole job. A new `encoding` option forces an
  encoding.
- Field-name collisions no longer drop values: duplicate CSV headers get `_2`, `_3`, …
  suffixes, a CSV `_row` column keeps its value (the row number moves to `_row_2`), and
  JSON keys that flatten alike, or MessagePack keys that render alike, get suffixes too.
- `html_extractor` also decodes with the `charset` of the HTTP `Content-Type` a page was
  fetched with (URL ingests and scheduled sources). Following the HTML standard, it
  ranks above a `<meta charset>`. A legacy page served as `text/html;
  charset=windows-1252` or `Shift_JIS` without a `<meta>` declaration is no longer
  indexed as replacement characters. Valid UTF-8 and a byte order mark still win.
- Files and spreadsheet sheets with non-ASCII names no longer share document ids:
  `報告.pdf` and `計画.pdf`, or sheets `売上` and `在庫`, used to overwrite each other. A name
  containing non-ASCII characters now ends with `_` and a 12-character hash of the
  original. Ids of such names change once, so re-ingesting them leaves the old documents
  in the index until you delete them. Pure-ASCII names keep exactly the ids they had.
- `avro_parser` refuses a truncated or malformed record or block (`invalid Avro record
  N: …`, `invalid Avro block: …`) instead of indexing nulls for what it could not read:
  each record is now measured from its bytes before it is decoded, so that
  `PARSER_MAX_DECODED_MB` holds for a single huge record too.

### Changed
- The vendored `lab-events.schema.json` follows the Lab's: it adds `sync.health_changed`, an
  event only Connect sends. Nothing glutony sends changes.
- Document ids built from a filename keep its folders: `reports/eu/q3.pdf` gives
  `reports_eu_q3` instead of `q3`, so same-named files in different folders of an
  archive no longer overwrite each other. Plain filenames (every upload, URL and object
  ref so far) give the same ids as before; only a `filename` field naming folders
  changes. Applies to every extractor and parser plugin.
- Gateway model ids are `provider/model` (`openai/gpt-4o-mini`): the Lab AI target
  fixtures use that form, and the provider cost table prices a prefixed id like its
  bare model (an exact `[plugin."provider/model"]` entry still wins). The docs of
  `llm_enricher` and `image_captioner` say a step's `model` uses that form behind the Lab.
- The pipeline examples (`config/pipelines/`, the marketplace `llm_enricher` template,
  README, SPEC, OpenAPI) use `openai/gpt-4o-mini`. Self-hosted, AI plugins calling a
  vendor's public API directly (`llm_enricher`, `image_captioner`, `jev_enricher`,
  `whisper_transcriber`) drop that vendor's own `provider/` prefix (`openai/gpt-4o-mini`
  → `gpt-4o-mini` on `api.openai.com`, `typesafe/jev` → `jev` on `api.typesafe.ai`), so
  the same pipeline runs behind the Lab and standalone. Bare ids, and ids sent to any
  other host (a proxy, vLLM, a self-hosted Gateway), are unchanged.
- The gateway holds a raw or multipart upload once in memory instead of twice, and no
  longer deep-copies JSON `items`/`documents` while parsing them.
- `POST /internal/resolve` no longer copies every tenant's pipelines on each upload, and
  an expired pipeline cache is reloaded with one query instead of one per in-flight
  upload.
- On a worker without `LLM_API_KEY` / `TYPESAFE_API_KEY` that serves Lab teams,
  `llm_enricher`, `jev_enricher` and `image_captioner` reuse one HTTP connection pool to
  the Lab Gateway across steps, instead of a new client and TLS handshake per step.
- `meili_indexer`, `pdf_extractor`, `docx_extractor` and `xlsx_extractor` no longer copy
  the whole input in memory before processing it, and `pdf_extractor` with `per_page:
  false` parses the file once instead of twice. The output is unchanged.
- The CSV and JSON parsers run off the worker's async runtime. A large upload no longer
  stalls other activities or the activity keepalive. The Avro, MessagePack and Parquet
  parsers no longer copy the whole payload before decoding it.
- Kubernetes: the `meili-gateway` and `workers-general` Deployments no longer set
  `replicas`; the HPA and the KEDA `ScaledObject` own the count, so `kubectl apply -k
  k8s/` no longer scales them back to 2 and 1. On a cluster first deployed with
  `replicas`, remove it from the last applied configuration before the first apply
  (`kubectl apply edit-last-applied`), or expect a one-off dip to one pod. See
  [Kubernetes](docs/deployment/kubernetes.mdx), "Upgrades".
- With the Lab seams on (`LAB_URL` / `LAB_INSTANCE_*` on the control plane), creating or
  validating a global pipeline whose `meili_indexer` step pins a `connection` answers
  `422`. A tenant job only resolves its own connections, so global pinned pipelines are
  standalone-only; pin connections in tenant pipelines.
- Creating a source whose `uid` is held by an archived source of the tenant now answers
  `409 conflict`. The message names the archived source and its deleted pipeline;
  archived sources keep their `uid` until deleted. The answer was a `422` that read like
  a live duplicate, and the gateway now passes the control plane's `409` through instead
  of a `502`.
- `GET /jobs` counts `total` up to 10 000 only, so a page request no longer scans a
  tenant's whole history; `total: 10000` means "10 000 or more".
- Boolean environment variables (`WRITE_PREFLIGHT`, `OBJECT_REF_AMBIENT_CREDENTIALS`,
  `LAB_EVENTS_ENABLED`, `LAB_AI_GATEWAY`, `CONTROL_PLANE_TOKEN_DISABLED`) are parsed the
  same way by the gateway, the workers and the control plane: `true`/`false`, `1`/`0`,
  `yes`/`no` or `on`/`off`, in any case. Any other value is `false` with a warning
  naming the variable. `WRITE_PREFLIGHT=1` no longer stops the gateway, and an
  unrecognised `OBJECT_REF_AMBIENT_CREDENTIALS` no longer stops the worker.
- `meili_indexer` with `auto_create_index: false` now fails the step (not retried),
  naming the index, when the target index does not exist, and sends no documents.
  Before, the additions still went out and Meilisearch created the index anyway. This
  replaces the earlier note that 'a missing index is still not created'.
- `llm_enricher` and `image_captioner` drop reply keys that start with `_` (`_geo`,
  `_vectors`, …), so a malformed key from the model can no longer make Meilisearch
  reject the whole batch. **A pipeline whose prompt asks for `_geo` (or any other `_`
  key) must now list it in the new `allow_reserved` option, e.g. `allow_reserved:
  ["_geo"]`, or the key is no longer indexed.**
- `llm_enricher` and `image_captioner` no longer fail the step when the provider
  declines a document (`finish_reason: content_filter`, a refusal, or an empty reply).
  The document is indexed un-enriched with `fields.llm_declined` set to
  `content_filter`, `refusal` or `empty`, and the job goes on. The new `on_refusal:
  fail` step option restores the old behaviour. HTTP errors (400, 401, 402, 403) still
  fail the step as before.
- `video_audio_extractor` caps decoding at four hours by default. With
  `max_duration_secs` unset or `null`, longer audio now fails the step with a
  non-retryable error naming `max_duration_secs` instead of being decoded without limit.
  An explicit `max_duration_secs` still keeps the first that many seconds and can be
  larger than four hours.
- Docs: files that differ only by extension (`guide.md` and `guide.html`, `data.csv` and
  `data.parquet`) share document ids in one index, and the one indexed last wins. The
  docs now say so and list the workarounds. They also no longer promise manifest
  overrides through `EXTERNAL_PLUGINS`, and they state the fixed ~48 MiB input limit of
  gRPC plugins.
- The Kubernetes manifests now deploy the release they ship with:
  `k8s/kustomization.yaml` pins `newTag` to `X.Y.Z` instead of `latest`, which nodes
  kept cached under `imagePullPolicy: IfNotPresent`. The release script writes it and CI
  fails when it drifts; apply the tree of the release tag you deploy.

### Added
- Automated releases: `scripts/prepare-release.sh` bumps the crate version
  (`Cargo.toml`, its pinned path dependencies, `Cargo.lock`) and
  `docs/openapi.yaml` `info.version` and folds `Unreleased` into a dated
  section in one step. The "Prepare release" workflow opens the
  `chore(release): vX.Y.Z` PR; merging it tags the commit and runs
  `release.yml`, whose GitHub release notes now come from this file, and which
  notifies `meilisearch/lab` (`glutony-released`) when `LAB_DISPATCH_TOKEN` is
  set. CI's new `version sync` job fails when the copies disagree.
- `LAB_AI_GATEWAY` (workers): behind the Lab, the AI steps of Lab accounts
  (`llm_enricher`, `image_captioner`, `jev_enricher`) run on the team's hosted
  Gateway with a per-team key, fetched before each step from the control plane's
  new `GET /internal/lab/ai/{account_id}`, which asks the Lab with its
  `LAB_INSTANCE_*` credentials. The key stays in the worker's memory, never in
  Temporal history or logs. A key the Gateway refuses (`401`) is reported so the
  retry gets a new one, and retries ask the control plane to refresh its cached
  target (`?refresh=true`); a spent Gateway budget (`402`) or a team without a
  Gateway fails the step. Those steps report no provider cost: the Gateway bills the
  team. With the flag on, the AI plugins register without `LLM_API_KEY` /
  `TYPESAFE_API_KEY`. The route refuses (`403 token_required`) while the control
  plane runs with `CONTROL_PLANE_TOKEN_DISABLED=true`.
- `POST /pipelines/validate` returns `warnings` naming every key of the body that the
  definition does not know and a save would drop silently (a `match:` block instead of
  `trigger:`, a misspelled step or `retry` field). Saving is unchanged.

### Changed
- **Breaking default:** tenant object refs (`{"s3": "s3://…"}`, `gs://`, `az://`)
  are read anonymously (unsigned, public objects only). They were read with the
  worker's ambient cloud credentials, which on a shared deployment can read the
  blob store where every tenant's uploads are staged. Set
  `OBJECT_REF_AMBIENT_CREDENTIALS=true` on the workers to sign them again, ideally
  with `OBJECT_REF_BUCKETS`. For S3 the bucket's region is looked up before an
  anonymous or signed read (skipped with a custom `AWS_ENDPOINT`).
- A ref to the blob store's own bucket is always refused: `422` at the gateway, a
  non-retryable step failure at the worker. `OBJECT_REF_BUCKETS` (comma-separated
  bucket names) restricts refs to an allowlist, on the gateway and the workers.
- Workers wait `SHUTDOWN_GRACE_SECS` (default 60, was a fixed 30) after `SIGTERM`
  before cancelling running activities, which Temporal then retries elsewhere; a
  plugin that ignores the cancellation is abandoned 10 s later, and the process
  exits at the latest 30 s after the grace.
- Release images are multi-arch (`linux/amd64` and `linux/arm64`), built on native
  runners; CI builds both.

### Fixed
- `docs/openapi.yaml` matches the gateway again: `POST /ingest/batch` takes
  `index` and `pipeline` once for the batch (per-item ones were never read, yet the
  example showed one), the JSON ingest body documents `pipeline` and no longer lists a
  `content_type` it never read, inline documents need no `id` (one is generated), and
  `POST /jobs/{job_id}/cancel` answers `{job_id, status: "cancelling"}`, not a job.
- The pipelines docs: only the first step runs on the job's initial input
  (`depends_on: []` is the same as leaving it out; "list root steps first" did not make
  two roots possible), and the built-in audio and video pipelines also take
  `audio/flac` and `video/x-matroska`.
- `meili_indexer` no longer sends `?primaryKey=<primary_key>` on document additions
  to an index that already has a primary key. An index created elsewhere with another
  key (`uid`, as Scrapix crawls create them) rejected every addition with
  `index_primary_key_already_exists`, failing the job even when the documents carried
  that field. The index's own key is now used (logged at info); `primary_key` still
  applies to indexes the indexer creates and to existing ones without a key yet. The
  index is now looked up with `auto_create_index: false` too, so the same rule
  applies; a missing index is still not created.
- `s3://` blob stores (and object refs) now read the standard `AWS_*` variables:
  `AWS_REGION` (requests always went to `us-east-1`), `AWS_ENDPOINT`, static keys,
  web identity and the ECS task role (`AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`).
  Only the EC2 instance profile worked before, so a blob store on ECS, Fargate or
  IRSA could not authenticate. `gs://` and `az://` stores read `GOOGLE_*` and
  `AZURE_*` likewise.

### Removed
- The admin UI (`ui/`), the gateway's `ui` cargo feature, its `/ui` routes and the
  `features.embedded_ui` field of `GET /health`. The Meilisearch Lab console is
  glutony's UI; the gateway is API-only. The image no longer has a Node build stage,
  and the dev stack no longer runs a `ui` service or sets `CORS_ALLOW_ORIGINS`.

### Security
- The gateway refuses a pipeline name that is not a pipeline uid (`[a-zA-Z0-9._-]+`) on
  `POST /ingest/pipeline/{name}`, its `/indexes/{index_uid}/...` alias and `GET`/`DELETE
  /pipelines/{uid}` with `400 bad_request`, and always sends the name to the control
  plane as one encoded path segment. A percent-encoded `?`, `#` or `../` could rewrite
  the tenant scope or the path of the authenticated control-plane request.
- `X-Meili-Envoy-Secret` is compared with `ENVOY_TRUSTED_HEADER` in constant time, like
  the management tokens.
- A bearer equal to `ADMIN_API_KEY` or `LAB_SERVICE_TOKEN` on an ingest route is no
  longer used as the Meilisearch API key, so it is not sent to Meilisearch or stored in
  the workflow input. `MEILI_API_KEY` (or `400 missing_context`) applies instead. If you
  used the same value for `ADMIN_API_KEY` and your Meilisearch key and relied on the
  bearer, set `MEILI_API_KEY` or send `X-Meili-Api-Key`.
- A Meilisearch key sent with a request to a pipeline pinned to a connection is dropped
  before the job starts, so it is no longer written to Temporal history. This includes
  archives forced through such a pipeline and, file by file, archives routed by type.
- The default `public` fetch host policy now refuses reserved, benchmarking and
  documentation ranges (`0.0.0.0/8`, `192.0.0.0/24`, `198.18.0.0/15`, `240.0.0.0/4`, …).
  It judges an IPv4 address inside an IPv6 one (NAT64 `64:ff9b::/96`, 6to4,
  IPv4-compatible) as that IPv4 address, and it classifies IPv6 literal URLs instead of
  failing them as DNS errors. Use `SOURCE_FETCH_HOSTS` to allow such a range on purpose.
- A pipeline's `trigger.filename_pattern` is now matched in linear time, so a pattern
  with many `*` can no longer stall routing on the control plane. Creating or updating a
  pipeline with a pattern over 256 characters is rejected with a validation error
  (`422`). A pipeline already stored over that limit keeps routing and running jobs, but
  its next edit must shorten the pattern. A filename over 1024 bytes matches no
  `filename_pattern`.
- A staged upload is never read back as a step output, whatever MIME its source or
  multipart part claims. An upload labelled `application/vnd.meili-ingest.output+json`
  could make a worker read other blob-store keys or load one large object many times
  over.
- `{"url": …}` and `{"s3": …}` ingest bodies are capped by the workers' new `REF_MAX_MB`
  (default 512 MiB). A larger target now fails the step as non-retryable instead of
  exhausting the worker's memory. URL fetches also give up, retryably, after 30 s
  without a connection or 60 s without data.
- An `adl://` or `azure://` object ref with a user (`azure://name@container/key`) is
  refused with a 422. Those schemes read the container from the host, so the user label
  slipped past `OBJECT_REF_BUCKETS` and the refusal of the blob store's own container.
- `document_script` discards `print` and `debug` output instead of writing tenant text
  to the worker's stdout.
- `document_script` budgets are bounded: `max_ops` is clamped to 100,000,000,
  `timeout_ms` to 30,000, and the step has a one-hour budget. Scripts run off the async
  runtime and stop mid-run when the job is cancelled.
- `docx_extractor` and `xlsx_extractor` refuse zip bombs. A `.docx` is limited to 64 MiB
  per decompressed XML part and 256 MiB per file (images included). A zipped workbook
  (`.xlsx`, `.xlsm`, `.xlsb`, `.ods`) is limited to 256 MiB per part and 512 MiB per
  file. A file past either limit fails the step without a retry, instead of exhausting
  the worker.
- `PATCH /connections/{uid}` no longer sends the stored Meilisearch key to a new host:
  moving `host` to another origin (scheme, host or port) without a new `api_key` now
  answers `422`. A path-only change, a trailing slash or a different letter case keeps
  the stored key.
- `PATCH /sources/{uid}` no longer carries a stored fetch credential to a new host:
  moving the `location` to another origin while a credential is stored now answers `422`
  unless `auth` is sent too (a new credential, or `null` to drop it). A path-only change
  keeps it.
- The control plane now requires `CONTROL_PLANE_TOKEN` on every route except `/health`
  and `/metrics` when a token is configured. `/pipelines`, `/pipelines/validate`,
  `/pipelines/{uid}`, `/jobs` and `/plugins` on port 9000 answered without it. Anything
  that calls them on port 9000 directly now needs `Authorization: Bearer
  $CONTROL_PLANE_TOKEN`; the gateway and workers already send it.
- A URL refused by a `SOURCE_FETCH_HOSTS` allowlist no longer tells a tenant which hosts
  are allowed. The ingest and source `422`, the job error and a scheduled source run's
  error (its `last_error`, including a redirect to a host that is not allowed) say only
  that the host is not allowed. Standalone callers still see the list, and the worker no
  longer prints the policy's internal representation.
- Tenant documents (raw uploads and extracted text) no longer stay readable in the blob
  store indefinitely after their job: they are deleted `BLOB_TTL_DAYS` after it ended.
- `parquet_parser`, `avro_parser` and `msgpack_parser` now decode at most
  `PARSER_MAX_ROWS` rows (default 1,000,000) and `PARSER_MAX_DECODED_MB` of decoded data
  (default 512) per step. A file over either fails its step without a retry instead of
  exhausting the worker's memory. A step's `max_rows` / `max_records` can only lower the
  row count. Before any row of a Parquet file is decoded, its row and value counts are
  checked, so a few-KB file whose one list cell holds millions of values is refused up
  front. Its `batch_size` is capped at 8192 rows.

## [0.1.0] - 2026-10-11

### Added
- Lab platform contract v2: per-instance credentials (`LAB_INSTANCE_ID`,
  `LAB_INSTANCE_SECRET`), `X-Lab-Instance-Id` / `X-Lab-Timestamp` / `X-Lab-Signature`
  on event batches, `GET /internal/instances/me` at boot.
- `usage.recorded` events carry raw units (`documents`, `bytes_in`, `step_seconds`,
  `llm_tokens_in`, `llm_tokens_out`, `audio_seconds`, `ocr_pages`) and
  `provider_cost_micro_usd`; `job.completed` / `job.failed` events per job.
- Hosted deployments refuse jobs and manual source runs (`POST /sources/{uid}/run`)
  for a Lab account without credits (`402 insufficient_credits`). After a failed Lab
  call the gateway does not ask again for 10 s.
- Scheduled source runs of a Lab account are pre-checked too: before fetching anything
  the worker asks the control plane (`GET /internal/lab/credits/{account}`), which
  asks the Lab with its own cache, under the same rules as the gateway. A run for an
  account without credits is recorded as failed with the reason and retried on the
  next tick; when the Lab cannot be asked the run fails closed. Upgrade the control
  plane before the workers.
- The binaries refuse to boot on the example placeholder `CHANGE_ME` for
  `CONTROL_PLANE_TOKEN`, `ADMIN_API_KEY` and `ENVOY_TRUSTED_HEADER`; the gateway
  refuses `LAB_INSTANCE_*` without `ENVOY_TRUSTED_HEADER`, and both Lab clients
  refuse credentials minted for another product.
- `503 lab_unavailable` when a hosted deployment cannot check credits (before the Lab
  confirms the credentials, or past the 300 s stale window).
- `CONTROL_PLANE_TOKEN`: the control plane's `/internal/*` routes require a bearer
  token; the gateway and workers present it.
- Release workflow publishing `ghcr.io/qdequele/glutony` on `v*` tags; `CONTRIBUTING.md`,
  `SECURITY.md`.
- `glutony_lab_events_dropped_total` metric.

### Changed
- Tenant jobs may only use their own Meilisearch connections; a request with a
  tenant never falls back to `MEILI_URL` / `MEILI_API_KEY`.
- Lab events unacknowledged for 24 h are dropped with an error log (they were
  retried forever).
- Kubernetes manifests: control plane runs 2 replicas; the `ConfigMap` carries the
  Lab variables; the image is `ghcr.io/qdequele/glutony`. The `Secret` is no longer
  applied by `kubectl apply -k k8s/` (re-applying reset it to `CHANGE_ME`): create it
  once; `k8s/secrets.example.yaml` lists every key, including `LAB_SERVICE_TOKEN`.
- `LAB_URL` must be a bare base URL (no path, query or fragment); surrounding
  whitespace is ignored.
- Tinybird is documented as analytics only; the Lab bills.
- Lab events still undelivered from a pre-v2 release are converted to the v2
  `usage.recorded` shape on upgrade (migration 0005) and get a fresh 24 h delivery
  window; they carry no job lifecycle event. Pre-v2 events that workers not upgraded
  yet post during the rollout are converted as they are stored, or before they are
  sent when an old control-plane replica stored them.

### Removed
- `LAB_EVENTS_SECRET` (control plane): glutony has no working legacy Lab route (a v2
  Lab attributes legacy batches to Scrapix), so the control plane refuses to boot
  while it is set. Remove it and set `LAB_INSTANCE_ID` / `LAB_INSTANCE_SECRET`.
- References to `meili-ingest-plugin-whisper` / `-ffmpeg` sidecar images that nothing
  built.

### Fixed
- A Lab `usage.recorded` event whose job had an unpriced provider call now bills the
  priced calls' `provider_cost_micro_usd` (it was sent as 0) and counts the missing
  calls in a new `unpriced_provider_calls` unit; the worker logs a warning.
- `ocr_pages` in Lab `usage.recorded` events counts the job's `ocr` plugin steps (their
  reported pages, else their output documents); it was always 0.
- The worker logs an error at boot listing every registered provider plugin
  (`llm_enricher`, `jev_enricher`, `image_captioner`, `whisper_transcriber`) with no
  entry (or only zero prices) in the provider cost table; the bundled tables now say
  loudly that their prices are placeholders and carry a commented
  `[jev_enricher.default]` template that stops the worker at boot until it is filled in.
