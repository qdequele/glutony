# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow SemVer.
Releases are tagged `vX.Y.Z` and build `ghcr.io/qdequele/glutony:X.Y.Z`.

## [Unreleased]

### Added
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

### Fixed
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

### Changed
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
