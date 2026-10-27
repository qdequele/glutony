# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow SemVer.
Releases are tagged `vX.Y.Z` and build `ghcr.io/qdequele/glutony:X.Y.Z`.

## [Unreleased]

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
