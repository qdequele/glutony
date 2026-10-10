# Changelog

All notable changes to this project are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/); versions follow SemVer.
Releases are tagged `vX.Y.Z` and build `ghcr.io/qdequele/glutony:X.Y.Z`.

## [Unreleased]

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

### Deprecated
- `LAB_EVENTS_SECRET` (control plane): still read, but a v2 Lab does not accept events
  signed with it (they are dropped after 24 h); boot logs an error. Set
  `LAB_INSTANCE_ID` / `LAB_INSTANCE_SECRET`. Removed in the next release.

### Removed
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
