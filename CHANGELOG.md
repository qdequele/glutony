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
- Hosted deployments refuse jobs for a Lab account without credits (`402
  insufficient_credits`).
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
- Kubernetes manifests: control plane runs 2 replicas; `Secret` and `ConfigMap`
  carry the Lab, admin, source-secret and control-plane-token variables; the image
  is `ghcr.io/qdequele/glutony`.
- Tinybird is documented as analytics only; the Lab bills.

### Deprecated
- `LAB_EVENTS_SECRET` (control plane): still accepted with a warning, removed in the
  next release.

### Removed
- References to `meili-ingest-plugin-whisper` / `-ffmpeg` sidecar images that nothing
  built.
