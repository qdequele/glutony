# CLAUDE.md

This file provides guidance to Claude Code when working with code in this repository.

## Project

glutony (crate and binary prefix `meili-ingest`) is an ingestion pipeline for
Meilisearch: content goes in over HTTP (PDF, Office, HTML, Markdown, CSV, JSON,
Parquet, images, audio, video, …), runs through a DAG of plugin steps
orchestrated by Temporal, and lands in a Meilisearch index. It runs standalone
and as the **Pipelines** data plane of the Meilisearch Lab (`../meilisearch-lab`).

100% Rust (edition 2024, Rust 1.94+). The gateway is API-only: the old admin
`ui/` was removed (any leftover `ui/` folder is untracked build output). Its
features belong in the Lab console.

## Architecture

Three binaries, one image (`ghcr.io/qdequele/glutony`):

| Binary | Crate | Role |
|---|---|---|
| `meili-ingest-gateway` | `crates/gateway` | Public axum API. Resolves the tenant `MeiliContext`, detects MIME, picks a pipeline, stages large payloads, starts the Temporal workflow. |
| `meili-ingest-control-plane` | `crates/control-plane` | Internal API + Postgres (`migrations/`). Pipeline registry, routing, connections, sources, plugin manifests, job cache, Lab events outbox, Lab AI targets and credits. |
| `meili-ingest-worker` | `crates/worker` | Temporal worker. `PipelineWorkflow` + one `execute_step` activity per step; `SourceRunWorkflow` for scheduled sources. Polls one `TASK_QUEUE`. |

Library crates:

- `plugin-sdk`: the `Plugin` trait and shared types (`MeiliContext`, `PipelineDefinition`, `Document`, `PluginInput/Output`, `ActivityContext`).
- `router`: MIME detection, MIME/filename → pipeline resolution, built-in pipeline table, task-queue mapping.
- `blob`: stages uploads and spills large step outputs to object storage.
- `plugin-runtime`: WASM (extism) and gRPC (`proto/plugin.proto`) plugins behind the same trait.
- `source`: scheduled URL sources (cron, ETag/Last-Modified/hash skip).
- `usage`: per-job usage events to Tinybird (`tinybird/`), deduplicated, at-least-once.
- `lab`: the Lab client (per-instance credentials, signed event batches, account lookups, AI Gateway targets; platform contract v2).
- `crates/plugins/*`: built-in plugins. `llm-enricher` is the reference example.

### Invariants

- The tenant `MeiliContext` is resolved **once** at the gateway and carried unchanged through the workflow. Workers never read Meilisearch credentials from env; only `meili_indexer` touches Meilisearch. A pinned `connection:` keeps the key out of Temporal history.
- Every owned row (pipeline, source, connection, job) belongs to a tenant or the global scope. Tenant pipelines win over global ones, which win over built-ins.
- `X-Meili-*` headers are trusted only with a matching `X-Meili-Envoy-Secret` (`ENVOY_TRUSTED_HEADER`).
- Lab seams (tenants, `LAB_SERVICE_TOKEN` management auth, signed usage events, `LAB_AI_GATEWAY`) are off unless their `LAB_*` vars are set. Standalone must keep working with none of them.
- With `LAB_AI_GATEWAY=true`, Lab tenants' AI steps (`llm_enricher`, `image_captioner`, `jev_enricher`) go through the team's hosted Lumen with a per-team `lab-pipelines` key, never the worker's own key. A 401 is retryable (the Lab rotates the key); 402/403/`no_gateway` are permanent.

## Dev

```bash
docker compose watch                 # Postgres, Temporal, Meilisearch + the 3 services, hot reload
./scripts/e2e.sh                     # full-stack check from the working tree (needs docker, temporal CLI, jq, python3)
```

OrbStack URLs: gateway `http://gateway.meili-ingest.orb.local:8080`, Temporal UI
`http://temporal.meili-ingest.orb.local:8233`, Meilisearch
`http://meilisearch.meili-ingest.orb.local:7700` (key `masterKey`).

Running it as the Lab's hosted Pipelines engine: from `../meilisearch-lab`, use
`just pipelines-dev` / `just pipelines-usage` / `just demo-seed`.

Before pushing:

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
DATABASE_URL=postgres://postgres:dev-password@localhost:5432/postgres cargo test -p meili-ingest-control-plane
```

Control-plane DB tests skip without `DATABASE_URL`. No `protoc` needed (`protox`).

## Rules

- **New route** → add it to `ROUTES` in `crates/gateway/src/routes.rs`, to `lib.rs`, and to `docs/openapi.yaml`. A test checks the spec against the router.
- **`contracts/vendor/` is never edited by hand.** The Lab owns `lab-events.schema.json`: change it in `../meilisearch-lab/contracts/`, then copy it (see `contracts/vendor/lab/README.md`). CI fails on drift.
- **Tinybird column names never change.** A rename means a new datasource plus a backfill.
- Secrets show as `<redacted>` in `Debug` and are never logged. Compare tokens in constant time (`subtle`).
- Every behaviour change gets a test, plus a `CHANGELOG.md` entry under `Unreleased` when users can see it.
- Docs are Mintlify in `docs/` (`mint.json`). Update them with the feature; env vars go in `docs/deployment/environment-variables.mdx`.
- Conventional commits (`feat(scope): …`, `fix(scope): …`, `docs: …`, `ci: …`). No AI co-author trailers.

## Versioning, releases and deployment

One version, kept in sync by `scripts/prepare-release.sh` and enforced by
CI's `version sync` job: `[workspace.package] version` and the pinned
`meili-ingest-plugin-sdk` version in `Cargo.toml`, the workspace crates in
`Cargo.lock`, `info.version` in `docs/openapi.yaml` (the Pipelines contract
the Lab vendors), and the newest `CHANGELOG.md` section.

- **Every PR**: add its entries under `Unreleased` in `CHANGELOG.md`, in the
  same PR. Leave every version alone: only the release script writes them.
- **Release**: run the "Prepare release" workflow, then merge the
  `chore(release): vX.Y.Z` PR it opens. The merge tags `vX.Y.Z`, pushes
  `ghcr.io/qdequele/glutony:X.Y.Z`, publishes the GitHub release and sends
  `glutony-released` to the Lab. Tags come only from that merge: never push
  or move one by hand. Steps: CONTRIBUTING.md, "Releasing".
- **Contract change** (`docs/openapi.yaml`): merge and release it here, then
  the Lab's Contracts sync PR re-vendors it (or re-vendor in the Lab in the
  same cross-repo task, same branch name, linked PRs). The Lab ships code
  that uses it after. A Lab-owned contract (`contracts/vendor/lab/`) goes the
  other way: the Lab changes it first, backward compatible, then copy it here.
- **Deploy**: production (the Lab's hosted Pipelines on hackersearch) is not
  the release. qdq-server's `glutony-autodeploy` builds every `main` commit
  whose `rustfmt`, `clippy`, `test`, `docker build (amd64)` and
  `docker build (arm64)` checks are green, so merging to `main` deploys.
  Renaming one of those jobs stalls the autodeploy until
  `GLUTONY_REQUIRED_CHECKS` in qdq-server follows. It `pg_dump`s the `glutony`
  database first; the control plane applies `migrations/` (sqlx) at boot,
  forward-only, and a rollback restores the dump when `migrations/` changed.
  Kubernetes: pin `newTag` in `k8s/kustomization.yaml` to the release, then
  `kubectl apply -k k8s/` (`docs/deployment/kubernetes.mdx`, "Upgrades").
- **Before merging a change that needs config**: a new required env var or
  secret goes into `/etc/glutony/glutony.env` on the box (and
  `docs/deployment/environment-variables.mdx`) first; an auth change that the
  gateway, workers and control plane must agree on follows the rollout order
  in `docs/deployment/kubernetes.mdx`. Keep migrations additive: a failed
  deploy restores the pre-deploy dump and loses the writes made meanwhile.
- **PR description**: a "Deploy notes" section listing env and config
  changes, migrations and the rollout order, or "none".
