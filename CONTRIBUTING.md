# Contributing

## Setup

- Rust 1.94 (`rust-toolchain` is pinned in CI), Docker with Compose v2.22+ (OrbStack on macOS).
- `docker compose watch` starts Postgres, Temporal, Meilisearch and the three services with hot reload. The dev stack sets dev-only secrets (`SOURCE_SECRET_KEY`, `CONTROL_PLANE_TOKEN`); never deploy them.
- Native: `cargo run --bin meili-ingest-control-plane` (needs `DATABASE_URL` and `CONTROL_PLANE_TOKEN` or `CONTROL_PLANE_TOKEN_DISABLED=true`), then the gateway and a worker.

## Before you push

```bash
cargo fmt --all
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
DATABASE_URL=postgres://postgres:dev-password@localhost:5432/postgres cargo test -p meili-ingest-control-plane
```

The control-plane DB tests skip without `DATABASE_URL`. `docs/openapi.yaml` is checked against the router by `crates/gateway/src/routes.rs`: a new route goes in `ROUTES`, `lib.rs` and the spec.

## Rules

- Secrets are `<redacted>` in `Debug` and never logged; tokens are compared in constant time (`subtle`).
- `contracts/vendor/` is never edited by hand: change the owner's file, then copy it (see `contracts/vendor/lab/README.md`).
- Tinybird column names do not change (a rename is a new datasource and a backfill).
- Commits use conventional messages: `feat(scope): …`, `fix(scope): …`, `docs: …`, `ci: …`. No AI co-author trailers.
- Every behaviour change comes with a test and, when user-facing, a `CHANGELOG.md` entry under `Unreleased`.

## Releasing

Move the `Unreleased` section of `CHANGELOG.md` under the new version, bump `version` in `Cargo.toml`, tag `vX.Y.Z` and push the tag. `.github/workflows/release.yml` builds and pushes `ghcr.io/qdequele/glutony:X.Y.Z` and creates the GitHub release.
