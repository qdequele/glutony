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

Releases are automated; nobody edits a version or pushes a tag by hand.

1. **Fill `Unreleased`** in `CHANGELOG.md` with each PR (the release refuses an
   empty section).
2. **Run the "Prepare release" workflow** (Actions > Prepare release > Run
   workflow) with a bump: `auto` (the default) picks it from the conventional
   commits since the last tag (`type!:` or a `BREAKING CHANGE` footer: major;
   any `feat`: minor; otherwise patch), or `patch`, `minor`, `major`, or an
   exact `X.Y.Z`. It runs `scripts/prepare-release.sh`, which bumps
   `[workspace.package] version` and the pinned `version` of the internal path
   dependencies in `Cargo.toml`, the workspace crates in `Cargo.lock` and
   `info.version` in `docs/openapi.yaml`, and folds `Unreleased` into
   `## [X.Y.Z] - YYYY-MM-DD`. It opens or refreshes the PR
   `chore(release): vX.Y.Z` and dispatches CI on it.
3. **Merge the release PR** on green. `release-on-merge.yml` tags the merge
   commit `vX.Y.Z` and runs `release.yml`: the multi-arch image
   `ghcr.io/qdequele/glutony:X.Y.Z` (plus `X.Y` and `latest`), the GitHub
   release with the version's CHANGELOG section as notes, and a
   `glutony-released` dispatch to `meilisearch/lab` so its Contracts sync
   re-vendors `docs/openapi.yaml` (only with the `LAB_DISPATCH_TOKEN` secret).

Locally, `scripts/prepare-release.sh` with no argument prints the suggested
bump; `scripts/prepare-release.sh patch` prepares the same commit the workflow
makes (re-running it is safe: bumps count from the last tag).
`scripts/prepare-release.sh --check` is CI's `version sync` job.

A tag pushed with `GITHUB_TOKEN` triggers no workflow, so
`release-on-merge.yml` calls `release.yml` as a reusable workflow; a tag pushed
by hand still runs `release.yml` (recovery only). Either way `release.yml`
publishes nothing unless `scripts/prepare-release.sh --verify-tag vX.Y.Z`
passes on the tagged tree: the crate version is `X.Y.Z`, every copy agrees and
`CHANGELOG.md` has the `[X.Y.Z]` section. So never create a release or tag from
GitHub's "Draft a new release" form: it tags `main` as it is, unbumped.
`scripts/test-prepare-release.sh` tests the script (CI's `version sync` job).

Repository setup: Settings > Actions > General > "Allow GitHub Actions to
create and approve pull requests". Optional secret `LAB_DISPATCH_TOKEN`: a
fine-grained token with Contents read and write on `meilisearch/lab`.
