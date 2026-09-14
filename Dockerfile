# syntax=docker/dockerfile:1.7
#
# meili-ingest — production image.
#
# One image ships all three binaries:
#   /usr/local/bin/meili-ingest-gateway
#   /usr/local/bin/meili-ingest-control-plane
#   /usr/local/bin/meili-ingest-worker
#
# The default entrypoint is the gateway. Pick another binary either with the
# `BIN` build argument (`docker build --build-arg BIN=meili-ingest-worker .`)
# or, preferably, at run time (`command:` in Kubernetes, `entrypoint:` in
# Compose) so that a single image can be reused for every service.
#
# Dependencies are compiled in a separate cargo-chef layer so that editing
# source code does not invalidate the (slow) dependency build.
#
# The admin UI is a Next.js static export built in its own stage and compiled into
# the gateway binary (the `ui` cargo feature), so the runtime image carries no Node
# and the browser talks to the API same-origin. For an API-only binary, build with
# plain `cargo build` outside Docker: the `ui` feature is off by default.

ARG RUST_VERSION=1.94
ARG NODE_VERSION=22

# ---------------------------------------------------------------------------
# Stage 1: tooling
# ---------------------------------------------------------------------------
FROM rust:${RUST_VERSION}-bookworm AS chef
RUN apt-get update \
 && apt-get install -y --no-install-recommends pkg-config libssl-dev \
 && rm -rf /var/lib/apt/lists/* \
 && cargo install cargo-chef --locked
WORKDIR /app

# ---------------------------------------------------------------------------
# Stage 2a: build the admin UI (static export)
# ---------------------------------------------------------------------------
FROM node:${NODE_VERSION}-bookworm-slim AS ui
WORKDIR /ui
RUN corepack enable
# Install from the lockfile first so a source-only change reuses this layer.
# `pnpm-workspace.yaml` must come along: it carries the build-script policy for
# `sharp` and `unrs-resolver`, and without it pnpm 10+ fails the install outright
# with ERR_PNPM_IGNORED_BUILDS rather than just warning.
COPY ui/package.json ui/pnpm-lock.yaml* ui/pnpm-workspace.yaml* ./
RUN pnpm install --frozen-lockfile
COPY ui/ ./
# Mounted under /ui so the export never shadows the API routes, which share the
# same names (/pipelines, /jobs, /plugins).
ENV NEXT_PUBLIC_BASE_PATH=/ui
RUN pnpm build && test -f out/index.html

# ---------------------------------------------------------------------------
# Stage 2: compute the dependency recipe
# ---------------------------------------------------------------------------
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Stage 3: build dependencies, then the workspace
# ---------------------------------------------------------------------------
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
# Build only the dependency graph (cached as long as Cargo.toml/Cargo.lock do not change).
RUN cargo chef cook --release --workspace --recipe-path recipe.json
# Now the real sources (proto/ and migrations/ are needed at compile time by
# tonic-prost-build and sqlx::migrate!).
COPY . .
# The exported UI must exist before cargo builds the gateway with `--features ui`,
# because rust-embed reads ui/out at compile time.
COPY --from=ui /ui/out ./ui/out
RUN cargo build --release --workspace --bins --features meili-ingest-gateway/ui \
 && mkdir -p /out \
 && cp target/release/meili-ingest-gateway \
       target/release/meili-ingest-control-plane \
       target/release/meili-ingest-worker \
       /out/

# ---------------------------------------------------------------------------
# Stage 4: minimal runtime
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS runtime
ARG BIN=meili-ingest-gateway

RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates tzdata \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 meili \
 && useradd  --system --uid 10001 --gid meili --home-dir /data --create-home --shell /usr/sbin/nologin meili \
 && mkdir -p /data/blobs \
 && chown -R meili:meili /data

COPY --from=builder /out/meili-ingest-gateway       /usr/local/bin/meili-ingest-gateway
COPY --from=builder /out/meili-ingest-control-plane /usr/local/bin/meili-ingest-control-plane
COPY --from=builder /out/meili-ingest-worker        /usr/local/bin/meili-ingest-worker

# `/usr/local/bin/meili-ingest` points at the binary selected by --build-arg BIN
# (gateway by default). Kubernetes `command:` / Compose `entrypoint:` override it.
RUN ln -sf "/usr/local/bin/${BIN}" /usr/local/bin/meili-ingest

USER meili
WORKDIR /data
ENV RUST_LOG=info \
    BLOB_STORE_URL=file:///data/blobs

# gateway 8080, control plane 9000 (workers expose nothing)
EXPOSE 8080 9000

ENTRYPOINT ["/usr/local/bin/meili-ingest"]
