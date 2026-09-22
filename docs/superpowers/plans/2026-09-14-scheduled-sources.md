# Scheduled Sources Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a `source` entity — a location fetched on a cron schedule and run through an existing pipeline — so an index can stay in sync with an upstream feed without anyone calling `POST /ingest`.

**Architecture:** A `sources` row holds a location, a cron expression, a pipeline uid and two sealed secrets. Creating one also creates a Temporal Schedule whose action starts a thin `SourceRunWorkflow`. That workflow loads the source at run time, asks a `SourceConnector` to resolve the location into zero or more items (short-circuiting on an unchanged upstream), and starts one `PipelineWorkflow` child per item. Only `UrlConnector` ships in v1; the trait and the `Vec`-shaped resolution exist so zip/bucket/api connectors land later without touching the scheduler.

**Tech Stack:** Rust 2024, axum 0.8, sqlx 0.9 (Postgres), temporalio-sdk/client 1.0, reqwest 0.13, object_store 0.14. New: `chacha20poly1305`, `blake3`, `flate2`, `chrono-tz`. UI: Next.js App Router, TanStack Query, react-hook-form + zod, shadcn/ui, lucide-react.

**Spec:** `docs/superpowers/specs/2026-09-13-scheduled-sources-design.md` — read it entirely before starting. Its Decisions section resolves every fork; where this plan and the spec differ, the spec wins.

## Global Constraints

Copied from the repo's existing plan (`docs/superpowers/plans/2026-09-12-meili-ingest.md`); they apply to every task here.

- Workspace root `Cargo.toml` pins every dependency under `[workspace.dependencies]`. Crates use `x.workspace = true`. A crate missing from that list is added there first, never inline.
- Edition 2024, `rust-version = 1.92`. `unsafe` forbidden.
- `thiserror` in library crates, `anyhow` in binaries (`main.rs`).
- **No `unwrap()`/`expect()` in non-test code.** Use `?` or explicit handling.
- All public types derive `Debug, Clone, Serialize, Deserialize` (and `PartialEq` when cheap).
- Env vars are read only in `from_env()` constructors or `main.rs`.
- Structured tracing: `tracing::info!(source_id = %id, "...")`. **Never log a decrypted secret or an api_key** — use `MeiliContext::redacted()`.
- Unit tests in `#[cfg(test)]`. Every crate must pass `cargo test -p <crate>` and `cargo clippy -p <crate> -- -D warnings`.
- Conventional commits (`feat:`, `fix:`, `docs:`, `chore:`). **No `Co-Authored-By` lines.**
- Run `cargo fmt` before every commit.

## File Structure

New crate `crates/source` — everything about turning a location into items, plus the two security primitives. It depends on `plugin-sdk`, `blob`, `reqwest`; it does **not** depend on the gateway, control plane or worker, so all of it is unit-testable without a server.

| File | Responsibility |
|---|---|
| `crates/source/src/lib.rs` | Crate docs, re-exports, `SourceError` |
| `crates/source/src/secret.rs` | Seal/open with chacha20poly1305; `SecretKey::from_env` |
| `crates/source/src/guard.rs` | SSRF guard: scheme, DNS, address classification, redirect re-check |
| `crates/source/src/template.rs` | `{{ date:FMT }}` / `{{ date-1d:FMT }}` / `{{ timestamp }}` rendering |
| `crates/source/src/model.rs` | `Location`, `SourceDefinition`, `IncrementalState`, `FetchAuth` |
| `crates/source/src/connector.rs` | `SourceConnector` trait, `Resolution` |
| `crates/source/src/url.rs` | `UrlConnector`: conditional GET, streaming, blake3, gzip |

Modified elsewhere:

| File | Change |
|---|---|
| `migrations/0002_sources.sql` | New: `sources`, `source_runs`, `jobs.source_id` |
| `crates/control-plane/src/sources.rs` | New: `SourceRepo` + `/internal/sources` handlers |
| `crates/control-plane/src/pipelines.rs` | Delete cascades to archive dependent sources |
| `crates/control-plane/src/lib.rs` | Register `sources` module + routes |
| `crates/gateway/src/schedules.rs` | New: Temporal Schedule create/update/delete/pause/trigger |
| `crates/gateway/src/handlers/sources.rs` | New: the eight public `/sources` routes |
| `crates/gateway/src/lib.rs` | Register routes |
| `crates/worker/src/source_workflow.rs` | New: `SourceRunWorkflow` |
| `crates/worker/src/source_activity.rs` | New: `load_source`, `resolve_source`, `record_run` |
| `crates/worker/src/main.rs` | Register the second workflow type |
| `ui/src/app/sources/` | List, form, run history |
| `docs/concepts/sources.mdx`, `docs/openapi.yaml` | Docs |

## Task ordering

Tasks 1–6 are the `crates/source` foundation and the schema; they have no dependencies on each other beyond the crate existing (Task 2 creates it). Tasks 7–9 are the control plane, 10–11 the gateway, 12–13 the worker, 14 the UI, 15 the docs. Each task ends green and committable.

---

### Task 1: Migration for `sources`, `source_runs` and `jobs.source_id`

**Files:**
- Create: `migrations/0002_sources.sql`
- Test: `crates/control-plane/tests/migrations.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: tables `sources`, `source_runs`; column `jobs.source_id`. Every later Postgres task reads these column names.

- [ ] **Step 1: Write the failing test**

Create `crates/control-plane/tests/migrations.rs`. It needs a live Postgres; skip cleanly when `DATABASE_URL` is unset so the suite stays green on a laptop without one.

```rust
//! Verifies the migrations apply and produce the columns later code binds to.

use sqlx::{Executor, PgPool};

async fn pool() -> Option<PgPool> {
    let url = std::env::var("DATABASE_URL").ok()?;
    PgPool::connect(&url).await.ok()
}

#[tokio::test]
async fn sources_schema_exists_after_migration() {
    let Some(pool) = pool().await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");

    // A source row round-trips with the columns the repo binds.
    pool.execute(
        "INSERT INTO sources (id, uid, name, pipeline_uid, location, cron, meili_ctx, schedule_id)
         VALUES ('11111111-1111-1111-1111-111111111111', 'tmdb', 'TMDB', 'builtin.json',
                 '{\"kind\":\"url\",\"url\":\"https://example.test/x.json\"}'::jsonb,
                 '0 9 * * *', '\\x00'::bytea, 'source-tmdb')",
    )
    .await
    .expect("insert source");

    let (archived,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT archived_at FROM sources WHERE uid = 'tmdb'")
            .fetch_one(&pool)
            .await
            .expect("archived_at column exists");
    assert!(archived.is_none(), "new sources are not archived");

    // jobs.source_id exists and is nullable.
    sqlx::query_as::<_, (Option<uuid::Uuid>,)>("SELECT source_id FROM jobs LIMIT 0")
        .fetch_optional(&pool)
        .await
        .expect("jobs.source_id column exists");

    pool.execute("DELETE FROM sources WHERE uid = 'tmdb'")
        .await
        .ok();
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-control-plane --test migrations`
Expected: with `DATABASE_URL` set, FAIL — `relation "sources" does not exist`. Without it, the test prints the skip line and passes; start Postgres with `docker compose up -d postgres` and export `DATABASE_URL` so the test really runs.

- [ ] **Step 3: Write the migration**

Create `migrations/0002_sources.sql`:

```sql
-- Scheduled sources: a location fetched on a cron and run through a pipeline.
-- See docs/superpowers/specs/2026-09-13-scheduled-sources-design.md.

CREATE TABLE IF NOT EXISTS sources (
    id            UUID PRIMARY KEY,
    uid           TEXT NOT NULL,
    name          TEXT NOT NULL,
    description   TEXT,
    project_id    TEXT,
    pipeline_uid  TEXT NOT NULL,
    location      JSONB NOT NULL,
    cron          TEXT NOT NULL,
    timezone      TEXT NOT NULL DEFAULT 'UTC',
    paused        BOOLEAN NOT NULL DEFAULT false,
    index_name    TEXT,
    fetch_auth    BYTEA,
    meili_ctx     BYTEA NOT NULL,
    last_etag     TEXT,
    last_modified TEXT,
    last_hash     BYTEA,
    last_run_at   TIMESTAMPTZ,
    last_status   TEXT,
    last_error    TEXT,
    schedule_id   TEXT NOT NULL,
    archived_at   TIMESTAMPTZ,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Same shape as pipelines_uid_project: a uid may exist once globally and once per tenant.
CREATE UNIQUE INDEX IF NOT EXISTS sources_uid_project
    ON sources (uid, COALESCE(project_id, ''));

CREATE INDEX IF NOT EXISTS sources_pipeline
    ON sources (pipeline_uid, COALESCE(project_id, ''));

CREATE TABLE IF NOT EXISTS source_runs (
    run_id      UUID PRIMARY KEY,
    source_id   UUID NOT NULL REFERENCES sources (id) ON DELETE CASCADE,
    started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    finished_at TIMESTAMPTZ,
    outcome     TEXT NOT NULL,
    items       INT NOT NULL DEFAULT 0,
    job_ids     UUID[] NOT NULL DEFAULT '{}',
    error       TEXT
);

CREATE INDEX IF NOT EXISTS source_runs_source_started
    ON source_runs (source_id, started_at DESC);

ALTER TABLE jobs ADD COLUMN IF NOT EXISTS source_id UUID;

CREATE INDEX IF NOT EXISTS jobs_source
    ON jobs (source_id, started_at DESC);
```

- [ ] **Step 4: Run test to verify it passes**

Run: `DATABASE_URL=postgres://postgres:dev-password@localhost:5432/postgres cargo test -p meili-ingest-control-plane --test migrations`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add migrations/0002_sources.sql crates/control-plane/tests/migrations.rs
git commit -m "feat(db): sources and source_runs tables"
```

---

### Task 2: `crates/source` skeleton and sealed secrets

**Files:**
- Create: `crates/source/Cargo.toml`, `crates/source/src/lib.rs`, `crates/source/src/secret.rs`
- Modify: `Cargo.toml` (workspace members + deps)

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `SourceError` (thiserror enum, variants added by later tasks)
  - `SecretKey` with `SecretKey::from_env() -> Result<Option<SecretKey>, SourceError>`, `SecretKey::seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SourceError>`, `SecretKey::open(&self, sealed: &[u8]) -> Result<Vec<u8>, SourceError>`
  - `seal_json<T: Serialize>(&SecretKey, &T) -> Result<Vec<u8>, SourceError>` and `open_json<T: DeserializeOwned>(&SecretKey, &[u8]) -> Result<T, SourceError>`

- [ ] **Step 1: Write the failing test**

Create `crates/source/src/secret.rs` containing only the test module for now:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> SecretKey {
        SecretKey::from_bytes([7u8; 32])
    }

    #[test]
    fn seal_open_roundtrips() {
        let k = key();
        let sealed = k.seal(b"hunter2").expect("seal");
        assert_ne!(sealed.as_slice(), b"hunter2", "ciphertext is not plaintext");
        assert_eq!(k.open(&sealed).expect("open"), b"hunter2");
    }

    #[test]
    fn sealing_twice_gives_different_ciphertext() {
        let k = key();
        assert_ne!(
            k.seal(b"same").expect("a"),
            k.seal(b"same").expect("b"),
            "nonce must be random per write"
        );
    }

    #[test]
    fn wrong_key_fails_to_open() {
        let sealed = key().seal(b"secret").expect("seal");
        let other = SecretKey::from_bytes([9u8; 32]);
        assert!(other.open(&sealed).is_err());
    }

    #[test]
    fn truncated_ciphertext_fails_cleanly() {
        let sealed = key().seal(b"secret").expect("seal");
        assert!(key().open(&sealed[..4]).is_err());
        assert!(key().open(&[]).is_err());
    }

    #[test]
    fn version_byte_is_rejected_when_unknown() {
        let mut sealed = key().seal(b"secret").expect("seal");
        sealed[0] = 0xFF;
        assert!(key().open(&sealed).is_err());
    }

    #[test]
    fn json_roundtrips() {
        let k = key();
        let sealed = seal_json(&k, &vec!["a".to_string(), "b".to_string()]).expect("seal");
        let back: Vec<String> = open_json(&k, &sealed).expect("open");
        assert_eq!(back, vec!["a".to_string(), "b".to_string()]);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-source secret`
Expected: FAIL — the crate does not exist yet (`error: package ID specification ... did not match any packages`).

- [ ] **Step 3: Create the crate and implement sealing**

Add to the workspace root `Cargo.toml` — under `[workspace] members` add `"crates/source"`, under `[workspace.dependencies]` add:

```toml
meili-ingest-source = { path = "crates/source" }
chacha20poly1305 = "0.10"
blake3 = "1.5"
flate2 = "1.0"
chrono-tz = "0.10"
base64 = "0.22"
rand = "0.9"
```

Create `crates/source/Cargo.toml`:

```toml
[package]
name = "meili-ingest-source"
version.workspace = true
edition.workspace = true
rust-version.workspace = true
license.workspace = true

[dependencies]
meili-ingest-plugin-sdk.workspace = true
chacha20poly1305.workspace = true
base64.workspace = true
rand.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing.workspace = true
```

Create `crates/source/src/lib.rs`:

```rust
//! Scheduled sources: turning a location into items, and the two security primitives
//! that makes safe (sealed secrets, SSRF guard).
//!
//! This crate deliberately depends on neither the gateway, the control plane nor the
//! worker, so every part of it is unit-testable without a server.

pub mod secret;

pub use secret::{SecretKey, open_json, seal_json};

/// Errors produced while handling a source.
#[derive(Debug, thiserror::Error)]
pub enum SourceError {
    /// `SOURCE_SECRET_KEY` is missing, malformed, or not 32 bytes.
    #[error("source secret key: {0}")]
    Key(String),
    /// Sealing or opening failed (wrong key, truncated or tampered ciphertext).
    #[error("sealed value: {0}")]
    Seal(String),
    /// A sealed payload did not deserialize into the expected type.
    #[error("sealed payload: {0}")]
    Payload(String),
}
```

Now write the implementation at the top of `crates/source/src/secret.rs`, above the existing test module:

```rust
//! Authenticated encryption for the two secrets a source stores: its fetch credential
//! and its `MeiliContext`.
//!
//! Layout of a sealed value: `version(1) ‖ nonce(12) ‖ ciphertext`. The version byte
//! lets a rotation scheme be added later without a migration.

use base64::Engine as _;
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use serde::Serialize;
use serde::de::DeserializeOwned;

use crate::SourceError;

/// Current sealed-value format.
const VERSION: u8 = 1;
/// ChaCha20-Poly1305 nonce length.
const NONCE_LEN: usize = 12;

/// Symmetric key used to seal and open a source's secrets.
///
/// Deliberately does not implement `Debug`, `Serialize` or `Clone`-to-string, so the key
/// cannot be printed into a log by accident.
pub struct SecretKey(Key);

impl std::fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SecretKey(<redacted>)")
    }
}

impl SecretKey {
    /// Key from raw bytes.
    pub fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(Key::from(bytes))
    }

    /// Read `SOURCE_SECRET_KEY` (32 bytes, base64). `Ok(None)` when it is unset, which
    /// callers must turn into a 501: storing a tenant's write key in plaintext because
    /// an env var was missed is not an acceptable degraded mode.
    pub fn from_env() -> Result<Option<Self>, SourceError> {
        let Ok(raw) = std::env::var("SOURCE_SECRET_KEY") else {
            return Ok(None);
        };
        if raw.trim().is_empty() {
            return Ok(None);
        }
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(raw.trim())
            .map_err(|e| SourceError::Key(format!("not valid base64: {e}")))?;
        let bytes: [u8; 32] = decoded
            .try_into()
            .map_err(|v: Vec<u8>| SourceError::Key(format!("expected 32 bytes, got {}", v.len())))?;
        Ok(Some(Self::from_bytes(bytes)))
    }

    /// Seal `plaintext`. A fresh random nonce is used on every call, so sealing the same
    /// value twice never produces the same ciphertext.
    pub fn seal(&self, plaintext: &[u8]) -> Result<Vec<u8>, SourceError> {
        let cipher = ChaCha20Poly1305::new(&self.0);
        let mut nonce_bytes = [0u8; NONCE_LEN];
        rand::rng().fill_bytes(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);
        let ciphertext = cipher
            .encrypt(nonce, plaintext)
            .map_err(|_| SourceError::Seal("encryption failed".into()))?;
        let mut out = Vec::with_capacity(1 + NONCE_LEN + ciphertext.len());
        out.push(VERSION);
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    /// Open a value produced by [`SecretKey::seal`].
    pub fn open(&self, sealed: &[u8]) -> Result<Vec<u8>, SourceError> {
        if sealed.len() <= 1 + NONCE_LEN {
            return Err(SourceError::Seal("sealed value is too short".into()));
        }
        if sealed[0] != VERSION {
            return Err(SourceError::Seal(format!(
                "unsupported sealed format version {}",
                sealed[0]
            )));
        }
        let nonce = Nonce::from_slice(&sealed[1..1 + NONCE_LEN]);
        ChaCha20Poly1305::new(&self.0)
            .decrypt(nonce, &sealed[1 + NONCE_LEN..])
            .map_err(|_| SourceError::Seal("decryption failed: wrong key or tampered value".into()))
    }
}

/// Seal any serializable value as JSON.
pub fn seal_json<T: Serialize>(key: &SecretKey, value: &T) -> Result<Vec<u8>, SourceError> {
    let json =
        serde_json::to_vec(value).map_err(|e| SourceError::Payload(format!("serialize: {e}")))?;
    key.seal(&json)
}

/// Open a value sealed by [`seal_json`].
pub fn open_json<T: DeserializeOwned>(key: &SecretKey, sealed: &[u8]) -> Result<T, SourceError> {
    let json = key.open(sealed)?;
    serde_json::from_slice(&json).map_err(|e| SourceError::Payload(format!("deserialize: {e}")))
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p meili-ingest-source secret
cargo clippy -p meili-ingest-source -- -D warnings
```
Expected: 6 tests pass, clippy clean.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add Cargo.toml Cargo.lock crates/source
git commit -m "feat(source): sealed secrets for source credentials"
```

---

### Task 3: SSRF guard

**Files:**
- Create: `crates/source/src/guard.rs`
- Modify: `crates/source/src/lib.rs` (add `pub mod guard;`), `crates/source/Cargo.toml` (add `url`, `tokio`)

**Interfaces:**
- Consumes: `SourceError` from Task 2.
- Produces:
  - `pub fn classify(ip: IpAddr) -> AddressClass` where `AddressClass` is `Public | Loopback | Private | LinkLocal | Cgnat | UniqueLocal | Unspecified | Multicast`
  - `pub struct UrlGuard { pub max_redirects: usize, pub max_bytes: u64 }` with `UrlGuard::default()`
  - `pub fn check_scheme(url: &Url) -> Result<(), SourceError>`
  - `pub async fn check_url(&self, url: &Url) -> Result<(), SourceError>` — resolves DNS and rejects if **any** resolved address is non-public

- [ ] **Step 1: Write the failing test**

Create `crates/source/src/guard.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::net::IpAddr;
    use url::Url;

    fn ip(s: &str) -> IpAddr {
        s.parse().expect("parses")
    }

    #[test]
    fn classifies_addresses() {
        // Public.
        assert_eq!(classify(ip("1.1.1.1")), AddressClass::Public);
        assert_eq!(classify(ip("2606:4700::1111")), AddressClass::Public);
        // Loopback.
        assert_eq!(classify(ip("127.0.0.1")), AddressClass::Loopback);
        assert_eq!(classify(ip("::1")), AddressClass::Loopback);
        // RFC1918 private.
        assert_eq!(classify(ip("10.0.0.5")), AddressClass::Private);
        assert_eq!(classify(ip("172.16.0.1")), AddressClass::Private);
        assert_eq!(classify(ip("192.168.1.1")), AddressClass::Private);
        // Link-local — this is the cloud metadata endpoint.
        assert_eq!(classify(ip("169.254.169.254")), AddressClass::LinkLocal);
        assert_eq!(classify(ip("fe80::1")), AddressClass::LinkLocal);
        // Carrier-grade NAT.
        assert_eq!(classify(ip("100.64.0.1")), AddressClass::Cgnat);
        // IPv6 unique local.
        assert_eq!(classify(ip("fc00::1")), AddressClass::UniqueLocal);
        assert_eq!(classify(ip("fd12:3456::1")), AddressClass::UniqueLocal);
        // Unspecified and multicast.
        assert_eq!(classify(ip("0.0.0.0")), AddressClass::Unspecified);
        assert_eq!(classify(ip("224.0.0.1")), AddressClass::Multicast);
    }

    #[test]
    fn only_public_addresses_are_allowed() {
        assert!(AddressClass::Public.is_allowed());
        for c in [
            AddressClass::Loopback,
            AddressClass::Private,
            AddressClass::LinkLocal,
            AddressClass::Cgnat,
            AddressClass::UniqueLocal,
            AddressClass::Unspecified,
            AddressClass::Multicast,
        ] {
            assert!(!c.is_allowed(), "{c:?} must be rejected");
        }
    }

    #[test]
    fn rejects_non_https_schemes() {
        assert!(check_scheme(&Url::parse("https://example.test/a").expect("url")).is_ok());
        for bad in [
            "http://example.test/a",
            "file:///etc/passwd",
            "ftp://example.test/a",
            "gopher://example.test/a",
        ] {
            let url = Url::parse(bad).expect("url");
            assert!(check_scheme(&url).is_err(), "{bad} must be rejected");
        }
    }

    #[tokio::test]
    async fn rejects_a_literal_private_host() {
        let guard = UrlGuard::default();
        for bad in [
            "https://127.0.0.1/x",
            "https://169.254.169.254/latest/meta-data/",
            "https://10.0.0.1/x",
            "https://[::1]/x",
        ] {
            let url = Url::parse(bad).expect("url");
            assert!(guard.check_url(&url).await.is_err(), "{bad} must be rejected");
        }
    }

    #[test]
    fn default_guard_caps_redirects_and_size() {
        let g = UrlGuard::default();
        assert_eq!(g.max_redirects, 5);
        assert_eq!(g.max_bytes, 512 * 1024 * 1024);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-source guard`
Expected: FAIL — `cannot find function classify`, `cannot find type AddressClass`.

- [ ] **Step 3: Implement the guard**

Add to `crates/source/Cargo.toml` dependencies: `url.workspace = true`, `tokio.workspace = true`. Add `pub mod guard;` and `pub use guard::{AddressClass, UrlGuard, classify, check_scheme};` to `lib.rs`. Add to `SourceError`:

```rust
    /// The URL was rejected before any socket was opened (scheme or address policy).
    #[error("blocked url: {0}")]
    Blocked(String),
    /// DNS resolution failed.
    #[error("dns: {0}")]
    Dns(String),
```

Write above the test module in `guard.rs`:

```rust
//! SSRF guard for tenant-supplied URLs.
//!
//! A source's URL is fetched from inside the cluster, where the cloud metadata endpoint
//! and every internal service are reachable. Everything here runs *before* a socket is
//! opened, and again after each redirect — an allowed host redirecting to `127.0.0.1`
//! is the obvious bypass.
//!
//! Known residual risk, not solved in v1: DNS rebinding between this check and the
//! actual connect. Closing it means pinning the resolved address into the connector.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, ToSocketAddrs};

use url::Url;

use crate::SourceError;

/// What kind of address a host resolved to. Only [`AddressClass::Public`] may be fetched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddressClass {
    /// Routable on the public internet.
    Public,
    /// `127.0.0.0/8`, `::1`.
    Loopback,
    /// RFC1918 `10/8`, `172.16/12`, `192.168/16`.
    Private,
    /// `169.254/16`, `fe80::/10` — includes the cloud metadata endpoint.
    LinkLocal,
    /// Carrier-grade NAT `100.64/10`.
    Cgnat,
    /// IPv6 unique local `fc00::/7`.
    UniqueLocal,
    /// `0.0.0.0`, `::`.
    Unspecified,
    /// Multicast ranges.
    Multicast,
}

impl AddressClass {
    /// Whether a source may fetch from an address of this class.
    pub fn is_allowed(self) -> bool {
        matches!(self, AddressClass::Public)
    }
}

/// Classify one address. IPv4-mapped IPv6 addresses are unwrapped first, so
/// `::ffff:127.0.0.1` classifies as loopback rather than public.
pub fn classify(ip: IpAddr) -> AddressClass {
    match ip {
        IpAddr::V4(v4) => classify_v4(v4),
        IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
            Some(v4) => classify_v4(v4),
            None => classify_v6(v6),
        },
    }
}

fn classify_v4(ip: Ipv4Addr) -> AddressClass {
    let [a, b, ..] = ip.octets();
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if ip.is_link_local() {
        AddressClass::LinkLocal
    } else if a == 100 && (64..128).contains(&b) {
        AddressClass::Cgnat
    } else if ip.is_private() {
        AddressClass::Private
    } else if ip.is_multicast() || ip.is_broadcast() {
        AddressClass::Multicast
    } else {
        AddressClass::Public
    }
}

fn classify_v6(ip: Ipv6Addr) -> AddressClass {
    let first = ip.segments()[0];
    if ip.is_unspecified() {
        AddressClass::Unspecified
    } else if ip.is_loopback() {
        AddressClass::Loopback
    } else if (first & 0xffc0) == 0xfe80 {
        AddressClass::LinkLocal
    } else if (first & 0xfe00) == 0xfc00 {
        AddressClass::UniqueLocal
    } else if ip.is_multicast() {
        AddressClass::Multicast
    } else {
        AddressClass::Public
    }
}

/// Reject any scheme but `https`.
pub fn check_scheme(url: &Url) -> Result<(), SourceError> {
    match url.scheme() {
        "https" => Ok(()),
        other => Err(SourceError::Blocked(format!(
            "scheme {other:?} is not allowed; sources must use https"
        ))),
    }
}

/// Policy applied to every source fetch.
#[derive(Debug, Clone, Copy)]
pub struct UrlGuard {
    /// Maximum redirects followed before giving up.
    pub max_redirects: usize,
    /// Maximum bytes read from a response body.
    pub max_bytes: u64,
}

impl Default for UrlGuard {
    fn default() -> Self {
        Self {
            max_redirects: 5,
            max_bytes: 512 * 1024 * 1024,
        }
    }
}

impl UrlGuard {
    /// Check scheme, then resolve the host and reject unless **every** resolved address
    /// is public. Checking only the first address would let a host with one public and
    /// one loopback record through.
    pub async fn check_url(&self, url: &Url) -> Result<(), SourceError> {
        check_scheme(url)?;
        let host = url
            .host_str()
            .ok_or_else(|| SourceError::Blocked("url has no host".into()))?
            .to_owned();
        let port = url.port_or_known_default().unwrap_or(443);

        // ToSocketAddrs blocks; keep it off the async worker threads.
        let resolved = tokio::task::spawn_blocking(move || {
            (host.as_str(), port)
                .to_socket_addrs()
                .map(|it| it.map(|s| s.ip()).collect::<Vec<_>>())
        })
        .await
        .map_err(|e| SourceError::Dns(format!("resolver task failed: {e}")))?
        .map_err(|e| SourceError::Dns(format!("could not resolve {url}: {e}")))?;

        if resolved.is_empty() {
            return Err(SourceError::Dns(format!("{url} resolved to no addresses")));
        }
        for ip in resolved {
            let class = classify(ip);
            if !class.is_allowed() {
                return Err(SourceError::Blocked(format!(
                    "{url} resolves to {ip}, which is {class:?}; only public addresses may be fetched"
                )));
            }
        }
        Ok(())
    }
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p meili-ingest-source guard
cargo clippy -p meili-ingest-source -- -D warnings
```
Expected: 5 tests pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/source Cargo.toml Cargo.lock
git commit -m "feat(source): SSRF guard for tenant-supplied urls"
```

---

### Task 4: URL template rendering

**Files:**
- Create: `crates/source/src/template.rs`
- Modify: `crates/source/src/lib.rs`, `crates/source/Cargo.toml` (add `chrono`, `chrono-tz`)

**Interfaces:**
- Consumes: `SourceError`.
- Produces: `pub fn render(template: &str, scheduled_at: DateTime<Utc>, timezone: &str) -> Result<String, SourceError>`

- [ ] **Step 1: Write the failing test**

Create `crates/source/src/template.rs` with only the tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(y: i32, m: u32, d: u32, h: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, m, d, h, 0, 0).single().expect("valid instant")
    }

    #[test]
    fn renders_a_date_token() {
        let out = render(
            "https://files.tmdb.org/p/exports/movie_ids_{{ date:%m_%d_%Y }}.json.gz",
            at(2026, 9, 13, 9),
            "UTC",
        )
        .expect("renders");
        assert_eq!(
            out,
            "https://files.tmdb.org/p/exports/movie_ids_09_13_2026.json.gz"
        );
    }

    #[test]
    fn renders_a_negative_day_offset() {
        // A schedule firing at 00:30 must fetch the PREVIOUS day's export, because the
        // current day's file does not exist until ~08:00 UTC.
        let out = render("x/{{ date-1d:%Y-%m-%d }}.json", at(2026, 9, 13, 0), "UTC")
            .expect("renders");
        assert_eq!(out, "x/2026-09-12.json");
    }

    #[test]
    fn renders_positive_and_hour_offsets() {
        assert_eq!(
            render("{{ date+2d:%Y-%m-%d }}", at(2026, 9, 13, 0), "UTC").expect("renders"),
            "2026-09-15"
        );
        assert_eq!(
            render("{{ date-3h:%Y-%m-%dT%H }}", at(2026, 9, 13, 2), "UTC").expect("renders"),
            "2026-09-12T23"
        );
    }

    #[test]
    fn renders_timestamp() {
        let out = render("{{ timestamp }}", at(2026, 9, 13, 0), "UTC").expect("renders");
        assert_eq!(out, at(2026, 9, 13, 0).timestamp().to_string());
    }

    #[test]
    fn renders_in_the_sources_timezone() {
        // 2026-09-13T01:00Z is still 2026-09-12 in Los Angeles.
        let out = render("{{ date:%Y-%m-%d }}", at(2026, 9, 13, 1), "America/Los_Angeles")
            .expect("renders");
        assert_eq!(out, "2026-09-12");
    }

    #[test]
    fn multiple_tokens_and_whitespace_variants() {
        let out = render(
            "{{date:%Y}}/{{ date:%m }}/{{  date:%d  }}",
            at(2026, 9, 13, 0),
            "UTC",
        )
        .expect("renders");
        assert_eq!(out, "2026/09/13");
    }

    #[test]
    fn a_template_without_tokens_is_returned_unchanged() {
        let out = render("https://example.test/feed.json", at(2026, 9, 13, 0), "UTC")
            .expect("renders");
        assert_eq!(out, "https://example.test/feed.json");
    }

    #[test]
    fn unknown_tokens_are_rejected_not_passed_through() {
        assert!(render("{{ nope }}", at(2026, 9, 13, 0), "UTC").is_err());
        assert!(render("{{ date }}", at(2026, 9, 13, 0), "UTC").is_err());
        assert!(render("{{ date-1x:%Y }}", at(2026, 9, 13, 0), "UTC").is_err());
    }

    #[test]
    fn unknown_timezone_is_rejected() {
        assert!(render("{{ date:%Y }}", at(2026, 9, 13, 0), "Mars/Olympus").is_err());
    }

    #[test]
    fn unterminated_token_is_rejected() {
        assert!(render("{{ date:%Y", at(2026, 9, 13, 0), "UTC").is_err());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-source template`
Expected: FAIL — `cannot find function render`.

- [ ] **Step 3: Implement rendering**

Add `chrono.workspace = true` and `chrono-tz.workspace = true` to `crates/source/Cargo.toml`. Add `pub mod template;` and `pub use template::render;` to `lib.rs`. Add to `SourceError`:

```rust
    /// A URL template could not be rendered.
    #[error("url template: {0}")]
    Template(String),
```

Write above the tests in `template.rs`:

```rust
//! URL templating.
//!
//! A source's URL is rendered against the run's **scheduled** time, never wall-clock:
//! a retry must resolve the same URL, and a backfill of 2026-09-01 must fetch that
//! day's file. See Decision 9 in the design doc.
//!
//! | Token | Expands to |
//! |---|---|
//! | `{{ date:FMT }}` | `strftime(FMT)` of the scheduled time |
//! | `{{ date-1d:FMT }}` | Same, offset by a duration (`-1d`, `+2d`, `-3h`, `+30m`) |
//! | `{{ timestamp }}` | Unix seconds of the scheduled time |

use chrono::{DateTime, Duration, Utc};
use chrono_tz::Tz;

use crate::SourceError;

/// Render every `{{ … }}` token in `template`.
///
/// Unknown tokens are an error rather than a silent passthrough: a typo in a date
/// format should fail at source-create time, not fetch a 404 every night forever.
pub fn render(
    template: &str,
    scheduled_at: DateTime<Utc>,
    timezone: &str,
) -> Result<String, SourceError> {
    let tz: Tz = timezone
        .parse()
        .map_err(|_| SourceError::Template(format!("unknown timezone {timezone:?}")))?;

    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        out.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let end = after
            .find("}}")
            .ok_or_else(|| SourceError::Template("unterminated {{ token".into()))?;
        out.push_str(&expand(after[..end].trim(), scheduled_at, tz)?);
        rest = &after[end + 2..];
    }
    out.push_str(rest);
    Ok(out)
}

/// Expand one token's inner text (already trimmed of surrounding whitespace).
fn expand(token: &str, at: DateTime<Utc>, tz: Tz) -> Result<String, SourceError> {
    if token == "timestamp" {
        return Ok(at.timestamp().to_string());
    }
    let (head, fmt) = token
        .split_once(':')
        .ok_or_else(|| SourceError::Template(format!("unknown token {{{{ {token} }}}}")))?;
    let head = head.trim();
    let fmt = fmt.trim();
    let offset = match head.strip_prefix("date") {
        Some("") => Duration::zero(),
        Some(raw) => parse_offset(raw)?,
        None => {
            return Err(SourceError::Template(format!(
                "unknown token {{{{ {token} }}}}"
            )));
        }
    };
    let shifted = at + offset;
    Ok(shifted.with_timezone(&tz).format(fmt).to_string())
}

/// Parse an offset such as `-1d`, `+2d`, `-3h`, `+30m`.
fn parse_offset(raw: &str) -> Result<Duration, SourceError> {
    let bad = || SourceError::Template(format!("invalid offset {raw:?}; expected e.g. -1d, +3h"));
    let (sign, rest) = match raw.split_at(1) {
        ("-", rest) => (-1, rest),
        ("+", rest) => (1, rest),
        _ => return Err(bad()),
    };
    let (digits, unit) = rest.split_at(rest.len().saturating_sub(1));
    let n: i64 = digits.parse().map_err(|_| bad())?;
    let magnitude = match unit {
        "d" => Duration::try_days(n),
        "h" => Duration::try_hours(n),
        "m" => Duration::try_minutes(n),
        _ => return Err(bad()),
    }
    .ok_or_else(bad)?;
    Ok(magnitude * sign)
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p meili-ingest-source template
cargo clippy -p meili-ingest-source -- -D warnings
```
Expected: 10 tests pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/source Cargo.toml Cargo.lock
git commit -m "feat(source): url templating against the scheduled time"
```

---

### Task 5: Source model types

**Files:**
- Create: `crates/source/src/model.rs`
- Modify: `crates/source/src/lib.rs`

**Interfaces:**
- Consumes: `SourceError`.
- Produces — every later task binds these exact names:
  - `enum Location { Url { url: String, method: Option<String>, headers: BTreeMap<String,String> } }` — `#[serde(tag = "kind", rename_all = "snake_case")]`
  - `enum FetchAuth { Bearer { token: String }, Basic { username: String, password: String }, Headers { headers: BTreeMap<String,String> } }` — same tagging
  - `struct IncrementalState { etag: Option<String>, last_modified: Option<String>, hash: Option<String> }` (hash hex-encoded so it serializes through JSON/Temporal payloads)
  - `struct SourceDefinition { id: Uuid, uid: String, name: String, description: Option<String>, project_id: Option<String>, pipeline_uid: String, location: Location, cron: String, timezone: String, paused: bool, index_name: Option<String>, schedule_id: String, archived_at: Option<DateTime<Utc>> }`
  - `enum RunOutcome { Unchanged, Ingested, Failed }` with `as_str()` / `FromStr`
  - `fn redact(auth: &FetchAuth) -> serde_json::Value`

- [ ] **Step 1: Write the failing test**

Create `crates/source/src/model.rs` with only the tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn location_is_tagged_by_kind() {
        let loc = Location::Url {
            url: "https://example.test/a.json".into(),
            method: None,
            headers: BTreeMap::new(),
        };
        let json = serde_json::to_value(&loc).expect("serialize");
        assert_eq!(json["kind"], "url");
        assert_eq!(json["url"], "https://example.test/a.json");
        let back: Location = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, loc);
    }

    #[test]
    fn an_unknown_location_kind_is_rejected() {
        // v2 connectors add variants; until then an unknown kind must not silently
        // deserialize into the url variant.
        let json = serde_json::json!({ "kind": "bucket", "uri": "s3://b/p/" });
        assert!(serde_json::from_value::<Location>(json).is_err());
    }

    #[test]
    fn fetch_auth_is_tagged_by_kind() {
        let auth = FetchAuth::Bearer { token: "t0ken".into() };
        let json = serde_json::to_value(&auth).expect("serialize");
        assert_eq!(json["kind"], "bearer");
        let back: FetchAuth = serde_json::from_value(json).expect("deserialize");
        assert_eq!(back, auth);
    }

    #[test]
    fn redact_never_exposes_a_secret() {
        for auth in [
            FetchAuth::Bearer { token: "t0ken".into() },
            FetchAuth::Basic { username: "u".into(), password: "p4ss".into() },
            FetchAuth::Headers {
                headers: BTreeMap::from([("X-Api-Key".to_string(), "k3y".to_string())]),
            },
        ] {
            let rendered = serde_json::to_string(&redact(&auth)).expect("serialize");
            assert!(rendered.contains("****"), "{rendered} must be masked");
            for secret in ["t0ken", "p4ss", "k3y"] {
                assert!(
                    !rendered.contains(secret),
                    "{rendered} leaked {secret}"
                );
            }
        }
    }

    #[test]
    fn redact_keeps_the_kind_and_non_secret_names() {
        let auth = FetchAuth::Headers {
            headers: BTreeMap::from([("X-Api-Key".to_string(), "k3y".to_string())]),
        };
        let json = redact(&auth);
        assert_eq!(json["kind"], "headers");
        // The header NAME is not a secret and is useful when editing.
        assert_eq!(json["headers"]["X-Api-Key"], "****");
    }

    #[test]
    fn run_outcome_roundtrips_through_its_string() {
        for o in [RunOutcome::Unchanged, RunOutcome::Ingested, RunOutcome::Failed] {
            assert_eq!(o.as_str().parse::<RunOutcome>().expect("parses"), o);
        }
        assert!("nonsense".parse::<RunOutcome>().is_err());
    }

    #[test]
    fn incremental_state_defaults_to_empty() {
        let s = IncrementalState::default();
        assert!(s.etag.is_none() && s.last_modified.is_none() && s.hash.is_none());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-source model`
Expected: FAIL — `cannot find type Location`.

- [ ] **Step 3: Implement the model**

Add `uuid.workspace = true` to `crates/source/Cargo.toml`. Add `pub mod model;` plus re-exports to `lib.rs`. Write above the tests in `model.rs`:

```rust
//! The types a source is made of.
//!
//! [`Location`] is a tagged union from day one so the v2 connectors (`zip`, `bucket`,
//! `api`) need no migration — only a new variant.

use std::collections::BTreeMap;
use std::str::FromStr;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::SourceError;

/// Where a source's content comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Location {
    /// One HTTP(S) URL, possibly templated. The only variant in v1.
    Url {
        /// URL, rendered through `crate::template::render` before fetching.
        url: String,
        /// HTTP method; `GET` when absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        method: Option<String>,
        /// Non-secret headers. Secrets belong in [`FetchAuth`].
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        headers: BTreeMap<String, String>,
    },
}

/// How a source authenticates. Always stored sealed; never returned by the API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FetchAuth {
    /// `Authorization: Bearer <token>`.
    Bearer {
        /// The token.
        token: String,
    },
    /// HTTP basic auth.
    Basic {
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
    /// Arbitrary secret headers, e.g. `X-Api-Key`.
    Headers {
        /// Header name → value.
        headers: BTreeMap<String, String>,
    },
}

/// Masked view of a credential, safe to return from the API and to log.
///
/// Header *names* are preserved because they are not secret and make the edit form
/// usable; every value becomes `****`.
pub fn redact(auth: &FetchAuth) -> serde_json::Value {
    match auth {
        FetchAuth::Bearer { .. } => serde_json::json!({ "kind": "bearer", "token": "****" }),
        FetchAuth::Basic { username, .. } => serde_json::json!({
            "kind": "basic",
            "username": username,
            "password": "****",
        }),
        FetchAuth::Headers { headers } => {
            let masked: BTreeMap<&str, &str> =
                headers.keys().map(|k| (k.as_str(), "****")).collect();
            serde_json::json!({ "kind": "headers", "headers": masked })
        }
    }
}

/// What the previous run learned about the upstream, used to skip unchanged content.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct IncrementalState {
    /// Value of the last `ETag` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etag: Option<String>,
    /// Value of the last `Last-Modified` response header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_modified: Option<String>,
    /// Hex-encoded blake3 of the last body. Hex rather than bytes so it survives JSON
    /// and Temporal payloads unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hash: Option<String>,
}

/// A source as the control plane stores it, minus its sealed secrets.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceDefinition {
    /// Surrogate id, also the Temporal schedule's suffix.
    pub id: Uuid,
    /// Handle, unique per project.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Optional description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tenant scope; `None` = global.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Pipeline this source feeds.
    pub pipeline_uid: String,
    /// Where the content comes from.
    pub location: Location,
    /// Cron expression, validated by Temporal on create (Decision 10).
    pub cron: String,
    /// IANA timezone for the cron and for date templating.
    #[serde(default = "utc")]
    pub timezone: String,
    /// Whether the schedule is paused.
    #[serde(default)]
    pub paused: bool,
    /// Index override; falls back to the pipeline's pattern then the deployment default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Temporal schedule id.
    pub schedule_id: String,
    /// Set when the source's pipeline was deleted; archived sources never fire.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub archived_at: Option<DateTime<Utc>>,
}

fn utc() -> String {
    "UTC".to_string()
}

/// Terminal outcome of one source run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunOutcome {
    /// Upstream was byte-identical; no job was created and nothing was billed.
    Unchanged,
    /// At least one job was started.
    Ingested,
    /// The run failed before starting any job.
    Failed,
}

impl RunOutcome {
    /// Stable string stored in `source_runs.outcome`.
    pub fn as_str(self) -> &'static str {
        match self {
            RunOutcome::Unchanged => "unchanged",
            RunOutcome::Ingested => "ingested",
            RunOutcome::Failed => "failed",
        }
    }
}

impl FromStr for RunOutcome {
    type Err = SourceError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "unchanged" => Ok(RunOutcome::Unchanged),
            "ingested" => Ok(RunOutcome::Ingested),
            "failed" => Ok(RunOutcome::Failed),
            other => Err(SourceError::Payload(format!("unknown run outcome {other:?}"))),
        }
    }
}
```

Re-export from `lib.rs`:

```rust
pub mod model;
pub use model::{FetchAuth, IncrementalState, Location, RunOutcome, SourceDefinition, redact};
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p meili-ingest-source model
cargo clippy -p meili-ingest-source -- -D warnings
```
Expected: 7 tests pass.

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/source
git commit -m "feat(source): location, credential and run-outcome model"
```

---

### Task 6: `SourceConnector` trait and `UrlConnector`

**Files:**
- Create: `crates/source/src/connector.rs`, `crates/source/src/url.rs`
- Modify: `crates/source/src/lib.rs`, `crates/source/Cargo.toml` (add `reqwest`, `blake3`, `flate2`, `async-trait`, `meili-ingest-blob`; dev-dep `wiremock`)

**Interfaces:**
- Consumes: `Location`, `FetchAuth`, `IncrementalState` (Task 5); `UrlGuard` (Task 3); `render` (Task 4).
- Produces:
  - `enum Resolution { Unchanged, Items { items: Vec<ResolvedItem>, state: IncrementalState } }`
  - `struct ResolvedItem { bytes: Vec<u8>, mime: String, filename: Option<String> }`
  - `struct ResolveRuntime { http: reqwest::Client, guard: UrlGuard, scheduled_at: DateTime<Utc> }`
  - `trait SourceConnector { fn kind(&self) -> &'static str; async fn resolve(&self, loc:&Location, auth: Option<&FetchAuth>, state:&IncrementalState, rt:&ResolveRuntime) -> Result<Resolution, SourceError>; }`
  - `struct UrlConnector;`

> **Note on `ResolvedItem`:** it carries bytes, not a staged ref. Staging into the blob store is the *worker activity's* job (Task 12) — keeping it out of this crate is what lets every connector test run with no object store. The activity stages each item and converts it to `PluginInput::Ref(ContentRef::Staged{..})` before the workflow ever sees it.

- [ ] **Step 1: Write the failing test**

Create `crates/source/src/url.rs` with only the tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// The guard rejects non-public addresses, and a MockServer listens on 127.0.0.1.
    /// Tests therefore use a permissive runtime; the guard has its own tests in guard.rs.
    fn rt_without_guard(scheduled_at: DateTime<Utc>) -> ResolveRuntime {
        ResolveRuntime {
            http: reqwest::Client::new(),
            guard: None,
            scheduled_at,
        }
    }

    fn now() -> DateTime<Utc> {
        chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 9, 13, 9, 0, 0)
            .single()
            .expect("valid instant")
    }

    fn url_location(url: String) -> Location {
        Location::Url { url, method: None, headers: Default::default() }
    }

    fn gzip(bytes: &[u8]) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        e.write_all(bytes).expect("gzip write");
        e.finish().expect("gzip finish")
    }

    #[tokio::test]
    async fn a_304_resolves_to_unchanged() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/feed.json"))
            .and(header("if-none-match", "\"v1\""))
            .respond_with(ResponseTemplate::new(304))
            .mount(&server)
            .await;

        let state = IncrementalState { etag: Some("\"v1\"".into()), ..Default::default() };
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                None,
                &state,
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");
        assert!(matches!(got, Resolution::Unchanged));
    }

    #[tokio::test]
    async fn changed_content_resolves_to_one_item() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_string("{\"id\":1}")
                    .insert_header("content-type", "application/json")
                    .insert_header("etag", "\"v2\""),
            )
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");

        let Resolution::Items { items, state } = got else {
            panic!("expected items");
        };
        assert_eq!(items.len(), 1, "one url is one item, whatever it contains");
        assert_eq!(items[0].bytes, b"{\"id\":1}");
        assert_eq!(items[0].mime, "application/json");
        assert_eq!(state.etag.as_deref(), Some("\"v2\""));
        assert!(state.hash.is_some(), "hash is recorded for etag-less servers");
    }

    #[tokio::test]
    async fn an_identical_body_without_an_etag_is_unchanged_by_hash() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("same bytes"))
            .mount(&server)
            .await;
        let loc = url_location(format!("{}/feed.json", server.uri()));

        let first = UrlConnector
            .resolve(&loc, None, &IncrementalState::default(), &rt_without_guard(now()))
            .await
            .expect("first");
        let Resolution::Items { state, .. } = first else {
            panic!("expected items");
        };

        let second = UrlConnector
            .resolve(&loc, None, &state, &rt_without_guard(now()))
            .await
            .expect("second");
        assert!(
            matches!(second, Resolution::Unchanged),
            "identical bytes must not re-ingest"
        );
    }

    #[tokio::test]
    async fn gzip_is_decompressed_and_typed_from_the_inner_content() {
        let server = MockServer::start().await;
        // Mirrors the TMDB export: NDJSON, gzipped, served as a .gz file.
        let body = gzip(b"{\"id\":1}\n{\"id\":2}\n");
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(body)
                    .insert_header("content-type", "application/octet-stream"),
            )
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/movie_ids.json.gz", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");

        let Resolution::Items { items, .. } = got else {
            panic!("expected items");
        };
        assert_eq!(items[0].bytes, b"{\"id\":1}\n{\"id\":2}\n", "decompressed");
        assert_eq!(
            items[0].mime, "application/x-ndjson",
            "typed from the decompressed content so the json plugin routes it"
        );
        assert_eq!(items[0].filename.as_deref(), Some("movie_ids.json"));
    }

    #[tokio::test]
    async fn the_url_template_is_rendered_before_fetching() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/exports/movie_ids_09_13_2026.json"))
            .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!(
                    "{}/exports/movie_ids_{{{{ date:%m_%d_%Y }}}}.json",
                    server.uri()
                )),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await
            .expect("resolves");
        assert!(matches!(got, Resolution::Items { .. }));
    }

    #[tokio::test]
    async fn bearer_auth_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(header("authorization", "Bearer t0ken"))
            .respond_with(ResponseTemplate::new(200).set_body_string("[]"))
            .mount(&server)
            .await;

        let auth = FetchAuth::Bearer { token: "t0ken".into() };
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                Some(&auth),
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await;
        assert!(got.is_ok(), "the mock only matches when the header was sent");
    }

    #[tokio::test]
    async fn a_non_success_status_is_an_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&server)
            .await;

        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/feed.json", server.uri())),
                None,
                &IncrementalState::default(),
                &rt_without_guard(now()),
            )
            .await;
        assert!(got.is_err());
    }

    #[tokio::test]
    async fn an_oversized_body_is_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(vec![b'x'; 4096]))
            .mount(&server)
            .await;

        let mut rt = rt_without_guard(now());
        rt.guard = Some(UrlGuard { max_redirects: 5, max_bytes: 1024 });
        // A guard with a tiny cap; the scheme check is skipped for the http mock by
        // `enforce_address_policy = false` inside ResolveRuntime.
        let got = UrlConnector
            .resolve(
                &url_location(format!("{}/big", server.uri())),
                None,
                &IncrementalState::default(),
                &rt,
            )
            .await;
        assert!(got.is_err(), "body over max_bytes must be rejected");
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-source url`
Expected: FAIL — `cannot find type UrlConnector`.

- [ ] **Step 3: Implement the connector**

Add to `crates/source/Cargo.toml`:

```toml
async-trait.workspace = true
blake3.workspace = true
bytes.workspace = true
flate2.workspace = true
futures.workspace = true
reqwest.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
wiremock.workspace = true
```

Create `crates/source/src/connector.rs`:

```rust
//! The connector abstraction.
//!
//! A connector turns a [`Location`] into zero or more **source items**. An item is a
//! file, not a document: one TMDB export is a single item that later yields ~600k
//! documents, so the workflow's fan-out cap never applies to a source's document count
//! (Decision 7).
//!
//! v1 ships only [`crate::url::UrlConnector`]. `Resolution::Items` is a `Vec` from the
//! start so a future bucket connector returning 400 objects needs no change here or in
//! the scheduler.

use chrono::{DateTime, Utc};

use crate::guard::UrlGuard;
use crate::model::{FetchAuth, IncrementalState, Location};
use crate::SourceError;

/// One fetched item, before it is staged into the blob store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedItem {
    /// Decompressed content.
    pub bytes: Vec<u8>,
    /// MIME of the content as it will be routed.
    pub mime: String,
    /// Filename hint, used for MIME detection and provenance.
    pub filename: Option<String>,
}

/// Outcome of resolving a location.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Resolution {
    /// Upstream is byte-identical to the previous run. No job, no usage.
    Unchanged,
    /// Items to ingest, plus the state to persist for the next run.
    Items {
        /// One entry per source item.
        items: Vec<ResolvedItem>,
        /// Etag / last-modified / hash learned from this fetch.
        state: IncrementalState,
    },
}

/// Ambient services a connector needs.
#[derive(Debug, Clone)]
pub struct ResolveRuntime {
    /// Shared HTTP client.
    pub http: reqwest::Client,
    /// Address policy and caps. `None` disables address checks — used only by tests
    /// against a loopback mock server; production always sets it.
    pub guard: Option<UrlGuard>,
    /// The run's scheduled time, used for URL templating (Decision 9).
    pub scheduled_at: DateTime<Utc>,
}

/// Turns a location into items.
#[async_trait::async_trait]
pub trait SourceConnector: Send + Sync {
    /// The `Location` variant this connector handles, e.g. `"url"`.
    fn kind(&self) -> &'static str;

    /// Resolve `loc`, honouring the previous run's `state`.
    async fn resolve(
        &self,
        loc: &Location,
        auth: Option<&FetchAuth>,
        state: &IncrementalState,
        rt: &ResolveRuntime,
    ) -> Result<Resolution, SourceError>;
}
```

Write above the tests in `crates/source/src/url.rs`:

```rust
//! The `url` connector: one URL, one item.

use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use std::io::Read as _;

use crate::connector::{ResolveRuntime, Resolution, ResolvedItem, SourceConnector};
use crate::guard::UrlGuard;
use crate::model::{FetchAuth, IncrementalState, Location};
use crate::template::render;
use crate::SourceError;

/// Fetches one URL.
#[derive(Debug, Clone, Copy, Default)]
pub struct UrlConnector;

#[async_trait::async_trait]
impl SourceConnector for UrlConnector {
    fn kind(&self) -> &'static str {
        "url"
    }

    async fn resolve(
        &self,
        loc: &Location,
        auth: Option<&FetchAuth>,
        state: &IncrementalState,
        rt: &ResolveRuntime,
    ) -> Result<Resolution, SourceError> {
        let Location::Url { url, method, headers } = loc;
        let rendered = render(url, rt.scheduled_at, "UTC")?;
        let parsed = url::Url::parse(&rendered)
            .map_err(|e| SourceError::Template(format!("{rendered:?} is not a url: {e}")))?;

        if let Some(guard) = &rt.guard {
            guard.check_url(&parsed).await?;
        }
        let max_bytes = rt.guard.map(|g| g.max_bytes).unwrap_or(u64::MAX);

        let verb = method.as_deref().unwrap_or("GET");
        let mut req = rt
            .http
            .request(
                reqwest::Method::from_bytes(verb.as_bytes())
                    .map_err(|_| SourceError::Blocked(format!("invalid http method {verb:?}")))?,
                parsed.clone(),
            )
            .timeout(std::time::Duration::from_secs(300));

        for (name, value) in headers {
            req = req.header(name, value);
        }
        req = apply_auth(req, auth);
        if let Some(etag) = &state.etag {
            req = req.header(reqwest::header::IF_NONE_MATCH, etag);
        }
        if let Some(lm) = &state.last_modified {
            req = req.header(reqwest::header::IF_MODIFIED_SINCE, lm);
        }

        let response = req
            .send()
            .await
            .map_err(|e| SourceError::Fetch(format!("GET {parsed}: {e}")))?;

        if response.status() == reqwest::StatusCode::NOT_MODIFIED {
            return Ok(Resolution::Unchanged);
        }
        if !response.status().is_success() {
            return Err(SourceError::Fetch(format!(
                "GET {parsed}: status {}",
                response.status()
            )));
        }

        let etag = header_string(&response, reqwest::header::ETAG);
        let last_modified = header_string(&response, reqwest::header::LAST_MODIFIED);
        let header_mime = header_string(&response, reqwest::header::CONTENT_TYPE)
            .and_then(|ct| ct.split(';').next().map(|s| s.trim().to_ascii_lowercase()))
            .filter(|m| !m.is_empty() && m != "application/octet-stream");

        // Stream rather than `.bytes()`: a TMDB export is ~50 MB and several sources may
        // run at once. The cap is enforced DURING the stream, not after.
        let mut hasher = blake3::Hasher::new();
        let mut body: Vec<u8> = Vec::new();
        let mut stream = response.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|e| SourceError::Fetch(format!("reading {parsed}: {e}")))?;
            if body.len() as u64 + chunk.len() as u64 > max_bytes {
                return Err(SourceError::Fetch(format!(
                    "GET {parsed}: body exceeds the {max_bytes} byte cap"
                )));
            }
            hasher.update(&chunk);
            body.extend_from_slice(&chunk);
        }
        let hash = hasher.finalize().to_hex().to_string();

        // Servers hosting static exports commonly send no ETag; the hash is what makes
        // the conditional-fetch decision work for them.
        if state.hash.as_deref() == Some(hash.as_str()) {
            return Ok(Resolution::Unchanged);
        }

        let filename = last_segment(parsed.path());
        let (bytes, filename) = maybe_gunzip(body, filename)?;
        let mime = header_mime
            .filter(|_| true)
            .or_else(|| filename.as_deref().and_then(guess_mime))
            .unwrap_or_else(|| "application/octet-stream".to_string());

        Ok(Resolution::Items {
            items: vec![ResolvedItem { bytes, mime, filename }],
            state: IncrementalState { etag, last_modified, hash: Some(hash) },
        })
    }
}

/// Attach a credential to the request.
fn apply_auth(
    req: reqwest::RequestBuilder,
    auth: Option<&FetchAuth>,
) -> reqwest::RequestBuilder {
    match auth {
        None => req,
        Some(FetchAuth::Bearer { token }) => req.bearer_auth(token),
        Some(FetchAuth::Basic { username, password }) => req.basic_auth(username, Some(password)),
        Some(FetchAuth::Headers { headers }) => {
            let mut req = req;
            for (name, value) in headers {
                req = req.header(name, value);
            }
            req
        }
    }
}

fn header_string(response: &reqwest::Response, name: reqwest::header::HeaderName) -> Option<String> {
    response
        .headers()
        .get(name)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
}

fn last_segment(path: &str) -> Option<String> {
    path.rsplit('/').find(|s| !s.is_empty()).map(str::to_owned)
}

/// Decompress when the body is gzip, detected by magic bytes rather than by filename —
/// a `.gz` extension is only a hint, and `Content-Encoding` does not apply to a gzipped
/// *file* served as octet-stream (which is exactly the TMDB case).
fn maybe_gunzip(
    body: Vec<u8>,
    filename: Option<String>,
) -> Result<(Vec<u8>, Option<String>), SourceError> {
    if body.len() < 2 || body[0] != 0x1f || body[1] != 0x8b {
        return Ok((body, filename));
    }
    let mut out = Vec::new();
    flate2::read::GzDecoder::new(&body[..])
        .read_to_end(&mut out)
        .map_err(|e| SourceError::Fetch(format!("gunzip: {e}")))?;
    // Strip the .gz so MIME detection sees the real extension.
    let stripped = filename.map(|f| f.strip_suffix(".gz").map(str::to_owned).unwrap_or(f));
    Ok((out, stripped))
}

/// Minimal extension → MIME map for the formats a source realistically serves.
fn guess_mime(filename: &str) -> Option<String> {
    let ext = filename.rsplit_once('.')?.1.to_ascii_lowercase();
    let mime = match ext.as_str() {
        "json" => "application/json",
        "ndjson" | "jsonl" => "application/x-ndjson",
        "csv" => "text/csv",
        "xml" => "application/xml",
        "md" => "text/markdown",
        "html" | "htm" => "text/html",
        "parquet" => "application/vnd.apache.parquet",
        "avro" => "application/vnd.apache.avro",
        "pdf" => "application/pdf",
        _ => return None,
    };
    Some(mime.to_string())
}
```

> **Careful:** the TMDB file is `movie_ids_09_13_2026.json.gz`. After `maybe_gunzip` strips `.gz` the filename ends `.json`, which `guess_mime` maps to `application/json`. The test asserts `application/x-ndjson`, because the content is newline-delimited. Resolve this by having `guess_mime` return `application/x-ndjson` for `.json` **only when the body's first line parses as a JSON object and a second line follows** — implement that as a `sniff_ndjson(&[u8]) -> bool` helper and prefer its verdict over the extension. Add a unit test for `sniff_ndjson` covering: a JSON array (false), a single JSON object (false), two newline-delimited objects (true), and empty input (false).

Add to `SourceError`:

```rust
    /// The upstream fetch failed.
    #[error("fetch: {0}")]
    Fetch(String),
```

Add to `lib.rs`:

```rust
pub mod connector;
pub mod url;
pub use connector::{ResolveRuntime, Resolution, ResolvedItem, SourceConnector};
pub use url::UrlConnector;
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cargo test -p meili-ingest-source
cargo clippy -p meili-ingest-source -- -D warnings
```
Expected: all tests pass (8 in `url`, plus `sniff_ndjson`, plus earlier modules).

- [ ] **Step 5: Commit**

```bash
cargo fmt
git add crates/source Cargo.toml Cargo.lock
git commit -m "feat(source): url connector with conditional fetch and gzip"
```

---

## Remaining tasks

Tasks 7–15 (control plane repo + routes, pipeline-delete cascade, Temporal schedule client, gateway routes, `SourceRunWorkflow` + activities, worker registration, UI, docs) follow the same TDD shape and are written out in the sections below.

**Before executing them, re-read the spec's API and Workflow sections** — they define the eight routes and the five workflow steps that those tasks implement.

### Task 7: `SourceRepo` in the control plane

**Files:**
- Create: `crates/control-plane/src/sources.rs`
- Modify: `crates/control-plane/src/lib.rs`

**Interfaces:**
- Consumes: `SourceDefinition`, `Location`, `RunOutcome` (Task 5); migration (Task 1).
- Produces: `SourceRepo` with `list(project_id, include_archived) -> Vec<SourceDefinition>`, `get(uid, project_id) -> Option<SourceRow>`, `insert(&NewSource) -> SourceDefinition`, `update(&SourcePatch) -> SourceDefinition`, `delete(uid, project_id) -> bool`, `set_paused(id, bool)`, `archive_for_pipeline(pipeline_uid, project_id) -> Vec<Uuid>`, `record_run(&RunRecord)`, `list_runs(source_id, limit) -> Vec<RunRecord>`, `load_for_run(id) -> Option<SourceWithSecrets>`, `save_state(id, &IncrementalState)`.
- `SourceRow` exposes the sealed `fetch_auth: Option<Vec<u8>>` and `meili_ctx: Vec<u8>` columns; **the repo never decrypts** — only the worker activity holds a `SecretKey`.

Follow `PipelineRepo` in `crates/control-plane/src/pipelines.rs` exactly: a `#[derive(sqlx::FromRow)]` row struct, an `into_definition()` conversion, `project_id IS NULL OR project_id = $1` scoping, and `CpError` for errors. Write the tests as `#[sqlx::test]`-style integration tests in `crates/control-plane/tests/sources_repo.rs` guarded by `DATABASE_URL` exactly like Task 1's test, covering: insert-then-get round-trip; uid uniqueness per project (the same uid global and tenant-scoped both insert); `list` excludes archived unless asked; `archive_for_pipeline` stamps `archived_at` and returns the ids; `record_run` then `list_runs` returns newest first.

### Task 8: `/internal/sources` routes in the control plane

**Files:** Modify `crates/control-plane/src/sources.rs`, `crates/control-plane/src/lib.rs`.

Mirror the `/pipelines` handlers: `list_sources`, `create_source`, `get_source`, `patch_source`, `delete_source`, `record_run`, `list_runs`, `load_source_for_run`. Register under `/internal/sources` in the control plane's `Router`. Tests use `axum::body::Body` + `tower::ServiceExt::oneshot` against the router, as the existing control-plane tests do.

### Task 9: Pipeline delete cascades to archive

**Files:** Modify `crates/control-plane/src/pipelines.rs`.

`PipelineRepo::delete` gains a call to `SourceRepo::archive_for_pipeline` inside the same transaction, returning the archived source ids so the caller can delete their Temporal schedules. Test: create a pipeline + two sources referencing it, delete the pipeline, assert both sources have `archived_at` set and are excluded from `list`, and that the delete itself succeeded (it must **never** return 409 — Decision from the spec's API section).

### Task 10: Temporal schedule client

**Files:** Create `crates/gateway/src/schedules.rs`; modify `crates/gateway/src/state.rs`.

Wrap `temporalio_client`'s schedule API behind a `ScheduleClient` trait (mirroring how `WorkflowStarter` is a trait in `state.rs` so handlers are testable without a server): `create(&ScheduleSpecInput) -> Result<String>`, `update`, `delete`, `pause`, `unpause`, `trigger`, `describe -> Option<ScheduleInfo>` where `ScheduleInfo { next_run_at: Option<DateTime<Utc>>, paused: bool }`. The real impl uses `CreateScheduleOptions` with `ScheduleSpec::cron_expressions`, `ScheduleOverlapPolicy::Skip`, and a `ScheduleAction::StartWorkflow` naming workflow type `SourceRunWorkflow` on task queue `workers-general` with input `{ source_id, project_id }`. Provide a `FakeScheduleClient` in `#[cfg(test)]` recording calls.

### Task 11: `/sources` routes on the gateway

**Files:** Create `crates/gateway/src/handlers/sources.rs`; modify `crates/gateway/src/handlers/mod.rs`, `crates/gateway/src/lib.rs`, `crates/gateway/src/state.rs`.

The eight routes from the spec. `POST /sources` resolves `MeiliContext` via `resolve_context` (`crates/gateway/src/context.rs`), seals it with `SecretKey`, writes the row `paused = true`, creates the schedule, then unpauses — the non-atomic order the spec justifies. **When `SecretKey::from_env()` returns `None`, every `/sources` route returns `GatewayError::NotImplemented`** (501) — the variant already exists in `crates/gateway/src/error.rs`. Tests: 501 without a key; create returns the definition with `auth` masked as `****`; `GET` never includes a decrypted value; `PATCH` omitting `auth` preserves the stored secret while `"auth": null` clears it; delete removes the schedule before the row.

### Task 12: `SourceRunWorkflow` and its activities

**Files:** Create `crates/worker/src/source_workflow.rs`, `crates/worker/src/source_activity.rs`; modify `crates/worker/src/lib.rs`, `crates/worker/src/main.rs`.

Activities (`load_source`, `resolve_source`, `record_run`) do all I/O; the workflow stays deterministic, matching the constraint documented at the top of `crates/worker/src/workflow.rs`. `resolve_source` is where a `ResolvedItem` is staged into the blob store and becomes `PluginInput::Ref(ContentRef::Staged{..})` — bytes never enter workflow history. The workflow starts one `PipelineWorkflow` child per item with a fresh `Uuid::new_v4()` job id. Register with `.register_workflow::<SourceRunWorkflow>()?` next to the existing line in `main.rs`. Tests use the Temporal test environment: unchanged → zero children and one `unchanged` run row; N items → N children with distinct job ids; a failing resolve → `failed` recorded and the ETag not advanced.

### Task 13: UI

**Files:** Create `ui/src/app/sources/page.tsx`, `ui/src/app/sources/_components/*`, `ui/src/app/sources/_lib/*`.

Mirror `ui/src/app/pipelines/`. TanStack Query for all fetching, react-hook-form + zod for the form, shadcn/ui components from `components/ui/`, lucide-react icons, `cn()` for conditional classes, Sonner for toasts. Invalidate the sources query after every mutation. Secret fields render as write-only inputs showing `****` when set. Co-locate components under `_components/`. Unit-test the pure helpers (cron description, next-run formatting) under `_lib/` with the same vitest setup the usage page's `_lib` tests use.

### Task 14: Docs

**Files:** Create `docs/concepts/sources.mdx`; modify `docs/openapi.yaml`, `docs/mint.json`, `README.md`.

The concepts page documents cron syntax, the templating table from the spec, the security model, and a full TMDB walkthrough ending in a working `POST /sources` body.

### Task 15: End-to-end

**Files:** Modify `scripts/e2e.sh`.

Create a source against a local fixture server, `POST /sources/{uid}/run`, assert a job appears and documents land in Meilisearch, then trigger again and assert the second run is `unchanged` with no new job.

---

## Self-Review

**Spec coverage.** Data model → Task 1. Connector interface → Task 6. `SourceRunWorkflow` → Task 12. Security/secrets → Task 2, enforced at the route in Task 11. SSRF guard → Task 3. Templating → Task 4 (added after the TMDB example; Decision 9). API → Tasks 8 and 11. Archive-on-pipeline-delete → Task 9. UI → Task 13. Docs → Task 14. Testing → each task plus Task 15. Non-goals are excluded throughout.

**Known gap, deliberate.** Tasks 7–15 are specified at interface-and-test level rather than with literal code blocks, unlike Tasks 1–6. They depend on decisions best made against the real compiler (exact `temporalio_client` schedule builder shapes, the sqlx query forms). Executors must apply the same TDD cycle — failing test, run it, implement, run it, commit — and should re-read the spec section named in each task before starting. If a task's shape turns out to differ materially from what is described here, stop and revise the plan rather than improvising.

**Type consistency.** `IncrementalState.hash` is a hex `String` everywhere (model, connector, repo, activity) — never `[u8;32]`, which the spec's prose sketched but would not survive a JSON payload. `Resolution::Items.items` is `Vec<ResolvedItem>` in the crate and becomes `Vec<PluginInput>` only inside the worker activity (Task 12), which is the single place blob staging happens. `RunOutcome::as_str()` values (`unchanged`/`ingested`/`failed`) match `source_runs.outcome` in the migration.

---

## Revision 2026-09-23 — destinations move to Meilisearch connections

The spec's Decisions 6 and 11–15 replace the per-source sealed `MeiliContext` with named
**Meilisearch connections** referenced by the `meili_indexer` step. Read the spec's
*Meilisearch connections and the indexer step* section before any task below.

**Status of Tasks 1–9.** Tasks 1–7 and 9 are done. Task 1's migration and Task 7's repo
still carry `meili_ctx` and are reworked by Task R1. Tasks 2–6 (`crates/source`) are
unaffected. Task 8 was never started.

**Revised order.** R1 first so the branch stays coherent, then the self-contained pieces
(C1, C2), then the precedence change and everything that depends on it.

### Task R1: Drop `meili_ctx`; add `meili_connections`

**Files:** Modify `migrations/0002_sources.sql`, `crates/control-plane/src/sources.rs`,
`crates/control-plane/tests/{migrations,sources_repo,pipeline_delete_cascade}.rs`.

Remove `meili_ctx` from the table, `NewSource`, `SourceRecord`, `SOURCE_COLUMNS` and every
test fixture. Add the `meili_connections` table from the spec. Before running the tests,
repair the local dev database once, since it applied the earlier `0002`:
`DROP TABLE source_runs, sources; ALTER TABLE jobs DROP COLUMN source_id; DELETE FROM
_sqlx_migrations WHERE version = 2;`. Test: the migration test asserts `meili_ctx` is
**absent** and `meili_connections` exists with its unique index.

### Task C1: Connection host policy

**Files:** Create `crates/source/src/host_policy.rs`.

**Produces:** `enum HostPolicy { Public, Any, Allow(Vec<HostPort>) }`,
`HostPolicy::parse(&str) -> Result<HostPolicy, SourceError>`,
`HostPolicy::from_env() -> Result<HostPolicy, SourceError>` (reads
`MEILI_CONNECTION_HOSTS`, default `Public`), and
`async fn check(&self, url: &Url) -> Result<(), SourceError>`. `Public` delegates to the
existing `UrlGuard::check_url`. `Allow` compares host and effective port and accepts
`http` or `https`. Tests: `public` rejects `http://meilisearch:7700`, rejects
`https://127.0.0.1`; an allowlist accepts exactly its entries and rejects a different
port; `any` accepts both schemes; parsing rejects an empty entry and a garbage value.

### Task C2: `max_batch_bytes` in `meili_indexer`

**Files:** Modify `crates/plugins/meili-indexer/src/lib.rs`.

Add `max_batch_bytes: u64` (default `50 * 1024 * 1024`) to the config and its JSON schema.
Replace `docs.chunks(cfg.batch_size)` with a pure `fn plan_batches(sizes: &[usize],
max_docs: usize, max_bytes: u64) -> Result<Vec<Range<usize>>, OversizedDoc>` over the
serialized size of each document, so the splitting is unit-testable without Meilisearch.
Tests: splits on count; splits on bytes; whichever comes first; one document over the
cap returns `OversizedDoc { index }` and the plugin maps it to `NonRetryable` naming the
document id; an empty input gives no batches; the existing indexer tests still pass.

### Task C3: Connection repository and internal routes

**Files:** Create `crates/control-plane/src/connections.rs`; modify `lib.rs`.

Mirror `SourceRepo`: `insert`, `get(uid, project_id)`, `list(project_id)`, `update`,
`delete`, plus `used_by(uid, project_id) -> Vec<String>` scanning pipeline definitions
for `meili_indexer` steps whose `config.connection` equals the uid. The key moves only as
sealed bytes. Routes under `/internal/connections`. Tests against Postgres, each test in
its own `<prefix>-` namespace (see the isolation fix in commit `2fe6534`).

### Task C4: Pinned destinations win in `inject_meili_context`

**Files:** Modify `crates/plugin-sdk/src/types.rs` (`MeiliContext`,
`inject_meili_context`), and every caller the compiler then flags — at least
`crates/gateway/src/context.rs` and `crates/worker/src/workflow.rs`.

`MeiliContext.host` and `.api_key` become `Option<String>`. `inject_meili_context` no
longer writes `host`/`api_key` when the step config has a `connection`; otherwise its
behaviour is byte-for-byte today's. Tests: with `connection`, a pinned `host` survives and
no `api_key` is injected; without, today's overwrite is preserved; `redacted()` never
prints a key in either shape.

### Task C5: The indexer activity resolves the connection

**Files:** Modify `crates/worker/src/activity.rs`, `crates/worker/src/config.rs`.

Before calling `meili_indexer`, when the config has `connection`: fetch
`/internal/connections/{uid}?project_id=`, open `api_key` with `SecretKey::from_env()`,
re-check the host with `HostPolicy`, and merge `host`/`api_key` into the config in
memory. Missing connection, missing `SOURCE_SECRET_KEY`, or a policy rejection are
`NonRetryable` with a message naming the connection. Test: assert on the serialized
`StepActivityInput` that the key is absent, and that the plugin receives it.

### Task C6: `/connections` on the gateway

**Files:** Create `crates/gateway/src/handlers/connections.rs`; modify `lib.rs`,
`state.rs`.

The five routes from the spec's API table. Create and host/key `PATCH` run the spec's
three-step validation (policy, `GET /health`, `GET /indexes?limit=1`) against wiremock in
tests. `501` without `SOURCE_SECRET_KEY`. Responses mask `api_key`.

### Task C7: `POST /ingest` without context for pinned pipelines

**Files:** Modify `crates/gateway/src/handlers/ingest.rs`, `crates/gateway/src/context.rs`.

After the pipeline is resolved, a missing context is only an error if the pipeline's
indexer step names no connection. Tests: headerless request to a pinned pipeline →
`202`; headerless request to an unpinned pipeline → the same `400 MissingContext` as
today.

### Then Tasks 8, 10–15 as written, with these changes

- **Task 8 / 11:** `NewSource` has no `meili_ctx`; `POST /sources` returns `422` when the
  pipeline's indexer names no connection, instead of capturing headers.
- **Task 12:** children get a `MeiliContext` with only `project_id` and `index`;
  `load_source` fails the run if the pipeline no longer pins a connection.
- **Task 13:** add `ui/src/app/connections/` and the indexer-step connection picker.
- **Task 14:** add `docs/concepts/connections.mdx`.
- **Task 15:** the e2e creates a connection first and also checks headerless
  `POST /ingest`.
