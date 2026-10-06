# Lab-ready seams Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make glutony ready for the Meilisearch Lab without depending on it: an opaque `tenant_id` on every owned row, an authenticated management API the Lab calls per account, signed `usage.recorded` billing events from a durable outbox, and an OpenAPI spec the Lab console can be built from.

**Architecture:** `project_id` becomes `tenant_id` everywhere (with `serde(alias)` so Temporal histories, schedules and rolling deploys keep working). The gateway grows a `Scope` extractor that authenticates management routes with `LAB_SERVICE_TOKEN` + `X-Glutony-Tenant-Id` or `ADMIN_API_KEY`, and stays open when neither is set. Paid plugins price their provider calls from a cost table; the worker's existing `record_usage` activity posts one Lab event per job to a new control-plane outbox (`lab_events`), and a control-plane sender task delivers it to `{LAB_URL}/internal/events` with `X-Lab-Signature`, never dropping a row.

**Tech Stack:** Rust 1.94 (edition 2024), axum 0.8, sqlx 0.9 (Postgres, runtime queries only), Temporal Rust SDK 1.0, reqwest 0.13 (rustls), wiremock 0.6, subtle, hmac/sha2, prometheus, toml, jsonschema (dev). UI: Next.js + vitest (rename only).

**Spec:** `docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md`. Read it before starting; this plan argues from it. Four refinements made while planning are recorded in the spec in Task 13 ("Spec amendments") and apply from Task 1 on:

1. **`source_uid` is dropped from the event** (§5.2): no workflow input carries it, and billing does not need it.
2. **`cost_complete` is derived, not stored** (§5.1): `UsageUnits` gains an additive `unpriced_calls: u64` (merges by `+=` like every other field); the event's `cost_complete` is `unpriced_calls == 0`.
3. **The usage activity's retry policy changes in every deployment** (§5.3): workflow code cannot read configuration, and adding a new activity would break replay of in-flight workflows. The policy becomes unlimited attempts, 5-minute maximum interval, bounded by a 7-day `schedule_to_close` window.
4. **The rollback guard uses a Postgres setting** (§10): `PGOPTIONS='-c glutony.rollback_force=on'` instead of `-v force=1`, so the script is plain SQL a test can run.

## Global Constraints

- `cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings` and `cargo test --workspace` pass at the end of every task. DB-backed tests run when `DATABASE_URL` is set (`DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres`, e.g. from `docker run -d --rm --name gl-pg -p 55432:5432 -e POSTGRES_PASSWORD=dev postgres:17-alpine`); run them for every task that touches the control plane.
- All SQL is runtime-checked (`sqlx::query`/`query_as`), never `query!`. `format!`-built SQL needs `sqlx::AssertSqlSafe`.
- Never edit `migrations/0001_init.sql` or `0002_sources.sql` (sqlx checksums them). New migrations: `0003_tenant_id.sql`, `0004_lab_events.sql`.
- Tenant ids: 1 to 128 characters of `[A-Za-z0-9._:-]`, otherwise `400 invalid_tenant`.
- Secrets (`LAB_SERVICE_TOKEN`, `ADMIN_API_KEY`, `LAB_EVENTS_SECRET`) never appear in logs, errors or `Debug` output (`<redacted>`).
- Every renamed field that is `Deserialize` carries `#[serde(alias = "project_id")]`.
- Tinybird column names stay `project_id`; `UsageEvent` serializes its tenant as `project_id`.
- The header `X-Meili-Project-Id` keeps working; `X-Meili-Tenant-Id` is added and wins.
- With none of `LAB_SERVICE_TOKEN`, `ADMIN_API_KEY`, `LAB_URL`, `LAB_EVENTS_SECRET`, `LAB_EVENTS_ENABLED` set, behavior is today's (except `tenant_id` in JSON): open management API, no outbox rows, no sender task, no outbound call.
- Event envelope: `id` (UUIDv5 of `job:{job_id}:usage` under `GLUTONY_LAB_NAMESPACE`), `type: "usage.recorded"`, `occurred_at` (RFC 3339, milliseconds, `Z`), `account_id` (= tenant, must be a UUID), `api_key_id: null`, `product: "glutony"`.
- Delivery: `POST {LAB_URL}/internal/events`, body `{"events":[…]}`, header `X-Lab-Signature: sha256=<hex HMAC-SHA256(LAB_EVENTS_SECRET, raw body)>`, batches of at most 500, 2 s tick, backoff `min(2^attempts s, 300 s)` ±20 % jitter, no redirects, 2 s connect / 10 s total timeout, rows never dropped, delivered rows purged after 7 days.
- Commit messages: conventional (`feat(gateway): …`), **no `Co-Authored-By` line** (the user's global rule).
- Deploy order when rolling out: control plane, then workers, then gateway (the aliases make every other order work too, but this one never sends a new field to an old reader).

## Review Focus

1. **Temporal payloads written before the rename** (running workflows, and source schedules whose action input is frozen forever) must still deserialize, including the `project_id` key the indexer config received through `IndexerConfig`'s `#[serde(flatten)] MeiliContext`. Task 1 test `pre_rename_payloads_still_deserialize`.
2. **A mixed-version deploy** (new gateway, old query key; old gateway sending `?project_id=` to a new control plane) must keep scoping. Task 1 test `tenant_query_accepts_the_old_key`.
3. **A Lab call that also carries trusted `X-Meili-Project-Id` / `X-Meili-Tenant-Id` for another tenant** must act on the `X-Glutony-Tenant-Id` tenant only. Task 3 test `auth_on_ignores_edge_tenant_headers`.
4. **A Lab that answers `200` listing only some ids, or answers with a redirect**, must leave the rest pending and never follow the redirect. Task 9 tests `partial_accept_leaves_the_rest_pending`, `redirect_is_not_followed`.
5. **A job whose control-plane row is missing** (the gateway's best-effort insert failed) must answer `404` to a tenant caller rather than leak, and keep today's behavior for callers without a tenant. Task 4 test `job_without_a_row_is_hidden_from_tenants`.

---

## File Map

| File | Task | Responsibility |
|---|---|---|
| `migrations/0003_tenant_id.sql` (create) | 1 | Rename `project_id` columns and indexes |
| every `crates/**` and `ui/src/**` file naming `project_id` (modify) | 1 | Mechanical rename + aliases |
| `crates/plugin-sdk/src/tenant.rs` (create) | 2, 4 | `validate_tenant_id`, `RowScope` |
| `crates/gateway/src/context.rs` (modify) | 2 | `X-Meili-Tenant-Id`, fallible tenant resolution |
| `crates/gateway/src/error.rs` (modify) | 2, 3 | `InvalidTenant`, `WWW-Authenticate` on 401 |
| `crates/gateway/src/auth.rs` (create) | 3 | `Scope` extractor, three auth modes |
| `crates/gateway/src/state.rs`, `main.rs` (modify) | 3 | `LAB_SERVICE_TOKEN`, `ADMIN_API_KEY`, startup warning |
| `crates/gateway/src/handlers/*.rs` (modify) | 3, 4 | Handlers take `Scope`; tenant checks; `scope` field |
| `crates/gateway/src/routes.rs` (create) | 5 | Route table + parity/guard tests |
| `docs/openapi.yaml` (modify) | 1, 5 | Field rename; missing routes; security |
| `crates/plugin-sdk/src/cost.rs`, `config/provider-costs.toml` (create) | 6 | Provider cost table |
| `crates/plugins/{llm-enricher,image-captioner,audio-transcriber,jev-enricher}` (modify) | 6 | Price each provider call |
| `crates/usage/src/lab.rs` (create), `contracts/vendor/lab/lab-events.schema.json` (create) | 7 | Lab event builder, vendored contract |
| `crates/control-plane/src/lab_events.rs` (create), `migrations/0004_lab_events.sql` (create) | 8 | Outbox repo + `POST /internal/lab-events` |
| `crates/control-plane/src/lab_sender.rs`, `metrics.rs` (create), `main.rs` (modify) | 9 | Sender task, `/metrics`, `LAB_URL` config |
| `crates/worker/src/{activity,workflow,config,main}.rs` (modify) | 10 | Post the event before Tinybird; retry window |
| `scripts/fake_lab.py` (create), `scripts/e2e.sh` (modify) | 11 | `--lab` end-to-end leg |
| `scripts/rollback-lab-seams.sql` (create), `crates/control-plane/tests/rollback.rs` (create) | 12 | Rollback |
| `docs/**`, `README.md`, `ui/AGENTS.md`, `ui/CLAUDE.md`, `.github/workflows/ci.yml`, the spec (modify); `docs/deployment/meilisearch-lab.mdx` (create) | 13 | Docs, UI freeze, drift job |

---

### Task 1: Rename `project_id` to `tenant_id`

A mechanical rename across the whole workspace, the UI and the OpenAPI field names, in one commit, because every crate must compile together. The only non-mechanical parts are the exceptions in Step 5 and the aliases in Step 6.

**Files:**
- Create: `migrations/0003_tenant_id.sql`
- Modify: every file under `crates/`, `ui/src/`, plus `docs/openapi.yaml` and `scripts/e2e.sh`, that contains `project_id` (list them with `rg -l project_id crates ui/src docs/openapi.yaml scripts/e2e.sh`)
- Test: `crates/plugin-sdk/src/types.rs` (test module), `crates/source/src/model.rs` (test module), `crates/control-plane/src/pipelines.rs` (test module), `crates/control-plane/tests/db.rs`

**Interfaces:**
- Produces (used by every later task): the field `tenant_id: Option<String>` on `MeiliContext`, `PipelineDefinition`, `StepActivityInput`, `SourceRunInput`, `SourceDefinition`, `JobRecord` (gateway and control plane), `ConnectionRecord`, `ConnectionView`, `SourceView`, `NewSource`, `ResolveRequest`; `tenant_id: String` on `JobUsageInput` and `UsageEvent`; `gateway::context::resolve_tenant_id(headers, config) -> Option<String>` (becomes fallible in Task 2); `control_plane::tenant_scope(explicit, headers) -> Option<String>`; `control_plane::pipelines::TenantQuery { tenant_id: Option<String> }`; `ControlPlaneClient::tenant_query(tenant_id) -> Vec<(&'static str, String)>` emitting `tenant_id`.

- [ ] **Step 1: Write the failing compatibility tests**

In `crates/plugin-sdk/src/types.rs`, inside the existing `#[cfg(test)] mod tests`, add:

```rust
    /// Rename every `tenant_id` key back to the pre-rename `project_id`, recursively,
    /// to build the payloads Temporal already holds.
    fn with_old_key(v: serde_json::Value) -> serde_json::Value {
        match v {
            serde_json::Value::Object(map) => serde_json::Value::Object(
                map.into_iter()
                    .map(|(k, v)| {
                        let k = if k == "tenant_id" { "project_id".to_string() } else { k };
                        (k, with_old_key(v))
                    })
                    .collect(),
            ),
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(with_old_key).collect())
            }
            other => other,
        }
    }

    #[test]
    fn pre_rename_payloads_still_deserialize() {
        let context = MeiliContext {
            tenant_id: Some("acme".into()),
            host: Some("http://meili:7700".into()),
            api_key: Some("k".into()),
            index: Some("docs".into()),
            region: None,
        };
        let old = with_old_key(serde_json::to_value(&context).unwrap());
        assert_eq!(old["project_id"], "acme", "the fixture must use the old key");
        let back: MeiliContext = serde_json::from_value(old).unwrap();
        assert_eq!(back, context);

        let pipeline: PipelineDefinition = serde_json::from_value(serde_json::json!({
            "uid": "p",
            "name": "p",
            "steps": [{"id": "index", "plugin": "meili_indexer"}],
            "project_id": "acme"
        }))
        .unwrap();
        assert_eq!(pipeline.tenant_id.as_deref(), Some("acme"));

        let step = StepActivityInput {
            job_id: uuid::Uuid::nil(),
            step_id: "index".into(),
            plugin: "meili_indexer".into(),
            config: serde_json::json!({}),
            input: PluginInput::Documents(vec![]),
            branch: None,
            branch_total: None,
            tenant_id: Some("acme".into()),
        };
        let back: StepActivityInput =
            serde_json::from_value(with_old_key(serde_json::to_value(&step).unwrap())).unwrap();
        assert_eq!(back, step);

        let wf = PipelineWorkflowInput {
            job_id: uuid::Uuid::nil(),
            pipeline: pipeline.clone(),
            input: PluginInput::Documents(vec![]),
            context: context.clone(),
        };
        let back: PipelineWorkflowInput =
            serde_json::from_value(with_old_key(serde_json::to_value(&wf).unwrap())).unwrap();
        assert_eq!(back, wf);

        // The indexer received the context flattened into its config under the old key.
        let config: IndexerConfig =
            serde_json::from_value(with_old_key(serde_json::to_value(&context).unwrap()))
                .unwrap();
        assert_eq!(config.meili.tenant_id.as_deref(), Some("acme"));
    }
```

If `PipelineDefinition` rejects the minimal JSON above (a required field this plan did not list), copy the smallest `PipelineDefinition` literal already used in this test module and set `tenant_id` on it instead; the assertion stays the same. If `IndexerConfig` has required fields besides `meili`, add them to the JSON with their smallest valid values.

In `crates/source/src/model.rs`, in its test module (create `#[cfg(test)] mod tests { use super::*; }` at the end of the file if there is none), add:

```rust
    #[test]
    fn a_schedule_frozen_before_the_rename_still_runs() {
        let input: SourceRunInput = serde_json::from_value(serde_json::json!({
            "source_id": "11111111-2222-3333-4444-555555555555",
            "project_id": "acme"
        }))
        .unwrap();
        assert_eq!(input.tenant_id.as_deref(), Some("acme"));
        let written = serde_json::to_value(&input).unwrap();
        assert_eq!(written["tenant_id"], "acme");
        assert!(written.get("project_id").is_none());
    }
```

In `crates/control-plane/src/pipelines.rs`, in its test module, add:

```rust
    #[test]
    fn tenant_query_accepts_the_old_key() {
        use axum::extract::Query;
        let uri: axum::http::Uri = "http://cp/pipelines?project_id=t1".parse().unwrap();
        let Query(q) = Query::<TenantQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.tenant_id.as_deref(), Some("t1"));
        let uri: axum::http::Uri = "http://cp/pipelines?tenant_id=t2".parse().unwrap();
        let Query(q) = Query::<TenantQuery>::try_from_uri(&uri).unwrap();
        assert_eq!(q.tenant_id.as_deref(), Some("t2"));
    }
```

In `crates/control-plane/tests/db.rs`, add:

```rust
#[tokio::test]
async fn tenant_id_replaces_project_id() {
    let Some(t) = setup().await else { return };

    let cols: Vec<(String, String)> = sqlx::query_as(
        "SELECT table_name::text, column_name::text FROM information_schema.columns \
         WHERE table_schema = current_schema() AND column_name IN ('project_id', 'tenant_id') \
         ORDER BY 1",
    )
    .fetch_all(&t.pool)
    .await
    .unwrap();
    let expected: Vec<(String, String)> = ["jobs", "meili_connections", "pipelines", "sources"]
        .into_iter()
        .map(|table| (table.to_string(), "tenant_id".to_string()))
        .collect();
    assert_eq!(cols, expected);

    let indexes: Vec<(String,)> = sqlx::query_as(
        "SELECT indexname::text FROM pg_indexes \
         WHERE schemaname = current_schema() AND indexname LIKE '%tenant%' ORDER BY 1",
    )
    .fetch_all(&t.pool)
    .await
    .unwrap();
    let names: Vec<&str> = indexes.iter().map(|(n,)| n.as_str()).collect();
    assert_eq!(
        names,
        [
            "jobs_tenant_started",
            "meili_connections_uid_tenant",
            "pipelines_uid_tenant",
            "sources_pipeline_tenant",
            "sources_uid_tenant"
        ]
    );

    // Still one row per (uid, tenant), NULL counting as one global scope.
    let insert = |tenant: Option<&'static str>| {
        sqlx::query(
            "INSERT INTO pipelines (uid, name, definition, tenant_id) \
             VALUES ('dup', 'dup', '{}'::jsonb, $1)",
        )
        .bind(tenant)
    };
    insert(Some("t1")).execute(&t.pool).await.unwrap();
    assert!(insert(Some("t1")).execute(&t.pool).await.is_err());
    insert(Some("t2")).execute(&t.pool).await.unwrap();
    insert(None).execute(&t.pool).await.unwrap();
    assert!(insert(None).execute(&t.pool).await.is_err());

    t.drop_schema().await;
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-plugin-sdk pre_rename_payloads_still_deserialize`
Expected: FAIL to compile (`no field tenant_id on MeiliContext`).

- [ ] **Step 3: Write the migration**

Create `migrations/0003_tenant_id.sql`:

```sql
-- The owner of a row is an opaque tenant (a Lab account, a Cloud project, or NULL for
-- the global scope). See docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md §3.
-- Renames only touch the catalog: no rewrite. Expression indexes follow the column.

ALTER TABLE pipelines         RENAME COLUMN project_id TO tenant_id;
ALTER TABLE jobs              RENAME COLUMN project_id TO tenant_id;
ALTER TABLE sources           RENAME COLUMN project_id TO tenant_id;
ALTER TABLE meili_connections RENAME COLUMN project_id TO tenant_id;

ALTER INDEX pipelines_uid_project         RENAME TO pipelines_uid_tenant;
ALTER INDEX jobs_project_started          RENAME TO jobs_tenant_started;
ALTER INDEX sources_uid_project           RENAME TO sources_uid_tenant;
ALTER INDEX sources_pipeline              RENAME TO sources_pipeline_tenant;
ALTER INDEX meili_connections_uid_project RENAME TO meili_connections_uid_tenant;
```

- [ ] **Step 4: Apply the mechanical rename**

```bash
rg -l 'project_id|ProjectQuery|project_scope|project_query' crates ui/src docs/openapi.yaml \
  | xargs sed -i '' \
      -e 's/project_id/tenant_id/g' \
      -e 's/ProjectQuery/TenantQuery/g' \
      -e 's/project_scope/tenant_scope/g' \
      -e 's/project_query/tenant_query/g'
```

(On Linux use `sed -i` without `''`.) This renames identifiers, SQL column names in query strings, `ON CONFLICT (uid, COALESCE(tenant_id, ''))`, JSON keys in tests, the meili-indexer schema's read-only `tenant_id`, and the UI's TypeScript fields. It does **not** touch `H_PROJECT_ID`, the `"x-meili-project-id"` header strings, or `X-Meili-Project-Id` in docs, which stay.

Then fix wording the rename made odd: `rg -n 'this project' crates` and change the duplicate-uid messages in `crates/control-plane/src/sources.rs` and `connections.rs` from "already exists in this project" to "already exists for this tenant". Update the doc comments that say "Tenant / project identifier (from `X-Meili-Project-Id`)" on `MeiliContext` to "Tenant id (from `X-Meili-Tenant-Id` or `X-Meili-Project-Id`)".

- [ ] **Step 5: Restore the three places that must keep `project_id`**

1. `crates/usage/src/lib.rs`, struct `UsageEvent`: the Tinybird column stays `project_id`. On the renamed field write:

   ```rust
       /// Tenant id. `""` when self-hosted, never `null`. Serialized as `project_id`
       /// because that is the Tinybird column: renaming a Tinybird column means a new
       /// datasource and a backfill (spec §3.5).
       #[serde(rename = "project_id")]
       pub tenant_id: String,
   ```

   The test `datasource_schema_matches_the_event_struct` must keep passing; if it compares Rust field names rather than serialized keys, make it read serialized keys (`serde_json::to_value(sample_event())` object keys) so the rename attribute is what it checks.
2. `crates/gateway/src/handlers/usage.rs`, the Tinybird pipe query in `get_usage`: the pipe parameter stays `project_id`. Change `("tenant_id", tenant_id.as_str())` back to `("project_id", tenant_id.as_str())` and add the comment `// The Tinybird pipe parameter keeps its historical name (spec §3.5).` The public `UsageResponse.tenant_id` stays renamed.
3. `scripts/e2e.sh`: the fake Tinybird rows keep `project_id` (line ~581: `r["project_id"] == "acme"`). Revert that line if Step 4's `rg` included the script; it did not in the command above, so edit `scripts/e2e.sh` by hand only where it reads **gateway JSON** fields named `project_id` (there are none today; confirm with `rg -n project_id scripts/e2e.sh`, which must show only the Tinybird lines).

- [ ] **Step 6: Add the aliases**

Every renamed field on a type that derives `Deserialize` gets `alias = "project_id"` in its existing `#[serde(...)]` attribute, or a new `#[serde(alias = "project_id")]` when it has none. Find them with:

```bash
rg -n -B3 'pub tenant_id|    tenant_id: Option<&' crates | rg -n 'serde|tenant_id'
```

The list is: `MeiliContext`, `PipelineDefinition`, `StepActivityInput` (plugin-sdk); `SourceDefinition`, `SourceRunInput` (source); `JobUsageInput` (usage); `JobRecord`, `JobListQuery`, `TenantQuery`, `NewSource`, `SourceListQuery`, `SourceScopeQuery`, `NewConnection`, `ConnectionRecord`, `ResolveRequest` (control plane); `JobRecord`, `ResolveRequest`, `ConnectionRecord`, `ConnectionView`, `SourceView` (gateway). Example:

```rust
    /// Tenant id (from `X-Meili-Tenant-Id` or `X-Meili-Project-Id`).
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "project_id")]
    pub tenant_id: Option<String>,
```

`JobUsageInput.tenant_id: String` has no serde attribute today; give it `#[serde(alias = "project_id")]`. `UsageEvent` gets the `rename` from Step 5, not an alias.

- [ ] **Step 7: Build, test, lint**

```bash
cargo build --workspace --all-targets
cargo test --workspace
DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
(cd ui && pnpm install --frozen-lockfile && pnpm test && pnpm exec tsc --noEmit)
```

Expected: all PASS, including the four new tests. Fix every compile error by finishing the rename (never by reintroducing `project_id` outside Step 5).

- [ ] **Step 8: Commit**

```bash
git add -A migrations crates ui/src docs/openapi.yaml scripts/e2e.sh
git commit -m "refactor!: project_id becomes an opaque tenant_id

Migration 0003 renames the column and its indexes. Every Deserialize type keeps
reading project_id through serde(alias), so running workflows, frozen source
schedules and mixed-version deploys keep working. Tinybird keeps its column name."
```

---

### Task 2: `X-Meili-Tenant-Id` and tenant validation

**Files:**
- Create: `crates/plugin-sdk/src/tenant.rs`
- Modify: `crates/plugin-sdk/src/lib.rs`, `crates/gateway/src/context.rs`, `crates/gateway/src/error.rs`, every caller of `resolve_request_context` / `resolve_tenant_id` / `context_for` in `crates/gateway/src/`
- Test: `crates/plugin-sdk/src/tenant.rs`, `crates/gateway/src/context.rs` (test module), `crates/gateway/src/error.rs` (test module)

**Interfaces:**
- Consumes: Task 1 names.
- Produces: `meili_ingest_plugin_sdk::validate_tenant_id(&str) -> Result<(), String>`, `meili_ingest_plugin_sdk::MAX_TENANT_ID_LEN: usize = 128`; `gateway::context::H_TENANT_ID = "x-meili-tenant-id"`; `gateway::context::resolve_tenant_id(&HeaderMap, &GatewayConfig) -> Result<Option<String>, GatewayError>`; `resolve_request_context(...) -> Result<MeiliContext, GatewayError>`; `handlers::ingest::context_for(...) -> Result<MeiliContext, GatewayError>`; `GatewayError::InvalidTenant(String)` (400, `invalid_tenant`); `context::header` and `context::bearer_token` become `pub(crate)`.

- [ ] **Step 1: Write the failing tests**

Create `crates/plugin-sdk/src/tenant.rs`:

```rust
//! Tenant ids: the opaque owner of pipelines, sources, connections and jobs.
//!
//! Glutony never interprets a tenant id. Behind the Meilisearch Lab it is an account
//! UUID; behind Meilisearch Cloud's Envoy it is a project id; standalone it is absent.
//! It is validated wherever it enters so it is always safe to log and to put in a
//! query string.

/// Longest accepted tenant id.
pub const MAX_TENANT_ID_LEN: usize = 128;

/// Check a tenant id: 1 to [`MAX_TENANT_ID_LEN`] characters of `[A-Za-z0-9._:-]`.
pub fn validate_tenant_id(id: &str) -> Result<(), String> {
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_uuids_project_ids_and_the_allowed_punctuation() {
        for ok in [
            "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61",
            "hackersearch",
            "acme.eu:prod_1",
            &"a".repeat(MAX_TENANT_ID_LEN),
        ] {
            assert_eq!(validate_tenant_id(ok), Ok(()), "{ok:?}");
        }
    }

    #[test]
    fn rejects_empty_too_long_and_other_characters() {
        for bad in [
            String::new(),
            "a".repeat(MAX_TENANT_ID_LEN + 1),
            "a/b".into(),
            "a b".into(),
            "a%2Fb".into(),
            "é".into(),
            "a\nb".into(),
        ] {
            assert!(validate_tenant_id(&bad).is_err(), "{bad:?} must be rejected");
        }
    }
}
```

In `crates/plugin-sdk/src/lib.rs` add `pub mod tenant;` next to the other modules and `pub use tenant::{MAX_TENANT_ID_LEN, validate_tenant_id};` next to the other `pub use` lines.

In `crates/gateway/src/context.rs` test module, add (reuse the module's existing `cfg()`, `cfg_secret()` and `headers()` helpers):

```rust
    #[test]
    fn tenant_header_beats_project_header() {
        let h = headers(&[("x-meili-tenant-id", "acct-1"), ("x-meili-project-id", "proj-1")]);
        assert_eq!(resolve_tenant_id(&h, &cfg()).unwrap().as_deref(), Some("acct-1"));
        let h = headers(&[("x-meili-project-id", "proj-1")]);
        assert_eq!(resolve_tenant_id(&h, &cfg()).unwrap().as_deref(), Some("proj-1"));
        assert_eq!(resolve_tenant_id(&HeaderMap::new(), &cfg()).unwrap(), None);
    }

    #[test]
    fn tenant_headers_need_the_envoy_secret_when_one_is_set() {
        let h = headers(&[("x-meili-tenant-id", "acct-1")]);
        assert_eq!(resolve_tenant_id(&h, &cfg_secret()).unwrap(), None);
    }

    #[test]
    fn an_invalid_tenant_from_a_trusted_edge_is_400() {
        let h = headers(&[("x-meili-tenant-id", "a/b")]);
        let err = resolve_tenant_id(&h, &cfg()).unwrap_err();
        assert_eq!(err.code(), "invalid_tenant");
        assert_eq!(err.status(), axum::http::StatusCode::BAD_REQUEST);
        let err = resolve_request_context(&h, None, &cfg()).unwrap_err();
        assert_eq!(err.code(), "invalid_tenant");
    }

    #[test]
    fn request_context_carries_the_tenant_header() {
        let h = headers(&[
            ("x-meili-host", "http://m:7700"),
            ("x-meili-api-key", "k"),
            ("x-meili-tenant-id", "acct-1"),
        ]);
        let ctx = resolve_request_context(&h, None, &cfg()).unwrap();
        assert_eq!(ctx.tenant_id.as_deref(), Some("acct-1"));
    }
```

In `crates/gateway/src/error.rs`, add a row for `InvalidTenant` to the existing status/code table test (same shape as its other rows): `(GatewayError::InvalidTenant("x".into()), StatusCode::BAD_REQUEST, "invalid_tenant")`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-plugin-sdk tenant && cargo test -p meili-ingest-gateway context`
Expected: the SDK tests panic at `todo!()`; the gateway tests fail to compile (`InvalidTenant` and `H_TENANT_ID` do not exist, `resolve_tenant_id` returns `Option`).

- [ ] **Step 3: Implement**

`validate_tenant_id`:

```rust
pub fn validate_tenant_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > MAX_TENANT_ID_LEN {
        return Err(format!(
            "tenant id must be 1 to {MAX_TENANT_ID_LEN} characters"
        ));
    }
    if let Some(c) = id
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | ':' | '-')))
    {
        return Err(format!(
            "tenant id contains {c:?}; allowed characters are A-Z a-z 0-9 . _ : -"
        ));
    }
    Ok(())
}
```

`crates/gateway/src/error.rs`: add the variant after `BadRequest`:

```rust
    /// A tenant id failed validation (400).
    #[error("invalid tenant id: {0}")]
    InvalidTenant(String),
```

with `GatewayError::InvalidTenant(_) => StatusCode::BAD_REQUEST` in `status()` (join the `MissingContext | BadRequest` arm) and `GatewayError::InvalidTenant(_) => "invalid_tenant"` in `code()`.

`crates/gateway/src/context.rs`:

```rust
/// Edge-injected header: tenant id (the Lab's account id). Wins over `X-Meili-Project-Id`.
pub const H_TENANT_ID: &str = "x-meili-tenant-id";
```

Make `fn header` and `fn bearer_token` `pub(crate)`. Replace `resolve_tenant_id` with:

```rust
/// Tenant id of the request from a trusted edge: `X-Meili-Tenant-Id`, else
/// `X-Meili-Project-Id` (kept for Cloud's Envoy contract). Both follow the Envoy trust
/// rule. An invalid value is a `400 invalid_tenant`, never silently dropped.
pub fn resolve_tenant_id(
    headers: &HeaderMap,
    config: &GatewayConfig,
) -> Result<Option<String>, GatewayError> {
    let raw = trusted_header(headers, config, H_TENANT_ID)
        .or_else(|| trusted_header(headers, config, H_PROJECT_ID));
    match raw {
        None => Ok(None),
        Some(tenant) => {
            meili_ingest_plugin_sdk::validate_tenant_id(&tenant)
                .map_err(GatewayError::InvalidTenant)?;
            Ok(Some(tenant))
        }
    }
}
```

Change `resolve_request_context` to return `Result<MeiliContext, GatewayError>`: replace `let tenant_id = envoy(H_PROJECT_ID);` with `let tenant_id = resolve_tenant_id(headers, config)?;` and wrap the final struct in `Ok(...)`. In `resolve_context`, write `let ctx = resolve_request_context(headers, query_index, config)?;`. Update the module doc's resolution list (step 3 becomes "`X-Meili-Tenant-Id`, else `X-Meili-Project-Id` → tenant_id").

Callers:
- `handlers/ingest.rs` `context_for`: return `Result<MeiliContext, GatewayError>`, body `resolve_request_context(headers, query_index, &state.config)`; its two callers write `let ctx = context_for(&state, &headers, &query, &extracted)?;`.
- `handlers/pipeline.rs` `ingest_with_pipeline_inner`: `let pre = resolve_request_context(...)?;`.
- Every `resolve_tenant_id(&headers, &state.config)` in `handlers/*.rs`: append `?` (these handlers return `Result<_, GatewayError>`; `sources.rs`'s `set_paused` too).
- `lib.rs` `health`: `"tenant_id": context::resolve_tenant_id(&headers, &state.config).ok().flatten(),` (health must never fail).
- Existing context tests that call `resolve_request_context(...)` get `.unwrap()`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-plugin-sdk && cargo test -p meili-ingest-gateway`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add crates/plugin-sdk crates/gateway
git commit -m "feat(gateway): X-Meili-Tenant-Id, and invalid tenant ids are a 400"
```

---

### Task 3: Management auth (`Scope`)

**Files:**
- Create: `crates/gateway/src/auth.rs`
- Modify: `Cargo.toml` (workspace dep `subtle`), `crates/gateway/Cargo.toml`, `crates/gateway/src/lib.rs` (`pub mod auth;`), `crates/gateway/src/state.rs`, `crates/gateway/src/error.rs`, `crates/gateway/src/main.rs`, `crates/gateway/src/handlers/{pipelines,connections,sources,jobs,usage,plugins,catalog}.rs`
- Test: `crates/gateway/src/auth.rs`, `crates/gateway/src/handlers/pipelines.rs` (test module), `crates/gateway/src/state.rs` (`config_debug_redacts_secrets`)

**Interfaces:**
- Consumes: `resolve_tenant_id`, `context::header`, `context::bearer_token`, `GatewayError::InvalidTenant` (Task 2).
- Produces: `gateway::auth::{Scope, Principal, authorize, management_auth_enabled, H_GLUTONY_TENANT_ID}`; `Scope { principal: Principal, tenant_id: Option<String> }` with `fn tenant(&self) -> Option<&str>`; `Principal::{Lab, Admin, Open}`; `GatewayConfig.lab_service_token: Option<String>`, `GatewayConfig.admin_api_key: Option<String>`. Every management handler takes `scope: Scope` as its first extractor after `State`.

- [ ] **Step 1: Add the dependency**

Root `Cargo.toml`, under `# errors / logging` → put in `# misc`: `subtle = "2.6"`. `crates/gateway/Cargo.toml` `[dependencies]`: `subtle.workspace = true`.

- [ ] **Step 2: Write the failing tests**

Create `crates/gateway/src/auth.rs` with the interface and tests; the bodies are `todo!()` until Step 4:

```rust
//! Management API authentication (spec §4).
//!
//! Three modes, chosen by configuration:
//!
//! | `LAB_SERVICE_TOKEN` | `ADMIN_API_KEY` | caller sends | principal |
//! |---|---|---|---|
//! | set | - | `Bearer <token>` + **required** `X-Glutony-Tenant-Id` | `Lab` |
//! | - | set | `Bearer <key>`, `X-Glutony-Tenant-Id` optional | `Admin` |
//! | unset | unset | nothing | `Open`: tenant from the trusted edge, as before |
//!
//! When either secret is set, the tenant of a management request comes **only** from
//! `X-Glutony-Tenant-Id`: trusted `X-Meili-*` headers front ingest, they never manage.

use axum::extract::FromRequestParts;
use axum::http::HeaderMap;
use axum::http::request::Parts;
use subtle::ConstantTimeEq;

use crate::context::{bearer_token, header, resolve_tenant_id};
use crate::error::GatewayError;
use crate::state::{AppState, GatewayConfig};

/// Header the Lab (or an operator) names the tenant with.
pub const H_GLUTONY_TENANT_ID: &str = "x-glutony-tenant-id";

/// Who is calling a management route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Principal {
    /// The Meilisearch Lab, with `LAB_SERVICE_TOKEN`; always acts for one tenant.
    Lab,
    /// An operator with `ADMIN_API_KEY`; global unless it names a tenant.
    Admin,
    /// No management auth configured (today's behavior).
    Open,
}

/// The authenticated caller of a management route and the tenant it acts for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Scope {
    /// Who called.
    pub principal: Principal,
    /// The tenant the call acts for; `None` is the global scope.
    pub tenant_id: Option<String>,
}

impl Scope {
    /// The tenant, borrowed.
    pub fn tenant(&self) -> Option<&str> {
        self.tenant_id.as_deref()
    }
}

/// Whether any management secret is configured.
pub fn management_auth_enabled(config: &GatewayConfig) -> bool {
    todo!()
}

/// Authenticate a management request.
pub fn authorize(headers: &HeaderMap, config: &GatewayConfig) -> Result<Scope, GatewayError> {
    todo!()
}

impl FromRequestParts<AppState> for Scope {
    type Rejection = GatewayError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &AppState,
    ) -> Result<Self, Self::Rejection> {
        authorize(&parts.headers, &state.config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                HeaderValue::from_str(v).unwrap(),
            );
        }
        h
    }

    fn cfg(lab: Option<&str>, admin: Option<&str>) -> GatewayConfig {
        GatewayConfig {
            lab_service_token: lab.map(str::to_string),
            admin_api_key: admin.map(str::to_string),
            ..GatewayConfig::default()
        }
    }

    #[test]
    fn open_mode_takes_the_tenant_from_the_edge() {
        let c = cfg(None, None);
        assert_eq!(
            authorize(&HeaderMap::new(), &c).unwrap(),
            Scope { principal: Principal::Open, tenant_id: None }
        );
        let h = headers(&[("x-meili-project-id", "proj-1")]);
        assert_eq!(authorize(&h, &c).unwrap().tenant(), Some("proj-1"));
    }

    #[test]
    fn a_missing_or_wrong_token_is_401() {
        let c = cfg(Some("lab-secret"), Some("admin-secret"));
        for h in [
            HeaderMap::new(),
            headers(&[("authorization", "Bearer nope")]),
            headers(&[("authorization", "Basic lab-secret")]),
            headers(&[("authorization", "Bearer lab-secretX")]),
        ] {
            let err = authorize(&h, &c).unwrap_err();
            assert_eq!(err.code(), "unauthorized", "{h:?}");
        }
        // A wrong token wins over a bad tenant: never tell an unauthenticated caller
        // anything about tenants.
        let h = headers(&[("authorization", "Bearer nope"), ("x-glutony-tenant-id", "a/b")]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "unauthorized");
    }

    #[test]
    fn the_lab_must_name_a_valid_tenant() {
        let c = cfg(Some("lab-secret"), None);
        let h = headers(&[("authorization", "Bearer lab-secret")]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "invalid_tenant");
        let h = headers(&[("authorization", "Bearer lab-secret"), ("x-glutony-tenant-id", "a b")]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "invalid_tenant");
        let h = headers(&[("authorization", "Bearer lab-secret"), ("x-glutony-tenant-id", "acct-1")]);
        assert_eq!(
            authorize(&h, &c).unwrap(),
            Scope { principal: Principal::Lab, tenant_id: Some("acct-1".into()) }
        );
    }

    #[test]
    fn the_admin_is_global_unless_it_names_a_tenant() {
        let c = cfg(None, Some("admin-secret"));
        let h = headers(&[("authorization", "Bearer admin-secret")]);
        assert_eq!(
            authorize(&h, &c).unwrap(),
            Scope { principal: Principal::Admin, tenant_id: None }
        );
        let h = headers(&[("authorization", "bearer admin-secret"), ("x-glutony-tenant-id", "t1")]);
        assert_eq!(authorize(&h, &c).unwrap().tenant(), Some("t1"));
    }

    #[test]
    fn a_token_only_counts_for_the_mode_it_configures() {
        // The admin key does not open the Lab mode and vice versa.
        let c = cfg(None, Some("admin-secret"));
        let h = headers(&[("authorization", "Bearer lab-secret"), ("x-glutony-tenant-id", "t1")]);
        assert_eq!(authorize(&h, &c).unwrap_err().code(), "unauthorized");
    }

    #[test]
    fn auth_on_ignores_edge_tenant_headers() {
        // ENVOY_TRUSTED_HEADER unset, so X-Meili-* would be trusted in open mode.
        let c = cfg(Some("lab-secret"), None);
        let h = headers(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "acct-b"),
            ("x-meili-tenant-id", "acct-a"),
            ("x-meili-project-id", "proj-a"),
        ]);
        assert_eq!(authorize(&h, &c).unwrap().tenant(), Some("acct-b"));
        assert!(management_auth_enabled(&c));
        assert!(!management_auth_enabled(&cfg(None, None)));
    }
}
```

In `crates/gateway/src/state.rs`, extend `config_debug_redacts_secrets` so the config literal also sets `lab_service_token: Some("lab-secret-value".into())` and `admin_api_key: Some("admin-secret-value".into())`, and assert neither string appears in `format!("{cfg:?}")`.

In `crates/gateway/src/handlers/pipelines.rs` test module add:

```rust
    #[tokio::test]
    async fn management_routes_need_the_token_when_auth_is_on() {
        let server = MockServer::start().await;
        let config = GatewayConfig {
            lab_service_token: Some("lab-secret".into()),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;
        let resp = app
            .clone()
            .oneshot(Request::get("/pipelines").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(resp.headers()["www-authenticate"], "Bearer");

        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .and(query_param("tenant_id", "acct-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .expect(1)
            .mount(&server)
            .await;
        let resp = app
            .oneshot(
                Request::get("/pipelines")
                    .header("authorization", "Bearer lab-secret")
                    .header("x-glutony-tenant-id", "acct-1")
                    // A trusted edge header for another tenant must not win.
                    .header("x-meili-project-id", "someone-else")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
```

(Add any of `Mock`, `method`, `path`, `query_param`, `ResponseTemplate`, `Request`, `Body`, `StatusCode`, `ServiceExt` the module does not already import.)

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-gateway auth management_routes_need config_debug`
Expected: FAIL to compile (`lab_service_token` is not a field of `GatewayConfig`).

- [ ] **Step 4: Implement**

`crates/gateway/src/state.rs`, `GatewayConfig`: add after `write_preflight`:

```rust
    /// Bearer token the Meilisearch Lab uses on management routes (`LAB_SERVICE_TOKEN`).
    /// Setting it (or `admin_api_key`) closes open mode. See [`crate::auth`].
    pub lab_service_token: Option<String>,
    /// Bearer token an operator uses on management routes (`ADMIN_API_KEY`).
    pub admin_api_key: Option<String>,
```

`Default`: both `None`. `from_env`: `lab_service_token: env_opt("LAB_SERVICE_TOKEN"), admin_api_key: env_opt("ADMIN_API_KEY"),`. `Debug`: `.field("lab_service_token", &self.lab_service_token.as_ref().map(|_| "<redacted>"))` and the same for `admin_api_key`.

`crates/gateway/src/auth.rs` bodies:

```rust
pub fn management_auth_enabled(config: &GatewayConfig) -> bool {
    config.lab_service_token.is_some() || config.admin_api_key.is_some()
}

pub fn authorize(headers: &HeaderMap, config: &GatewayConfig) -> Result<Scope, GatewayError> {
    if !management_auth_enabled(config) {
        return Ok(Scope {
            principal: Principal::Open,
            tenant_id: resolve_tenant_id(headers, config)?,
        });
    }
    let unauthorized =
        || GatewayError::Unauthorized("missing or invalid management token".into());
    let token = bearer_token(headers).ok_or_else(unauthorized)?;
    let principal = if token_matches(config.lab_service_token.as_deref(), &token) {
        Principal::Lab
    } else if token_matches(config.admin_api_key.as_deref(), &token) {
        Principal::Admin
    } else {
        return Err(unauthorized());
    };
    let tenant_id = match header(headers, H_GLUTONY_TENANT_ID) {
        Some(t) => {
            meili_ingest_plugin_sdk::validate_tenant_id(&t)
                .map_err(GatewayError::InvalidTenant)?;
            Some(t)
        }
        None if principal == Principal::Lab => {
            return Err(GatewayError::InvalidTenant(format!(
                "{H_GLUTONY_TENANT_ID} is required with the Lab service token"
            )));
        }
        None => None,
    };
    Ok(Scope { principal, tenant_id })
}

/// Constant-time comparison against a configured secret.
fn token_matches(expected: Option<&str>, got: &str) -> bool {
    expected.is_some_and(|e| bool::from(e.as_bytes().ct_eq(got.as_bytes())))
}
```

`crates/gateway/src/lib.rs`: `pub mod auth;`.

`crates/gateway/src/error.rs`, `IntoResponse`: after building the response, add the challenge header on 401:

```rust
        let is_unauthorized = status == StatusCode::UNAUTHORIZED;
        let mut response = (status, Json(body)).into_response();
        if is_unauthorized {
            response.headers_mut().insert(
                axum::http::header::WWW_AUTHENTICATE,
                axum::http::HeaderValue::from_static("Bearer"),
            );
        }
        response
```

Handlers: in each handler below, add `scope: Scope` (import `use crate::auth::Scope;`) right after `State(state): State<AppState>`, replace `let tenant_id = resolve_tenant_id(&headers, &state.config)?;` with `let tenant_id = scope.tenant_id.clone();` (or use `scope.tenant()` where a `&str` is passed), and remove the `headers: HeaderMap` parameter and its import when it has no other use (clippy `-D warnings` rejects unused variables):
- `pipelines.rs`: `create_pipeline`, `list_pipelines`, `validate_pipeline`, `get_pipeline`, `delete_pipeline`.
- `connections.rs`: `list_connections`, `create_connection`, `get_connection`, `patch_connection`, `delete_connection`.
- `sources.rs`: `list_sources`, `create_source`, `get_source`, `patch_source`, `pause_source`, `unpause_source`, `run_source`, `delete_source`, `list_runs`; change `set_paused(state, uid, headers, paused)` to `set_paused(state: &AppState, uid: &str, tenant_id: Option<&str>, paused: bool)` and pass `scope.tenant()`.
- `jobs.rs`: `list_jobs` (push `("tenant_id", t)` from `scope.tenant_id`), `cancel_job` (add `scope: Scope`; it is used in Task 4 — until then name it `_scope`).
- `usage.rs`: `get_usage` (`let tenant_id = scope.tenant_id.clone().unwrap_or_default();`).
- `plugins.rs` `list_plugins` and `catalog.rs` `get_catalog`: add `_scope: Scope`. `get_catalog` has no `State`; give it `State(_state): State<AppState>, _scope: Scope` so the extractor has its state.

Existing handler tests that sent `x-meili-project-id` for scoping keep working in open mode (default config) — do not change them.

`crates/gateway/src/main.rs`, after the `ENVOY_TRUSTED_HEADER` warning:

```rust
    if !meili_ingest_gateway::auth::management_auth_enabled(&config) {
        tracing::warn!(
            "LAB_SERVICE_TOKEN and ADMIN_API_KEY are unset: management routes are open \
             (tenant from trusted X-Meili-* headers); keep them off any public hostname"
        );
    }
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-gateway`
Expected: PASS (new tests and every existing handler test).

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add Cargo.toml Cargo.lock crates/gateway
git commit -m "feat(gateway): management routes accept a Lab service token or an admin key

LAB_SERVICE_TOKEN requires X-Glutony-Tenant-Id; ADMIN_API_KEY is global unless it
names a tenant; neither keeps today's open mode. With auth on, trusted X-Meili-*
headers no longer set a management request's tenant."
```

---

### Task 4: Tenant scoping, `scope` on rows, job checks

**Files:**
- Modify: `crates/plugin-sdk/src/tenant.rs` (`RowScope`), `crates/plugin-sdk/src/lib.rs`, `crates/gateway/src/connections.rs` (`ConnectionView`), `crates/gateway/src/sources.rs` (`SourceView`), `crates/gateway/src/handlers/{pipelines,connections,jobs}.rs`
- Test: those handler test modules; `crates/control-plane/tests/db.rs`

**Interfaces:**
- Consumes: `Scope` (Task 3), `resolve_tenant_id` (Task 2).
- Produces: `meili_ingest_plugin_sdk::RowScope::{Tenant, Global, Builtin}` (`serde(rename_all = "snake_case")`, `Default = Global`) with `RowScope::of(builtin: bool, tenant_id: Option<&str>) -> RowScope`; `gateway::handlers::pipelines::PipelineView { #[serde(flatten)] pipeline: PipelineDefinition, scope: RowScope }`; `ConnectionView.scope`, `SourceView.scope`; `handlers::jobs::ensure_job_visible(&AppState, Uuid, Option<&str>) -> Result<(), GatewayError>`.

- [ ] **Step 1: Write the failing tests**

`crates/plugin-sdk/src/tenant.rs` tests:

```rust
    #[test]
    fn row_scope_of_a_row() {
        assert_eq!(RowScope::of(true, None), RowScope::Builtin);
        assert_eq!(RowScope::of(false, None), RowScope::Global);
        assert_eq!(RowScope::of(false, Some("t1")), RowScope::Tenant);
        assert_eq!(serde_json::to_value(RowScope::Tenant).unwrap(), "tenant");
    }
```

`crates/gateway/src/handlers/jobs.rs` test module (reuse its `record(job_id)` helper; set `tenant_id` on the returned record):

```rust
    fn lab_config() -> GatewayConfig {
        GatewayConfig {
            lab_service_token: Some("lab-secret".into()),
            ..GatewayConfig::default()
        }
    }

    async fn mount_job(server: &MockServer, job_id: Uuid, tenant: Option<&str>) {
        let mut r = record(job_id);
        r.tenant_id = tenant.map(str::to_string);
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&r))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_tenant_cannot_read_or_cancel_another_tenants_job() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;

        // Data route: tenant from the trusted edge (ENVOY_TRUSTED_HEADER unset).
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .header("x-meili-tenant-id", "acct-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Management route: tenant from the Lab principal.
        let (app, starter_lab) = test_app(&server, lab_config()).await;
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .header("authorization", "Bearer lab-secret")
                    .header("x-glutony-tenant-id", "acct-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(starter_lab.cancelled().is_empty());
        assert!(starter.cancelled().is_empty());
    }

    #[tokio::test]
    async fn the_owner_cancels_its_job() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;
        let (app, starter) = test_app(&server, lab_config()).await;
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .header("authorization", "Bearer lab-secret")
                    .header("x-glutony-tenant-id", "acct-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(starter.cancelled(), vec![job_id]);
    }

    #[tokio::test]
    async fn job_without_a_row_is_hidden_from_tenants() {
        // The gateway's best-effort job insert failed: Temporal knows the job, the
        // control plane does not.
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(
                serde_json::json!({"error": "not found", "code": "not_found"}),
            ))
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.set_snapshot(
            job_id,
            JobSnapshot { status: JobStatus::Running, progress: None },
        );
        let with_tenant = app
            .clone()
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .header("x-meili-tenant-id", "acct-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(with_tenant.status(), StatusCode::NOT_FOUND);
        // No tenant: unchanged, the Temporal snapshot answers.
        let without = app
            .oneshot(Request::get(format!("/jobs/{job_id}")).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(without.status(), StatusCode::OK);
    }
```

(`JobSnapshot` may have more fields; construct it the way the module's existing snapshot tests do.)

`crates/gateway/src/handlers/connections.rs` test module (reuse its `app`, `call`, `get`, `record` helpers; `record` returns JSON — set `"tenant_id"` on it):

```rust
    #[tokio::test]
    async fn a_tenant_cannot_delete_a_global_connection() {
        let cp = MockServer::start().await;
        let global = record("prod", "https://m.example", b"sealed"); // no tenant_id
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(global))
            .mount(&cp)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/internal/connections/prod"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&cp)
            .await;
        let app = app(&cp, Some(test_key()));
        let req = Request::delete("/connections/prod")
            .header("x-meili-project-id", "tenant-1")
            .body(Body::empty())
            .unwrap();
        let (status, _) = call(&app, req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn rows_say_whose_they_are() {
        let cp = MockServer::start().await;
        let mut own = record("mine", "https://m.example", b"sealed");
        own["tenant_id"] = "tenant-1".into();
        let global = record("prod", "https://m.example", b"sealed");
        Mock::given(method("GET"))
            .and(path("/internal/connections"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([own, global])))
            .mount(&cp)
            .await;
        let app = app(&cp, Some(test_key()));
        let req = Request::get("/connections")
            .header("x-meili-project-id", "tenant-1")
            .body(Body::empty())
            .unwrap();
        let (status, body) = call(&app, req).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body[0]["scope"], "tenant");
        assert_eq!(body[1]["scope"], "global");
    }
```

(`test_key()` stands for however the module's existing tests build the `Arc<SecretKey>` they pass to `app`; reuse that exact expression.)

`crates/gateway/src/handlers/pipelines.rs` test module:

```rust
    #[tokio::test]
    async fn pipelines_say_whose_they_are() {
        let server = MockServer::start().await;
        let mut own = sample_pipeline("mine", None);
        own.tenant_id = Some("t1".into());
        let global = sample_pipeline("shared", None);
        let mut builtin = sample_pipeline("builtin.pdf", None);
        builtin.builtin = true;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![own, global, builtin]))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(
                Request::get("/pipelines")
                    .header("x-meili-project-id", "t1")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let json = json_body(resp).await;
        assert_eq!(json[0]["scope"], "tenant");
        assert_eq!(json[1]["scope"], "global");
        assert_eq!(json[2]["scope"], "builtin");
        assert_eq!(json[0]["uid"], "mine", "the definition is flattened, not nested");
    }
```

`crates/control-plane/tests/db.rs`:

```rust
#[tokio::test]
async fn a_tenant_delete_never_reaches_the_global_pipeline() {
    let Some(t) = setup().await else { return };
    let app = app(AppState::new(t.pool.clone()));
    let def = serde_json::json!({
        "uid": "shared", "name": "shared",
        "steps": [{"id": "index", "plugin": "meili_indexer"}]
    });
    let (status, _) = call(app.clone(), req_json("POST", "/pipelines", &def)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, _) = call(
        app.clone(),
        Request::delete("/pipelines/shared?tenant_id=t1").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(app, get("/pipelines/shared")).await;
    assert_eq!(status, StatusCode::OK, "the global pipeline is still there");
    t.drop_schema().await;
}
```

(If `POST /pipelines` answers `200` rather than `201` in this crate, assert what the existing create tests in `db.rs` assert.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-plugin-sdk row_scope && cargo test -p meili-ingest-gateway`
Expected: FAIL (`RowScope` missing; job and connection tests get `200`/`202`/`204` instead of `404`; no `scope` field).

- [ ] **Step 3: Implement**

`crates/plugin-sdk/src/tenant.rs`:

```rust
/// Whose a pipeline, source or connection is, as seen by the caller.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum RowScope {
    /// Owned by the caller's tenant.
    Tenant,
    /// Shared by every tenant; read-only for tenants.
    #[default]
    Global,
    /// Compiled into glutony; read-only for everyone.
    Builtin,
}

impl RowScope {
    /// The scope of a row from its flags.
    pub fn of(builtin: bool, tenant_id: Option<&str>) -> Self {
        match (builtin, tenant_id) {
            (true, _) => RowScope::Builtin,
            (false, Some(_)) => RowScope::Tenant,
            (false, None) => RowScope::Global,
        }
    }
}
```

and `pub use tenant::RowScope;` in `lib.rs`.

`ConnectionView` and `SourceView`: add

```rust
    /// Whose row this is (spec §4.3).
    #[serde(default)]
    pub scope: RowScope,
```

set in their `from_record` constructors with `RowScope::of(false, r.tenant_id.as_deref())` (for sources, `d.tenant_id.as_deref()`).

`handlers/pipelines.rs`:

```rust
/// A pipeline as the API returns it: the definition plus whose it is.
#[derive(Debug, Clone, Serialize)]
pub struct PipelineView {
    /// The definition, flattened into the response object.
    #[serde(flatten)]
    pub pipeline: PipelineDefinition,
    /// Whose it is.
    pub scope: RowScope,
}

impl From<PipelineDefinition> for PipelineView {
    fn from(pipeline: PipelineDefinition) -> Self {
        let scope = RowScope::of(pipeline.builtin, pipeline.tenant_id.as_deref());
        Self { pipeline, scope }
    }
}
```

`list_pipelines` returns `Json<Vec<PipelineView>>` (`.into_iter().map(PipelineView::from).collect()`), `get_pipeline` returns `Json<PipelineView>`, `create_pipeline` returns `(StatusCode, Json<PipelineView>)`. Update existing tests that deserialize those responses as `PipelineDefinition`: `serde_json::from_value::<PipelineDefinition>` still works because the view is a superset (unknown `scope` field is ignored unless `PipelineDefinition` has `deny_unknown_fields`; if it does, read the fields they assert from `serde_json::Value` instead).

`handlers/connections.rs` `delete_connection`: before calling `delete_connection`, check ownership exactly like `patch_connection` does:

```rust
    // Writes never reach a row outside the caller's exact scope (spec §4.3).
    state
        .control_plane
        .get_connection(&uid, tenant_id.as_deref())
        .await?
        .filter(|r| r.tenant_id == tenant_id)
        .ok_or_else(|| not_found(&uid))?;
```

`handlers/jobs.rs`:

```rust
/// `404` unless the job belongs to `tenant`. No tenant: no check (today's behavior,
/// and an admin's global view). A job without a control-plane row is hidden from
/// tenants rather than shown unchecked.
pub async fn ensure_job_visible(
    state: &AppState,
    job_id: Uuid,
    tenant: Option<&str>,
) -> Result<(), GatewayError> {
    let Some(tenant) = tenant else {
        return Ok(());
    };
    let not_found = || GatewayError::NotFound(format!("job {job_id} not found"));
    match state.control_plane.get_job(job_id).await {
        Ok(r) if r.tenant_id.as_deref() == Some(tenant) => Ok(()),
        Ok(_) | Err(GatewayError::NotFound(_)) => Err(not_found()),
        Err(e) => Err(e),
    }
}
```

`get_job` gains `headers: HeaderMap` and starts with:

```rust
    let job_id = parse_job_id(&id)?;
    let tenant = resolve_tenant_id(&headers, &state.config)?;
    ensure_job_visible(&state, job_id, tenant.as_deref()).await?;
```

`cancel_job` renames `_scope` to `scope` and calls `ensure_job_visible(&state, job_id, scope.tenant()).await?;` before `state.temporal.cancel`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-plugin-sdk && cargo test -p meili-ingest-gateway && DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane --test db`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add crates/plugin-sdk crates/gateway crates/control-plane/tests/db.rs
git commit -m "feat(gateway): rows carry their scope; tenants cannot read, cancel or delete what is not theirs"
```

---

### Task 5: Route table, OpenAPI completion, parity tests

**Files:**
- Create: `crates/gateway/src/routes.rs`
- Modify: `crates/gateway/src/lib.rs` (`pub mod routes;`), `crates/gateway/Cargo.toml` (dev-dep `regex`), `docs/openapi.yaml`
- Test: `crates/gateway/src/routes.rs`

**Interfaces:**
- Consumes: the router in `lib.rs`, `test_support::{test_app, GatewayConfig}`.
- Produces: `gateway::routes::{ROUTES, RouteSpec, RouteClass}`; `RouteSpec { method: &'static str, path: &'static str, class: RouteClass }`; `RouteClass::{Data, Management}`. OpenAPI security schemes `MeiliKey`, `LabServiceToken`, `AdminKey`; parameters `XGlutonyTenantId`, `XMeiliTenantId`; responses `Unauthorized`, `InvalidTenant`; schemas `RowScope`, `JobList`, `JobRecord`, `ValidationResult`, `UsageResponse`, `UsageRow`.

- [ ] **Step 1: Write the route table and the failing tests**

Create `crates/gateway/src/routes.rs`:

```rust
//! Every route the gateway mounts, and whether it is a data or a management route.
//!
//! This table is the source of truth the tests below hold `router()` (in `lib.rs`),
//! `docs/openapi.yaml` and the auth split (spec §4.1) against: adding a route means
//! adding it here, in `lib.rs` and in the OpenAPI spec, or the build fails.

/// Which auth model a route follows (spec §4.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RouteClass {
    /// The caller's Meilisearch key plus trusted edge headers.
    Data,
    /// `ManagementAuth`: Lab service token, admin key, or open mode.
    Management,
}

/// One mounted route.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RouteSpec {
    /// Upper-case HTTP method.
    pub method: &'static str,
    /// Path as written in `router()`.
    pub path: &'static str,
    /// Auth model.
    pub class: RouteClass,
}

const fn data(method: &'static str, path: &'static str) -> RouteSpec {
    RouteSpec { method, path, class: RouteClass::Data }
}
const fn mgmt(method: &'static str, path: &'static str) -> RouteSpec {
    RouteSpec { method, path, class: RouteClass::Management }
}

/// Every route `router()` mounts, except the admin UI's.
pub const ROUTES: &[RouteSpec] = &[
    data("GET", "/health"),
    data("POST", "/ingest"),
    data("POST", "/ingest/batch"),
    data("POST", "/ingest/pipeline/{name}"),
    data("POST", "/indexes/{index_uid}/ingest"),
    data("POST", "/indexes/{index_uid}/ingest/batch"),
    data("POST", "/indexes/{index_uid}/ingest/pipeline/{name}"),
    data("GET", "/jobs/{id}"),
    mgmt("GET", "/jobs"),
    mgmt("POST", "/jobs/{id}/cancel"),
    mgmt("GET", "/pipelines"),
    mgmt("POST", "/pipelines"),
    mgmt("POST", "/pipelines/validate"),
    mgmt("GET", "/pipelines/{name}"),
    mgmt("DELETE", "/pipelines/{name}"),
    mgmt("GET", "/connections"),
    mgmt("POST", "/connections"),
    mgmt("GET", "/connections/{uid}"),
    mgmt("PATCH", "/connections/{uid}"),
    mgmt("DELETE", "/connections/{uid}"),
    mgmt("GET", "/sources"),
    mgmt("POST", "/sources"),
    mgmt("GET", "/sources/{uid}"),
    mgmt("PATCH", "/sources/{uid}"),
    mgmt("DELETE", "/sources/{uid}"),
    mgmt("POST", "/sources/{uid}/pause"),
    mgmt("POST", "/sources/{uid}/unpause"),
    mgmt("POST", "/sources/{uid}/run"),
    mgmt("GET", "/sources/{uid}/runs"),
    mgmt("GET", "/plugins"),
    mgmt("GET", "/catalog"),
    mgmt("GET", "/usage"),
];

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;
    use wiremock::MockServer;

    use super::*;
    use crate::test_support::*;

    /// `/jobs/{job_id}` and `/jobs/{id}` are the same route.
    fn normalize(path: &str) -> String {
        regex::Regex::new(r"\{[^}]+\}").unwrap().replace_all(path, "{}").into_owned()
    }

    fn table() -> BTreeSet<(String, String)> {
        ROUTES.iter().map(|r| (r.method.to_string(), normalize(r.path))).collect()
    }

    #[test]
    fn the_table_lists_every_route_in_lib_rs() {
        let source = include_str!("lib.rs");
        let re = regex::Regex::new(r#"\.route\(\s*"([^"]+)""#).unwrap();
        let mounted: BTreeSet<String> =
            re.captures_iter(source).map(|c| normalize(&c[1])).collect();
        let listed: BTreeSet<String> = ROUTES.iter().map(|r| normalize(r.path)).collect();
        assert_eq!(mounted, listed, "lib.rs router() and ROUTES disagree");
    }

    fn openapi() -> serde_yaml::Value {
        serde_yaml::from_str(include_str!("../../../docs/openapi.yaml")).unwrap()
    }

    #[test]
    fn the_table_matches_the_openapi_spec() {
        let spec = openapi();
        let mut documented = BTreeSet::new();
        for (path, item) in spec["paths"].as_mapping().unwrap() {
            for (method, _) in item.as_mapping().unwrap() {
                let method = method.as_str().unwrap();
                if ["get", "post", "put", "patch", "delete"].contains(&method) {
                    documented.insert((method.to_uppercase(), normalize(path.as_str().unwrap())));
                }
            }
        }
        assert_eq!(documented, table(), "docs/openapi.yaml and ROUTES disagree");
    }

    #[test]
    fn the_openapi_security_follows_the_route_class() {
        let spec = openapi();
        for r in ROUTES {
            let item = spec["paths"]
                .as_mapping()
                .unwrap()
                .iter()
                .find(|(p, _)| normalize(p.as_str().unwrap()) == normalize(r.path))
                .map(|(_, v)| v)
                .unwrap();
            let op = &item[r.method.to_lowercase().as_str()];
            let schemes: BTreeSet<String> = op["security"]
                .as_sequence()
                .unwrap_or_else(|| panic!("{} {} has no security block", r.method, r.path))
                .iter()
                .flat_map(|req| req.as_mapping().unwrap().keys().cloned())
                .map(|k| k.as_str().unwrap().to_string())
                .collect();
            let management = schemes.contains("LabServiceToken") && schemes.contains("AdminKey");
            assert_eq!(
                management,
                r.class == RouteClass::Management,
                "{} {}: security {schemes:?}",
                r.method,
                r.path
            );
        }
    }

    fn concrete(path: &str) -> String {
        path.replace("{id}", "00000000-0000-0000-0000-000000000001")
            .replace("{name}", "p")
            .replace("{uid}", "u")
            .replace("{index_uid}", "docs")
    }

    #[tokio::test]
    async fn every_management_route_is_guarded() {
        let server = MockServer::start().await;
        let config = GatewayConfig {
            admin_api_key: Some("admin-secret".into()),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;
        for r in ROUTES.iter().filter(|r| r.class == RouteClass::Management) {
            let req = Request::builder()
                .method(r.method)
                .uri(concrete(r.path))
                .body(Body::empty())
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{} {}", r.method, r.path);
        }
    }

    #[tokio::test]
    async fn every_data_route_is_mounted() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        for r in ROUTES.iter().filter(|r| r.class == RouteClass::Data) {
            let req = Request::builder()
                .method(r.method)
                .uri(concrete(r.path))
                .body(Body::empty())
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            let status = resp.status();
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert_ne!(status, StatusCode::METHOD_NOT_ALLOWED, "{} {}", r.method, r.path);
            // The router's own 404 has an empty body; a handler's 404 is JSON.
            assert!(
                status != StatusCode::NOT_FOUND || !body.is_empty(),
                "{} {} is not mounted",
                r.method,
                r.path
            );
        }
    }
}
```

`crates/gateway/Cargo.toml` `[dev-dependencies]`: add `regex.workspace = true` and `http-body-util = "0.1"`. `lib.rs`: `pub mod routes;`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-gateway routes`
Expected: `the_table_lists_every_route_in_lib_rs`, `every_management_route_is_guarded` and `every_data_route_is_mounted` PASS; `the_table_matches_the_openapi_spec` FAILS (missing `GET /jobs`, `POST /pipelines/validate`, `GET /usage`) and `the_openapi_security_follows_the_route_class` FAILS (no per-operation `security`).

- [ ] **Step 3: Complete `docs/openapi.yaml`**

1. Delete the top-level `security:` block (the two lines after `servers:`).
2. Replace `components.securitySchemes` with:

```yaml
  securitySchemes:
    MeiliKey:
      type: http
      scheme: bearer
      description: |
        Data routes. A Meilisearch API key with write access on the target index,
        used when no trusted `X-Meili-Api-Key` header is present.
    LabServiceToken:
      type: http
      scheme: bearer
      description: |
        Management routes, for the Meilisearch Lab (`LAB_SERVICE_TOKEN`). Requires
        `X-Glutony-Tenant-Id`. Server-to-server only.
    AdminKey:
      type: http
      scheme: bearer
      description: |
        Management routes, for an operator (`ADMIN_API_KEY`). Global scope unless
        `X-Glutony-Tenant-Id` names a tenant.
```

3. Under `components.parameters` add:

```yaml
    XGlutonyTenantId:
      name: X-Glutony-Tenant-Id
      in: header
      required: false
      description: |
        Tenant a management call acts for. Required with `LabServiceToken`, optional
        with `AdminKey`, ignored in open mode. 1-128 characters of `[A-Za-z0-9._:-]`.
      schema:
        type: string
        pattern: "^[A-Za-z0-9._:-]{1,128}$"
      example: 0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61
    XMeiliTenantId:
      name: X-Meili-Tenant-Id
      in: header
      required: false
      description: Tenant id injected by a trusted edge. Wins over `X-Meili-Project-Id`.
      schema:
        type: string
        pattern: "^[A-Za-z0-9._:-]{1,128}$"
```

   and change `XMeiliProjectId`'s description to `Tenant id injected by Meilisearch Cloud's Envoy. Used when X-Meili-Tenant-Id is absent.`

4. Under `components.responses` add:

```yaml
    Unauthorized:
      description: Management auth is configured and the token is missing or wrong.
      headers:
        WWW-Authenticate:
          schema: { type: string, const: Bearer }
      content:
        application/json:
          schema: { $ref: "#/components/schemas/Error" }
          example: { "error": "missing or invalid management token", "code": "unauthorized" }
    InvalidTenant:
      description: The tenant header is missing (Lab token) or malformed.
      content:
        application/json:
          schema: { $ref: "#/components/schemas/Error" }
          example: { "error": "invalid tenant id: tenant id contains '/'", "code": "invalid_tenant" }
```

5. Under `components.schemas` add:

```yaml
    RowScope:
      type: string
      enum: [tenant, global, builtin]
      description: Whose the row is. Tenants can read global and built-in rows but only write their own.
    JobRecord:
      type: object
      required: [job_id, workflow_id, pipeline_uid, status, started_at, updated_at]
      properties:
        job_id: { type: string, format: uuid }
        workflow_id: { type: string }
        pipeline_uid: { type: string }
        tenant_id: { type: string, description: "Tenant scope. Absent = global." }
        index_name: { type: string }
        status: { $ref: "#/components/schemas/JobStatus" }
        current_step: { type: string }
        error: { type: string }
        started_at: { type: string, format: date-time }
        updated_at: { type: string, format: date-time }
        source_id: { type: string, format: uuid }
    JobList:
      type: object
      required: [jobs, limit, offset, total]
      properties:
        jobs: { type: array, items: { $ref: "#/components/schemas/JobRecord" } }
        limit: { type: integer }
        offset: { type: integer }
        total: { type: integer, description: "Rows matching the filters, ignoring paging." }
    ValidationResult:
      type: object
      required: [valid, order, normalized]
      properties:
        valid: { type: boolean, const: true }
        order: { type: array, items: { type: string }, description: "Topological step order." }
        normalized: { $ref: "#/components/schemas/PipelineDefinition" }
    UsageRow:
      type: object
      required: [day, pipeline_uid, plugin]
      properties:
        day: { type: string, format: date }
        pipeline_uid: { type: string }
        plugin: { type: string, description: "Empty on job-level rows." }
      additionalProperties:
        description: Numeric metrics passed through from the analytics store.
    UsageResponse:
      type: object
      required: [tenant_id, data]
      properties:
        tenant_id: { type: string, description: "Empty when self-hosted." }
        data: { type: array, items: { $ref: "#/components/schemas/UsageRow" } }
```

   and add `scope: { $ref: "#/components/schemas/RowScope" }` (with `readOnly: true`) to the properties of `PipelineDefinition`, `Connection` and `Source`.

6. Add the three missing paths (place `/jobs` before `/jobs/{job_id}`, `/pipelines/validate` before `/pipelines/{uid}`, `/usage` after `/catalog`):

```yaml
  /jobs:
    get:
      tags: [Jobs]
      operationId: listJobs
      summary: List jobs, newest first
      description: Always scoped to the caller's tenant; an admin without a tenant sees every job.
      security: [{ LabServiceToken: [] }, { AdminKey: [] }, {}]
      parameters:
        - $ref: "#/components/parameters/XGlutonyTenantId"
        - { name: status, in: query, schema: { $ref: "#/components/schemas/JobStatus" } }
        - { name: pipeline_uid, in: query, schema: { type: string } }
        - { name: limit, in: query, schema: { type: integer, minimum: 1, maximum: 200, default: 50 } }
        - { name: offset, in: query, schema: { type: integer, minimum: 0, default: 0 } }
      responses:
        "200":
          description: One page of jobs.
          content:
            application/json:
              schema: { $ref: "#/components/schemas/JobList" }
        "400": { $ref: "#/components/responses/InvalidTenant" }
        "401": { $ref: "#/components/responses/Unauthorized" }
        "502": { $ref: "#/components/responses/UpstreamUnavailable" }

  /pipelines/validate:
    post:
      tags: [Pipelines]
      operationId: validatePipeline
      summary: Dry-run a pipeline definition
      description: Runs the same checks as a create and persists nothing.
      security: [{ LabServiceToken: [] }, { AdminKey: [] }, {}]
      parameters:
        - $ref: "#/components/parameters/XGlutonyTenantId"
      requestBody:
        required: true
        content:
          application/json:
            schema: { $ref: "#/components/schemas/PipelineDefinition" }
          application/x-yaml:
            schema: { $ref: "#/components/schemas/PipelineDefinition" }
      responses:
        "200":
          description: The pipeline is valid.
          content:
            application/json:
              schema: { $ref: "#/components/schemas/ValidationResult" }
        "401": { $ref: "#/components/responses/Unauthorized" }
        "422": { $ref: "#/components/responses/Unprocessable" }

  /usage:
    get:
      tags: [System]
      operationId: getUsage
      summary: Daily usage of the caller's tenant
      security: [{ LabServiceToken: [] }, { AdminKey: [] }, {}]
      parameters:
        - $ref: "#/components/parameters/XGlutonyTenantId"
        - { name: date_from, in: query, required: true, schema: { type: string, format: date } }
        - { name: date_to, in: query, required: true, schema: { type: string, format: date } }
      responses:
        "200":
          description: Daily rows, oldest first.
          content:
            application/json:
              schema: { $ref: "#/components/schemas/UsageResponse" }
        "401": { $ref: "#/components/responses/Unauthorized" }
        "501": { $ref: "#/components/responses/NotEnabled" }
```

7. On every **existing** operation add a `security` line right after `summary`:
   - data routes (`/health` keeps `security: []`… change it to `security: [{}]`; `/ingest*`, `/indexes/{index_uid}/ingest*`, `GET /jobs/{job_id}`): `security: [{ MeiliKey: [] }, {}]`;
   - every other operation: `security: [{ LabServiceToken: [] }, { AdminKey: [] }, {}]`, plus `- $ref: "#/components/parameters/XGlutonyTenantId"` in its `parameters` (create the list if absent), and `"401": { $ref: "#/components/responses/Unauthorized" }` in its responses.
   Also add `- $ref: "#/components/parameters/XMeiliTenantId"` next to every existing `XMeiliProjectId` reference.
8. In `info.description`, replace the **Tenant context** paragraph's first sentence list to include `X-Meili-Tenant-Id` before `X-Meili-Project-Id`, and add a paragraph:

```yaml
    **Management auth**

    Data routes (ingest, job status, health) use the caller's Meilisearch key. Every
    other route is a management route: with `LAB_SERVICE_TOKEN` or `ADMIN_API_KEY`
    set it needs `Authorization: Bearer <token>` (and `X-Glutony-Tenant-Id` for the
    Lab), otherwise it is open and scoped by the trusted `X-Meili-*` headers.
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-gateway routes`
Expected: PASS (all five).

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add crates/gateway docs/openapi.yaml Cargo.lock
git commit -m "docs(openapi): every route documented with its auth; a test keeps router and spec in sync"
```

---

### Task 6: Provider cost table and priced plugin calls

**Files:**
- Create: `crates/plugin-sdk/src/cost.rs`, `config/provider-costs.toml`
- Modify: `Cargo.toml` (workspace dep `toml`), `crates/plugin-sdk/Cargo.toml`, `crates/plugin-sdk/src/lib.rs`, `crates/plugin-sdk/src/types.rs` (`UsageUnits`), `crates/plugins/llm-enricher/src/lib.rs`, `crates/plugins/image-captioner/src/lib.rs`, `crates/plugins/audio-transcriber/src/lib.rs`, `crates/plugins/jev-enricher/src/lib.rs`
- Test: `crates/plugin-sdk/src/cost.rs`, `crates/plugin-sdk/src/types.rs`, each plugin's test module

**Interfaces:**
- Produces: `UsageUnits.cost_micro_usd: u64`, `UsageUnits.unpriced_calls: u64` (both `#[serde(default)]`, merged by `+=`); `meili_ingest_plugin_sdk::cost::{ProviderCosts, Price}`; `ProviderCosts::from_toml(&str) -> Result<ProviderCosts, String>`, `ProviderCosts::bundled() -> ProviderCosts`, `ProviderCosts::global() -> &'static ProviderCosts`, `ProviderCosts::price(&self, plugin: &str, model: &str) -> Option<Price>`, `ProviderCosts::apply(&self, plugin: &str, model: &str, units: &mut UsageUnits)`.

- [ ] **Step 1: Add dependencies**

Root `Cargo.toml` `# serialization`: `toml = "0.9"`. `crates/plugin-sdk/Cargo.toml` `[dependencies]`: `toml.workspace = true` and `tracing.workspace = true`.

- [ ] **Step 2: Write the failing tests**

In `crates/plugin-sdk/src/types.rs` test module:

```rust
    #[test]
    fn cost_fields_merge_and_default_for_old_payloads() {
        let mut a = UsageUnits { cost_micro_usd: 100, unpriced_calls: 1, ..UsageUnits::llm(10, 2) };
        a.merge(UsageUnits { cost_micro_usd: 50, ..UsageUnits::llm(1, 1) });
        assert_eq!(a.cost_micro_usd, 150);
        assert_eq!(a.unpriced_calls, 1);
        // A payload written before the fields existed.
        let old: UsageUnits = serde_json::from_value(serde_json::json!({"llm_requests": 1})).unwrap();
        assert_eq!((old.cost_micro_usd, old.unpriced_calls), (0, 0));
    }
```

Create `crates/plugin-sdk/src/cost.rs` with the API (`todo!()` bodies until Step 4) and tests:

```rust
//! Provider cost table: what glutony pays a provider for one call (spec §5.1).
//!
//! Prices are micro-USD, keyed by `(plugin, model)` with an optional `default` entry
//! per plugin. The bundled table (`config/provider-costs.toml`) is compiled in;
//! `PROVIDER_COSTS_FILE` replaces it. A call the table cannot price costs 0 and counts
//! in [`UsageUnits::unpriced_calls`], so the bill shows the gap instead of hiding it.
//! The provider behind a model depends on the deployment's `*_BASE_URL`: operators
//! who point a plugin at another provider must ship their own table.

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

use serde::Deserialize;

use crate::types::UsageUnits;

/// The bundled table.
const BUNDLED: &str = include_str!("../../../config/provider-costs.toml");

/// Price of one model, in micro-USD. Absent dimensions cost nothing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Price {
    /// Per million prompt tokens.
    #[serde(default)]
    pub input_per_mtok: u64,
    /// Per million completion tokens.
    #[serde(default)]
    pub output_per_mtok: u64,
    /// Per second of audio.
    #[serde(default)]
    pub per_audio_second: u64,
    /// Per request.
    #[serde(default)]
    pub per_request: u64,
}

/// The whole table: plugin name → model (or `default`) → price.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ProviderCosts {
    entries: HashMap<String, HashMap<String, Price>>,
}

impl ProviderCosts {
    /// Parse a TOML table.
    pub fn from_toml(source: &str) -> Result<Self, String> {
        todo!()
    }

    /// The compiled-in table.
    pub fn bundled() -> Self {
        todo!()
    }

    /// The process-wide table: `PROVIDER_COSTS_FILE` if set, else the bundled one.
    /// An unreadable or invalid file logs an error and falls back to the bundled table.
    pub fn global() -> &'static ProviderCosts {
        todo!()
    }

    /// The price of `model` for `plugin`, falling back to the plugin's `default`.
    pub fn price(&self, plugin: &str, model: &str) -> Option<Price> {
        todo!()
    }

    /// Price one provider call in place: add its cost to `units.cost_micro_usd`, or
    /// count it in `units.unpriced_calls` when the table cannot price it.
    pub fn apply(&self, plugin: &str, model: &str, units: &mut UsageUnits) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TABLE: &str = r#"
        [llm_enricher."gpt-4o-mini"]
        input_per_mtok = 150000
        output_per_mtok = 600000

        [whisper_transcriber."whisper-1"]
        per_audio_second = 100

        [jev_enricher.default]
        per_request = 250
    "#;

    fn costs() -> ProviderCosts {
        ProviderCosts::from_toml(TABLE).unwrap()
    }

    #[test]
    fn prices_tokens_rounding_up_to_the_micro_dollar() {
        let mut u = UsageUnits::llm(1_000, 100);
        costs().apply("llm_enricher", "gpt-4o-mini", &mut u);
        // 1000 * 0.15 + 100 * 0.6 = 150 + 60 micro-USD
        assert_eq!(u.cost_micro_usd, 210);
        assert_eq!(u.unpriced_calls, 0);
        let mut tiny = UsageUnits::llm(1, 0);
        costs().apply("llm_enricher", "gpt-4o-mini", &mut tiny);
        assert_eq!(tiny.cost_micro_usd, 1, "0.15 micro-USD rounds up");
    }

    #[test]
    fn prices_audio_seconds_and_requests() {
        let mut u = UsageUnits::transcription(12.5);
        costs().apply("whisper_transcriber", "whisper-1", &mut u);
        assert_eq!(u.cost_micro_usd, 1_250);
        let mut j = UsageUnits::llm(10, 2);
        costs().apply("jev_enricher", "jev-latest", &mut j);
        assert_eq!(j.cost_micro_usd, 250, "the plugin's default entry applies");
    }

    #[test]
    fn unknown_models_and_unknown_quantities_are_counted_not_guessed() {
        let mut u = UsageUnits::llm(1_000, 100);
        costs().apply("llm_enricher", "gpt-9", &mut u);
        assert_eq!((u.cost_micro_usd, u.unpriced_calls), (0, 1));
        // The provider returned no token counts.
        let mut no_tokens = UsageUnits { llm_requests: 1, ..UsageUnits::default() };
        costs().apply("llm_enricher", "gpt-4o-mini", &mut no_tokens);
        assert_eq!((no_tokens.cost_micro_usd, no_tokens.unpriced_calls), (0, 1));
        // The transcription duration is unknown.
        let mut no_seconds = UsageUnits { external_requests: 1, ..UsageUnits::default() };
        costs().apply("whisper_transcriber", "whisper-1", &mut no_seconds);
        assert_eq!((no_seconds.cost_micro_usd, no_seconds.unpriced_calls), (0, 1));
    }

    #[test]
    fn a_bad_table_is_an_error() {
        assert!(ProviderCosts::from_toml("[llm_enricher.m]\ninput_per_mtok = -1").is_err());
        assert!(ProviderCosts::from_toml("[llm_enricher.m]\ntypo = 1").is_err());
    }

    #[test]
    fn the_bundled_table_parses() {
        let b = ProviderCosts::bundled();
        assert!(b.price("llm_enricher", "gpt-4o-mini").is_some());
        assert!(b.price("whisper_transcriber", "whisper-1").is_some());
    }
}
```

In `lib.rs`: `pub mod cost;`.

Create `config/provider-costs.toml`:

```toml
# What glutony pays a provider per call, in micro-USD (1 USD = 1_000_000).
# Keys: [<plugin name>.<model>] or [<plugin name>.default]. Dimensions:
# input_per_mtok, output_per_mtok (per million tokens), per_audio_second, per_request.
#
# PLACEHOLDERS: these are public list prices at the time of writing, for the
# providers the plugins default to (OpenAI). Set real prices before billing, and ship
# your own file (PROVIDER_COSTS_FILE) if a plugin's *_BASE_URL points elsewhere.
# A model missing here is billed at 0 and flagged (cost_complete: false).

[llm_enricher."gpt-4o-mini"]
input_per_mtok = 150000
output_per_mtok = 600000

[llm_enricher."gpt-4o"]
input_per_mtok = 2500000
output_per_mtok = 10000000

[image_captioner."gpt-4o-mini"]
input_per_mtok = 150000
output_per_mtok = 600000

[image_captioner."gpt-4o"]
input_per_mtok = 2500000
output_per_mtok = 10000000

[whisper_transcriber."whisper-1"]
per_audio_second = 100

# jev_enricher (TypeSafe Jev) has no public price: left out on purpose, so its calls
# are flagged as unpriced until you add [jev_enricher.default] or a model entry.
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-plugin-sdk cost`
Expected: FAIL to compile (`no field cost_micro_usd on UsageUnits`).

- [ ] **Step 4: Implement**

`UsageUnits` in `types.rs`: add

```rust
    /// What glutony paid providers for these units, in micro-USD (spec §5.1).
    #[serde(default)]
    pub cost_micro_usd: u64,
    /// Provider calls the cost table could not price (billed at 0, flagged).
    #[serde(default)]
    pub unpriced_calls: u64,
```

and in `merge`: `self.cost_micro_usd += other.cost_micro_usd; self.unpriced_calls += other.unpriced_calls;`.

`cost.rs` bodies:

```rust
    pub fn from_toml(source: &str) -> Result<Self, String> {
        let entries: HashMap<String, HashMap<String, Price>> =
            toml::from_str(source).map_err(|e| format!("invalid provider cost table: {e}"))?;
        Ok(Self { entries })
    }

    pub fn bundled() -> Self {
        // The bundled file is tested (`the_bundled_table_parses`); an empty table is
        // the safe fallback that flags every call as unpriced.
        Self::from_toml(BUNDLED).unwrap_or_default()
    }

    pub fn global() -> &'static ProviderCosts {
        static GLOBAL: OnceLock<ProviderCosts> = OnceLock::new();
        GLOBAL.get_or_init(|| {
            let Some(path) = std::env::var("PROVIDER_COSTS_FILE")
                .ok()
                .filter(|p| !p.trim().is_empty())
            else {
                return Self::bundled();
            };
            match std::fs::read_to_string(&path)
                .map_err(|e| e.to_string())
                .and_then(|s| Self::from_toml(&s))
            {
                Ok(costs) => costs,
                Err(e) => {
                    tracing::error!(path, error = %e, "cannot load PROVIDER_COSTS_FILE; using the bundled table");
                    Self::bundled()
                }
            }
        })
    }

    pub fn price(&self, plugin: &str, model: &str) -> Option<Price> {
        let models = self.entries.get(plugin)?;
        models.get(model).or_else(|| models.get("default")).copied()
    }

    pub fn apply(&self, plugin: &str, model: &str, units: &mut UsageUnits) {
        let Some(price) = self.price(plugin, model) else {
            warn_unpriced_once(plugin, model);
            units.unpriced_calls += 1;
            return;
        };
        let prices_tokens = price.input_per_mtok > 0 || price.output_per_mtok > 0;
        let no_tokens = units.llm_input_tokens == 0 && units.llm_output_tokens == 0;
        let no_seconds = units.audio_seconds <= 0.0;
        if (prices_tokens && no_tokens) || (price.per_audio_second > 0 && no_seconds) {
            // The table knows the model but the provider did not report the quantity.
            units.unpriced_calls += 1;
            return;
        }
        let tokens = u128::from(units.llm_input_tokens) * u128::from(price.input_per_mtok)
            + u128::from(units.llm_output_tokens) * u128::from(price.output_per_mtok);
        let token_cost = tokens.div_ceil(1_000_000);
        let audio_cost = (units.audio_seconds * price.per_audio_second as f64).ceil() as u128;
        let requests = u128::from(units.llm_requests + units.external_requests).max(1);
        let request_cost = requests * u128::from(price.per_request);
        let total = token_cost + audio_cost + request_cost;
        units.cost_micro_usd += u64::try_from(total).unwrap_or(u64::MAX);
    }
```

and, below the `impl`:

```rust
/// Log once per `(plugin, model)` that it has no price.
fn warn_unpriced_once(plugin: &str, model: &str) {
    static SEEN: OnceLock<Mutex<HashSet<(String, String)>>> = OnceLock::new();
    let seen = SEEN.get_or_init(|| Mutex::new(HashSet::new()));
    if let Ok(mut seen) = seen.lock()
        && seen.insert((plugin.to_string(), model.to_string()))
    {
        tracing::warn!(plugin, model, "no provider price for this model; its calls are billed at 0 and flagged");
    }
}
```

Plugins (each: price the units right before `ctx.record_usage`):

- `llm-enricher`, `LlmClient::enrich`: bind the model once before building the body — `let model = cfg.model.as_deref().unwrap_or(&self.default_model);` — use `"model": model` in the JSON, and replace `ctx.record_usage(completion.usage);` with:

  ```rust
        let mut usage = completion.usage;
        ProviderCosts::global().apply(NAME, model, &mut usage);
        ctx.record_usage(usage);
  ```

- `image-captioner`, `execute`: same `let model = …` binding; after `units.images = 1;` add `ProviderCosts::global().apply(NAME, model, &mut units);`.
- `audio-transcriber`, `execute`: `let mut usage = transcription_usage(duration, &blob.data); ProviderCosts::global().apply(NAME, model, &mut usage); ctx.record_usage(usage);` (`model` is already a local).
- `jev-enricher`, `JevClient::enrich`: bind `let model = cfg.model.as_deref().unwrap_or(&self.default_model);`, use it in the body, and price the `UsageUnits` value before `ctx.record_usage`.

Import `use meili_ingest_plugin_sdk::cost::ProviderCosts;` in each. `NAME` is each plugin's existing name constant (`"llm_enricher"`, `"image_captioner"`, `"whisper_transcriber"`, `"jev_enricher"`); if the constant is named differently in a crate, use that constant.

Plugin tests (add one per plugin, next to its existing usage test, reusing its mocks and helpers):

```rust
    // llm-enricher: a model in the bundled table is priced, the test model is flagged.
    #[tokio::test]
    async fn provider_calls_are_priced_from_the_cost_table() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(UsagePerDocument)
            .expect(2)
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(
                &ctx,
                PluginInput::Documents(vec![Document::with_id("a", "x")]),
                serde_json::json!({"model": "gpt-4o-mini"}),
            )
            .await
            .unwrap();
        let usage = ctx.usage();
        assert!(usage.cost_micro_usd > 0);
        assert_eq!(usage.unpriced_calls, 0);

        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, PluginInput::Documents(vec![Document::with_id("b", "y")]), serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(ctx.usage().unpriced_calls, 1, "test-model is not in the table");
    }
```

For `image-captioner` assert the same with `{"model": "gpt-4o-mini"}` (priced) using its `usage_records_tokens_and_one_image` mock. For `audio-transcriber` extend `duration_in_the_response_is_recorded_as_audio_seconds` with `assert_eq!(usage.cost_micro_usd, 13_725);` (137.25 s × 100, `whisper-1` is the default model; if `usage_for` sets another model, pass `"model": "whisper-1"` in its config). For `jev-enricher` extend `concurrency_preserves_order_and_usage_sums_into_one_total` with `assert_eq!(usage.unpriced_calls, 25);` (Jev is left out of the bundled table).

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-plugin-sdk && cargo test -p meili-ingest-plugin-llm-enricher -p meili-ingest-plugin-image-captioner -p meili-ingest-plugin-audio-transcriber -p meili-ingest-plugin-jev-enricher`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add Cargo.toml Cargo.lock config/provider-costs.toml crates/plugin-sdk crates/plugins
git commit -m "feat(plugins): paid provider calls are priced from a cost table

UsageUnits gains cost_micro_usd and unpriced_calls. A model the table does not
know, or a call without token counts or duration, is billed at 0 and flagged."
```

---

### Task 7: Lab event builder and the vendored contract

**Files:**
- Create: `crates/usage/src/lab.rs`, `contracts/vendor/lab/lab-events.schema.json`
- Modify: `Cargo.toml` (uuid `v5` feature), `crates/usage/Cargo.toml`, `crates/usage/src/lib.rs`
- Test: `crates/usage/src/lab.rs`

**Interfaces:**
- Consumes: `JobUsageInput` (with `tenant_id: String`), `UsageUnits` cost fields (Task 6).
- Produces: `meili_ingest_usage::lab::{LabEvent, GlutonyUsageData, LabUnits, SkipReason, GLUTONY_LAB_NAMESPACE, lab_event_for_job, lab_event_id}`; `lab_event_for_job(&JobUsageInput) -> Result<LabEvent, SkipReason>`; `lab_event_id(job_id: Uuid) -> Uuid`; `LabEvent` serializes to the §5.2 envelope; `SkipReason::{NoTenant, NotAUuid}` with `fn as_str(&self) -> &'static str` (`"no_tenant"`, `"not_a_uuid"`).

- [ ] **Step 1: Dependencies**

Root `Cargo.toml`: `uuid = { version = "1", features = ["v4", "v5", "serde"] }`. `crates/usage/Cargo.toml` `[dev-dependencies]`: `jsonschema = { version = "0.45", default-features = false }`.

- [ ] **Step 2: Write the vendored schema**

Create `contracts/vendor/lab/lab-events.schema.json`. It is Lumen's copy (Lumen `contracts/lab-events.schema.json`, ADR 015) with `"glutony"` added to `product`, a `glutonyUsageData` definition, and two rules; nothing else differs:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://lab.meilisearch.com/contracts/lab-events.schema.json",
  "title": "Meilisearch Lab event",
  "description": "One event a product reports to the Lab at POST /internal/events, delivered in batches as {\"events\": [<event>, ...]}. Owned by the Lab; glutony vendors this copy and tests its serializer against it. This copy starts from LUMEN's (ADR 015) and adds product \"glutony\" and glutonyUsageData, pending adoption by the Lab.",
  "type": "object",
  "required": ["id", "type", "occurred_at", "account_id", "api_key_id", "product", "data"],
  "additionalProperties": false,
  "properties": {
    "id": { "$ref": "#/$defs/uuid" },
    "type": { "enum": ["usage.recorded", "job.completed", "job.failed"] },
    "occurred_at": { "type": "string", "format": "date-time" },
    "account_id": { "$ref": "#/$defs/uuid" },
    "api_key_id": { "type": ["string", "null"] },
    "product": { "enum": ["scrapix", "lumen", "glutony"] },
    "data": { "type": "object" }
  },
  "allOf": [
    {
      "if": { "properties": { "type": { "const": "usage.recorded" }, "product": { "const": "scrapix" } }, "required": ["type", "product"] },
      "then": { "properties": { "data": { "$ref": "#/$defs/usageData" } } }
    },
    {
      "if": { "properties": { "type": { "const": "usage.recorded" }, "product": { "const": "lumen" } }, "required": ["type", "product"] },
      "then": { "properties": { "data": { "$ref": "#/$defs/lumenUsageData" } } }
    },
    {
      "if": { "properties": { "product": { "const": "lumen" } }, "required": ["product"] },
      "then": { "properties": { "type": { "const": "usage.recorded" } } }
    },
    {
      "if": { "properties": { "type": { "const": "usage.recorded" }, "product": { "const": "glutony" } }, "required": ["type", "product"] },
      "then": { "properties": { "data": { "$ref": "#/$defs/glutonyUsageData" } } }
    },
    {
      "if": { "properties": { "product": { "const": "glutony" } }, "required": ["product"] },
      "then": { "properties": { "type": { "const": "usage.recorded" } } }
    }
  ],
  "$defs": {
    "uuid": {
      "type": "string",
      "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    },
    "usageData": {
      "type": "object",
      "required": ["operation", "credits", "units", "description"],
      "additionalProperties": false,
      "properties": {
        "operation": { "enum": ["scrape", "map", "search", "parse", "ocr", "extract", "crawl"] },
        "credits": { "type": "integer", "minimum": 0 },
        "units": { "type": "object" },
        "description": { "type": "string" },
        "job_id": { "type": "string" }
      }
    },
    "lumenUsageData": {
      "type": "object",
      "required": ["cost_micro_usd", "requests", "tokens", "window", "source", "key_id", "group"],
      "additionalProperties": false,
      "properties": {
        "cost_micro_usd": { "type": "integer", "minimum": 1 },
        "requests": { "type": "integer", "minimum": 0 },
        "tokens": { "type": "integer", "minimum": 0 },
        "window": {
          "type": "object",
          "required": ["start", "end"],
          "additionalProperties": false,
          "properties": {
            "start": { "type": "string", "format": "date-time" },
            "end": { "type": "string", "format": "date-time" }
          }
        },
        "source": { "type": "string", "minLength": 1, "maxLength": 64 },
        "key_id": { "type": "string", "minLength": 1 },
        "group": {
          "type": "object",
          "required": ["id", "spent_micro", "budget_max_micro"],
          "additionalProperties": false,
          "properties": {
            "id": { "type": "string", "minLength": 1 },
            "spent_micro": { "type": "integer" },
            "budget_max_micro": { "type": ["integer", "null"] }
          }
        }
      }
    },
    "glutonyUsageData": {
      "type": "object",
      "required": ["job_id", "pipeline_uid", "status", "duration_ms", "cost_micro_usd", "cost_complete", "units"],
      "additionalProperties": false,
      "properties": {
        "job_id": { "$ref": "#/$defs/uuid" },
        "pipeline_uid": { "type": "string", "minLength": 1 },
        "status": { "enum": ["succeeded", "failed", "cancelled"] },
        "duration_ms": { "type": "integer", "minimum": 0 },
        "cost_micro_usd": { "type": "integer", "minimum": 0 },
        "cost_complete": { "type": "boolean" },
        "units": {
          "type": "object",
          "required": ["documents_out", "input_bytes", "pages", "images", "audio_seconds", "llm_input_tokens", "llm_output_tokens", "llm_requests", "external_requests"],
          "additionalProperties": false,
          "properties": {
            "documents_out": { "type": "integer", "minimum": 0 },
            "input_bytes": { "type": "integer", "minimum": 0 },
            "pages": { "type": "integer", "minimum": 0 },
            "images": { "type": "integer", "minimum": 0 },
            "audio_seconds": { "type": "number", "minimum": 0 },
            "llm_input_tokens": { "type": "integer", "minimum": 0 },
            "llm_output_tokens": { "type": "integer", "minimum": 0 },
            "llm_requests": { "type": "integer", "minimum": 0 },
            "external_requests": { "type": "integer", "minimum": 0 }
          }
        }
      }
    }
  }
}
```

- [ ] **Step 3: Write the failing tests**

Create `crates/usage/src/lab.rs` with types, `todo!()` bodies for the two fns, and tests:

```rust
//! Lab billing events (spec §5.2): one `usage.recorded` per finished job, in the
//! Meilisearch Lab envelope. Pure: same input, byte-identical event, same id.

use chrono::SecondsFormat;
use meili_ingest_plugin_sdk::{JobStatus, UsageUnits};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::JobUsageInput;

/// Namespace of glutony's deterministic Lab event ids. Never change it: a new value
/// would give redelivered events new ids and bill them twice.
pub const GLUTONY_LAB_NAMESPACE: Uuid = Uuid::from_u128(0x6c8f_4a1e_2b7d_4c3a_9e51_7f0d_2a6b_8c14);

/// One Lab event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabEvent {
    /// Idempotency key: [`lab_event_id`] of the job.
    pub id: Uuid,
    /// Always `usage.recorded`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The job's finish time, RFC 3339 with milliseconds.
    pub occurred_at: String,
    /// The Lab account: the job's tenant id, a UUID.
    pub account_id: String,
    /// Glutony has no Lab key ids.
    pub api_key_id: Option<String>,
    /// Always `glutony`.
    pub product: String,
    /// The job's usage.
    pub data: GlutonyUsageData,
}

/// `data` of a glutony `usage.recorded` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlutonyUsageData {
    /// Job id.
    pub job_id: Uuid,
    /// Pipeline that ran.
    pub pipeline_uid: String,
    /// `succeeded`, `failed` or `cancelled`.
    pub status: String,
    /// Wall time of the job.
    pub duration_ms: u64,
    /// What glutony paid providers, summed over the steps.
    pub cost_micro_usd: u64,
    /// `false` when a provider call could not be priced.
    pub cost_complete: bool,
    /// Units, summed over the steps.
    pub units: LabUnits,
}

/// Billable units of a job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct LabUnits {
    pub documents_out: u64,
    pub input_bytes: u64,
    pub pages: u64,
    pub images: u64,
    pub audio_seconds: f64,
    pub llm_input_tokens: u64,
    pub llm_output_tokens: u64,
    pub llm_requests: u64,
    pub external_requests: u64,
}

/// Why a job produced no Lab event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Standalone job: no tenant.
    NoTenant,
    /// The tenant is not a Lab account (e.g. a Cloud project id).
    NotAUuid,
}

impl SkipReason {
    /// Label for logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::NoTenant => "no_tenant",
            SkipReason::NotAUuid => "not_a_uuid",
        }
    }
}

/// The deterministic id of a job's usage event.
pub fn lab_event_id(job_id: Uuid) -> Uuid {
    todo!()
}

/// Build the job's Lab event, or say why there is none.
pub fn lab_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    todo!()
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use meili_ingest_plugin_sdk::StepResult;

    use super::*;

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";

    fn step(id: &str, docs: usize, usage: UsageUnits) -> StepResult {
        StepResult {
            step_id: id.into(),
            plugin: "p".into(),
            status: JobStatus::Succeeded,
            document_count: docs,
            branches: 1,
            error: None,
            duration_ms: 10,
            input_bytes: 100,
            usage,
        }
    }

    fn input() -> JobUsageInput {
        JobUsageInput {
            job_id: "11111111-2222-3333-4444-555555555555".parse().unwrap(),
            workflow_id: "ingest-11111111-2222-3333-4444-555555555555".into(),
            pipeline_uid: "builtin.pdf".into(),
            pipeline_builtin: true,
            tenant_id: ACCOUNT.into(),
            region: String::new(),
            index_name: "documents".into(),
            task_queue: "workers-general".into(),
            status: JobStatus::Succeeded,
            started_at: Utc.with_ymd_and_hms(2026, 10, 1, 10, 0, 0).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 10, 1, 10, 0, 9).unwrap(),
            input_bytes: 5_000,
            input_mime: "application/pdf".into(),
            steps: vec![
                step("extract", 3, UsageUnits::pages(9)),
                step(
                    "enrich",
                    12,
                    UsageUnits { cost_micro_usd: 210, unpriced_calls: 1, ..UsageUnits::llm(1_000, 100) },
                ),
            ],
            error: None,
        }
    }

    fn validator() -> jsonschema::Validator {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../contracts/vendor/lab/lab-events.schema.json"
        ))
        .unwrap();
        jsonschema::options().should_validate_formats(true).build(&schema).unwrap()
    }

    #[test]
    fn a_lab_job_becomes_one_valid_event() {
        let event = lab_event_for_job(&input()).unwrap();
        let v = serde_json::to_value(&event).unwrap();
        let errors: Vec<String> = validator().iter_errors(&v).map(|e| e.to_string()).collect();
        assert!(errors.is_empty(), "{errors:?}\n{v:#}");
        assert_eq!(v["type"], "usage.recorded");
        assert_eq!(v["product"], "glutony");
        assert_eq!(v["account_id"], ACCOUNT);
        assert!(v["api_key_id"].is_null());
        assert_eq!(v["occurred_at"], "2026-10-01T10:00:09.000Z");
        assert_eq!(v["data"]["duration_ms"], 9_000);
        assert_eq!(v["data"]["cost_micro_usd"], 210);
        assert_eq!(v["data"]["cost_complete"], false);
        assert_eq!(v["data"]["units"]["pages"], 9);
        assert_eq!(v["data"]["units"]["llm_input_tokens"], 1_000);
        assert_eq!(v["data"]["units"]["documents_out"], 12, "the final step's documents");
        assert_eq!(v["data"]["units"]["input_bytes"], 5_000, "the job's payload size");
    }

    #[test]
    fn the_id_is_deterministic() {
        let a = lab_event_for_job(&input()).unwrap();
        let b = lab_event_for_job(&input()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.id, lab_event_id(input().job_id));
        assert_eq!(a.id.get_version_num(), 5);
        let mut other = input();
        other.job_id = Uuid::new_v4();
        assert_ne!(lab_event_for_job(&other).unwrap().id, a.id);
    }

    #[test]
    fn jobs_outside_the_lab_are_skipped() {
        let mut standalone = input();
        standalone.tenant_id = String::new();
        assert_eq!(lab_event_for_job(&standalone), Err(SkipReason::NoTenant));
        let mut cloud = input();
        cloud.tenant_id = "hackersearch".into();
        assert_eq!(lab_event_for_job(&cloud), Err(SkipReason::NotAUuid));
    }

    #[test]
    fn failed_jobs_are_billed_too_and_complete_cost_says_so() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        failed.steps[1].usage.unpriced_calls = 0;
        let v = serde_json::to_value(lab_event_for_job(&failed).unwrap()).unwrap();
        assert!(validator().is_valid(&v));
        assert_eq!(v["data"]["status"], "failed");
        assert_eq!(v["data"]["cost_complete"], true);
    }

    #[test]
    fn the_schema_is_really_enforced() {
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["surprise"] = 1.into();
        assert!(!validator().is_valid(&v));
    }
}
```

In `crates/usage/src/lib.rs`: `pub mod lab;`.

- [ ] **Step 4: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-usage lab`
Expected: FAIL (panics at `todo!()`).

- [ ] **Step 5: Implement**

```rust
pub fn lab_event_id(job_id: Uuid) -> Uuid {
    Uuid::new_v5(&GLUTONY_LAB_NAMESPACE, format!("job:{job_id}:usage").as_bytes())
}

pub fn lab_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    if input.tenant_id.is_empty() {
        return Err(SkipReason::NoTenant);
    }
    let account = Uuid::parse_str(&input.tenant_id).map_err(|_| SkipReason::NotAUuid)?;
    let mut totals = UsageUnits::none();
    for step in &input.steps {
        totals.merge(step.usage);
    }
    let duration_ms = (input.finished_at - input.started_at)
        .num_milliseconds()
        .max(0) as u64;
    Ok(LabEvent {
        id: lab_event_id(input.job_id),
        kind: "usage.recorded".into(),
        occurred_at: input.finished_at.to_rfc3339_opts(SecondsFormat::Millis, true),
        account_id: account.hyphenated().to_string(),
        api_key_id: None,
        product: "glutony".into(),
        data: GlutonyUsageData {
            job_id: input.job_id,
            pipeline_uid: input.pipeline_uid.clone(),
            status: input.status.as_str().to_string(),
            duration_ms,
            cost_micro_usd: totals.cost_micro_usd,
            cost_complete: totals.unpriced_calls == 0,
            units: LabUnits {
                documents_out: input.steps.last().map(|s| s.document_count as u64).unwrap_or(0),
                input_bytes: input.input_bytes,
                pages: totals.pages,
                images: totals.images,
                audio_seconds: totals.audio_seconds,
                llm_input_tokens: totals.llm_input_tokens,
                llm_output_tokens: totals.llm_output_tokens,
                llm_requests: totals.llm_requests,
                external_requests: totals.external_requests,
            },
        },
    })
}
```

`account_id` is the parsed UUID re-rendered lower-case hyphenated, so an upper-case tenant id still matches the Lab's UUID. If `jsonschema::options().should_validate_formats(true)` does not exist under that name in 0.45, use the 0.45 equivalent that turns on `format` validation (Lumen's `crates/auth/src/billing.rs` does the same); the tests must validate `date-time`.

- [ ] **Step 6: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-usage`
Expected: PASS (all existing usage tests too).

- [ ] **Step 7: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add Cargo.toml Cargo.lock crates/usage contracts/vendor/lab/lab-events.schema.json
git commit -m "feat(usage): a finished Lab job becomes one usage.recorded event

Deterministic UUIDv5 id, units and provider cost summed over the steps, validated
against the vendored Lab contract (Lumen's copy plus product glutony)."
```

---

### Task 8: The outbox in the control plane

**Files:**
- Create: `migrations/0004_lab_events.sql`, `crates/control-plane/src/lab_events.rs`, `crates/control-plane/tests/lab_events.rs`
- Modify: `crates/control-plane/src/lib.rs` (module, route, `AppState.lab_notify`)
- Test: `crates/control-plane/tests/lab_events.rs`

**Interfaces:**
- Produces: table `lab_events`; `control_plane::lab_events::{LabEventRepo, PendingEvent, LabEventStats, ingest_lab_events}`; `LabEventRepo::new(PgPool)`; `insert_many(&self, events: &[serde_json::Value]) -> Result<u64, CpError>` (each event must have a UUID `id`; duplicates ignored; returns rows inserted); `claim_due(&self, limit: i64, lease: Duration) -> Result<Vec<PendingEvent>, CpError>`; `mark_delivered(&self, ids: &[Uuid]) -> Result<u64, CpError>`; `reschedule(&self, ids: &[Uuid]) -> Result<u64, CpError>`; `purge_delivered(&self, older_than: Duration) -> Result<u64, CpError>`; `stats(&self) -> Result<LabEventStats, CpError>`; `PendingEvent { id: Uuid, body: serde_json::Value, attempts: i32 }`; `LabEventStats { pending: i64, oldest_pending_seconds: i64 }`; `AppState.lab_notify: Arc<tokio::sync::Notify>`; route `POST /internal/lab-events` → `202 {"inserted": n}`.

- [ ] **Step 1: Write the migration**

```sql
-- Outbox of Meilisearch Lab billing events (spec §5.3). The worker inserts one row per
-- finished Lab job; the control plane's sender delivers them. Rows are never deleted
-- before delivery; delivered rows are purged after 7 days.
CREATE TABLE IF NOT EXISTS lab_events (
    id            UUID PRIMARY KEY,               -- the event id: redelivery-safe
    body          JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_attempt  TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS lab_events_due ON lab_events (next_attempt)
    WHERE delivered_at IS NULL;
```

- [ ] **Step 2: Write the failing tests**

Create `crates/control-plane/tests/lab_events.rs` (the fresh-schema harness is copied from `tests/db.rs`, so these tests never touch a developer's shared schema):

```rust
//! Lab events outbox. Needs DATABASE_URL (skips otherwise), e.g.
//! `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane --test lab_events`.

use std::str::FromStr;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use meili_ingest_control_plane::lab_events::LabEventRepo;
use meili_ingest_control_plane::{AppState, app, db};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use tower::ServiceExt;
use uuid::Uuid;

struct TestDb {
    pool: PgPool,
    schema: String,
    admin: PgPool,
}

impl TestDb {
    async fn drop_schema(self) {
        self.pool.close().await;
        let _ = sqlx::query(AssertSqlSafe(format!("DROP SCHEMA IF EXISTS {} CASCADE", self.schema)))
            .execute(&self.admin)
            .await;
        self.admin.close().await;
    }
}

async fn setup() -> Option<TestDb> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL not set");
        return None;
    };
    let schema = format!("cp_lab_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new().max_connections(1).connect(&url).await.unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new().max_connections(4).connect_with(opts).await.unwrap();
    db::migrate(&pool).await.unwrap();
    Some(TestDb { pool, schema, admin })
}

fn event(id: Uuid) -> serde_json::Value {
    serde_json::json!({"id": id, "type": "usage.recorded", "product": "glutony"})
}

#[tokio::test]
async fn inserting_twice_keeps_one_row() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let id = Uuid::new_v4();
    assert_eq!(repo.insert_many(&[event(id)]).await.unwrap(), 1);
    assert_eq!(repo.insert_many(&[event(id), event(id)]).await.unwrap(), 0);
    assert_eq!(repo.stats().await.unwrap().pending, 1);
    t.drop_schema().await;
}

#[tokio::test]
async fn an_event_without_a_uuid_id_is_rejected() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    assert!(repo.insert_many(&[serde_json::json!({"id": "nope"})]).await.is_err());
    assert!(repo.insert_many(&[serde_json::json!({})]).await.is_err());
    assert_eq!(repo.stats().await.unwrap().pending, 0);
    t.drop_schema().await;
}

#[tokio::test]
async fn a_claim_leases_rows_so_two_senders_never_share_one() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
    let events: Vec<_> = ids.iter().copied().map(event).collect();
    repo.insert_many(&events).await.unwrap();

    let lease = Duration::from_secs(30);
    let (a, b) = tokio::join!(repo.claim_due(6, lease), repo.claim_due(6, lease));
    let (a, b) = (a.unwrap(), b.unwrap());
    let mut seen: Vec<Uuid> = a.iter().chain(b.iter()).map(|e| e.id).collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), a.len() + b.len(), "a row was claimed twice");
    assert_eq!(a.len() + b.len(), 10);
    assert!(repo.claim_due(10, lease).await.unwrap().is_empty(), "leased rows are not due");
    t.drop_schema().await;
}

#[tokio::test]
async fn delivered_rows_are_done_and_failed_rows_back_off() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let (ok, ko) = (Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(ok), event(ko)]).await.unwrap();
    let claimed = repo.claim_due(10, Duration::ZERO).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].body["product"], "glutony");

    assert_eq!(repo.mark_delivered(&[ok]).await.unwrap(), 1);
    assert_eq!(repo.reschedule(&[ko]).await.unwrap(), 1);
    let (attempts, wait): (i32, f64) = sqlx::query_as(
        "SELECT attempts, EXTRACT(EPOCH FROM next_attempt - now())::float8 FROM lab_events WHERE id = $1",
    )
    .bind(ko)
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert_eq!(attempts, 1);
    assert!((1.5..=2.5).contains(&wait), "first backoff is 2 s ± 20 %, got {wait}");
    assert!(repo.claim_due(10, Duration::ZERO).await.unwrap().is_empty());

    let stats = repo.stats().await.unwrap();
    assert_eq!(stats.pending, 1, "delivered rows are not pending; failed ones are");
    t.drop_schema().await;
}

#[tokio::test]
async fn backoff_is_capped_at_five_minutes() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let id = Uuid::new_v4();
    repo.insert_many(&[event(id)]).await.unwrap();
    sqlx::query("UPDATE lab_events SET attempts = 30 WHERE id = $1").bind(id).execute(&t.pool).await.unwrap();
    repo.reschedule(&[id]).await.unwrap();
    let wait: f64 = sqlx::query_scalar("SELECT EXTRACT(EPOCH FROM next_attempt - now())::float8 FROM lab_events WHERE id = $1")
        .bind(id)
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert!((240.0..=360.0).contains(&wait), "{wait}");
    t.drop_schema().await;
}

#[tokio::test]
async fn purge_only_touches_old_delivered_rows() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let (old, recent, pending) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(old), event(recent), event(pending)]).await.unwrap();
    repo.mark_delivered(&[old, recent]).await.unwrap();
    sqlx::query("UPDATE lab_events SET delivered_at = now() - interval '8 days', created_at = now() - interval '9 days' WHERE id = $1")
        .bind(old)
        .execute(&t.pool)
        .await
        .unwrap();
    // A very old undelivered row must survive any purge.
    sqlx::query("UPDATE lab_events SET created_at = now() - interval '30 days' WHERE id = $1")
        .bind(pending)
        .execute(&t.pool)
        .await
        .unwrap();
    assert_eq!(repo.purge_delivered(Duration::from_secs(7 * 86_400)).await.unwrap(), 1);
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM lab_events ORDER BY created_at")
        .fetch_all(&t.pool)
        .await
        .unwrap();
    assert_eq!(left.len(), 2);
    assert!(left.contains(&pending) && left.contains(&recent));
    let stats = repo.stats().await.unwrap();
    assert!(stats.oldest_pending_seconds >= 30 * 86_400 - 5);
    t.drop_schema().await;
}

#[tokio::test]
async fn the_internal_route_inserts_and_is_idempotent() {
    let Some(t) = setup().await else { return };
    let app = app(AppState::new(t.pool.clone()));
    let id = Uuid::new_v4();
    let body = serde_json::json!({"events": [event(id)]});
    for expected in [1, 0] {
        let req = Request::post("/internal/lab-events")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["inserted"], expected);
    }
    let bad = Request::post("/internal/lab-events")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"events":[{"id":"x"}]}"#))
        .unwrap();
    assert_eq!(app.oneshot(bad).await.unwrap().status(), StatusCode::UNPROCESSABLE_ENTITY);
    t.drop_schema().await;
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane --test lab_events`
Expected: FAIL to compile (`lab_events` module missing).

- [ ] **Step 4: Implement**

Create `crates/control-plane/src/lab_events.rs`:

```rust
//! Outbox of Meilisearch Lab billing events (spec §5.3, §5.4).
//!
//! The worker posts each finished Lab job's event to `POST /internal/lab-events`;
//! the insert ignores duplicates, so a Temporal retry cannot bill twice. The sender
//! (`crate::lab_sender`) leases due rows with `claim_due`, so several control-plane
//! replicas never deliver the same row, and no transaction is held across HTTP.

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{AppState, CpError, JsonBody};

/// A leased row, ready to send.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct PendingEvent {
    /// Event id.
    pub id: Uuid,
    /// The event as the worker built it.
    pub body: serde_json::Value,
    /// Failed deliveries so far.
    pub attempts: i32,
}

/// Outbox health, for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LabEventStats {
    /// Rows not delivered yet.
    pub pending: i64,
    /// Age of the oldest undelivered row, 0 when none.
    pub oldest_pending_seconds: i64,
}

/// Queries over `lab_events`.
#[derive(Debug, Clone)]
pub struct LabEventRepo {
    pool: PgPool,
}

impl LabEventRepo {
    /// Wrap a pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert events, ignoring ids already present. Every event needs a UUID `id`;
    /// otherwise nothing is inserted and the call fails.
    pub async fn insert_many(&self, events: &[serde_json::Value]) -> Result<u64, CpError> {
        let mut ids = Vec::with_capacity(events.len());
        for e in events {
            let id = e
                .get("id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| CpError::Validation("every lab event needs a UUID \"id\"".into()))?;
            ids.push(id);
        }
        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;
        for (id, body) in ids.iter().zip(events) {
            inserted += sqlx::query(
                "INSERT INTO lab_events (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            )
            .bind(id)
            .bind(sqlx::types::Json(body))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }
        tx.commit().await?;
        Ok(inserted)
    }

    /// Lease up to `limit` due rows for `lease`: they stay undelivered but are not due
    /// again until the lease ends, so a sender that dies mid-batch loses nothing.
    pub async fn claim_due(&self, limit: i64, lease: Duration) -> Result<Vec<PendingEvent>, CpError> {
        Ok(sqlx::query_as(
            "UPDATE lab_events SET next_attempt = now() + make_interval(secs => $2) \
             WHERE id IN ( \
                 SELECT id FROM lab_events \
                 WHERE delivered_at IS NULL AND next_attempt <= now() \
                 ORDER BY created_at \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED) \
             RETURNING id, body, attempts",
        )
        .bind(limit)
        .bind(lease.as_secs_f64())
        .fetch_all(&self.pool)
        .await?)
    }

    /// Mark rows delivered.
    pub async fn mark_delivered(&self, ids: &[Uuid]) -> Result<u64, CpError> {
        Ok(sqlx::query(
            "UPDATE lab_events SET delivered_at = now() WHERE id = ANY($1) AND delivered_at IS NULL",
        )
        .bind(ids)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Count a failed delivery and back off: `min(2^attempts s, 300 s)` ± 20 %.
    pub async fn reschedule(&self, ids: &[Uuid]) -> Result<u64, CpError> {
        Ok(sqlx::query(
            "UPDATE lab_events SET attempts = attempts + 1, \
                 next_attempt = now() + make_interval(secs => \
                     LEAST(power(2, attempts + 1), 300) * (0.8 + random() * 0.4)) \
             WHERE id = ANY($1) AND delivered_at IS NULL",
        )
        .bind(ids)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Delete delivered rows older than `older_than`. Never touches undelivered rows.
    pub async fn purge_delivered(&self, older_than: Duration) -> Result<u64, CpError> {
        Ok(sqlx::query(
            "DELETE FROM lab_events \
             WHERE delivered_at IS NOT NULL AND delivered_at < now() - make_interval(secs => $1)",
        )
        .bind(older_than.as_secs_f64())
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Pending count and oldest pending age.
    pub async fn stats(&self) -> Result<LabEventStats, CpError> {
        let (pending, oldest): (i64, Option<f64>) = sqlx::query_as(
            "SELECT count(*), EXTRACT(EPOCH FROM now() - min(created_at))::float8 \
             FROM lab_events WHERE delivered_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(LabEventStats {
            pending,
            oldest_pending_seconds: oldest.map(|s| s as i64).unwrap_or(0),
        })
    }
}

/// Body of `POST /internal/lab-events`.
#[derive(Debug, Deserialize)]
pub struct LabEventBatch {
    /// Events as the worker built them.
    pub events: Vec<serde_json::Value>,
}

/// `POST /internal/lab-events` → `202 {"inserted": n}`. Accepts whether or not
/// `LAB_URL` is configured: rows wait until it is (spec §5.3).
pub async fn ingest_lab_events(
    State(state): State<AppState>,
    JsonBody(batch): JsonBody<LabEventBatch>,
) -> Result<(StatusCode, Json<serde_json::Value>), CpError> {
    let inserted = LabEventRepo::new(state.pool.clone())
        .insert_many(&batch.events)
        .await?;
    if inserted > 0 {
        state.lab_notify.notify_one();
    }
    Ok((StatusCode::ACCEPTED, Json(serde_json::json!({ "inserted": inserted }))))
}
```

`crates/control-plane/src/lib.rs`: add `pub mod lab_events;`; add to `AppState`:

```rust
    /// Wakes the Lab events sender right after an insert.
    pub lab_notify: std::sync::Arc<tokio::sync::Notify>,
```

initialized in `AppState::new` with `std::sync::Arc::new(tokio::sync::Notify::new())`; and the route `.route("/internal/lab-events", post(lab_events::ingest_lab_events))` after `/internal/source-runs`.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane`
Expected: PASS.

- [ ] **Step 6: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add migrations/0004_lab_events.sql crates/control-plane
git commit -m "feat(control-plane): lab_events outbox and POST /internal/lab-events"
```

---

### Task 9: The sender, `/metrics`, and `LAB_URL`

**Files:**
- Create: `crates/control-plane/src/lab_sender.rs`, `crates/control-plane/src/metrics.rs`, `crates/control-plane/tests/lab_sender.rs`
- Modify: `Cargo.toml` (workspace deps `hmac`, `sha2`, `hex`, `prometheus`), `crates/control-plane/Cargo.toml`, `crates/control-plane/src/lib.rs` (modules, `AppState.metrics`, `/metrics` route), `crates/control-plane/src/main.rs`
- Test: `crates/control-plane/src/lab_sender.rs` (unit), `crates/control-plane/tests/lab_sender.rs` (DB + wiremock)

**Interfaces:**
- Consumes: `LabEventRepo`, `AppState.lab_notify` (Task 8).
- Produces: `control_plane::lab_sender::{LabConfig, LabSender, Delivery, sign}`; `LabConfig::from_values(url: Option<String>, secret: Option<String>) -> anyhow::Result<Option<LabConfig>>`, `LabConfig::from_env() -> anyhow::Result<Option<LabConfig>>`, `LabConfig::events_url(&self) -> String`; `sign(secret: &[u8], body: &[u8]) -> String` (`"sha256=<hex>"`); `LabSender::new(repo: LabEventRepo, config: LabConfig, metrics: LabMetrics, notify: Arc<Notify>) -> anyhow::Result<LabSender>`; `LabSender::deliver_once(&self) -> Delivery`; `LabSender::run(self, cancel: CancellationToken)`; `Delivery::{Idle, Sent { delivered: usize, pending: usize }, Failed { reason: &'static str, pending: usize }}`; `control_plane::metrics::{LabMetrics, render}`; `AppState.metrics: LabMetrics`; `GET /metrics`.

- [ ] **Step 1: Dependencies**

Root `Cargo.toml`: `hmac = "0.12"`, `sha2 = "0.10"`, `hex = "0.4"`, `prometheus = { version = "0.14", default-features = false }`. `crates/control-plane/Cargo.toml` `[dependencies]`: `reqwest.workspace = true`, `url.workspace = true`, `tokio-util.workspace = true`, `hmac.workspace = true`, `sha2.workspace = true`, `hex.workspace = true`, `prometheus.workspace = true`, `rand.workspace = true`; `[dev-dependencies]`: `wiremock.workspace = true`.

- [ ] **Step 2: Write the failing unit tests**

Create `crates/control-plane/src/metrics.rs`:

```rust
//! Prometheus metrics of the control plane (spec §5.5), served at `GET /metrics`.

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

/// Lab events outbox metrics.
#[derive(Clone)]
pub struct LabMetrics {
    registry: Registry,
    /// `glutony_lab_events_pending`.
    pub pending: IntGauge,
    /// `glutony_lab_events_oldest_pending_seconds`.
    pub oldest_pending_seconds: IntGauge,
    /// `glutony_lab_events_delivered_total`.
    pub delivered_total: IntCounter,
    /// `glutony_lab_events_failed_total{reason}`.
    pub failed_total: IntCounterVec,
}

impl Default for LabMetrics {
    fn default() -> Self {
        let registry = Registry::new();
        let pending = IntGauge::new("glutony_lab_events_pending", "Lab events not delivered yet")
            .expect("valid metric");
        let oldest_pending_seconds = IntGauge::new(
            "glutony_lab_events_oldest_pending_seconds",
            "Age of the oldest undelivered Lab event",
        )
        .expect("valid metric");
        let delivered_total =
            IntCounter::new("glutony_lab_events_delivered_total", "Lab events delivered")
                .expect("valid metric");
        let failed_total = IntCounterVec::new(
            Opts::new("glutony_lab_events_failed_total", "Lab event deliveries that failed"),
            &["reason"],
        )
        .expect("valid metric");
        for m in [
            Box::new(pending.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(oldest_pending_seconds.clone()),
            Box::new(delivered_total.clone()),
            Box::new(failed_total.clone()),
        ] {
            registry.register(m).expect("unique metric");
        }
        Self { registry, pending, oldest_pending_seconds, delivered_total, failed_total }
    }
}

impl std::fmt::Debug for LabMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabMetrics").finish_non_exhaustive()
    }
}

/// Render every metric in the Prometheus text format.
pub fn render(metrics: &LabMetrics) -> String {
    let mut buf = Vec::new();
    let _ = TextEncoder::new().encode(&metrics.registry.gather(), &mut buf);
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_lab_metrics() {
        let m = LabMetrics::default();
        m.failed_total.with_label_values(&["auth"]).inc();
        let text = render(&m);
        for name in [
            "glutony_lab_events_pending",
            "glutony_lab_events_oldest_pending_seconds",
            "glutony_lab_events_delivered_total",
            "glutony_lab_events_failed_total{reason=\"auth\"} 1",
        ] {
            assert!(text.contains(name), "{name} missing from\n{text}");
        }
    }
}
```

(The `expect`s run once at startup on constant names; they cannot fail at runtime.)

Create `crates/control-plane/src/lab_sender.rs` with the API and unit tests (bodies `todo!()` until Step 4):

```rust
//! Delivers the Lab events outbox to `POST {LAB_URL}/internal/events` (spec §5.4).
//!
//! Every 2 s, or right after an insert, lease up to 500 due rows, send them in one
//! signed batch, mark the ids the Lab lists in `accepted` as delivered and back the
//! rest off. Rows are never dropped. A Lab `401` is logged and retried: the Lab can
//! never stop glutony from starting or serving.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use sha2::Sha256;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::lab_events::LabEventRepo;
use crate::metrics::LabMetrics;

/// Rows per batch.
pub const BATCH_SIZE: i64 = 500;
/// Idle tick.
pub const TICK: Duration = Duration::from_secs(2);
/// How long a claimed batch is leased before it is due again.
pub const LEASE: Duration = Duration::from_secs(30);
/// Delivered rows are kept this long.
pub const RETENTION: Duration = Duration::from_secs(7 * 86_400);

/// `LAB_URL` + `LAB_EVENTS_SECRET`.
#[derive(Clone)]
pub struct LabConfig {
    /// Lab base URL, without trailing slash.
    pub url: String,
    secret: String,
}

impl std::fmt::Debug for LabConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabConfig")
            .field("url", &self.url)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl LabConfig {
    /// Both or neither; `https` unless the host is loopback or private.
    pub fn from_values(url: Option<String>, secret: Option<String>) -> anyhow::Result<Option<Self>> {
        todo!()
    }

    /// Read `LAB_URL` and `LAB_EVENTS_SECRET`.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(get("LAB_URL"), get("LAB_EVENTS_SECRET"))
    }

    /// `{url}/internal/events`.
    pub fn events_url(&self) -> String {
        format!("{}/internal/events", self.url)
    }
}

/// `sha256=<hex HMAC-SHA256(secret, body)>`.
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    todo!()
}

/// Outcome of one delivery attempt, for tests and logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// Nothing was due.
    Idle,
    /// The Lab answered `200`; `pending` ids were not in `accepted`.
    Sent {
        /// Rows marked delivered.
        delivered: usize,
        /// Rows backed off because the Lab did not accept them.
        pending: usize,
    },
    /// The batch failed as a whole and was backed off.
    Failed {
        /// `connect`, `timeout`, `auth`, `status`, `malformed` or `db`.
        reason: &'static str,
        /// Rows backed off.
        pending: usize,
    },
}

/// The sender task.
pub struct LabSender {
    repo: LabEventRepo,
    config: LabConfig,
    metrics: LabMetrics,
    notify: Arc<Notify>,
    http: reqwest::Client,
}

impl LabSender {
    /// Build the sender and its HTTP client (no redirects, 2 s connect, 10 s total).
    pub fn new(
        repo: LabEventRepo,
        config: LabConfig,
        metrics: LabMetrics,
        notify: Arc<Notify>,
    ) -> anyhow::Result<Self> {
        todo!()
    }

    /// Lease one batch and send it.
    pub async fn deliver_once(&self) -> Delivery {
        todo!()
    }

    /// Loop until `cancel`: deliver while batches come back full, then wait for the
    /// tick or an insert; refresh the gauges; purge hourly.
    pub async fn run(self, cancel: CancellationToken) {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn both_or_neither() {
        assert!(LabConfig::from_values(None, None).unwrap().is_none());
        assert!(LabConfig::from_values(Some("https://lab.example".into()), None).is_err());
        assert!(LabConfig::from_values(None, Some("s".into())).is_err());
        let c = LabConfig::from_values(Some("https://lab.example/".into()), Some("s".into()))
            .unwrap()
            .unwrap();
        assert_eq!(c.events_url(), "https://lab.example/internal/events");
        assert!(!format!("{c:?}").contains("\"s\""));
    }

    #[test]
    fn http_only_on_loopback_or_private_hosts() {
        for ok in [
            "http://127.0.0.1:8091",
            "http://localhost:8091",
            "http://10.0.0.5",
            "http://192.168.1.2:3000",
            "http://172.20.0.3",
            "http://[::1]:8091",
            "https://lab.meilisearch.com",
        ] {
            assert!(
                LabConfig::from_values(Some(ok.into()), Some("s".into())).is_ok(),
                "{ok}"
            );
        }
        for bad in ["http://lab.meilisearch.com", "http://8.8.8.8", "ftp://10.0.0.1", "not a url"] {
            assert!(
                LabConfig::from_values(Some(bad.into()), Some("s".into())).is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn signature_matches_a_known_vector() {
        // echo -n '{"events":[]}' | openssl dgst -sha256 -hmac secret
        assert_eq!(
            sign(b"secret", br#"{"events":[]}"#),
            format!(
                "sha256={}",
                hex::encode({
                    let mut m = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
                    m.update(br#"{"events":[]}"#);
                    m.finalize().into_bytes()
                })
            )
        );
        assert!(sign(b"k", b"body").starts_with("sha256="));
        assert_eq!(sign(b"k", b"body").len(), "sha256=".len() + 64);
    }
}
```

- [ ] **Step 3: Write the failing integration tests**

Create `crates/control-plane/tests/lab_sender.rs` (copy the `TestDb`/`setup` harness verbatim from `tests/lab_events.rs`, with schema prefix `cp_send_`), then:

```rust
use meili_ingest_control_plane::lab_events::LabEventRepo;
use meili_ingest_control_plane::lab_sender::{Delivery, LabConfig, LabSender, sign};
use meili_ingest_control_plane::metrics::{LabMetrics, render};
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request as WmRequest, ResponseTemplate};

const SECRET: &str = "lab-events-secret";

async fn sender(t: &TestDb, lab: &MockServer) -> (LabSender, LabEventRepo, LabMetrics) {
    let repo = LabEventRepo::new(t.pool.clone());
    let config = LabConfig::from_values(Some(lab.uri()), Some(SECRET.into())).unwrap().unwrap();
    let metrics = LabMetrics::default();
    let s = LabSender::new(repo.clone(), config, metrics.clone(), Default::default()).unwrap();
    (s, repo, metrics)
}

fn event(id: Uuid) -> serde_json::Value {
    serde_json::json!({"id": id, "type": "usage.recorded", "product": "glutony"})
}

/// Answers 200 accepting every id it received.
fn accept_all(req: &WmRequest) -> ResponseTemplate {
    let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    let ids: Vec<_> = v["events"].as_array().unwrap().iter().map(|e| e["id"].clone()).collect();
    ResponseTemplate::new(200).set_body_json(serde_json::json!({"accepted": ids}))
}

#[tokio::test]
async fn a_batch_is_signed_and_delivered() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/events"))
        .and(header("content-type", "application/json"))
        .respond_with(accept_all)
        .expect(1)
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    let id = Uuid::new_v4();
    repo.insert_many(&[event(id)]).await.unwrap();

    assert_eq!(s.deliver_once().await, Delivery::Sent { delivered: 1, pending: 0 });
    let req = &lab.received_requests().await.unwrap()[0];
    let sig = req.headers.get("x-lab-signature").unwrap().to_str().unwrap();
    assert_eq!(sig, sign(SECRET.as_bytes(), &req.body));
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["events"][0]["id"], id.to_string());
    assert_eq!(repo.stats().await.unwrap().pending, 0);
    assert!(render(&metrics).contains("glutony_lab_events_delivered_total 1"));
    assert_eq!(s.deliver_once().await, Delivery::Idle);
    t.drop_schema().await;
}

#[tokio::test]
async fn partial_accept_leaves_the_rest_pending() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let (keep, skip) = (Uuid::new_v4(), Uuid::new_v4());
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"accepted": [keep]})))
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(keep), event(skip)]).await.unwrap();
    assert_eq!(s.deliver_once().await, Delivery::Sent { delivered: 1, pending: 1 });
    assert_eq!(repo.stats().await.unwrap().pending, 1);
    assert!(render(&metrics).contains("glutony_lab_events_failed_total{reason=\"not_accepted\"} 1"));
    t.drop_schema().await;
}

#[tokio::test]
async fn failures_back_off_and_never_drop() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();

    for (status, reason) in [(503, "status"), (401, "auth")] {
        lab.reset().await;
        Mock::given(method("POST")).respond_with(ResponseTemplate::new(status)).mount(&lab).await;
        // Make the row due again despite the backoff.
        sqlx::query("UPDATE lab_events SET next_attempt = now()").execute(&t.pool).await.unwrap();
        assert_eq!(s.deliver_once().await, Delivery::Failed { reason, pending: 1 });
    }
    lab.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&lab)
        .await;
    sqlx::query("UPDATE lab_events SET next_attempt = now()").execute(&t.pool).await.unwrap();
    assert_eq!(s.deliver_once().await, Delivery::Failed { reason: "malformed", pending: 1 });

    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM lab_events").fetch_one(&t.pool).await.unwrap();
    assert_eq!(attempts, 3);
    assert_eq!(repo.stats().await.unwrap().pending, 1, "nothing is ever dropped");
    let text = render(&metrics);
    assert!(text.contains("reason=\"auth\"} 1") && text.contains("reason=\"status\"} 1"));
    t.drop_schema().await;
}

#[tokio::test]
async fn redirect_is_not_followed() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(307).insert_header("location", format!("{}/internal/events", elsewhere.uri())))
        .mount(&lab)
        .await;
    Mock::given(method("POST")).respond_with(accept_all).expect(0).mount(&elsewhere).await;
    let (s, repo, _) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();
    assert_eq!(s.deliver_once().await, Delivery::Failed { reason: "status", pending: 1 });
    t.drop_schema().await;
}

#[tokio::test]
async fn an_unreachable_lab_is_a_connect_failure() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let config = LabConfig::from_values(Some("http://127.0.0.1:1".into()), Some(SECRET.into())).unwrap().unwrap();
    let s = LabSender::new(repo.clone(), config, LabMetrics::default(), Default::default()).unwrap();
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();
    assert_eq!(s.deliver_once().await, Delivery::Failed { reason: "connect", pending: 1 });
    t.drop_schema().await;
}

#[tokio::test]
async fn batches_hold_at_most_500_events() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST")).respond_with(accept_all).mount(&lab).await;
    let (s, repo, _) = sender(&t, &lab).await;
    let events: Vec<_> = (0..501).map(|_| event(Uuid::new_v4())).collect();
    repo.insert_many(&events).await.unwrap();
    assert_eq!(s.deliver_once().await, Delivery::Sent { delivered: 500, pending: 0 });
    assert_eq!(s.deliver_once().await, Delivery::Sent { delivered: 1, pending: 0 });
    t.drop_schema().await;
}
```

(`Default::default()` for `Arc<Notify>` works because `Notify: Default`.)

- [ ] **Step 4: Run the tests to verify they fail**

Run: `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane lab_sender metrics`
Expected: FAIL (`todo!()` panics / modules missing from `lib.rs`).

- [ ] **Step 5: Implement**

`lib.rs`: `pub mod lab_sender; pub mod metrics;`; `AppState` gains `pub metrics: metrics::LabMetrics,` (initialized `metrics::LabMetrics::default()` in `new`); route `.route("/metrics", get(serve_metrics))` with:

```rust
/// `GET /metrics` in the Prometheus text format.
pub async fn serve_metrics(State(state): State<AppState>) -> Response {
    (
        [(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")],
        metrics::render(&state.metrics),
    )
        .into_response()
}
```

`lab_sender.rs` bodies:

```rust
    pub fn from_values(url: Option<String>, secret: Option<String>) -> anyhow::Result<Option<Self>> {
        let (url, secret) = match (url, secret) {
            (None, None) => return Ok(None),
            (Some(u), Some(s)) => (u, s),
            (Some(_), None) => anyhow::bail!("LAB_URL is set but LAB_EVENTS_SECRET is not"),
            (None, Some(_)) => anyhow::bail!("LAB_EVENTS_SECRET is set but LAB_URL is not"),
        };
        let parsed = url::Url::parse(&url).map_err(|e| anyhow::anyhow!("LAB_URL is not a URL: {e}"))?;
        let private = match parsed.host() {
            Some(url::Host::Domain(d)) => d == "localhost",
            Some(url::Host::Ipv4(ip)) => ip.is_loopback() || ip.is_private(),
            Some(url::Host::Ipv6(ip)) => ip.is_loopback() || ip.is_unique_local(),
            None => false,
        };
        match parsed.scheme() {
            "https" => {}
            "http" if private => {}
            "http" => anyhow::bail!("LAB_URL must be https unless its host is loopback or private"),
            other => anyhow::bail!("LAB_URL has unsupported scheme {other:?}"),
        }
        Ok(Some(Self { url: url.trim_end_matches('/').to_string(), secret }))
    }
```

(`Ipv6Addr::is_unique_local` is stable since Rust 1.84.)

```rust
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    // HMAC accepts keys of any length; new_from_slice cannot fail for Hmac<Sha256>.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC takes any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}
```

```rust
    pub fn new(
        repo: LabEventRepo,
        config: LabConfig,
        metrics: LabMetrics,
        notify: Arc<Notify>,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self { repo, config, metrics, notify, http })
    }

    pub async fn deliver_once(&self) -> Delivery {
        let batch = match self.repo.claim_due(BATCH_SIZE, LEASE).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "cannot read the lab_events outbox");
                return Delivery::Failed { reason: "db", pending: 0 };
            }
        };
        if batch.is_empty() {
            return Delivery::Idle;
        }
        let ids: Vec<Uuid> = batch.iter().map(|e| e.id).collect();
        let bodies: Vec<&serde_json::Value> = batch.iter().map(|e| &e.body).collect();
        let body = match serde_json::to_vec(&serde_json::json!({ "events": bodies })) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "cannot serialize a lab events batch");
                return self.fail(&ids, "malformed").await;
            }
        };
        let resp = self
            .http
            .post(self.config.events_url())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header("x-lab-signature", sign(self.config.secret.as_bytes(), &body))
            .body(body)
            .send()
            .await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                let reason = if e.is_timeout() { "timeout" } else { "connect" };
                tracing::warn!(reason, error = %e, "Lab unreachable; lab events stay pending");
                return self.fail(&ids, reason).await;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            tracing::error!("the Lab rejected LAB_EVENTS_SECRET (401); lab events stay pending");
            return self.fail(&ids, "auth").await;
        }
        if status != reqwest::StatusCode::OK {
            tracing::warn!(%status, "the Lab did not accept the lab events batch");
            return self.fail(&ids, "status").await;
        }
        #[derive(serde::Deserialize)]
        struct Ack {
            accepted: Vec<Uuid>,
        }
        let Ok(ack) = resp.json::<Ack>().await else {
            tracing::warn!("the Lab answered 200 with a malformed body");
            return self.fail(&ids, "malformed").await;
        };
        let (done, rest): (Vec<Uuid>, Vec<Uuid>) =
            ids.iter().copied().partition(|id| ack.accepted.contains(id));
        if let Err(e) = self.repo.mark_delivered(&done).await {
            // Not marked: they will be sent again and the Lab ignores the duplicates.
            tracing::error!(error = %e, "cannot mark lab events delivered");
        }
        self.metrics.delivered_total.inc_by(done.len() as u64);
        if !rest.is_empty() {
            self.metrics.failed_total.with_label_values(&["not_accepted"]).inc_by(rest.len() as u64);
            if let Err(e) = self.repo.reschedule(&rest).await {
                tracing::error!(error = %e, "cannot back off unaccepted lab events");
            }
        }
        Delivery::Sent { delivered: done.len(), pending: rest.len() }
    }

    async fn fail(&self, ids: &[Uuid], reason: &'static str) -> Delivery {
        self.metrics.failed_total.with_label_values(&[reason]).inc();
        if let Err(e) = self.repo.reschedule(ids).await {
            // The lease still expires, so the rows come back anyway.
            tracing::error!(error = %e, "cannot back off lab events");
        }
        Delivery::Failed { reason, pending: ids.len() }
    }

    pub async fn run(self, cancel: CancellationToken) {
        let mut last_purge = Instant::now() - Duration::from_secs(3_600);
        loop {
            // Drain while batches come back full.
            loop {
                match self.deliver_once().await {
                    Delivery::Sent { delivered, pending } if (delivered + pending) as i64 == BATCH_SIZE => continue,
                    _ => break,
                }
            }
            if let Ok(stats) = self.repo.stats().await {
                self.metrics.pending.set(stats.pending);
                self.metrics.oldest_pending_seconds.set(stats.oldest_pending_seconds);
            }
            if last_purge.elapsed() >= Duration::from_secs(3_600) {
                match self.repo.purge_delivered(RETENTION).await {
                    Ok(n) if n > 0 => tracing::info!(purged = n, "old delivered lab events purged"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "cannot purge delivered lab events"),
                }
                last_purge = Instant::now();
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = self.notify.notified() => {}
                () = tokio::time::sleep(TICK) => {}
            }
        }
        tracing::info!("lab events sender stopped");
    }
```

`crates/control-plane/src/main.rs`: after migrations and before `axum::serve`:

```rust
    let state = AppState::new(pool.clone());
    let cancel = tokio_util::sync::CancellationToken::new();
    let sender = match meili_ingest_control_plane::lab_sender::LabConfig::from_env()
        .context("invalid Lab events configuration")?
    {
        Some(config) => {
            tracing::info!(url = %config.url, "lab events sender enabled");
            let sender = meili_ingest_control_plane::lab_sender::LabSender::new(
                meili_ingest_control_plane::lab_events::LabEventRepo::new(pool.clone()),
                config,
                state.metrics.clone(),
                state.lab_notify.clone(),
            )?;
            Some(tokio::spawn(sender.run(cancel.clone())))
        }
        None => {
            tracing::info!("LAB_URL is unset: lab events are kept in the outbox, not sent");
            None
        }
    };
```

serve with `app(state)` instead of `app(AppState::new(pool.clone()))`, and after `axum::serve(...).await`:

```rust
    cancel.cancel();
    if let Some(handle) = sender {
        let _ = handle.await;
    }
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane`
Expected: PASS.

- [ ] **Step 7: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add Cargo.toml Cargo.lock crates/control-plane
git commit -m "feat(control-plane): deliver lab events to the Lab, signed, never dropped

LAB_URL and LAB_EVENTS_SECRET enable the sender; GET /metrics exposes the outbox."
```

---

### Task 10: The worker posts the event; the usage activity retries for 7 days

**Files:**
- Modify: `crates/worker/src/config.rs`, `crates/worker/src/activity.rs`, `crates/worker/src/workflow.rs`, `crates/worker/src/main.rs`, `crates/worker/Cargo.toml` (dev-deps if missing)
- Test: `crates/worker/src/activity.rs` (test module)

**Interfaces:**
- Consumes: `meili_ingest_usage::lab::{lab_event_for_job, lab_event_id}` (Task 7), `POST /internal/lab-events` (Task 8).
- Produces: `WorkerConfig.lab_events_enabled: bool` (`LAB_EVENTS_ENABLED` = `true`/`1`); `StepActivities.lab_events: bool` + `with_lab_events(bool) -> Self`; `StepActivities::report_usage(&self, input: &JobUsageInput) -> Result<(), UsageReportError>`; `UsageReportError::{Retryable(String), Permanent(String)}`.

- [ ] **Step 1: Write the failing tests**

In `crates/worker/src/activity.rs` test module (it already uses wiremock for the control plane):

```rust
    fn lab_usage_input(tenant: &str) -> meili_ingest_usage::JobUsageInput {
        meili_ingest_usage::JobUsageInput {
            job_id: Uuid::new_v4(),
            workflow_id: "ingest-x".into(),
            pipeline_uid: "builtin.json".into(),
            tenant_id: tenant.into(),
            status: meili_ingest_plugin_sdk::JobStatus::Succeeded,
            ..Default::default()
        }
    }

    async fn control_plane_with_lab_events(status: u16, expect: u64) -> MockServer {
        let cp = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path_regex(r"^/internal/jobs/.+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&cp)
            .await;
        Mock::given(method("POST"))
            .and(path("/internal/lab-events"))
            .respond_with(ResponseTemplate::new(status).set_body_json(serde_json::json!({"inserted": 1})))
            .expect(expect)
            .mount(&cp)
            .await;
        cp
    }

    fn acts_with(cp: &MockServer, lab: bool) -> StepActivities {
        StepActivities::new(Arc::new(PluginRegistry::builtin()), BlobStore::memory(), 1 << 20)
            .with_control_plane(Some(cp.uri()))
            .with_lab_events(lab)
    }

    #[tokio::test]
    async fn a_lab_job_posts_its_event_to_the_outbox() {
        let cp = control_plane_with_lab_events(202, 1).await;
        let input = lab_usage_input("0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61");
        acts_with(&cp, true).report_usage(&input).await.unwrap();
        let posted = cp
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.url.path() == "/internal/lab-events")
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&posted.body).unwrap();
        assert_eq!(
            body["events"][0]["id"],
            meili_ingest_usage::lab::lab_event_id(input.job_id).to_string()
        );
    }

    #[tokio::test]
    async fn no_event_without_the_flag_or_a_lab_tenant() {
        let cp = control_plane_with_lab_events(202, 0).await;
        acts_with(&cp, false)
            .report_usage(&lab_usage_input("0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61"))
            .await
            .unwrap();
        acts_with(&cp, true).report_usage(&lab_usage_input("hackersearch")).await.unwrap();
        acts_with(&cp, true).report_usage(&lab_usage_input("")).await.unwrap();
    }

    #[tokio::test]
    async fn an_outbox_failure_is_retryable_and_comes_before_analytics() {
        let cp = control_plane_with_lab_events(500, 1).await;
        let tinybird = MockServer::start().await;
        Mock::given(method("POST")).respond_with(ResponseTemplate::new(200)).expect(0).mount(&tinybird).await;
        let usage = meili_ingest_usage::UsageClient::new(
            tinybird.uri(),
            "token",
            "meili_ingest_usage",
            reqwest::Client::new(),
        );
        let acts = acts_with(&cp, true).with_usage(Some(usage));
        let err = acts
            .report_usage(&lab_usage_input("0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61"))
            .await
            .unwrap_err();
        assert!(matches!(err, UsageReportError::Retryable(_)), "{err:?}");
    }
```

(`UsageClient::new`'s argument types are whatever that constructor takes — check `crates/usage/src/lib.rs` and adapt the literal types; `path_regex` comes from `wiremock::matchers`.)

In `crates/worker/src/config.rs`, add a unit test module:

```rust
#[cfg(test)]
mod tests {
    #[test]
    fn lab_events_flag_parsing() {
        for (raw, expected) in [("true", true), ("1", true), ("TRUE", true), ("false", false), ("0", false), ("", false)] {
            assert_eq!(super::parse_flag(Some(raw)), expected, "{raw:?}");
        }
        assert!(!super::parse_flag(None));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-worker report_usage lab_ config`
Expected: FAIL to compile (`with_lab_events`, `report_usage`, `UsageReportError`, `parse_flag` missing).

- [ ] **Step 3: Implement**

`config.rs`: field `/// Post Lab billing events to the control plane's outbox (`LAB_EVENTS_ENABLED`). pub lab_events_enabled: bool,` set with `lab_events_enabled: parse_flag(std::env::var("LAB_EVENTS_ENABLED").ok().as_deref()),` and:

```rust
/// `true`/`1` (any case) → on; anything else or unset → off.
fn parse_flag(raw: Option<&str>) -> bool {
    raw.map(str::trim)
        .is_some_and(|v| v == "1" || v.eq_ignore_ascii_case("true"))
}
```

`activity.rs`:

```rust
/// Why reporting a job's usage failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UsageReportError {
    /// Try again (control plane or analytics store unavailable).
    #[error("{0}")]
    Retryable(String),
    /// A human must fix something (rejected payload, bad token).
    #[error("{0}")]
    Permanent(String),
}

impl From<UsageReportError> for ActivityError {
    fn from(e: UsageReportError) -> Self {
        match e {
            UsageReportError::Retryable(m) => {
                ActivityError::application(ApplicationFailure::new(anyhow::anyhow!(m)))
            }
            UsageReportError::Permanent(m) => {
                ActivityError::application(ApplicationFailure::non_retryable(anyhow::anyhow!(m)))
            }
        }
    }
}
```

(Add `thiserror` to the worker's dependencies if it is not there: `thiserror.workspace = true`.)

`StepActivities`: field `/// Post Lab billing events to the control plane (spec §5.3). pub lab_events: bool,` (`false` in `new`), and

```rust
    /// Post each finished Lab job's billing event to the control plane's outbox.
    pub fn with_lab_events(mut self, enabled: bool) -> Self {
        self.lab_events = enabled;
        self
    }
```

Replace the body of the `record_usage` activity with `self.report_usage(&input).await.map_err(ActivityError::from)`, and add the seam (outside the `#[activities]` block, in a plain `impl StepActivities`):

```rust
    /// Record a finished job: status write-back, then the Lab billing event, then
    /// analytics. The bill goes first so an analytics outage cannot hold it back;
    /// every step is idempotent, so a retry repeats them harmlessly.
    pub async fn report_usage(&self, input: &JobUsageInput) -> Result<(), UsageReportError> {
        let job_id = input.job_id;
        self.patch_job(job_id, input.status, None, input.error.clone())
            .await
            .map_err(|e| UsageReportError::Retryable(e.to_string()))?;

        if self.lab_events {
            self.post_lab_event(input).await?;
        }

        let Some(client) = &self.usage else {
            return Ok(());
        };
        let events = events_for_job(input);
        match client.send(&events).await {
            Ok(()) => {
                tracing::info!(job_id = %job_id, events = events.len(), "usage recorded");
                Ok(())
            }
            Err(e) if e.is_retryable() => {
                Err(UsageReportError::Retryable(format!("usage store unavailable: {e}")))
            }
            Err(e) => Err(UsageReportError::Permanent(format!("usage rejected: {e}"))),
        }
    }

    async fn post_lab_event(&self, input: &JobUsageInput) -> Result<(), UsageReportError> {
        let event = match meili_ingest_usage::lab::lab_event_for_job(input) {
            Ok(e) => e,
            Err(reason) => {
                tracing::debug!(job_id = %input.job_id, reason = reason.as_str(), "no Lab event for this job");
                return Ok(());
            }
        };
        let Some(base) = &self.control_plane_url else {
            return Err(UsageReportError::Permanent(
                "LAB_EVENTS_ENABLED needs CONTROL_PLANE_URL".into(),
            ));
        };
        let resp = self
            .http
            .post(format!("{base}/internal/lab-events"))
            .json(&serde_json::json!({ "events": [event] }))
            .send()
            .await
            .map_err(|e| UsageReportError::Retryable(format!("control plane unreachable: {e}")))?;
        if !resp.status().is_success() {
            return Err(UsageReportError::Retryable(format!(
                "control plane refused the lab event: {}",
                resp.status()
            )));
        }
        tracing::info!(job_id = %input.job_id, "lab event recorded");
        Ok(())
    }
```

`workflow.rs` `emit_usage`: replace `USAGE_MAX_ATTEMPTS` and the options with:

```rust
/// How long the usage activity keeps retrying. Its rows are what the Lab bills from,
/// so a control-plane or analytics outage must not lose them; the job's own status is
/// written first and does not wait on this (spec §5.3).
const USAGE_RETRY_WINDOW: Duration = Duration::from_secs(7 * 24 * 3_600);
```

```rust
        let opts = ActivityOptions::with_close_timeouts(ActivityCloseTimeouts::ScheduleAndStartToClose {
            schedule_to_close: USAGE_RETRY_WINDOW,
            start_to_close: Duration::from_secs(60),
        })
        .task_queue("workers-general".to_string())
        // (keep the existing comment about detaching from workflow cancellation)
        .cancellation_token(WorkflowCancellationToken::new())
        .retry_policy(
            RetryPolicy::builder()
                .initial_interval(Duration::from_secs(2))
                .backoff_coefficient(2.0)
                .maximum_interval(Duration::from_secs(300))
                .build(),
        )
        .build();
```

`maximum_attempts` is left at its default, 0, which Temporal treats as unlimited within the window. `ActivityCloseTimeouts` is re-exported by the Temporal SDK next to `ActivityOptions` (it is defined in `temporalio-common-wasm`); import it from the same crate path `ActivityOptions` comes from in this file, or find the path with `rg -n 'pub use.*ActivityCloseTimeouts' ~/.cargo/registry/src/*/temporalio-*1.0.0/src`. This change alters only retry options, never the commands the workflow emits, so in-flight workflows replay unchanged.

`main.rs`: `.with_lab_events(config.lab_events_enabled)` on the `StepActivities` builder, and before it:

```rust
    if config.lab_events_enabled && config.control_plane_url.is_none() {
        anyhow::bail!("LAB_EVENTS_ENABLED needs CONTROL_PLANE_URL: the events go to its outbox");
    }
    if config.lab_events_enabled {
        tracing::info!("Lab billing events enabled");
    }
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-worker`
Expected: PASS.

- [ ] **Step 5: Lint and commit**

```bash
cargo clippy --workspace --all-targets -- -D warnings && cargo fmt --all
git add crates/worker Cargo.lock
git commit -m "feat(worker): post each Lab job's billing event before analytics

LAB_EVENTS_ENABLED turns it on. The usage activity now retries for up to 7 days
(5-minute max interval) instead of 10 attempts, so no outage loses a bill."
```

---

### Task 11: End-to-end Lab leg

**Files:**
- Create: `scripts/fake_lab.py`
- Modify: `scripts/e2e.sh`

**Interfaces:**
- Consumes: everything above. `scripts/fake_lab.py <port> <secret> <events.ndjson>` serves `POST /internal/events` (checks `X-Lab-Signature`, `401` on mismatch, answers `{"accepted":[ids]}`) and `GET /events` (JSON array of every received event).

- [ ] **Step 1: Write the fake Lab**

```python
#!/usr/bin/env python3
"""A stand-in for the Meilisearch Lab's event receiver, for scripts/e2e.sh --lab.

POST /internal/events  checks X-Lab-Signature (sha256=<hex HMAC-SHA256(secret, body)>),
                       stores each event once by id, answers {"accepted": [ids]}.
GET  /events           every stored event, as a JSON array.
"""

import hashlib
import hmac
import json
import sys
from http.server import BaseHTTPRequestHandler, HTTPServer

PORT, SECRET, OUT = int(sys.argv[1]), sys.argv[2].encode(), sys.argv[3]
EVENTS: dict[str, dict] = {}


class Handler(BaseHTTPRequestHandler):
    def _json(self, status: int, body) -> None:
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if self.path != "/internal/events":
            return self._json(404, {"error": "not found"})
        expected = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(self.headers.get("X-Lab-Signature", ""), expected):
            return self._json(401, {"error": "bad signature"})
        accepted = []
        for event in json.loads(body)["events"]:
            if event["id"] not in EVENTS:
                EVENTS[event["id"]] = event
                with open(OUT, "a") as f:
                    f.write(json.dumps(event) + "\n")
            accepted.append(event["id"])
        self._json(200, {"accepted": accepted})

    def do_GET(self):
        if self.path == "/events":
            return self._json(200, list(EVENTS.values()))
        self._json(404, {"error": "not found"})

    def log_message(self, *_):
        pass


HTTPServer(("127.0.0.1", PORT), Handler).serve_forever()
```

- [ ] **Step 2: Add the `--lab` leg to `scripts/e2e.sh`**

At the top, after `set -euo pipefail`:

```bash
LAB=0
[ "${1:-}" = "--lab" ] && LAB=1
LAB_PORT=${LAB_PORT:-58191}
GW_LAB_PORT=${GW_LAB_PORT:-58081}
LAB_EVENTS_SECRET_VALUE=e2e-lab-events-secret
LAB_TOKEN=e2e-lab-service-token
```

In `--- services`, when `LAB=1`, start the fake Lab before the control plane and pass the Lab variables to the control plane and the worker:

```bash
CP_LAB_ENV=()
WORKER_LAB_ENV=()
if [ "$LAB" = 1 ]; then
  python3 scripts/fake_lab.py "${LAB_PORT}" "${LAB_EVENTS_SECRET_VALUE}" "${WORK}/lab-events.ndjson" >"${WORK}/lab.log" 2>&1 &
  PIDS+=($!)
  CP_LAB_ENV=(LAB_URL="http://127.0.0.1:${LAB_PORT}" LAB_EVENTS_SECRET="${LAB_EVENTS_SECRET_VALUE}")
  WORKER_LAB_ENV=(LAB_EVENTS_ENABLED=true)
fi
```

Change the control-plane launch to `env ${CP_LAB_ENV[@]+"${CP_LAB_ENV[@]}"} BIND="0.0.0.0:${CP_PORT}" ./target/debug/meili-ingest-control-plane …` and the worker launch to `env ${WORKER_LAB_ENV[@]+"${WORKER_LAB_ENV[@]}"} TASK_QUEUE=workers-general ./target/debug/meili-ingest-worker …`. The `${arr[@]+…}` form is required: macOS ships bash 3.2, where `"${arr[@]}"` on an empty array is an "unbound variable" error under `set -u`. With an empty array, `env` runs the command unchanged. When `LAB=1`, also start a second gateway with auth on (the first stays open so every existing assertion is unchanged):

```bash
if [ "$LAB" = 1 ]; then
  BIND="0.0.0.0:${GW_LAB_PORT}" ENVOY_TRUSTED_HEADER="${ENVOY_SECRET}" LAB_SERVICE_TOKEN="${LAB_TOKEN}" \
    ./target/debug/meili-ingest-gateway >"${WORK}/gw-lab.log" 2>&1 &
  PIDS+=($!)
  wait_for "http://localhost:${GW_LAB_PORT}/health" gateway-lab
fi
```

Just before `echo "=== E2E PASSED ==="`:

```bash
if [ "$LAB" = 1 ]; then
  echo "--- Meilisearch Lab: tenants, management auth, billing events"
  GWL="http://localhost:${GW_LAB_PORT}"
  TA=0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61
  TB=0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b62
  code=$(curl -sS -o /dev/null -w '%{http_code}' "${GWL}/pipelines")
  [ "$code" = 401 ] || { echo "management without a token → ${code}, expected 401" >&2; exit 1; }

  curl -fsS -X POST -H "Authorization: Bearer ${LAB_TOKEN}" -H "X-Glutony-Tenant-Id: ${TA}" \
    -H 'Content-Type: application/json' \
    -d '{"uid":"e2e-lab-docs","name":"Lab docs","steps":[{"id":"index","plugin":"meili_indexer"}]}' \
    "${GWL}/pipelines" | jq -e ".tenant_id == \"${TA}\" and .scope == \"tenant\"" >/dev/null \
    || { echo "tenant A could not create its pipeline" >&2; exit 1; }
  code=$(curl -sS -o /dev/null -w '%{http_code}' -H "Authorization: Bearer ${LAB_TOKEN}" \
    -H "X-Glutony-Tenant-Id: ${TB}" "${GWL}/pipelines/e2e-lab-docs")
  [ "$code" = 404 ] || { echo "tenant B saw tenant A's pipeline (${code})" >&2; exit 1; }

  EDGE=(-H "X-Meili-Envoy-Secret: ${ENVOY_SECRET}" -H "X-Meili-Host: ${MEILI_URL}" -H 'X-Meili-Api-Key: masterKey')
  RESPL=$(curl -fsS "${EDGE[@]}" -H "X-Meili-Tenant-Id: ${TA}" -H 'Content-Type: application/json' \
    -d '{"documents":[{"id":"lab-1","title":"billed through the Lab"}]}' \
    "${GWL}/ingest/pipeline/e2e-lab-docs?index=e2e_lab")
  JOBL=$(echo "$RESPL" | jq -r .job_id)
  for _ in $(seq 1 60); do
    S=$(curl -fsS "${EDGE[@]}" -H "X-Meili-Tenant-Id: ${TA}" "${GWL}/jobs/${JOBL}" | jq -r .status)
    [ "$S" = succeeded ] && break
    [ "$S" = failed ] && { echo "lab job failed" >&2; tail -40 "${WORK}/worker.log"; exit 1; }
    sleep 1
  done
  [ "$S" = succeeded ] || { echo "lab job state ${S}" >&2; exit 1; }
  code=$(curl -sS -o /dev/null -w '%{http_code}' "${EDGE[@]}" -H "X-Meili-Tenant-Id: ${TB}" "${GWL}/jobs/${JOBL}")
  [ "$code" = 404 ] || { echo "tenant B saw tenant A's job (${code})" >&2; exit 1; }

  for _ in $(seq 1 30); do
    N=$(curl -fsS "http://127.0.0.1:${LAB_PORT}/events" \
      | jq "[.[] | select(.data.job_id == \"${JOBL}\" and .account_id == \"${TA}\" and .product == \"glutony\")] | length")
    [ "$N" = 1 ] && break
    sleep 1
  done
  [ "$N" = 1 ] || { echo "the Lab did not receive exactly one usage event for ${JOBL} (${N})" >&2; tail -40 "${WORK}/cp.log"; exit 1; }
  curl -fsS "http://localhost:${CP_PORT}/metrics" | grep -q '^glutony_lab_events_delivered_total [1-9]' \
    || { echo "delivered_total did not move" >&2; exit 1; }
  echo "Lab leg passed"
fi
```

Update the script's header comment to mention `--lab`.

- [ ] **Step 3: Run it**

Run: `./scripts/e2e.sh --lab` (needs Docker, the `temporal` CLI, `jq`, `python3`), then `./scripts/e2e.sh` once more without the flag.
Expected: both end with `=== E2E PASSED ===`; the first also prints `Lab leg passed`.

- [ ] **Step 4: Commit**

```bash
chmod +x scripts/fake_lab.py
git add scripts/fake_lab.py scripts/e2e.sh
git commit -m "test(e2e): --lab runs a tenant through the service token, the edge and billing"
```

---

### Task 12: Rollback script

**Files:**
- Create: `scripts/rollback-lab-seams.sql`, `crates/control-plane/tests/rollback.rs`

**Interfaces:**
- Produces: a plain-SQL script that refuses to run while `lab_events` has undelivered rows unless the session setting `glutony.rollback_force` is `on`; renames columns and indexes back; drops `lab_events`; deletes migrations 3 and 4 from `_sqlx_migrations`.

- [ ] **Step 1: Write the failing test**

Create `crates/control-plane/tests/rollback.rs` (copy the `TestDb`/`setup` harness from `tests/lab_events.rs`, schema prefix `cp_rb_`), then:

```rust
const SCRIPT: &str = include_str!("../../../scripts/rollback-lab-seams.sql");

#[tokio::test]
async fn rollback_refuses_undelivered_events_then_restores_the_old_schema() {
    let Some(t) = setup().await else { return };
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO lab_events (id, body) VALUES ($1, '{}'::jsonb)")
        .bind(id)
        .execute(&t.pool)
        .await
        .unwrap();

    let err = sqlx::raw_sql(AssertSqlSafe(SCRIPT)).execute(&t.pool).await.unwrap_err();
    assert!(err.to_string().contains("undelivered"), "{err}");
    let still: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.columns \
        WHERE table_schema = current_schema() AND column_name = 'tenant_id'")
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert_eq!(still, 4, "a refused rollback changes nothing");

    sqlx::query("UPDATE lab_events SET delivered_at = now()").execute(&t.pool).await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(SCRIPT)).execute(&t.pool).await.unwrap();

    let old: i64 = sqlx::query_scalar("SELECT count(*) FROM information_schema.columns \
        WHERE table_schema = current_schema() AND column_name = 'project_id'")
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert_eq!(old, 4);
    let lab: Option<String> = sqlx::query_scalar("SELECT to_regclass('lab_events')::text")
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert_eq!(lab, None);
    let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM _sqlx_migrations ORDER BY version")
        .fetch_all(&t.pool)
        .await
        .unwrap();
    assert_eq!(versions, vec![1, 2], "the previous binary sees only the migrations it knows");
    let index: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_indexes \
        WHERE schemaname = current_schema() AND indexname = 'pipelines_uid_project'")
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert_eq!(index, 1);
    t.drop_schema().await;
}

#[tokio::test]
async fn force_rolls_back_despite_undelivered_events() {
    let Some(t) = setup().await else { return };
    sqlx::query("INSERT INTO lab_events (id, body) VALUES ($1, '{}'::jsonb)")
        .bind(Uuid::new_v4())
        .execute(&t.pool)
        .await
        .unwrap();
    let mut conn = t.pool.acquire().await.unwrap();
    sqlx::query("SET glutony.rollback_force = 'on'").execute(&mut *conn).await.unwrap();
    sqlx::raw_sql(AssertSqlSafe(SCRIPT)).execute(&mut *conn).await.unwrap();
    drop(conn);
    t.drop_schema().await;
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane --test rollback`
Expected: FAIL to compile (`scripts/rollback-lab-seams.sql` does not exist).

- [ ] **Step 3: Write the script**

```sql
-- Roll back migrations 0003 (tenant_id) and 0004 (lab_events) so the previous
-- glutony binary starts: sqlx refuses a database with a migration it does not know.
--
--   psql "$DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f scripts/rollback-lab-seams.sql
--
-- Undelivered Lab events are lost by a rollback, so the script refuses while any
-- exist. Deliver them (the sender drains the outbox) or force it:
--
--   PGOPTIONS='-c glutony.rollback_force=on' psql "$DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f scripts/rollback-lab-seams.sql
--
-- `--single-transaction` makes it all-or-nothing. The file has no BEGIN/COMMIT of its
-- own on purpose: run as one batch (as the test does), Postgres already wraps it in
-- one implicit transaction, and an explicit BEGIN would leave the connection in an
-- aborted transaction when the guard fires.
--
-- Stop every glutony service first. See docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md §10.

DO $$
BEGIN
    IF to_regclass('lab_events') IS NOT NULL
       AND coalesce(current_setting('glutony.rollback_force', true), '') <> 'on'
       AND EXISTS (SELECT 1 FROM lab_events WHERE delivered_at IS NULL)
    THEN
        RAISE EXCEPTION 'lab_events has undelivered rows; let the sender deliver them or set glutony.rollback_force = on';
    END IF;
END $$;

DROP TABLE IF EXISTS lab_events;

ALTER TABLE pipelines         RENAME COLUMN tenant_id TO project_id;
ALTER TABLE jobs              RENAME COLUMN tenant_id TO project_id;
ALTER TABLE sources           RENAME COLUMN tenant_id TO project_id;
ALTER TABLE meili_connections RENAME COLUMN tenant_id TO project_id;

ALTER INDEX pipelines_uid_tenant         RENAME TO pipelines_uid_project;
ALTER INDEX jobs_tenant_started          RENAME TO jobs_project_started;
ALTER INDEX sources_uid_tenant           RENAME TO sources_uid_project;
ALTER INDEX sources_pipeline_tenant      RENAME TO sources_pipeline;
ALTER INDEX meili_connections_uid_tenant RENAME TO meili_connections_uid_project;

DELETE FROM _sqlx_migrations WHERE version IN (3, 4);
```

(`sqlx::raw_sql` sends the script as one simple-query batch, which Postgres runs as one implicit transaction: a failed guard rolls back the whole batch and leaves the connection usable.)

- [ ] **Step 4: Run the tests to verify they pass**

Run: `DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane --test rollback`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add scripts/rollback-lab-seams.sql crates/control-plane/tests/rollback.rs
git commit -m "chore: rollback script for the tenant rename and the lab events outbox"
```

---

### Task 13: Docs, UI freeze, CI drift job, spec amendments

**Files:**
- Create: `docs/deployment/meilisearch-lab.mdx`
- Modify: `docs/mint.json`, `docs/concepts/multi-tenancy.mdx`, `docs/concepts/usage.mdx`, `docs/concepts/jobs.mdx`, `docs/concepts/pipelines.mdx`, `docs/deployment/environment-variables.mdx`, `docs/deployment/meilisearch-cloud.mdx`, `README.md`, `ui/AGENTS.md`, `ui/CLAUDE.md`, `.github/workflows/ci.yml`, `docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md`

**Interfaces:** documentation only.

- [ ] **Step 1: Rename the tenant in the prose**

Run `rg -n 'project_id|project id' docs/concepts docs/deployment README.md`. Every hit about glutony's API, tables or Temporal payloads becomes `tenant_id` / "tenant id". Hits about Tinybird columns and pipes (`docs/concepts/usage.mdx`, `tinybird/README.md`) and the `X-Meili-Project-Id` header stay as they are.

- [ ] **Step 2: Rewrite `docs/concepts/multi-tenancy.mdx`**

Keep its structure (MeiliContext, Envoy injection, trust, resolution order, index resolution, tenant-scoped pipelines, jobs) and change:
- `MeiliContext` shows `tenant_id: Option<String> // opaque: Lab account, Cloud project, or none`.
- A new first section **Tenants** stating spec §3.1 (opaque, 1-128 characters of `[A-Za-z0-9._:-]`, validated, `400 invalid_tenant`).
- Resolution order step 3 becomes `X-Meili-Tenant-Id → tenant_id`, then `X-Meili-Project-Id → tenant_id (when X-Meili-Tenant-Id is absent)`.
- A new section **Management routes** with the table from spec §4.2 and the scoping rules from §4.3 (reads: own plus global rows, marked `scope`; writes: own rows only; cross-tenant is `404`).
- The **Jobs** section: `GET /jobs/{id}` and `POST /jobs/{id}/cancel` answer `404` for another tenant's job.

- [ ] **Step 3: Write `docs/deployment/meilisearch-lab.mdx`**

```mdx
---
title: Meilisearch Lab
description: Run glutony as a Meilisearch Lab data plane - tenants, management auth and billing events
---

glutony runs on its own. The Meilisearch Lab plugs into it through three seams,
each off until you configure it.

## Tenants

Every pipeline, source, connection and job belongs to a **tenant** or to the global
scope. Behind the Lab the tenant is the Lab account id (a UUID). Ingest traffic
arrives on a Meilisearch hostname, so the Lab's edge injects it:

| Header | Value |
|---|---|
| `X-Meili-Envoy-Secret` | the gateway's `ENVOY_TRUSTED_HEADER` |
| `X-Meili-Tenant-Id` | the account id that owns the instance |

`X-Meili-Project-Id` still works (Meilisearch Cloud's Envoy sends it);
`X-Meili-Tenant-Id` wins when both are present.

## Management auth

| `LAB_SERVICE_TOKEN` | `ADMIN_API_KEY` | Caller sends | Acts as |
|---|---|---|---|
| set | - | `Authorization: Bearer <token>` + `X-Glutony-Tenant-Id` | that tenant |
| - | set | `Authorization: Bearer <key>` (+ optional `X-Glutony-Tenant-Id`) | global, or that tenant |
| unset | unset | nothing | open: tenant from the trusted edge |

Management routes are every route except ingest, `GET /jobs/{id}` and `/health`.
Once either secret is set, `X-Meili-*` headers no longer grant management access.
The Lab console calls these routes **server side only**: the token never reaches a
browser.

The frozen admin UI (`ui/`) cannot send a token. Once auth is on, the proxy in front
of the admin hostname must send `Authorization: Bearer <ADMIN_API_KEY>` for it, and
the Meilisearch key it used to inject as the bearer token moves to
`X-Meili-Api-Key` (with `X-Meili-Host` and `X-Meili-Envoy-Secret`).

## Billing events

Each job run for a Lab account produces one `usage.recorded` event:

```json
{
  "id": "<UUIDv5 of job:{job_id}:usage>",
  "type": "usage.recorded",
  "occurred_at": "2026-10-01T10:00:09.000Z",
  "account_id": "<tenant id>",
  "api_key_id": null,
  "product": "glutony",
  "data": {
    "job_id": "…", "pipeline_uid": "builtin.pdf", "status": "succeeded",
    "duration_ms": 9000, "cost_micro_usd": 210, "cost_complete": true,
    "units": { "documents_out": 12, "input_bytes": 5000, "pages": 9, "images": 0,
               "audio_seconds": 0, "llm_input_tokens": 1000, "llm_output_tokens": 100,
               "llm_requests": 1, "external_requests": 0 }
  }
}
```

- Jobs without a UUID tenant (standalone, Cloud projects) are never sent.
- Failed and cancelled jobs are sent: provider cost already spent is real.
- `cost_micro_usd` is what glutony paid providers, from the provider cost table
  (`config/provider-costs.toml`, or `PROVIDER_COSTS_FILE`). A call it cannot price
  costs 0 and sets `cost_complete: false`. The bundled prices are placeholders: set
  yours before billing.
- The Lab converts units and cost to credits; glutony never computes credits.

Enable it with `LAB_EVENTS_ENABLED=true` on the workers and `LAB_URL` +
`LAB_EVENTS_SECRET` on the control plane. Workers write each event to the control
plane's `lab_events` outbox; the control plane sends batches of up to 500 to
`POST {LAB_URL}/internal/events` with
`X-Lab-Signature: sha256=<hex HMAC-SHA256(LAB_EVENTS_SECRET, body)>`, marks the ids
the Lab lists in `accepted`, and retries the rest with backoff. Events are never
dropped. `LAB_URL` must be `https` unless its host is loopback or private.

### Monitoring

The control plane serves `GET /metrics`:

- `glutony_lab_events_pending`
- `glutony_lab_events_oldest_pending_seconds`
- `glutony_lab_events_delivered_total`
- `glutony_lab_events_failed_total{reason}` (`connect`, `timeout`, `auth`, `status`, `malformed`, `not_accepted`)

Alert (same threshold as Lumen):

```yaml
- alert: GlutonyLabEventsStuck
  expr: glutony_lab_events_oldest_pending_seconds > 900
  for: 5m
  annotations:
    summary: glutony has billing events the Lab has not accepted for 15 minutes
```

## Known limit: no spend enforcement

glutony bills after the fact and never refuses work for lack of credits (Lumen and
Scrapix answer `402`). Because glutony pays the LLM and transcription providers
itself, an account at zero credits can still run up real cost. Do not sell glutony
through the Lab to paying accounts until a suspend switch or a lease exists.

## What the Lab has to do

1. Accept `product: "glutony"` and `glutonyUsageData` (schema:
   `contracts/vendor/lab/lab-events.schema.json` in this repo).
2. Accept `X-Lab-Signature`.
3. Convert `cost_micro_usd` and units to credits.
4. Give each glutony deployment its own events secret and service token.
5. Inject `X-Meili-Tenant-Id` and `X-Meili-Envoy-Secret` on glutony's ingest routes.
6. Call the management routes from the console's server with
   `LAB_SERVICE_TOKEN` and `X-Glutony-Tenant-Id`.

## Rollback

`scripts/rollback-lab-seams.sql` undoes the tenant rename and drops the outbox so the
previous binary starts. Run it with `psql --single-transaction -v ON_ERROR_STOP=1 -f`.
It refuses while events are undelivered unless run with
`PGOPTIONS='-c glutony.rollback_force=on'`.
```

Add `"deployment/meilisearch-lab"` to the deployment group in `docs/mint.json`, after `deployment/meilisearch-cloud`.

- [ ] **Step 4: Environment variables, usage, Cloud docs**

`docs/deployment/environment-variables.mdx`: add rows, in the existing table format, for `LAB_SERVICE_TOKEN` and `ADMIN_API_KEY` (gateway), `LAB_URL` and `LAB_EVENTS_SECRET` (control plane), `LAB_EVENTS_ENABLED` and `PROVIDER_COSTS_FILE` (worker), with the one-line meanings from spec §5.6.

`docs/concepts/usage.mdx`: a section **Analytics and billing** — Tinybird rows are analytics (dashboard, `GET /usage`); the Lab events are the bill; the provider cost table and `cost_complete`; link to `/deployment/meilisearch-lab`.

`docs/deployment/meilisearch-cloud.mdx`: add `X-Meili-Tenant-Id` to the injected-header list, noting `X-Meili-Project-Id` still works.

- [ ] **Step 5: Freeze the UI**

Prepend to `ui/AGENTS.md` and `ui/CLAUDE.md`:

```markdown
> **Frozen.** This admin UI takes bug fixes only. New screens go to the Meilisearch
> Lab console, built from `docs/openapi.yaml`. It works in open mode only (no
> `LAB_SERVICE_TOKEN` / `ADMIN_API_KEY`), or behind a proxy that injects the admin key.
```

In `README.md`, under `## Admin UI`, insert the same note as the section's first paragraph, and add a short `## Meilisearch Lab` section (three sentences: the three seams, off by default, link to `docs/deployment/meilisearch-lab.mdx`).

- [ ] **Step 6: The contract drift job**

Append to `.github/workflows/ci.yml` `jobs:`:

```yaml
  lab-contract-drift:
    name: Lab contract drift
    # Disabled until the Lab publishes a lab-events schema that includes glutony
    # (docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md §5.7). Then set
    # LAB_SCHEMA_URL to its raw URL on meilisearch/lab main and remove this `if`.
    if: false
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Compare the vendored schema with the Lab's
        env:
          LAB_SCHEMA_URL: https://raw.githubusercontent.com/meilisearch/lab/main/contracts/lab-events.schema.json
        run: |
          curl -fsSL "$LAB_SCHEMA_URL" -o /tmp/lab-events.schema.json
          diff -u /tmp/lab-events.schema.json contracts/vendor/lab/lab-events.schema.json
```

- [ ] **Step 7: Record the spec amendments**

Append to `docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md`:

```markdown
## 12. Amendments made while planning (2026-10-01)

1. **§5.2:** `data.source_uid` is dropped. No workflow input carries the source, and
   billing does not need it.
2. **§5.1:** `UsageUnits` carries an additive `unpriced_calls: u64` instead of a
   `cost_complete: bool`, because units merge by addition from an all-zero start. The
   event's `cost_complete` is `unpriced_calls == 0`.
3. **§5.3:** the usage activity's retry policy changes in every deployment, not only
   with lab events on: workflow code cannot read configuration, and a new activity
   would break replay of in-flight workflows. It retries without an attempt limit,
   5-minute maximum interval, within a 7-day `schedule_to_close` window.
4. **§10:** the rollback guard is the session setting `glutony.rollback_force = on`
   (`PGOPTIONS='-c glutony.rollback_force=on'`), not `-v force=1`, so the script is
   plain SQL a test can run; it is run with `psql --single-transaction`.
5. **§5.7:** the Jev enricher is left out of the bundled cost table, so its calls are
   flagged as unpriced until a price is set, rather than billed at a placeholder 0.
```

- [ ] **Step 8: Check the docs build and commit**

Run: `(cd docs && npx --yes mintlify broken-links)` if Mintlify is available locally; otherwise check by hand that every new link target exists.
Expected: no broken links.

```bash
git add docs README.md ui/AGENTS.md ui/CLAUDE.md .github/workflows/ci.yml
git commit -m "docs: Meilisearch Lab deployment guide, tenants, frozen admin UI"
```

---

## Final verification (after Task 13)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
DATABASE_URL=postgres://postgres:dev@localhost:55432/postgres cargo test -p meili-ingest-control-plane
(cd ui && pnpm test && pnpm exec tsc --noEmit)
./scripts/e2e.sh
./scripts/e2e.sh --lab
rg -n 'project_id' crates --glob '!**/usage/src/lib.rs' --glob '!**/handlers/usage.rs'
```

Expected: everything passes; the last command prints only `alias = "project_id"` attributes, the pre-rename compatibility tests, and comments explaining them.
