# glutony Lab platform contract v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

Step 0: copy the spec and this plan from the advisor worktree paths above into this worktree's docs/superpowers/ before starting:
- spec: `/Users/quentindequelen/Projects/Meilisearch/_side_projects/meilisearch-lab/.claude/worktrees/advisor-838e2a/docs/superpowers/specs/2026-10-08-lab-platform-contract-v2.md` → `docs/superpowers/specs/2026-10-08-lab-platform-contract-v2.md`
- plan: `/Users/quentindequelen/Projects/Meilisearch/_side_projects/meilisearch-lab/.claude/worktrees/advisor-838e2a/docs/superpowers/plans/2026-10-08-glutony-lab-contract-v2.md` → `docs/superpowers/plans/2026-10-08-glutony-lab-contract-v2.md`

The implementation repo is the glutony worktree `/Users/quentindequelen/Projects/Meilisearch/_side_projects/glutony/.claude/worktrees/lab-contract-v2` (branch `qdequele/lab-contract-v2`, from `origin/main` 85cc0ce). Every path below is relative to it. Line numbers were verified on that commit.

**Goal:** Make glutony report usage to the Meilisearch Lab under the v2 platform contract (per-instance credentials, Lab-owned events schema with raw units, hosted-only credit pre-check), close the control-plane and tenant-isolation gaps the audit found, and make the project releasable (image, release workflow, k8s secrets, docs).

**Architecture:** A new `meili-ingest-lab` crate holds what the gateway and the control plane share about the Lab (credentials, the `{timestamp}.{body}` HMAC, `GET /internal/instances/me`, `GET /internal/accounts/{id}`). The control plane's existing outbox sender keeps its lease/backoff model and only changes how it signs, plus a 24 h drop. The worker's pure event builder (`crates/usage/src/lab.rs`) emits the v2 `usage.recorded` shape and the two job lifecycle events. The gateway gains a credit pre-check in `submit_one` that only runs on hosted deployments. Internal control-plane routes get a bearer token; tenant jobs can only use their own connections and never fall back to the deployment's `MEILI_URL`.

**Tech Stack:** Rust 1.94 (edition 2024), axum 0.8, reqwest 0.13 (rustls), sqlx 0.9 (Postgres), hmac/sha2/hex, jsonschema 0.45 (tests), wiremock 0.6 (tests), GitHub Actions, Kustomize, Mintlify docs.

**Spec:** `docs/superpowers/specs/2026-10-08-lab-platform-contract-v2.md` (read with `docs/superpowers/specs/2026-10-01-lab-ready-seams-design.md`, the seams it builds on).

## Global Constraints

- Decision A (owner, 2026-10-08, revised): engines are hosted by Meilisearch only; there is no customer-run glutony reporting to the Lab. Every Lab-account job is billed, so the gateway pre-checks credits whenever `LAB_INSTANCE_*` are set, and refuses (503) rather than run unbilled work when it cannot.
- Decision B (owner, 2026-10-08): the price table lives in the Lab; engines send raw units and `provider_cost_micro_usd`; plan limits come from the Lab. glutony never computes credits and keeps no tier constants.
- Spec §3.3: engine env names are exactly `LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET`. `LAB_EVENTS_SECRET` is accepted for one more release with a boot warning, then removed.
- Spec §3.4: service calls carry `Authorization: Bearer <LAB_INSTANCE_SECRET>` and `X-Lab-Instance-Id`; event batches carry `X-Lab-Instance-Id`, `X-Lab-Timestamp` (unix seconds, integer) and `X-Lab-Signature: sha256=<hex HMAC-SHA256(LAB_INSTANCE_SECRET, "<X-Lab-Timestamp>.<raw body>")>`.
- Spec §3.5: an event never acknowledged for 24 h is permanently rejected: drop it with an error log.
- Spec §3.6: `GET /internal/instances/me` returns `{instance_id, kind: "hosted", product, region, lab_url}`; engines call it at boot to confirm their credentials; a 401 aborts boot.
- Spec §4: batches of at most 500 events, `{"events": [...]}`, response `200 {"accepted": [ids]}`; `instance_id` is never in the body.
- Spec §4.2/§4.3: glutony sends `operation: "ingest"` and units `documents, bytes_in, step_seconds, llm_tokens_in, llm_tokens_out, audio_seconds, ocr_pages` (integers ≥ 0); extra unit names are allowed and ignored by pricing.
- Spec §4.4: `job.completed {job_id, index_uid, pages_crawled, documents_indexed, duration_secs}`, `job.failed {job_id, error_message, pages_crawled}`; glutony maps `pipeline_uid` → `index_uid`, `documents` → `documents_indexed`, `pages_crawled = 0`.
- Spec §8.1: engines call `GET /internal/accounts/{id}` (cached 30 s, stale up to 300 s) and refuse with 402 when `credits.balance <= 0`; past the stale window with the Lab unreachable: 503 (fail closed).
- Spec §10: the glutony vs meili-ingest rename is out of scope. Crate names, the `meili-ingest-*` binaries, the `meili-ingest` k8s namespace and the `ui/` stay as they are. This plan only picks one published image name: `ghcr.io/qdequele/glutony`.
- Repo rules (seams spec, in force): never bind-mount source in compose; secrets are `<redacted>` in `Debug` and never logged; tokens compared with `subtle::ConstantTimeEq`; `contracts/vendor/` is a byte copy of the owner's file and is never hand-edited after vendoring; `docs/openapi.yaml` is held against `crates/gateway/src/routes.rs::ROUTES` by a test; Tinybird columns keep their names.
- Dev commands: `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D warnings`, `cargo test --workspace`; DB-backed control-plane tests need `DATABASE_URL` (e.g. `postgres://postgres:dev-password@localhost:5432/postgres` from `docker compose up postgres`).
- Commits: conventional messages (`feat(scope): …`, `fix(scope): …`, `docs: …`, `ci: …`), no `Co-Authored-By` lines (user rule).

## Review Focus

1. A deployment that cannot reach the Lab at boot: `GET /internal/instances/me` fails. Expected: it keeps serving (a 401 aborts boot, anything else is retried every 30 s), and until the Lab has confirmed the credentials every Lab-account job is refused with 503 `lab_unavailable`, never run unbilled. Pinned in Task 6 (`resolve_identity_retries_until_the_lab_answers` + `check_credits_fails_closed_until_the_identity_is_known`).
2. A `X-Lab-Timestamp` from a clock far from the Lab's: the Lab rejects anything more than 300 s off, and the batch comes back 401 forever. Expected: the sender logs `auth`, keeps retrying with backoff, and the 24 h drop eventually sheds the rows with an error log, never a silent loss. Pinned in Task 5 (`failures_back_off_and_never_drop` keeps the 401 path; `maintain_drops_rows_older_than_24h`).
3. A job whose steps sum to more than `i64::MAX` micro-USD or a NaN `audio_seconds`: the schema must still accept the event. Expected: cost is capped at `i64::MAX`, non-finite audio becomes 0, every unit is a non-negative integer. Pinned in Task 1 (`the_cost_is_capped_at_i64_max_for_a_signed_ledger`, `non_finite_audio_seconds_still_make_a_valid_event`).
4. A tenant request that names a *global* connection through a global (built-in) pipeline: after Task 8 the worker asks for `scope=tenant` and the connection is not found, so the step fails non-retryably with a message naming the connection. Expected: the message tells the operator that tenants may only use their own connections. Pinned in Task 8 (`a_tenant_lookup_asks_for_the_tenants_row_only`, `a_tenant_pipeline_cannot_name_a_global_connection`).
5. A dev or test environment that starts the control plane without `CONTROL_PLANE_TOKEN`: the process must refuse with a message that names `CONTROL_PLANE_TOKEN_DISABLED=true`, and `docker compose watch` must keep working. Pinned in Task 7 (`control_plane_token_policy` unit test and the compose change).

---

## File structure

| Path | Responsibility |
|---|---|
| `contracts/vendor/lab/lab-events.schema.json` | Byte copy of the Lab-owned v2 schema (Task 1) |
| `contracts/vendor/lab/README.md` | Vendoring rule and drift check (Task 3) |
| `crates/usage/src/lab.rs` | Pure builders: v2 `usage.recorded`, `job.completed`/`job.failed`, ids (Tasks 1, 2) |
| `crates/worker/src/activity.rs` | Posts the per-job events to the outbox; presents `CONTROL_PLANE_TOKEN` (Tasks 2, 7) |
| `crates/lab/` (new crate `meili-ingest-lab`) | `LabCredentials`, `sign_batch`, `InstanceInfo`, `AccountLookup`, HTTP fetches (Task 4) |
| `crates/control-plane/src/lab_sender.rs` | `LabConfig` v2 + legacy, request signing, `maintain` (24 h drop) (Task 5) |
| `crates/control-plane/src/lab_events.rs` | `drop_stale` (Task 5) |
| `crates/control-plane/src/metrics.rs` | `glutony_lab_events_dropped_total` (Task 5) |
| `crates/control-plane/src/main.rs` | Reads `LAB_*`, logs the instance identity, enforces `CONTROL_PLANE_TOKEN` (Tasks 5, 7) |
| `crates/control-plane/src/lib.rs`, `error.rs` | `/internal/*` bearer middleware, `CpError::Unauthorized` (Task 7) |
| `crates/gateway/src/lab.rs` (new) | `LabClient`: identity, account cache, `check_credits` (Task 6) |
| `crates/gateway/src/state.rs`, `error.rs`, `main.rs`, `handlers/ingest.rs` | Wiring, `PaymentRequired`, pre-check call site, `CONTROL_PLANE_TOKEN` (Tasks 6, 7) |
| `crates/gateway/src/context.rs` | No env destination fallback with a tenant (Task 8) |
| `crates/control-plane/src/connections.rs`, `pipelines.rs`; `crates/worker/src/connection.rs` | Tenant-only connection lookup (Task 8) |
| `.github/workflows/ci.yml`, `.github/workflows/release.yml` | Drift job, release (Tasks 3, 9) |
| `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md` | Project files (Task 9) |
| `k8s/*.yaml`, `k8s/provider-costs.toml` | Secret/ConfigMap, replicas, image (Tasks 9, 10) |
| `docs/deployment/meilisearch-lab.mdx`, `docs/deployment/environment-variables.mdx`, `docs/concepts/usage.mdx`, `README.md`, `tinybird/pipes/tenant_usage.pipe`, `tinybird/README.md` | Docs (Task 11) |

---

### Task 1: Vendor the v2 events schema and emit the v2 `usage.recorded` event

**Files:**
- Modify: `contracts/vendor/lab/lab-events.schema.json` (replace the whole file)
- Modify: `crates/usage/src/lab.rs` (replace the whole file; today lines 1-356)

**Interfaces:**
- Consumes: `meili_ingest_usage::JobUsageInput` (`crates/usage/src/lib.rs:343-378`), `meili_ingest_plugin_sdk::{UsageUnits, JobStatus, StepResult}`.
- Produces: `meili_ingest_usage::lab::{LabEvent, LabEventData, UsageData, SkipReason, GLUTONY_LAB_NAMESPACE, OPERATION, PRODUCT, lab_event_id, lab_event_for_job, lab_units}`. Task 2 adds the job events on top of this file; Task 2's worker code calls `lab_events_for_job`.

- [ ] **Step 1: Replace the vendored schema with the v2 text**

Write `contracts/vendor/lab/lab-events.schema.json` exactly as follows (this is the text the Lab publishes at `contracts/lab-events.schema.json`; Task 3 diffs the two):

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "$id": "https://lab.meilisearch.com/contracts/lab-events.schema.json",
  "title": "Meilisearch Lab event",
  "description": "One event a product engine reports to the Lab at POST /internal/events, delivered in batches of at most 500 as {\"events\": [<event>, ...]} and acknowledged with 200 {\"accepted\": [<id>, ...]}. Owned by meilisearch/lab (platform contract v2, 2026-10-08); engines vendor a byte copy under contracts/vendor/lab/ and drift-check it in CI. instance_id is never in the body: the Lab takes it from the authenticated X-Lab-Instance-Id header.",
  "type": "object",
  "required": ["id", "type", "occurred_at", "account_id", "api_key_id", "product", "data"],
  "additionalProperties": false,
  "properties": {
    "id": {
      "$ref": "#/$defs/uuid",
      "description": "Idempotency key. UUIDv7 per request; UUIDv5 of a stable name for job-final events (job:{job_id}:usage, job:{job_id}:lifecycle)."
    },
    "type": { "enum": ["usage.recorded", "job.completed", "job.failed"] },
    "occurred_at": { "type": "string", "format": "date-time" },
    "account_id": { "$ref": "#/$defs/uuid", "description": "The Lab account the usage belongs to." },
    "api_key_id": { "type": ["string", "null"], "description": "The Lab API key id when known." },
    "product": { "enum": ["scrapix", "lumen", "glutony"] },
    "data": { "type": "object" }
  },
  "allOf": [
    {
      "if": { "properties": { "type": { "const": "usage.recorded" } }, "required": ["type"] },
      "then": { "properties": { "data": { "$ref": "#/$defs/usageData" } } }
    },
    {
      "if": { "properties": { "type": { "const": "job.completed" } }, "required": ["type"] },
      "then": { "properties": { "data": { "$ref": "#/$defs/jobCompletedData" } } }
    },
    {
      "if": { "properties": { "type": { "const": "job.failed" } }, "required": ["type"] },
      "then": { "properties": { "data": { "$ref": "#/$defs/jobFailedData" } } }
    },
    {
      "if": { "properties": { "type": { "const": "usage.recorded" }, "product": { "const": "scrapix" } }, "required": ["type", "product"] },
      "then": { "properties": { "data": { "properties": { "operation": { "enum": ["scrape", "map", "search", "parse", "ocr", "extract", "crawl"] } } } } }
    },
    {
      "if": { "properties": { "type": { "const": "usage.recorded" }, "product": { "const": "glutony" } }, "required": ["type", "product"] },
      "then": { "properties": { "data": { "properties": { "operation": { "const": "ingest" } } } } }
    },
    {
      "if": { "properties": { "type": { "const": "usage.recorded" }, "product": { "const": "lumen" } }, "required": ["type", "product"] },
      "then": { "properties": { "data": { "properties": { "operation": { "enum": ["chat", "embed", "rerank", "systemone", "gateway"] } } } } }
    },
    {
      "if": { "properties": { "product": { "const": "lumen" } }, "required": ["product"] },
      "then": { "properties": { "type": { "const": "usage.recorded" } } }
    }
  ],
  "$defs": {
    "uuid": {
      "type": "string",
      "pattern": "^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    },
    "units": {
      "type": "object",
      "description": "Raw units by name. The Lab prices the names in saas/config/pricing.yml and stores the rest.",
      "additionalProperties": { "type": "integer", "minimum": 0 }
    },
    "usageData": {
      "type": "object",
      "required": ["operation", "units"],
      "additionalProperties": false,
      "properties": {
        "operation": { "type": "string", "minLength": 1 },
        "units": { "$ref": "#/$defs/units" },
        "provider_cost_micro_usd": {
          "type": "integer",
          "minimum": 0,
          "maximum": 9223372036854775807,
          "default": 0,
          "description": "Real money the engine paid upstream (LLM tokens, OCR API), priced by markup."
        },
        "description": { "type": "string" },
        "job_id": { "type": "string" },
        "credits": {
          "type": "integer",
          "minimum": 0,
          "deprecated": true,
          "description": "Legacy hosted Scrapix only; used when units price to nothing. Removed next release."
        }
      }
    },
    "jobCompletedData": {
      "type": "object",
      "required": ["job_id", "index_uid", "pages_crawled", "documents_indexed", "duration_secs"],
      "additionalProperties": false,
      "properties": {
        "job_id": { "type": "string" },
        "index_uid": { "type": "string" },
        "pages_crawled": { "type": "integer", "minimum": 0 },
        "documents_indexed": { "type": "integer", "minimum": 0 },
        "duration_secs": { "type": "integer", "minimum": 0 }
      }
    },
    "jobFailedData": {
      "type": "object",
      "required": ["job_id", "error_message", "pages_crawled"],
      "additionalProperties": false,
      "properties": {
        "job_id": { "type": "string" },
        "error_message": { "type": "string" },
        "pages_crawled": { "type": "integer", "minimum": 0 }
      }
    }
  }
}
```

- [ ] **Step 2: Write the failing tests (new test module of `crates/usage/src/lab.rs`)**

Replace the `#[cfg(test)] mod tests` at the bottom of `crates/usage/src/lab.rs` with the module below. It compiles only once Step 4 lands (new names), so this is the red step.

```rust
#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use meili_ingest_plugin_sdk::{JobStatus, StepResult};

    use super::*;

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";

    fn step(id: &str, docs: usize, duration_ms: u64, usage: UsageUnits) -> StepResult {
        StepResult {
            step_id: id.into(),
            plugin: "p".into(),
            status: JobStatus::Succeeded,
            document_count: docs,
            branches: 1,
            error: None,
            duration_ms,
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
                step("extract", 3, 1_200, UsageUnits::pages(9)),
                step(
                    "enrich",
                    12,
                    2_100,
                    UsageUnits {
                        cost_micro_usd: 210,
                        unpriced_calls: 1,
                        audio_seconds: 2.4,
                        ..UsageUnits::llm(1_000, 100)
                    },
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
        jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .unwrap()
    }

    fn errors_of(v: &serde_json::Value) -> Vec<String> {
        validator().iter_errors(v).map(|e| e.to_string()).collect()
    }

    #[test]
    fn a_lab_job_becomes_one_valid_v2_usage_event() {
        let event = lab_event_for_job(&input()).unwrap();
        let v = serde_json::to_value(&event).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "usage.recorded");
        assert_eq!(v["product"], "glutony");
        assert_eq!(v["account_id"], ACCOUNT);
        assert!(v["api_key_id"].is_null());
        assert_eq!(v["occurred_at"], "2026-10-01T10:00:09.000Z");
        assert_eq!(v["data"]["operation"], "ingest");
        assert_eq!(v["data"]["job_id"], "11111111-2222-3333-4444-555555555555");
        // One unpriced call: the pass-through cost is unknown, so it is reported as 0.
        assert_eq!(v["data"]["provider_cost_micro_usd"], 0);
        assert!(
            v["data"]["description"]
                .as_str()
                .unwrap()
                .contains("provider cost incomplete")
        );
        let units = &v["data"]["units"];
        assert_eq!(units["documents"], 12, "the final step's documents");
        assert_eq!(units["bytes_in"], 5_000, "the job's payload size");
        assert_eq!(units["step_seconds"], 4, "3.3 s of steps, rounded up");
        assert_eq!(units["llm_tokens_in"], 1_000);
        assert_eq!(units["llm_tokens_out"], 100);
        assert_eq!(units["audio_seconds"], 3, "2.4 s, rounded up");
        assert_eq!(units["ocr_pages"], 0, "no plugin reports OCR pages yet");
        // Extra units: stored by the Lab, ignored by pricing (spec §4.3).
        assert_eq!(units["pages"], 9);
        assert_eq!(units["llm_requests"], 1);
        for (name, value) in units.as_object().unwrap() {
            assert!(value.is_u64(), "{name} must be a non-negative integer, got {value}");
        }
        // No field the schema does not know (status and cost_complete live in description).
        let keys: Vec<&String> = v["data"].as_object().unwrap().keys().collect();
        assert_eq!(
            keys,
            ["description", "job_id", "operation", "provider_cost_micro_usd", "units"]
        );
    }

    #[test]
    fn complete_cost_is_passed_through() {
        let mut i = input();
        i.steps[1].usage.unpriced_calls = 0;
        let event = lab_event_for_job(&i).unwrap();
        let LabEventData::Usage(data) = &event.data else {
            panic!("usage event expected");
        };
        assert_eq!(data.provider_cost_micro_usd, 210);
        assert!(!data.description.contains("incomplete"));
    }

    #[test]
    fn the_id_is_deterministic() {
        let a = lab_event_for_job(&input()).unwrap();
        let b = lab_event_for_job(&input()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.id, lab_event_id(input().job_id));
        assert_eq!(a.id.get_version_num(), 5);
        // Golden value: changing GLUTONY_LAB_NAMESPACE or the name re-ids every
        // redelivered event and the Lab would count them again.
        assert_eq!(
            lab_event_id("11111111-2222-3333-4444-555555555555".parse().unwrap()),
            "eb3df490-5f14-59b7-9cad-c97f959127ae"
                .parse::<Uuid>()
                .unwrap()
        );
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
    fn only_the_canonical_uuid_form_is_a_lab_account() {
        let upper = ACCOUNT.to_uppercase();
        let simple = ACCOUNT.replace('-', "");
        let braced = format!("{{{ACCOUNT}}}");
        let urn = format!("urn:uuid:{ACCOUNT}");
        for form in [upper, simple, braced, urn] {
            let mut i = input();
            i.tenant_id = form.clone();
            assert_eq!(
                lab_event_for_job(&i),
                Err(SkipReason::NotAUuid),
                "{form} must be skipped"
            );
        }
        assert_eq!(lab_event_for_job(&input()).unwrap().account_id, ACCOUNT);
    }

    #[test]
    fn the_cost_is_capped_at_i64_max_for_a_signed_ledger() {
        let mut i = input();
        i.steps = vec![step(
            "enrich",
            1,
            1,
            UsageUnits {
                cost_micro_usd: u64::MAX,
                ..UsageUnits::default()
            },
        )];
        let v = serde_json::to_value(lab_event_for_job(&i).unwrap()).unwrap();
        assert_eq!(v["data"]["provider_cost_micro_usd"], i64::MAX);
        assert!(errors_of(&v).is_empty(), "{:?}", errors_of(&v));
        let mut over = v.clone();
        over["data"]["provider_cost_micro_usd"] = serde_json::json!(u64::MAX);
        assert!(!validator().is_valid(&over));
    }

    #[test]
    fn non_finite_audio_seconds_still_make_a_valid_event() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -3.0] {
            let mut i = input();
            i.steps = vec![step(
                "transcribe",
                1,
                1,
                UsageUnits {
                    audio_seconds: bad,
                    ..UsageUnits::default()
                },
            )];
            let v = serde_json::to_value(lab_event_for_job(&i).unwrap()).unwrap();
            assert_eq!(v["data"]["units"]["audio_seconds"], 0, "{bad}");
            assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        }
    }

    #[test]
    fn failed_jobs_are_reported_too() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        let v = serde_json::to_value(lab_event_for_job(&failed).unwrap()).unwrap();
        assert!(validator().is_valid(&v));
        assert!(v["data"]["description"].as_str().unwrap().contains("failed"));
    }

    #[test]
    fn the_schema_is_really_enforced() {
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["surprise"] = 1.into();
        assert!(!validator().is_valid(&v));
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["units"]["documents"] = (-1).into();
        assert!(!validator().is_valid(&v));
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["operation"] = "crawl".into();
        assert!(!validator().is_valid(&v), "glutony only sends ingest");
    }
}
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-usage lab::`
Expected: compile error (`LabEventData`, `lab_units` do not exist yet).

- [ ] **Step 4: Write the implementation (everything above the test module in `crates/usage/src/lab.rs`)**

```rust
//! Lab events (platform contract v2, spec §4): one `usage.recorded` and one job
//! lifecycle event per finished job, in the Meilisearch Lab envelope. Pure: same
//! input, byte-identical events, same ids.
//!
//! Credits are the Lab's business (decision B): this module reports raw units and
//! the provider cost glutony paid, nothing priced.

use std::collections::BTreeMap;

use chrono::SecondsFormat;
use meili_ingest_plugin_sdk::{JobStatus, UsageUnits};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::JobUsageInput;

/// Namespace of glutony's deterministic Lab event ids. Never change it: a new value
/// would give redelivered events new ids and bill them twice.
pub const GLUTONY_LAB_NAMESPACE: Uuid = Uuid::from_u128(0x6c8f_4a1e_2b7d_4c3a_9e51_7f0d_2a6b_8c14);
/// The one operation glutony reports (spec §4.3).
pub const OPERATION: &str = "ingest";
/// The `product` of every glutony event.
pub const PRODUCT: &str = "glutony";

/// One Lab event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabEvent {
    /// Idempotency key: [`lab_event_id`] or [`lab_job_event_id`] of the job.
    pub id: Uuid,
    /// `usage.recorded`, `job.completed` or `job.failed`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The job's finish time, RFC 3339 with milliseconds.
    pub occurred_at: String,
    /// The Lab account: the job's tenant id, a canonical UUID.
    pub account_id: String,
    /// Glutony has no Lab key ids.
    pub api_key_id: Option<String>,
    /// Always `glutony`.
    pub product: String,
    /// Per-type payload.
    pub data: LabEventData,
}

/// `data` of a Lab event, by type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LabEventData {
    /// `usage.recorded`.
    Usage(UsageData),
    /// `job.completed`.
    JobCompleted(JobCompletedData),
    /// `job.failed`.
    JobFailed(JobFailedData),
}

/// `data` of a `usage.recorded` event (spec §4.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageData {
    /// Always [`OPERATION`].
    pub operation: String,
    /// Raw units by name, sorted so the serialized event is byte-stable.
    pub units: BTreeMap<String, u64>,
    /// What glutony paid providers, or 0 when a call could not be priced.
    pub provider_cost_micro_usd: u64,
    /// Human label for the ledger: pipeline, status, documents, cost completeness.
    pub description: String,
    /// The job id.
    pub job_id: String,
}

/// `data` of a `job.completed` event (spec §4.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobCompletedData {
    /// The job id.
    pub job_id: String,
    /// The pipeline uid (glutony has no crawl index).
    pub index_uid: String,
    /// Always 0 for glutony.
    pub pages_crawled: u64,
    /// The final step's documents.
    pub documents_indexed: u64,
    /// Wall time of the job, whole seconds.
    pub duration_secs: u64,
}

/// `data` of a `job.failed` event (spec §4.4). Cancelled jobs are reported here too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobFailedData {
    /// The job id.
    pub job_id: String,
    /// The job's error, or its status when it carries none.
    pub error_message: String,
    /// Always 0 for glutony.
    pub pages_crawled: u64,
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
    Uuid::new_v5(
        &GLUTONY_LAB_NAMESPACE,
        format!("job:{job_id}:usage").as_bytes(),
    )
}

/// The Lab account a tenant id names, or why it names none. Only the canonical
/// lower-case hyphenated form counts: other spellings of the same UUID (upper-case,
/// simple, braced, URN) would bill a different string.
fn lab_account(tenant_id: &str) -> Result<Uuid, SkipReason> {
    if tenant_id.is_empty() {
        return Err(SkipReason::NoTenant);
    }
    Uuid::parse_str(tenant_id)
        .ok()
        .filter(|a| a.hyphenated().to_string() == tenant_id)
        .ok_or(SkipReason::NotAUuid)
}

fn totals(input: &JobUsageInput) -> UsageUnits {
    let mut totals = UsageUnits::none();
    for step in &input.steps {
        totals.merge(step.usage);
    }
    totals
}

fn documents_out(input: &JobUsageInput) -> u64 {
    input
        .steps
        .last()
        .map(|s| s.document_count as u64)
        .unwrap_or(0)
}

/// The v2 units of a job (spec §4.3), from the step totals. Every value is a
/// non-negative integer; fractional seconds round up so a started second is billed.
/// `ocr_pages` stays 0 until a plugin reports OCR'd pages: the PDF extractor's `pages`
/// are not OCR work and are sent as an extra, unpriced unit instead.
pub fn lab_units(input: &JobUsageInput, totals: &UsageUnits) -> BTreeMap<String, u64> {
    let step_ms = input
        .steps
        .iter()
        .fold(0u64, |acc, s| acc.saturating_add(s.duration_ms));
    let audio_seconds = if totals.audio_seconds.is_finite() && totals.audio_seconds > 0.0 {
        totals.audio_seconds.ceil() as u64
    } else {
        0
    };
    BTreeMap::from([
        ("documents".to_string(), documents_out(input)),
        ("bytes_in".to_string(), input.input_bytes),
        ("step_seconds".to_string(), step_ms.div_ceil(1000)),
        ("llm_tokens_in".to_string(), totals.llm_input_tokens),
        ("llm_tokens_out".to_string(), totals.llm_output_tokens),
        ("audio_seconds".to_string(), audio_seconds),
        ("ocr_pages".to_string(), 0),
        // Extra units the Lab stores but does not price (spec §4.3).
        ("pages".to_string(), totals.pages),
        ("images".to_string(), totals.images),
        ("llm_requests".to_string(), totals.llm_requests),
        ("external_requests".to_string(), totals.external_requests),
    ])
}

fn envelope(input: &JobUsageInput, id: Uuid, kind: &str, data: LabEventData) -> Result<LabEvent, SkipReason> {
    let account = lab_account(&input.tenant_id)?;
    Ok(LabEvent {
        id,
        kind: kind.to_string(),
        occurred_at: input
            .finished_at
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        account_id: account.hyphenated().to_string(),
        api_key_id: None,
        product: PRODUCT.to_string(),
        data,
    })
}

/// Build the job's `usage.recorded` event, or say why there is none.
pub fn lab_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    let totals = totals(input);
    let complete = totals.unpriced_calls == 0;
    // The Lab's ledger is a signed bigint; the schema caps the value too. An
    // incomplete cost is reported as 0 rather than as a number that is too small.
    let provider_cost_micro_usd = if complete {
        totals.cost_micro_usd.min(i64::MAX as u64)
    } else {
        0
    };
    let units = lab_units(input, &totals);
    let description = format!(
        "Job {} ({}, {}, {} documents{})",
        input.job_id,
        input.pipeline_uid,
        input.status.as_str(),
        documents_out(input),
        if complete { "" } else { ", provider cost incomplete" }
    );
    envelope(
        input,
        lab_event_id(input.job_id),
        "usage.recorded",
        LabEventData::Usage(UsageData {
            operation: OPERATION.to_string(),
            units,
            provider_cost_micro_usd,
            description,
            job_id: input.job_id.to_string(),
        }),
    )
}
```

`JobStatus` is only named by Task 2: write the import as `use meili_ingest_plugin_sdk::UsageUnits;` now (clippy runs with `-D warnings`, so an unused import fails the build) and change it to `use meili_ingest_plugin_sdk::{JobStatus, UsageUnits};` in Task 2 Step 3.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `cargo test -p meili-ingest-usage lab::`
Expected: 9 tests pass. Then `cargo clippy -p meili-ingest-usage --all-targets -- -D warnings` and `cargo fmt --all`.

- [ ] **Step 6: Commit**

```bash
git add contracts/vendor/lab/lab-events.schema.json crates/usage/src/lab.rs
git commit -m "feat(usage): vendor the Lab events schema v2 and report raw units"
```

---

### Task 2: Job lifecycle events and the worker posts every event of a job

**Files:**
- Modify: `crates/usage/src/lab.rs` (add below `lab_event_for_job`; add tests)
- Modify: `crates/worker/src/activity.rs:163-216` (`report_usage`, `post_lab_event`) and its tests at lines 976-1046
- Modify: `docs/deployment/meilisearch-lab.mdx:52-72` (event example; the full rewrite is Task 11)

**Interfaces:**
- Consumes: Task 1's `LabEvent`, `LabEventData`, `envelope`, `documents_out`, `lab_account`.
- Produces: `lab_job_event_id(job_id) -> Uuid`, `lab_job_event_for_job(&JobUsageInput) -> Result<LabEvent, SkipReason>`, `lab_events_for_job(&JobUsageInput) -> Result<Vec<LabEvent>, SkipReason>` (usage first, then lifecycle). The worker's `StepActivities::post_lab_events` posts that vector.

- [ ] **Step 1: Write the failing tests (append inside `mod tests` of `crates/usage/src/lab.rs`)**

```rust
    #[test]
    fn a_succeeded_job_also_emits_job_completed() {
        let events = lab_events_for_job(&input()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "usage.recorded");
        let done = &events[1];
        assert_eq!(done.kind, "job.completed");
        assert_eq!(done.id, lab_job_event_id(input().job_id));
        assert_ne!(done.id, events[0].id);
        assert_eq!(done.id.get_version_num(), 5);
        let v = serde_json::to_value(done).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["data"]["index_uid"], "builtin.pdf");
        assert_eq!(v["data"]["documents_indexed"], 12);
        assert_eq!(v["data"]["pages_crawled"], 0);
        assert_eq!(v["data"]["duration_secs"], 9);
        assert_eq!(v["data"]["job_id"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn failed_and_cancelled_jobs_emit_job_failed() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        failed.error = Some("pdf extractor exploded".into());
        let v = serde_json::to_value(lab_job_event_for_job(&failed).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "job.failed");
        assert_eq!(v["data"]["error_message"], "pdf extractor exploded");
        assert_eq!(v["data"]["pages_crawled"], 0);

        let mut cancelled = input();
        cancelled.status = JobStatus::Cancelled;
        let v = serde_json::to_value(lab_job_event_for_job(&cancelled).unwrap()).unwrap();
        assert!(validator().is_valid(&v));
        assert_eq!(v["type"], "job.failed");
        assert_eq!(v["data"]["error_message"], "cancelled");
    }

    #[test]
    fn job_events_follow_the_same_skip_rules() {
        let mut cloud = input();
        cloud.tenant_id = "hackersearch".into();
        assert_eq!(lab_events_for_job(&cloud), Err(SkipReason::NotAUuid));
        let mut standalone = input();
        standalone.tenant_id = String::new();
        assert_eq!(lab_job_event_for_job(&standalone), Err(SkipReason::NoTenant));
    }

    #[test]
    fn the_lifecycle_id_golden_value() {
        // Python: uuid.uuid5(GLUTONY_LAB_NAMESPACE, "job:11111111-2222-3333-4444-555555555555:lifecycle")
        let id = lab_job_event_id("11111111-2222-3333-4444-555555555555".parse().unwrap());
        assert_eq!(id.get_version_num(), 5);
        assert_eq!(
            id,
            Uuid::new_v5(
                &GLUTONY_LAB_NAMESPACE,
                b"job:11111111-2222-3333-4444-555555555555:lifecycle"
            )
        );
    }
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p meili-ingest-usage lab::`
Expected: compile error (`lab_events_for_job`, `lab_job_event_for_job`, `lab_job_event_id` missing).

- [ ] **Step 3: Implement (append after `lab_event_for_job` in `crates/usage/src/lab.rs`)**

```rust
/// The deterministic id of a job's lifecycle event (`job.completed` / `job.failed`).
pub fn lab_job_event_id(job_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &GLUTONY_LAB_NAMESPACE,
        format!("job:{job_id}:lifecycle").as_bytes(),
    )
}

/// Build the job's lifecycle event (spec §4.4): `job.completed` for a succeeded job,
/// `job.failed` otherwise (failed and cancelled; the usage activity only runs on
/// terminal statuses).
pub fn lab_job_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    let duration_secs = (input.finished_at - input.started_at)
        .num_seconds()
        .max(0) as u64;
    let job_id = input.job_id.to_string();
    let (kind, data) = match input.status {
        JobStatus::Succeeded => (
            "job.completed",
            LabEventData::JobCompleted(JobCompletedData {
                job_id,
                index_uid: input.pipeline_uid.clone(),
                pages_crawled: 0,
                documents_indexed: documents_out(input),
                duration_secs,
            }),
        ),
        status => (
            "job.failed",
            LabEventData::JobFailed(JobFailedData {
                job_id,
                error_message: input
                    .error
                    .clone()
                    .filter(|e| !e.trim().is_empty())
                    .unwrap_or_else(|| status.as_str().to_string()),
                pages_crawled: 0,
            }),
        ),
    };
    envelope(input, lab_job_event_id(input.job_id), kind, data)
}

/// Every Lab event of a finished job: the usage event, then the lifecycle event.
pub fn lab_events_for_job(input: &JobUsageInput) -> Result<Vec<LabEvent>, SkipReason> {
    Ok(vec![lab_event_for_job(input)?, lab_job_event_for_job(input)?])
}
```

- [ ] **Step 4: Run the usage tests**

Run: `cargo test -p meili-ingest-usage lab::`
Expected: 13 tests pass.

- [ ] **Step 5: Write the failing worker test**

In `crates/worker/src/activity.rs` replace the test `a_lab_job_posts_its_event_to_the_outbox` (lines 1006-1023) with:

```rust
    #[tokio::test]
    async fn a_lab_job_posts_its_usage_and_lifecycle_events_to_the_outbox() {
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
        let events = body["events"].as_array().unwrap();
        assert_eq!(events.len(), 2, "one batch carries both events of the job");
        assert_eq!(
            events[0]["id"],
            meili_ingest_usage::lab::lab_event_id(input.job_id).to_string()
        );
        assert_eq!(events[0]["type"], "usage.recorded");
        assert_eq!(
            events[1]["id"],
            meili_ingest_usage::lab::lab_job_event_id(input.job_id).to_string()
        );
        assert_eq!(events[1]["type"], "job.completed");
    }
```

- [ ] **Step 6: Run it to verify it fails**

Run: `cargo test -p meili-ingest-worker a_lab_job_posts_its_usage_and_lifecycle_events_to_the_outbox`
Expected: FAIL on `events.len() == 2` (today one event is posted).

- [ ] **Step 7: Implement in `crates/worker/src/activity.rs`**

At line 164 change `self.post_lab_event(input).await?;` to `self.post_lab_events(input).await?;` and replace `post_lab_event` (lines 184-216) with:

```rust
    async fn post_lab_events(&self, input: &JobUsageInput) -> Result<(), UsageReportError> {
        let events = match meili_ingest_usage::lab::lab_events_for_job(input) {
            Ok(e) => e,
            Err(reason) => {
                tracing::debug!(
                    job_id = %input.job_id,
                    reason = reason.as_str(),
                    "no Lab events for this job"
                );
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
            .json(&serde_json::json!({ "events": events }))
            .send()
            .await
            .map_err(|e| UsageReportError::Retryable(format!("control plane unreachable: {e}")))?;
        if !resp.status().is_success() {
            return Err(UsageReportError::Retryable(format!(
                "control plane refused the lab events: {}",
                resp.status()
            )));
        }
        tracing::info!(job_id = %input.job_id, events = events.len(), "lab events recorded");
        Ok(())
    }
```

Update the doc comment on `with_lab_events` (line 144) to "Post each finished Lab job's usage and lifecycle events to the control plane's outbox."

- [ ] **Step 8: Run the worker tests and the workspace build**

Run: `cargo test -p meili-ingest-worker lab && cargo clippy --workspace --all-targets -- -D warnings`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add crates/usage/src/lab.rs crates/worker/src/activity.rs
git commit -m "feat(worker): report job.completed and job.failed Lab events"
```

---

### Task 3: Enable the Lab contract drift check in CI

**Files:**
- Modify: `.github/workflows/ci.yml:96-110` (the `lab-contract-drift` job)
- Create: `contracts/vendor/lab/README.md`

**Interfaces:**
- Consumes: the vendored file from Task 1. The Lab publishes `contracts/lab-events.schema.json` on `meilisearch/lab` `main` (private repo, hence the token).
- Produces: a required CI job that passes when the bytes match and is skipped with a notice (exit 0) on forks or when `LAB_REPO_TOKEN` is absent.

- [ ] **Step 1: Replace the job in `.github/workflows/ci.yml`**

Replace lines 96-110 with:

```yaml
  lab-contract-drift:
    name: Lab contract drift
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Compare the vendored schema with the Lab's
        # meilisearch/lab is private: LAB_REPO_TOKEN is a fine-grained PAT with
        # read access to its contents. Without it (forks, missing secret) the check
        # is skipped loudly rather than failing every PR.
        env:
          LAB_REPO_TOKEN: ${{ secrets.LAB_REPO_TOKEN }}
          LAB_SCHEMA_URL: https://raw.githubusercontent.com/meilisearch/lab/main/contracts/lab-events.schema.json
        run: |
          if [ -z "$LAB_REPO_TOKEN" ]; then
            echo "::notice title=Lab contract drift::LAB_REPO_TOKEN is not set; skipping the drift check"
            exit 0
          fi
          curl -fsSL -H "Authorization: Bearer $LAB_REPO_TOKEN" "$LAB_SCHEMA_URL" -o /tmp/lab-events.schema.json
          diff -u /tmp/lab-events.schema.json contracts/vendor/lab/lab-events.schema.json
```

- [ ] **Step 2: Write `contracts/vendor/lab/README.md`**

```markdown
# Vendored Lab contracts

`lab-events.schema.json` is a byte copy of `contracts/lab-events.schema.json` in
`meilisearch/lab` (platform contract v2, 2026-10-08). The Lab owns it.

- Never edit it here. Change it in the Lab, then copy the file:
  `curl -fsSL -H "Authorization: Bearer $LAB_REPO_TOKEN" https://raw.githubusercontent.com/meilisearch/lab/main/contracts/lab-events.schema.json -o contracts/vendor/lab/lab-events.schema.json`
- `crates/usage/src/lab.rs` validates every event it builds against this copy.
- CI (`lab-contract-drift` in `.github/workflows/ci.yml`) diffs the copy against the
  Lab's `main` and fails on drift. It needs the `LAB_REPO_TOKEN` repository secret
  (read access to `meilisearch/lab`); without it the job is skipped with a notice.
```

- [ ] **Step 3: Validate the workflow file parses and the local diff is byte-exact when the Lab file is present**

Run: `ruby -ryaml -e 'YAML.load_file(".github/workflows/ci.yml"); puts "ok"'`
Expected: `ok`.
Run (only if the Lab checkout is available): `diff -u /Users/quentindequelen/Projects/Meilisearch/_side_projects/meilisearch-lab/contracts/lab-events.schema.json contracts/vendor/lab/lab-events.schema.json`
Expected: no output, or "No such file" if the Lab has not published yet (then the CI job is the check, once the Lab lands its copy from the same text as Task 1 Step 1).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows/ci.yml contracts/vendor/lab/README.md
git commit -m "ci: check the vendored Lab events schema against meilisearch/lab"
```

---

### Task 4: The `meili-ingest-lab` crate (credentials, v2 signature, identity, account lookup)

**Files:**
- Modify: `Cargo.toml` (workspace `members` after `"crates/usage"`, and `[workspace.dependencies]` after `meili-ingest-usage`)
- Create: `crates/lab/Cargo.toml`, `crates/lab/src/lib.rs`

**Interfaces:**
- Consumes: nothing from the workspace.
- Produces (used by Tasks 5 and 6):
  - `pub fn validate_lab_url(url: &str) -> anyhow::Result<String>` (https, or http to loopback/private; trailing slash trimmed)
  - `pub struct LabCredentials` with `new(url, instance_id, secret) -> anyhow::Result<Self>`, `from_values(Option<String>, Option<String>, Option<String>) -> anyhow::Result<Option<Self>>`, `from_env()`, `url()`, `instance_id()`, `endpoint(path) -> String`, `authorize(RequestBuilder) -> RequestBuilder`, `sign_batch(RequestBuilder, timestamp: u64, body: &[u8]) -> RequestBuilder`
  - `pub fn sign_batch(secret: &[u8], timestamp: u64, body: &[u8]) -> String`, `pub fn unix_now() -> u64`, `pub fn is_lab_account(id: &str) -> bool`
  - `pub enum InstanceKind { Hosted }` (the only kind: there is no customer-run engine), `pub struct InstanceInfo { instance_id, kind, product, region: Option<String>, lab_url: Option<String> }`
  - `pub struct AccountLookup { active, account_id, tier, credits: Option<Credits>, cache_ttl }` with `can_spend(&self) -> bool`; `pub struct Credits { balance: i64 }`
  - `pub enum LabError { Transport(String), Unauthorized, Status(u16), Malformed(String) }`
  - `pub async fn fetch_instance_info(&reqwest::Client, &LabCredentials) -> Result<InstanceInfo, LabError>`, `pub async fn fetch_account(&reqwest::Client, &LabCredentials, account_id: &str) -> Result<AccountLookup, LabError>`
  - header name consts `H_INSTANCE_ID = "x-lab-instance-id"`, `H_TIMESTAMP = "x-lab-timestamp"`, `H_SIGNATURE = "x-lab-signature"`

- [ ] **Step 1: Register the crate**

In `Cargo.toml` add `"crates/lab",` after `"crates/usage",` in `members`, and `meili-ingest-lab = { path = "crates/lab" }` after the `meili-ingest-usage` line in `[workspace.dependencies]`.

Create `crates/lab/Cargo.toml`:

```toml
[package]
name = "meili-ingest-lab"
description = "Meilisearch Lab client shared by the gateway and the control plane: per-instance credentials, event-batch signatures, instance identity and account lookups (platform contract v2)."
version.workspace = true
edition.workspace = true
license.workspace = true
repository.workspace = true
rust-version.workspace = true

[dependencies]
reqwest.workspace = true
serde.workspace = true
serde_json.workspace = true
hmac.workspace = true
sha2.workspace = true
hex.workspace = true
url.workspace = true
anyhow.workspace = true
thiserror.workspace = true
tracing.workspace = true

[dev-dependencies]
tokio = { workspace = true, features = ["macros", "rt-multi-thread"] }
wiremock.workspace = true
```

- [ ] **Step 2: Write the failing tests (`crates/lab/src/lib.rs`, bottom)**

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

    #[test]
    fn the_batch_signature_matches_a_known_vector() {
        // printf '1700000000.{"events":[]}' | openssl dgst -sha256 -hmac secret
        assert_eq!(
            sign_batch(b"secret", 1_700_000_000, br#"{"events":[]}"#),
            "sha256=3947de27ec923573170fccda604ddfb25583ff98dc51e96cca2e11c59545026a"
        );
        // Not the v1 signature over the bare body.
        assert_ne!(
            sign_batch(b"secret", 1_700_000_000, br#"{"events":[]}"#),
            "sha256=a642b59553c93e227ec0f2f38910fbf71231a2197c00899833c00478cec86f34"
        );
    }

    #[test]
    fn credentials_go_together_and_redact_the_secret() {
        assert!(LabCredentials::from_values(None, None, None).unwrap().is_none());
        for (u, i, s) in [
            (Some("https://lab.example"), None, None),
            (Some("https://lab.example"), Some(ID), None),
            (None, Some(ID), Some("s")),
            (Some("https://lab.example"), Some(" "), Some("s")),
            (Some("https://lab.example"), Some(ID), Some("")),
            (Some("http://lab.example"), Some(ID), Some("s")),
        ] {
            assert!(
                LabCredentials::from_values(
                    u.map(String::from),
                    i.map(String::from),
                    s.map(String::from)
                )
                .is_err(),
                "{u:?} {i:?} {s:?}"
            );
        }
        let c = LabCredentials::new("https://lab.example/", ID, "topsecret").unwrap();
        assert_eq!(c.url(), "https://lab.example");
        assert_eq!(c.endpoint("/internal/events"), "https://lab.example/internal/events");
        assert!(!format!("{c:?}").contains("topsecret"));
        assert!(LabCredentials::new("http://127.0.0.1:8091", ID, "s").is_ok());
    }

    #[test]
    fn only_canonical_lowercase_uuids_are_lab_accounts() {
        assert!(is_lab_account(ID));
        assert!(!is_lab_account(&ID.to_uppercase()));
        assert!(!is_lab_account(&ID.replace('-', "")));
        assert!(!is_lab_account("hackersearch"));
        assert!(!is_lab_account(""));
        assert!(!is_lab_account("../internal/ping"));
    }

    #[tokio::test]
    async fn service_calls_carry_the_bearer_secret_and_the_instance_id() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .and(header("authorization", "Bearer topsecret"))
            .and(header("x-lab-instance-id", ID))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instance_id": ID, "kind": "hosted", "product": "glutony",
                "region": "eu-west-1", "lab_url": lab.uri()
            })))
            .expect(1)
            .mount(&lab)
            .await;
        let creds = LabCredentials::new(&lab.uri(), ID, "topsecret").unwrap();
        let info = fetch_instance_info(&reqwest::Client::new(), &creds).await.unwrap();
        assert_eq!(info.kind, InstanceKind::Hosted);
        assert_eq!(info.region.as_deref(), Some("eu-west-1"));
        assert_eq!(info.product, "glutony");
    }

    #[tokio::test]
    async fn account_lookups_parse_and_errors_are_classified() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": ID, "tier": "pro",
                "credits": {"balance": 42}, "cache_ttl": 30
            })))
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path("/internal/accounts/00000000-0000-0000-0000-000000000000"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"active": false})))
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path("/internal/accounts/11111111-1111-1111-1111-111111111111"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&lab)
            .await;
        let creds = LabCredentials::new(&lab.uri(), ID, "s").unwrap();
        let http = reqwest::Client::new();
        let ok = fetch_account(&http, &creds, ID).await.unwrap();
        assert!(ok.can_spend());
        assert_eq!(ok.credits.unwrap().balance, 42);
        let inactive = fetch_account(&http, &creds, "00000000-0000-0000-0000-000000000000")
            .await
            .unwrap();
        assert!(!inactive.can_spend());
        assert_eq!(
            fetch_account(&http, &creds, "11111111-1111-1111-1111-111111111111").await,
            Err(LabError::Unauthorized)
        );
        assert!(matches!(
            fetch_account(&http, &creds, "not-an-account").await,
            Err(LabError::Malformed(_))
        ));
        let dead = LabCredentials::new("http://127.0.0.1:1", ID, "s").unwrap();
        assert!(matches!(
            fetch_account(&http, &dead, ID).await,
            Err(LabError::Transport(_))
        ));
    }

    #[test]
    fn zero_balance_cannot_spend() {
        let a = AccountLookup {
            active: true,
            credits: Some(Credits { balance: 0 }),
            ..Default::default()
        };
        assert!(!a.can_spend());
        let b = AccountLookup {
            active: true,
            credits: None,
            ..Default::default()
        };
        assert!(!b.can_spend());
    }
}
```

- [ ] **Step 3: Run to verify failure**

Run: `cargo test -p meili-ingest-lab`
Expected: compile errors (nothing is defined yet).

- [ ] **Step 4: Implement `crates/lab/src/lib.rs` (above the tests)**

```rust
//! Meilisearch Lab client (platform contract v2, spec §3).
//!
//! What the gateway and the control plane share about the Lab: the per-instance
//! credentials (`LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET`), the headers of
//! service calls and signed event batches, `GET /internal/instances/me` (are these
//! credentials good, and which product and region is this?) and
//! `GET /internal/accounts/{id}` (can this account still spend?). No business logic
//! lives here; the callers decide what to do.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Header naming the reporting deployment on every engine-to-Lab request.
pub const H_INSTANCE_ID: &str = "x-lab-instance-id";
/// Header carrying the unix timestamp an event batch was signed with.
pub const H_TIMESTAMP: &str = "x-lab-timestamp";
/// Header carrying `sha256=<hex HMAC>` of an event batch.
pub const H_SIGNATURE: &str = "x-lab-signature";

/// Accept a Lab base URL: `https`, or `http` when the host is loopback or private.
/// Returns it without a trailing slash.
pub fn validate_lab_url(url: &str) -> anyhow::Result<String> {
    let parsed = url::Url::parse(url).map_err(|e| anyhow::anyhow!("LAB_URL is not a URL: {e}"))?;
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
    Ok(url.trim_end_matches('/').to_string())
}

/// `LAB_URL` + `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET`.
#[derive(Clone)]
pub struct LabCredentials {
    url: String,
    instance_id: String,
    secret: String,
}

impl std::fmt::Debug for LabCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabCredentials")
            .field("url", &self.url)
            .field("instance_id", &self.instance_id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl LabCredentials {
    /// Validate and build. The id and secret are trimmed and must be non-empty.
    pub fn new(url: &str, instance_id: &str, secret: &str) -> anyhow::Result<Self> {
        let url = validate_lab_url(url)?;
        let instance_id = instance_id.trim();
        if instance_id.is_empty() {
            anyhow::bail!("LAB_INSTANCE_ID is empty");
        }
        let secret = secret.trim();
        if secret.is_empty() {
            anyhow::bail!("LAB_INSTANCE_SECRET is empty");
        }
        Ok(Self {
            url,
            instance_id: instance_id.to_string(),
            secret: secret.to_string(),
        })
    }

    /// All three values or none.
    pub fn from_values(
        url: Option<String>,
        instance_id: Option<String>,
        secret: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        match (url, instance_id, secret) {
            (None, None, None) => Ok(None),
            (Some(u), Some(i), Some(s)) => Self::new(&u, &i, &s).map(Some),
            (u, i, s) => anyhow::bail!(
                "LAB_URL, LAB_INSTANCE_ID and LAB_INSTANCE_SECRET go together \
                 (set: LAB_URL={}, LAB_INSTANCE_ID={}, LAB_INSTANCE_SECRET={})",
                u.is_some(),
                i.is_some(),
                s.is_some()
            ),
        }
    }

    /// Read `LAB_URL`, `LAB_INSTANCE_ID` and `LAB_INSTANCE_SECRET` (blank counts as unset).
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(get("LAB_URL"), get("LAB_INSTANCE_ID"), get("LAB_INSTANCE_SECRET"))
    }

    /// Lab base URL, without trailing slash.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// This deployment's id in the Lab.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// `{url}{path}`.
    pub fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.url)
    }

    /// Service-call headers (spec §3.3): `Authorization: Bearer <secret>` and the instance id.
    pub fn authorize(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req.bearer_auth(&self.secret)
            .header(H_INSTANCE_ID, &self.instance_id)
    }

    /// Event-batch headers (spec §3.3): instance id, timestamp and signature over
    /// `"{timestamp}.{body}"`.
    pub fn sign_batch(
        &self,
        req: reqwest::RequestBuilder,
        timestamp: u64,
        body: &[u8],
    ) -> reqwest::RequestBuilder {
        req.header(H_INSTANCE_ID, &self.instance_id)
            .header(H_TIMESTAMP, timestamp.to_string())
            .header(H_SIGNATURE, sign_batch(self.secret.as_bytes(), timestamp, body))
    }
}

/// `sha256=<hex HMAC-SHA256(secret, "{timestamp}.{body}")>`.
pub fn sign_batch(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
    // HMAC accepts keys of any length; new_from_slice cannot fail for Hmac<Sha256>.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC takes any key length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// Current unix time in seconds (0 if the clock is before 1970, which the Lab rejects
/// anyway).
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether `id` is a Lab account id: the canonical lower-case hyphenated UUID form,
/// the only spelling the Lab keys accounts by. Also what makes it safe in a URL path.
pub fn is_lab_account(id: &str) -> bool {
    let b = id.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(c),
        })
}

/// Which kind of reporting deployment this is (spec §3.1). Engines are hosted by
/// Meilisearch only (decision A), so there is exactly one kind; the enum keeps the
/// wire shape explicit and refuses anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceKind {
    /// Meilisearch-run: reports for any active account, every job debited.
    Hosted,
}

/// `GET /internal/instances/me` (spec §3.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// This deployment's id.
    pub instance_id: String,
    /// Always `hosted`.
    pub kind: InstanceKind,
    /// `scrapix`, `lumen` or `glutony`.
    pub product: String,
    /// Region of the engine.
    #[serde(default)]
    pub region: Option<String>,
    /// The Lab's public base URL.
    #[serde(default)]
    pub lab_url: Option<String>,
}

/// `credits` of an account lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Credits {
    /// Spendable balance; 0 or less means no more work.
    pub balance: i64,
}

/// `GET /internal/accounts/{id}`: `{"active": false}` for an unknown or inactive account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AccountLookup {
    /// Whether the account exists and is active.
    pub active: bool,
    /// The account id, when active.
    #[serde(default)]
    pub account_id: Option<String>,
    /// Plan tier, when active.
    #[serde(default)]
    pub tier: Option<String>,
    /// Balance, when active.
    #[serde(default)]
    pub credits: Option<Credits>,
    /// How long the Lab suggests caching this answer, in seconds.
    #[serde(default)]
    pub cache_ttl: Option<u64>,
}

impl AccountLookup {
    /// Active with a positive balance (spec §8: refuse when `credits.balance <= 0`).
    pub fn can_spend(&self) -> bool {
        self.active && self.credits.is_some_and(|c| c.balance > 0)
    }
}

/// Why a Lab service call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LabError {
    /// Connect, DNS, TLS or timeout.
    #[error("Lab unreachable: {0}")]
    Transport(String),
    /// The Lab refused the instance credentials.
    #[error("the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401)")]
    Unauthorized,
    /// Any other non-2xx.
    #[error("the Lab answered {0}")]
    Status(u16),
    /// A 2xx whose body is not what the contract says, or an id that cannot be a path segment.
    #[error("the Lab answered with an unreadable body: {0}")]
    Malformed(String),
}

async fn get_json<T: DeserializeOwned>(
    http: &reqwest::Client,
    creds: &LabCredentials,
    path: &str,
) -> Result<T, LabError> {
    let resp = creds
        .authorize(http.get(creds.endpoint(path)))
        .send()
        .await
        .map_err(|e| LabError::Transport(e.to_string()))?;
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(LabError::Unauthorized);
    }
    if !status.is_success() {
        return Err(LabError::Status(status.as_u16()));
    }
    resp.json::<T>()
        .await
        .map_err(|e| LabError::Malformed(e.to_string()))
}

/// Ask the Lab what this deployment is (spec §3.5).
pub async fn fetch_instance_info(
    http: &reqwest::Client,
    creds: &LabCredentials,
) -> Result<InstanceInfo, LabError> {
    get_json(http, creds, "/internal/instances/me").await
}

/// Look an account up (spec §8). `account_id` must be a canonical UUID: anything else
/// is refused here rather than sent as a path segment.
pub async fn fetch_account(
    http: &reqwest::Client,
    creds: &LabCredentials,
    account_id: &str,
) -> Result<AccountLookup, LabError> {
    if !is_lab_account(account_id) {
        return Err(LabError::Malformed(format!(
            "{account_id:?} is not a Lab account id"
        )));
    }
    get_json(http, creds, &format!("/internal/accounts/{account_id}")).await
}
```

- [ ] **Step 5: Run the tests**

Run: `cargo test -p meili-ingest-lab && cargo clippy -p meili-ingest-lab --all-targets -- -D warnings`
Expected: 6 tests pass, no warnings. (`cargo fmt --all` after.)

- [ ] **Step 6: Commit**

```bash
git add Cargo.toml Cargo.lock crates/lab
git commit -m "feat(lab): shared Lab client with per-instance credentials and v2 signatures"
```

---

### Task 5: Control plane sender: instance credentials, v2 headers, legacy fallback, 24 h drop, identity at boot

**Files:**
- Modify: `crates/control-plane/Cargo.toml` (add `meili-ingest-lab.workspace = true` under `[dependencies]`)
- Modify: `crates/control-plane/src/lab_sender.rs` (lines 32-99 `LabConfig`/`sign`; 193-203 request; 213-216 401 log; 268-298 `run`; tests 301-363)
- Modify: `crates/control-plane/src/lab_events.rs` (add `drop_stale` after `purge_delivered`, line 139)
- Modify: `crates/control-plane/src/metrics.rs` (add `dropped_total`)
- Modify: `crates/control-plane/src/main.rs:31-50`
- Modify: `crates/control-plane/tests/lab_sender.rs` (helper `sender` lines 68-76, tests 94-130, 243-266), `crates/control-plane/tests/lab_events.rs` (new test)

**Interfaces:**
- Consumes: Task 4's `LabCredentials`, `validate_lab_url`, `unix_now`, `fetch_instance_info`, `H_*`.
- Produces: `LabConfig::from_values(url, instance_id, instance_secret, legacy_secret) -> anyhow::Result<Option<LabConfig>>`, `LabConfig::from_env()`, `LabConfig::credentials() -> Option<&LabCredentials>`, `LabConfig::is_legacy() -> bool`, `LabSender::maintain(&self)`, `LabEventRepo::drop_stale(Duration) -> Result<Vec<Uuid>, CpError>`, `STALE_AFTER = 24 h`, metric `glutony_lab_events_dropped_total`. `sign(secret, body)` stays (legacy v1).

- [ ] **Step 1: Write the failing tests**

In `crates/control-plane/tests/lab_sender.rs`:

Change the constants and helper (lines 16, 68-76) to:

```rust
const SECRET: &str = "lab-instance-secret";
const INSTANCE_ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";
```

```rust
async fn sender(t: &TestDb, lab: &MockServer) -> (LabSender, LabEventRepo, LabMetrics) {
    let repo = LabEventRepo::new(t.pool.clone());
    let config = LabConfig::from_values(
        Some(lab.uri()),
        Some(INSTANCE_ID.into()),
        Some(SECRET.into()),
        None,
    )
    .unwrap()
    .unwrap();
    let metrics = LabMetrics::default();
    let s = LabSender::new(repo.clone(), config, metrics.clone(), Default::default()).unwrap();
    (s, repo, metrics)
}
```

Replace the signature assertions in `a_batch_is_signed_and_delivered` (lines 116-123) with:

```rust
    let req = &lab.received_requests().await.unwrap()[0];
    let hdr = |n: &str| req.headers.get(n).unwrap().to_str().unwrap().to_string();
    assert_eq!(hdr("x-lab-instance-id"), INSTANCE_ID);
    let ts: u64 = hdr("x-lab-timestamp").parse().unwrap();
    let now = meili_ingest_lab::unix_now();
    assert!(now - 5 <= ts && ts <= now, "timestamp {ts} is not now ({now})");
    assert_eq!(
        hdr("x-lab-signature"),
        meili_ingest_lab::sign_batch(SECRET.as_bytes(), ts, &req.body)
    );
    assert!(
        req.headers.get("authorization").is_none(),
        "event batches are signed, not bearer-authenticated"
    );
```

In `an_unreachable_lab_is_a_connect_failure` (line 247) change the config to `LabConfig::from_values(Some("http://127.0.0.1:1".into()), Some(INSTANCE_ID.into()), Some(SECRET.into()), None)`.

Add two tests:

```rust
#[tokio::test]
async fn a_legacy_events_secret_still_signs_the_v1_way() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/events"))
        .respond_with(accept_all)
        .mount(&lab)
        .await;
    let repo = LabEventRepo::new(t.pool.clone());
    let config = LabConfig::from_values(Some(lab.uri()), None, None, Some("old-secret".into()))
        .unwrap()
        .unwrap();
    assert!(config.is_legacy());
    let s = LabSender::new(repo.clone(), config, LabMetrics::default(), Default::default()).unwrap();
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 1,
            pending: 0
        }
    );
    let req = &lab.received_requests().await.unwrap()[0];
    assert_eq!(
        req.headers.get("x-lab-signature").unwrap().to_str().unwrap(),
        sign(b"old-secret", &req.body)
    );
    assert!(req.headers.get("x-lab-instance-id").is_none());
    assert!(req.headers.get("x-lab-timestamp").is_none());
    t.drop_schema().await;
}

#[tokio::test]
async fn maintain_drops_rows_never_acknowledged_for_24h_and_counts_them() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    let (old, fresh) = (Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(old), event(fresh)]).await.unwrap();
    sqlx::query("UPDATE lab_events SET created_at = now() - interval '25 hours', attempts = 300 WHERE id = $1")
        .bind(old)
        .execute(&t.pool)
        .await
        .unwrap();
    s.maintain().await;
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM lab_events")
        .fetch_all(&t.pool)
        .await
        .unwrap();
    assert_eq!(left, vec![fresh]);
    assert!(render(&metrics).contains("glutony_lab_events_dropped_total 1"));
    t.drop_schema().await;
}
```

In `crates/control-plane/tests/lab_events.rs` add:

```rust
#[tokio::test]
async fn drop_stale_only_removes_old_undelivered_rows() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let (old_pending, old_delivered, recent) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(old_pending), event(old_delivered), event(recent)])
        .await
        .unwrap();
    repo.mark_delivered(&[old_delivered]).await.unwrap();
    sqlx::query("UPDATE lab_events SET created_at = now() - interval '2 days' WHERE id = ANY($1)")
        .bind(vec![old_pending, old_delivered])
        .execute(&t.pool)
        .await
        .unwrap();
    let dropped = repo.drop_stale(Duration::from_secs(24 * 3_600)).await.unwrap();
    assert_eq!(dropped, vec![old_pending]);
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM lab_events ORDER BY created_at")
        .fetch_all(&t.pool)
        .await
        .unwrap();
    assert_eq!(left.len(), 2);
    assert!(left.contains(&old_delivered) && left.contains(&recent));
    t.drop_schema().await;
}
```

Replace the in-module unit test `both_or_neither` (lab_sender.rs lines 305-315) with:

```rust
    #[test]
    fn instance_credentials_legacy_secret_or_nothing() {
        let fv = |u: Option<&str>, i: Option<&str>, s: Option<&str>, l: Option<&str>| {
            LabConfig::from_values(
                u.map(String::from),
                i.map(String::from),
                s.map(String::from),
                l.map(String::from),
            )
        };
        assert!(fv(None, None, None, None).unwrap().is_none());
        let c = fv(Some("https://lab.example/"), Some("id"), Some("s"), None)
            .unwrap()
            .unwrap();
        assert_eq!(c.events_url(), "https://lab.example/internal/events");
        assert!(!c.is_legacy());
        assert!(c.credentials().is_some());
        assert!(!format!("{c:?}").contains("\"s\""));
        let legacy = fv(Some("https://lab.example"), None, None, Some("old"))
            .unwrap()
            .unwrap();
        assert!(legacy.is_legacy());
        assert!(legacy.credentials().is_none());
        assert!(!format!("{legacy:?}").contains("old"));
        // Instance credentials win over a leftover legacy secret.
        let both = fv(Some("https://lab.example"), Some("id"), Some("s"), Some("old"))
            .unwrap()
            .unwrap();
        assert!(!both.is_legacy());
        for bad in [
            fv(Some("https://lab.example"), None, None, None),
            fv(Some("https://lab.example"), Some("id"), None, None),
            fv(Some("https://lab.example"), None, Some("s"), None),
            fv(None, Some("id"), Some("s"), None),
            fv(None, None, None, Some("old")),
        ] {
            assert!(bad.is_err());
        }
    }
```

And change the calls in `http_only_on_loopback_or_private_hosts` from `LabConfig::from_values(Some(x.into()), Some("s".into()))` to `LabConfig::from_values(Some(x.into()), Some("id".into()), Some("s".into()), None)`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p meili-ingest-control-plane --test lab_sender --test lab_events` (with `DATABASE_URL`) and `cargo test -p meili-ingest-control-plane lab_sender::tests`
Expected: compile errors (`from_values` arity, `is_legacy`, `maintain`, `drop_stale`).

- [ ] **Step 3: Implement**

`crates/control-plane/src/lab_sender.rs`: replace lines 32-91 (`LabConfig` and its impl) with:

```rust
use meili_ingest_lab::{LabCredentials, validate_lab_url};

/// How the sender authenticates to the Lab.
#[derive(Clone)]
pub enum LabAuth {
    /// `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET` (spec v2 §3.3).
    Instance(LabCredentials),
    /// `LAB_EVENTS_SECRET`: the pre-v2 global secret, accepted for one more release.
    Legacy {
        /// HMAC key over the bare body.
        secret: String,
    },
}

/// `LAB_URL` plus either the instance credentials or the legacy secret.
#[derive(Clone)]
pub struct LabConfig {
    /// Lab base URL, without trailing slash.
    pub url: String,
    auth: LabAuth,
}

impl std::fmt::Debug for LabConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabConfig")
            .field("url", &self.url)
            .field(
                "auth",
                &match &self.auth {
                    LabAuth::Instance(c) => format!("instance {}", c.instance_id()),
                    LabAuth::Legacy { .. } => "legacy <redacted>".to_string(),
                },
            )
            .finish()
    }
}

impl LabConfig {
    /// `LAB_URL` with `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET`, or with the legacy
    /// `LAB_EVENTS_SECRET` (warned, removed next release); nothing at all is `None`.
    pub fn from_values(
        url: Option<String>,
        instance_id: Option<String>,
        instance_secret: Option<String>,
        legacy_secret: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(url) = url else {
            if instance_id.is_some() || instance_secret.is_some() || legacy_secret.is_some() {
                anyhow::bail!("LAB_INSTANCE_ID / LAB_INSTANCE_SECRET / LAB_EVENTS_SECRET need LAB_URL");
            }
            return Ok(None);
        };
        match (instance_id, instance_secret) {
            (Some(id), Some(secret)) => {
                if legacy_secret.is_some() {
                    tracing::warn!(
                        "LAB_EVENTS_SECRET is ignored because LAB_INSTANCE_ID and LAB_INSTANCE_SECRET are set; remove it"
                    );
                }
                let creds = LabCredentials::new(&url, &id, &secret)?;
                Ok(Some(Self {
                    url: creds.url().to_string(),
                    auth: LabAuth::Instance(creds),
                }))
            }
            (None, None) => match legacy_secret {
                Some(secret) => {
                    tracing::warn!(
                        "LAB_EVENTS_SECRET is deprecated: set LAB_INSTANCE_ID and LAB_INSTANCE_SECRET \
                         (the global secret is removed in the next release)"
                    );
                    Ok(Some(Self {
                        url: validate_lab_url(&url)?,
                        auth: LabAuth::Legacy { secret },
                    }))
                }
                None => anyhow::bail!(
                    "LAB_URL is set but LAB_INSTANCE_ID and LAB_INSTANCE_SECRET are not"
                ),
            },
            _ => anyhow::bail!("LAB_INSTANCE_ID and LAB_INSTANCE_SECRET go together"),
        }
    }

    /// Read `LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET` and the legacy `LAB_EVENTS_SECRET`.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(
            get("LAB_URL"),
            get("LAB_INSTANCE_ID"),
            get("LAB_INSTANCE_SECRET"),
            get("LAB_EVENTS_SECRET"),
        )
    }

    /// `{url}/internal/events`.
    pub fn events_url(&self) -> String {
        format!("{}/internal/events", self.url)
    }

    /// The instance credentials, when not running on the legacy secret.
    pub fn credentials(&self) -> Option<&LabCredentials> {
        match &self.auth {
            LabAuth::Instance(c) => Some(c),
            LabAuth::Legacy { .. } => None,
        }
    }

    /// Whether the deprecated global secret is in use.
    pub fn is_legacy(&self) -> bool {
        matches!(self.auth, LabAuth::Legacy { .. })
    }
}
```

Keep `sign` (legacy v1, lines 93-99) as is. Add a constant next to `RETENTION`:

```rust
/// Undelivered rows older than this were never acknowledged and are dropped (spec §3.4).
pub const STALE_AFTER: Duration = Duration::from_secs(24 * 3_600);
```

Replace the request construction (lines 193-203) with:

```rust
        let req = self
            .http
            .post(self.config.events_url())
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        let req = match &self.config.auth {
            LabAuth::Instance(creds) => {
                creds.sign_batch(req, meili_ingest_lab::unix_now(), &body)
            }
            LabAuth::Legacy { secret } => {
                req.header(meili_ingest_lab::H_SIGNATURE, sign(secret.as_bytes(), &body))
            }
        };
        let resp = req.body(body).send().await;
```

Change the 401 log (line 214) to `tracing::error!("the Lab rejected the instance credentials (401); lab events stay pending")`.

Replace the purge block inside `run` (lines 283-290) with a call to `maintain`, and add the method:

```rust
            if last_purge.elapsed() >= Duration::from_secs(3_600) {
                self.maintain().await;
                last_purge = Instant::now();
            }
```

```rust
    /// Hourly housekeeping: purge delivered rows past retention, and drop undelivered
    /// rows the Lab never acknowledged within [`STALE_AFTER`] (spec §3.4: a skipped
    /// event is never acknowledged, so after 24 h it is permanently rejected).
    pub async fn maintain(&self) {
        match self.repo.purge_delivered(RETENTION).await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "old delivered lab events purged"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot purge delivered lab events"),
        }
        match self.repo.drop_stale(STALE_AFTER).await {
            Ok(ids) if !ids.is_empty() => {
                self.metrics.dropped_total.inc_by(ids.len() as u64);
                tracing::error!(
                    count = ids.len(),
                    ids = ?ids,
                    "lab events never acknowledged for 24 h were dropped; the Lab skipped them (check its logs for the reason)"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot drop stale lab events"),
        }
    }
```

Update the module doc (lines 1-6): replace "Rows are never dropped." with "Rows are retried for 24 h; after that they are dropped with an error log (spec v2 §3.4)."

`crates/control-plane/src/lab_events.rs`, after `purge_delivered`:

```rust
    /// Delete undelivered rows older than `older_than` and return their ids. Delivered
    /// rows are never touched here (see `purge_delivered`).
    pub async fn drop_stale(&self, older_than: Duration) -> Result<Vec<Uuid>, CpError> {
        Ok(sqlx::query_scalar(
            "DELETE FROM lab_events \
             WHERE delivered_at IS NULL AND created_at < now() - make_interval(secs => $1) \
             RETURNING id",
        )
        .bind(older_than.as_secs_f64())
        .fetch_all(&self.pool)
        .await?)
    }
```

`crates/control-plane/src/metrics.rs`: add the field `/// \`glutony_lab_events_dropped_total\`.\n pub dropped_total: IntCounter,`, build it in `Default` as `IntCounter::new("glutony_lab_events_dropped_total", "Lab events dropped after 24 h without acknowledgement").expect("valid metric")`, register it in the `for m in [...]` list, and add `"glutony_lab_events_dropped_total"` to the names checked in `renders_the_lab_metrics`.

`crates/control-plane/src/main.rs`: replace lines 36-43 (the `Some(config)` arm) with:

```rust
        Some(config) => {
            tracing::info!(url = %config.url, legacy = config.is_legacy(), "lab events sender enabled");
            if let Some(creds) = config.credentials() {
                match meili_ingest_lab::fetch_instance_info(&reqwest::Client::new(), creds).await {
                    Ok(info) => tracing::info!(
                        kind = ?info.kind,
                        product = %info.product,
                        region = ?info.region,
                        "Lab instance identity confirmed"
                    ),
                    // Spec §3.6: a 401 aborts boot; the credentials are wrong or revoked.
                    Err(meili_ingest_lab::LabError::Unauthorized) => anyhow::bail!(
                        "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); fix the credentials"
                    ),
                    Err(e) => tracing::warn!(
                        error = %e,
                        "could not confirm this deployment's Lab identity; events are sent anyway and retried"
                    ),
                }
            }
            let sender = meili_ingest_control_plane::lab_sender::LabSender::new(
                meili_ingest_control_plane::lab_events::LabEventRepo::new(pool.clone()),
                config,
                state.metrics.clone(),
                state.lab_notify.clone(),
            )?;
            Some(tokio::spawn(sender.run(cancel.clone())))
        }
```

- [ ] **Step 4: Run the tests**

Run: `DATABASE_URL=postgres://postgres:dev-password@localhost:5432/postgres cargo test -p meili-ingest-control-plane` then `cargo clippy --workspace --all-targets -- -D warnings`
Expected: all pass, including `a_legacy_events_secret_still_signs_the_v1_way`, `maintain_drops_rows_never_acknowledged_for_24h_and_counts_them`, `drop_stale_only_removes_old_undelivered_rows`.

- [ ] **Step 5: Commit**

```bash
git add crates/control-plane Cargo.lock
git commit -m "feat(control-plane): sign Lab event batches with instance credentials, drop events unacknowledged for 24h"
```

---

### Task 6: Gateway credit pre-check for hosted deployments

**Files:**
- Modify: `crates/gateway/Cargo.toml` (add `meili-ingest-lab.workspace = true` under `# internal`)
- Create: `crates/gateway/src/lab.rs`
- Modify: `crates/gateway/src/lib.rs:9-20` (add `pub mod lab;`), test_support lines 291-313 (add `test_app_with_lab`)
- Modify: `crates/gateway/src/error.rs` (new variant `PaymentRequired`)
- Modify: `crates/gateway/src/state.rs:757-828` (`AppState.lab`, `with_lab`)
- Modify: `crates/gateway/src/handlers/ingest.rs:213-215` (call site before `let job_id`)
- Modify: `crates/gateway/src/main.rs:119-128`

**Interfaces:**
- Consumes: Task 4's `LabCredentials`, `InstanceInfo`, `InstanceKind`, `AccountLookup`, `LabError`, `fetch_instance_info`, `fetch_account`, `is_lab_account`.
- Produces: `crate::lab::LabClient` with `new(LabCredentials, reqwest::Client)`, `identity() -> Option<InstanceInfo>`, `set_identity(InstanceInfo)`, `refresh_identity() -> Result<InstanceInfo, LabError>`, `resolve_identity(every: Duration)` (loops until known), `check_credits(account_id) -> Result<(), GatewayError>` (503 until the identity is confirmed, 402 when the balance is 0, 503 past the stale window); the pre-check runs whenever `AppState.lab` is `Some`, that is whenever `LAB_INSTANCE_*` are set (there is no kind check and no `is_hosted`); `GatewayError::PaymentRequired(String)` → 402 `insufficient_credits`; `AppState.lab: Option<Arc<LabClient>>`, `AppState::with_lab(Arc<LabClient>)`; `FRESH_FOR = 30 s`, `STALE_FOR = 300 s`.

- [ ] **Step 1: Write the failing tests**

`crates/gateway/src/error.rs`: in `status_mapping` add `assert_eq!(GatewayError::PaymentRequired("x".into()).status(), StatusCode::PAYMENT_REQUIRED); assert_eq!(GatewayError::PaymentRequired("x".into()).code(), "insufficient_credits");` and add `GatewayError::PaymentRequired("x".into()),` to the `all` list in `codes_are_snake_case_and_unique`.

`crates/gateway/src/lab.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_lab::InstanceKind;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
    const ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

    fn hosted() -> InstanceInfo {
        InstanceInfo {
            instance_id: ID.into(),
            kind: InstanceKind::Hosted,
            product: "glutony".into(),
            region: Some("eu-west-1".into()),
            lab_url: None,
        }
    }

    fn client(lab: &MockServer) -> LabClient {
        LabClient::new(
            LabCredentials::new(&lab.uri(), ID, "s").unwrap(),
            reqwest::Client::new(),
        )
    }

    async fn mount_balance(lab: &MockServer, balance: i64, expect: u64) {
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": ACCOUNT, "tier": "pro",
                "credits": {"balance": balance}, "cache_ttl": 30
            })))
            .expect(expect)
            .mount(lab)
            .await;
    }

    #[tokio::test]
    async fn an_account_without_credits_is_402_on_a_hosted_engine() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 0, 1).await;
        let c = client(&lab);
        c.set_identity(hosted());
        let err = c.check_credits(ACCOUNT).await.unwrap_err();
        assert_eq!(err.code(), "insufficient_credits");
        assert_eq!(err.status(), axum::http::StatusCode::PAYMENT_REQUIRED);
    }

    #[tokio::test]
    async fn an_inactive_account_is_402_too() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"active": false})))
            .mount(&lab)
            .await;
        let c = client(&lab);
        c.set_identity(hosted());
        assert_eq!(c.check_credits(ACCOUNT).await.unwrap_err().code(), "insufficient_credits");
    }

    #[tokio::test]
    async fn non_lab_tenants_never_call_the_lab() {
        // A Cloud project id or any other opaque tenant is not a Lab account: nothing
        // to bill, nothing to check.
        let lab = MockServer::start().await;
        mount_balance(&lab, 0, 0).await;
        let c = client(&lab);
        c.set_identity(hosted());
        c.check_credits("hackersearch").await.unwrap();
        c.check_credits(&ACCOUNT.to_uppercase()).await.unwrap();
    }

    #[tokio::test]
    async fn check_credits_fails_closed_until_the_identity_is_known() {
        // Until the Lab has confirmed the credentials, no Lab-account job runs: every
        // job is billed, so there is no availability argument for letting one through.
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 0).await;
        let c = client(&lab);
        assert!(c.identity().is_none());
        let err = c.check_credits(ACCOUNT).await.unwrap_err();
        assert_eq!(err.code(), "lab_unavailable");
        assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test(start_paused = true)]
    async fn lookups_are_cached_for_30s_and_served_stale_for_300s() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 1).await;
        let c = client(&lab);
        c.set_identity(hosted());
        c.check_credits(ACCOUNT).await.unwrap();
        tokio::time::advance(Duration::from_secs(29)).await;
        c.check_credits(ACCOUNT).await.unwrap(); // cached: expect(1) holds
        // The Lab goes down; the 29 s-old entry is stale but usable for 300 s.
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        tokio::time::advance(Duration::from_secs(2)).await;
        c.check_credits(ACCOUNT).await.unwrap();
        // A zero balance seen before the outage keeps refusing while stale.
        c.accounts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(ACCOUNT)
            .unwrap()
            .lookup
            .credits = Some(meili_ingest_lab::Credits { balance: 0 });
        assert_eq!(c.check_credits(ACCOUNT).await.unwrap_err().code(), "insufficient_credits");
        // Past 300 s with no Lab: fail closed (503 lab_unavailable), like Scrapix.
        tokio::time::advance(Duration::from_secs(300)).await;
        assert_eq!(c.check_credits(ACCOUNT).await.unwrap_err().code(), "lab_unavailable");
    }

    #[tokio::test]
    async fn resolve_identity_retries_until_the_lab_answers() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instance_id": ID, "kind": "hosted", "product": "glutony"
            })))
            .mount(&lab)
            .await;
        let c = client(&lab);
        c.resolve_identity(Duration::from_millis(10)).await;
        assert_eq!(c.identity().map(|i| i.kind), Some(InstanceKind::Hosted));
    }
}
```

`crates/gateway/src/handlers/ingest.rs` tests (inside the existing `mod tests`, which imports `crate::test_support::*`):

```rust
    #[tokio::test]
    async fn a_hosted_engine_refuses_a_lab_account_without_credits() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": ACCOUNT, "tier": "free",
                "credits": {"balance": 0}, "cache_ttl": 30
            })))
            .mount(&lab)
            .await;
        let client = std::sync::Arc::new(crate::lab::LabClient::new(
            meili_ingest_lab::LabCredentials::new(
                &lab.uri(),
                "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d",
                "s",
            )
            .unwrap(),
            reqwest::Client::new(),
        ));
        client.set_identity(meili_ingest_lab::InstanceInfo {
            instance_id: "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d".into(),
            kind: meili_ingest_lab::InstanceKind::Hosted,
            product: "glutony".into(),
            region: None,
            lab_url: None,
        });
        let (app, starter) = test_app_with_lab(&server, GatewayConfig::default(), client).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let mut req = standalone_request("/ingest", ct, body);
        req.headers_mut()
            .insert("x-meili-tenant-id", ACCOUNT.parse().unwrap());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYMENT_REQUIRED);
        assert_eq!(json_body(resp).await["code"], "insufficient_credits");
        assert!(starter.inputs().is_empty(), "nothing was queued");
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p meili-ingest-gateway lab:: && cargo test -p meili-ingest-gateway a_hosted_engine_refuses`
Expected: compile errors (`crate::lab`, `PaymentRequired`, `test_app_with_lab`).

- [ ] **Step 3: Implement**

`crates/gateway/src/error.rs`: add after `Forbidden`:

```rust
    /// A hosted Lab engine refused work for an account out of credits (402).
    #[error("{0}")]
    PaymentRequired(String),
    /// A hosted Lab engine could not check an account's credits: the Lab has been
    /// unreachable for longer than the stale window (503, retry later).
    #[error("{0}")]
    LabUnavailable(String),
```

`status()`: `GatewayError::PaymentRequired(_) => StatusCode::PAYMENT_REQUIRED,` and `GatewayError::LabUnavailable(_) => StatusCode::SERVICE_UNAVAILABLE,`; `code()`: `GatewayError::PaymentRequired(_) => "insufficient_credits",` and `GatewayError::LabUnavailable(_) => "lab_unavailable",`. Extend the module doc's status list with "402 insufficient credits" and "503 lab unavailable". Add both variants to the `all` list in `codes_are_snake_case_and_unique` and to `status_mapping` (`assert_eq!(GatewayError::LabUnavailable("x".into()).status(), StatusCode::SERVICE_UNAVAILABLE); assert_eq!(GatewayError::LabUnavailable("x".into()).code(), "lab_unavailable");`).

`crates/gateway/src/lab.rs`:

```rust
//! The deployment's Lab identity (spec v2 §3.6) and the credit pre-check (spec v2
//! §8.1). Engines are hosted by Meilisearch only (decision A): every Lab-account job
//! is billed, so the check runs whenever Lab credentials are configured and fails
//! closed (503 `lab_unavailable`) whenever it cannot be made: before the Lab has
//! confirmed the credentials at boot, and past the stale window of an outage.
//!
//! Lookups are cached per account for [`FRESH_FOR`]; when the Lab cannot answer, an
//! entry up to [`STALE_FOR`] old is used.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use meili_ingest_lab::{
    AccountLookup, InstanceInfo, LabCredentials, LabError, fetch_account, fetch_instance_info,
    is_lab_account,
};
use tokio::time::Instant;

use crate::error::GatewayError;

/// A lookup younger than this is reused without asking the Lab.
pub const FRESH_FOR: Duration = Duration::from_secs(30);
/// A lookup younger than this is reused when the Lab cannot answer.
pub const STALE_FOR: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct Cached {
    fetched_at: Instant,
    lookup: AccountLookup,
}

/// Lab identity and account cache of this gateway.
pub struct LabClient {
    creds: LabCredentials,
    http: reqwest::Client,
    identity: RwLock<Option<InstanceInfo>>,
    accounts: Mutex<HashMap<String, Cached>>,
}

impl std::fmt::Debug for LabClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabClient")
            .field("creds", &self.creds)
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}

impl LabClient {
    /// Build with unknown identity; call [`LabClient::resolve_identity`] at boot.
    pub fn new(creds: LabCredentials, http: reqwest::Client) -> Self {
        Self {
            creds,
            http,
            identity: RwLock::new(None),
            accounts: Mutex::new(HashMap::new()),
        }
    }

    /// The credentials in use.
    pub fn credentials(&self) -> &LabCredentials {
        &self.creds
    }

    /// What the Lab said this deployment is, once known.
    pub fn identity(&self) -> Option<InstanceInfo> {
        self.identity
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Set the identity directly (tests).
    pub fn set_identity(&self, info: InstanceInfo) {
        *self.identity.write().unwrap_or_else(|p| p.into_inner()) = Some(info);
    }

    /// Ask the Lab once and remember the answer.
    pub async fn refresh_identity(&self) -> Result<InstanceInfo, LabError> {
        let info = fetch_instance_info(&self.http, &self.creds).await?;
        self.set_identity(info.clone());
        Ok(info)
    }

    /// Ask until the Lab answers, waiting `every` between attempts. Spawned at boot
    /// (after one synchronous attempt in `main`, where a 401 aborts boot) so a Lab
    /// outage never blocks startup; until it returns, every Lab-account job is
    /// refused with 503.
    pub async fn resolve_identity(&self, every: Duration) {
        loop {
            match self.refresh_identity().await {
                Ok(info) => {
                    tracing::info!(kind = ?info.kind, product = %info.product, region = ?info.region, "Lab instance identity confirmed");
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "could not confirm this deployment's Lab identity; retrying (Lab-account jobs are refused until then)"
                    );
                    tokio::time::sleep(every).await;
                }
            }
        }
    }

    /// Refuse billable work for a Lab account out of credits. A tenant that is not a
    /// Lab account id (a Cloud project id) always passes: nothing is billed for it.
    pub async fn check_credits(&self, account_id: &str) -> Result<(), GatewayError> {
        if !is_lab_account(account_id) {
            return Ok(());
        }
        if self.identity().is_none() {
            return Err(GatewayError::LabUnavailable(
                "the Lab has not confirmed this deployment's credentials yet; retry shortly".into(),
            ));
        }
        match self.lookup(account_id).await {
            Some(lookup) if !lookup.can_spend() => Err(GatewayError::PaymentRequired(format!(
                "account {account_id} has no credits left; top up in the Lab console"
            ))),
            Some(_) => Ok(()),
            // Fail closed, as Scrapix does: money is checked or the job waits.
            None => Err(GatewayError::LabUnavailable(format!(
                "the Lab has been unreachable for more than {}s; cannot check account {account_id}'s credits",
                STALE_FOR.as_secs()
            ))),
        }
    }

    fn cached(&self, account_id: &str) -> Option<Cached> {
        self.accounts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(account_id)
            .cloned()
    }

    async fn lookup(&self, account_id: &str) -> Option<AccountLookup> {
        let now = Instant::now();
        if let Some(c) = self.cached(account_id)
            && now.duration_since(c.fetched_at) < FRESH_FOR
        {
            return Some(c.lookup);
        }
        match fetch_account(&self.http, &self.creds, account_id).await {
            Ok(lookup) => {
                self.accounts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        account_id.to_string(),
                        Cached {
                            fetched_at: now,
                            lookup: lookup.clone(),
                        },
                    );
                Some(lookup)
            }
            Err(e) => match self.cached(account_id) {
                Some(c) if now.duration_since(c.fetched_at) < STALE_FOR => {
                    tracing::warn!(error = %e, account_id, "Lab unreachable; using a stale account lookup");
                    Some(c.lookup)
                }
                _ => {
                    tracing::error!(
                        error = %e,
                        account_id,
                        "Lab unreachable and no usable cached lookup; refusing the job (503)"
                    );
                    None
                }
            },
        }
    }
}
```

`crates/gateway/src/state.rs`: add to `AppState` after `fetch_policy`:

```rust
    /// The Lab, when this deployment reports to one (`LAB_URL` + `LAB_INSTANCE_*`).
    pub lab: Option<Arc<crate::lab::LabClient>>,
```

initialise `lab: None` in `AppState::new`, add `.field("lab", &self.lab)` to its `Debug`, and the builder:

```rust
    /// Attach the Lab client: hosted deployments then pre-check credits before each job.
    pub fn with_lab(mut self, lab: Arc<crate::lab::LabClient>) -> Self {
        self.lab = Some(lab);
        self
    }
```

`crates/gateway/src/handlers/ingest.rs`, before `let job_id = Uuid::new_v4();` (line 213):

```rust
    // A Lab engine refuses work for an account out of credits (spec v2 §8.1), and
    // refuses rather than runs unbilled work when the Lab cannot answer (decision A:
    // every Lab-account job is billed).
    if let (Some(lab), Some(account)) = (&state.lab, ctx.tenant_id.as_deref()) {
        lab.check_credits(account).await?;
    }
```

`crates/gateway/src/lib.rs`: add `pub mod lab;` after `pub mod handlers;`; in `test_support` add after `test_app`:

```rust
    /// Router + starter wired to a `wiremock` control plane and a Lab client.
    pub async fn test_app_with_lab(
        server: &MockServer,
        mut config: GatewayConfig,
        lab: Arc<crate::lab::LabClient>,
    ) -> (Router, Arc<FakeStarter>) {
        config.control_plane_url = server.uri();
        let starter = Arc::new(FakeStarter::default());
        let state = AppState::new(
            config,
            starter.clone(),
            BlobStore::memory(),
            reqwest::Client::new(),
        )
        .with_lab(lab);
        (crate::router(state), starter)
    }
```

`crates/gateway/src/main.rs`: after the `fetch_policy` block (line 117) add:

```rust
    // Lab identity and credit pre-check (spec v2 §3.6, §8.1). Optional: without
    // LAB_INSTANCE_* this gateway never talks to the Lab. One synchronous attempt
    // first: a 401 aborts boot (wrong or revoked credentials); any other failure is
    // retried in the background, and Lab-account jobs are refused until it succeeds.
    let lab = match meili_ingest_lab::LabCredentials::from_env().context("invalid LAB_* configuration")? {
        Some(creds) => {
            tracing::info!(url = creds.url(), instance_id = creds.instance_id(), "Lab client enabled");
            let client = Arc::new(meili_ingest_gateway::lab::LabClient::new(creds, http.clone()));
            match client.refresh_identity().await {
                Ok(info) => tracing::info!(kind = ?info.kind, product = %info.product, region = ?info.region, "Lab instance identity confirmed"),
                Err(meili_ingest_lab::LabError::Unauthorized) => anyhow::bail!(
                    "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); fix the credentials"
                ),
                Err(e) => {
                    tracing::warn!(error = %e, "could not confirm the Lab identity; retrying in the background");
                    let resolver = client.clone();
                    tokio::spawn(async move { resolver.resolve_identity(Duration::from_secs(30)).await });
                }
            }
            Some(client)
        }
        None => None,
    };
```

and change the state construction to:

```rust
    let mut state = AppState::new(config, Arc::new(TemporalStarter(temporal)), blob, http)
        .with_connections(meili_ingest_gateway::connections::ConnectionConfig::new(
            connection_key,
            host_policy,
        ))
        .with_sources(schedules, fetch_policy);
    if let Some(lab) = lab {
        state = state.with_lab(lab);
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p meili-ingest-gateway && cargo clippy -p meili-ingest-gateway --all-targets -- -D warnings`
Expected: PASS (`routes::tests::the_table_matches_the_openapi_spec` still passes: no route changed).

- [ ] **Step 5: Document the 402 in `docs/openapi.yaml`**

Add a `402` response to the six ingest operations (`/ingest`, `/ingest/batch`, `/ingest/pipeline/{name}`, and the three `/indexes/{index_uid}/…` variants), next to their `400`:

```yaml
        "402":
          description: Hosted Lab engine only. The Lab account has no credits left (`code: insufficient_credits`); nothing was queued.
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Error"
```

(Use the same `Error` schema name the file already uses for `400`; check with `grep -n '"400"' -A 5 docs/openapi.yaml`.)

- [ ] **Step 6: Commit**

```bash
git add crates/gateway Cargo.lock docs/openapi.yaml
git commit -m "feat(gateway): refuse jobs for Lab accounts without credits on hosted engines"
```

---

### Task 7: Authenticate the control plane's `/internal/*` routes

**Files:**
- Modify: `crates/control-plane/Cargo.toml` (add `subtle.workspace = true`)
- Modify: `crates/control-plane/src/error.rs` (variant `Unauthorized`)
- Modify: `crates/control-plane/src/lib.rs:42-75` (`AppState.internal_token`, `with_internal_token`), `app()` (middleware), new `require_internal_token`
- Modify: `crates/control-plane/src/main.rs:17-31`
- Modify: `crates/control-plane/tests/routes.rs`, `crates/control-plane/tests/lab_events.rs:236-262`
- Modify: `crates/gateway/src/state.rs` (`GatewayConfig.control_plane_token`, `ControlPlaneClient.token`, `authed`), `crates/gateway/src/main.rs`
- Modify: `crates/worker/src/config.rs`, `crates/worker/src/connection.rs` (`ControlPlane.token`, `authed`), `crates/worker/src/activity.rs`, `crates/worker/src/source_activity.rs`, `crates/worker/src/main.rs`
- Modify: `compose.yaml` (`x-worker-env`, `control-plane.environment`, `gateway.environment`)

**Interfaces:**
- Produces: env `CONTROL_PLANE_TOKEN` (all three binaries) and `CONTROL_PLANE_TOKEN_DISABLED=true` (control plane, dev only); `AppState::with_internal_token(Option<String>)`; `CpError::Unauthorized` → 401 `unauthorized`; `ControlPlaneClient::with_token(Option<String>)`; `meili_ingest_worker::connection::authed(RequestBuilder, Option<&str>) -> RequestBuilder`; `StepActivities::with_control_plane_token`, `SourceActivities::with_control_plane_token`; `pub fn control_plane_token_policy(token: Option<String>, disabled: bool) -> anyhow::Result<Option<String>>` in `crates/control-plane/src/lib.rs`.

- [ ] **Step 1: Write the failing tests**

`crates/control-plane/tests/routes.rs`, append:

```rust
#[tokio::test]
async fn internal_routes_need_the_control_plane_token() {
    let app = app(state().with_internal_token(Some("cp-token".into())));
    for req in [
        post_json("/internal/lab-events", r#"{"events":[]}"#),
        Request::post("/internal/lab-events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer nope")
            .body(Body::from(r#"{"events":[]}"#))
            .unwrap(),
        Request::post("/internal/lab-events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Basic cp-token")
            .body(Body::from(r#"{"events":[]}"#))
            .unwrap(),
        Request::get("/internal/connections/x").body(Body::empty()).unwrap(),
    ] {
        let (status, body) = call(app.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let err: ErrorBody = json(&body);
        assert_eq!(err.code, "unauthorized");
    }
    // The right token reaches the handler. With the lazy pool that handler fails on
    // the database (500 `db`), which is exactly "past the auth gate".
    let (status, _) = call(
        app.clone(),
        Request::get("/internal/connections/x")
            .header(header::AUTHORIZATION, "Bearer cp-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
    // Non-internal routes are untouched: /metrics never needs the token.
    let (status, _) = call(app, Request::get("/metrics").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn without_a_configured_token_internal_routes_stay_open() {
    let (status, _) = call(
        app(state()),
        Request::get("/metrics").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app(state()),
        Request::get("/internal/connections/x").body(Body::empty()).unwrap(),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
}

#[test]
fn control_plane_token_policy() {
    use meili_ingest_control_plane::control_plane_token_policy as policy;
    assert_eq!(policy(Some("t".into()), false).unwrap().as_deref(), Some("t"));
    assert_eq!(policy(Some("t".into()), true).unwrap().as_deref(), Some("t"));
    assert_eq!(policy(None, true).unwrap(), None);
    let err = policy(None, false).unwrap_err().to_string();
    assert!(err.contains("CONTROL_PLANE_TOKEN") && err.contains("CONTROL_PLANE_TOKEN_DISABLED=true"), "{err}");
    assert!(policy(Some("  ".into()), false).is_err(), "blank is unset");
}
```

`crates/control-plane/tests/lab_events.rs`, in `the_internal_route_inserts_and_is_idempotent` change `let app = app(AppState::new(t.pool.clone()));` to `let app = app(AppState::new(t.pool.clone()).with_internal_token(Some("cp-token".into())));` and add `.header(header::AUTHORIZATION, "Bearer cp-token")` to both `Request::post("/internal/lab-events")` builders in that test.

`crates/gateway/src/state.rs` tests, append:

```rust
    #[tokio::test]
    async fn the_control_plane_token_is_sent_as_a_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .and(wiremock::matchers::header("authorization", "Bearer cp-token"))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&server)
            .await;
        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new())
            .with_token(Some("cp-token".into()));
        let job_id = Uuid::new_v4();
        cp.create_job(&JobRecord {
            job_id,
            workflow_id: format!("ingest-{job_id}"),
            pipeline_uid: "builtin.pdf".into(),
            tenant_id: None,
            index_name: None,
            status: JobStatus::Queued,
            current_step: None,
            error: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
        let cfg = GatewayConfig {
            control_plane_token: Some("cp-token-value".into()),
            ..Default::default()
        };
        assert!(!format!("{cfg:?}").contains("cp-token-value"));
    }
```

`crates/worker/src/connection.rs` tests: in `control_plane_with` add `.and(wiremock::matchers::header("authorization", "Bearer cp-token"))` to the mock, and in every `ControlPlane { http: &http, base_url: ... }` literal of the test module (7 sites, lines 186-189, 210-213, 233-236, 259-262, 284-287, 313-316, 344-347) add `token: Some("cp-token"),`.

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p meili-ingest-control-plane --test routes` and `cargo test -p meili-ingest-gateway the_control_plane_token_is_sent_as_a_bearer` and `cargo test -p meili-ingest-worker connection::`
Expected: compile errors (`with_internal_token`, `control_plane_token_policy`, `with_token`, `token` field).

- [ ] **Step 3: Implement the control plane**

`crates/control-plane/src/error.rs`: add variant `/// Missing or wrong \`CONTROL_PLANE_TOKEN\` on an internal route (401, \`unauthorized\`).\n #[error("missing or invalid control plane token")]\n Unauthorized,` with `StatusCode::UNAUTHORIZED` in `status()` and `"unauthorized"` in `code()`; add `(CpError::Unauthorized, StatusCode::UNAUTHORIZED, "unauthorized"),` to the `cases` list of `status_and_code_mapping`.

`crates/control-plane/src/lib.rs`:

```rust
use axum::http::header::AUTHORIZATION;
use axum::middleware::{self, Next};
use subtle::ConstantTimeEq;
```

Add to `AppState`: `/// Bearer token internal callers (gateway, workers) present on \`/internal/*\`; \`None\` leaves them open (dev only).\n pub internal_token: Option<String>,` initialised to `None` in `new`, plus:

```rust
    /// Require `Authorization: Bearer <token>` on every `/internal/*` route.
    pub fn with_internal_token(mut self, token: Option<String>) -> Self {
        self.internal_token = token.map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
        self
    }
```

```rust
/// Decide what the control plane runs with: the token, or none only when the operator
/// said so explicitly (`CONTROL_PLANE_TOKEN_DISABLED=true`, dev).
pub fn control_plane_token_policy(
    token: Option<String>,
    disabled: bool,
) -> anyhow::Result<Option<String>> {
    match token.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()) {
        Some(t) => Ok(Some(t)),
        None if disabled => {
            tracing::warn!("CONTROL_PLANE_TOKEN_DISABLED=true: /internal/* routes are open; never expose this port");
            Ok(None)
        }
        None => anyhow::bail!(
            "CONTROL_PLANE_TOKEN is not set: the gateway and workers authenticate to /internal/* with it. \
             Set it (openssl rand -hex 32) on all three services, or set CONTROL_PLANE_TOKEN_DISABLED=true in dev"
        ),
    }
}

/// Middleware: `/internal/*` needs the configured bearer token; everything else passes.
pub async fn require_internal_token(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = &state.internal_token else {
        return next.run(req).await;
    };
    if !req.uri().path().starts_with("/internal/") {
        return next.run(req).await;
    }
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")))
        .map(str::trim);
    match presented {
        Some(t) if bool::from(t.as_bytes().ct_eq(expected.as_bytes())) => next.run(req).await,
        _ => CpError::Unauthorized.into_response(),
    }
}
```

In `app()` add, before `.layer(TraceLayer::new_for_http())`:

```rust
        .layer(middleware::from_fn_with_state(state.clone(), require_internal_token))
```

`crates/control-plane/src/main.rs`, after `let bind = ...` (line 19):

```rust
    let internal_token = meili_ingest_control_plane::control_plane_token_policy(
        std::env::var("CONTROL_PLANE_TOKEN").ok(),
        std::env::var("CONTROL_PLANE_TOKEN_DISABLED")
            .map(|v| v.trim().eq_ignore_ascii_case("true") || v.trim() == "1")
            .unwrap_or(false),
    )?;
```

and change line 31 to `let state = AppState::new(pool.clone()).with_internal_token(internal_token);`.

- [ ] **Step 4: Implement the gateway side**

`crates/gateway/src/state.rs`: `GatewayConfig` gains `/// Bearer token for the control plane's \`/internal/*\` routes (\`CONTROL_PLANE_TOKEN\`).\n pub control_plane_token: Option<String>,` (`None` in `Default`, `env_opt("CONTROL_PLANE_TOKEN")` in `from_env`, redacted in `Debug` like `admin_api_key`). `ControlPlaneClient` gains `token: Option<String>` (`None` in `new`), and:

```rust
    /// Present `CONTROL_PLANE_TOKEN` on every request.
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }
```

In `send_json` and `send_empty` change `let resp = req.send().await?;` to `let resp = self.authed(req).send().await?;`; in `delete_pipeline` wrap the builder: `let resp = self.authed(self.http.delete(...).query(...)).send().await?;`. In `AppState::new`: `let control_plane = ControlPlaneClient::new(config.control_plane_url.clone(), http.clone()).with_token(config.control_plane_token.clone());`. (`connections.rs:249,266` and `handlers/usage.rs:86` talk to Meilisearch and Tinybird, not the control plane: leave them.)

- [ ] **Step 5: Implement the worker side**

`crates/worker/src/config.rs`: field `/// Bearer token for the control plane's \`/internal/*\` routes (\`CONTROL_PLANE_TOKEN\`).\n pub control_plane_token: Option<String>,` read with `std::env::var("CONTROL_PLANE_TOKEN").ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())`. Derive stays `Debug`: replace it with a manual `impl std::fmt::Debug for WorkerConfig` that prints every field and `control_plane_token` as `self.control_plane_token.as_ref().map(|_| "<redacted>")`.

`crates/worker/src/connection.rs`: `ControlPlane<'a>` gains `/// \`CONTROL_PLANE_TOKEN\`, presented as a bearer.\n pub token: Option<&'a str>,`; add

```rust
/// Present the control plane token, when there is one.
pub fn authed(req: reqwest::RequestBuilder, token: Option<&str>) -> reqwest::RequestBuilder {
    match token {
        Some(t) => req.bearer_auth(t),
        None => req,
    }
}
```

and in `fetch` take `token: Option<&str>` (pass `control_plane.token` from `resolve_connection`) and use `authed(http.get(url), token).send()`.

`crates/worker/src/activity.rs`: `StepActivities` gains `pub control_plane_token: Option<String>` (`None` in `new`), builder `pub fn with_control_plane_token(mut self, token: Option<String>) -> Self`, and `authed(self.http.post(...), self.control_plane_token.as_deref())` in `post_lab_events` and `authed(self.http.patch(&url), ...)` in `patch_job`; the `ControlPlane { ... }` literal at line 318 gains `token: self.control_plane_token.as_deref(),`. Import `use crate::connection::authed;`.

`crates/worker/src/source_activity.rs`: field `pub control_plane_token: Option<String>` (`None` in `new`), builder `with_control_plane_token`, and `crate::connection::authed(..., self.control_plane_token.as_deref())` applied to `self.http.get(url)` in `get_json` and to `req` as the first line of `send_json` and `send_json_unless_gone`.

`crates/worker/src/main.rs`: `publish_manifests(cp, config.control_plane_token.as_deref(), &registry)` with the signature `async fn publish_manifests(control_plane_url: &str, token: Option<&str>, registry: &PluginRegistry)` and `authed(reqwest::Client::new().post(&url), token).json(&manifests)`; add `.with_control_plane_token(config.control_plane_token.clone())` to both `SourceActivities::new(...)` and `StepActivities::new(...)` chains.

- [ ] **Step 6: Compose**

`compose.yaml`: add `CONTROL_PLANE_TOKEN: ${CONTROL_PLANE_TOKEN:-dev-only-control-plane-token}` to `x-worker-env`, to `control-plane.environment` and to `gateway.environment`, with the comment `# Shared by the three services. The default is a well-known dev-only value: never deploy it.` once above the `x-worker-env` entry.

- [ ] **Step 7: Run everything**

Run: `cargo fmt --all && cargo clippy --workspace --all-targets -- -D warnings && cargo test --workspace && DATABASE_URL=postgres://postgres:dev-password@localhost:5432/postgres cargo test -p meili-ingest-control-plane`
Expected: all green. Then `docker compose config > /dev/null` to validate compose.

- [ ] **Step 8: Commit**

```bash
git add crates/control-plane crates/gateway crates/worker compose.yaml Cargo.lock
git commit -m "feat: authenticate control-plane /internal/* routes with CONTROL_PLANE_TOKEN"
```

---

### Task 8: Tenant isolation: own connections only, no `MEILI_URL` fallback with a tenant

**Files:**
- Modify: `crates/control-plane/src/connections.rs:115-135` (`ConnectionRepo::get`; add `get_owned`), 270-283 (`get_connection` handler; add `ConnectionQuery`)
- Modify: `crates/control-plane/src/pipelines.rs:277-292` (`check_against_registry`)
- Modify: `crates/worker/src/connection.rs:101-117` (`fetch` query) and its tests
- Modify: `crates/gateway/src/context.rs:168-174` (env fallbacks) and tests
- Modify: `crates/control-plane/tests/connections_repo.rs` (new tests)
- Modify: `docs/deployment/meilisearch-lab.mdx:122-127` ("Before connecting a Lab": the rule is now enforced; the full rewrite is Task 11)

**Interfaces:**
- Produces: `ConnectionRepo::get_owned(uid, tenant_id: &str) -> Result<Option<ConnectionRecord>, CpError>`; `GET /internal/connections/{uid}?tenant_id=X&scope=tenant` (tenant's row only; `scope=tenant` without a tenant is 422 `validation`); the worker always sends `scope=tenant` when it has a tenant; `resolve_request_context` only applies `MEILI_URL`/`MEILI_API_KEY` when `tenant_id` is `None`.

- [ ] **Step 1: Write the failing tests**

`crates/control-plane/tests/connections_repo.rs`, append:

```rust
#[tokio::test]
async fn get_owned_never_returns_the_global_row() {
    let Some(pool) = pool("cn-owned").await else {
        return;
    };
    let repo = ConnectionRepo::new(pool);
    repo.insert(&new_connection("cn-owned-1", None)).await.expect("global");
    repo.insert(&new_connection("cn-owned-1", Some("cp-2"))).await.expect("tenant");
    assert!(repo.get_owned("cn-owned-1", "cp-2").await.expect("get").is_some());
    assert!(
        repo.get_owned("cn-owned-1", "cp-3").await.expect("get").is_none(),
        "a global connection is a template, never a tenant's key"
    );
}

#[tokio::test]
async fn a_tenant_pipeline_cannot_name_a_global_connection() {
    let Some(pool) = pool("cn-tpl").await else {
        return;
    };
    let connections = ConnectionRepo::new(pool.clone());
    connections.insert(&new_connection("cn-tpl-global", None)).await.expect("global");
    connections.insert(&new_connection("cn-tpl-own", Some("cp-9"))).await.expect("own");
    let state = meili_ingest_control_plane::AppState::new(pool);
    let manifests = serde_json::json!([
        {"name": "json_parser", "version": "0"}, {"name": "meili_indexer", "version": "0"}
    ]);
    let app = meili_ingest_control_plane::app(state);
    use axum::body::Body;
    use axum::http::{Request, StatusCode, header};
    use tower::ServiceExt;
    let reg = Request::post("/internal/plugins")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(manifests.to_string()))
        .unwrap();
    assert!(app.clone().oneshot(reg).await.unwrap().status().is_success());
    for (connection, expected) in [("cn-tpl-global", StatusCode::UNPROCESSABLE_ENTITY), ("cn-tpl-own", StatusCode::OK)] {
        let def = pipeline_using("cn-tpl-p", Some("cp-9"), connection);
        let req = Request::post("/pipelines/validate")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&def).unwrap()))
            .unwrap();
        let status = app.clone().oneshot(req).await.unwrap().status();
        assert_eq!(status, expected, "{connection}");
    }
}
```

(If `POST /internal/plugins` expects a different body shape, read `crates/control-plane/src/plugins.rs::register_plugins` and build the manifests with `meili_ingest_plugin_sdk::PluginManifest::new("json_parser", "0")` serialized in a `Vec`; the test's intent is the two `validate` statuses.)

`crates/worker/src/connection.rs` tests: in `control_plane_with` add `.and(query_param("scope", "tenant"))` to the mock. Add:

```rust
    #[tokio::test]
    async fn a_tenant_lookup_asks_for_the_tenants_row_only() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod-movies"))
            .and(query_param("tenant_id", "tenant-1"))
            .and(query_param("scope", "tenant"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
            token: Some("cp-token"),
        };
        let err = resolve_connection(
            &cp,
            &settings(HostPolicy::Any),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies" }),
            Some("tenant-1"),
        )
        .await
        .expect_err("global rows are not the tenant's");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("prod-movies") && m.contains("own connections")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_global_lookup_has_no_scope() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod-movies"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
            token: None,
        };
        let _ = resolve_connection(
            &cp,
            &settings(HostPolicy::Any),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies" }),
            None,
        )
        .await;
        let req = &server.received_requests().await.unwrap()[0];
        assert!(req.url.query().is_none(), "{:?}", req.url);
    }
```

`crates/gateway/src/context.rs` tests, append:

```rust
    #[test]
    fn a_tenant_request_never_falls_back_to_the_env_destination() {
        let h = headers(&[("x-meili-tenant-id", "acct-1")]);
        let err = resolve_context(&h, None, &cfg_env()).unwrap_err();
        assert!(matches!(err, GatewayError::MissingContext(_)), "{err:?}");
        let ctx = resolve_request_context(&h, None, &cfg_env()).unwrap();
        assert_eq!(ctx.tenant_id.as_deref(), Some("acct-1"));
        assert_eq!(ctx.host, None);
        assert_eq!(ctx.api_key, None);
        // A tenant that brings its own destination keeps it.
        let h = headers(&[
            ("x-meili-tenant-id", "acct-1"),
            ("x-meili-host", "http://m:7700"),
            ("authorization", "Bearer k"),
        ]);
        let ctx = resolve_context(&h, None, &cfg_env()).unwrap();
        assert_eq!(ctx.host.as_deref(), Some("http://m:7700"));
        assert_eq!(ctx.api_key.as_deref(), Some("k"));
    }
```

- [ ] **Step 2: Run to verify failure**

Run: `cargo test -p meili-ingest-worker connection:: ; cargo test -p meili-ingest-gateway context::a_tenant_request_never_falls_back`
Expected: worker tests fail on `query_param("scope", ...)` (no such query sent) and on the message; the gateway test fails because `host` is filled from env.

- [ ] **Step 3: Implement**

`crates/control-plane/src/connections.rs`, after `get`:

```rust
    /// The tenant's own row only. This is the lookup a tenant job uses: a global
    /// connection is a template an operator may copy, never a key a tenant can write
    /// with (spec v2, tenant isolation).
    pub async fn get_owned(
        &self,
        uid: &str,
        tenant_id: &str,
    ) -> Result<Option<ConnectionRecord>, CpError> {
        Ok(sqlx::query_as(
            "SELECT id, uid, name, tenant_id, host, api_key, created_at, updated_at \
             FROM meili_connections \
             WHERE uid = $1 AND tenant_id = $2",
        )
        .bind(uid)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?)
    }
```

Update `get`'s doc comment: "This is the lookup management reads use (a tenant sees its row, else the global template). Jobs use `get_owned`." Replace the handler:

```rust
/// Query of `GET /internal/connections/{uid}`.
#[derive(Debug, Deserialize)]
pub struct ConnectionQuery {
    /// Tenant scope; falls back to the `X-Meili-Project-Id` header.
    #[serde(default, alias = "project_id")]
    pub tenant_id: Option<String>,
    /// `tenant`: only the tenant's own row, what a tenant job may use.
    #[serde(default)]
    pub scope: Option<String>,
}

/// `GET /internal/connections/{uid}?tenant_id=[&scope=tenant]`.
pub async fn get_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<ConnectionQuery>,
    headers: HeaderMap,
) -> Result<Json<ConnectionRecord>, CpError> {
    let tenant_id = tenant_scope(q.tenant_id.as_deref(), &headers);
    let row = match (q.scope.as_deref(), tenant_id.as_deref()) {
        (Some("tenant"), Some(t)) => state.connections().get_owned(&uid, t).await?,
        (Some("tenant"), None) => {
            return Err(CpError::Validation("scope=tenant needs a tenant_id".into()));
        }
        (Some(other), _) => {
            return Err(CpError::Validation(format!("unknown scope {other:?}")));
        }
        (None, t) => state.connections().get(&uid, t).await?,
    };
    row.map(Json).ok_or_else(|| not_found(&uid, tenant_id.as_deref()))
}
```

`crates/control-plane/src/pipelines.rs` lines 283-292:

```rust
        let found = match def.tenant_id.as_deref() {
            Some(t) => connections.get_owned(uid, t).await?,
            None => connections.get(uid, None).await?,
        };
        if found.is_none() {
            errors.push(format!(
                "step {:?} ({}): connection {uid:?} does not exist for this tenant \
                 (a tenant pipeline may only name its own connections; global ones are templates)",
                step.id, step.plugin
            ));
        }
```

and update the comment above (lines 266-268) to say the lookup matches the worker's: tenant's own row for a tenant pipeline, the global row for a global one.

`crates/worker/src/connection.rs` in `fetch`:

```rust
    if let Some(p) = tenant_id {
        // A tenant job may only write with its own connection (spec v2 isolation).
        url.query_pairs_mut()
            .append_pair("tenant_id", p)
            .append_pair("scope", "tenant");
    }
```

and the 404 message:

```rust
        return Err(PluginError::NonRetryable(format!(
            "connection {name:?} not found (tenant_id={tenant_id:?}); a tenant job may only \
             use its own connections (global ones are templates), or it was deleted while \
             this pipeline still names it"
        )));
```

`crates/gateway/src/context.rs` lines 168-174:

```rust
    // 7: env fallbacks, standalone only. A request that carries a tenant never falls
    // back to the deployment's own Meilisearch (spec v2 isolation): its destination is
    // its own headers or its pipeline's connection, nothing else.
    if tenant_id.is_none() {
        if host.is_none() {
            host = config.meili_url.clone();
        }
        if api_key.is_none() {
            api_key = config.meili_api_key.clone();
        }
    }
```

and update the module doc line 11 to "7. `MEILI_URL` / `MEILI_API_KEY` env vars → host / api_key (self-hosted fallback, only without a tenant)".

- [ ] **Step 4: Run the tests**

Run: `cargo test -p meili-ingest-worker connection:: && cargo test -p meili-ingest-gateway context:: && DATABASE_URL=postgres://postgres:dev-password@localhost:5432/postgres cargo test -p meili-ingest-control-plane --test connections_repo --test connections_routes` then clippy.
Expected: PASS. `a_tenant_sees_its_own_connection_before_the_global_one` still passes (`get` is unchanged).

- [ ] **Step 5: Commit**

```bash
git add crates/control-plane crates/worker crates/gateway
git commit -m "fix: tenant jobs only use their own connections and never the deployment's MEILI_URL"
```

---

### Task 9: Release workflow, one image name, project files

**Files:**
- Create: `.github/workflows/release.yml`, `CHANGELOG.md`, `CONTRIBUTING.md`, `SECURITY.md`
- Modify: `.github/workflows/ci.yml:87,93` (tags), `k8s/kustomization.yaml:4-5,22`, `k8s/control-plane.yaml:92`, `k8s/gateway.yaml:46`, `k8s/workers.yaml:62,165,290,320-346`, `README.md:61`, `docs/deployment/kubernetes.mdx:7-8,138-142`, `docs/plugins/authoring-grpc.mdx:121`, `Dockerfile` header comment

**Interfaces:**
- Produces: the image `ghcr.io/qdequele/glutony:{version}` and `:latest`, pushed on tags `v*`; a GitHub release with generated notes.

Naming note (deferred decision, spec §10): crates, binaries, the `meili-ingest` k8s namespace, Cargo `repository` and the README's clone URL keep the `meili-ingest` name. The only name this task settles is the published image, so that k8s, CI, docs and the release workflow agree.

- [ ] **Step 1: Write `.github/workflows/release.yml`**

```yaml
name: Release

on:
  push:
    tags: ["v*"]

permissions:
  contents: write
  packages: write

env:
  IMAGE: ghcr.io/qdequele/glutony

jobs:
  image:
    name: Build and push the image
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: docker/setup-buildx-action@v3
      - uses: docker/login-action@v3
        with:
          registry: ghcr.io
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}
      - id: meta
        uses: docker/metadata-action@v5
        with:
          images: ${{ env.IMAGE }}
          tags: |
            type=semver,pattern={{version}}
            type=semver,pattern={{major}}.{{minor}}
            type=raw,value=latest
      - uses: docker/build-push-action@v6
        with:
          context: .
          file: Dockerfile
          push: true
          tags: ${{ steps.meta.outputs.tags }}
          labels: ${{ steps.meta.outputs.labels }}
          cache-from: type=gha
          cache-to: type=gha,mode=max
      - name: Smoke test the pushed image
        run: |
          tag="${GITHUB_REF_NAME#v}"
          for bin in meili-ingest-gateway meili-ingest-control-plane meili-ingest-worker; do
            docker run --rm --entrypoint /usr/bin/test "$IMAGE:$tag" -x /usr/local/bin/$bin
          done

  release:
    name: GitHub release
    needs: [image]
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: softprops/action-gh-release@v2
        with:
          generate_release_notes: true
          body: |
            Image: `ghcr.io/qdequele/glutony:${{ github.ref_name }}` (also `latest`).
            Changes: see CHANGELOG.md.
```

- [ ] **Step 2: Rename the image everywhere**

`sed -i '' 's#ghcr.io/meilisearch/meili-ingest#ghcr.io/qdequele/glutony#g' .github/workflows/ci.yml k8s/kustomization.yaml k8s/control-plane.yaml k8s/gateway.yaml k8s/workers.yaml README.md docs/deployment/kubernetes.mdx docs/plugins/authoring-grpc.mdx Dockerfile` then `grep -rn "ghcr.io/meilisearch" . --exclude-dir=target --exclude-dir=node_modules` must print nothing.

In `k8s/workers.yaml` remove the two sidecar containers (`whisper-plugin`, `ffmpeg-plugin`, lines 320-346) and replace them with a comment:

```yaml
        # gRPC plugin sidecars go here (Whisper, ffmpeg, OCR). Nothing in this repo
        # builds such images: bring your own, implementing proto/plugin.proto, and
        # list them in EXTERNAL_PLUGINS above. Example:
        # - name: whisper-plugin
        #   image: <your registry>/whisper-plugin:latest
        #   ports: [{ name: grpc, containerPort: 50051 }]
        #   resources:
        #     limits: { nvidia.com/gpu: 1, memory: 16Gi }
```

In `docs/deployment/kubernetes.mdx` lines 138-142 change "Replace the example sidecar images in `workers.yaml` with yours" to "`workers.yaml` ships no sidecar image: add yours (implementing `proto/plugin.proto`) under the commented example". In `k8s/kustomization.yaml` keep `newTag: latest` and update the comment's example tag to `ghcr.io/qdequele/glutony=ghcr.io/qdequele/glutony:v0.1.0`.

- [ ] **Step 3: Write `CHANGELOG.md`**

```markdown
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
```

- [ ] **Step 4: Write `CONTRIBUTING.md`**

```markdown
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
```

- [ ] **Step 5: Write `SECURITY.md`**

```markdown
# Security

## Reporting a vulnerability

Email security@meilisearch.com with a description, reproduction steps and the commit or image tag. Do not open a public issue. You will get an acknowledgement within 3 business days and a fix or mitigation plan within 30 days for confirmed issues.

## Supported versions

Only the latest tagged release (`ghcr.io/qdequele/glutony:latest` and its semver tag) receives fixes.

## Deployment notes

- The control plane is internal: keep port 9000 off any public network and set `CONTROL_PLANE_TOKEN` on every service (`CONTROL_PLANE_TOKEN_DISABLED=true` is for local development only).
- `ENVOY_TRUSTED_HEADER` must be set wherever `X-Meili-*` headers can come from untrusted clients; `LAB_SERVICE_TOKEN` refuses to start without it.
- `SOURCE_SECRET_KEY` seals stored Meilisearch keys and source credentials; rotating it makes stored secrets unreadable.
- `LAB_INSTANCE_SECRET` is minted and rotated by the Meilisearch Lab; revoke it there if it leaks.
- Tenant jobs can only write with their own tenant's connections; global connections are templates.
```

- [ ] **Step 6: Validate and commit**

Run: `ruby -ryaml -e 'YAML.load_file(".github/workflows/release.yml"); puts "ok"'` and `kubectl kustomize k8s/ > /dev/null && echo ok` (or `kustomize build k8s/`).
Expected: `ok` twice.

```bash
git add .github/workflows/release.yml .github/workflows/ci.yml CHANGELOG.md CONTRIBUTING.md SECURITY.md k8s README.md docs/deployment/kubernetes.mdx docs/plugins/authoring-grpc.mdx Dockerfile
git commit -m "ci: release workflow publishing ghcr.io/qdequele/glutony, project files"
```

---

### Task 10: Kubernetes: Lab, admin and control-plane secrets, provider costs, control plane x2

**Files:**
- Modify: `k8s/control-plane.yaml` (ConfigMap lines 16-42, Secret 44-66, Deployment env 95-108, `replicas: 1` line 75)
- Modify: `k8s/gateway.yaml:52-70` (env)
- Modify: `k8s/workers.yaml` (env of the three worker Deployments, lines 64-76, 167-193, 292-317; volume mounts)
- Modify: `k8s/kustomization.yaml` (`configMapGenerator`)
- Create: `k8s/provider-costs.toml` (copy of `config/provider-costs.toml`)
- Modify: `docs/deployment/kubernetes.mdx:49-56` (secret command)

**Interfaces:**
- Consumes: env names from Tasks 5-7 (`LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET`, `CONTROL_PLANE_TOKEN`), `ADMIN_API_KEY` and `SOURCE_SECRET_KEY` (existing), `PROVIDER_COSTS_FILE` (existing, `crates/plugin-sdk/src/cost.rs:78`), `LAB_EVENTS_ENABLED` (worker).
- Multi-replica safety: `LabEventRepo::claim_due` (`crates/control-plane/src/lab_events.rs:81-100`) leases rows with `FOR UPDATE SKIP LOCKED` and a 30 s lease, and `tests/lab_events.rs::a_claim_leases_rows_so_two_senders_never_share_one` proves two concurrent senders never claim the same row. Two control-plane replicas therefore each run a sender safely; inserts are idempotent (`ON CONFLICT (id) DO NOTHING`) and migrations run under sqlx's advisory lock.

- [ ] **Step 1: ConfigMap and Secret (`k8s/control-plane.yaml`)**

Append to the ConfigMap `data`:

```yaml
  # Meilisearch Lab (platform contract v2). Leave LAB_URL empty to run without a Lab.
  LAB_URL: ""
  # Workers write one usage and one lifecycle event per Lab job to the outbox.
  LAB_EVENTS_ENABLED: "false"
  # Provider cost table mounted from the meili-ingest-provider-costs ConfigMap.
  PROVIDER_COSTS_FILE: "/etc/meili-ingest/provider-costs.toml"
```

Append to the Secret `stringData` (and to the `kubectl create secret` comment at the top of the file, one `--from-literal` per key):

```yaml
  # Control plane <-> gateway/workers. Generate with `openssl rand -hex 32`.
  CONTROL_PLANE_TOKEN: "CHANGE_ME"
  # Seals stored Meilisearch keys and source credentials: `openssl rand -base64 32`.
  SOURCE_SECRET_KEY: "CHANGE_ME"
  # Operator bearer for the management routes.
  ADMIN_API_KEY: "CHANGE_ME"
  # Minted by the Lab (POST /instances/:id/credentials, or the hosted-engine rake task).
  LAB_INSTANCE_ID: "CHANGE_ME"
  LAB_INSTANCE_SECRET: "CHANGE_ME"
```

Control plane Deployment: `replicas: 2` with the comment `# Two replicas: the Lab events sender leases rows with FOR UPDATE SKIP LOCKED, so both can run it.` and add to `env` after `DATABASE_URL`:

```yaml
            - name: CONTROL_PLANE_TOKEN
              valueFrom:
                secretKeyRef:
                  name: meili-ingest-secrets
                  key: CONTROL_PLANE_TOKEN
            - name: LAB_URL
              valueFrom:
                configMapKeyRef:
                  name: meili-ingest-config
                  key: LAB_URL
                  optional: true
            - name: LAB_INSTANCE_ID
              valueFrom:
                secretKeyRef:
                  name: meili-ingest-secrets
                  key: LAB_INSTANCE_ID
                  optional: true
            - name: LAB_INSTANCE_SECRET
              valueFrom:
                secretKeyRef:
                  name: meili-ingest-secrets
                  key: LAB_INSTANCE_SECRET
                  optional: true
```

- [ ] **Step 2: Gateway and workers**

`k8s/gateway.yaml` env: add the same `CONTROL_PLANE_TOKEN`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET` entries (LAB_URL comes from `envFrom` the ConfigMap already), plus:

```yaml
            - name: SOURCE_SECRET_KEY
              valueFrom:
                secretKeyRef:
                  name: meili-ingest-secrets
                  key: SOURCE_SECRET_KEY
            - name: ADMIN_API_KEY
              valueFrom:
                secretKeyRef:
                  name: meili-ingest-secrets
                  key: ADMIN_API_KEY
                  optional: true
```

Each of the three worker Deployments in `k8s/workers.yaml`: add the `CONTROL_PLANE_TOKEN` (required) and `SOURCE_SECRET_KEY` (required) `secretKeyRef` entries to `env`, and mount the cost table:

```yaml
          volumeMounts:
            - name: tmp
              mountPath: /tmp
            - name: provider-costs
              mountPath: /etc/meili-ingest
              readOnly: true
      volumes:
        - name: tmp
          emptyDir: {}
        - name: provider-costs
          configMap:
            name: meili-ingest-provider-costs
```

`k8s/kustomization.yaml`: add

```yaml
configMapGenerator:
  - name: meili-ingest-provider-costs
    files:
      - provider-costs.toml
    options:
      disableNameSuffixHash: true
```

and `cp config/provider-costs.toml k8s/provider-costs.toml`, prepending the line `# Copy of config/provider-costs.toml for the k8s ConfigMap; set your prices here.`

- [ ] **Step 3: Docs**

`docs/deployment/kubernetes.mdx` secret command (lines 49-56): add `--from-literal=CONTROL_PLANE_TOKEN="$(openssl rand -hex 32)"`, `--from-literal=SOURCE_SECRET_KEY="$(openssl rand -base64 32)"`, `--from-literal=ADMIN_API_KEY="$(openssl rand -hex 32)"`, `--from-literal=LAB_INSTANCE_ID='<from the Lab>'`, `--from-literal=LAB_INSTANCE_SECRET='<from the Lab>'`, and after it a sentence: "Set `LAB_URL` in the ConfigMap and `LAB_EVENTS_ENABLED: "true"` to report to a Meilisearch Lab; see [Meilisearch Lab](/deployment/meilisearch-lab). The control plane runs two replicas; its outbox sender leases rows with `SKIP LOCKED`, so both deliver without duplicates."

- [ ] **Step 4: Validate and commit**

Run: `kubectl kustomize k8s/ | grep -c "CONTROL_PLANE_TOKEN"` (expect ≥ 5) and `kubectl kustomize k8s/ | grep -A1 "name: meili-control-plane$" | head`; `kubectl kustomize k8s/ | grep "replicas: 2" | wc -l` (expect 2: gateway and control plane).

```bash
git add k8s docs/deployment/kubernetes.mdx
git commit -m "feat(k8s): Lab, admin, source-secret and control-plane-token variables; control plane x2"
```

---

### Task 11: Docs: Lab v2 deployment guide, env vars, Tinybird is analytics

**Files:**
- Modify: `docs/deployment/meilisearch-lab.mdx` (replace sections "Billing events" 52-91, "Known limit" 115-120, "Before connecting a Lab" 122-127, "What the Lab has to do" 129-138)
- Modify: `docs/deployment/environment-variables.mdx` (gateway 12-31, worker 35-64, control plane 71-77)
- Modify: `docs/concepts/usage.mdx:63-110`, `README.md:286-305`, `tinybird/pipes/tenant_usage.pipe:1-14`, `tinybird/README.md` (lines 17, 75, 132, 259, 295: wording only), `docs/concepts/multi-tenancy.mdx:80-92`

**Interfaces:** none; documents Tasks 1-10. Tinybird resource names (`usage_daily_billing`, `usage_daily_billing_hourly`) are kept: renaming a datasource is a migration (seams spec §3.5).

- [ ] **Step 1: `docs/deployment/meilisearch-lab.mdx`**

Replace "## Billing events" through the end of "### Monitoring" with:

```markdown
## Reporting to the Lab

The Lab mints each deployment an id and a secret (Settings → Connection for a
customer-run instance; the hosted-engine rake task for a Meilisearch-run one):

| Variable | Service | Notes |
|---|---|---|
| `LAB_URL` | control plane, gateway | `https` unless the host is loopback or private |
| `LAB_INSTANCE_ID` | control plane, gateway | this deployment's id in the Lab |
| `LAB_INSTANCE_SECRET` | control plane, gateway | 64 hex chars; rotate or revoke in the Lab |
| `LAB_EVENTS_ENABLED` | workers | `true` writes events to the control plane's outbox |

`LAB_EVENTS_SECRET` (the pre-v2 global secret) still works on the control plane with
a boot warning and is removed next release.

At boot both binaries call `GET /internal/instances/me` to confirm the credentials
and log the product and region. A 401 aborts boot. Any other failure is retried every
30 s; until the Lab answers, the gateway refuses jobs for Lab accounts with
`503 {"code": "lab_unavailable"}` (every job is billed, so none runs unchecked).

### Events

Each finished Lab job produces two events, built once and delivered at least once
with deterministic ids:

```json
{
  "id": "<UUIDv5 of job:{job_id}:usage>",
  "type": "usage.recorded",
  "occurred_at": "2026-10-01T10:00:09.000Z",
  "account_id": "<tenant id>",
  "api_key_id": null,
  "product": "glutony",
  "data": {
    "operation": "ingest",
    "units": { "documents": 12, "bytes_in": 5000, "step_seconds": 4,
               "llm_tokens_in": 1000, "llm_tokens_out": 100, "audio_seconds": 0,
               "ocr_pages": 0, "pages": 9, "images": 0, "llm_requests": 1,
               "external_requests": 0 },
    "provider_cost_micro_usd": 210,
    "description": "Job … (builtin.pdf, succeeded, 12 documents)",
    "job_id": "…"
  }
}
```

and `job.completed` (`{job_id, index_uid: <pipeline uid>, pages_crawled: 0,
documents_indexed, duration_secs}`) or `job.failed` (`{job_id, error_message,
pages_crawled: 0}`), id `UUIDv5 of job:{job_id}:lifecycle`.

- The Lab prices `documents`, `ocr_pages` and `provider_cost_micro_usd` (its
  `pricing.yml`); other units are stored and shown. glutony never computes credits.
- `provider_cost_micro_usd` is what glutony paid providers (`config/provider-costs.toml`
  or `PROVIDER_COSTS_FILE`). When any call could not be priced it is sent as 0 and the
  description says `provider cost incomplete`.
- Jobs without a canonical-UUID tenant (standalone, Cloud projects) are never sent.
  Failed and cancelled jobs are.

### Delivery

Workers post events to `POST /internal/lab-events` on the control plane (with
`CONTROL_PLANE_TOKEN`); the control plane sends batches of up to 500 to
`POST {LAB_URL}/internal/events` with `X-Lab-Instance-Id`, `X-Lab-Timestamp` and
`X-Lab-Signature: sha256=<hex HMAC-SHA256(LAB_INSTANCE_SECRET, "<timestamp>.<body>")>`,
marks the ids the Lab lists in `accepted`, and retries the rest with backoff (max
5 min). An event the Lab has not accepted after **24 h** was skipped on purpose
(for example `product mismatch`): it is dropped with an error log
and counted in `glutony_lab_events_dropped_total`. Set `LAB_URL` before enabling
`LAB_EVENTS_ENABLED`, or events older than 24 h at first start are dropped too.

### Credit pre-check

Before queueing a job for a Lab account, the gateway calls
`GET {LAB_URL}/internal/accounts/{account}` (cached 30 s, stale answers used for up
to 300 s when the Lab is down) and answers `402 {"code": "insufficient_credits"}` when
the balance is 0 or the account is inactive. Past the stale window, or before the Lab
has confirmed the credentials at boot, it answers `503 {"code": "lab_unavailable"}`
and queues nothing. Tenants that are not Lab account ids (Cloud projects) are never
checked.

### Monitoring

The control plane serves `GET /metrics`:

- `glutony_lab_events_pending`
- `glutony_lab_events_oldest_pending_seconds`
- `glutony_lab_events_delivered_total`
- `glutony_lab_events_dropped_total`
- `glutony_lab_events_failed_total{reason}` (`connect`, `timeout`, `auth`, `status`, `malformed`, `not_accepted`)

Alert: `glutony_lab_events_oldest_pending_seconds > 900` for 5 minutes (same threshold
as Lumen), and any increase of `glutony_lab_events_dropped_total`.
```

Replace "## Known limit: no spend enforcement" with:

```markdown
## Spend enforcement

The gateway refuses work for an account out of credits (`402`) and refuses rather
than runs unbilled work when it cannot check (`503`): before the Lab has confirmed
the credentials, and during a Lab outage longer than 5 minutes. The pre-check is a
snapshot: work already running keeps running.
```

Replace "## Before connecting a Lab" with:

```markdown
## Isolation

- A tenant job may only write with a connection its tenant owns. Global connections
  are templates: a tenant pipeline that names one fails validation on save and, if
  it was saved before this rule, fails the indexer step with "a tenant job may only
  use its own connections".
- A request that carries a tenant never falls back to `MEILI_URL` / `MEILI_API_KEY`:
  it brings its own `X-Meili-Host` / key, or its pipeline is pinned to a connection.
- The control plane's `/internal/*` routes require `CONTROL_PLANE_TOKEN`.
```

Replace "## What the Lab has to do" with:

```markdown
## What the Lab provides

1. The events schema this repo vendors (`contracts/vendor/lab/lab-events.schema.json`).
2. Per-deployment credentials (`LAB_INSTANCE_ID` / `LAB_INSTANCE_SECRET`) and
   `GET /internal/instances/me`.
3. `GET /internal/accounts/{id}` for the hosted pre-check.
4. `X-Meili-Tenant-Id` and `X-Meili-Envoy-Secret` injected on glutony's ingest routes.
5. The console calls the management routes server side with `LAB_SERVICE_TOKEN` and
   `X-Glutony-Tenant-Id`.
```

Update the page `description` (line 3) to "Run glutony as a Meilisearch Lab data plane - tenants, management auth, usage events, credit pre-check".

- [ ] **Step 2: `docs/deployment/environment-variables.mdx`**

Gateway table: add rows

| `CONTROL_PLANE_TOKEN` | none | Bearer token presented on every control-plane call. Required when the control plane enforces it (production). |
| `LAB_URL` / `LAB_INSTANCE_ID` / `LAB_INSTANCE_SECRET` | none | All three or none. Enables the identity check at boot (a 401 aborts boot) and the credit pre-check (`402` out of credits, `503` when the Lab cannot be asked). See [Meilisearch Lab](/deployment/meilisearch-lab). |

Worker table: add `CONTROL_PLANE_TOKEN` (same wording) and change the `LAB_EVENTS_ENABLED` row to "`true` makes the usage step write one `usage.recorded` and one `job.completed`/`job.failed` event per Lab job to the control plane's outbox."

Control plane table: replace the `LAB_EVENTS_SECRET` row with

| `LAB_INSTANCE_ID` / `LAB_INSTANCE_SECRET` | none | This deployment's Lab credentials; sign each batch (`X-Lab-Signature` over `"<timestamp>.<body>"`). Required with `LAB_URL`. Never logged. |
| `LAB_EVENTS_SECRET` | none | **Deprecated.** The pre-v2 global secret; accepted with a boot warning when the instance credentials are unset. Removed next release. |
| `CONTROL_PLANE_TOKEN` | none (required) | Bearer token `/internal/*` callers must present. `CONTROL_PLANE_TOKEN_DISABLED=true` runs without it (dev only). |

- [ ] **Step 3: Tinybird is analytics**

`docs/concepts/usage.mdx`: rename "## Which table to bill from" to "## Which table to read", change the table's "Use for" column to `drill-down, audits` / `dashboards, live estimates` / `**reports, reconciliation**`, replace "invoices" and "invoice record" with "the reconciliation record" in lines 77-86, and rewrite "## Analytics and billing" (93-102) as "## Analytics and the Lab" with: "Tinybird rows are analytics: the dashboard, `GET /usage`, and a way to reconcile what the Lab billed. They carry no price. Lab events are what the Lab bills from (hosted engines) or displays (BYO instances); glutony reports raw units and `provider_cost_micro_usd` and never computes credits." Keep the provider-cost paragraph (104-110), replacing "set yours before billing" with "set yours before reporting to a Lab".

`README.md` lines 296-301: "Every job records per-tenant usage … into Tinybird for analytics (dashboards, `GET /usage`, reconciliation). Billing is the Meilisearch Lab's: see [docs/deployment/meilisearch-lab.mdx]. … Read `usage_daily_billing` for deduplicated daily totals" (drop "Bill from").

`tinybird/pipes/tenant_usage.pipe` header: "ENDPOINT: the per-tenant usage API over the daily rollup" and "deduplicated and safe to reconcile from" / "for a usage export" instead of "safe to invoice from" / "for a billing export". `tinybird/README.md`: replace "billing API" (line 17), "invoice-relevant" (132), "invoice reads" (259), "**invoices**" (295) with "usage API", "reconciliation-relevant", "reconciliation reads", "**reports**". `docs/concepts/multi-tenancy.mdx` step 7 (line 86): append "(standalone only: never with a tenant)".

- [ ] **Step 4: Check and commit**

Run: `grep -rn -i "invoice\|bill from\|billing API" README.md docs tinybird | grep -v "usage_daily_billing"` → expect no output; `cd docs && npx mintlify broken-links` if Mintlify is installed (optional).

```bash
git add docs README.md tinybird
git commit -m "docs: Lab contract v2 deployment guide, env vars, Tinybird is analytics"
```

---

## Self-review

**Spec coverage.** §2 A (hosted only, no BYO) → Tasks 4, 6 (one `InstanceKind`, no account_id, pre-check whenever credentials are set). §3.3 env names → Tasks 4, 5, 6; legacy `LAB_EVENTS_SECRET` with warning → Task 5. §3.4 headers → Task 4 (`authorize`, `sign_batch`), Task 5 (batches), Task 6 (service calls). §3.5 never-acknowledged events dropped after 24 h → Task 5 (`maintain`, `drop_stale`). §3.6 `GET /internal/instances/me` at boot in both binaries, 401 aborts boot → Tasks 5, 6. §4.1-4.4 schema, units, job events → Tasks 1, 2; vendoring/drift (§2 C) → Task 3. §8.1 pre-check: 402 on no credits, 503 past the stale window and before the identity is confirmed → Task 6. Audit gaps: control-plane auth → Task 7; isolation (global connections, `MEILI_URL` fallback) → Task 8; release/image/project files → Task 9; k8s secrets/replicas → Task 10; docs/billing wording → Task 11. Not in scope and said so: the rename (§10), Temporal namespace/task-queue sharing (audit note; no spec requirement), Lab-side work.

**Placeholder scan.** No TBD/TODO. Every code step shows the code. Task 8 Step 1 names a fallback for the `POST /internal/plugins` body shape with the exact alternative; Task 6 Step 5 tells the implementer which schema name to reuse and how to find it.

**Type consistency.** `LabCredentials::new(url, id, secret)`, `from_values(url, id, secret)` (Task 4) vs `LabConfig::from_values(url, id, secret, legacy)` (Task 5): different types, different arity, both used consistently in their tests. `InstanceInfo` fields (`instance_id, kind, product, region, lab_url`) match in Tasks 4, 5 and 6; `InstanceKind` has the single variant `Hosted` everywhere; `LabClient` has no `is_hosted`. `LabEventData` variants and `lab_events_for_job` are the same in Tasks 1, 2 and the worker. `ControlPlane { http, base_url, token }` in Task 7 and Task 8 tests. `CpError::Unauthorized` is unit-like everywhere. `GatewayError::PaymentRequired(String)` everywhere.

**Review Focus.** All five lines name their pinning test and task: (1) Task 6 `check_credits_fails_closed_until_the_identity_is_known` + `resolve_identity_retries_until_the_lab_answers`; (2) Task 5 `maintain_drops_rows_never_acknowledged_for_24h_and_counts_them`; (3) Task 1 cost cap and non-finite audio tests; (4) Task 8 `a_tenant_lookup_asks_for_the_tenants_row_only`, `a_tenant_pipeline_cannot_name_a_global_connection`; (5) Task 7 `control_plane_token_policy` and the compose default.

**Decisions taken while planning (confirm with the owner):**
- `ocr_pages` is sent as 0 until a plugin reports OCR'd pages; the PDF extractor's `pages` go out as an extra unpriced unit (billing them at the OCR price would overcharge).
- `provider_cost_micro_usd` is 0 when any call was unpriced (owner's instruction); the description carries "provider cost incomplete".
- Cancelled jobs are `job.failed` with `error_message: "cancelled"`.
- The 24 h drop is by `created_at`; `maintain` runs after the first delivery pass, hourly.
- Pre-check fails closed (503 `lab_unavailable`) both when the Lab is down for more than 300 s (matching Scrapix) and before the Lab has confirmed the credentials at boot: with every job billed there is no availability argument for running one unchecked. Boot itself is not blocked, except by a 401 (spec §3.6).
- The control plane requires the token on `/internal/*` only (as asked); `/pipelines`, `/plugins`, `/jobs` stay internal-but-open, and the gateway/worker send the token on every call so widening is a one-line change.
- The published image is `ghcr.io/qdequele/glutony`; the two unbuilt plugin sidecar images are removed from `workers.yaml`.
