# Lab-ready seams: tenants, management auth, billing events - design

Date: 2026-10-01 · Status: approved in brainstorming, awaiting written-spec review
Branch: `qdequele/glutony-meilisearch-prep-cd7d41`

## 1. Goal

Prepare glutony for the **Meilisearch Lab** without depending on it. Glutony
stays fully usable standalone, and gains the seams the Lab will plug into:

1. an opaque **tenant** on every owned row, which the Lab sets to its account id;
2. an **authenticated management API** the Lab calls on an account's behalf;
3. **billing events** in the Lab's `lab-events` envelope, from a durable outbox;
4. an **OpenAPI spec** complete enough for the Lab console to replace `ui/`.

Everything is tested against a fake Lab. Nothing here needs a running Lab.

Context, decided outside this repo (2026-09-28 to 2026-09-30):

- The Lab is one Rails control plane (`meilisearch/lab`) shared by Scrapix,
  Lumen and glutony: accounts, teams, keys, one credit ledger, SSO, a
  Meilisearch registry. It is transitional; Meilisearch Cloud's control plane
  replaces it later, so glutony depends on contracts, never on Lab internals.
- Data planes stay in their own repos. The Lab console replaces glutony's `ui/`.
- Scrapix reports usage as signed, idempotent `usage.recorded` events to
  `POST {LAB_URL}/internal/events`. Lumen does the same with `X-Lab-Signature`,
  reports **micro-USD cost plus units**, and lets the Lab convert to credits.
  The Lab owns `lab-events.schema.json`; products vendor it.
- Lumen is driven by the Lab over its admin API (push-sync), with no Lab call on
  the request path. Glutony follows the same model.

Success:

- Every pipeline, source, connection and job belongs to a `tenant_id` or to the
  global scope, and no tenant can read or change another tenant's rows.
- With `LAB_SERVICE_TOKEN` set, the Lab can manage any account's pipelines,
  sources and connections, and nothing else can without a token.
- Every job run for a Lab account produces exactly one `usage.recorded` event
  that reaches the Lab, even across Lab outages and worker crashes.
- The console team can build every screen from `docs/openapi.yaml` alone, and a
  test keeps the spec and the router in sync.
- A deployment that sets none of the new variables behaves as today, except
  that JSON says `tenant_id` where it said `project_id`.

## 2. Decisions

| # | Decision | Rejected |
|---|----------|----------|
| D1 | Scope is Lab-ready seams, no runtime Lab dependency | full integration (introspection, Lab Meilisearch registry); docs-only cleanup |
| D2 | Owner is an **opaque `tenant_id`** replacing `project_id` | `account_id` beside `project_id`; keep `project_id` and pass the account id in it |
| D3 | The Lab reaches management routes with a **service token plus a tenant header** | treat the Lab as another Envoy (one secret for edge trust and management); Caddy `forward_auth` |
| D4 | Billing goes to a **new lab-events sink beside Tinybird**; Tinybird stays for analytics | replace Tinybird; defer billing |
| D5 | Glutony reports **units plus `cost_micro_usd`**; the Lab owns credits and markup (Lumen D5) | glutony computes credits from a price table (Scrapix model) |
| D6 | Billing events go through a **durable outbox in the control plane's Postgres** | Temporal retries alone (today's usage activity gives up after 10 attempts) |
| D7 | `ui/` is **frozen, not deleted** | delete now; keep evolving |

## 3. Tenancy

### 3.1 The tenant

`tenant_id` is an opaque string glutony never interprets: a Lab account UUID
behind the Lab, a Cloud project id behind Cloud's Envoy, `NULL` standalone.
It is validated wherever it enters: 1 to 128 characters of `[A-Za-z0-9._:-]`,
otherwise `400 invalid_tenant`.

### 3.2 Database

Migration `migrations/0003_tenant_id.sql`:

```sql
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

Renames touch only the catalog: no rewrite, no downtime. Postgres tracks
expression-index columns by attribute number, so the `COALESCE(project_id, '')`
indexes follow the rename. The migration test checks that.

### 3.3 Code

Every `project_id` in `crates/` becomes `tenant_id`, including
`MeiliContext`, `PipelineWorkflowInput`, `JobUsageInput`, `SourceSchedule`,
the control-plane repositories and query parameters, and the router's
resolver. The tenant stays on `MeiliContext`: pulling it into its own type is
not needed for this goal.

**Temporal compatibility.** Running workflows and **source schedules** already
hold `project_id` in serialized inputs; a schedule's stored action lives
forever. Every struct that crosses Temporal renames the field with
`#[serde(alias = "project_id")]`: new payloads write `tenant_id`, old ones
still read. A test deserializes a captured pre-rename workflow input and a
pre-rename schedule action.

### 3.4 Where a request's tenant comes from

First match wins; nothing falls back to an untrusted source.

1. **An authenticated management principal** (§4): its tenant.
2. **A trusted edge**, only when the `X-Meili-Envoy-Secret` check passes as today:
   `X-Meili-Tenant-Id` (new), else `X-Meili-Project-Id` (kept, so Cloud's
   Envoy contract does not break).
3. Otherwise no tenant: the global scope.

Ingest traffic reaches glutony on a Meilisearch hostname, not through the Lab.
So a pipeline a user builds in the Lab console (stored under their account id)
only matches their ingest traffic if the Lab's edge injects
`X-Meili-Tenant-Id: <account uuid>`. The Lab can do that: its registry knows
which account owns each instance. Glutony only cares that the edge is trusted.

### 3.5 What keeps its old name

- **Tinybird** columns stay `project_id`. Renaming a Tinybird column means a new
  datasource and a backfill. `UsageEvent` serializes its `tenant_id` field as
  `project_id` (`#[serde(rename = "project_id")]`), with a comment saying why.
- **`X-Meili-Project-Id`**, see §3.4.

### 3.6 Public API

JSON fields `project_id` become `tenant_id` in requests and responses. This is
a breaking change, accepted because glutony is pre-1.0 and the only client is
the frozen `ui/`, which is renamed in the same change.

## 4. Management API auth

### 4.1 Route classes

**Data routes** keep today's model (the caller's Meilisearch key, trusted edge
headers): `/health`, `/ingest`, `/ingest/batch`, `/ingest/pipeline/{name}`,
`/indexes/{index_uid}/ingest…`, and `GET /jobs/{id}`, the status poll that
ingest callers use on the public hostname.

One change: when the request has a tenant (§3.4), `GET /jobs/{id}` returns
`404` unless the job's `tenant_id` matches. The job id is no longer the only
protection.

**Management routes** require a `ManagementAuth` extractor: `/pipelines…`
(including `/pipelines/validate`), `/connections…`, `/sources…`, `GET /jobs`
(the list), `POST /jobs/{id}/cancel`, `/usage`, `/plugins`, `/catalog`.

### 4.2 Modes

| Configuration | Caller sends | Principal |
|---|---|---|
| `LAB_SERVICE_TOKEN` set | `Authorization: Bearer <token>` and **required** `X-Glutony-Tenant-Id` | `Lab { tenant }` |
| `ADMIN_API_KEY` set | `Authorization: Bearer <key>`, `X-Glutony-Tenant-Id` optional | `Admin { tenant: Option }` (global without the header) |
| neither | nothing | `Open`: today's behavior, tenant from §3.4 step 2; startup warning |

- Both may be set: the Lab and an operator side by side. The token that matches
  decides the principal.
- Tokens are compared in constant time (`subtle`), never logged, and shown as
  `<redacted>` in `Debug`, like `envoy_trusted_header` today.
- **When either is set, trusted `X-Meili-*` headers grant no management access**
  and never set the tenant of a management request. The edge fronts ingest; it
  does not manage.
- Errors:
  - missing or wrong token: `401`, `WWW-Authenticate: Bearer`,
    `{"code": "unauthorized"}`;
  - Lab principal without a valid tenant header: `400 invalid_tenant`;
  - another tenant's row: `404`, the same answer as a row that does not exist.

### 4.3 Scoping for a principal with a tenant

- **Reads** return the tenant's rows plus global rows (`tenant_id IS NULL`),
  like built-in pipelines. Each row carries `"scope": "tenant" | "global"`
  (`"builtin"` for built-in pipelines).
- **Writes** only create or change the tenant's own rows. PATCH, DELETE, pause,
  unpause and run on a global row answer `404`.
- `GET /jobs`, `/sources/{uid}/runs`, `POST /jobs/{id}/cancel` and `/usage` are
  always filtered to the tenant.

`Admin` without a tenant is the global scope: it reads and writes global rows,
and reads every tenant's jobs (today's behavior).

### 4.4 Unchanged

- The **control plane** stays internal and unauthenticated. The gateway passes
  it the resolved tenant, as it does today. The docs repeat that it must never be
  exposed.
- The frozen **`ui/`** cannot send a management token, so it works in open mode
  only, which is documented. qdq-server stays in open mode behind its allowlist
  until the Lab console replaces the UI.

## 5. Billing events

### 5.1 Cost

- `UsageUnits` (`crates/plugin-sdk/src/types.rs`) gains
  `cost_micro_usd: u64` and `cost_complete: bool` (both `#[serde(default)]`;
  `cost_complete` defaults to `true` and `merge` ANDs it).
- The plugins that call a paid provider set them: `llm-enricher`,
  `image-captioner`, `audio-transcriber`, `jev-enricher`. Each knows its model.
- Prices come from a **provider cost table**, `config/provider-costs.toml`,
  overridable with `PROVIDER_COSTS_FILE`, loaded once per worker:

  ```toml
  # micro-USD. Keys are (plugin, model).
  [llm_enricher."gpt-4o-mini"]
  input_per_mtok = 150000
  output_per_mtok = 600000

  [audio_transcriber."whisper-1"]
  per_audio_second = 100

  [jev_enricher.default]
  per_request = 0
  ```

- A model missing from the table costs 0, logs a warning once per
  `(plugin, model)`, and sets `cost_complete = false`, so the Lab sees the gap
  instead of silently under-billing.
- Provider calls glutony does not pay for (a local gRPC plugin) cost 0 and stay
  complete.
- Tinybird rows do not change: cost only goes to the Lab.

### 5.2 Event

One event per finished job, built by a pure function in `crates/usage`
(`lab_event_for_job(&JobUsageInput) -> Option<LabEvent>`):

```json
{
  "id": "<UUIDv5(GLUTONY_LAB_NAMESPACE, \"job:{job_id}:usage\")>",
  "type": "usage.recorded",
  "occurred_at": "<finished_at>",
  "account_id": "<tenant_id>",
  "api_key_id": null,
  "product": "glutony",
  "data": {
    "job_id": "…",
    "pipeline_uid": "…",
    "source_uid": "… or null",
    "status": "succeeded | failed | cancelled",
    "duration_ms": 1234,
    "cost_micro_usd": 1834,
    "cost_complete": true,
    "units": {
      "documents_out": 12, "input_bytes": 482133, "pages": 9, "images": 0,
      "audio_seconds": 0.0, "llm_input_tokens": 8120, "llm_output_tokens": 950,
      "llm_requests": 12, "external_requests": 0
    }
  }
}
```

- `GLUTONY_LAB_NAMESPACE` is a fixed UUID constant, so the id is deterministic
  and every retry carries the same one.
- Returns `None` (no event) when `tenant_id` is empty or not a UUID: standalone
  jobs and Cloud-project tenants are never billed to the Lab. The worker logs
  one `debug` line per skipped job, with the reason (`no_tenant`,
  `not_a_uuid`).
- Failed and cancelled jobs are sent too: provider cost already spent is real.
  The Lab decides what to bill.
- Units are summed from step rows, like `usage_daily.pipe`; `documents_out` is
  the final step's, like the Tinybird job row.
- No document content, prompt, index name or Meilisearch key is carried.

### 5.3 Outbox

Migration `migrations/0004_lab_events.sql`:

```sql
CREATE TABLE IF NOT EXISTS lab_events (
    id            UUID PRIMARY KEY,
    body          JSONB NOT NULL,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_attempt  TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS lab_events_due ON lab_events (next_attempt)
    WHERE delivered_at IS NULL;
```

- New control-plane route `POST /internal/lab-events` takes `{"events": [...]}`
  and inserts with `ON CONFLICT (id) DO NOTHING`.
- The worker's `record_usage` activity (`crates/worker/src/activity.rs`) already
  calls the control plane (`patch_job`). When lab events are enabled on the
  worker (`LAB_EVENTS_ENABLED=true`), it builds the event and posts it
  **before** sending to Tinybird. A Tinybird failure then cannot keep a bill
  from being recorded, and a retry re-inserts the same id harmlessly.
- The route always accepts, whether or not the control plane has `LAB_URL`.
  If a worker is enabled before the control plane is, rows wait in the outbox
  and are delivered once `LAB_URL` is set: a configuration mismatch loses
  nothing.
- With lab events enabled, the usage activity's retry policy becomes unlimited
  attempts with a 5-minute maximum interval (instead of `USAGE_MAX_ATTEMPTS` =
  10). The workflow stays open until the control plane accepts the event, so
  no loss path is left short of losing the control plane's database. The job
  row's status is still written first, so billing never holds back a job's
  status.

### 5.4 Sender

A task in the control plane, spawned only when `LAB_URL` is set:

- every 2 s, and right after an insert, select up to 500 rows with
  `delivered_at IS NULL AND next_attempt <= now()` ordered by `created_at`
  (`FOR UPDATE SKIP LOCKED`, so several control-plane replicas do not send the
  same batch);
- `POST {LAB_URL}/internal/events`, body `{"events": [<body>, …]}`, headers
  `Content-Type: application/json` and
  `X-Lab-Signature: sha256=<hex HMAC-SHA256(LAB_EVENTS_SECRET, raw body)>`;
- `200` with `{"accepted": [ids]}`: mark those ids delivered; ids not listed stay
  pending with backoff;
- anything else (connect error, timeout, non-`200`, malformed body): the batch
  stays pending, `attempts += 1`,
  `next_attempt = now() + min(2^attempts s, 300 s)` with ±20 % jitter;
- rows are **never dropped**; delivered rows older than 7 days are purged hourly;
- no redirects followed; rustls; 2 s connect and 10 s overall timeout;
- a Lab `401` logs at `error` and keeps retrying. The Lab can never prevent
  glutony from starting or serving.

### 5.5 Metrics

The control plane gets a `GET /metrics` route (Prometheus text format; none of
glutony's services has one today):

- `glutony_lab_events_pending` (gauge)
- `glutony_lab_events_oldest_pending_seconds` (gauge)
- `glutony_lab_events_delivered_total` (counter)
- `glutony_lab_events_failed_total{reason}`, `reason` in `connect`, `timeout`,
  `auth`, `status`, `malformed`, `not_accepted`

Alert, same threshold as Lumen's: `glutony_lab_events_oldest_pending_seconds >
900` for 5 minutes. Glutony has no Prometheus rules of its own, so the rule is
documented in `docs/deployment/meilisearch-lab.mdx` for the deployment's
monitoring (qdq-server's `monitoring/`) to load.

### 5.6 Configuration

| Variable | Service | Notes |
|---|---|---|
| `LAB_URL` | control plane | Lab base URL; enables the sender |
| `LAB_EVENTS_SECRET` | control plane | HMAC key; required with `LAB_URL`, boot error otherwise; never logged |
| `LAB_EVENTS_ENABLED` | worker | `true` makes `record_usage` write lab events |
| `PROVIDER_COSTS_FILE` | worker | defaults to the bundled `config/provider-costs.toml` |
| `LAB_SERVICE_TOKEN` | gateway | §4.2 |
| `ADMIN_API_KEY` | gateway | §4.2 |

- `LAB_URL` must be `https` unless its host is loopback or a private address.
- `LAB_URL` without `LAB_EVENTS_SECRET`, or the reverse, is a boot error.
- With none of these set: no outbox rows, no sender task, no outbound calls.

### 5.7 Contract

- Glutony vendors the Lab-owned schema at
  `contracts/vendor/lab/lab-events.schema.json`. The starting point is
  **Lumen's vendored copy** (Lumen `contracts/lab-events.schema.json`, ADR 015,
  `$id` `https://lab.meilisearch.com/contracts/lab-events.schema.json`),
  unchanged except for three additions: `"glutony"` in the `product` enum,
  a `glutonyUsageData` definition matching §5.2 (`cost_micro_usd` minimum 0,
  since a zero-cost job still carries billable units), and two `if/then`
  rules (glutony `usage.recorded` data is `glutonyUsageData`; glutony only
  sends `usage.recorded`). The result is the one three-product schema the Lab
  adopts, rather than two diverging proposals.
- A unit test validates `lab_event_for_job` output against it, with the same
  dev dependency and format validation Lumen uses
  (`jsonschema = { version = "0.45", default-features = false }`).
- A CI job diffs the vendored copy against the Lab's published file. It is
  added now but disabled (`if: false`, with a comment) until the Lab publishes.

### 5.8 Lab-side changes (listed, not built here)

1. Schema: allow `product: "glutony"`; add `glutonyUsageData`.
2. Receiver: accept `X-Lab-Signature` (Lumen needs it too).
3. Ledger: convert `cost_micro_usd` and units to credits at the Lab's rates.
4. Secrets: one events secret and one service token per glutony deployment.
5. Edge: inject `X-Meili-Tenant-Id: <account uuid>` and `X-Meili-Envoy-Secret`
   on glutony ingest routes of Lab-registered instances.
6. Console: call glutony's management routes with `LAB_SERVICE_TOKEN` and
   `X-Glutony-Tenant-Id`, server side only.

## 6. OpenAPI and the frozen UI

### 6.1 `docs/openapi.yaml`

- Add the missing routes: `GET /jobs`, `POST /pipelines/validate`, `GET /usage`.
- Add security schemes `MeiliKey`, `LabServiceToken` (with the
  `X-Glutony-Tenant-Id` header parameter) and `AdminKey`, and a `security`
  block on every operation per §4.1.
- `project_id` becomes `tenant_id`; rows gain `scope`.
- Document `401`, `400 invalid_tenant` and cross-tenant `404`.

### 6.2 Route parity test

`crates/gateway/tests/openapi_parity.rs` builds the list of `(method, path)`
the router mounts, from a single route table the router is built from, and
compares it with the operations in `docs/openapi.yaml`. A route missing from
the spec, or a spec path the router does not serve, fails the test. The `ui`
routes are excluded by name.

### 6.3 Freezing `ui/`

- A banner at the top of `ui/AGENTS.md`, `ui/CLAUDE.md` and the README's
  "Admin UI" section: frozen, bug fixes only; new screens go to the Meilisearch
  Lab console; works in open mode only.
- The only change to `ui/` in this work is the `tenant_id` rename.

## 7. Documentation (Mintlify)

- `docs/concepts/multi-tenancy.mdx`: rewritten around tenants, the tenant
  sources (§3.4), and scoping (§4.3).
- New `docs/deployment/meilisearch-lab.mdx`: the three auth modes, the tenant
  header, enabling lab events, the event shape, the Lab-side checklist (§5.8).
  Added to `mint.json`.
- `docs/deployment/environment-variables.mdx`: the variables of §5.6.
- `docs/concepts/usage.mdx`: Tinybird for analytics, lab events for the bill;
  the provider cost table.
- `docs/deployment/meilisearch-cloud.mdx`: `X-Meili-Tenant-Id`, and that
  `X-Meili-Project-Id` still works.

## 8. Error handling

| Situation | Behavior |
|---|---|
| Management call, auth configured, no or wrong token | `401` |
| Lab principal without a valid `X-Glutony-Tenant-Id` | `400 invalid_tenant` |
| Another tenant's pipeline, source, connection, job or run | `404` |
| PATCH/DELETE/run on a global row as a tenant | `404` |
| Invalid tenant from a trusted edge header | `400 invalid_tenant` |
| Lab unreachable or `5xx` | Rows stay pending with backoff; serving unaffected |
| Lab `401` | `error` log, `failed_total{reason="auth"}`, keep retrying |
| Event not in `accepted` | Retried forever; visible as the oldest pending row |
| Control plane down during `record_usage` | Activity retries (unlimited with lab events on) |
| Worker has lab events on, control plane has no `LAB_URL` | Rows wait in the outbox; delivered once `LAB_URL` is set |
| Unknown model in the cost table | Cost 0, `cost_complete: false`, one warning per model |
| Pre-rename Temporal payload | Read through the `project_id` alias |

## 9. Testing

- **Tenancy:** the migration renames columns and indexes and keeps values; the
  unique indexes still enforce one row per `(uid, tenant)`; pre-rename workflow
  input and schedule action deserialize; tenant validation; edge header
  precedence (`X-Meili-Tenant-Id` over `X-Meili-Project-Id`), trusted only with
  the Envoy secret.
- **Management auth**, one table-driven test over every management route: each
  mode accepts and refuses the right callers; Lab without a tenant header is
  `400`; cross-tenant GET, PATCH, DELETE, cancel and runs are `404`; global rows
  readable, not writable; edge headers grant nothing while auth is on; open mode
  unchanged; tokens absent from `Debug` output and logs.
- **`GET /jobs/{id}`:** matching tenant `200`, other tenant `404`, no tenant
  unchanged.
- **Billing:** event shape validated against the vendored schema;
  deterministic ids; no event for an empty or non-UUID tenant; duplicate insert
  is a no-op; cost table (known model, unknown model, local plugin); `merge`
  ANDs `cost_complete`.
- **Sender, against an in-process fake Lab** (axum): signature header format and
  value; batches of at most 500; partial `accepted`; backoff on `5xx`; retry on
  `401`; redirects not followed; nothing dropped; purge after 7 days; two
  senders never deliver the same row twice (`SKIP LOCKED`).
- **Nothing configured:** no rows written, no outbound request.
- **Rollback script:** applied to a migrated database, the previous binary's
  migrations and control-plane tests pass.
- **OpenAPI parity** (§6.2).
- **End to end:** `scripts/e2e.sh --lab` starts the fake Lab
  (`scripts/fake_lab.py`, beside `fake_tinybird.py`), sets the `LAB_*`
  variables, creates a pipeline as tenant A over the service token, ingests as
  tenant A through trusted edge headers, and asserts one signed
  `usage.recorded` event for A arrives, and that tenant B sees neither A's
  pipeline nor A's job.

## 10. Rollout

1. Ship. Migrations `0003` and `0004` run at control-plane startup.
2. qdq-server needs no configuration change: open mode behind its allowlist,
   no `LAB_*` set. The visible change is `tenant_id` in JSON, and the `ui/`
   ships renamed in the same build.
3. Later, the first Lab hookup: set `LAB_SERVICE_TOKEN` on the gateway,
   `LAB_URL` and `LAB_EVENTS_SECRET` on the control plane,
   `LAB_EVENTS_ENABLED=true` on the workers, and have Caddy inject
   `X-Meili-Tenant-Id` on the ingest routes. Setting `LAB_SERVICE_TOKEN` closes
   open mode, so the admin hostname's Caddy block changes for the frozen UI:
   it sends `Authorization: Bearer <ADMIN_API_KEY>` for management, and moves
   the `glutony-admin-ui` Meilisearch key to `X-Meili-Api-Key` with
   `X-Meili-Host` and `X-Meili-Envoy-Secret`. A trusted `X-Meili-Api-Key`
   already wins over the bearer token on data routes (the resolution order in
   `docs/concepts/multi-tenancy.mdx`), so ingest from the UI keeps using the
   Meilisearch key.

**Rollback.** `sqlx` refuses to start a binary that does not know an applied
migration, so going back to the previous binary needs
`scripts/rollback-lab-seams.sql`: it renames the columns and indexes back,
drops `lab_events`, and deletes the `0003` and `0004` rows from
`_sqlx_migrations`. A rollback loses undelivered events, so the script
refuses to run while `lab_events` has undelivered rows unless the session setting
`glutony.rollback_force = on` is set (`PGOPTIONS='-c glutony.rollback_force=on'`; see
the amendments in §12). It is run with
`psql "$DATABASE_URL" --single-transaction -v ON_ERROR_STOP=1 -f scripts/rollback-lab-seams.sql`,
once, with every glutony service stopped: it is not idempotent.

## 11. Out of scope

- Credential introspection; Lab API keys accepted by glutony.
- Replacing `meili_connections` with the Lab's Meilisearch registry.
- The Lab calling ingest routes on an account's behalf.
- Deleting `ui/`.
- Per-model cost breakdown in events.
- All Lab-side work (§5.8).
- **Spend enforcement.** Unlike Lumen (a lease the Lab tops up, `402` when it
  runs out) and Scrapix (a balance pre-check, `402`), glutony never refuses
  work for lack of credits: it bills after the fact. Because glutony pays the
  LLM and transcription providers itself, an account at zero credits can
  still run up real cost, which the Lab can only record as debt. Accepted
  while glutony is not sold through the Lab. The follow-up is a tenant
  suspend switch or a Lumen-style lease, decided before the first paying
  Lab account uses glutony. `docs/deployment/meilisearch-lab.mdx` states
  this limit.

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

Recorded while implementing:

6. **§4.1:** with management auth on, `GET /jobs/{id}` accepts either a valid
   management token (scoped by its `X-Glutony-Tenant-Id`; an admin without one sees
   every job) or a trusted edge tenant, and anything else is `401`. With auth off it
   is scoped by the trusted edge tenant as described in §4.1.
7. **§5.4, §5.5:** the control plane refreshes the pending gauges on every
   `GET /metrics` scrape, so they are correct even when `LAB_URL` is unset, and the
   sender reads at most 1 MiB of the Lab's acknowledgement body.
