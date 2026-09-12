# Tinybird resources for meili-ingest usage metering

These datafiles define the analytics side of per-tenant usage metering. The producing
side is the `meili-ingest-usage` crate (`crates/usage`), which the worker calls from a
Temporal activity after every finished job.

```
tinybird/
├── datasources/
│   ├── meili_ingest_usage.datasource   # raw rows, one per step attempt + one per job
│   └── usage_daily.datasource          # daily rollup (AggregatingMergeTree)
├── pipes/
│   ├── usage_daily.pipe                # MATERIALIZED: raw → rollup
│   └── tenant_usage.pipe               # ENDPOINT: billing API over the rollup
└── README.md
```

> **Not validated against a live workspace.** These datafiles were written to the
> documented Tinybird datafile format, but the Tinybird CLI was not available in the
> environment where they were authored, so they have never been pushed, linted or run.
> Treat the first `tb push` / `tb deploy` as the real syntax check, in a branch or a
> staging workspace, and expect to adjust details (see *Assumptions* at the bottom).

## 1. Install and authenticate

```bash
pip install tinybird-cli          # `tb` command
tb auth                           # paste a workspace ADMIN token when prompted
tb auth --region eu_central_1     # or pick the region interactively / by name
```

`tb auth` writes `.tinyb` in the working directory (it holds a token — keep it out of
git; add `tinybird/.tinyb` to `.gitignore` if you authenticate from this directory).

## 2. Push the resources

This repository assumes the **classic CLI workflow** (`tinybird-cli`, `tb push`), which
is what the datafile format below targets:

```bash
cd tinybird
tb push datasources/meili_ingest_usage.datasource
tb push datasources/usage_daily.datasource
tb push pipes/usage_daily.pipe          # creates the materialized view
tb push pipes/tenant_usage.pipe         # publishes the endpoint
# or simply: tb push --push-deps
```

If you are on the **newer CLI** (`tb` 5.x / Tinybird Forward), the same datafiles are
deployed with a project-level command instead:

```bash
tb login
tb --cloud deploy      # validates the whole project and deploys it
tb dev                 # local container running the same datafiles
```

Either way the datafiles are the same artefacts; only the deploy verb differs. Pick one
per workspace and stick to it.

Materializations do **not** backfill by default: `tb push pipes/usage_daily.pipe` starts
aggregating rows inserted from that moment on. To populate the rollup from existing raw
data, add `--populate` (classic CLI) and wait for the job to finish.

## 3. Get the append token for `TINYBIRD_TOKEN`

The worker needs a token with **APPEND** scope on `meili_ingest_usage` — not the admin
token.

```bash
# classic CLI: pushing a data source creates a "<name>_append" token for it
tb token ls
tb token copy meili_ingest_usage_append
```

or in the UI: *Tokens → Create token → scope `DATASOURCES:APPEND` on
`meili_ingest_usage`*. For reading `tenant_usage` from a dashboard, create a separate
token with `PIPES:READ` on that endpoint — never ship the append or admin token to a
client.

Worker configuration:

```bash
TINYBIRD_TOKEN=p.ey...            # append token; unset ⇒ usage reporting is simply off
TINYBIRD_BASE_URL=https://api.eu-central-1.aws.tinybird.co
TINYBIRD_DATASOURCE=meili_ingest_usage
TINYBIRD_TIMEOUT_SECS=20
```

### The base URL is regional

`https://api.tinybird.co` is **not** universal. Each workspace lives in one region and
only answers on that region's host:

| region | API host |
|---|---|
| US East (default `api.tinybird.co`) | `https://api.tinybird.co` |
| EU Central (AWS) | `https://api.eu-central-1.aws.tinybird.co` |
| US East (AWS, explicit) | `https://api.us-east.aws.tinybird.co` |
| GCP regions | `https://api.<region>.gcp.tinybird.co` |

Posting to the wrong host authenticates against the wrong workspace and fails with 401
or 403. `tb auth --list-regions` (classic) prints the exact host for your workspace;
whatever it prints goes into `TINYBIRD_BASE_URL`. That is why the crate has no
hardcoded host beyond a default.

## 4. Verify rows are landing

```bash
tb sql "SELECT count() FROM meili_ingest_usage"
tb sql "SELECT project_id, kind, count() FROM meili_ingest_usage GROUP BY project_id, kind"

# most recent rows
tb sql "SELECT ts, kind, job_id, step_id, plugin, status, documents_out, llm_input_tokens
        FROM meili_ingest_usage ORDER BY ts DESC LIMIT 20 FORMAT JSON"

# schema drift: rows Tinybird refused to type
tb sql "SELECT count() FROM meili_ingest_usage_quarantine"
tb datasource ls   # quarantine tables show up as <name>_quarantine

# the rollup and the endpoint
tb sql "SELECT count() FROM usage_daily"
tb pipe data tenant_usage --project_id acme --date_from 2026-04-01 --date_to 2026-04-30
```

A manual smoke test of the exact request the crate makes:

```bash
curl -X POST "$TINYBIRD_BASE_URL/v0/events?name=meili_ingest_usage&wait=true" \
     -H "Authorization: Bearer $TINYBIRD_TOKEN" \
     -H "Content-Type: application/x-ndjson" \
     --data-binary $'{"event_id":"smoke:job","kind":"job","ts":"2026-04-01T10:00:09.000Z","job_id":"smoke","workflow_id":"ingest-smoke","project_id":"acme","region":"us-west","pipeline_uid":"builtin.pdf","pipeline_builtin":1,"index_name":"documents","step_id":"","plugin":"","task_queue":"workers-general","status":"succeeded","attempt":1,"branches":0,"duration_ms":9000,"input_bytes":1000,"input_mime":"application/pdf","documents_out":3,"llm_input_tokens":0,"llm_output_tokens":0,"llm_requests":0,"audio_seconds":0.0,"pages":3,"images":0,"external_requests":0,"error_kind":"","error":""}\n'
```

The reply is `{"successful_rows": 1, "quarantined_rows": 0}`. **A non-zero
`quarantined_rows` is a schema drift alarm**, not a transient error: the crate turns it
into a permanent `UsageError::Quarantined` so Temporal stops retrying and the mismatch
between `UsageEvent` and the `.datasource` schema surfaces immediately.

## 5. Why duplicates are harmless (and where they are not)

Delivery is at-least-once — Temporal retries the reporting activity until it succeeds —
so the same rows can arrive twice. `event_id` is deterministic
(`{job_id}:{step_id}:{attempt}`, `{job_id}:job`) and every other field, `ts` included,
is a pure function of the job's result, so a redelivery is byte-identical.
`meili_ingest_usage` is a `ReplacingMergeTree` sorted on `project_id, ts, event_id`:
duplicates collapse on merge.

Two consequences worth knowing:

* Deduplication happens **on merge**, so an exact count needs
  `SELECT … FROM meili_ingest_usage FINAL` (or `GROUP BY event_id`).
* A materialized view is a trigger on INSERT and does **not** see that deduplication, so
  a duplicate is counted twice in `usage_daily`. The rollup is for dashboards; an
  invoice should be computed from the raw table with `FINAL`.

## Assumptions to re-check on the first deploy

1. `ENGINE_VER "ts"` on a `ReplacingMergeTree` — the version column must be part of the
   schema (it is) and non-decreasing per key; identical redeliveries make this moot.
2. `ENGINE_TTL "toDateTime(ts) + INTERVAL 180 DAY"` — `ts` is a `DateTime64(3)`, hence
   the explicit `toDateTime`. Some ClickHouse versions accept the `DateTime64` directly.
3. RFC 3339 timestamps with a `Z` suffix and millisecond precision
   (`2026-04-01T10:00:09.000Z`) parse into `DateTime64(3)` on JSON ingestion.
4. `{{ String(x, required=True) }}` / `{{ Date(x, required=True) }}` template parameters
   without a default value.
5. `AggregateFunction(...)` state columns declared directly in a materialized target
   `SCHEMA >` block, with `TYPE MATERIALIZED` + `DATASOURCE` on the pipe.

## Which table do I read?

| Table | Freshness | Deduplicated | Use it for |
|---|---|---|---|
| `meili_ingest_usage` | real time | with `FINAL` | ad-hoc drill-down, audits |
| `usage_daily` (materialized) | real time | **no** | dashboards, live estimates |
| `usage_daily_billing` (hourly COPY) | up to 1 h | yes | **invoices** |

Usage reporting is an at-least-once Temporal activity, so the same event can arrive
twice. The raw table collapses duplicates because `event_id` is deterministic and the
engine is a `ReplacingMergeTree` — but a materialized view is a trigger on INSERT and
never sees that collapse, so `usage_daily` can over-count a redelivery. The
`usage_daily_billing` copy re-reads the raw table with `FINAL` every hour, which is why
it is the one to bill from.

Sanity-check the two against each other after a deploy:

```bash
tb sql "SELECT sum(llm_input_tokens) FROM meili_ingest_usage FINAL WHERE kind = 'step'"
tb sql "SELECT sum(llm_input_tokens) FROM usage_daily_billing"
```

They should agree. A persistent gap means duplicates are arriving and the retry path
deserves a look.
