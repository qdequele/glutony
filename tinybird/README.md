# Tinybird resources for meili-ingest usage metering

These datafiles define the analytics side of per-tenant usage metering. The producing
side is the `meili-ingest-usage` crate (`crates/usage`), which the worker calls from a
Temporal activity after every finished job. The consuming side is the gateway's
`GET /usage`, which reads the `tenant_usage` endpoint with `TINYBIRD_READ_TOKEN`.

```
tinybird/
├── datasources/
│   ├── meili_ingest_usage.datasource      # raw rows, one per step attempt + one per job
│   ├── usage_daily.datasource             # daily rollup (AggregatingMergeTree), real time
│   └── usage_daily_billing.datasource     # daily rollup (MergeTree), deduplicated, hourly
├── pipes/
│   ├── usage_daily_mv.pipe                # MATERIALIZED: raw → usage_daily
│   ├── usage_daily_billing_hourly.pipe    # COPY (hourly): raw FINAL → usage_daily_billing
│   └── tenant_usage.pipe                  # ENDPOINT: billing API over usage_daily_billing
└── README.md
```

Resource names (data sources, pipes and the nodes inside pipes) share one namespace in
a Tinybird workspace, which is why the pipes are not named after the data sources they
write to.

These files are built and deployed with the **Tinybird Forward CLI** (`tb`, verified
with 4.6) against Tinybird Local (`tinybirdco/tinybird-local`): rows appended with the
worker token flow through the materialization and the copy and come back out of
`tenant_usage` with the gateway token.

## 1. Install the CLI

```bash
curl -LsSf https://tbrd.co/fwd | sh   # or: uv tool install tinybird
tb --version
```

## 2. Deploy

### Locally (Tinybird Local)

```bash
tb local start --daemon            # runs the tinybirdco/tinybird-local container (API on :7181)
cd tinybird
tb --local build                   # validates the project server side
tb --local deploy                  # deploys it and mints the tokens below
```

Tinybird Local keys its workspace on the project directory, so deploying the same
directory twice is an update (`No changes` when nothing changed), and a copy of the
directory elsewhere gets a workspace of its own.

### Tinybird Cloud

```bash
tb login                           # browser login; pick the workspace and its region
cd tinybird
tb --cloud deploy                  # validates the whole project, then deploys it
```

`tb login` stores credentials in a `.tinyb` file, which holds a token: keep it out of
git (`tinybird/.tinyb` is in `.gitignore`).

### `fixtures/` is a by-product

`tb build` writes a `fixtures/` directory into the project. It is not part of the
project: `tinybird/fixtures/` is gitignored. Tooling that must leave the checkout
untouched (the Lab's `just pipelines-usage`, CI) can deploy from a copy instead:

```bash
tmp=$(mktemp -d) && cp -R tinybird/. "$tmp" && (cd "$tmp" && tb --local deploy)
```

A deployment that introduces `usage_daily_mv` backfills `usage_daily` from the rows
already in `meili_ingest_usage` (the deploy plan lists it under *Data that will be
copied*). The billing copy runs on its schedule (`17 * * * *`); to refresh it now:

```bash
tb --local copy run usage_daily_billing_hourly --wait     # or --cloud
```

### Changing a materialization: what a redeploy backfills

What a deployment does depends on what changed. Each case was verified on Tinybird
Local (tb 4.6.22) against a workspace that already held rows:

| change | what the deployment does | rows |
|---|---|---|
| comments only (`#` lines, `--` in SQL) | nothing: `No changes`, not deployed | untouched |
| a pipe's `DESCRIPTION` | redeploys the pipe; *No data will be copied* | untouched |
| `usage_daily_mv`'s **SQL** | rebuilds `usage_daily` by re-running the pipe over `meili_ingest_usage` (*Backfill type: Using Materialized Pipe usage_daily_mv*) | `usage_daily` recomputed from the last 180 days, **under the new query** |
| `usage_daily_billing_hourly`'s SQL | redeploys the copy; no backfill, the next scheduled run uses the new query | untouched |
| `usage_daily_billing`'s schema | `ALTER TABLE` on promotion; no data movement | kept |
| a data source's `ENGINE_TTL` (e.g. upgrading a workspace deployed before `usage_daily` had one) | `ALTER TABLE` on promotion; no data movement | kept, then expired by the TTL |

The rebuild in the third row is why `usage_daily` carries
`ENGINE_TTL "day + INTERVAL 180 DAY"`. Tinybird refuses a materialized backfill whose
target would keep data longer than the source it is rebuilt from, since a rebuild
would silently drop every older day:

```
ERROR: Error on datasource 'datasources/usage_daily.datasource': it has no TTL, while the
table that is going to be used for backfill 'meili_ingest_usage' has a TTL of
toDateTime(ts) + INTERVAL 180 DAY.
```

A TTL *longer* than the source's is refused the same way ("it has a longer TTL"). So
`usage_daily`'s TTL must stay at or below `meili_ingest_usage`'s. If you change one,
change both. Without it, the first deployment that touched the pipe's SQL failed on
any workspace holding rows. An empty workspace never showed it, because there was
nothing to backfill.

The error message lists two other ways out. Both keep `usage_daily`'s old days
computed by the **old** query, so they are deliberately not used here:

* `FORWARD_QUERY > SELECT *` on `usage_daily` copies the live table instead of
  recomputing it.
* `DEPLOYMENT_METHOD alter` on the pipe swaps the query in place with
  `ALTER TABLE … MODIFY QUERY` and touches no stored row. It also only works once the
  view exists.

Reach for one only for a change that must not touch history, and expect a dashboard
that mixes old and new semantics. `--allow-destructive-operations` is never needed with
the TTL in place.

**What to expect in production (Tinybird Cloud).** `tb --cloud deploy --check` prints
the same plan without deploying: look for `usage_daily` under *Data that will be
copied*. A real deployment builds a staging deployment, rebuilds `usage_daily` there
from up to 180 days of raw rows, and promotes it once the backfill finishes. The live
deployment keeps serving until then, and the backfill's progress shows on the
deployment's *Data movements* tab. The cost is one aggregation over the raw table,
which Tinybird may run on on-demand compute for large tables. Nothing
invoice-relevant moves: `tenant_usage` reads `usage_daily_billing`, which no
deployment recomputes. Only the real-time dashboard rollup is rebuilt.

## 3. Tokens

Every deployment mints two scoped static tokens declared in the datafiles; neither is
the admin token, and neither can do the other's job:

| token | declared in | scope | goes into |
|---|---|---|---|
| `glutony_worker_append` | `meili_ingest_usage.datasource` | `APPEND` on `meili_ingest_usage` | worker `TINYBIRD_TOKEN` |
| `glutony_gateway_read` | `tenant_usage.pipe` | `READ` on `tenant_usage` | gateway `TINYBIRD_READ_TOKEN` |

```bash
tb --local token ls                              # or --cloud; prints each token's value
tb --local token copy glutony_worker_append      # copies the value to the clipboard
tb --local token copy glutony_gateway_read
```

The append token gets a 403 on the endpoint and the read token a 403 on `/v0/events`.
Never ship either, nor the admin token, to a browser: the dashboard reads usage through
the gateway, which scopes the query to the caller's tenant.

Worker configuration:

```bash
TINYBIRD_TOKEN=p.ey...            # glutony_worker_append; unset ⇒ usage reporting is simply off
TINYBIRD_BASE_URL=https://api.eu-central-1.aws.tinybird.co   # http://localhost:7181 for Local
TINYBIRD_DATASOURCE=meili_ingest_usage
TINYBIRD_TIMEOUT_SECS=20
```

Gateway configuration:

```bash
TINYBIRD_READ_TOKEN=p.ey...       # glutony_gateway_read; unset ⇒ GET /usage answers "not enabled"
TINYBIRD_BASE_URL=https://api.eu-central-1.aws.tinybird.co   # http://localhost:7181 for Local
TINYBIRD_USAGE_PIPE=tenant_usage  # the default
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
or 403. `tb --cloud info` prints the exact API host for your workspace; whatever it
prints goes into `TINYBIRD_BASE_URL`. That is why the crate has no hardcoded host
beyond a default.

## 4. Verify rows are landing

Use `--local` or `--cloud` on every `tb` command, matching where you deployed.

```bash
tb --local sql "SELECT count() FROM meili_ingest_usage"
tb --local sql "SELECT project_id, kind, count() FROM meili_ingest_usage GROUP BY project_id, kind"

# most recent rows
tb --local sql "SELECT ts, kind, job_id, step_id, plugin, status, documents_out, llm_input_tokens
                FROM meili_ingest_usage ORDER BY ts DESC LIMIT 20"

# schema drift: rows Tinybird refused to type
tb --local sql "SELECT count() FROM meili_ingest_usage_quarantine"

# the rollups and the endpoint
tb --local sql "SELECT count() FROM usage_daily"
tb --local sql "SELECT count() FROM usage_daily_billing"
tb --local endpoint data tenant_usage --project_id acme --date_from 2026-10-01 --date_to 2026-10-31
```

A manual smoke test of the exact request the crate makes. The row is stamped "now"
because the billing copy only recomputes the current month (and the previous one for
two days after a month boundary), so an older `ts` never reaches `tenant_usage`:

```bash
TINYBIRD_BASE_URL=http://localhost:7181          # or your region's API host
TINYBIRD_TOKEN=...                               # glutony_worker_append
TS=$(date -u +%Y-%m-%dT%H:%M:%S.000Z)
printf '{"event_id":"smoke:job","kind":"job","ts":"%s","job_id":"smoke","workflow_id":"ingest-smoke","project_id":"acme","region":"us-west","pipeline_uid":"builtin.pdf","pipeline_builtin":1,"index_name":"documents","step_id":"","plugin":"","task_queue":"workers-general","status":"succeeded","attempt":1,"branches":0,"duration_ms":9000,"input_bytes":1000,"input_mime":"application/pdf","documents_out":3,"llm_input_tokens":0,"llm_output_tokens":0,"llm_requests":0,"audio_seconds":0.0,"pages":3,"images":0,"external_requests":0,"error_kind":"","error":""}\n' "$TS" |
  curl -X POST "$TINYBIRD_BASE_URL/v0/events?name=meili_ingest_usage&wait=true" \
       -H "Authorization: Bearer $TINYBIRD_TOKEN" \
       -H "Content-Type: application/x-ndjson" \
       --data-binary @-
```

The reply is `{"successful_rows":1,"quarantined_rows":0}`. **A non-zero
`quarantined_rows` is a schema drift alarm**, not a transient error: the crate turns it
into a permanent `UsageError::Quarantined` so Temporal stops retrying and the mismatch
between `UsageEvent` and the `.datasource` schema surfaces immediately.

Then read it back the way the gateway does:

```bash
TINYBIRD_READ_TOKEN=...                          # glutony_gateway_read
tb --local copy run usage_daily_billing_hourly --wait
DAY=$(date -u +%Y-%m-%d)
curl -H "Authorization: Bearer $TINYBIRD_READ_TOKEN" \
     "$TINYBIRD_BASE_URL/v0/pipes/tenant_usage.json?project_id=acme&date_from=$DAY&date_to=$DAY"
```

`data` holds one row for `plugin = ""` with `jobs: 1`, `jobs_succeeded: 1` and
`documents_indexed: 3`.

## 5. Why duplicates are harmless (and where they are not)

Delivery is at-least-once — Temporal retries the reporting activity until it succeeds —
so the same rows can arrive twice. `event_id` is deterministic
(`{job_id}:{step_id}:{attempt}`, `{job_id}:job`) and every other field, `ts` included,
is a pure function of the job's result, so a redelivery is byte-identical.
`meili_ingest_usage` is a `ReplacingMergeTree` sorted on `project_id, ts, event_id`:
duplicates collapse on merge. It has no `ENGINE_VER`: since the copies are identical,
it does not matter which one the merge keeps.

Two consequences worth knowing:

* Deduplication happens **on merge**, so an exact count needs
  `SELECT … FROM meili_ingest_usage FINAL` (or `GROUP BY event_id`).
* A materialized view is a trigger on INSERT and does **not** see that deduplication, so
  a duplicate is counted twice in `usage_daily`. That rollup is for dashboards; an
  invoice reads `usage_daily_billing`, which the hourly copy rebuilds from the raw
  table with `FINAL`.

## Datafile decisions the first deploy settled

1. **No `ENGINE_VER`.** The `ReplacingMergeTree` used to declare `ENGINE_VER "ts"`, which
   Tinybird rejects because `ts` is in the sorting key (a version column inside the key
   would keep every version apart instead of collapsing them). The row identity is
   `project_id` + `event_id`; `ts` stays in the key for time-range pruning only, which
   is safe because it is deterministic. Redeliveries are byte-identical, so no version
   is needed to pick a winner.
2. **Qualified raw columns in aggregating pipes.** Both rollup pipes alias the source
   (`FROM meili_ingest_usage AS u`) and write `u.documents_out`, `u.kind`, … inside
   the aggregates. The output aliases reuse the raw column names, and ClickHouse
   resolves an unqualified name to the alias, so `sum(if(kind = 'job', documents_out,
   0)) AS documents_indexed` would nest one aggregate in another (copy pipe) or mix a
   `UInt64` with an `AggregateFunction` (materialized pipe).
3. **Unique resource names.** A pipe cannot share a name with a data source or with a
   node, hence `usage_daily_mv` and `usage_daily_billing_hourly`.
4. `ENGINE_TTL "toDateTime(ts) + INTERVAL 180 DAY"`, RFC 3339 timestamps with a `Z`
   suffix into `DateTime64(3)`, `{{ String(x, required=True) }}` /
   `{{ Date(x, required=True) }}` parameters, and `AggregateFunction(...)` columns in a
   materialized target all work as written.
5. **`usage_daily` expires with its source.** `ENGINE_TTL "day + INTERVAL 180 DAY"`
   matches `meili_ingest_usage`'s 180 days, so a deployment that changes
   `usage_daily_mv`'s query can rebuild it (§2, *Changing a materialization*). The
   second deploy that changed the pipe on a workspace holding rows failed without it.
   `usage_daily_billing` has no TTL: it is the invoice record and deployments never
   rebuild it.

## Which table do I read?

| Table | Freshness | Deduplicated | Retention | Use it for |
|---|---|---|---|---|
| `meili_ingest_usage` | real time | with `FINAL` | 180 days | ad-hoc drill-down, audits |
| `usage_daily` (materialized) | real time | **no** | 180 days | dashboards, live estimates |
| `usage_daily_billing` (hourly COPY) | up to 1 h | yes | indefinite | **invoices** |

Usage reporting is an at-least-once Temporal activity, so the same event can arrive
twice. The raw table collapses duplicates because `event_id` is deterministic and the
engine is a `ReplacingMergeTree` — but a materialized view is a trigger on INSERT and
never sees that collapse, so `usage_daily` can over-count a redelivery. The
`usage_daily_billing` copy re-reads the raw table with `FINAL` every hour, which is why
it is the one to bill from.

Sanity-check the two against each other after a deploy:

```bash
tb --local sql "SELECT sum(llm_input_tokens) FROM meili_ingest_usage FINAL WHERE kind = 'step'"
tb --local sql "SELECT sum(llm_input_tokens) FROM usage_daily_billing"
```

They should agree. A persistent gap means duplicates are arriving and the retry path
deserves a look.
