//! # `meili-ingest-usage` — per-tenant usage metering into Tinybird
//!
//! Postgres only holds the *mutable* status of a job, so nothing durable records how
//! much work a tenant actually consumed. This crate produces that record: one flat row
//! per pipeline step attempt plus one row per job, shipped to a
//! [Tinybird](https://www.tinybird.co) (managed ClickHouse) data source through the
//! Events API. Those rows are what billing and the per-project usage dashboards read.
//!
//! ## The delivery contract: at-least-once + `ReplacingMergeTree`
//!
//! Usage data is billing-grade, so losing it is not acceptable: the worker reports it
//! from a Temporal activity, and Temporal keeps retrying that activity until it
//! returns `Ok`. Two properties make that safe:
//!
//! 1. **Correct error classification.** [`UsageError::is_retryable`] tells the activity
//!    whether Temporal should try again (transport failures, timeouts, HTTP 429 and
//!    5xx) or give up immediately because a human has to fix something (bad token,
//!    rejected payload, schema drift). Retrying a permanent error just burns the retry
//!    budget and hides the bug.
//! 2. **Deterministic, idempotent rows.** Every event carries an `event_id` derived
//!    only from data the job already produced:
//!
//!    | row | `event_id` |
//!    |---|---|
//!    | step | `{job_id}:{step_id}:{attempt}` |
//!    | job  | `{job_id}:job` |
//!
//!    and every other field of a row — the timestamp included, see below — is a pure
//!    function of [`JobUsageInput`]. So calling [`events_for_job`] twice yields
//!    byte-identical rows, and the Tinybird data source
//!    (`tinybird/datasources/meili_ingest_usage.datasource`) is a `ReplacingMergeTree`
//!    sorted on `project_id, ts, event_id`: a duplicate **replaces** the earlier row
//!    on merge instead of double-counting it. That is the whole reason at-least-once
//!    delivery is safe here.
//!
//!    Note the coupling: ClickHouse replaces rows that share the *sorting key*, so
//!    `ts` has to be deterministic too. Every row of a job is stamped with
//!    [`JobUsageInput::finished_at`], never with `Utc::now()`.
//!
//! ## What is *not* in a usage row
//!
//! Never the Meilisearch API key. [`JobUsageInput`] deliberately takes `project_id` and
//! `region` as plain strings rather than a whole
//! [`MeiliContext`](meili_ingest_plugin_sdk::MeiliContext), so a credential cannot
//! reach this crate, its logs, or the analytics store by accident. `error` carries a
//! truncated operator-facing message (no end-user payload), and `error_kind` a short
//! classification suitable for grouping in a dashboard.
//!
//! ## Configuration
//!
//! [`UsageClient::from_env`] returns `Ok(None)` when `TINYBIRD_TOKEN` is unset: usage
//! reporting is optional, and a deployment without it simply logs once that metering
//! is off.
//!
//! | var | default | notes |
//! |---|---|---|
//! | `TINYBIRD_TOKEN` | — | absent ⇒ metering disabled (`Ok(None)`). Needs `APPEND` scope on the data source. |
//! | `TINYBIRD_BASE_URL` | `https://api.tinybird.co` | **region specific**, see below |
//! | `TINYBIRD_DATASOURCE` | `meili_ingest_usage` | |
//! | `TINYBIRD_TIMEOUT_SECS` | `20` | |
//!
//! Tinybird is regional and the host is *not* the same everywhere: an EU workspace is
//! `https://api.eu-central-1.aws.tinybird.co`, other regions differ again
//! (`https://api.us-east.aws.tinybird.co`, GCP variants, …). Posting to the wrong
//! region authenticates against the wrong workspace, so the base URL is configuration,
//! never a constant to hardcode.
//!
//! ## Example
//!
//! ```no_run
//! # async fn run() -> Result<(), meili_ingest_usage::UsageError> {
//! use meili_ingest_usage::{UsageClient, events_for_job, JobUsageInput};
//!
//! let input = JobUsageInput::default(); // built by the worker from the workflow output
//! if let Some(client) = UsageClient::from_env()? {
//!     client.send(&events_for_job(&input)).await?; // Temporal retries this on Err
//! }
//! # Ok(()) }
//! ```

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::fmt;
use std::time::Duration;

use chrono::{DateTime, Utc};
use meili_ingest_plugin_sdk::{JobStatus, StepResult, UsageUnits};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default Tinybird API host (US / `api.tinybird.co` workspaces).
pub const DEFAULT_BASE_URL: &str = "https://api.tinybird.co";
/// Base URL of an EU (`eu-central-1`) Tinybird workspace, for reference.
pub const EU_BASE_URL: &str = "https://api.eu-central-1.aws.tinybird.co";
/// Default data source name (see `tinybird/datasources/meili_ingest_usage.datasource`).
pub const DEFAULT_DATASOURCE: &str = "meili_ingest_usage";
/// Default HTTP timeout in seconds.
pub const DEFAULT_TIMEOUT_SECS: u64 = 20;
/// Env var holding the Tinybird append token. Absent ⇒ usage reporting is disabled.
pub const ENV_TOKEN: &str = "TINYBIRD_TOKEN";
/// Env var overriding the regional API host.
pub const ENV_BASE_URL: &str = "TINYBIRD_BASE_URL";
/// Env var overriding the target data source name.
pub const ENV_DATASOURCE: &str = "TINYBIRD_DATASOURCE";
/// Env var overriding the HTTP timeout, in seconds.
pub const ENV_TIMEOUT_SECS: &str = "TINYBIRD_TIMEOUT_SECS";

/// Maximum number of characters kept from an error message or a response body.
pub const MAX_ERROR_CHARS: usize = 512;

/// Attempt number stamped on step rows.
///
/// [`StepResult`] does not carry a per-step attempt counter today — Temporal retries a
/// step activity internally and only the final outcome reaches the workflow — so every
/// step row is attempt `1`. The number is part of `event_id` precisely so that, once
/// per-attempt reporting exists, each attempt gets its own row instead of replacing
/// the previous one.
pub const REPORTED_ATTEMPT: u32 = 1;

// ---------------------------------------------------------------------------
// Errors
// ---------------------------------------------------------------------------

/// Everything that can go wrong while reporting usage.
///
/// The split that matters is [`UsageError::is_retryable`]: the Temporal activity
/// wrapping [`UsageClient::send`] must let Temporal retry the retryable ones and fail
/// fast (non-retryable application failure) on the rest.
#[derive(Debug, thiserror::Error)]
pub enum UsageError {
    /// The client could not be configured (bad env var, HTTP client build failure).
    /// Permanent: an operator has to fix the deployment.
    #[error("usage reporting is misconfigured: {0}")]
    Config(String),

    /// Network failure, DNS failure, TLS failure or timeout. Retryable.
    #[error("usage transport failure talking to Tinybird: {0}")]
    Transport(String),

    /// Tinybird is unhappy but might not be next time: HTTP 429 or 5xx. Retryable.
    #[error("Tinybird returned HTTP {status} (retryable): {body}")]
    Upstream {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        body: String,
    },

    /// HTTP 401 / 403: the token is missing, wrong, expired, or lacks `APPEND` scope
    /// on the data source. Permanent, and deliberately carries no response body so a
    /// token echoed back by the API can never reach a log.
    #[error(
        "Tinybird rejected the credentials (HTTP {status}); check {ENV_TOKEN} and its \
         scope on the data source"
    )]
    Auth {
        /// HTTP status code (401 or 403).
        status: u16,
    },

    /// Any other 4xx: the request itself is wrong (unknown data source, malformed
    /// NDJSON, payload too large). Permanent.
    #[error("Tinybird rejected the request (HTTP {status}): {body}")]
    Rejected {
        /// HTTP status code.
        status: u16,
        /// Truncated response body.
        body: String,
    },

    /// Tinybird accepted the request but sent some rows to the quarantine table: the
    /// event struct and the data source schema have drifted apart. Permanent on
    /// purpose — retrying identical rows quarantines them again, and this is exactly
    /// the bug that must be loud.
    #[error(
        "Tinybird quarantined {count} row(s) written to data source {datasource:?}: the \
         UsageEvent struct and the .datasource schema have drifted; inspect \
         {datasource}_quarantine"
    )]
    Quarantined {
        /// Number of quarantined rows reported by the API.
        count: u64,
        /// Target data source.
        datasource: String,
    },

    /// An event could not be serialized to JSON. Permanent (a bug in this crate).
    #[error("cannot serialize usage events: {0}")]
    Encode(String),
}

impl UsageError {
    /// Whether Temporal should retry the activity that produced this error.
    ///
    /// `true` only for failures that a later identical attempt can plausibly fix:
    /// transport/timeout, HTTP 429, HTTP 5xx. Everything else needs a human.
    pub fn is_retryable(&self) -> bool {
        matches!(self, UsageError::Transport(_) | UsageError::Upstream { .. })
    }
}

// ---------------------------------------------------------------------------
// Events
// ---------------------------------------------------------------------------

/// Whether a row describes a whole job or a single step attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageEventKind {
    /// One row per finished job: wall-clock duration, input size, totals.
    Job,
    /// One row per step attempt: the unit billing and dashboards aggregate.
    Step,
}

impl UsageEventKind {
    /// Wire form (`"job"` / `"step"`), matching the `kind` column.
    pub fn as_str(&self) -> &'static str {
        match self {
            UsageEventKind::Job => "job",
            UsageEventKind::Step => "step",
        }
    }
}

/// One analytics row.
///
/// The field names and order mirror
/// `tinybird/datasources/meili_ingest_usage.datasource` one for one; the JSON path of
/// every column there is `$.<field>`. Nothing is ever skipped on serialization and
/// nothing is ever `null` — ClickHouse `LowCardinality(String)` columns take `""` for
/// "not applicable" (a self-hosted deployment has no `project_id`, a job row has no
/// `step_id`). Adding, renaming or retyping a field here **must** be mirrored in the
/// datafile, otherwise Tinybird quarantines the rows and
/// [`UsageError::Quarantined`] fires.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageEvent {
    /// Deduplication key: `{job_id}:{step_id}:{attempt}` for step rows,
    /// `{job_id}:job` for job rows. Deterministic, so a redelivery replaces the row
    /// instead of adding one.
    pub event_id: String,
    /// Row kind.
    pub kind: UsageEventKind,
    /// Event time, RFC 3339 with milliseconds; parsed into `DateTime64(3)`. Always the
    /// job's `finished_at`, never "now", so redeliveries keep the same sorting key.
    #[serde(with = "rfc3339_millis")]
    pub ts: DateTime<Utc>,
    /// Ingest job id.
    pub job_id: String,
    /// Temporal workflow id (`ingest-<job_id>`).
    pub workflow_id: String,
    /// Tenant id. `""` when self-hosted — never `null`.
    pub project_id: String,
    /// Meilisearch Cloud region tag, `""` when unknown.
    pub region: String,
    /// Pipeline uid that ran.
    pub pipeline_uid: String,
    /// Whether that pipeline is a built-in one (`1`/`0`).
    #[serde(with = "bool_as_u8")]
    pub pipeline_builtin: bool,
    /// Target Meilisearch index, `""` when the pipeline did not index.
    pub index_name: String,
    /// Step id, `""` on job rows.
    pub step_id: String,
    /// Plugin name, `""` on job rows.
    pub plugin: String,
    /// Temporal task queue the work ran on (`workers-general`, `workers-llm`, …).
    pub task_queue: String,
    /// [`JobStatus::as_str`] of the step (or of the job on job rows).
    pub status: String,
    /// Attempt number, see [`REPORTED_ATTEMPT`].
    pub attempt: u32,
    /// Parallel branches the step fanned out into (`0` on job rows).
    pub branches: u32,
    /// Wall time in milliseconds: the step's own duration, or `finished_at -
    /// started_at` on job rows.
    pub duration_ms: u64,
    /// Bytes fed into the step, or the job's original payload size on job rows.
    pub input_bytes: u64,
    /// MIME type of the job's original payload, `""` when unknown.
    pub input_mime: String,
    /// Documents the step produced; on job rows, the documents the final step produced.
    pub documents_out: u64,
    /// [`UsageUnits::llm_input_tokens`].
    pub llm_input_tokens: u64,
    /// [`UsageUnits::llm_output_tokens`].
    pub llm_output_tokens: u64,
    /// [`UsageUnits::llm_requests`].
    pub llm_requests: u64,
    /// [`UsageUnits::audio_seconds`].
    pub audio_seconds: f64,
    /// [`UsageUnits::pages`].
    pub pages: u64,
    /// [`UsageUnits::images`].
    pub images: u64,
    /// [`UsageUnits::external_requests`].
    pub external_requests: u64,
    /// Short, low-cardinality classification of `error`, see [`classify_error`].
    pub error_kind: String,
    /// Operator-facing error message, truncated to [`MAX_ERROR_CHARS`] characters.
    /// `""` when the step or job succeeded.
    pub error: String,
}

impl UsageEvent {
    /// The deterministic id of a step row: `{job_id}:{step_id}:{attempt}`.
    pub fn step_event_id(job_id: &str, step_id: &str, attempt: u32) -> String {
        format!("{job_id}:{step_id}:{attempt}")
    }

    /// The deterministic id of a job row: `{job_id}:job`.
    pub fn job_event_id(job_id: &str) -> String {
        format!("{job_id}:job")
    }
}

// ---------------------------------------------------------------------------
// Event building
// ---------------------------------------------------------------------------

/// Everything [`events_for_job`] needs about one finished job.
///
/// Assembled by the worker from [`PipelineWorkflowInput`] and
/// [`PipelineWorkflowOutput`]; it is itself the payload of the reporting Temporal
/// activity, hence `Serialize`/`Deserialize`. It carries `project_id` and `region` as
/// plain strings rather than the tenant
/// [`MeiliContext`](meili_ingest_plugin_sdk::MeiliContext) so the Meilisearch API key
/// cannot travel with usage data.
///
/// [`PipelineWorkflowInput`]: meili_ingest_plugin_sdk::PipelineWorkflowInput
/// [`PipelineWorkflowOutput`]: meili_ingest_plugin_sdk::PipelineWorkflowOutput
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct JobUsageInput {
    /// Ingest job id.
    pub job_id: Uuid,
    /// Temporal workflow id.
    pub workflow_id: String,
    /// Pipeline uid.
    pub pipeline_uid: String,
    /// Whether the pipeline is built in.
    pub pipeline_builtin: bool,
    /// Tenant id; empty string when self-hosted.
    pub project_id: String,
    /// Region tag; empty string when unknown.
    pub region: String,
    /// Resolved target index; empty string when the pipeline did not index.
    pub index_name: String,
    /// Task queue the reporting worker ran on. Applied to every row: per-step queues
    /// can differ (`meili_ingest_router::plugin_task_queue`), and a caller that knows
    /// better can rewrite `task_queue` on the returned rows before sending them.
    pub task_queue: String,
    /// Terminal status of the job.
    pub status: JobStatus,
    /// When the job started (used for the job row's `duration_ms`).
    pub started_at: DateTime<Utc>,
    /// When the job finished. Stamped as `ts` on **every** row of this job, which is
    /// what makes redeliveries replaceable.
    pub finished_at: DateTime<Utc>,
    /// Size of the job's original payload in bytes.
    pub input_bytes: u64,
    /// MIME type of the job's original payload.
    pub input_mime: String,
    /// Per-step outcomes, in execution order.
    pub steps: Vec<StepResult>,
    /// Job-level error message, if any.
    pub error: Option<String>,
}

/// Build every usage row for one finished job: one row per step (in execution order)
/// followed by one job row.
///
/// Pure and total — no clock, no randomness, no I/O — so two calls with the same input
/// produce byte-identical rows. That is what lets Temporal retry the delivery activity
/// blindly (see the crate docs).
///
/// The job row summarises the pipeline: its `duration_ms` is the wall-clock
/// `finished_at - started_at` (≥ the sum of the steps, because it includes queueing),
/// its `input_bytes` is the original payload, its `documents_out` is what the **final**
/// step produced (summing steps would count a document once per stage), and its usage
/// units are the sum over all steps. Because the units appear both on the step rows and
/// on the job row, any aggregation must pick one: the rollup in
/// `tinybird/pipes/usage_daily.pipe` sums units from `kind = 'step'` rows only.
pub fn events_for_job(input: &JobUsageInput) -> Vec<UsageEvent> {
    let job_id = input.job_id.to_string();
    let mut events = Vec::with_capacity(input.steps.len() + 1);

    let mut totals = UsageUnits::none();
    for step in &input.steps {
        totals.merge(step.usage);
        let mut event = base_event(input, &job_id);
        event.event_id = UsageEvent::step_event_id(&job_id, &step.step_id, REPORTED_ATTEMPT);
        event.kind = UsageEventKind::Step;
        event.step_id = step.step_id.clone();
        event.plugin = step.plugin.clone();
        event.status = step.status.as_str().to_string();
        event.branches = clamp_u32(step.branches);
        event.duration_ms = step.duration_ms;
        event.input_bytes = step.input_bytes;
        event.documents_out = step.document_count as u64;
        apply_units(&mut event, step.usage);
        event.error_kind = error_kind_for(step.status, step.error.as_deref()).to_string();
        event.error = truncate_chars(step.error.as_deref().unwrap_or_default(), MAX_ERROR_CHARS);
        events.push(event);
    }

    let mut job = base_event(input, &job_id);
    job.event_id = UsageEvent::job_event_id(&job_id);
    job.kind = UsageEventKind::Job;
    job.status = input.status.as_str().to_string();
    job.duration_ms = wall_clock_ms(input.started_at, input.finished_at);
    job.input_bytes = input.input_bytes;
    job.documents_out = input
        .steps
        .last()
        .map(|s| s.document_count as u64)
        .unwrap_or(0);
    apply_units(&mut job, totals);
    job.error_kind = error_kind_for(input.status, input.error.as_deref()).to_string();
    job.error = truncate_chars(input.error.as_deref().unwrap_or_default(), MAX_ERROR_CHARS);
    events.push(job);

    events
}

/// A row with everything that is common to all rows of a job filled in, and the
/// per-row fields left at their "not applicable" value.
fn base_event(input: &JobUsageInput, job_id: &str) -> UsageEvent {
    UsageEvent {
        event_id: String::new(),
        kind: UsageEventKind::Job,
        ts: input.finished_at,
        job_id: job_id.to_string(),
        workflow_id: input.workflow_id.clone(),
        project_id: input.project_id.clone(),
        region: input.region.clone(),
        pipeline_uid: input.pipeline_uid.clone(),
        pipeline_builtin: input.pipeline_builtin,
        index_name: input.index_name.clone(),
        step_id: String::new(),
        plugin: String::new(),
        task_queue: input.task_queue.clone(),
        status: input.status.as_str().to_string(),
        attempt: REPORTED_ATTEMPT,
        branches: 0,
        duration_ms: 0,
        input_bytes: 0,
        input_mime: input.input_mime.clone(),
        documents_out: 0,
        llm_input_tokens: 0,
        llm_output_tokens: 0,
        llm_requests: 0,
        audio_seconds: 0.0,
        pages: 0,
        images: 0,
        external_requests: 0,
        error_kind: String::new(),
        error: String::new(),
    }
}

fn apply_units(event: &mut UsageEvent, units: UsageUnits) {
    event.llm_input_tokens = units.llm_input_tokens;
    event.llm_output_tokens = units.llm_output_tokens;
    event.llm_requests = units.llm_requests;
    event.audio_seconds = units.audio_seconds;
    event.pages = units.pages;
    event.images = units.images;
    event.external_requests = units.external_requests;
}

fn wall_clock_ms(started: DateTime<Utc>, finished: DateTime<Utc>) -> u64 {
    let ms = finished.signed_duration_since(started).num_milliseconds();
    u64::try_from(ms).unwrap_or(0)
}

fn clamp_u32(v: usize) -> u32 {
    u32::try_from(v).unwrap_or(u32::MAX)
}

/// Classification used when a step or job has no error.
pub const ERROR_KIND_NONE: &str = "";
/// The work exceeded a deadline.
pub const ERROR_KIND_TIMEOUT: &str = "timeout";
/// Credentials were missing or rejected.
pub const ERROR_KIND_AUTH: &str = "auth";
/// The submitted content was unusable.
pub const ERROR_KIND_INVALID_INPUT: &str = "invalid_input";
/// The pipeline / plugin configuration was wrong.
pub const ERROR_KIND_INVALID_CONFIG: &str = "invalid_config";
/// A third-party service failed.
pub const ERROR_KIND_UPSTREAM: &str = "upstream";
/// The job was cancelled.
pub const ERROR_KIND_CANCELLED: &str = "cancelled";
/// Anything else — our bug until proven otherwise.
pub const ERROR_KIND_INTERNAL: &str = "internal";

/// Reduce a free-form error message to one of the eight low-cardinality kinds.
///
/// Dashboards group on this column, so it must stay small and stable — the raw message
/// lives in `error` and is never used as a grouping key. The classifier is a documented
/// ordered scan of the lower-cased message; the **first** matching rule wins:
///
/// 1. `timeout`, `timed out`, `deadline exceeded`, `elapsed` → [`ERROR_KIND_TIMEOUT`]
/// 2. `cancel` → [`ERROR_KIND_CANCELLED`]
/// 3. `invalid config`, `invalidconfig`, `not set`, `missing config`,
///    `unknown plugin` → [`ERROR_KIND_INVALID_CONFIG`] (before the credential rules, so
///    that `LLM_API_KEY not set` is a configuration problem and not an auth failure)
/// 4. `unauthorized`, `forbidden`, `http 401`, `http 403`, `api key`, `api_key`,
///    `credential`, `authentication` → [`ERROR_KIND_AUTH`]
/// 5. `invalid input`, `invalidinput`, `unsupported`, `malformed`, `corrupt`,
///    `cannot parse`, `parse error`, `decode` → [`ERROR_KIND_INVALID_INPUT`]
/// 6. `upstream`, `rate limit`, `too many requests`, `http 5`, `502`, `503`, `504`,
///    `connection`, `connect`, `dns`, `refused`, `unavailable` →
///    [`ERROR_KIND_UPSTREAM`]
/// 7. anything else → [`ERROR_KIND_INTERNAL`]
///
/// An empty (or whitespace-only) message classifies as [`ERROR_KIND_NONE`].
pub fn classify_error(message: &str) -> &'static str {
    let m = message.trim().to_ascii_lowercase();
    if m.is_empty() {
        return ERROR_KIND_NONE;
    }
    let has = |needles: &[&str]| needles.iter().any(|n| m.contains(n));

    if has(&["timeout", "timed out", "deadline exceeded", "elapsed"]) {
        ERROR_KIND_TIMEOUT
    } else if has(&["cancel"]) {
        ERROR_KIND_CANCELLED
    } else if has(&[
        "invalid config",
        "invalidconfig",
        "not set",
        "missing config",
        "unknown plugin",
    ]) {
        ERROR_KIND_INVALID_CONFIG
    } else if has(&[
        "unauthorized",
        "forbidden",
        "http 401",
        "http 403",
        "api key",
        "api_key",
        "credential",
        "authentication",
    ]) {
        ERROR_KIND_AUTH
    } else if has(&[
        "invalid input",
        "invalidinput",
        "unsupported",
        "malformed",
        "corrupt",
        "cannot parse",
        "parse error",
        "decode",
    ]) {
        ERROR_KIND_INVALID_INPUT
    } else if has(&[
        "upstream",
        "rate limit",
        "too many requests",
        "http 5",
        "502",
        "503",
        "504",
        "connection",
        "connect",
        "dns",
        "refused",
        "unavailable",
    ]) {
        ERROR_KIND_UPSTREAM
    } else {
        ERROR_KIND_INTERNAL
    }
}

/// A cancelled status always classifies as [`ERROR_KIND_CANCELLED`], whatever the
/// message says; otherwise the message decides.
fn error_kind_for(status: JobStatus, error: Option<&str>) -> &'static str {
    match status {
        JobStatus::Cancelled => ERROR_KIND_CANCELLED,
        _ => classify_error(error.unwrap_or_default()),
    }
}

/// Keep at most `max` characters, cutting on a character boundary.
fn truncate_chars(s: &str, max: usize) -> String {
    match s.char_indices().nth(max) {
        Some((i, _)) => s[..i].to_string(),
        None => s.to_string(),
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Tinybird Events API client.
///
/// Cheap to clone (the inner [`reqwest::Client`] is an `Arc`), so the worker builds one
/// at startup and shares it with every activity.
#[derive(Clone)]
pub struct UsageClient {
    base_url: String,
    token: String,
    datasource: String,
    http: reqwest::Client,
}

/// Redacts the token: a usage client is routinely printed in worker startup logs.
impl fmt::Debug for UsageClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UsageClient")
            .field("base_url", &self.base_url)
            .field("datasource", &self.datasource)
            .field("token", &"<redacted>")
            .finish()
    }
}

impl UsageClient {
    /// Build a client from the environment.
    ///
    /// Returns `Ok(None)` when `TINYBIRD_TOKEN` is absent or empty: usage reporting is
    /// optional and the worker just logs once that metering is off. Returns
    /// [`UsageError::Config`] when a variable is present but unusable — a typo in
    /// `TINYBIRD_TIMEOUT_SECS` should not silently disable billing data.
    ///
    /// See the crate docs for the variable table and the regional base-URL caveat.
    pub fn from_env() -> Result<Option<Self>, UsageError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }

    /// [`Self::from_env`] with an injectable lookup — the testable core.
    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Result<Option<Self>, UsageError> {
        let non_empty = |key: &str| {
            get(key)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
        };

        let Some(token) = non_empty(ENV_TOKEN) else {
            return Ok(None);
        };
        let base_url = non_empty(ENV_BASE_URL).unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let datasource =
            non_empty(ENV_DATASOURCE).unwrap_or_else(|| DEFAULT_DATASOURCE.to_string());
        let timeout_secs = match non_empty(ENV_TIMEOUT_SECS) {
            Some(raw) => raw.parse::<u64>().map_err(|e| {
                UsageError::Config(format!(
                    "{ENV_TIMEOUT_SECS} must be a number of seconds: {e}"
                ))
            })?,
            None => DEFAULT_TIMEOUT_SECS,
        };
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(timeout_secs))
            .build()
            .map_err(|e| UsageError::Config(format!("cannot build the HTTP client: {e}")))?;

        Ok(Some(Self::new(base_url, token, datasource, http)))
    }

    /// Build a client explicitly (tests, or a worker wiring its own HTTP client).
    ///
    /// `base_url` is the **regional** Tinybird API host, without a trailing slash
    /// (one is trimmed if present).
    pub fn new(
        base_url: impl Into<String>,
        token: impl Into<String>,
        datasource: impl Into<String>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            base_url: base_url.into().trim_end_matches('/').to_string(),
            token: token.into(),
            datasource: datasource.into(),
            http,
        }
    }

    /// Target data source name.
    pub fn datasource(&self) -> &str {
        &self.datasource
    }

    /// Regional API host this client posts to.
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    /// Append `events` to the data source through
    /// `POST {base_url}/v0/events?name={datasource}&wait=true`.
    ///
    /// The body is newline-delimited JSON, one event per line — the Events API format.
    /// `wait=true` makes Tinybird acknowledge the actual write rather than the receipt,
    /// which is what makes a Temporal retry meaningful: an `Ok(())` means the rows are
    /// durable.
    ///
    /// An empty slice is a no-op and issues no request.
    ///
    /// Safe to call twice with the same events: rows are keyed by their deterministic
    /// `event_id` in a `ReplacingMergeTree`, so a duplicate replaces rather than
    /// double-counts (see the crate docs).
    ///
    /// # Errors
    ///
    /// Retryable: transport failures and timeouts ([`UsageError::Transport`]), HTTP 429
    /// and 5xx ([`UsageError::Upstream`]). Permanent: 401/403 ([`UsageError::Auth`]),
    /// other 4xx ([`UsageError::Rejected`]), quarantined rows
    /// ([`UsageError::Quarantined`]), serialization failures
    /// ([`UsageError::Encode`]). Branch on [`UsageError::is_retryable`], not on the
    /// variant.
    pub async fn send(&self, events: &[UsageEvent]) -> Result<(), UsageError> {
        if events.is_empty() {
            return Ok(());
        }

        let mut body = String::new();
        for event in events {
            let line = serde_json::to_string(event)
                .map_err(|e| UsageError::Encode(format!("event {}: {e}", event.event_id)))?;
            body.push_str(&line);
            body.push('\n');
        }

        let url = format!(
            "{}/v0/events?name={}&wait=true",
            self.base_url, self.datasource
        );
        tracing::debug!(
            datasource = %self.datasource,
            base_url = %self.base_url,
            events = events.len(),
            bytes = body.len(),
            "sending usage events to Tinybird"
        );

        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.token)
            .header("content-type", "application/x-ndjson")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                UsageError::Transport(format!(
                    "POST {}/v0/events (name={}) failed: {e}",
                    self.base_url, self.datasource
                ))
            })?;

        let status = response.status().as_u16();
        let text = response.text().await.map_err(|e| {
            UsageError::Transport(format!("Tinybird response body unreadable: {e}"))
        })?;

        if !(200..300).contains(&status) {
            return Err(map_http_failure(status, &text));
        }

        // A 200 does not mean every row landed: rows whose types do not match the
        // schema go to `<datasource>_quarantine` and are reported here.
        let ack: EventsAck = serde_json::from_str(&text).unwrap_or_default();
        if ack.quarantined_rows > 0 {
            tracing::error!(
                datasource = %self.datasource,
                quarantined_rows = ack.quarantined_rows,
                "Tinybird quarantined usage rows: schema drift"
            );
            return Err(UsageError::Quarantined {
                count: ack.quarantined_rows,
                datasource: self.datasource.clone(),
            });
        }
        tracing::debug!(
            successful_rows = ack.successful_rows,
            "usage events accepted by Tinybird"
        );
        Ok(())
    }
}

/// Body of a successful Events API reply. Tolerant: an unexpected shape deserializes
/// into the default (nothing quarantined) rather than failing a write that succeeded.
#[derive(Debug, Default, Deserialize)]
struct EventsAck {
    #[serde(default)]
    successful_rows: u64,
    #[serde(default)]
    quarantined_rows: u64,
}

/// Map a non-2xx status to the right retryability. 429 and 5xx are transient; 401/403
/// are credentials (body dropped so a reflected token cannot be logged); every other
/// 4xx is a request a retry cannot fix.
fn map_http_failure(status: u16, body: &str) -> UsageError {
    match status {
        401 | 403 => UsageError::Auth { status },
        429 | 500..=599 => UsageError::Upstream {
            status,
            body: truncate_chars(body, MAX_ERROR_CHARS),
        },
        _ => UsageError::Rejected {
            status,
            body: truncate_chars(body, MAX_ERROR_CHARS),
        },
    }
}

// ---------------------------------------------------------------------------
// serde helpers
// ---------------------------------------------------------------------------

/// `DateTime<Utc>` ⇄ RFC 3339 with exactly three fractional digits and a `Z` suffix,
/// which ClickHouse reads straight into `DateTime64(3)`. Fixed precision keeps
/// redelivered rows byte-identical.
mod rfc3339_millis {
    use chrono::{DateTime, SecondsFormat, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(ts: &DateTime<Utc>, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&ts.to_rfc3339_opts(SecondsFormat::Millis, true))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<DateTime<Utc>, D::Error> {
        let raw = String::deserialize(d)?;
        DateTime::parse_from_rfc3339(&raw)
            .map(|t| t.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }
}

/// `bool` ⇄ `0`/`1`, because the column is a ClickHouse `UInt8`.
mod bool_as_u8 {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &bool, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_u8(u8::from(*v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<bool, D::Error> {
        Ok(u8::deserialize(d)? != 0)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const JOB: &str = "11111111-2222-3333-4444-555555555555";

    fn client(server: &MockServer) -> UsageClient {
        UsageClient::new(
            server.uri(),
            "p.secret-append-token",
            "meili_ingest_usage",
            reqwest::Client::new(),
        )
    }

    fn ts(secs: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 4, 1, 10, 0, secs)
            .single()
            .expect("valid timestamp")
    }

    fn step(id: &str, plugin: &str, status: JobStatus, usage: UsageUnits) -> StepResult {
        StepResult {
            step_id: id.into(),
            plugin: plugin.into(),
            status,
            document_count: 12,
            branches: 3,
            error: None,
            duration_ms: 250,
            input_bytes: 4_096,
            usage,
        }
    }

    fn job_input() -> JobUsageInput {
        JobUsageInput {
            job_id: JOB.parse().expect("valid uuid"),
            workflow_id: format!("ingest-{JOB}"),
            pipeline_uid: "builtin.pdf".into(),
            pipeline_builtin: true,
            project_id: "acme".into(),
            region: "us-west".into(),
            index_name: "documents".into(),
            task_queue: "workers-general".into(),
            status: JobStatus::Succeeded,
            started_at: ts(0),
            finished_at: ts(9),
            input_bytes: 1_000_000,
            input_mime: "application/pdf".into(),
            steps: vec![
                step(
                    "extract",
                    "pdf_extractor",
                    JobStatus::Succeeded,
                    UsageUnits::pages(9),
                ),
                step(
                    "enrich",
                    "llm_enricher",
                    JobStatus::Succeeded,
                    UsageUnits::llm(400, 90),
                ),
            ],
            error: None,
        }
    }

    fn one_event() -> UsageEvent {
        let events = events_for_job(&job_input());
        events.into_iter().next().expect("at least one event")
    }

    // -- send: happy path ----------------------------------------------------

    #[tokio::test]
    async fn happy_path_posts_ndjson_with_query_and_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v0/events"))
            .and(query_param("name", "meili_ingest_usage"))
            .and(query_param("wait", "true"))
            .and(header("authorization", "Bearer p.secret-append-token"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "successful_rows": 3, "quarantined_rows": 0
            })))
            .expect(1)
            .mount(&server)
            .await;

        let events = events_for_job(&job_input());
        assert_eq!(events.len(), 3, "two steps + one job row");
        client(&server).send(&events).await.expect("send succeeds");

        let requests = server.received_requests().await.expect("recorded requests");
        assert_eq!(requests.len(), 1);
        let body = String::from_utf8(requests[0].body.clone()).expect("utf-8 body");

        // Exactly N newline-separated JSON objects, one per event.
        let lines: Vec<&str> = body.trim_end_matches('\n').split('\n').collect();
        assert_eq!(lines.len(), events.len());
        assert!(body.ends_with('\n'));
        assert!(!body.contains("}{"), "objects must not be concatenated");

        let first: serde_json::Map<String, serde_json::Value> =
            serde_json::from_str(lines[0]).expect("line 0 is a JSON object");
        let expected = [
            "event_id",
            "kind",
            "ts",
            "job_id",
            "workflow_id",
            "project_id",
            "region",
            "pipeline_uid",
            "pipeline_builtin",
            "index_name",
            "step_id",
            "plugin",
            "task_queue",
            "status",
            "attempt",
            "branches",
            "duration_ms",
            "input_bytes",
            "input_mime",
            "documents_out",
            "llm_input_tokens",
            "llm_output_tokens",
            "llm_requests",
            "audio_seconds",
            "pages",
            "images",
            "external_requests",
            "error_kind",
            "error",
        ];
        for key in expected {
            assert!(first.contains_key(key), "missing field {key}");
        }
        assert_eq!(first.len(), expected.len(), "unexpected extra fields");
        assert_eq!(first["event_id"], format!("{JOB}:extract:1"));
        assert_eq!(first["kind"], "step");
        assert_eq!(first["ts"], "2026-04-01T10:00:09.000Z");
        assert_eq!(first["pipeline_builtin"], 1);
        assert!(
            !first.values().any(serde_json::Value::is_null),
            "no column may be null"
        );

        let last: serde_json::Value = serde_json::from_str(lines[2]).expect("line 2 is JSON");
        assert_eq!(last["event_id"], format!("{JOB}:job"));
        assert_eq!(last["kind"], "job");
    }

    #[tokio::test]
    async fn empty_slice_sends_nothing() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        client(&server).send(&[]).await.expect("no-op is Ok");
    }

    #[tokio::test]
    async fn plain_ok_body_without_ack_json_is_accepted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(202).set_body_string("OK"))
            .expect(1)
            .mount(&server)
            .await;
        client(&server)
            .send(&[one_event()])
            .await
            .expect("a 2xx without a JSON ack still counts as written");
    }

    // -- send: error classification -----------------------------------------

    #[tokio::test]
    async fn rate_limited_is_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .mount(&server)
            .await;
        let err = client(&server)
            .send(&[one_event()])
            .await
            .expect_err("429 fails");
        assert!(err.is_retryable(), "{err:?}");
        assert!(matches!(err, UsageError::Upstream { status: 429, .. }));
    }

    #[tokio::test]
    async fn server_error_is_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream unavailable"))
            .mount(&server)
            .await;
        let err = client(&server)
            .send(&[one_event()])
            .await
            .expect_err("503 fails");
        assert!(err.is_retryable(), "{err:?}");
        assert!(err.to_string().contains("503"));
    }

    #[tokio::test]
    async fn unauthorized_is_permanent_and_never_echoes_the_token() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(401)
                    // A hostile / careless API reflecting the token back must not leak.
                    .set_body_string(r#"{"error":"invalid token p.secret-append-token"}"#),
            )
            .mount(&server)
            .await;
        let err = client(&server)
            .send(&[one_event()])
            .await
            .expect_err("401 fails");
        assert!(!err.is_retryable(), "{err:?}");
        assert!(matches!(err, UsageError::Auth { status: 401 }));
        assert!(
            !err.to_string().contains("p.secret-append-token"),
            "token leaked into {err}"
        );
    }

    #[tokio::test]
    async fn forbidden_is_permanent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403).set_body_string("no APPEND scope"))
            .mount(&server)
            .await;
        let err = client(&server)
            .send(&[one_event()])
            .await
            .expect_err("403 fails");
        assert!(!err.is_retryable(), "{err:?}");
        assert!(matches!(err, UsageError::Auth { status: 403 }));
    }

    #[tokio::test]
    async fn bad_request_is_permanent_and_keeps_the_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(400).set_body_string(r#"{"error":"unknown datasource"}"#),
            )
            .mount(&server)
            .await;
        let err = client(&server)
            .send(&[one_event()])
            .await
            .expect_err("400 fails");
        assert!(!err.is_retryable(), "{err:?}");
        assert!(err.to_string().contains("unknown datasource"), "{err}");
    }

    #[tokio::test]
    async fn quarantined_rows_are_a_permanent_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "successful_rows": 1, "quarantined_rows": 2
            })))
            .mount(&server)
            .await;
        let err = client(&server)
            .send(&[one_event()])
            .await
            .expect_err("quarantine fails the write");
        assert!(!err.is_retryable(), "retrying would quarantine again");
        assert!(matches!(
            err,
            UsageError::Quarantined { count: 2, ref datasource } if datasource == "meili_ingest_usage"
        ));
        let msg = err.to_string();
        assert!(msg.contains("quarantined 2 row"), "{msg}");
    }

    #[tokio::test]
    async fn transport_failure_is_retryable() {
        // Port 1 on loopback: nothing listens there, so the connection is refused.
        let client = UsageClient::new(
            "http://127.0.0.1:1",
            "tok",
            DEFAULT_DATASOURCE,
            reqwest::Client::new(),
        );
        let err = client
            .send(&[one_event()])
            .await
            .expect_err("connection refused");
        assert!(err.is_retryable(), "{err:?}");
        assert!(matches!(err, UsageError::Transport(_)));
        assert!(!err.to_string().contains("tok"), "{err}");
    }

    // -- configuration -------------------------------------------------------

    #[test]
    fn from_env_without_a_token_disables_reporting() {
        let built = UsageClient::from_lookup(|_| None).expect("no error");
        assert!(built.is_none(), "usage reporting is optional");
        // An empty / whitespace token counts as absent too.
        let blank = UsageClient::from_lookup(|k| (k == ENV_TOKEN).then(|| "  ".to_string()))
            .expect("no error");
        assert!(blank.is_none());
    }

    #[test]
    fn from_env_applies_defaults_and_overrides() {
        let defaults = UsageClient::from_lookup(|k| (k == ENV_TOKEN).then(|| "tok".to_string()))
            .expect("no error")
            .expect("configured");
        assert_eq!(defaults.base_url(), DEFAULT_BASE_URL);
        assert_eq!(defaults.datasource(), DEFAULT_DATASOURCE);

        let eu = UsageClient::from_lookup(|k| match k {
            ENV_TOKEN => Some("tok".into()),
            // Regional host: an EU workspace is not reachable on api.tinybird.co.
            ENV_BASE_URL => Some(format!("{EU_BASE_URL}/")),
            ENV_DATASOURCE => Some("usage_staging".into()),
            ENV_TIMEOUT_SECS => Some("5".into()),
            _ => None,
        })
        .expect("no error")
        .expect("configured");
        assert_eq!(eu.base_url(), EU_BASE_URL, "trailing slash trimmed");
        assert_eq!(eu.datasource(), "usage_staging");
    }

    #[test]
    fn from_env_rejects_a_bad_timeout() {
        let err = UsageClient::from_lookup(|k| match k {
            ENV_TOKEN => Some("tok".into()),
            ENV_TIMEOUT_SECS => Some("twenty".into()),
            _ => None,
        })
        .expect_err("a typo must not silently disable metering");
        assert!(!err.is_retryable());
        assert!(matches!(err, UsageError::Config(_)));
    }

    #[test]
    fn debug_redacts_the_token() {
        let c = UsageClient::new(
            DEFAULT_BASE_URL,
            "p.super-secret",
            DEFAULT_DATASOURCE,
            reqwest::Client::new(),
        );
        let printed = format!("{c:?}");
        assert!(!printed.contains("p.super-secret"), "{printed}");
        assert!(printed.contains("<redacted>"));
        assert!(printed.contains(DEFAULT_DATASOURCE));
    }

    // -- events_for_job ------------------------------------------------------

    #[test]
    fn events_for_job_emits_one_row_per_step_plus_a_job_row() {
        let mut input = job_input();
        input.status = JobStatus::Failed;
        input.error = Some("step enrich failed: upstream returned HTTP 503".into());
        input.steps[1].status = JobStatus::Failed;
        input.steps[1].error = Some("LLM request timed out after 120s".into());
        input.steps[1].document_count = 0;

        let events = events_for_job(&input);
        assert_eq!(events.len(), 3);

        let extract = &events[0];
        assert_eq!(extract.kind, UsageEventKind::Step);
        assert_eq!(extract.event_id, format!("{JOB}:extract:1"));
        assert_eq!(extract.step_id, "extract");
        assert_eq!(extract.plugin, "pdf_extractor");
        assert_eq!(extract.status, "succeeded");
        assert_eq!(extract.attempt, REPORTED_ATTEMPT);
        assert_eq!(extract.branches, 3);
        assert_eq!(extract.duration_ms, 250);
        assert_eq!(extract.input_bytes, 4_096);
        assert_eq!(extract.documents_out, 12);
        assert_eq!(extract.pages, 9);
        assert_eq!(extract.error_kind, ERROR_KIND_NONE);
        assert_eq!(extract.error, "");

        let enrich = &events[1];
        assert_eq!(enrich.event_id, format!("{JOB}:enrich:1"));
        assert_eq!(enrich.llm_input_tokens, 400);
        assert_eq!(enrich.llm_output_tokens, 90);
        assert_eq!(enrich.llm_requests, 1);
        assert_eq!(enrich.error_kind, ERROR_KIND_TIMEOUT);
        assert_eq!(enrich.error, "LLM request timed out after 120s");

        let job = &events[2];
        assert_eq!(job.kind, UsageEventKind::Job);
        assert_eq!(job.event_id, format!("{JOB}:job"));
        assert_eq!(job.step_id, "", "job rows carry no step");
        assert_eq!(job.plugin, "");
        assert_eq!(job.status, "failed");
        assert_eq!(job.duration_ms, 9_000, "wall clock, not the sum of steps");
        assert_eq!(job.input_bytes, 1_000_000);
        assert_eq!(job.input_mime, "application/pdf");
        assert_eq!(job.documents_out, 0, "documents out of the final step");
        // Units are summed across every step.
        assert_eq!(job.pages, 9);
        assert_eq!(job.llm_input_tokens, 400);
        assert_eq!(job.llm_output_tokens, 90);
        assert_eq!(job.llm_requests, 1);
        assert_eq!(job.error_kind, ERROR_KIND_UPSTREAM);

        // Tenant columns are on every row: billing filters by project_id first.
        for e in &events {
            assert_eq!(e.project_id, "acme");
            assert_eq!(e.region, "us-west");
            assert_eq!(e.pipeline_uid, "builtin.pdf");
            assert!(e.pipeline_builtin);
            assert_eq!(e.task_queue, "workers-general");
            assert_eq!(e.ts, input.finished_at, "one timestamp per job");
        }
    }

    #[test]
    fn a_job_with_no_steps_still_produces_the_job_row() {
        let mut input = job_input();
        input.steps.clear();
        input.status = JobStatus::Failed;
        input.error = Some("no pipeline matched the content type".into());

        let events = events_for_job(&input);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].kind, UsageEventKind::Job);
        assert_eq!(events[0].event_id, format!("{JOB}:job"));
        assert_eq!(events[0].documents_out, 0);
        assert_eq!(events[0].branches, 0);
        assert!(events[0].audio_seconds.abs() < f64::EPSILON);
    }

    #[test]
    fn events_for_job_is_deterministic() {
        let input = job_input();
        let first = events_for_job(&input);
        let second = events_for_job(&input);
        assert_eq!(first, second, "a Temporal retry must rebuild the same rows");

        let ids: Vec<&str> = first.iter().map(|e| e.event_id.as_str()).collect();
        assert_eq!(
            ids,
            vec![
                format!("{JOB}:extract:1"),
                format!("{JOB}:enrich:1"),
                format!("{JOB}:job"),
            ]
        );
        // Byte-identical on the wire is what makes ReplacingMergeTree dedup exact.
        let render = |evs: &[UsageEvent]| {
            evs.iter()
                .map(|e| serde_json::to_string(e).expect("serializable"))
                .collect::<Vec<_>>()
                .join("\n")
        };
        assert_eq!(render(&first), render(&second));
    }

    #[test]
    fn self_hosted_jobs_use_empty_strings_not_nulls() {
        let mut input = job_input();
        input.project_id = String::new();
        input.region = String::new();
        input.index_name = String::new();
        let events = events_for_job(&input);
        let json: serde_json::Value = serde_json::to_value(&events[2]).expect("event serializes");
        assert_eq!(json["project_id"], "");
        assert_eq!(json["region"], "");
        assert!(
            !json
                .as_object()
                .expect("object")
                .values()
                .any(|v| v.is_null())
        );
    }

    #[test]
    fn cancelled_jobs_classify_as_cancelled() {
        let mut input = job_input();
        input.status = JobStatus::Cancelled;
        input.steps[0].status = JobStatus::Cancelled;
        input.steps[0].error = None;
        let events = events_for_job(&input);
        assert_eq!(events[0].error_kind, ERROR_KIND_CANCELLED);
        assert_eq!(events[2].error_kind, ERROR_KIND_CANCELLED);
    }

    #[test]
    fn error_messages_are_classified_and_truncated() {
        let cases = [
            ("", ERROR_KIND_NONE),
            ("   ", ERROR_KIND_NONE),
            ("activity StartToClose timeout", ERROR_KIND_TIMEOUT),
            ("request timed out after 30s", ERROR_KIND_TIMEOUT),
            (
                "LLM API rejected the credentials (HTTP 401)",
                ERROR_KIND_AUTH,
            ),
            ("LLM_API_KEY is not a valid api key", ERROR_KIND_AUTH),
            ("workflow was cancelled by the user", ERROR_KIND_CANCELLED),
            (
                "invalid config: max_concurrent must be >= 1",
                ERROR_KIND_INVALID_CONFIG,
            ),
            ("LLM_API_KEY not set", ERROR_KIND_INVALID_CONFIG),
            (
                "unsupported content type application/zip",
                ERROR_KIND_INVALID_INPUT,
            ),
            (
                "malformed PDF: xref table corrupt",
                ERROR_KIND_INVALID_INPUT,
            ),
            ("upstream returned HTTP 502", ERROR_KIND_UPSTREAM),
            ("connection refused", ERROR_KIND_UPSTREAM),
            ("rate limit exceeded", ERROR_KIND_UPSTREAM),
            ("index out of bounds", ERROR_KIND_INTERNAL),
        ];
        for (message, expected) in cases {
            assert_eq!(classify_error(message), expected, "for {message:?}");
        }

        let mut input = job_input();
        input.error = Some("é".repeat(1_000));
        let events = events_for_job(&input);
        let job = events.last().expect("job row");
        assert_eq!(job.error.chars().count(), MAX_ERROR_CHARS);
        assert!(job.error.is_char_boundary(job.error.len()));
    }

    /// The Tinybird datafile is part of the contract: a column added on one side and
    /// not the other quarantines every row at runtime. Checking it here turns that
    /// production failure into a compile-time-adjacent test failure.
    #[test]
    fn datasource_schema_matches_the_event_struct() {
        const DATAFILE: &str =
            include_str!("../../../tinybird/datasources/meili_ingest_usage.datasource");

        let schema = DATAFILE
            .split("SCHEMA >")
            .nth(1)
            .and_then(|rest| rest.split("\nENGINE ").next())
            .expect("the datafile has a SCHEMA block");

        let mut columns = Vec::new();
        for line in schema.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let mut parts = line.split('`');
            let name = parts.nth(1).expect("`column` first on the line");
            columns.push(name.to_string());
            // Every column is fed by the identically named JSON key.
            assert!(
                line.contains(&format!("json:$.{name}")),
                "column {name} is not mapped to json:$.{name}"
            );
        }

        let json = serde_json::to_value(one_event()).expect("event serializes");
        let fields: Vec<String> = json
            .as_object()
            .expect("an event is a JSON object")
            .keys()
            .cloned()
            .collect();

        // Compared as sets: columns are matched by JSON path, so the order of the two
        // lists is documentation, not semantics.
        columns.sort();
        assert_eq!(
            columns, fields,
            "UsageEvent and meili_ingest_usage.datasource have drifted apart"
        );
    }

    #[test]
    fn events_round_trip_through_json() {
        // The rows are also read back by tooling; the wire form must be stable.
        let event = one_event();
        let json = serde_json::to_string(&event).expect("serializes");
        let back: UsageEvent = serde_json::from_str(&json).expect("deserializes");
        assert_eq!(back, event);
        assert_eq!(UsageEventKind::Step.as_str(), "step");
        assert_eq!(UsageEventKind::Job.as_str(), "job");
    }
}
