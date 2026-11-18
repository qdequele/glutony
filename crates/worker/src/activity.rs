//! The `execute_step` and `expand_fan_out` Temporal activities.
//!
//! `execute_step` is the single activity type every pipeline step runs through: it
//! resolves references in the input (URL / S3 / staged blobs), builds the plugin-facing
//! [`ActivityContext`] (heartbeat forwarding + cancellation flag), dispatches to the
//! plugin from the [`PluginRegistry`], maps [`PluginError`] onto Temporal retry
//! semantics and spills oversized outputs to the blob store.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use meili_ingest_blob::{BlobError, BlobStore};
use meili_ingest_plugin_sdk::{
    ActivityContext as PluginContext, JobStatus, PluginError, PluginInput, PluginOutput,
    StepActivityInput, StepActivityOutput,
};
use meili_ingest_usage::{JobUsageInput, UsageClient, events_for_job};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::ApplicationFailure;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tokio::sync::mpsc;
use uuid::Uuid;

use crate::ai_target::{LAB_AI_PLUGINS, fetch_ai_target};
use crate::connection::{
    CONTROL_PLANE_TIMEOUT, ConnectionSettings, ControlPlane, authed_within, resolve_connection,
};
use crate::registry::PluginRegistry;

/// Interval at which the activity heartbeats on its own, independent of the plugin.
const KEEPALIVE_HEARTBEAT: Duration = Duration::from_secs(20);

/// How long a plugin may keep running after its activity is cancelled (a job cancel,
/// or the worker shutting down) before it is abandoned and the activity reports the
/// cancellation. Plugins that poll the cancellation flag stop well within it.
const CANCEL_GRACE: Duration = Duration::from_secs(10);

/// Waits before trying a job-row patch again after a 404. The gateway inserts the row
/// right after starting the workflow, so a worker that picks the job up quickly (or
/// finishes a small one) can patch before the row exists. Bounded, because the insert
/// is best effort: when it failed the row never appears, and the terminal patch gates
/// the usage report, so it must not wait for long.
const JOB_ROW_MISSING_BACKOFF: [Duration; 3] = [
    Duration::from_millis(200),
    Duration::from_millis(400),
    Duration::from_millis(800),
];

/// Run `work`, but once `cancelled` resolves give it only `grace` more before giving up
/// with [`PluginError::Cancelled`]. Without this a plugin that never polls the flag
/// would keep a shutting-down worker alive until it is killed, and Temporal would only
/// retry the step after its heartbeat timeout.
async fn abandon_after_cancel<T>(
    work: impl std::future::Future<Output = Result<T, PluginError>>,
    cancelled: impl std::future::Future<Output = ()>,
    grace: Duration,
) -> Result<T, PluginError> {
    tokio::pin!(work);
    tokio::select! {
        result = &mut work => return result,
        () = cancelled => {}
    }
    match tokio::time::timeout(grace, work).await {
        Ok(result) => result,
        Err(_) => {
            tracing::warn!(
                grace_secs = grace.as_secs(),
                "plugin ignored cancellation, abandoning it"
            );
            Err(PluginError::Cancelled)
        }
    }
}

/// Input of the `expand_fan_out` activity: split a (possibly spilled) upstream output
/// into branch inputs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FanOutActivityInput {
    /// Job id.
    pub job_id: Uuid,
    /// Fan-out step id.
    pub step_id: String,
    /// Upstream output (usually a `Ref` when this activity is needed).
    pub upstream: PluginOutput,
    /// Fan-out path (`$.documents` or `$.many`).
    pub path: String,
    /// Maximum number of branches.
    pub max_branches: usize,
}

/// Input of the `record_job_started` activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobStartedInput {
    /// Job id.
    pub job_id: Uuid,
    /// First step about to run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
}

/// Output of the `expand_fan_out` activity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FanOutActivityOutput {
    /// One input per branch. Branches are spilled to the blob store as needed for the
    /// whole list to fit the spill threshold.
    pub branches: Vec<PluginInput>,
}

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

/// Map a usage-reporting error onto Temporal's retry semantics. A function rather than
/// a `From` impl: the SDK's blanket `From<E: Error>` for `ActivityError` would conflict.
fn usage_activity_error(e: UsageReportError) -> ActivityError {
    match e {
        UsageReportError::Retryable(m) => {
            ActivityError::application(ApplicationFailure::new(anyhow::anyhow!(m)))
        }
        UsageReportError::Permanent(m) => {
            ActivityError::application(ApplicationFailure::non_retryable(anyhow::anyhow!(m)))
        }
    }
}

/// Shared state of the activities running on this worker.
pub struct StepActivities {
    /// Plugins available here.
    pub registry: Arc<PluginRegistry>,
    /// Blob store for staged inputs and spilled outputs.
    pub blob: BlobStore,
    /// HTTP client used to fetch URL inputs.
    pub http: reqwest::Client,
    /// Outputs larger than this are spilled.
    pub spill_threshold: usize,
    /// Analytics client, absent when usage reporting is not configured.
    pub usage: Option<UsageClient>,
    /// Control plane base URL, used to keep the job row's status honest and to resolve
    /// Meilisearch connections.
    pub control_plane_url: Option<String>,
    /// Bearer token for the control plane's `/internal/*` routes (`CONTROL_PLANE_TOKEN`).
    pub control_plane_token: Option<String>,
    /// Deadline of each control plane request ([`CONTROL_PLANE_TIMEOUT`]).
    pub control_plane_timeout: Duration,
    /// Key and host policy for resolving an indexer step's Meilisearch connection.
    pub connections: ConnectionSettings,
    /// Post Lab billing events to the control plane (spec §5.3).
    pub lab_events: bool,
    /// Run the AI steps of Lab accounts on the team's Gateway (`LAB_AI_GATEWAY`).
    pub lab_ai_gateway: bool,
}

impl StepActivities {
    /// Build the activity set.
    pub fn new(registry: Arc<PluginRegistry>, blob: BlobStore, spill_threshold: usize) -> Self {
        Self {
            registry,
            blob,
            http: reqwest::Client::new(),
            spill_threshold,
            usage: None,
            control_plane_url: None,
            control_plane_token: None,
            control_plane_timeout: CONTROL_PLANE_TIMEOUT,
            connections: ConnectionSettings::default(),
            lab_events: false,
            lab_ai_gateway: false,
        }
    }

    /// Attach what resolving a Meilisearch connection needs: the key that opens sealed
    /// connection keys, and the host policy re-applied at use.
    pub fn with_connections(mut self, connections: ConnectionSettings) -> Self {
        self.connections = connections;
        self
    }

    /// Attach the analytics client used by [`StepActivities::record_usage`].
    pub fn with_usage(mut self, usage: Option<UsageClient>) -> Self {
        self.usage = usage;
        self
    }

    /// Attach the control plane, so the workflow can write the job's status back.
    pub fn with_control_plane(mut self, url: Option<String>) -> Self {
        self.control_plane_url = url.map(|u| u.trim_end_matches('/').to_string());
        self
    }

    /// Present `CONTROL_PLANE_TOKEN` on every control plane request.
    pub fn with_control_plane_token(mut self, token: Option<String>) -> Self {
        self.control_plane_token = token;
        self
    }

    /// Post each finished Lab job's usage and lifecycle events to the control plane's outbox.
    pub fn with_lab_events(mut self, enabled: bool) -> Self {
        self.lab_events = enabled;
        self
    }

    /// Run the AI steps ([`LAB_AI_PLUGINS`]) of Lab accounts on the team's Gateway,
    /// with the target the control plane serves (`GET /internal/lab/ai/{account}`).
    pub fn with_lab_ai_gateway(mut self, enabled: bool) -> Self {
        self.lab_ai_gateway = enabled;
        self
    }

    fn control_plane(&self) -> ControlPlane<'_> {
        ControlPlane {
            http: &self.http,
            base_url: self.control_plane_url.as_deref(),
            token: self.control_plane_token.as_deref(),
            timeout: self.control_plane_timeout,
        }
    }

    /// Attach the step's Lab Gateway target to `plugin_ctx` when this worker runs AI
    /// steps on the Lab, the plugin is one of [`LAB_AI_PLUGINS`] and `tenant` is a Lab
    /// account. Returns whether one was attached. The key stays in memory: it is not
    /// part of the activity input, so it never reaches Temporal history.
    ///
    /// A retried attempt asks the control plane to refresh its cached target: the
    /// replica answering may still hold a key another one saw rotated.
    async fn attach_ai_target(
        &self,
        plugin_ctx: &PluginContext,
        plugin: &str,
        tenant: Option<&str>,
    ) -> Result<bool, PluginError> {
        if !self.lab_ai_gateway || !LAB_AI_PLUGINS.contains(&plugin) {
            return Ok(false);
        }
        let Some(tenant) = tenant.filter(|t| meili_ingest_lab::is_lab_account(t)) else {
            return Ok(false);
        };
        let refresh = plugin_ctx.attempt() > 1;
        match fetch_ai_target(&self.control_plane(), tenant, None, refresh).await? {
            Some(target) => {
                plugin_ctx.set_ai_target(target);
                Ok(true)
            }
            None => {
                tracing::warn!(
                    tenant,
                    plugin,
                    "LAB_AI_GATEWAY is on but the control plane served no Lab Gateway target \
                     for this Lab account (it has no LAB_INSTANCE_* credentials?); the step \
                     falls back to this worker's own provider key"
                );
                Ok(false)
            }
        }
    }

    /// After a failed step: when the Gateway refused the Lab key, report it once so the
    /// Lab replaces it before Temporal retries the step. The answer only matters to the
    /// next attempt (which fetches again), so it is logged, never returned.
    async fn report_rejected_key(&self, plugin_ctx: &PluginContext, tenant: Option<&str>) {
        let (Some(target), Some(tenant)) = (plugin_ctx.ai_target(), tenant) else {
            return;
        };
        if !target.is_rejected() {
            return;
        }
        let rejected = target.key_id.as_str();
        match fetch_ai_target(&self.control_plane(), tenant, Some(rejected), false).await {
            Ok(Some(next)) => tracing::warn!(
                tenant,
                rejected_key_id = rejected,
                key_id = %next.key_id,
                "the Gateway refused the Lab key; reported it, the retry uses the Lab's answer"
            ),
            Ok(None) => tracing::warn!(
                tenant,
                rejected_key_id = rejected,
                "the Gateway refused the Lab key; reported it, the Lab served no target"
            ),
            Err(e) => tracing::warn!(
                tenant,
                rejected_key_id = rejected,
                error = %e,
                "the Gateway refused the Lab key and reporting it failed"
            ),
        }
    }

    /// Record a finished job: status write-back, then the Lab events, then
    /// analytics. The bill goes first so an analytics outage cannot hold it back;
    /// every step is idempotent, so a retry repeats them harmlessly.
    pub async fn report_usage(&self, input: &JobUsageInput) -> Result<(), UsageReportError> {
        let job_id = input.job_id;

        // Write the outcome back to the cached job row first. Without this the job
        // list shows "queued" for finished work until someone opens that job, because
        // the gateway only refreshes a row when it serves that job's detail.
        self.patch_job(job_id, input.status, None, input.error.clone())
            .await
            .map_err(|e| UsageReportError::Retryable(e.to_string()))?;

        if self.lab_events {
            self.post_lab_events(input).await?;
        }

        let Some(client) = &self.usage else {
            // Usage reporting is off; the writes above still happened.
            return Ok(());
        };
        let events = events_for_job(input);
        match client.send(&events).await {
            Ok(()) => {
                tracing::info!(job_id = %job_id, events = events.len(), "usage recorded");
                Ok(())
            }
            Err(e) if e.is_retryable() => Err(UsageReportError::Retryable(format!(
                "usage store unavailable: {e}"
            ))),
            Err(e) => Err(UsageReportError::Permanent(format!("usage rejected: {e}"))),
        }
    }

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
        let resp = authed_within(
            self.http.post(format!("{base}/internal/lab-events")),
            self.control_plane_token.as_deref(),
            self.control_plane_timeout,
        )
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
        let unpriced = meili_ingest_usage::lab::unpriced_provider_calls(input);
        if unpriced > 0 {
            tracing::warn!(
                job_id = %input.job_id,
                pipeline = %input.pipeline_uid,
                unpriced_provider_calls = unpriced,
                "Lab usage event under-reports provider cost: some calls had no price"
            );
        }
        Ok(())
    }

    /// Patch the cached job row. Returns `Ok(false)` when no control plane is
    /// configured or the row never appeared (see [`JOB_ROW_MISSING_BACKOFF`]), and an
    /// error only when the request itself failed.
    async fn patch_job(
        &self,
        job_id: Uuid,
        status: JobStatus,
        current_step: Option<String>,
        error: Option<String>,
    ) -> Result<bool, PluginError> {
        let Some(base) = &self.control_plane_url else {
            return Ok(false);
        };
        let url = format!("{base}/internal/jobs/{job_id}");
        let mut body = serde_json::Map::new();
        body.insert("status".into(), serde_json::json!(status.as_str()));
        if let Some(step) = current_step {
            body.insert("current_step".into(), serde_json::json!(step));
        }
        if let Some(err) = error {
            // The column is a summary for operators, not a full stack trace.
            let truncated: String = err.chars().take(1000).collect();
            body.insert("error".into(), serde_json::json!(truncated));
        }
        let body = serde_json::Value::Object(body);
        let mut missing_retries = JOB_ROW_MISSING_BACKOFF.iter();
        loop {
            let resp = authed_within(
                self.http.patch(&url),
                self.control_plane_token.as_deref(),
                self.control_plane_timeout,
            )
            .json(&body)
            .send()
            .await
            .map_err(|e| PluginError::Retryable(format!("control plane unreachable: {e}")))?;
            if resp.status() == reqwest::StatusCode::NOT_FOUND {
                // The gateway inserts the row only after the workflow started, so a
                // fast worker can get here first: wait a little for the row. When
                // it never shows up the gateway's best-effort insert failed, and
                // there is nothing to update.
                if let Some(delay) = missing_retries.next() {
                    tokio::time::sleep(*delay).await;
                    continue;
                }
                tracing::warn!(job_id = %job_id, "no cached job row to update");
                return Ok(false);
            }
            if !resp.status().is_success() {
                return Err(PluginError::Retryable(format!(
                    "control plane rejected the job status update: {}",
                    resp.status()
                )));
            }
            return Ok(true);
        }
    }

    /// Run one plugin invocation without Temporal (used by tests and the CLI).
    pub async fn run_step(
        &self,
        plugin_ctx: &PluginContext,
        input: StepActivityInput,
    ) -> Result<StepActivityOutput, PluginError> {
        let started = Instant::now();
        let plugin = self.registry.resolve(&input.plugin)?;
        let manifest = plugin.manifest();
        let resolved = self
            .blob
            .resolve_input(input.input, &self.http)
            .await
            .map_err(|e| match e {
                // A forbidden URL or object ref stays forbidden: retrying would only
                // re-run the check.
                BlobError::Blocked(_) => {
                    PluginError::NonRetryable(format!("refusing to fetch the step input: {e}"))
                }
                // A 404 / 403 / 410 ... answers the same way on every attempt.
                BlobError::Rejected(_) => {
                    PluginError::NonRetryable(format!("failed to resolve step input: {e}"))
                }
                other => PluginError::Retryable(format!("failed to resolve step input: {other}")),
            })?;
        if !manifest.accepts.is_empty() && !manifest.accepts_kind(resolved.kind()) {
            // Be lenient for Many → Documents: most plugins accepting Documents can take a
            // flattened Many.
            if manifest.accepts_kind(meili_ingest_plugin_sdk::InputKind::Documents)
                && resolved.kind() == meili_ingest_plugin_sdk::InputKind::Many
            {
                // fall through: plugins use into_documents() which flattens Many
            } else {
                return Err(PluginError::InvalidInput(format!(
                    "plugin {} accepts {:?} but received {:?}",
                    manifest.name,
                    manifest.accepts,
                    resolved.kind()
                )));
            }
        }
        let input_bytes = input_size_bytes(&resolved);
        tracing::info!(
            job_id = %input.job_id,
            step = %input.step_id,
            plugin = %input.plugin,
            branch = ?input.branch,
            input_kind = ?resolved.kind(),
            input_bytes,
            "executing step"
        );
        // Resolved here, in memory, rather than in the workflow: the activity input only
        // ever names the connection, so its key never reaches Temporal history.
        let config = resolve_connection(
            &self.control_plane(),
            &self.connections,
            &input.plugin,
            input.config,
            input.tenant_id.as_deref(),
        )
        .await?;
        // Same rule for a Lab tenant's Gateway key: fetched here, held by the context.
        let lab_target = self
            .attach_ai_target(plugin_ctx, &input.plugin, input.tenant_id.as_deref())
            .await?;
        let output = match plugin.execute(plugin_ctx, resolved, config).await {
            Ok(output) => output,
            Err(e) => {
                self.report_rejected_key(plugin_ctx, input.tenant_id.as_deref())
                    .await;
                return Err(e);
            }
        };
        let doc_count = output.document_count();
        let output = self
            .blob
            .spill_output(
                input.job_id,
                &input.step_id,
                input.branch,
                output,
                branch_spill_threshold(self.spill_threshold, input.branch_total, input.merge_total),
            )
            .await
            .map_err(|e| PluginError::Retryable(format!("failed to spill step output: {e}")))?;
        let duration_ms = started.elapsed().as_millis() as u64;
        // Whatever the plugin spent on external services during `execute`.
        let mut usage = plugin_ctx.usage();
        if lab_target {
            // The team's Gateway bills these calls to the team; glutony paid no provider.
            usage.cost_micro_usd = 0;
            usage.unpriced_calls = 0;
        }
        tracing::info!(
            job_id = %input.job_id,
            step = %input.step_id,
            plugin = %input.plugin,
            documents = doc_count,
            input_bytes,
            duration_ms,
            llm_tokens = usage.llm_input_tokens + usage.llm_output_tokens,
            audio_seconds = usage.audio_seconds,
            "step finished"
        );
        Ok(StepActivityOutput {
            step_id: input.step_id,
            output,
            duration_ms,
            usage,
            input_bytes,
            documents_out: doc_count as u64,
        })
    }

    /// Expand a fan-out without Temporal.
    pub async fn run_fan_out(
        &self,
        input: FanOutActivityInput,
    ) -> Result<FanOutActivityOutput, PluginError> {
        let hydrated = self
            .blob
            .hydrate_output(input.upstream)
            .await
            .map_err(|e| {
                PluginError::Retryable(format!("failed to hydrate upstream output: {e}"))
            })?;
        let branches = match crate::dag::fan_out_branches(&input.path, hydrated, input.max_branches)
        {
            crate::dag::FanOut::Branches(b) => b,
            crate::dag::FanOut::NeedsActivity(_) => {
                return Err(PluginError::NonRetryable(format!(
                    "fan_out {:?} of step {}: the upstream output is still a reference after \
                     hydration",
                    input.path, input.step_id
                )));
            }
            crate::dag::FanOut::Invalid(why) => {
                return Err(PluginError::InvalidInput(format!(
                    "fan_out {:?} of step {} cannot split the upstream output: {why}",
                    input.path, input.step_id
                )));
            }
        };
        // The whole branch list is one activity result, so it is the list that has to
        // fit the budget, not each branch. Branches stay inline while there is room
        // for them and for a reference to every branch still to come; the rest are
        // spilled.
        let total = branches.len();
        let mut used = 0usize;
        let mut out = Vec::with_capacity(total);
        for (i, branch) in branches.into_iter().enumerate() {
            let as_output = match branch {
                PluginInput::Documents(d) => PluginOutput::Documents(d),
                PluginInput::Bytes(b) => PluginOutput::Bytes(b),
                PluginInput::Many(m) => PluginOutput::Many(m),
                PluginInput::Ref(r) => PluginOutput::Ref(r),
                PluginInput::Empty => PluginOutput::Empty,
            };
            let reserved = (total - i - 1).saturating_mul(SPILLED_REF_SIZE);
            let room = self
                .spill_threshold
                .saturating_sub(used.saturating_add(reserved))
                // Nothing gets smaller than the reference that would replace it.
                .max(SPILLED_REF_SIZE);
            let spilled = self
                .blob
                .spill_output(
                    input.job_id,
                    &format!("{}-fanout", input.step_id),
                    Some(i),
                    as_output,
                    room,
                )
                .await
                .map_err(|e| {
                    PluginError::Retryable(format!("failed to spill fan-out branch: {e}"))
                })?;
            used = used.saturating_add(spilled.approx_size());
            out.push(PluginInput::from(spilled));
        }
        Ok(FanOutActivityOutput { branches: out })
    }
}

/// What a spilled output weighs once it is a reference
/// ([`PluginOutput::approx_size`] of a `Ref`).
const SPILLED_REF_SIZE: usize = 256;

/// Spill threshold of one output of a step running as `branch_total` branches, whose
/// output a later step merges with `merge_total - 1` others.
///
/// The workflow merges the branch outputs into one `Many`, and a step with several
/// dependencies merges their outputs into another, which is the single input of that
/// step: it is the sum that has to stay under the threshold, and the outputs are
/// produced without knowing each other's size. So each gets an equal share of the
/// budget. A step that is neither fanned out nor merged keeps the whole of it.
fn branch_spill_threshold(
    threshold: usize,
    branch_total: Option<usize>,
    merge_total: Option<usize>,
) -> usize {
    let shares = branch_total
        .unwrap_or(1)
        .max(1)
        .saturating_mul(merge_total.unwrap_or(1).max(1));
    if shares <= 1 {
        return threshold;
    }
    // Never below a reference: spilling something smaller would only grow it.
    (threshold / shares).max(SPILLED_REF_SIZE)
}

/// Bytes a step actually consumed, for per-tenant metering.
///
/// Raw uploads report their true length; document inputs report the text and fields
/// they carry, which is what a tenant is charged for processing.
fn input_size_bytes(input: &PluginInput) -> u64 {
    match input {
        PluginInput::Bytes(b) => b.data.len() as u64,
        other => other.approx_size() as u64,
    }
}

/// Map a plugin error onto Temporal's retry semantics.
pub fn to_activity_error(err: PluginError) -> ActivityError {
    match err {
        PluginError::Cancelled => ActivityError::cancelled(),
        e if e.is_retryable() => {
            ActivityError::application(ApplicationFailure::new(anyhow::anyhow!("{e}")))
        }
        e => ActivityError::application(ApplicationFailure::non_retryable(anyhow::anyhow!("{e}"))),
    }
}

/// The checkpoint the previous attempt of this activity recorded with
/// [`PluginContext::record_progress`], read from the heartbeat details Temporal hands a
/// retry. `None` on a first attempt, and for details that carry no checkpoint (a plain
/// heartbeat message) or that cannot be decoded: the plugin then starts over.
fn previous_progress(ctx: &ActivityContext) -> Option<serde_json::Value> {
    match ctx.heartbeat_details().deserialize::<serde_json::Value>() {
        Ok(details) => details
            .as_ref()
            .and_then(PluginContext::progress_from_heartbeat_details),
        Err(e) => {
            tracing::debug!(error = %e, "ignoring undecodable heartbeat details");
            None
        }
    }
}

/// Wire the SDK [`ActivityContext`] to a plugin-facing context: heartbeats sent by the
/// plugin are forwarded to Temporal, a keepalive heartbeat is sent periodically, and
/// Temporal cancellation flips the shared flag the plugin polls.
///
/// Every heartbeat carries the plugin's latest checkpoint
/// ([`PluginContext::heartbeat_details`]), because Temporal keeps only the last details,
/// and the previous attempt's checkpoint is handed to the plugin
/// ([`PluginContext::previous_progress`]).
fn bridge_context(
    ctx: &ActivityContext,
    job_id: Uuid,
    step_id: &str,
) -> (PluginContext, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let cancelled = Arc::new(AtomicBool::new(false));
    let plugin_ctx = PluginContext::new(job_id, step_id, ctx.info().attempt, tx, cancelled.clone())
        .with_previous_progress(previous_progress(ctx));
    // Reads the checkpoint for each heartbeat. It holds a sender too, so the channel
    // never closes on its own: `execute_step` aborts this task once the plugin returns.
    let details = plugin_ctx.clone();
    let sdk_ctx = ctx.clone();
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(KEEPALIVE_HEARTBEAT);
        tick.tick().await; // first tick fires immediately; skip it
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(message) => {
                        let payload = details.heartbeat_details(message);
                        if let Err(e) = sdk_ctx.record_heartbeat(payload).await {
                            tracing::debug!(error = %e, "heartbeat failed");
                        }
                    }
                    None => break,
                },
                _ = tick.tick() => {
                    let payload = details.heartbeat_details("keepalive");
                    if let Err(e) = sdk_ctx.record_heartbeat(payload).await {
                        tracing::debug!(error = %e, "keepalive heartbeat failed");
                    }
                }
                _ = sdk_ctx.cancelled() => {
                    cancelled.store(true, Ordering::Relaxed);
                    tracing::warn!("activity cancellation requested");
                    // keep draining heartbeats until the plugin returns
                    while rx.recv().await.is_some() {}
                    break;
                }
            }
        }
    });
    (plugin_ctx, handle)
}

#[activities]
impl StepActivities {
    /// Execute one pipeline step (one branch when fanned out).
    #[activity]
    pub async fn execute_step(
        self: Arc<Self>,
        ctx: ActivityContext,
        input: StepActivityInput,
    ) -> Result<StepActivityOutput, ActivityError> {
        let (plugin_ctx, forwarder) = bridge_context(&ctx, input.job_id, &input.step_id);
        let result = abandon_after_cancel(
            self.run_step(&plugin_ctx, input),
            ctx.cancelled(),
            CANCEL_GRACE,
        )
        .await;
        forwarder.abort();
        result.map_err(to_activity_error)
    }

    /// Split a spilled upstream output into fan-out branches.
    #[activity]
    pub async fn expand_fan_out(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: FanOutActivityInput,
    ) -> Result<FanOutActivityOutput, ActivityError> {
        self.run_fan_out(input).await.map_err(to_activity_error)
    }

    /// Ship one job's usage rows to the analytics store and, for Lab jobs, its billing
    /// event to the control plane's outbox (see [`StepActivities::report_usage`]).
    ///
    /// Runs as an activity so Temporal retries it: that is what turns best-effort
    /// telemetry into billing-grade metering. Rows carry deterministic ids, so a
    /// retry that partially succeeded does not double-count.
    #[activity]
    pub async fn record_usage(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: JobUsageInput,
    ) -> Result<(), ActivityError> {
        self.report_usage(&input)
            .await
            .map_err(usage_activity_error)
    }

    /// Mark a job as running in the cached job row.
    ///
    /// Separate from [`StepActivities::record_usage`] because it fires at the start of
    /// the workflow, when there is nothing to meter yet.
    #[activity]
    pub async fn record_job_started(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: JobStartedInput,
    ) -> Result<(), ActivityError> {
        self.patch_job(input.job_id, JobStatus::Running, input.current_step, None)
            .await
            .map(|_| ())
            .map_err(to_activity_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::Document;
    use meili_ingest_plugin_sdk::prelude::*;

    #[tokio::test]
    async fn a_plugin_ignoring_cancellation_is_abandoned_after_the_grace() {
        let never = std::future::pending::<Result<(), PluginError>>();
        let result = abandon_after_cancel(never, async {}, Duration::from_millis(10)).await;
        assert!(matches!(result, Err(PluginError::Cancelled)), "{result:?}");
    }

    #[tokio::test]
    async fn a_plugin_finishing_within_the_grace_keeps_its_result() {
        let work = async {
            tokio::time::sleep(Duration::from_millis(20)).await;
            Ok::<_, PluginError>(7)
        };
        let result = abandon_after_cancel(work, async {}, Duration::from_secs(5)).await;
        assert_eq!(result.unwrap(), 7);
        // Not cancelled: the work runs as long as it needs.
        let work = async {
            tokio::time::sleep(Duration::from_millis(30)).await;
            Ok::<_, PluginError>(8)
        };
        let result =
            abandon_after_cancel(work, std::future::pending(), Duration::from_millis(1)).await;
        assert_eq!(result.unwrap(), 8);
    }

    struct Upper;
    #[async_trait]
    impl Plugin for Upper {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("upper", "0").accepts([InputKind::Documents])
        }
        async fn execute(
            &self,
            ctx: &PluginContext,
            input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            ctx.heartbeat("working");
            let docs = input.into_documents()?;
            Ok(PluginOutput::Documents(
                docs.into_iter()
                    .map(|mut d| {
                        d.content = d.content.to_uppercase();
                        d
                    })
                    .collect(),
            ))
        }
    }

    struct Boom(PluginError);
    #[async_trait]
    impl Plugin for Boom {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("boom", "0")
        }
        async fn execute(
            &self,
            _ctx: &PluginContext,
            _input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            Err(match &self.0 {
                PluginError::Retryable(m) => PluginError::Retryable(m.clone()),
                PluginError::NonRetryable(m) => PluginError::NonRetryable(m.clone()),
                PluginError::Cancelled => PluginError::Cancelled,
                other => PluginError::NonRetryable(other.to_string()),
            })
        }
    }

    fn acts(plugins: Vec<Arc<dyn Plugin>>) -> StepActivities {
        let mut reg = PluginRegistry::new();
        for p in plugins {
            reg.register(p);
        }
        StepActivities::new(Arc::new(reg), BlobStore::memory(), 1024 * 1024)
    }

    fn step_input(plugin: &str, input: PluginInput) -> StepActivityInput {
        StepActivityInput {
            job_id: Uuid::new_v4(),
            step_id: "s".into(),
            plugin: plugin.into(),
            config: serde_json::json!({}),
            input,
            branch: None,
            branch_total: None,
            merge_total: None,
            tenant_id: None,
        }
    }

    /// Stands in for `meili_indexer` and records the config it was handed.
    struct CapturingIndexer(Arc<std::sync::Mutex<Option<serde_json::Value>>>);
    #[async_trait]
    impl Plugin for CapturingIndexer {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new(meili_ingest_plugin_sdk::INDEXER_PLUGIN, "0")
        }
        async fn execute(
            &self,
            _ctx: &PluginContext,
            _input: PluginInput,
            config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            if let Ok(mut slot) = self.0.lock() {
                *slot = Some(config);
            }
            Ok(PluginOutput::Empty)
        }
    }

    #[tokio::test]
    async fn a_connection_key_reaches_the_plugin_but_never_the_activity_input() {
        use meili_ingest_source::{HostPolicy, SecretKey};
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        const PLAINTEXT: &str = "sk-live-NEVER-IN-HISTORY";
        let key = Arc::new(SecretKey::from_bytes([5u8; 32]));
        let sealed = key.seal(PLAINTEXT.as_bytes()).expect("seal");

        let control_plane = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod-movies"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "host": "http://meilisearch:7700",
                "api_key": sealed,
            })))
            .mount(&control_plane)
            .await;

        let seen = Arc::new(std::sync::Mutex::new(None));
        let a = acts(vec![Arc::new(CapturingIndexer(seen.clone()))])
            .with_control_plane(Some(control_plane.uri()))
            .with_connections(ConnectionSettings {
                key: Some(key),
                policy: HostPolicy::parse("meilisearch:7700").expect("policy"),
            });

        let mut input = step_input(meili_ingest_plugin_sdk::INDEXER_PLUGIN, PluginInput::Empty);
        input.config = serde_json::json!({ "connection": "prod-movies", "index": "movies" });
        input.tenant_id = Some("tenant-1".into());

        // What Temporal records for this activity.
        let recorded = serde_json::to_string(&input).expect("serialize");
        assert!(
            !recorded.contains(PLAINTEXT) && !recorded.contains("api_key"),
            "the activity input must carry the connection name only: {recorded}"
        );

        a.run_step(&PluginContext::noop(), input)
            .await
            .expect("step runs");

        let received = seen.lock().expect("lock").clone().expect("plugin ran");
        assert_eq!(
            received["api_key"], PLAINTEXT,
            "the plugin gets the opened key"
        );
        assert_eq!(received["host"], "http://meilisearch:7700");
        assert_eq!(received["connection"], "prod-movies");
    }

    /// A Lab account id (canonical UUID), so its AI steps go through the Lab.
    const LAB_ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
    const OTHER_LAB_ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b62";
    const LAB_KEY: &str = "sk-lab-NEVER-IN-HISTORY";

    /// Stands in for an AI plugin: answers with the `key_id` of the target it saw
    /// (`"none"` without one), records priced usage, and can refuse the key.
    struct Probe {
        name: &'static str,
        reject: bool,
        runs: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl Probe {
        fn new(name: &'static str) -> Self {
            Self {
                name,
                reject: false,
                runs: Arc::default(),
            }
        }

        fn rejecting(mut self) -> Self {
            self.reject = true;
            self
        }
    }

    #[async_trait]
    impl Plugin for Probe {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new(self.name, "0")
        }
        async fn execute(
            &self,
            ctx: &PluginContext,
            _input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            self.runs.fetch_add(1, Ordering::SeqCst);
            ctx.record_usage(UsageUnits {
                llm_input_tokens: 10,
                cost_micro_usd: 50,
                unpriced_calls: 1,
                ..Default::default()
            });
            let target = ctx.ai_target();
            if self.reject
                && let Some(t) = target
            {
                t.mark_rejected();
                return Err(PluginError::Retryable("the Gateway refused the key".into()));
            }
            let seen = target.map_or_else(|| "none".to_string(), |t| t.key_id.clone());
            Ok(PluginOutput::Documents(vec![Document::with_id(
                "seen", seen,
            )]))
        }
    }

    fn seen_key(out: &StepActivityOutput) -> String {
        match &out.output {
            PluginOutput::Documents(d) => d[0].content.clone(),
            other => panic!("{other:?}"),
        }
    }

    fn lab_target(key_id: &str) -> serde_json::Value {
        serde_json::json!({ "target": {
            "base_url": "http://lumen:8080/v1",
            "api_key": LAB_KEY,
            "key_id": key_id,
            "default_model": "openai/gpt-4o-mini",
            "vision_model": "openai/gpt-4o-mini",
            "cache_ttl": 300,
        }})
    }

    /// A control plane answering `GET /internal/lab/ai/{account}` with `answer`.
    async fn ai_control_plane(answer: ResponseTemplate) -> MockServer {
        let cp = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path_regex(r"^/internal/lab/ai/[^/]+$"))
            .respond_with(answer)
            .mount(&cp)
            .await;
        cp
    }

    async fn ai_requests(cp: &MockServer) -> Vec<wiremock::Request> {
        cp.received_requests()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.url.path().starts_with("/internal/lab/ai/"))
            .collect()
    }

    fn ai_acts(cp: &MockServer, plugin: Probe, lab_ai_gateway: bool) -> StepActivities {
        acts(vec![Arc::new(plugin)])
            .with_control_plane(Some(cp.uri()))
            .with_control_plane_token(Some("cp-token".into()))
            .with_lab_ai_gateway(lab_ai_gateway)
    }

    fn ai_step(plugin: &str, tenant: &str) -> StepActivityInput {
        let mut input = step_input(plugin, PluginInput::Empty);
        input.tenant_id = Some(tenant.into());
        input
    }

    #[tokio::test]
    async fn a_lab_tenant_ai_step_gets_its_target() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let a = ai_acts(&cp, Probe::new("llm_enricher"), true);
        let input = ai_step("llm_enricher", LAB_ACCOUNT);

        // What Temporal records for this activity.
        let recorded = serde_json::to_string(&input).expect("serialize");
        assert!(
            !recorded.contains("api_key") && !recorded.contains(LAB_KEY),
            "the activity input must not carry the Gateway key: {recorded}"
        );

        let out = a
            .run_step(&PluginContext::noop(), input)
            .await
            .expect("step runs");
        assert_eq!(seen_key(&out), "k1");
        let reqs = ai_requests(&cp).await;
        assert_eq!(reqs.len(), 1);
        assert_eq!(
            reqs[0].url.path(),
            format!("/internal/lab/ai/{LAB_ACCOUNT}")
        );
        assert_eq!(
            reqs[0]
                .headers
                .get("authorization")
                .and_then(|v| v.to_str().ok()),
            Some("Bearer cp-token")
        );
        let shown = format!("{out:?}");
        assert!(!shown.contains(LAB_KEY), "{shown}");
    }

    #[tokio::test]
    async fn a_non_lab_tenant_gets_no_target_and_no_call() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let a = ai_acts(&cp, Probe::new("llm_enricher"), true);
        let out = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", "tenant-1"))
            .await
            .expect("step runs");
        assert_eq!(seen_key(&out), "none");
        assert!(ai_requests(&cp).await.is_empty());

        let no_tenant = step_input("llm_enricher", PluginInput::Empty);
        let out = a
            .run_step(&PluginContext::noop(), no_tenant)
            .await
            .expect("step runs");
        assert_eq!(seen_key(&out), "none");
        assert!(ai_requests(&cp).await.is_empty());
    }

    #[tokio::test]
    async fn the_flag_off_makes_no_call() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let a = ai_acts(&cp, Probe::new("llm_enricher"), false);
        let out = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect("step runs");
        assert_eq!(seen_key(&out), "none");
        assert!(ai_requests(&cp).await.is_empty());
    }

    #[tokio::test]
    async fn a_non_ai_plugin_makes_no_call() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let a = ai_acts(&cp, Probe::new("chunker"), true);
        let out = a
            .run_step(&PluginContext::noop(), ai_step("chunker", LAB_ACCOUNT))
            .await
            .expect("step runs");
        assert_eq!(seen_key(&out), "none");
        assert!(ai_requests(&cp).await.is_empty());
    }

    #[tokio::test]
    async fn a_rejected_key_is_reported_once() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let probe = Probe::new("llm_enricher").rejecting();
        let runs = probe.runs.clone();
        let a = ai_acts(&cp, probe, true);
        let err = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect_err("the Gateway refused the key");
        assert!(err.is_retryable(), "{err:?}");
        assert!(!err.to_string().contains(LAB_KEY), "{err}");
        assert_eq!(runs.load(Ordering::SeqCst), 1);

        let reqs = ai_requests(&cp).await;
        assert_eq!(reqs.len(), 2, "the lookup, then one report: {reqs:?}");
        assert!(reqs[0].url.query().is_none(), "{:?}", reqs[0].url);
        let reported = reqs
            .iter()
            .filter(|r| {
                r.url
                    .query_pairs()
                    .any(|(k, v)| k == "rejected_key_id" && v == "k1")
            })
            .count();
        assert_eq!(reported, 1);
    }

    #[tokio::test]
    async fn a_retried_step_asks_for_a_refreshed_target() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let a = ai_acts(&cp, Probe::new("llm_enricher"), true);
        let ctx_at = |attempt| {
            let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
            PluginContext::new(
                uuid::Uuid::nil(),
                "s",
                attempt,
                tx,
                Arc::new(std::sync::atomic::AtomicBool::new(false)),
            )
        };
        a.run_step(&ctx_at(1), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect("attempt 1 runs");
        a.run_step(&ctx_at(2), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect("attempt 2 runs");
        let reqs = ai_requests(&cp).await;
        assert_eq!(reqs.len(), 2);
        assert_eq!(reqs[0].url.query(), None, "attempt 1 uses the cache");
        assert_eq!(reqs[1].url.query(), Some("refresh=true"));
    }

    #[tokio::test]
    async fn a_lab_target_step_reports_no_provider_cost() {
        let cp = ai_control_plane(ResponseTemplate::new(200).set_body_json(lab_target("k1"))).await;
        let a = ai_acts(&cp, Probe::new("llm_enricher"), true);
        let out = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect("step runs");
        assert_eq!(out.usage.cost_micro_usd, 0, "the Gateway bills the team");
        assert_eq!(out.usage.unpriced_calls, 0);
        assert_eq!(out.usage.llm_input_tokens, 10, "units are still metered");

        // Without a Lab target the provider cost is ours and stays.
        let out = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", "tenant-1"))
            .await
            .expect("step runs");
        assert_eq!(out.usage.cost_micro_usd, 50);
        assert_eq!(out.usage.unpriced_calls, 1);
    }

    #[tokio::test]
    async fn two_tenants_get_their_own_keys() {
        let cp = MockServer::start().await;
        for (account, key_id) in [(LAB_ACCOUNT, "k-a"), (OTHER_LAB_ACCOUNT, "k-b")] {
            Mock::given(method("GET"))
                .and(path(format!("/internal/lab/ai/{account}")))
                .respond_with(
                    ResponseTemplate::new(200)
                        .set_body_json(lab_target(key_id))
                        .set_delay(Duration::from_millis(20)),
                )
                .mount(&cp)
                .await;
        }
        let a = ai_acts(&cp, Probe::new("llm_enricher"), true);
        let (ctx_a, ctx_b) = (PluginContext::noop(), PluginContext::noop());
        let (out_a, out_b) = tokio::join!(
            a.run_step(&ctx_a, ai_step("llm_enricher", LAB_ACCOUNT)),
            a.run_step(&ctx_b, ai_step("llm_enricher", OTHER_LAB_ACCOUNT)),
        );
        assert_eq!(seen_key(&out_a.expect("a runs")), "k-a");
        assert_eq!(seen_key(&out_b.expect("b runs")), "k-b");
    }

    #[tokio::test]
    async fn no_gateway_is_final() {
        let cp = ai_control_plane(ResponseTemplate::new(404).set_body_json(serde_json::json!({
            "error": format!("account {LAB_ACCOUNT} has no Gateway"),
            "code": "no_gateway",
        })))
        .await;
        let probe = Probe::new("llm_enricher");
        let runs = probe.runs.clone();
        let a = ai_acts(&cp, probe, true);
        let err = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect_err("no Gateway");
        assert!(!err.is_retryable(), "{err:?}");
        assert!(err.to_string().contains("no Gateway"), "{err}");
        assert_eq!(runs.load(Ordering::SeqCst), 0, "the plugin never runs");
    }

    #[tokio::test]
    async fn run_step_dispatches_to_plugin() {
        let a = acts(vec![Arc::new(Upper)]);
        let out = a
            .run_step(
                &PluginContext::noop(),
                step_input(
                    "upper",
                    PluginInput::Documents(vec![Document::with_id("a", "hi")]),
                ),
            )
            .await
            .unwrap();
        assert_eq!(out.step_id, "s");
        match out.output {
            PluginOutput::Documents(d) => assert_eq!(d[0].content, "HI"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn run_step_flattens_many_for_document_plugins() {
        let a = acts(vec![Arc::new(Upper)]);
        let many = PluginInput::Many(vec![
            PluginOutput::Documents(vec![Document::with_id("a", "x")]),
            PluginOutput::Documents(vec![Document::with_id("b", "y")]),
        ]);
        let out = a
            .run_step(&PluginContext::noop(), step_input("upper", many))
            .await
            .unwrap();
        assert_eq!(out.output.document_count(), 2);
    }

    #[tokio::test]
    async fn run_step_rejects_wrong_input_kind() {
        let a = acts(vec![Arc::new(Upper)]);
        let err = a
            .run_step(
                &PluginContext::noop(),
                step_input(
                    "upper",
                    PluginInput::Bytes(Blob::new(vec![1], "application/pdf", None)),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn run_step_unknown_plugin_is_non_retryable() {
        let a = acts(vec![]);
        let err = a
            .run_step(
                &PluginContext::noop(),
                step_input("nope", PluginInput::Empty),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)));
    }

    #[tokio::test]
    async fn run_step_spills_large_output() {
        let mut reg = PluginRegistry::new();
        reg.register(Arc::new(Upper));
        let a = StepActivities::new(Arc::new(reg), BlobStore::memory(), 64);
        let big = "x".repeat(10_000);
        let out = a
            .run_step(
                &PluginContext::noop(),
                step_input(
                    "upper",
                    PluginInput::Documents(vec![Document::with_id("a", big)]),
                ),
            )
            .await
            .unwrap();
        assert!(matches!(out.output, PluginOutput::Ref(_)));
        // and it hydrates back
        let back = a.blob.hydrate_output(out.output).await.unwrap();
        assert_eq!(back.document_count(), 1);
    }

    #[tokio::test]
    async fn fan_out_expands_spilled_output() {
        let a = StepActivities::new(Arc::new(PluginRegistry::new()), BlobStore::memory(), 64);
        let job = Uuid::new_v4();
        let docs: Vec<Document> = (0..3)
            .map(|i| Document::with_id(format!("d{i}"), "y".repeat(100)))
            .collect();
        let spilled = a
            .blob
            .spill_output(job, "prev", None, PluginOutput::Documents(docs), 64)
            .await
            .unwrap();
        assert!(matches!(spilled, PluginOutput::Ref(_)));
        let out = a
            .run_fan_out(FanOutActivityInput {
                job_id: job,
                step_id: "enrich".into(),
                upstream: spilled,
                path: "$.documents".into(),
                max_branches: 100,
            })
            .await
            .unwrap();
        assert_eq!(out.branches.len(), 3);
        // each branch is resolvable back to exactly one document
        for b in out.branches {
            let resolved = a.blob.resolve_input(b, &a.http).await.unwrap();
            assert_eq!(resolved.into_documents().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn fan_out_branches_fit_the_payload_budget_together() {
        // A spilled upstream well over Temporal's 2 MiB payload cap: the branch list
        // is one activity result, so it must fit the budget as a whole.
        let threshold = 1024 * 1024;
        let a = StepActivities::new(
            Arc::new(PluginRegistry::new()),
            BlobStore::memory(),
            threshold,
        );
        let job = Uuid::new_v4();
        let docs: Vec<Document> = (0..3000)
            .map(|i| Document::with_id(format!("d{i}"), "y".repeat(1024)))
            .collect();
        let upstream = PluginOutput::Documents(docs);
        assert!(upstream.approx_size() > 2 * threshold);
        let spilled = a
            .blob
            .spill_output(job, "chunk", None, upstream, threshold)
            .await
            .unwrap();
        assert!(matches!(spilled, PluginOutput::Ref(_)));
        let out = a
            .run_fan_out(FanOutActivityInput {
                job_id: job,
                step_id: "enrich".into(),
                upstream: spilled,
                path: "$.documents".into(),
                max_branches: 500,
            })
            .await
            .unwrap();
        assert_eq!(out.branches.len(), 500);
        let total: usize = out.branches.iter().map(PluginInput::approx_size).sum();
        assert!(
            total <= threshold,
            "the branch list is {total} bytes, over the {threshold} byte budget"
        );
        // Small branches still travel inline: only the overflow is spilled.
        assert!(matches!(out.branches[0], PluginInput::Documents(_)));
        // Nothing is lost on the way.
        let mut seen = 0;
        for b in out.branches {
            let resolved = a.blob.resolve_input(b, &a.http).await.unwrap();
            seen += resolved.into_documents().unwrap().len();
        }
        assert_eq!(seen, 3000);
    }

    /// Run `total` branches of a fanned-out `upper` step, each over `content_len` bytes
    /// of content, and return what the workflow merges: `Many` of the branch outputs.
    async fn fan_in(a: &StepActivities, total: usize, content_len: usize) -> PluginOutput {
        let job = Uuid::new_v4();
        let mut outputs = Vec::with_capacity(total);
        for i in 0..total {
            let mut input = step_input(
                "upper",
                PluginInput::Documents(vec![Document::with_id(
                    format!("d{i}"),
                    "y".repeat(content_len),
                )]),
            );
            input.job_id = job;
            input.branch = Some(i);
            input.branch_total = Some(total);
            let out = a.run_step(&PluginContext::noop(), input).await.unwrap();
            outputs.push(out.output);
        }
        PluginOutput::Many(outputs)
    }

    fn upper_acts(threshold: usize) -> StepActivities {
        let mut reg = PluginRegistry::new();
        reg.register(Arc::new(Upper));
        StepActivities::new(Arc::new(reg), BlobStore::memory(), threshold)
    }

    #[tokio::test]
    async fn merged_branch_outputs_fit_the_payload_budget_together() {
        let threshold = 64 * 1024;
        let a = upper_acts(threshold);
        // Each branch output is well under the threshold on its own, but the eight of
        // them are merged into the single input of the next step.
        let merged = fan_in(&a, 8, 16 * 1024).await;
        assert!(
            merged.approx_size() <= threshold,
            "the merged output is {} bytes, over the {threshold} byte budget",
            merged.approx_size()
        );
        let back = a.blob.hydrate_output(merged).await.unwrap();
        assert_eq!(back.document_count(), 8);
    }

    #[tokio::test]
    async fn small_branch_outputs_stay_inline() {
        let a = upper_acts(64 * 1024);
        let PluginOutput::Many(outputs) = fan_in(&a, 8, 100).await else {
            panic!("fan_in returns Many");
        };
        assert!(
            outputs
                .iter()
                .all(|o| matches!(o, PluginOutput::Documents(_))),
            "{outputs:?}"
        );
    }

    #[tokio::test]
    async fn fan_out_hydrates_spilled_outputs_nested_in_a_fan_in() {
        // The upstream is itself a fanned-out step: `Many` of branch outputs, one of
        // them spilled.
        let a = StepActivities::new(Arc::new(PluginRegistry::new()), BlobStore::memory(), 64);
        let job = Uuid::new_v4();
        let spilled = a
            .blob
            .spill_output(
                job,
                "prev",
                Some(1),
                PluginOutput::Documents(vec![
                    Document::with_id("b", "y".repeat(100)),
                    Document::with_id("c", "y".repeat(100)),
                ]),
                64,
            )
            .await
            .unwrap();
        assert!(matches!(spilled, PluginOutput::Ref(_)));
        let upstream = PluginOutput::Many(vec![
            PluginOutput::Documents(vec![Document::with_id("a", "x")]),
            spilled,
        ]);
        let out = a
            .run_fan_out(FanOutActivityInput {
                job_id: job,
                step_id: "enrich".into(),
                upstream,
                path: "$.documents".into(),
                max_branches: 100,
            })
            .await
            .unwrap();
        assert_eq!(out.branches.len(), 3);
        let mut ids = Vec::new();
        for b in out.branches {
            let resolved = a.blob.resolve_input(b, &a.http).await.unwrap();
            ids.extend(resolved.into_documents().unwrap().into_iter().map(|d| d.id));
        }
        assert_eq!(ids, ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn fan_out_over_an_output_without_documents_fails() {
        let a = StepActivities::new(Arc::new(PluginRegistry::new()), BlobStore::memory(), 64);
        let err = a
            .run_fan_out(FanOutActivityInput {
                job_id: Uuid::new_v4(),
                step_id: "enrich".into(),
                upstream: PluginOutput::Bytes(Blob::new(vec![1], "application/pdf", None)),
                path: "$.documents".into(),
                max_branches: 100,
            })
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err:?}");
        assert!(err.to_string().contains("enrich"), "{err}");
    }

    /// Hands its input straight back, whatever it is: shows what the step received.
    struct Echo;
    #[async_trait]
    impl Plugin for Echo {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("echo", "0")
        }
        async fn execute(
            &self,
            _ctx: &PluginContext,
            input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            match input {
                PluginInput::Bytes(b) => Ok(PluginOutput::Bytes(b)),
                other => Err(PluginError::InvalidInput(format!("{:?}", other.kind()))),
            }
        }
    }

    fn guarded_acts(policy: &str) -> StepActivities {
        let mut reg = PluginRegistry::new();
        reg.register(Arc::new(Echo));
        let blob = BlobStore::memory().with_fetch_guard(crate::fetch_guard::fetch_guard(
            meili_ingest_source::HostPolicy::parse(policy).unwrap(),
        ));
        StepActivities::new(Arc::new(reg), blob, 1024 * 1024)
    }

    fn url_input(url: String) -> PluginInput {
        PluginInput::Ref(meili_ingest_plugin_sdk::ContentRef::Url {
            url,
            mime: None,
            filename: None,
        })
    }

    #[tokio::test]
    async fn a_loopback_url_ref_is_blocked_non_retryably_under_public() {
        let internal = wiremock::MockServer::start().await;
        let a = guarded_acts("public");
        let err = a
            .run_step(
                &PluginContext::noop(),
                step_input(
                    "echo",
                    url_input(format!("{}/?query=SELECT 1", internal.uri())),
                ),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err:?}");
        assert!(err.to_string().contains("SOURCE_FETCH_HOSTS"), "{err}");
        assert!(internal.received_requests().await.unwrap().is_empty());

        // https does not help: the address, not the scheme, is what is refused.
        let err = a
            .run_step(
                &PluginContext::noop(),
                step_input("echo", url_input("https://127.0.0.1:8123/".into())),
            )
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Loopback"), "{err}");
    }

    #[tokio::test]
    async fn a_redirect_to_a_host_outside_the_policy_is_blocked() {
        use wiremock::matchers::{method, path};
        let allowed = wiremock::MockServer::start().await;
        let internal = wiremock::MockServer::start().await;
        wiremock::Mock::given(method("GET"))
            .and(path("/doc.txt"))
            .respond_with(
                wiremock::ResponseTemplate::new(302)
                    .insert_header("location", format!("{}/metrics", internal.uri())),
            )
            .mount(&allowed)
            .await;
        let a = guarded_acts(allowed.uri().trim_start_matches("http://"));
        let err = a
            .run_step(
                &PluginContext::noop(),
                step_input("echo", url_input(format!("{}/doc.txt", allowed.uri()))),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err:?}");
        assert!(internal.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn an_allowlisted_host_is_fetched() {
        use wiremock::matchers::{method, path};
        let files = wiremock::MockServer::start().await;
        wiremock::Mock::given(method("GET"))
            .and(path("/doc.txt"))
            .respond_with(wiremock::ResponseTemplate::new(200).set_body_string("hello"))
            .mount(&files)
            .await;
        let a = guarded_acts(files.uri().trim_start_matches("http://"));
        let out = a
            .run_step(
                &PluginContext::noop(),
                step_input("echo", url_input(format!("{}/doc.txt", files.uri()))),
            )
            .await
            .unwrap();
        match out.output {
            PluginOutput::Bytes(b) => assert_eq!(b.data, b"hello"),
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn a_permanent_client_error_on_the_input_url_is_non_retryable() {
        // A 404 stays a 404: retrying only delays the failed job.
        for (status, permanent) in [
            (404, true),
            (403, true),
            (410, true),
            (408, false),
            (429, false),
            (503, false),
        ] {
            let files = wiremock::MockServer::start().await;
            wiremock::Mock::given(wiremock::matchers::method("GET"))
                .respond_with(wiremock::ResponseTemplate::new(status))
                .mount(&files)
                .await;
            let err = guarded_acts(files.uri().trim_start_matches("http://"))
                .run_step(
                    &PluginContext::noop(),
                    step_input("echo", url_input(format!("{}/doc.txt", files.uri()))),
                )
                .await
                .unwrap_err();
            if permanent {
                assert!(
                    matches!(err, PluginError::NonRetryable(_)),
                    "{status}: {err:?}"
                );
            } else {
                assert!(
                    matches!(err, PluginError::Retryable(_)),
                    "{status}: {err:?}"
                );
            }
            assert!(err.to_string().contains(&status.to_string()), "{err}");
        }
    }

    #[tokio::test]
    async fn a_public_address_passes_the_public_policy() {
        // The check only: no request leaves the test. An IP literal needs no DNS.
        use meili_ingest_blob::UrlCheck as _;
        let check = crate::fetch_guard::PolicyCheck(meili_ingest_source::HostPolicy::Public);
        check
            .check(&url::Url::parse("https://1.1.1.1/doc.pdf").unwrap())
            .await
            .unwrap();
        for blocked in [
            "https://169.254.169.254/latest/meta-data/",
            "https://10.0.0.5/",
            "https://[::1]/",
            "http://1.1.1.1/doc.pdf",
        ] {
            assert!(
                check
                    .check(&url::Url::parse(blocked).unwrap())
                    .await
                    .is_err(),
                "{blocked} must be refused under `public`"
            );
        }
    }

    use wiremock::matchers::{method, path, path_regex};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            .respond_with(
                ResponseTemplate::new(status).set_body_json(serde_json::json!({"inserted": 1})),
            )
            .expect(expect)
            .mount(&cp)
            .await;
        cp
    }

    #[tokio::test]
    async fn a_job_patch_that_beats_the_gateway_insert_is_tried_again() {
        // The gateway starts the workflow, then inserts the cached row. A fast worker
        // patches before that insert lands and gets a 404: the row is late, not
        // missing, so the status must not be dropped.
        let cp = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path_regex(r"^/internal/jobs/.+$"))
            .respond_with(ResponseTemplate::new(404))
            .up_to_n_times(1)
            .mount(&cp)
            .await;
        Mock::given(method("PATCH"))
            .and(path_regex(r"^/internal/jobs/.+$"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
            .mount(&cp)
            .await;
        let patched = acts_with(&cp, false)
            .patch_job(
                Uuid::new_v4(),
                JobStatus::Running,
                Some("extract".into()),
                None,
            )
            .await
            .expect("patch");
        assert!(patched, "the row that appeared was patched");
        let requests = cp.received_requests().await.unwrap();
        assert_eq!(requests.len(), 2);
        let body: serde_json::Value = serde_json::from_slice(&requests[1].body).unwrap();
        assert_eq!(
            body,
            serde_json::json!({"status": "running", "current_step": "extract"})
        );
    }

    #[tokio::test]
    async fn a_job_patch_gives_up_on_a_row_that_never_appears() {
        // The gateway's insert is best effort: when it failed there is no row, and
        // the patch must stay a no-op that neither fails nor holds the job for long.
        let cp = MockServer::start().await;
        Mock::given(method("PATCH"))
            .and(path_regex(r"^/internal/jobs/.+$"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&cp)
            .await;
        let started = Instant::now();
        let patched = acts_with(&cp, false)
            .patch_job(Uuid::new_v4(), JobStatus::Succeeded, None, None)
            .await
            .expect("a missing row is not an error");
        assert!(!patched);
        assert_eq!(cp.received_requests().await.unwrap().len(), 4);
        assert!(started.elapsed() < Duration::from_secs(3));
    }

    fn acts_with(cp: &MockServer, lab: bool) -> StepActivities {
        StepActivities::new(
            Arc::new(PluginRegistry::builtin()),
            BlobStore::memory(),
            1 << 20,
        )
        .with_control_plane(Some(cp.uri()))
        .with_lab_events(lab)
    }

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

    #[tokio::test]
    async fn no_event_without_the_flag_or_a_lab_tenant() {
        let cp = control_plane_with_lab_events(202, 0).await;
        acts_with(&cp, false)
            .report_usage(&lab_usage_input("0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61"))
            .await
            .unwrap();
        acts_with(&cp, true)
            .report_usage(&lab_usage_input("hackersearch"))
            .await
            .unwrap();
        acts_with(&cp, true)
            .report_usage(&lab_usage_input(""))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn an_outbox_failure_is_retryable_and_comes_before_analytics() {
        let cp = control_plane_with_lab_events(500, 1).await;
        let tinybird = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&tinybird)
            .await;
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

    /// A control plane that accepts the request and then stays silent for `delay`.
    async fn silent_control_plane(delay: Duration) -> MockServer {
        let cp = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({}))
                    .set_delay(delay),
            )
            .mount(&cp)
            .await;
        cp
    }

    const SILENCE: Duration = Duration::from_secs(5);

    /// Activities whose control plane requests give up after 100 ms.
    fn impatient(a: StepActivities) -> StepActivities {
        StepActivities {
            control_plane_timeout: Duration::from_millis(100),
            ..a
        }
    }

    #[tokio::test]
    async fn a_silent_control_plane_fails_the_job_status_write_as_retryable() {
        let cp = silent_control_plane(SILENCE).await;
        let a = impatient(acts_with(&cp, false));
        let started = Instant::now();
        let err = a
            .patch_job(Uuid::new_v4(), JobStatus::Running, None, None)
            .await
            .expect_err("the control plane never answered");
        assert!(matches!(err, PluginError::Retryable(_)), "{err:?}");
        assert!(started.elapsed() < SILENCE, "{:?}", started.elapsed());
    }

    #[tokio::test]
    async fn a_silent_control_plane_fails_the_usage_report_as_retryable() {
        let cp = silent_control_plane(SILENCE).await;
        let a = impatient(acts_with(&cp, true));
        let started = Instant::now();
        let err = a
            .report_usage(&lab_usage_input("0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61"))
            .await
            .expect_err("the control plane never answered");
        assert!(matches!(err, UsageReportError::Retryable(_)), "{err:?}");
        assert!(started.elapsed() < SILENCE, "{:?}", started.elapsed());
    }

    #[tokio::test]
    async fn a_silent_control_plane_fails_the_lab_events_post_as_retryable() {
        let cp = silent_control_plane(SILENCE).await;
        let a = impatient(acts_with(&cp, true));
        let started = Instant::now();
        let err = a
            .post_lab_events(&lab_usage_input("0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61"))
            .await
            .expect_err("the control plane never answered");
        assert!(matches!(err, UsageReportError::Retryable(_)), "{err:?}");
        assert!(started.elapsed() < SILENCE, "{:?}", started.elapsed());
    }

    #[tokio::test]
    async fn a_silent_control_plane_fails_a_step_as_retryable() {
        use meili_ingest_source::{HostPolicy, SecretKey};
        let cp = silent_control_plane(SILENCE).await;

        // Resolving the indexer's connection.
        let seen = Arc::new(std::sync::Mutex::new(None));
        let a = impatient(
            acts(vec![Arc::new(CapturingIndexer(seen))])
                .with_control_plane(Some(cp.uri()))
                .with_connections(ConnectionSettings {
                    key: Some(Arc::new(SecretKey::from_bytes([5u8; 32]))),
                    policy: HostPolicy::Any,
                }),
        );
        let mut input = step_input(meili_ingest_plugin_sdk::INDEXER_PLUGIN, PluginInput::Empty);
        input.config = serde_json::json!({ "connection": "prod-movies", "index": "movies" });
        let started = Instant::now();
        let err = a
            .run_step(&PluginContext::noop(), input)
            .await
            .expect_err("the control plane never answered");
        assert!(matches!(err, PluginError::Retryable(_)), "{err:?}");
        assert!(started.elapsed() < SILENCE, "{:?}", started.elapsed());

        // Fetching a Lab tenant's Gateway target.
        let a = impatient(ai_acts(&cp, Probe::new("llm_enricher"), true));
        let started = Instant::now();
        let err = a
            .run_step(&PluginContext::noop(), ai_step("llm_enricher", LAB_ACCOUNT))
            .await
            .expect_err("the control plane never answered");
        assert!(matches!(err, PluginError::Retryable(_)), "{err:?}");
        assert!(started.elapsed() < SILENCE, "{:?}", started.elapsed());
    }

    #[test]
    fn a_spilled_reference_weighs_what_the_budget_assumes() {
        let spilled = PluginOutput::Ref(meili_ingest_plugin_sdk::ContentRef::Staged {
            uri: "jobs/x/s-0.json".into(),
            mime: Some(meili_ingest_blob::SPILLED_OUTPUT_MIME.to_owned()),
            filename: None,
            origin: None,
            charset: None,
        });
        assert_eq!(spilled.approx_size(), SPILLED_REF_SIZE);
    }

    #[test]
    fn a_fanned_out_step_shares_the_spill_budget_between_its_branches() {
        assert_eq!(branch_spill_threshold(1 << 20, None, None), 1 << 20);
        assert_eq!(branch_spill_threshold(1 << 20, Some(1), None), 1 << 20);
        assert_eq!(branch_spill_threshold(1 << 20, Some(4), None), 1 << 18);
        // Never below what a reference weighs.
        assert_eq!(
            branch_spill_threshold(64, Some(500), None),
            SPILLED_REF_SIZE
        );
    }

    #[test]
    fn a_merged_step_shares_the_spill_budget_with_the_other_merged_outputs() {
        assert_eq!(branch_spill_threshold(1 << 20, None, Some(1)), 1 << 20);
        assert_eq!(branch_spill_threshold(1 << 20, None, Some(2)), 1 << 19);
        // A fanned-out step merged with others: its branches share its share.
        assert_eq!(branch_spill_threshold(1 << 20, Some(4), Some(2)), 1 << 17);
        assert_eq!(
            branch_spill_threshold(1 << 20, Some(usize::MAX), Some(2)),
            SPILLED_REF_SIZE
        );
    }

    #[tokio::test]
    async fn outputs_merged_by_a_step_with_several_dependencies_fit_the_threshold_together() {
        let threshold = 1024 * 1024;
        let a = acts(vec![Arc::new(Upper)]);
        assert_eq!(a.spill_threshold, threshold);
        let pipeline = PipelineDefinition {
            uid: "p".into(),
            name: "p".into(),
            description: None,
            version: 1,
            trigger: None,
            steps: vec![
                StepDefinition::new("a", "upper"),
                StepDefinition::new("b", "upper"),
                StepDefinition::new("c", "upper"),
                StepDefinition::new("index", "upper").depends_on(["a", "b", "c"]),
            ],
            builtin: false,
            tenant_id: None,
        };
        // Each output alone is under the threshold, the three together are not.
        let mut done = std::collections::HashMap::new();
        for id in ["a", "b", "c"] {
            let mut input = step_input(
                "upper",
                PluginInput::Documents(vec![Document::with_id(id, "x".repeat(400 * 1024))]),
            );
            input.step_id = id.into();
            input.merge_total = crate::dag::merge_total(&pipeline, id);
            let out = a
                .run_step(&PluginContext::noop(), input)
                .await
                .expect("step runs");
            done.insert(id.to_string(), out.output);
        }
        let merged = crate::dag::resolve_input(
            pipeline.step("index").expect("step"),
            &PluginInput::Empty,
            &done,
        );
        assert!(
            merged.approx_size() <= threshold,
            "the merged input is {} bytes, over the {threshold} threshold",
            merged.approx_size()
        );
        // Nothing is lost: the next step reads every document back.
        let docs = a
            .blob
            .resolve_input(merged, &a.http)
            .await
            .expect("hydrate")
            .into_documents()
            .expect("documents");
        assert_eq!(docs.len(), 3);
    }

    /// Answers with the checkpoint the previous attempt left (`null` without one), then
    /// checkpoints `{"done": attempt}`.
    struct Checkpointer;
    #[async_trait]
    impl Plugin for Checkpointer {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("checkpointer", "0")
        }
        async fn execute(
            &self,
            ctx: &PluginContext,
            _input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            let previous = ctx.previous_progress().cloned().unwrap_or_default();
            ctx.record_progress(serde_json::json!({ "done": ctx.attempt() }));
            ctx.heartbeat("after the checkpoint");
            // Let the forwarder send the heartbeats before the activity returns.
            tokio::time::sleep(Duration::from_millis(100)).await;
            Ok(PluginOutput::Documents(vec![Document::with_id(
                "previous",
                previous.to_string(),
            )]))
        }
    }

    /// Run `execute_step` on [`Checkpointer`] as attempt `attempt`, with `details` as the
    /// previous attempt's heartbeat details. Returns what the plugin saw and the
    /// heartbeat details sent to Temporal.
    async fn run_checkpointer(
        attempt: u32,
        details: Option<serde_json::Value>,
    ) -> (String, Vec<serde_json::Value>) {
        use temporalio_sdk::testing::{ActivityEnvironment, TestActivityInfoOptions};

        let sent = Arc::new(std::sync::Mutex::new(Vec::new()));
        let sink = sent.clone();
        let mut env = ActivityEnvironment::builder_with_default();
        if let Some(details) = details {
            env = env.heartbeat_details(details).expect("encode details");
        }
        let env = env
            .info(
                TestActivityInfoOptions::builder()
                    .attempt(attempt)
                    .deadline(None)
                    .build(),
            )
            .on_heartbeat(move |d| {
                let d = d
                    .downcast::<serde_json::Value>()
                    .expect("heartbeat details are JSON");
                sink.lock().expect("lock").push(*d);
            })
            .register_activities(acts(vec![Arc::new(Checkpointer)]))
            .build();
        let out = env
            .run(
                StepActivities::execute_step,
                step_input("checkpointer", PluginInput::Empty),
            )
            .await
            .expect("step runs");
        let seen = match out.output {
            PluginOutput::Documents(d) => d[0].content.clone(),
            other => panic!("{other:?}"),
        };
        let sent = sent.lock().expect("lock").clone();
        (seen, sent)
    }

    #[tokio::test]
    async fn a_retry_reads_the_progress_the_previous_attempt_heartbeated() {
        // Attempt 1 has nothing to resume and checkpoints in every heartbeat.
        let (seen, sent) = run_checkpointer(1, None).await;
        assert_eq!(seen, "null");
        let last = sent.last().expect("heartbeats were sent").clone();
        assert_eq!(last["message"], "after the checkpoint");

        // Attempt 2 is handed exactly what attempt 1 sent last.
        let (seen, sent) = run_checkpointer(2, Some(last)).await;
        assert_eq!(seen, r#"{"done":1}"#);
        assert_eq!(
            PluginContext::progress_from_heartbeat_details(sent.last().expect("heartbeats")),
            Some(serde_json::json!({ "done": 2 }))
        );
    }

    #[tokio::test]
    async fn plain_heartbeat_details_carry_no_progress() {
        // What a worker without checkpoints (or a plugin recording none) leaves behind.
        let (seen, _) = run_checkpointer(2, Some(serde_json::json!("keepalive"))).await;
        assert_eq!(seen, "null");
    }

    #[test]
    fn error_mapping() {
        assert!(matches!(
            to_activity_error(PluginError::Cancelled),
            ActivityError::Cancelled { .. }
        ));
        match to_activity_error(PluginError::Retryable("r".into())) {
            ActivityError::Application(f) => assert!(!f.is_non_retryable()),
            other => panic!("{other:?}"),
        }
        match to_activity_error(PluginError::NonRetryable("n".into())) {
            ActivityError::Application(f) => assert!(f.is_non_retryable()),
            other => panic!("{other:?}"),
        }
        match to_activity_error(PluginError::InvalidConfig("c".into())) {
            ActivityError::Application(f) => assert!(f.is_non_retryable()),
            other => panic!("{other:?}"),
        }
        let _ = Boom(PluginError::Cancelled); // keep the helper type used
    }
}
