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

use crate::connection::{ConnectionSettings, ControlPlane, resolve_connection};
use crate::registry::PluginRegistry;

/// Interval at which the activity heartbeats on its own, independent of the plugin.
const KEEPALIVE_HEARTBEAT: Duration = Duration::from_secs(20);

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
    /// One input per branch (large branches are spilled to the blob store).
    pub branches: Vec<PluginInput>,
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
    /// Key and host policy for resolving an indexer step's Meilisearch connection.
    pub connections: ConnectionSettings,
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
            connections: ConnectionSettings::default(),
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

    /// Patch the cached job row. Returns `Ok(false)` when no control plane is
    /// configured, and an error only when the request itself failed.
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
        let resp = self
            .http
            .patch(&url)
            .json(&serde_json::Value::Object(body))
            .send()
            .await
            .map_err(|e| PluginError::Retryable(format!("control plane unreachable: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            // The gateway failed to insert the row (best effort at submit time).
            // Nothing to update and retrying will not create it.
            tracing::warn!(job_id = %job_id, "no cached job row to update");
            return Ok(false);
        }
        if !resp.status().is_success() {
            return Err(PluginError::Retryable(format!(
                "control plane rejected the job status update: {}",
                resp.status()
            )));
        }
        Ok(true)
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
                // A forbidden URL stays forbidden: retrying would only re-run the check.
                BlobError::Blocked(_) => {
                    PluginError::NonRetryable(format!("refusing to fetch the step input: {e}"))
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
            &ControlPlane {
                http: &self.http,
                base_url: self.control_plane_url.as_deref(),
            },
            &self.connections,
            &input.plugin,
            input.config,
            input.project_id.as_deref(),
        )
        .await?;
        let output = plugin.execute(plugin_ctx, resolved, config).await?;
        let doc_count = output.document_count();
        let output = self
            .blob
            .spill_output(
                input.job_id,
                &input.step_id,
                input.branch,
                output,
                self.spill_threshold,
            )
            .await
            .map_err(|e| PluginError::Retryable(format!("failed to spill step output: {e}")))?;
        let duration_ms = started.elapsed().as_millis() as u64;
        // Whatever the plugin spent on external services during `execute`.
        let usage = plugin_ctx.usage();
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
                return Err(PluginError::NonRetryable(
                    "upstream output is still a reference after hydration".into(),
                ));
            }
        };
        let mut out = Vec::with_capacity(branches.len());
        for (i, branch) in branches.into_iter().enumerate() {
            let as_output = match branch {
                PluginInput::Documents(d) => PluginOutput::Documents(d),
                PluginInput::Bytes(b) => PluginOutput::Bytes(b),
                PluginInput::Many(m) => PluginOutput::Many(m),
                PluginInput::Ref(r) => PluginOutput::Ref(r),
                PluginInput::Empty => PluginOutput::Empty,
            };
            let spilled = self
                .blob
                .spill_output(
                    input.job_id,
                    &format!("{}-fanout", input.step_id),
                    Some(i),
                    as_output,
                    self.spill_threshold,
                )
                .await
                .map_err(|e| {
                    PluginError::Retryable(format!("failed to spill fan-out branch: {e}"))
                })?;
            out.push(PluginInput::from(spilled));
        }
        Ok(FanOutActivityOutput { branches: out })
    }
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

/// Wire the SDK [`ActivityContext`] to a plugin-facing context: heartbeats sent by the
/// plugin are forwarded to Temporal, a keepalive heartbeat is sent periodically, and
/// Temporal cancellation flips the shared flag the plugin polls.
fn bridge_context(
    ctx: &ActivityContext,
    job_id: Uuid,
    step_id: &str,
) -> (PluginContext, tokio::task::JoinHandle<()>) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let cancelled = Arc::new(AtomicBool::new(false));
    let plugin_ctx = PluginContext::new(job_id, step_id, ctx.info().attempt, tx, cancelled.clone());
    let sdk_ctx = ctx.clone();
    let handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(KEEPALIVE_HEARTBEAT);
        tick.tick().await; // first tick fires immediately; skip it
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(details) => {
                        if let Err(e) = sdk_ctx.record_heartbeat(details).await {
                            tracing::debug!(error = %e, "heartbeat failed");
                        }
                    }
                    None => break,
                },
                _ = tick.tick() => {
                    if let Err(e) = sdk_ctx.record_heartbeat("keepalive".to_string()).await {
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
        let result = self.run_step(&plugin_ctx, input).await;
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

    /// Ship one job's usage rows to the analytics store.
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
        let job_id = input.job_id;

        // Write the outcome back to the cached job row first. Without this the job
        // list shows "queued" for finished work until someone opens that job, because
        // the gateway only refreshes a row when it serves that job's detail.
        self.patch_job(job_id, input.status, None, input.error.clone())
            .await
            .map_err(to_activity_error)?;

        let Some(client) = &self.usage else {
            // Usage reporting is off; the status write above still happened.
            return Ok(());
        };
        let events = events_for_job(&input);
        match client.send(&events).await {
            Ok(()) => {
                tracing::info!(job_id = %job_id, events = events.len(), "usage recorded");
                Ok(())
            }
            Err(e) if e.is_retryable() => Err(ActivityError::application(ApplicationFailure::new(
                anyhow::anyhow!("usage store unavailable: {e}"),
            ))),
            Err(e) => Err(ActivityError::application(
                ApplicationFailure::non_retryable(anyhow::anyhow!("usage rejected: {e}")),
            )),
        }
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
            project_id: None,
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
        input.project_id = Some("tenant-1".into());

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
