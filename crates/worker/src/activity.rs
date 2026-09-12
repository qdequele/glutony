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

use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{
    ActivityContext as PluginContext, PluginError, PluginInput, PluginOutput, StepActivityInput,
    StepActivityOutput,
};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::ApplicationFailure;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use tokio::sync::mpsc;
use uuid::Uuid;

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
}

impl StepActivities {
    /// Build the activity set.
    pub fn new(registry: Arc<PluginRegistry>, blob: BlobStore, spill_threshold: usize) -> Self {
        Self {
            registry,
            blob,
            http: reqwest::Client::new(),
            spill_threshold,
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
            .map_err(|e| PluginError::Retryable(format!("failed to resolve step input: {e}")))?;
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
        tracing::info!(
            job_id = %input.job_id,
            step = %input.step_id,
            plugin = %input.plugin,
            branch = ?input.branch,
            input_kind = ?resolved.kind(),
            "executing step"
        );
        let output = plugin.execute(plugin_ctx, resolved, input.config).await?;
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
        tracing::info!(
            job_id = %input.job_id,
            step = %input.step_id,
            plugin = %input.plugin,
            documents = doc_count,
            duration_ms,
            "step finished"
        );
        Ok(StepActivityOutput {
            step_id: input.step_id,
            output,
            duration_ms,
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
