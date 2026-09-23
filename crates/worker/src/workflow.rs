//! The `PipelineWorkflow`: one Temporal workflow per ingest job.
//!
//! The workflow walks the pipeline DAG in topological order, running every step that
//! is ready as an `execute_step` activity (fanned out into N parallel activities when
//! `fan_out` is set), routing each activity to the task queue of its plugin
//! (SPEC §8.3) and injecting the tenant [`MeiliContext`] into the indexer's config
//! (SPEC §7.4).
//!
//! Workflow code must stay deterministic: no I/O, no clocks, no tokio primitives —
//! only `ctx.execute_activity` and `temporalio_sdk::workflows::join_all`.

use std::collections::HashMap;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use chrono::{DateTime, Utc};

use meili_ingest_plugin_sdk::{
    INDEXER_PLUGIN, IndexReport, JobStatus, PipelineWorkflowInput, PipelineWorkflowOutput,
    PluginInput, PluginOutput, StepActivityInput, StepDefinition, StepResult, UsageUnits,
    WorkflowProgress, inject_meili_context,
};
use meili_ingest_router::plugin_task_queue;
use meili_ingest_usage::JobUsageInput;
use temporalio_common::RetryPolicy;
use temporalio_common::protos::temporal::api::failure::v1::Failure;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::workflows::join_all;
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, ApplicationFailure, SyncWorkflowContext,
    WorkflowCancellationToken, WorkflowContext, WorkflowContextView, WorkflowResult,
    WorkflowTermination,
};

use crate::activity::{FanOutActivityInput, JobStartedInput, StepActivities};
use crate::dag::{FanOut, fan_out_branches, ready_steps, resolve_input, retry_params};

/// Upper bound on parallel activities scheduled by one fan-out step (keeps the
/// workflow history well under Temporal's event limits).
pub const MAX_FAN_OUT_BRANCHES: usize = 500;

/// Attempts allowed for the usage-reporting activity before the job gives up on it.
/// Generous, because these rows are billed from; the job still succeeds either way.
const USAGE_MAX_ATTEMPTS: u32 = 10;

/// Heartbeat timeout for every step activity. The activity keeps itself alive every
/// 20 s independently of the plugin.
const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(90);

/// One ingest job.
#[workflow]
#[derive(Default)]
pub struct PipelineWorkflow {
    progress: WorkflowProgress,
}

#[workflow_methods]
impl PipelineWorkflow {
    /// Run the pipeline to completion.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: PipelineWorkflowInput,
    ) -> WorkflowResult<PipelineWorkflowOutput> {
        let order = match input.pipeline.validate() {
            Ok(o) => o,
            Err(e) => {
                let msg = format!("invalid pipeline {}: {e}", input.pipeline.uid);
                ctx.state_mut(|s| {
                    s.progress.status = JobStatus::Failed;
                    s.progress.error = Some(msg.clone());
                });
                return Err(ApplicationFailure::non_retryable(anyhow::anyhow!(msg)).into());
            }
        };
        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Running;
            s.progress.total_steps = order.len();
        });
        // Workflow time, not the wall clock: workflow code must stay deterministic on
        // replay, and this is the value Temporal records in history.
        let started_at = ctx.workflow_time();

        // Tell the control plane the job is running, so the job list does not show
        // "queued" for work that is already under way. Best effort: a failure here
        // must not fail the ingest.
        let started_input = JobStartedInput {
            job_id: input.job_id,
            current_step: order.first().cloned(),
        };
        if let Err(e) = ctx
            .execute_activity(
                StepActivities::record_job_started,
                started_input,
                ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
                    .task_queue("workers-general".to_string())
                    .retry_policy(RetryPolicy::builder().maximum_attempts(3).build())
                    .build(),
            )
            .await
        {
            tracing::warn!(job_id = %input.job_id, error = %e, "could not mark the job running");
        }

        let mut done: HashMap<String, PluginOutput> = HashMap::new();
        let mut index_report: Option<IndexReport> = None;

        loop {
            if ctx.state(|s| s.progress.cancel_requested) {
                return Self::finish_cancelled(ctx, &input, started_at).await;
            }
            let ready: Vec<StepDefinition> = ready_steps(&input.pipeline, &order, &done)
                .into_iter()
                .cloned()
                .collect();
            if ready.is_empty() {
                break;
            }
            ctx.state_mut(|s| {
                s.progress.current_step = ready.first().map(|st| st.id.clone());
            });

            // Run all ready (mutually independent) steps in parallel.
            let mut step_futs = Vec::with_capacity(ready.len());
            for step in &ready {
                step_futs.push(Self::run_step(ctx, &input, step, &done));
            }
            let results = join_all(step_futs).await;

            for (step, result) in ready.iter().zip(results) {
                match result {
                    Ok(outcome) => {
                        if let PluginOutput::Indexed(r) = &outcome.output
                            && step.plugin == INDEXER_PLUGIN
                        {
                            index_report = Some(r.clone());
                        }
                        ctx.state_mut(|s| {
                            s.progress.completed_steps += 1;
                            s.progress.steps.push(StepResult {
                                step_id: step.id.clone(),
                                plugin: step.plugin.clone(),
                                status: JobStatus::Succeeded,
                                document_count: outcome.output.document_count(),
                                branches: outcome.branches,
                                error: None,
                                duration_ms: outcome.duration_ms,
                                input_bytes: outcome.input_bytes,
                                usage: outcome.usage,
                            });
                        });
                        done.insert(step.id.clone(), outcome.output);
                    }
                    Err(StepFailure::Cancelled) => {
                        return Self::finish_cancelled(ctx, &input, started_at).await;
                    }
                    Err(StepFailure::Failed(msg)) => {
                        let full = format!("step {} ({}) failed: {msg}", step.id, step.plugin);
                        ctx.state_mut(|s| {
                            s.progress.status = JobStatus::Failed;
                            s.progress.error = Some(full.clone());
                            s.progress.steps.push(StepResult {
                                step_id: step.id.clone(),
                                plugin: step.plugin.clone(),
                                status: JobStatus::Failed,
                                document_count: 0,
                                branches: 1,
                                error: Some(msg.clone()),
                                duration_ms: 0,
                                input_bytes: 0,
                                usage: UsageUnits::none(),
                            });
                        });
                        // A failed job still consumed tokens and CPU, so meter it
                        // before failing. Cancellation does the same.
                        Self::emit_usage(
                            ctx,
                            &input,
                            JobStatus::Failed,
                            started_at,
                            Some(full.clone()),
                        )
                        .await;
                        return Err(ApplicationFailure::non_retryable(anyhow::anyhow!(full)).into());
                    }
                }
            }
        }

        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Succeeded;
            s.progress.current_step = None;
        });
        Self::emit_usage(ctx, &input, JobStatus::Succeeded, started_at, None).await;
        let steps = ctx.state(|s| s.progress.steps.clone());
        Ok(PipelineWorkflowOutput {
            job_id: input.job_id,
            status: JobStatus::Succeeded,
            steps,
            index_report,
            error: None,
        })
    }

    /// Request cancellation: no further steps are scheduled.
    #[signal]
    pub fn cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: ()) {
        self.progress.cancel_requested = true;
    }

    /// Full progress snapshot.
    #[query]
    pub fn progress(&self, _ctx: &WorkflowContextView) -> WorkflowProgress {
        self.progress.clone()
    }

    /// Currently executing step id (empty when none).
    #[query]
    pub fn current_step(&self, _ctx: &WorkflowContextView) -> String {
        self.progress.current_step.clone().unwrap_or_default()
    }
}

/// Human-readable reason an activity failed.
///
/// Temporal's own `Display` is always "Activity task failed"; the message a plugin
/// produced sits in the failure's cause chain. Walk it and return the deepest
/// non-empty message so `GET /jobs/{id}` shows something actionable.
pub(crate) fn describe_activity_failure(err: &ActivityExecutionError) -> String {
    err.failure()
        .and_then(deepest_failure_message)
        .unwrap_or_else(|| err.to_string())
}

/// Deepest non-generic message in a failure's cause chain.
pub(crate) fn deepest_failure_message(failure: &Failure) -> Option<String> {
    {
        fn deepest(failure: &Failure) -> Option<String> {
            let mut best = non_empty(&failure.message);
            let mut current = failure.cause.as_deref();
            while let Some(f) = current {
                if let Some(m) = non_empty(&f.message) {
                    best = Some(m);
                }
                current = f.cause.as_deref();
            }
            best
        }
        fn non_empty(s: &str) -> Option<String> {
            let s = s.trim();
            (!s.is_empty() && s != "Activity task failed").then(|| s.to_string())
        }
        deepest(failure)
    }
}

#[cfg(test)]
fn describe_failure_for_test(failure: &Failure) -> String {
    deepest_failure_message(failure).unwrap_or_default()
}

/// Workflow time as a UTC timestamp for the usage event.
///
/// Uses `WorkflowContext::workflow_time`, which replays identically, rather than the
/// wall clock, which would make the workflow non-deterministic.
pub(crate) fn to_utc(time: Option<SystemTime>) -> DateTime<Utc> {
    time.map(DateTime::<Utc>::from)
        .unwrap_or_else(|| DateTime::<Utc>::from(UNIX_EPOCH))
}

/// MIME of the job's original input, when it is known.
fn initial_mime(input: &PluginInput) -> String {
    match input {
        PluginInput::Bytes(b) => b.mime.clone(),
        PluginInput::Ref(r) => r.mime().unwrap_or("").to_string(),
        PluginInput::Documents(_) => "application/json".to_string(),
        _ => String::new(),
    }
}

/// What a completed step produced, plus the metering the usage event needs.
struct StepOutcome {
    output: PluginOutput,
    /// Parallel branches the step ran as (1 unless fanned out).
    branches: usize,
    /// Wall time, summed across branches.
    duration_ms: u64,
    /// Bytes consumed, summed across branches.
    input_bytes: u64,
    /// Billable units the plugin reported, summed across branches.
    usage: UsageUnits,
}

/// Why a step did not complete.
enum StepFailure {
    Cancelled,
    Failed(String),
}

impl PipelineWorkflow {
    async fn finish_cancelled(
        ctx: &mut WorkflowContext<Self>,
        input: &PipelineWorkflowInput,
        started_at: Option<SystemTime>,
    ) -> WorkflowResult<PipelineWorkflowOutput> {
        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Cancelled;
            s.progress.current_step = None;
        });
        Self::emit_usage(ctx, input, JobStatus::Cancelled, started_at, None).await;
        Err(WorkflowTermination::cancelled())
    }

    /// Ship this job's usage to the analytics store.
    ///
    /// Delivery is at-least-once: the activity is retried by Temporal until it
    /// succeeds, which is what makes the data safe to bill from. Duplicates from a
    /// retry are collapsed downstream because every row carries a deterministic id.
    /// The activity is best-effort in one direction only — if it exhausts its
    /// retries the job's own outcome is not changed, since losing a usage row must
    /// never turn a successful ingest into a failure.
    async fn emit_usage(
        ctx: &WorkflowContext<Self>,
        input: &PipelineWorkflowInput,
        status: JobStatus,
        started_at: Option<SystemTime>,
        error: Option<String>,
    ) {
        let steps = ctx.state(|s| s.progress.steps.clone());
        let usage_input = JobUsageInput {
            job_id: input.job_id,
            workflow_id: PipelineWorkflowInput::workflow_id(input.job_id),
            pipeline_uid: input.pipeline.uid.clone(),
            pipeline_builtin: input.pipeline.builtin,
            project_id: input.context.project_id.clone().unwrap_or_default(),
            region: input.context.region.clone().unwrap_or_default(),
            index_name: input.context.index.clone().unwrap_or_default(),
            task_queue: "workers-general".to_string(),
            status,
            started_at: to_utc(started_at),
            finished_at: to_utc(ctx.workflow_time()),
            input_bytes: input.input.approx_size() as u64,
            input_mime: initial_mime(&input.input),
            steps,
            error,
        };
        let opts = ActivityOptions::with_start_to_close_timeout(Duration::from_secs(60))
            .task_queue("workers-general".to_string())
            // Detached from the workflow's cancellation token. Activities inherit
            // workflow cancellation, so on a cancelled job this one would be killed
            // before it ran — and a cancelled job has still consumed tokens and CPU
            // that must be billed.
            .cancellation_token(WorkflowCancellationToken::new())
            .retry_policy(
                RetryPolicy::builder()
                    .maximum_attempts(USAGE_MAX_ATTEMPTS)
                    .initial_interval(Duration::from_secs(2))
                    .backoff_coefficient(2.0)
                    .build(),
            )
            .build();
        if let Err(e) = ctx
            .execute_activity(StepActivities::record_usage, usage_input, opts)
            .await
        {
            // Swallow: the job's result must not depend on the analytics store.
            tracing::error!(job_id = %input.job_id, error = %e, "usage event was not recorded");
        }
    }

    fn activity_options(step: &StepDefinition) -> ActivityOptions {
        let retry = retry_params(&step.effective_retry());
        let policy = RetryPolicy::builder()
            .maximum_attempts(retry.maximum_attempts)
            .backoff_coefficient(retry.backoff_coefficient)
            .initial_interval(Duration::from_secs(retry.initial_interval_secs))
            .build();
        ActivityOptions::with_start_to_close_timeout(Duration::from_secs(
            step.effective_timeout_secs(),
        ))
        .task_queue(plugin_task_queue(&step.plugin).to_string())
        .heartbeat_timeout(HEARTBEAT_TIMEOUT)
        .retry_policy(policy)
        .build()
    }

    fn step_config(step: &StepDefinition, input: &PipelineWorkflowInput) -> serde_json::Value {
        let mut config = step.config.clone();
        if step.plugin == INDEXER_PLUGIN {
            inject_meili_context(&mut config, &input.context);
        }
        config
    }

    fn map_activity_error(e: ActivityExecutionError) -> StepFailure {
        match e {
            ActivityExecutionError::Cancelled(_) => StepFailure::Cancelled,
            other => StepFailure::Failed(describe_activity_failure(&other)),
        }
    }

    /// Run one step (all its branches). Returns the merged output and branch count.
    async fn run_step(
        ctx: &WorkflowContext<Self>,
        input: &PipelineWorkflowInput,
        step: &StepDefinition,
        done: &HashMap<String, PluginOutput>,
    ) -> Result<StepOutcome, StepFailure> {
        let opts = Self::activity_options(step);
        let config = Self::step_config(step, input);
        let mk = |branch: Option<usize>, total: Option<usize>, branch_input: PluginInput| {
            StepActivityInput {
                job_id: input.job_id,
                step_id: step.id.clone(),
                plugin: step.plugin.clone(),
                config: config.clone(),
                input: branch_input,
                branch,
                branch_total: total,
                project_id: input.context.project_id.clone(),
            }
        };

        let Some(path) = step.fan_out.as_deref() else {
            let step_input = resolve_input(step, &input.input, done);
            let out = ctx
                .execute_activity(
                    StepActivities::execute_step,
                    mk(None, None, step_input),
                    opts,
                )
                .await
                .map_err(Self::map_activity_error)?;
            return Ok(StepOutcome {
                branches: 1,
                duration_ms: out.duration_ms,
                input_bytes: out.input_bytes,
                usage: out.usage,
                output: out.output,
            });
        };

        // Fan-out: exactly one dependency (validated).
        let upstream = step
            .depends_on
            .first()
            .and_then(|d| done.get(d))
            .cloned()
            .unwrap_or(PluginOutput::Empty);
        let branches = match fan_out_branches(path, upstream, MAX_FAN_OUT_BRANCHES) {
            FanOut::Branches(b) => b,
            FanOut::NeedsActivity(spilled) => {
                let expanded = ctx
                    .execute_activity(
                        StepActivities::expand_fan_out,
                        FanOutActivityInput {
                            job_id: input.job_id,
                            step_id: step.id.clone(),
                            upstream: spilled,
                            path: path.to_string(),
                            max_branches: MAX_FAN_OUT_BRANCHES,
                        },
                        ActivityOptions::with_start_to_close_timeout(Duration::from_secs(300))
                            .task_queue("workers-general".to_string())
                            .retry_policy(RetryPolicy::builder().maximum_attempts(3).build())
                            .build(),
                    )
                    .await
                    .map_err(Self::map_activity_error)?;
                expanded.branches
            }
        };
        if branches.is_empty() {
            return Ok(StepOutcome {
                output: PluginOutput::Many(vec![]),
                branches: 0,
                duration_ms: 0,
                input_bytes: 0,
                usage: UsageUnits::none(),
            });
        }
        let total = branches.len();
        let futs: Vec<_> = branches
            .into_iter()
            .enumerate()
            .map(|(i, b)| {
                ctx.execute_activity(
                    StepActivities::execute_step,
                    mk(Some(i), Some(total), b),
                    opts.clone(),
                )
            })
            .collect();
        let results = join_all(futs).await;
        let mut outputs = Vec::with_capacity(total);
        let mut cancelled = false;
        let mut first_error: Option<String> = None;
        // Metering is summed across branches: one fanned-out step is one billable
        // line, however many activities carried it out.
        let mut duration_ms = 0u64;
        let mut input_bytes = 0u64;
        let mut usage = UsageUnits::none();
        for r in results {
            match r {
                Ok(o) => {
                    duration_ms += o.duration_ms;
                    input_bytes += o.input_bytes;
                    usage.merge(o.usage);
                    outputs.push(o.output);
                }
                Err(ActivityExecutionError::Cancelled(_)) => cancelled = true,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(describe_activity_failure(&e));
                    }
                }
            }
        }
        if let Some(msg) = first_error {
            return Err(StepFailure::Failed(format!(
                "{} of {total} branches failed; first error: {msg}",
                total - outputs.len()
            )));
        }
        if cancelled {
            return Err(StepFailure::Cancelled);
        }
        Ok(StepOutcome {
            output: PluginOutput::Many(outputs),
            branches: total,
            duration_ms,
            input_bytes,
            usage,
        })
    }
}

#[cfg(test)]
mod tests {
    use temporalio_common::protos::temporal::api::failure::v1::Failure;

    fn failure(message: &str, cause: Option<Failure>) -> Failure {
        Failure {
            message: message.to_string(),
            cause: cause.map(Box::new),
            ..Default::default()
        }
    }

    #[test]
    fn deepest_cause_message_is_reported() {
        // Temporal wraps the plugin's message two levels down.
        let inner = failure(
            "plugin \"chunker\" received bytes that are not valid UTF-8 text",
            None,
        );
        let middle = failure("activity error", Some(inner));
        let top = failure("Activity task failed", Some(middle));
        assert_eq!(
            super::describe_failure_for_test(&top),
            "plugin \"chunker\" received bytes that are not valid UTF-8 text"
        );
    }

    #[test]
    fn generic_wrapper_alone_is_not_reported_as_the_reason() {
        let top = failure("Activity task failed", None);
        assert_eq!(super::describe_failure_for_test(&top), "");
    }

    #[test]
    fn single_message_is_kept() {
        let top = failure("meilisearch rejected the batch", None);
        assert_eq!(
            super::describe_failure_for_test(&top),
            "meilisearch rejected the batch"
        );
    }
}
