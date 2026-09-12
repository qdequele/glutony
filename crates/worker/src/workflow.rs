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
use std::time::Duration;

use meili_ingest_plugin_sdk::{
    INDEXER_PLUGIN, IndexReport, JobStatus, PipelineWorkflowInput, PipelineWorkflowOutput,
    PluginInput, PluginOutput, StepActivityInput, StepDefinition, StepResult, WorkflowProgress,
    inject_meili_context,
};
use meili_ingest_router::plugin_task_queue;
use temporalio_common::RetryPolicy;
use temporalio_common::protos::temporal::api::failure::v1::Failure;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::workflows::join_all;
use temporalio_sdk::{
    ActivityExecutionError, ActivityOptions, ApplicationFailure, SyncWorkflowContext,
    WorkflowContext, WorkflowContextView, WorkflowResult, WorkflowTermination,
};

use crate::activity::{FanOutActivityInput, StepActivities};
use crate::dag::{FanOut, fan_out_branches, ready_steps, resolve_input, retry_params};

/// Upper bound on parallel activities scheduled by one fan-out step (keeps the
/// workflow history well under Temporal's event limits).
pub const MAX_FAN_OUT_BRANCHES: usize = 500;

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

        let mut done: HashMap<String, PluginOutput> = HashMap::new();
        let mut index_report: Option<IndexReport> = None;

        loop {
            if ctx.state(|s| s.progress.cancel_requested) {
                return Self::finish_cancelled(ctx, &input, done);
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
                    Ok((output, branches)) => {
                        if let PluginOutput::Indexed(r) = &output
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
                                document_count: output.document_count(),
                                branches,
                                error: None,
                            });
                        });
                        done.insert(step.id.clone(), output);
                    }
                    Err(StepFailure::Cancelled) => {
                        return Self::finish_cancelled(ctx, &input, done);
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
                            });
                        });
                        return Err(ApplicationFailure::non_retryable(anyhow::anyhow!(full)).into());
                    }
                }
            }
        }

        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Succeeded;
            s.progress.current_step = None;
        });
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
fn describe_activity_failure(err: &ActivityExecutionError) -> String {
    err.failure()
        .and_then(deepest_failure_message)
        .unwrap_or_else(|| err.to_string())
}

/// Deepest non-generic message in a failure's cause chain.
fn deepest_failure_message(failure: &Failure) -> Option<String> {
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

/// Why a step did not complete.
enum StepFailure {
    Cancelled,
    Failed(String),
}

impl PipelineWorkflow {
    fn finish_cancelled(
        ctx: &mut WorkflowContext<Self>,
        _input: &PipelineWorkflowInput,
        _done: HashMap<String, PluginOutput>,
    ) -> WorkflowResult<PipelineWorkflowOutput> {
        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Cancelled;
            s.progress.current_step = None;
        });
        Err(WorkflowTermination::cancelled())
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
    ) -> Result<(PluginOutput, usize), StepFailure> {
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
            return Ok((out.output, 1));
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
            return Ok((PluginOutput::Many(vec![]), 0));
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
        for r in results {
            match r {
                Ok(o) => outputs.push(o.output),
                Err(ActivityExecutionError::Cancelled(_)) => cancelled = true,
                Err(e) => {
                    if first_error.is_none() {
                        first_error = Some(e.to_string());
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
        Ok((PluginOutput::Many(outputs), total))
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
