//! `SourceRunWorkflow`: one scheduled run of a source (spec *`SourceRunWorkflow`*).
//!
//! A Temporal schedule starts this workflow on every tick. It resolves the source
//! (fetch + stage, in an activity), starts one child `PipelineWorkflow` per fetched
//! item, and records the run. The incremental state is saved only when every child
//! succeeded, so a failed run is retried on the next tick rather than being mistaken
//! for "unchanged".
//!
//! Deterministic like `PipelineWorkflow`: the run id comes from `ctx.uuid4()`, times
//! from workflow time and the schedule's search attribute, all I/O from activities.

use std::time::{Duration, SystemTime};

use chrono::{DateTime, Utc};
use meili_ingest_plugin_sdk::{JobStatus, PipelineWorkflowInput};
use meili_ingest_source::{RunOutcome, SourceRunInput};
use serde::{Deserialize, Serialize};
use temporalio_common::RetryPolicy;
use temporalio_common::search_attributes::{SearchAttributeKey, Timestamp};
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::workflows::join_all;
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, ChildWorkflowOptions, WorkflowContext, WorkflowResult,
};
use uuid::Uuid;

use crate::source_activity::{
    RecordRunInput, ResolveSourceInput, ResolveSourceOutput, SourceActivities, SourceJob,
};
use crate::workflow::{
    PipelineWorkflow, deepest_failure_message, describe_activity_failure, to_utc,
};

/// Temporal sets this on every workflow a schedule starts: the tick's nominal time,
/// which URL templates render against (Decision 9). A manual trigger sets it too.
const SCHEDULED_START_TIME: SearchAttributeKey<Timestamp> =
    SearchAttributeKey::datetime("TemporalScheduledStartTime");

/// Result of one source run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRunOutput {
    /// Run id, as recorded in `source_runs`.
    pub run_id: Uuid,
    /// What the run did.
    pub outcome: RunOutcome,
    /// Jobs it started.
    pub job_ids: Vec<Uuid>,
}

/// One scheduled run of a source.
#[workflow]
#[derive(Default)]
pub struct SourceRunWorkflow;

#[workflow_methods]
impl SourceRunWorkflow {
    /// Resolve, ingest, record.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: SourceRunInput,
    ) -> WorkflowResult<SourceRunOutput> {
        // Deterministic on replay: the SDK seeds uuid4 from history.
        let run_id = Uuid::parse_str(&ctx.uuid4()).unwrap_or_else(|_| Uuid::nil());
        let started_at = to_utc(ctx.workflow_time());
        let scheduled_at = ctx
            .search_attributes()
            .get(&SCHEDULED_START_TIME)
            .and_then(|t| SystemTime::try_from(t).ok())
            .map(DateTime::<Utc>::from)
            .unwrap_or(started_at);

        let mut record = RecordRunInput {
            run_id,
            source_id: input.source_id,
            started_at,
            finished_at: started_at,
            outcome: RunOutcome::Failed,
            items: 0,
            job_ids: vec![],
            error: None,
            state: None,
        };

        let resolved = ctx
            .execute_activity(
                SourceActivities::resolve_source,
                ResolveSourceInput {
                    source_id: input.source_id,
                    run_id,
                    scheduled_at,
                },
                resolve_options(),
            )
            .await;
        let (jobs, state) = match resolved {
            Ok(ResolveSourceOutput::Unchanged) => {
                record.outcome = RunOutcome::Unchanged;
                return Self::finish(ctx, record).await;
            }
            Ok(ResolveSourceOutput::Ready { jobs, state }) => (jobs, state),
            Err(e) => {
                record.error = Some(describe_activity_failure(&e));
                return Self::finish(ctx, record).await;
            }
        };

        record.job_ids = jobs.iter().map(|j| j.workflow_input.job_id).collect();
        let results = join_all(jobs.into_iter().map(|job| Self::run_job(ctx, job))).await;
        let failures: Vec<String> = results.into_iter().filter_map(Result::err).collect();
        record.items = i32::try_from(record.job_ids.len() - failures.len()).unwrap_or(i32::MAX);
        if failures.is_empty() {
            record.outcome = RunOutcome::Ingested;
            record.state = Some(state);
        } else {
            record.error = Some(summarize_failures(&failures, record.job_ids.len()));
        }
        Self::finish(ctx, record).await
    }
}

impl SourceRunWorkflow {
    /// Run one item's pipeline as a child workflow, under the same workflow id a
    /// request-driven job gets so `GET /jobs/{id}` works for it unchanged.
    async fn run_job(ctx: &WorkflowContext<Self>, job: SourceJob) -> Result<(), String> {
        let job_id = job.workflow_input.job_id;
        let opts = ChildWorkflowOptions::workflow_id(PipelineWorkflowInput::workflow_id(job_id));
        let started = ctx
            .start_child_workflow(PipelineWorkflow::run, job.workflow_input, opts)
            .await
            .map_err(|e| format!("job {job_id} did not start: {e}"))?;
        let output = started.result().await.map_err(|e| {
            let why = e
                .failure()
                .and_then(deepest_failure_message)
                .unwrap_or_else(|| e.to_string());
            format!("job {job_id} failed: {why}")
        })?;
        match output.status {
            JobStatus::Succeeded => Ok(()),
            other => Err(format!("job {job_id} ended {}", other.as_str())),
        }
    }

    /// Record the run, then end the workflow: failed runs fail the workflow so the
    /// schedule's recent-actions view shows them, without being retried by Temporal —
    /// the next tick is the retry.
    async fn finish(
        ctx: &WorkflowContext<Self>,
        mut record: RecordRunInput,
    ) -> WorkflowResult<SourceRunOutput> {
        record.finished_at = to_utc(ctx.workflow_time());
        let output = SourceRunOutput {
            run_id: record.run_id,
            outcome: record.outcome,
            job_ids: record.job_ids.clone(),
        };
        let error = record.error.clone();
        if let Err(e) = ctx
            .execute_activity(
                SourceActivities::record_source_run,
                record,
                record_options(),
            )
            .await
        {
            // The jobs themselves are unaffected; the run row is what went missing.
            tracing::error!(run_id = %output.run_id, error = %e, "source run was not recorded");
        }
        match (output.outcome, error) {
            (RunOutcome::Failed, Some(msg)) => {
                Err(ApplicationFailure::non_retryable(anyhow::anyhow!(msg)).into())
            }
            _ => Ok(output),
        }
    }
}

/// Fetching can move hundreds of MiB, so the timeout is generous; transient network
/// failures get a few retries before the run is given up until the next tick.
fn resolve_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(15 * 60))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(3)
                .initial_interval(Duration::from_secs(10))
                .backoff_coefficient(3.0)
                .build(),
        )
        .build()
}

/// Recording is cheap and must land: the run's state rides on it.
fn record_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(10)
                .initial_interval(Duration::from_secs(2))
                .backoff_coefficient(2.0)
                .build(),
        )
        .build()
}

/// One line for the run row: the first failure, plus how many others there were.
fn summarize_failures(failures: &[String], total: usize) -> String {
    match failures {
        [] => String::new(),
        [only] if total == 1 => only.clone(),
        [first, rest @ ..] => format!(
            "{} of {total} jobs failed; first: {first}{}",
            failures.len(),
            if rest.is_empty() {
                ""
            } else {
                " (see the jobs for the others)"
            }
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_failure_is_reported_verbatim() {
        let f = vec!["job x failed: boom".to_string()];
        assert_eq!(summarize_failures(&f, 1), "job x failed: boom");
    }

    #[test]
    fn several_failures_are_counted() {
        let f = vec!["a".to_string(), "b".to_string()];
        assert_eq!(
            summarize_failures(&f, 3),
            "2 of 3 jobs failed; first: a (see the jobs for the others)"
        );
        assert_eq!(
            summarize_failures(&f[..1], 3),
            "1 of 3 jobs failed; first: a"
        );
    }
}
