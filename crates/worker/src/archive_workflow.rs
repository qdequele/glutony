//! `ArchiveWorkflow`: one archive upload (zip, tar, gzip), ingested as one job per file.
//!
//! The workflow expands the archive once (in an activity that stages every file and
//! writes a manifest), then walks the manifest in waves of [`WAVE`] files: an activity
//! routes the wave's files to their pipelines, and each file runs as a child
//! `PipelineWorkflow` under its own job id. A wave finishes before the next starts, so
//! at most [`WAVE`] files of one archive run at once.
//!
//! Every [`WAVES_PER_RUN`] waves (or sooner, when Temporal suggests it) the workflow
//! continues as new, carrying its position and counts, so an archive of thousands of
//! files never outgrows Temporal's history limits. Its workflow id stays
//! `ingest-<job_id>`, so `GET /jobs/{id}` and cancellation see one job throughout.
//!
//! The archive job succeeds when every file it handed to a pipeline succeeded and at
//! least one did. Skipped entries (unsafe paths, empty files, types no pipeline
//! handles…) are reported but do not fail it.

use std::time::Duration;

use meili_ingest_plugin_sdk::{
    ArchiveFileIssue, ArchiveProgress, ArchiveResume, ArchiveWorkflowInput, ArchiveWorkflowOutput,
    JobStatus, WorkflowProgress,
};
use temporalio_common::RetryPolicy;
use temporalio_macros::{workflow, workflow_methods};
use temporalio_sdk::workflows::join_all;
use temporalio_sdk::{
    ActivityOptions, ApplicationFailure, ChildWorkflowOptions, ContinueAsNewOptions,
    SyncWorkflowContext, WorkflowContext, WorkflowContextView, WorkflowResult, WorkflowTermination,
};
use uuid::Uuid;

use crate::activity::{JobStartedInput, StepActivities};
use crate::archive_activity::{
    ArchiveActivities, ArchiveChildJob, ArchiveFinishedInput, ExpandArchiveInput,
    PrepareArchiveInput,
};
use crate::workflow::{PipelineWorkflow, deepest_failure_message, describe_activity_failure};

/// Files of one archive started at once.
pub const WAVE: usize = 50;

/// Waves per workflow run before it continues as new.
pub const WAVES_PER_RUN: usize = 10;

/// Name of the expansion phase in `current_step`.
const UNPACK_STEP: &str = "unpack";

/// One archive job.
#[workflow]
#[derive(Default)]
pub struct ArchiveWorkflow {
    progress: WorkflowProgress,
}

/// How one file's job ended, when not in success.
struct FileFailure {
    path: String,
    job_id: Uuid,
    reason: String,
}

#[workflow_methods]
impl ArchiveWorkflow {
    /// Expand the archive, then run its files wave by wave.
    #[run]
    pub async fn run(
        ctx: &mut WorkflowContext<Self>,
        input: ArchiveWorkflowInput,
    ) -> WorkflowResult<ArchiveWorkflowOutput> {
        let (manifest, total, mut next) = match &input.resume {
            Some(r) => {
                let progress = r.progress.clone();
                ctx.state_mut(|s| s.progress = progress);
                (r.manifest.clone(), r.total, r.next)
            }
            None => match Self::expand(ctx, &input).await {
                Ok(started) => started,
                Err(message) => return Self::fail(ctx, input.job_id, message).await,
            },
        };

        let (wave, waves_per_run) = input.batching.map_or((WAVE, WAVES_PER_RUN), |b| {
            (b.wave.max(1), b.waves_per_run.max(1))
        });
        let mut waves = 0usize;
        while next < total {
            if ctx.state(|s| s.progress.cancel_requested) {
                return Self::finish_cancelled(ctx, input.job_id).await;
            }
            if waves >= waves_per_run || (waves > 0 && ctx.continue_as_new_suggested()) {
                let resume = ArchiveResume {
                    manifest,
                    total,
                    next,
                    progress: ctx.state(|s| s.progress.clone()),
                };
                let continued = ArchiveWorkflowInput {
                    resume: Some(resume),
                    ..input
                };
                return match ctx.continue_as_new(continued, ContinueAsNewOptions::default()) {
                    Err(t) => Err(t),
                    Ok(never) => match never {},
                };
            }

            let last = (next + wave).min(total);
            ctx.state_mut(|s| {
                s.progress.current_step = Some(format!("files {}-{last} of {total}", next + 1));
            });
            let prepared = ctx
                .execute_activity(
                    ArchiveActivities::prepare_archive_files,
                    PrepareArchiveInput {
                        job_id: input.job_id,
                        manifest: manifest.clone(),
                        offset: next,
                        limit: wave,
                        context: input.context.clone(),
                        forced_pipeline: input.forced_pipeline.clone(),
                        locked_index: input.locked_index.clone(),
                    },
                    prepare_options(),
                )
                .await;
            let prepared = match prepared {
                Ok(p) => p,
                Err(e) => {
                    let message = format!(
                        "routing files {}-{last} failed: {}",
                        next + 1,
                        describe_activity_failure(&e)
                    );
                    return Self::fail(ctx, input.job_id, message).await;
                }
            };
            if prepared.consumed == 0 {
                let message = format!("the archive manifest ended at file {next} of {total}");
                return Self::fail(ctx, input.job_id, message).await;
            }
            let started = prepared.jobs.len();
            ctx.state_mut(|s| {
                let a = s.progress.archive.get_or_insert_with(Default::default);
                prepared.skipped.iter().cloned().for_each(|i| a.skip(i));
                prepared.failed.iter().cloned().for_each(|i| a.fail(i));
                a.files_started += started;
            });

            let results = join_all(
                prepared
                    .jobs
                    .into_iter()
                    .map(|job| Self::run_file(ctx, job)),
            )
            .await;
            ctx.state_mut(|s| {
                let a = s.progress.archive.get_or_insert_with(Default::default);
                for result in results {
                    match result {
                        Ok(()) => a.files_succeeded += 1,
                        Err(f) => a.fail(ArchiveFileIssue {
                            path: f.path,
                            reason: f.reason,
                            job_id: Some(f.job_id),
                        }),
                    }
                }
            });
            next += prepared.consumed;
            waves += 1;
            ctx.state_mut(|s| s.progress.completed_steps = next);
        }

        if ctx.state(|s| s.progress.cancel_requested) {
            return Self::finish_cancelled(ctx, input.job_id).await;
        }
        let archive = ctx.state(|s| s.progress.archive.clone().unwrap_or_default());
        match outcome_error(&archive) {
            Some(message) => Self::fail(ctx, input.job_id, message).await,
            None => {
                ctx.state_mut(|s| {
                    s.progress.status = JobStatus::Succeeded;
                    s.progress.current_step = None;
                });
                Self::record_finished(ctx, input.job_id, JobStatus::Succeeded, None).await;
                Ok(ArchiveWorkflowOutput {
                    job_id: input.job_id,
                    status: JobStatus::Succeeded,
                    archive,
                    error: None,
                })
            }
        }
    }

    /// Request cancellation: no further wave is started.
    #[signal]
    pub fn cancel(&mut self, _ctx: &mut SyncWorkflowContext<Self>, _input: ()) {
        self.progress.cancel_requested = true;
    }

    /// Full progress snapshot, with per-file counts under `archive`.
    #[query]
    pub fn progress(&self, _ctx: &WorkflowContextView) -> WorkflowProgress {
        self.progress.clone()
    }
}

impl ArchiveWorkflow {
    /// First run: mark the job running and expand the archive. Returns the manifest,
    /// its file count and the first entry to start.
    async fn expand(
        ctx: &mut WorkflowContext<Self>,
        input: &ArchiveWorkflowInput,
    ) -> Result<(String, usize, usize), String> {
        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Running;
            s.progress.current_step = Some(UNPACK_STEP.into());
            s.progress.archive = Some(ArchiveProgress::default());
        });
        if let Err(e) = ctx
            .execute_activity(
                StepActivities::record_job_started,
                JobStartedInput {
                    job_id: input.job_id,
                    current_step: Some(UNPACK_STEP.into()),
                },
                short_options(3),
            )
            .await
        {
            tracing::warn!(job_id = %input.job_id, error = %e, "could not mark the archive job running");
        }
        let expanded = ctx
            .execute_activity(
                ArchiveActivities::expand_archive,
                ExpandArchiveInput {
                    job_id: input.job_id,
                    input: input.input.clone(),
                },
                expand_options(),
            )
            .await
            .map_err(|e| {
                format!(
                    "could not read the archive: {}",
                    describe_activity_failure(&e)
                )
            })?;
        ctx.state_mut(|s| {
            s.progress.total_steps = expanded.files;
            let a = s.progress.archive.get_or_insert_with(Default::default);
            a.files_total = expanded.files;
            // Keep the samples, but count every skipped entry.
            let samples = expanded.skipped_samples.len();
            expanded
                .skipped_samples
                .iter()
                .cloned()
                .for_each(|i| a.skip(i));
            a.files_skipped += expanded.skipped.saturating_sub(samples);
        });
        Ok((expanded.manifest, expanded.files, 0))
    }

    /// Run one file as a child workflow under its own job id, like a source item.
    async fn run_file(
        ctx: &WorkflowContext<Self>,
        job: ArchiveChildJob,
    ) -> Result<(), FileFailure> {
        let job_id = job.workflow_input.job_id;
        let path = job.path;
        let failure = |reason: String| FileFailure {
            path: path.clone(),
            job_id,
            reason,
        };
        let opts = ChildWorkflowOptions::workflow_id(
            meili_ingest_plugin_sdk::PipelineWorkflowInput::workflow_id(job_id),
        );
        let started = ctx
            .start_child_workflow(PipelineWorkflow::run, job.workflow_input, opts)
            .await
            .map_err(|e| failure(format!("did not start: {e}")))?;
        let output = started.result().await.map_err(|e| {
            failure(
                e.failure()
                    .and_then(deepest_failure_message)
                    .unwrap_or_else(|| e.to_string()),
            )
        })?;
        match output.status {
            JobStatus::Succeeded => Ok(()),
            other => Err(failure(format!("ended {}", other.as_str()))),
        }
    }

    async fn fail(
        ctx: &mut WorkflowContext<Self>,
        job_id: Uuid,
        message: String,
    ) -> WorkflowResult<ArchiveWorkflowOutput> {
        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Failed;
            s.progress.error = Some(message.clone());
        });
        Self::record_finished(ctx, job_id, JobStatus::Failed, Some(message.clone())).await;
        Err(ApplicationFailure::non_retryable(anyhow::anyhow!(message)).into())
    }

    async fn finish_cancelled(
        ctx: &mut WorkflowContext<Self>,
        job_id: Uuid,
    ) -> WorkflowResult<ArchiveWorkflowOutput> {
        ctx.state_mut(|s| {
            s.progress.status = JobStatus::Cancelled;
            s.progress.current_step = None;
        });
        Self::record_finished(ctx, job_id, JobStatus::Cancelled, None).await;
        Err(WorkflowTermination::cancelled())
    }

    /// Best effort: the row is a cache, Temporal holds the truth.
    async fn record_finished(
        ctx: &WorkflowContext<Self>,
        job_id: Uuid,
        status: JobStatus,
        error: Option<String>,
    ) {
        if let Err(e) = ctx
            .execute_activity(
                ArchiveActivities::record_archive_finished,
                ArchiveFinishedInput {
                    job_id,
                    status,
                    error,
                },
                short_options(5),
            )
            .await
        {
            tracing::warn!(job_id = %job_id, error = %e, "could not record the archive job's status");
        }
    }
}

/// Why a finished archive job failed, if it did.
pub fn outcome_error(a: &ArchiveProgress) -> Option<String> {
    if a.files_failed > 0 {
        let handled = a.files_succeeded + a.files_failed;
        let first = a
            .failed
            .first()
            .map(|f| format!("; first: {}: {}", f.path, f.reason))
            .unwrap_or_default();
        return Some(format!(
            "{} of {handled} files failed{first}",
            a.files_failed
        ));
    }
    if a.files_succeeded == 0 {
        return Some(if a.files_skipped == 0 {
            "the archive holds no files".into()
        } else {
            format!(
                "none of the archive's files could be ingested ({} skipped; see \
                 progress.archive.skipped)",
                a.files_skipped
            )
        });
    }
    None
}

/// Reading and staging a large archive takes a while: generous timeout, kept alive by
/// heartbeats, and a few retries for a blob store or network hiccup.
fn expand_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(60 * 60))
        .heartbeat_timeout(Duration::from_secs(90))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(3)
                .initial_interval(Duration::from_secs(10))
                .backoff_coefficient(3.0)
                .build(),
        )
        .build()
}

/// Routing a wave is a few control-plane calls per file.
fn prepare_options() -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(5 * 60))
        .retry_policy(
            RetryPolicy::builder()
                .maximum_attempts(5)
                .initial_interval(Duration::from_secs(5))
                .backoff_coefficient(2.0)
                .build(),
        )
        .build()
}

fn short_options(attempts: u32) -> ActivityOptions {
    ActivityOptions::with_start_to_close_timeout(Duration::from_secs(30))
        .task_queue("workers-general".to_string())
        .retry_policy(RetryPolicy::builder().maximum_attempts(attempts).build())
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn issue(path: &str) -> ArchiveFileIssue {
        ArchiveFileIssue {
            path: path.into(),
            reason: "step extract (pdf_extractor) failed: bad xref".into(),
            job_id: None,
        }
    }

    #[test]
    fn an_archive_with_only_successes_succeeds() {
        let a = ArchiveProgress {
            files_total: 3,
            files_started: 3,
            files_succeeded: 3,
            files_skipped: 2,
            ..Default::default()
        };
        assert_eq!(outcome_error(&a), None);
    }

    #[test]
    fn one_failed_file_fails_the_archive_and_is_named() {
        let mut a = ArchiveProgress {
            files_succeeded: 2,
            ..Default::default()
        };
        a.fail(issue("docs/b.pdf"));
        assert_eq!(
            outcome_error(&a).unwrap(),
            "1 of 3 files failed; first: docs/b.pdf: step extract (pdf_extractor) failed: bad xref"
        );
    }

    #[test]
    fn an_archive_with_nothing_ingestable_fails() {
        assert_eq!(
            outcome_error(&ArchiveProgress::default()).unwrap(),
            "the archive holds no files"
        );
        let a = ArchiveProgress {
            files_skipped: 4,
            ..Default::default()
        };
        assert!(outcome_error(&a).unwrap().contains("4 skipped"));
    }

    #[test]
    fn issue_lists_are_capped_but_counts_are_not() {
        let mut a = ArchiveProgress::default();
        for i in 0..(meili_ingest_plugin_sdk::ARCHIVE_ISSUES_KEPT + 5) {
            a.fail(issue(&format!("f{i}")));
            a.skip(issue(&format!("s{i}")));
        }
        assert_eq!(
            a.files_failed,
            meili_ingest_plugin_sdk::ARCHIVE_ISSUES_KEPT + 5
        );
        assert_eq!(a.failed.len(), meili_ingest_plugin_sdk::ARCHIVE_ISSUES_KEPT);
        assert_eq!(
            a.files_skipped,
            meili_ingest_plugin_sdk::ARCHIVE_ISSUES_KEPT + 5
        );
        assert_eq!(
            a.skipped.len(),
            meili_ingest_plugin_sdk::ARCHIVE_ISSUES_KEPT
        );
    }
}
