//! Temporal Schedules for scheduled sources (spec Decisions 3, 4 and 10).
//!
//! Each source owns one Schedule whose action starts `SourceRunWorkflow` with a tiny,
//! frozen input — the source id and tenant — so the schedule never needs rewriting when
//! the source or its pipeline changes. The gateway does not link the worker, so the
//! workflow is started untyped, exactly as `PipelineWorkflow` already is.
//!
//! [`ScheduleClient`] is a trait so the `/sources` handlers are testable without a
//! Temporal server, mirroring [`crate::state::WorkflowStarter`].

use std::time::SystemTime;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use meili_ingest_source::{SOURCE_RUN_WORKFLOW, SourceRunInput};
use temporalio_client::schedules::{
    CreateScheduleOptions, DeleteScheduleOptions, PauseScheduleOptions, ScheduleAction,
    ScheduleError, ScheduleOverlapPolicy, ScheduleSpec, TriggerScheduleOptions,
    UnpauseScheduleOptions,
};
use temporalio_client::tonic::Code;
use temporalio_client::{Client, UntypedWorkflow};
use temporalio_common::data_converters::{PayloadConverter, RawValue};
use uuid::Uuid;

use crate::error::GatewayError;
use crate::state::WORKFLOW_TASK_QUEUE;

/// Everything a source's schedule is built from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SourceSchedule {
    /// Temporal schedule id (see [`SourceRunInput::schedule_id`]).
    pub schedule_id: String,
    /// The source the schedule runs.
    pub source_id: Uuid,
    /// Its tenant scope.
    pub project_id: Option<String>,
    /// Cron expression; Temporal validates it (Decision 10).
    pub cron: String,
    /// IANA timezone the cron is evaluated in.
    pub timezone: String,
    /// Whether the schedule starts paused.
    pub paused: bool,
}

/// What `describe` reports back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleInfo {
    /// Whether the schedule is paused in Temporal.
    pub paused: bool,
    /// Next time it will fire, if any.
    pub next_run_at: Option<DateTime<Utc>>,
}

/// Temporal Schedule operations the `/sources` routes need.
#[async_trait]
pub trait ScheduleClient: Send + Sync {
    /// Create the schedule.
    async fn create(&self, schedule: &SourceSchedule) -> Result<(), GatewayError>;
    /// Replace the cron/timezone of an existing schedule.
    async fn update(&self, schedule: &SourceSchedule) -> Result<(), GatewayError>;
    /// Delete the schedule. A schedule that is already gone counts as deleted, so a
    /// retried delete (or a cleanup after a partial failure) is safe.
    async fn delete(&self, schedule_id: &str) -> Result<(), GatewayError>;
    /// Pause or unpause.
    async fn set_paused(&self, schedule_id: &str, paused: bool) -> Result<(), GatewayError>;
    /// Run now, off-schedule. Skipped if a run is already in flight.
    async fn trigger(&self, schedule_id: &str) -> Result<(), GatewayError>;
    /// Current state; `Ok(None)` when the schedule does not exist.
    async fn describe(&self, schedule_id: &str) -> Result<Option<ScheduleInfo>, GatewayError>;
}

/// The overlap policy every source schedule uses: a tick that fires while the previous
/// run is still in flight is dropped. A daily import that takes 40 minutes must never
/// have two copies racing into the same index.
pub const OVERLAP: ScheduleOverlapPolicy = ScheduleOverlapPolicy::Skip;

/// The spec for a source: its cron, evaluated in its timezone.
pub fn spec_for(s: &SourceSchedule) -> ScheduleSpec {
    ScheduleSpec::builder()
        .cron_strings(vec![s.cron.clone()])
        .timezone_name(s.timezone.clone())
        .build()
}

/// The action for a source: start `SourceRunWorkflow` with the frozen, tiny input.
pub fn action_for(s: &SourceSchedule) -> ScheduleAction {
    let input = SourceRunInput {
        source_id: s.source_id,
        project_id: s.project_id.clone(),
    };
    ScheduleAction::start_workflow(
        UntypedWorkflow::new(SOURCE_RUN_WORKFLOW),
        RawValue::from_value(&input, &PayloadConverter::default()),
        WORKFLOW_TASK_QUEUE,
        SourceRunInput::workflow_id_prefix(s.source_id),
    )
}

/// Full create options for a source.
pub fn create_options(s: &SourceSchedule) -> CreateScheduleOptions {
    CreateScheduleOptions::builder()
        .action(action_for(s))
        .spec(spec_for(s))
        .overlap_policy(OVERLAP)
        .paused(s.paused)
        .note(format!("meili-ingest source {}", s.source_id))
        .build()
}

/// Map a schedule error onto the gateway's HTTP semantics.
///
/// `InvalidArgument` is how Temporal rejects a malformed cron or timezone, so it is the
/// user's input that is wrong: 422, per Decision 10, rather than shipping a second cron
/// parser that could disagree with the one actually firing.
pub fn map_schedule_error(e: ScheduleError, what: &str) -> GatewayError {
    match &e {
        ScheduleError::Rpc(status) => match status.code() {
            Code::InvalidArgument => GatewayError::Unprocessable(format!(
                "Temporal rejected the schedule: {}",
                status.message()
            )),
            Code::AlreadyExists => GatewayError::Unprocessable(format!(
                "a schedule already exists for this source: {}",
                status.message()
            )),
            Code::NotFound => GatewayError::NotFound(format!("schedule not found: {what}")),
            _ => GatewayError::Upstream(format!("cannot {what}: {e}")),
        },
        _ => GatewayError::Upstream(format!("cannot {what}: {e}")),
    }
}

fn to_utc(t: SystemTime) -> Option<DateTime<Utc>> {
    Some(DateTime::<Utc>::from(t))
}

/// [`ScheduleClient`] backed by a real Temporal client.
#[derive(Clone)]
pub struct TemporalSchedules(pub Client);

impl std::fmt::Debug for TemporalSchedules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TemporalSchedules")
    }
}

#[async_trait]
impl ScheduleClient for TemporalSchedules {
    async fn create(&self, s: &SourceSchedule) -> Result<(), GatewayError> {
        self.0
            .create_schedule(s.schedule_id.clone(), create_options(s))
            .await
            .map(|_| ())
            .map_err(|e| map_schedule_error(e, "create schedule"))
    }

    async fn update(&self, s: &SourceSchedule) -> Result<(), GatewayError> {
        let spec = spec_for(s);
        self.0
            .get_schedule_handle(s.schedule_id.clone())
            .update(
                move |u| {
                    u.set_spec(spec).set_overlap_policy(OVERLAP);
                },
                Default::default(),
            )
            .await
            .map_err(|e| map_schedule_error(e, "update schedule"))
    }

    async fn delete(&self, schedule_id: &str) -> Result<(), GatewayError> {
        match self
            .0
            .get_schedule_handle(schedule_id)
            .delete(DeleteScheduleOptions::default())
            .await
        {
            Ok(()) => Ok(()),
            Err(ScheduleError::Rpc(s)) if s.code() == Code::NotFound => Ok(()),
            Err(e) => Err(map_schedule_error(e, "delete schedule")),
        }
    }

    async fn set_paused(&self, schedule_id: &str, paused: bool) -> Result<(), GatewayError> {
        let handle = self.0.get_schedule_handle(schedule_id);
        let result = if paused {
            handle
                .pause(
                    Some("paused via meili-ingest"),
                    PauseScheduleOptions::default(),
                )
                .await
        } else {
            handle
                .unpause(
                    Some("unpaused via meili-ingest"),
                    UnpauseScheduleOptions::default(),
                )
                .await
        };
        result.map_err(|e| map_schedule_error(e, "pause or unpause schedule"))
    }

    async fn trigger(&self, schedule_id: &str) -> Result<(), GatewayError> {
        self.0
            .get_schedule_handle(schedule_id)
            .trigger(OVERLAP, TriggerScheduleOptions::default())
            .await
            .map_err(|e| map_schedule_error(e, "trigger schedule"))
    }

    async fn describe(&self, schedule_id: &str) -> Result<Option<ScheduleInfo>, GatewayError> {
        match self
            .0
            .get_schedule_handle(schedule_id)
            .describe(Default::default())
            .await
        {
            Ok(d) => Ok(Some(ScheduleInfo {
                paused: d.paused(),
                next_run_at: d.future_action_times().into_iter().next().and_then(to_utc),
            })),
            Err(ScheduleError::Rpc(s)) if s.code() == Code::NotFound => Ok(None),
            Err(e) => Err(map_schedule_error(e, "describe schedule")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use temporalio_client::tonic::Status;

    fn sample(paused: bool) -> SourceSchedule {
        let id = Uuid::nil();
        SourceSchedule {
            schedule_id: SourceRunInput::schedule_id(id),
            source_id: id,
            project_id: Some("tenant-1".into()),
            cron: "30 0 * * *".into(),
            timezone: "Europe/Paris".into(),
            paused,
        }
    }

    #[test]
    fn the_spec_carries_the_cron_in_the_sources_timezone() {
        let spec = spec_for(&sample(false));
        assert_eq!(spec.cron_strings, vec!["30 0 * * *".to_string()]);
        assert_eq!(spec.timezone_name, "Europe/Paris");
        assert!(spec.intervals.is_empty() && spec.calendars.is_empty());
    }

    #[test]
    fn the_action_starts_source_run_workflow_on_the_workflow_queue() {
        let ScheduleAction::StartWorkflow {
            workflow_type,
            task_queue,
            workflow_id,
            input,
        } = action_for(&sample(false))
        else {
            panic!("a source schedule starts a workflow");
        };
        assert_eq!(workflow_type, SOURCE_RUN_WORKFLOW);
        assert_eq!(task_queue, WORKFLOW_TASK_QUEUE);
        assert_eq!(workflow_id, format!("source-run-{}", Uuid::nil()));
        assert!(input.is_some(), "the frozen input is attached");
    }

    #[test]
    fn create_options_skip_overlaps_and_honour_paused() {
        let opts = create_options(&sample(true));
        assert_eq!(opts.overlap_policy, ScheduleOverlapPolicy::Skip);
        assert!(opts.paused);
        assert!(
            !opts.trigger_immediately,
            "the first run waits for the cron"
        );
    }

    #[test]
    fn a_malformed_cron_is_422_not_502() {
        let e = map_schedule_error(
            ScheduleError::Rpc(Status::invalid_argument("invalid cron string")),
            "create schedule",
        );
        assert!(
            matches!(&e, GatewayError::Unprocessable(m) if m.contains("invalid cron")),
            "{e:?}"
        );
    }

    #[test]
    fn other_rpc_failures_are_upstream_errors() {
        let e = map_schedule_error(
            ScheduleError::Rpc(Status::unavailable("frontend down")),
            "create schedule",
        );
        assert!(matches!(e, GatewayError::Upstream(_)), "{e:?}");
        let e = map_schedule_error(ScheduleError::Rpc(Status::not_found("x")), "describe");
        assert!(matches!(e, GatewayError::NotFound(_)), "{e:?}");
    }
}
