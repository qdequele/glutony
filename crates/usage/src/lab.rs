//! Lab billing events (spec §5.2): one `usage.recorded` per finished job, in the
//! Meilisearch Lab envelope. Pure: same input, byte-identical event, same id.

use chrono::SecondsFormat;
use meili_ingest_plugin_sdk::UsageUnits;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::JobUsageInput;

/// Namespace of glutony's deterministic Lab event ids. Never change it: a new value
/// would give redelivered events new ids and bill them twice.
pub const GLUTONY_LAB_NAMESPACE: Uuid = Uuid::from_u128(0x6c8f_4a1e_2b7d_4c3a_9e51_7f0d_2a6b_8c14);

/// One Lab event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabEvent {
    /// Idempotency key: [`lab_event_id`] of the job.
    pub id: Uuid,
    /// Always `usage.recorded`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The job's finish time, RFC 3339 with milliseconds.
    pub occurred_at: String,
    /// The Lab account: the job's tenant id, a UUID.
    pub account_id: String,
    /// Glutony has no Lab key ids.
    pub api_key_id: Option<String>,
    /// Always `glutony`.
    pub product: String,
    /// The job's usage.
    pub data: GlutonyUsageData,
}

/// `data` of a glutony `usage.recorded` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GlutonyUsageData {
    /// Job id.
    pub job_id: Uuid,
    /// Pipeline that ran.
    pub pipeline_uid: String,
    /// `succeeded`, `failed` or `cancelled`.
    pub status: String,
    /// Wall time of the job.
    pub duration_ms: u64,
    /// What glutony paid providers, summed over the steps.
    pub cost_micro_usd: u64,
    /// `false` when a provider call could not be priced.
    pub cost_complete: bool,
    /// Units, summed over the steps.
    pub units: LabUnits,
}

/// Billable units of a job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[allow(missing_docs)]
pub struct LabUnits {
    pub documents_out: u64,
    pub input_bytes: u64,
    pub pages: u64,
    pub images: u64,
    pub audio_seconds: f64,
    pub llm_input_tokens: u64,
    pub llm_output_tokens: u64,
    pub llm_requests: u64,
    pub external_requests: u64,
}

/// Why a job produced no Lab event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    /// Standalone job: no tenant.
    NoTenant,
    /// The tenant is not a Lab account (e.g. a Cloud project id).
    NotAUuid,
}

impl SkipReason {
    /// Label for logs.
    pub fn as_str(&self) -> &'static str {
        match self {
            SkipReason::NoTenant => "no_tenant",
            SkipReason::NotAUuid => "not_a_uuid",
        }
    }
}

/// The deterministic id of a job's usage event.
pub fn lab_event_id(job_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &GLUTONY_LAB_NAMESPACE,
        format!("job:{job_id}:usage").as_bytes(),
    )
}

/// Build the job's Lab event, or say why there is none.
pub fn lab_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    if input.tenant_id.is_empty() {
        return Err(SkipReason::NoTenant);
    }
    let account = Uuid::parse_str(&input.tenant_id).map_err(|_| SkipReason::NotAUuid)?;
    let mut totals = UsageUnits::none();
    for step in &input.steps {
        totals.merge(step.usage);
    }
    let duration_ms = (input.finished_at - input.started_at)
        .num_milliseconds()
        .max(0) as u64;
    Ok(LabEvent {
        id: lab_event_id(input.job_id),
        kind: "usage.recorded".into(),
        occurred_at: input
            .finished_at
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        account_id: account.hyphenated().to_string(),
        api_key_id: None,
        product: "glutony".into(),
        data: GlutonyUsageData {
            job_id: input.job_id,
            pipeline_uid: input.pipeline_uid.clone(),
            status: input.status.as_str().to_string(),
            duration_ms,
            cost_micro_usd: totals.cost_micro_usd,
            cost_complete: totals.unpriced_calls == 0,
            units: LabUnits {
                documents_out: input
                    .steps
                    .last()
                    .map(|s| s.document_count as u64)
                    .unwrap_or(0),
                input_bytes: input.input_bytes,
                pages: totals.pages,
                images: totals.images,
                audio_seconds: totals.audio_seconds,
                llm_input_tokens: totals.llm_input_tokens,
                llm_output_tokens: totals.llm_output_tokens,
                llm_requests: totals.llm_requests,
                external_requests: totals.external_requests,
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use meili_ingest_plugin_sdk::{JobStatus, StepResult};

    use super::*;

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";

    fn step(id: &str, docs: usize, usage: UsageUnits) -> StepResult {
        StepResult {
            step_id: id.into(),
            plugin: "p".into(),
            status: JobStatus::Succeeded,
            document_count: docs,
            branches: 1,
            error: None,
            duration_ms: 10,
            input_bytes: 100,
            usage,
        }
    }

    fn input() -> JobUsageInput {
        JobUsageInput {
            job_id: "11111111-2222-3333-4444-555555555555".parse().unwrap(),
            workflow_id: "ingest-11111111-2222-3333-4444-555555555555".into(),
            pipeline_uid: "builtin.pdf".into(),
            pipeline_builtin: true,
            tenant_id: ACCOUNT.into(),
            region: String::new(),
            index_name: "documents".into(),
            task_queue: "workers-general".into(),
            status: JobStatus::Succeeded,
            started_at: Utc.with_ymd_and_hms(2026, 10, 1, 10, 0, 0).unwrap(),
            finished_at: Utc.with_ymd_and_hms(2026, 10, 1, 10, 0, 9).unwrap(),
            input_bytes: 5_000,
            input_mime: "application/pdf".into(),
            steps: vec![
                step("extract", 3, UsageUnits::pages(9)),
                step(
                    "enrich",
                    12,
                    UsageUnits {
                        cost_micro_usd: 210,
                        unpriced_calls: 1,
                        ..UsageUnits::llm(1_000, 100)
                    },
                ),
            ],
            error: None,
        }
    }

    fn validator() -> jsonschema::Validator {
        let schema: serde_json::Value = serde_json::from_str(include_str!(
            "../../../contracts/vendor/lab/lab-events.schema.json"
        ))
        .unwrap();
        jsonschema::options()
            .should_validate_formats(true)
            .build(&schema)
            .unwrap()
    }

    #[test]
    fn a_lab_job_becomes_one_valid_event() {
        let event = lab_event_for_job(&input()).unwrap();
        let v = serde_json::to_value(&event).unwrap();
        let errors: Vec<String> = validator().iter_errors(&v).map(|e| e.to_string()).collect();
        assert!(errors.is_empty(), "{errors:?}\n{v:#}");
        assert_eq!(v["type"], "usage.recorded");
        assert_eq!(v["product"], "glutony");
        assert_eq!(v["account_id"], ACCOUNT);
        assert!(v["api_key_id"].is_null());
        assert_eq!(v["occurred_at"], "2026-10-01T10:00:09.000Z");
        assert_eq!(v["data"]["duration_ms"], 9_000);
        assert_eq!(v["data"]["cost_micro_usd"], 210);
        assert_eq!(v["data"]["cost_complete"], false);
        assert_eq!(v["data"]["units"]["pages"], 9);
        assert_eq!(v["data"]["units"]["llm_input_tokens"], 1_000);
        assert_eq!(
            v["data"]["units"]["documents_out"], 12,
            "the final step's documents"
        );
        assert_eq!(
            v["data"]["units"]["input_bytes"], 5_000,
            "the job's payload size"
        );
    }

    #[test]
    fn the_id_is_deterministic() {
        let a = lab_event_for_job(&input()).unwrap();
        let b = lab_event_for_job(&input()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.id, lab_event_id(input().job_id));
        assert_eq!(a.id.get_version_num(), 5);
        let mut other = input();
        other.job_id = Uuid::new_v4();
        assert_ne!(lab_event_for_job(&other).unwrap().id, a.id);
    }

    #[test]
    fn jobs_outside_the_lab_are_skipped() {
        let mut standalone = input();
        standalone.tenant_id = String::new();
        assert_eq!(lab_event_for_job(&standalone), Err(SkipReason::NoTenant));
        let mut cloud = input();
        cloud.tenant_id = "hackersearch".into();
        assert_eq!(lab_event_for_job(&cloud), Err(SkipReason::NotAUuid));
    }

    #[test]
    fn failed_jobs_are_billed_too_and_complete_cost_says_so() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        failed.steps[1].usage.unpriced_calls = 0;
        let v = serde_json::to_value(lab_event_for_job(&failed).unwrap()).unwrap();
        assert!(validator().is_valid(&v));
        assert_eq!(v["data"]["status"], "failed");
        assert_eq!(v["data"]["cost_complete"], true);
    }

    #[test]
    fn the_schema_is_really_enforced() {
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["surprise"] = 1.into();
        assert!(!validator().is_valid(&v));
    }
}
