//! Lab events (platform contract v2, spec §4): one `usage.recorded` and one job
//! lifecycle event per finished job, in the Meilisearch Lab envelope. Pure: same
//! input, byte-identical events, same ids.
//!
//! Credits are the Lab's business (decision B): this module reports raw units and
//! the provider cost glutony paid, nothing priced.

use std::collections::BTreeMap;

use chrono::SecondsFormat;
use meili_ingest_plugin_sdk::{JobStatus, UsageUnits};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::JobUsageInput;

/// Namespace of glutony's deterministic Lab event ids. Never change it: a new value
/// would give redelivered events new ids and bill them twice.
pub const GLUTONY_LAB_NAMESPACE: Uuid = Uuid::from_u128(0x6c8f_4a1e_2b7d_4c3a_9e51_7f0d_2a6b_8c14);
/// The one operation glutony reports (spec §4.3).
pub const OPERATION: &str = "ingest";
/// The `product` of every glutony event.
pub const PRODUCT: &str = "glutony";

/// One Lab event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabEvent {
    /// Idempotency key: [`lab_event_id`] or [`lab_job_event_id`] of the job.
    pub id: Uuid,
    /// `usage.recorded`, `job.completed` or `job.failed`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The job's finish time, RFC 3339 with milliseconds.
    pub occurred_at: String,
    /// The Lab account: the job's tenant id, a canonical UUID.
    pub account_id: String,
    /// Glutony has no Lab key ids.
    pub api_key_id: Option<String>,
    /// Always `glutony`.
    pub product: String,
    /// Per-type payload.
    pub data: LabEventData,
}

/// `data` of a Lab event, by type.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum LabEventData {
    /// `usage.recorded`.
    Usage(UsageData),
    /// `job.completed`.
    JobCompleted(JobCompletedData),
    /// `job.failed`.
    JobFailed(JobFailedData),
}

/// `data` of a `usage.recorded` event (spec §4.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageData {
    /// Always [`OPERATION`].
    pub operation: String,
    /// Raw units by name, sorted so the serialized event is byte-stable.
    pub units: BTreeMap<String, u64>,
    /// What glutony paid providers, or 0 when a call could not be priced.
    pub provider_cost_micro_usd: u64,
    /// Human label for the ledger: pipeline, status, documents, cost completeness.
    pub description: String,
    /// The job id.
    pub job_id: String,
}

/// `data` of a `job.completed` event (spec §4.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobCompletedData {
    /// The job id.
    pub job_id: String,
    /// The pipeline uid (glutony has no crawl index).
    pub index_uid: String,
    /// Always 0 for glutony.
    pub pages_crawled: u64,
    /// The final step's documents.
    pub documents_indexed: u64,
    /// Wall time of the job, whole seconds.
    pub duration_secs: u64,
}

/// `data` of a `job.failed` event (spec §4.4). Cancelled jobs are reported here too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobFailedData {
    /// The job id.
    pub job_id: String,
    /// The job's error, or its status when it carries none.
    pub error_message: String,
    /// Always 0 for glutony.
    pub pages_crawled: u64,
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

/// The Lab account a tenant id names, or why it names none. Only the canonical
/// lower-case hyphenated form counts: other spellings of the same UUID (upper-case,
/// simple, braced, URN) would bill a different string.
fn lab_account(tenant_id: &str) -> Result<Uuid, SkipReason> {
    if tenant_id.is_empty() {
        return Err(SkipReason::NoTenant);
    }
    Uuid::parse_str(tenant_id)
        .ok()
        .filter(|a| a.hyphenated().to_string() == tenant_id)
        .ok_or(SkipReason::NotAUuid)
}

fn totals(input: &JobUsageInput) -> UsageUnits {
    let mut totals = UsageUnits::none();
    for step in &input.steps {
        totals.merge(step.usage);
    }
    totals
}

fn documents_out(input: &JobUsageInput) -> u64 {
    input
        .steps
        .last()
        .map(|s| s.document_count as u64)
        .unwrap_or(0)
}

/// The v2 units of a job (spec §4.3), from the step totals. Every value is a
/// non-negative integer; fractional seconds round up so a started second is billed.
/// `ocr_pages` stays 0 until a plugin reports OCR'd pages: the PDF extractor's `pages`
/// are not OCR work and are sent as an extra, unpriced unit instead.
pub fn lab_units(input: &JobUsageInput, totals: &UsageUnits) -> BTreeMap<String, u64> {
    let step_ms = input
        .steps
        .iter()
        .fold(0u64, |acc, s| acc.saturating_add(s.duration_ms));
    let audio_seconds = if totals.audio_seconds.is_finite() && totals.audio_seconds > 0.0 {
        totals.audio_seconds.ceil() as u64
    } else {
        0
    };
    BTreeMap::from([
        ("documents".to_string(), documents_out(input)),
        ("bytes_in".to_string(), input.input_bytes),
        ("step_seconds".to_string(), step_ms.div_ceil(1000)),
        ("llm_tokens_in".to_string(), totals.llm_input_tokens),
        ("llm_tokens_out".to_string(), totals.llm_output_tokens),
        ("audio_seconds".to_string(), audio_seconds),
        ("ocr_pages".to_string(), 0),
        // Extra units the Lab stores but does not price (spec §4.3).
        ("pages".to_string(), totals.pages),
        ("images".to_string(), totals.images),
        ("llm_requests".to_string(), totals.llm_requests),
        ("external_requests".to_string(), totals.external_requests),
    ])
}

fn envelope(
    input: &JobUsageInput,
    id: Uuid,
    kind: &str,
    data: LabEventData,
) -> Result<LabEvent, SkipReason> {
    let account = lab_account(&input.tenant_id)?;
    Ok(LabEvent {
        id,
        kind: kind.to_string(),
        occurred_at: input
            .finished_at
            .to_rfc3339_opts(SecondsFormat::Millis, true),
        account_id: account.hyphenated().to_string(),
        api_key_id: None,
        product: PRODUCT.to_string(),
        data,
    })
}

/// Build the job's `usage.recorded` event, or say why there is none.
pub fn lab_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    let totals = totals(input);
    let complete = totals.unpriced_calls == 0;
    // The Lab's ledger is a signed bigint; the schema caps the value too. An
    // incomplete cost is reported as 0 rather than as a number that is too small.
    let provider_cost_micro_usd = if complete {
        totals.cost_micro_usd.min(i64::MAX as u64)
    } else {
        0
    };
    let units = lab_units(input, &totals);
    let description = format!(
        "Job {} ({}, {}, {} documents{})",
        input.job_id,
        input.pipeline_uid,
        input.status.as_str(),
        documents_out(input),
        if complete {
            ""
        } else {
            ", provider cost incomplete"
        }
    );
    envelope(
        input,
        lab_event_id(input.job_id),
        "usage.recorded",
        LabEventData::Usage(UsageData {
            operation: OPERATION.to_string(),
            units,
            provider_cost_micro_usd,
            description,
            job_id: input.job_id.to_string(),
        }),
    )
}

/// The deterministic id of a job's lifecycle event (`job.completed` / `job.failed`).
pub fn lab_job_event_id(job_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &GLUTONY_LAB_NAMESPACE,
        format!("job:{job_id}:lifecycle").as_bytes(),
    )
}

/// Build the job's lifecycle event (spec §4.4): `job.completed` for a succeeded job,
/// `job.failed` otherwise (failed and cancelled; the usage activity only runs on
/// terminal statuses).
pub fn lab_job_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    let duration_secs = (input.finished_at - input.started_at).num_seconds().max(0) as u64;
    let job_id = input.job_id.to_string();
    let (kind, data) = match input.status {
        JobStatus::Succeeded => (
            "job.completed",
            LabEventData::JobCompleted(JobCompletedData {
                job_id,
                index_uid: input.pipeline_uid.clone(),
                pages_crawled: 0,
                documents_indexed: documents_out(input),
                duration_secs,
            }),
        ),
        status => (
            "job.failed",
            LabEventData::JobFailed(JobFailedData {
                job_id,
                error_message: input
                    .error
                    .clone()
                    .filter(|e| !e.trim().is_empty())
                    .unwrap_or_else(|| status.as_str().to_string()),
                pages_crawled: 0,
            }),
        ),
    };
    envelope(input, lab_job_event_id(input.job_id), kind, data)
}

/// Every Lab event of a finished job: the usage event, then the lifecycle event.
pub fn lab_events_for_job(input: &JobUsageInput) -> Result<Vec<LabEvent>, SkipReason> {
    Ok(vec![
        lab_event_for_job(input)?,
        lab_job_event_for_job(input)?,
    ])
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use meili_ingest_plugin_sdk::StepResult;

    use super::*;

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";

    fn step(id: &str, docs: usize, duration_ms: u64, usage: UsageUnits) -> StepResult {
        StepResult {
            step_id: id.into(),
            plugin: "p".into(),
            status: JobStatus::Succeeded,
            document_count: docs,
            branches: 1,
            error: None,
            duration_ms,
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
                step("extract", 3, 1_200, UsageUnits::pages(9)),
                step(
                    "enrich",
                    12,
                    2_100,
                    UsageUnits {
                        cost_micro_usd: 210,
                        unpriced_calls: 1,
                        audio_seconds: 2.4,
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

    fn errors_of(v: &serde_json::Value) -> Vec<String> {
        validator().iter_errors(v).map(|e| e.to_string()).collect()
    }

    #[test]
    fn a_lab_job_becomes_one_valid_v2_usage_event() {
        let event = lab_event_for_job(&input()).unwrap();
        let v = serde_json::to_value(&event).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "usage.recorded");
        assert_eq!(v["product"], "glutony");
        assert_eq!(v["account_id"], ACCOUNT);
        assert!(v["api_key_id"].is_null());
        assert_eq!(v["occurred_at"], "2026-10-01T10:00:09.000Z");
        assert_eq!(v["data"]["operation"], "ingest");
        assert_eq!(v["data"]["job_id"], "11111111-2222-3333-4444-555555555555");
        // One unpriced call: the pass-through cost is unknown, so it is reported as 0.
        assert_eq!(v["data"]["provider_cost_micro_usd"], 0);
        assert!(
            v["data"]["description"]
                .as_str()
                .unwrap()
                .contains("provider cost incomplete")
        );
        let units = &v["data"]["units"];
        assert_eq!(units["documents"], 12, "the final step's documents");
        assert_eq!(units["bytes_in"], 5_000, "the job's payload size");
        assert_eq!(units["step_seconds"], 4, "3.3 s of steps, rounded up");
        assert_eq!(units["llm_tokens_in"], 1_000);
        assert_eq!(units["llm_tokens_out"], 100);
        assert_eq!(units["audio_seconds"], 3, "2.4 s, rounded up");
        assert_eq!(units["ocr_pages"], 0, "no plugin reports OCR pages yet");
        // Extra units: stored by the Lab, ignored by pricing (spec §4.3).
        assert_eq!(units["pages"], 9);
        assert_eq!(units["llm_requests"], 1);
        for (name, value) in units.as_object().unwrap() {
            assert!(
                value.is_u64(),
                "{name} must be a non-negative integer, got {value}"
            );
        }
        // No field the schema does not know (status and cost_complete live in description).
        let keys: Vec<&String> = v["data"].as_object().unwrap().keys().collect();
        assert_eq!(
            keys,
            [
                "description",
                "job_id",
                "operation",
                "provider_cost_micro_usd",
                "units"
            ]
        );
    }

    #[test]
    fn complete_cost_is_passed_through() {
        let mut i = input();
        i.steps[1].usage.unpriced_calls = 0;
        let event = lab_event_for_job(&i).unwrap();
        let LabEventData::Usage(data) = &event.data else {
            panic!("usage event expected");
        };
        assert_eq!(data.provider_cost_micro_usd, 210);
        assert!(!data.description.contains("incomplete"));
    }

    #[test]
    fn the_id_is_deterministic() {
        let a = lab_event_for_job(&input()).unwrap();
        let b = lab_event_for_job(&input()).unwrap();
        assert_eq!(a, b);
        assert_eq!(a.id, lab_event_id(input().job_id));
        assert_eq!(a.id.get_version_num(), 5);
        // Golden value: changing GLUTONY_LAB_NAMESPACE or the name re-ids every
        // redelivered event and the Lab would count them again.
        assert_eq!(
            lab_event_id("11111111-2222-3333-4444-555555555555".parse().unwrap()),
            "eb3df490-5f14-59b7-9cad-c97f959127ae"
                .parse::<Uuid>()
                .unwrap()
        );
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
    fn only_the_canonical_uuid_form_is_a_lab_account() {
        let upper = ACCOUNT.to_uppercase();
        let simple = ACCOUNT.replace('-', "");
        let braced = format!("{{{ACCOUNT}}}");
        let urn = format!("urn:uuid:{ACCOUNT}");
        for form in [upper, simple, braced, urn] {
            let mut i = input();
            i.tenant_id = form.clone();
            assert_eq!(
                lab_event_for_job(&i),
                Err(SkipReason::NotAUuid),
                "{form} must be skipped"
            );
        }
        assert_eq!(lab_event_for_job(&input()).unwrap().account_id, ACCOUNT);
    }

    #[test]
    fn the_cost_is_capped_at_i64_max_for_a_signed_ledger() {
        let mut i = input();
        i.steps = vec![step(
            "enrich",
            1,
            1,
            UsageUnits {
                cost_micro_usd: u64::MAX,
                ..UsageUnits::default()
            },
        )];
        let v = serde_json::to_value(lab_event_for_job(&i).unwrap()).unwrap();
        assert_eq!(v["data"]["provider_cost_micro_usd"], i64::MAX);
        assert!(errors_of(&v).is_empty(), "{:?}", errors_of(&v));
        let mut over = v.clone();
        over["data"]["provider_cost_micro_usd"] = serde_json::json!(u64::MAX);
        assert!(!validator().is_valid(&over));
    }

    #[test]
    fn non_finite_audio_seconds_still_make_a_valid_event() {
        for bad in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -3.0] {
            let mut i = input();
            i.steps = vec![step(
                "transcribe",
                1,
                1,
                UsageUnits {
                    audio_seconds: bad,
                    ..UsageUnits::default()
                },
            )];
            let v = serde_json::to_value(lab_event_for_job(&i).unwrap()).unwrap();
            assert_eq!(v["data"]["units"]["audio_seconds"], 0, "{bad}");
            assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        }
    }

    #[test]
    fn failed_jobs_are_reported_too() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        let v = serde_json::to_value(lab_event_for_job(&failed).unwrap()).unwrap();
        assert!(validator().is_valid(&v));
        assert!(
            v["data"]["description"]
                .as_str()
                .unwrap()
                .contains("failed")
        );
    }

    #[test]
    fn the_schema_is_really_enforced() {
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["surprise"] = 1.into();
        assert!(!validator().is_valid(&v));
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["units"]["documents"] = (-1).into();
        assert!(!validator().is_valid(&v));
        let mut v = serde_json::to_value(lab_event_for_job(&input()).unwrap()).unwrap();
        v["data"]["operation"] = "crawl".into();
        assert!(!validator().is_valid(&v), "glutony only sends ingest");
    }

    #[test]
    fn a_succeeded_job_also_emits_job_completed() {
        let events = lab_events_for_job(&input()).unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].kind, "usage.recorded");
        let done = &events[1];
        assert_eq!(done.kind, "job.completed");
        assert_eq!(done.id, lab_job_event_id(input().job_id));
        assert_ne!(done.id, events[0].id);
        assert_eq!(done.id.get_version_num(), 5);
        let v = serde_json::to_value(done).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["data"]["index_uid"], "builtin.pdf");
        assert_eq!(v["data"]["documents_indexed"], 12);
        assert_eq!(v["data"]["pages_crawled"], 0);
        assert_eq!(v["data"]["duration_secs"], 9);
        assert_eq!(v["data"]["job_id"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn failed_and_cancelled_jobs_emit_job_failed() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        failed.error = Some("pdf extractor exploded".into());
        let v = serde_json::to_value(lab_job_event_for_job(&failed).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "job.failed");
        assert_eq!(v["data"]["error_message"], "pdf extractor exploded");
        assert_eq!(v["data"]["pages_crawled"], 0);

        let mut cancelled = input();
        cancelled.status = JobStatus::Cancelled;
        let v = serde_json::to_value(lab_job_event_for_job(&cancelled).unwrap()).unwrap();
        assert!(validator().is_valid(&v));
        assert_eq!(v["type"], "job.failed");
        assert_eq!(v["data"]["error_message"], "cancelled");
    }

    #[test]
    fn job_events_follow_the_same_skip_rules() {
        let mut cloud = input();
        cloud.tenant_id = "hackersearch".into();
        assert_eq!(lab_events_for_job(&cloud), Err(SkipReason::NotAUuid));
        let mut standalone = input();
        standalone.tenant_id = String::new();
        assert_eq!(
            lab_job_event_for_job(&standalone),
            Err(SkipReason::NoTenant)
        );
    }

    #[test]
    fn the_lifecycle_id_golden_value() {
        // Python: uuid.uuid5(GLUTONY_LAB_NAMESPACE, "job:11111111-2222-3333-4444-555555555555:lifecycle")
        let id = lab_job_event_id("11111111-2222-3333-4444-555555555555".parse().unwrap());
        assert_eq!(id.get_version_num(), 5);
        assert_eq!(
            id,
            Uuid::new_v5(
                &GLUTONY_LAB_NAMESPACE,
                b"job:11111111-2222-3333-4444-555555555555:lifecycle"
            )
        );
        assert_eq!(
            id,
            "b1b2c9fd-369e-5f6b-afcf-df40c0b32539"
                .parse::<Uuid>()
                .unwrap()
        );
    }
}
