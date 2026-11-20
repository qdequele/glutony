//! Lab events (platform contract v2, spec §4): one `usage.recorded` and one job
//! lifecycle event (`job.completed`, `job.failed` or `job.cancelled`) per finished
//! job, and a `source.status_changed` when a scheduled source starts failing or
//! recovers, in the Meilisearch Lab envelope. Pure: same input, byte-identical
//! events, same ids.
//!
//! Credits are the Lab's business (decision B): this module reports raw units and
//! the provider cost glutony paid, nothing priced.

use std::collections::BTreeMap;

use chrono::{DateTime, SecondsFormat, Utc};
use meili_ingest_plugin_sdk::{INDEXER_PLUGIN, JobGroup, JobStatus, UsageUnits};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{JobUsageInput, documents_indexed};

/// Namespace of glutony's deterministic Lab event ids. Never change it: a new value
/// would give redelivered events new ids and bill them twice.
pub const GLUTONY_LAB_NAMESPACE: Uuid = Uuid::from_u128(0x6c8f_4a1e_2b7d_4c3a_9e51_7f0d_2a6b_8c14);
/// The one operation glutony reports (spec §4.3).
pub const OPERATION: &str = "ingest";
/// The `product` of every glutony event.
pub const PRODUCT: &str = "glutony";
/// The plugin that does OCR: the external gRPC plugin of that name (the control
/// plane's `EXTERNAL_PLUGINS`). Its steps are the job's `ocr_pages`.
pub const OCR_PLUGIN: &str = "ocr";

/// One Lab event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LabEvent {
    /// Idempotency key: [`lab_event_id`] or [`lab_job_event_id`] of the job.
    pub id: Uuid,
    /// `usage.recorded`, `job.completed`, `job.failed`, `job.cancelled` or
    /// `source.status_changed`.
    #[serde(rename = "type")]
    pub kind: String,
    /// The job's (or the source run's) finish time, RFC 3339 with milliseconds.
    pub occurred_at: String,
    /// The Lab account: the job's (or the source's) tenant id, a canonical UUID.
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
    /// `job.cancelled`.
    JobCancelled(JobCancelledData),
    /// `source.status_changed`.
    SourceStatus(SourceStatusData),
}

/// `data` of a `usage.recorded` event (spec §4.2).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UsageData {
    /// Always [`OPERATION`].
    pub operation: String,
    /// Raw units by name, sorted so the serialized event is byte-stable.
    pub units: BTreeMap<String, u64>,
    /// What glutony paid providers for the calls it could price; calls it could not
    /// price are missing from it and counted in the `unpriced_provider_calls` unit.
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
    /// The Meilisearch index the job wrote to, or the pipeline uid when the job did
    /// not name one (what glutony sent here before `pipeline` existed).
    pub index_uid: String,
    /// Always 0 for glutony.
    pub pages_crawled: u64,
    /// The documents that reached Meilisearch ([`documents_indexed`]).
    pub documents_indexed: u64,
    /// Wall time of the job, whole seconds.
    pub duration_secs: u64,
    /// The pipeline's uid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// The run the job belongs to (source run, archive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<JobGroup>,
}

/// `data` of a `job.failed` event (spec §4.4).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobFailedData {
    /// The job id.
    pub job_id: String,
    /// The job's error, or its status when it carries none.
    pub error_message: String,
    /// Always 0 for glutony.
    pub pages_crawled: u64,
    /// Why it failed, a code of the contract's `reason` list ([`failure_reason`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The id of the step that failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step: Option<String>,
    /// The Meilisearch index the job writes to, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    /// The pipeline's uid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// The run the job belongs to (source run, archive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<JobGroup>,
}

/// `data` of a `job.cancelled` event: the lifecycle event of a cancelled job.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobCancelledData {
    /// The job id.
    pub job_id: String,
    /// The Meilisearch index the job writes to, when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_uid: Option<String>,
    /// The documents that reached Meilisearch before the cancel ([`documents_indexed`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub documents_indexed: Option<u64>,
    /// The pipeline's uid.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// The run the job belongs to (source run, archive).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group: Option<JobGroup>,
}

/// Whether a scheduled source works.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    /// Its last run failed.
    Failing,
    /// Its last run fetched or found nothing new.
    Ok,
}

/// `data` of a `source.status_changed` event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceStatusData {
    /// The source id.
    pub source_id: String,
    /// The source's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    /// What it changed to.
    pub status: SourceStatus,
    /// The run that changed it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
    /// The pipeline the source runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
    /// Why it fails ([`source_failure_reason`]); only when failing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    /// The run's error; only when failing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /// Failed runs in a row, this one included; 0 when ok.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub consecutive_failures: Option<u64>,
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

/// Provider calls of the job the cost table could not price: their cost is missing
/// from `provider_cost_micro_usd`.
pub fn unpriced_provider_calls(input: &JobUsageInput) -> u64 {
    totals(input).unpriced_calls
}

/// Pages OCR'd by the job: for each [`OCR_PLUGIN`] step, the `pages` it reported, or
/// else its output documents. The gRPC plugin contract carries no usage, so today the
/// plugin reports nothing and emits one document per page it read.
fn ocr_pages(input: &JobUsageInput) -> u64 {
    input
        .steps
        .iter()
        .filter(|s| s.plugin == OCR_PLUGIN)
        .map(|s| {
            if s.usage.pages > 0 {
                s.usage.pages
            } else {
                s.document_count as u64
            }
        })
        .fold(0u64, u64::saturating_add)
}

/// The v2 units of a job (spec §4.3), from the step totals. Every value is a
/// non-negative integer; fractional seconds round up so a started second is billed.
/// `ocr_pages` counts only the [`OCR_PLUGIN`] steps (see [`ocr_pages`]): the PDF
/// extractor's `pages` are not OCR work and are sent as an extra, unpriced unit.
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
        ("documents".to_string(), documents_indexed(input)),
        ("bytes_in".to_string(), input.input_bytes),
        ("step_seconds".to_string(), step_ms.div_ceil(1000)),
        ("llm_tokens_in".to_string(), totals.llm_input_tokens),
        ("llm_tokens_out".to_string(), totals.llm_output_tokens),
        ("audio_seconds".to_string(), audio_seconds),
        ("ocr_pages".to_string(), ocr_pages(input)),
        // Extra units the Lab stores but does not price (spec §4.3).
        ("pages".to_string(), totals.pages),
        ("images".to_string(), totals.images),
        ("llm_requests".to_string(), totals.llm_requests),
        ("external_requests".to_string(), totals.external_requests),
        // Provider calls missing from `provider_cost_micro_usd` (0 when it is complete).
        ("unpriced_provider_calls".to_string(), totals.unpriced_calls),
    ])
}

fn envelope(
    input: &JobUsageInput,
    id: Uuid,
    kind: &str,
    data: LabEventData,
) -> Result<LabEvent, SkipReason> {
    envelope_at(&input.tenant_id, input.finished_at, id, kind, data)
}

fn envelope_at(
    tenant_id: &str,
    occurred_at: DateTime<Utc>,
    id: Uuid,
    kind: &str,
    data: LabEventData,
) -> Result<LabEvent, SkipReason> {
    let account = lab_account(tenant_id)?;
    Ok(LabEvent {
        id,
        kind: kind.to_string(),
        occurred_at: occurred_at.to_rfc3339_opts(SecondsFormat::Millis, true),
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
    // Always the priced sum, even when some calls could not be priced: those calls are
    // missing from it (flagged in the description and counted in the
    // `unpriced_provider_calls` unit), but the priced ones are real money and billed.
    // The Lab's ledger is a signed bigint; the schema caps the value too.
    let provider_cost_micro_usd = totals.cost_micro_usd.min(i64::MAX as u64);
    let units = lab_units(input, &totals);
    let description = format!(
        "Job {} ({}, {}, {} documents{})",
        input.job_id,
        input.pipeline_uid,
        input.status.as_str(),
        documents_indexed(input),
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

/// The deterministic id of a job's lifecycle event (`job.completed`, `job.failed` or
/// `job.cancelled`: a job has exactly one).
pub fn lab_job_event_id(job_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &GLUTONY_LAB_NAMESPACE,
        format!("job:{job_id}:lifecycle").as_bytes(),
    )
}

fn non_blank(s: &str) -> Option<String> {
    let s = s.trim();
    (!s.is_empty()).then(|| s.to_string())
}

/// Build the job's lifecycle event (spec §4.4): `job.completed` for a succeeded job,
/// `job.cancelled` for a cancelled one, `job.failed` otherwise (the usage activity
/// only runs on terminal statuses).
pub fn lab_job_event_for_job(input: &JobUsageInput) -> Result<LabEvent, SkipReason> {
    let duration_secs = (input.finished_at - input.started_at).num_seconds().max(0) as u64;
    let job_id = input.job_id.to_string();
    let index = non_blank(&input.index_name);
    let pipeline = non_blank(&input.pipeline_uid);
    let group = input.group.clone();
    let (kind, data) = match input.status {
        JobStatus::Succeeded => (
            "job.completed",
            LabEventData::JobCompleted(JobCompletedData {
                job_id,
                index_uid: index.unwrap_or_else(|| input.pipeline_uid.clone()),
                pages_crawled: 0,
                documents_indexed: documents_indexed(input),
                duration_secs,
                pipeline,
                group,
            }),
        ),
        JobStatus::Cancelled => (
            "job.cancelled",
            LabEventData::JobCancelled(JobCancelledData {
                job_id,
                index_uid: index,
                documents_indexed: Some(documents_indexed(input)),
                pipeline,
                group,
            }),
        ),
        status => {
            // The step that ended the job: settle_round records it after the steps of
            // its round that completed, and earlier rounds only hold completed steps.
            let failed = input.steps.iter().find(|s| s.status == JobStatus::Failed);
            let error_message = input
                .error
                .clone()
                .filter(|e| !e.trim().is_empty())
                .unwrap_or_else(|| status.as_str().to_string());
            let reason = input.reason.clone().unwrap_or_else(|| {
                let message = failed
                    .and_then(|s| s.error.as_deref())
                    .unwrap_or(&error_message);
                failure_reason(failed.map(|s| s.plugin.as_str()), message).to_string()
            });
            (
                "job.failed",
                LabEventData::JobFailed(JobFailedData {
                    job_id,
                    error_message,
                    pages_crawled: 0,
                    reason: Some(reason),
                    step: failed.map(|s| s.step_id.clone()),
                    index_uid: index,
                    pipeline,
                    group,
                }),
            )
        }
    };
    envelope(input, lab_job_event_id(input.job_id), kind, data)
}

/// The `reason` codes of the Lab contract glutony sends (`$defs.reason` of
/// `lab-events.schema.json`).
pub mod reason {
    /// The team has no Lab credits.
    pub const INSUFFICIENT_CREDITS: &str = "insufficient_credits";
    /// The team's AI Gateway budget is spent.
    pub const GATEWAY_BUDGET_EXHAUSTED: &str = "gateway_budget_exhausted";
    /// An AI step had no Gateway to call.
    pub const NO_GATEWAY: &str = "no_gateway";
    /// Meilisearch refused the key.
    pub const DESTINATION_AUTH: &str = "destination_auth";
    /// Meilisearch did not answer.
    pub const DESTINATION_UNREACHABLE: &str = "destination_unreachable";
    /// Meilisearch refused the documents or the index.
    pub const DESTINATION_REJECTED: &str = "destination_rejected";
    /// The source refused the credentials (401/403).
    pub const SOURCE_AUTH: &str = "source_auth";
    /// The source is gone (404/410).
    pub const SOURCE_NOT_FOUND: &str = "source_not_found";
    /// The source did not answer.
    pub const SOURCE_UNREACHABLE: &str = "source_unreachable";
    /// The pipeline or a step's config is invalid.
    pub const INVALID_CONFIG: &str = "invalid_config";
    /// A file or document could not be read.
    pub const INVALID_INPUT: &str = "invalid_input";
    /// Anything else.
    pub const INTERNAL: &str = "internal";
}

/// Why a job failed, as a Lab `reason` code, from the failed step's plugin and error.
///
/// The error is the text the worker recorded: a [`PluginError`]'s `Display`
/// (`invalid config: …`, `invalid input: …`, `non-retryable: …`), possibly wrapped by
/// a fan-out (`… branches failed; first error: …`) or the job (`step x (p) failed:
/// …`). The plugin's own classification wins; then the messages the worker and the
/// indexer write for credits, the AI Gateway and Meilisearch; anything else is
/// `internal`.
///
/// [`PluginError`]: meili_ingest_plugin_sdk::PluginError
pub fn failure_reason(plugin: Option<&str>, message: &str) -> &'static str {
    let m = message.to_lowercase();
    let has = |needle: &str| m.contains(needle);
    if has("invalid config: ") || m.starts_with("invalid pipeline ") {
        return reason::INVALID_CONFIG;
    }
    if has("invalid input: ") {
        return reason::INVALID_INPUT;
    }
    // The credit pre-check (402 from the control plane).
    if has("no lab credits") || has("insufficient_credits") || has("source run refused") {
        return reason::INSUFFICIENT_CREDITS;
    }
    // plugin_sdk::ai::gateway_failure on a 402 from the team's Gateway.
    if has("gateway budget is exhausted") {
        return reason::GATEWAY_BUDGET_EXHAUSTED;
    }
    // ai_target: `404 no_gateway` and `403 token_required` from the control plane.
    if has("has no gateway in the lab")
        || has("(no_gateway)")
        || has("(token_required)")
        || has("serves no lab gateway target without")
    {
        return reason::NO_GATEWAY;
    }
    if plugin == Some(INDEXER_PLUGIN) || has("meilisearch") || has(INDEXER_PLUGIN) {
        if has("authentication failed")
            || has("check the api key")
            || has("http 401")
            || has("http 403")
        {
            return reason::DESTINATION_AUTH;
        }
        if has("network error talking to meilisearch")
            || has("timed out waiting for meilisearch")
            || has("unexpected http 5")
        {
            return reason::DESTINATION_UNREACHABLE;
        }
        if (has("meilisearch task") && has("failed"))
            || has("meilisearch returned [")
            || has("auto_create_index is false")
            || has("cannot be sent in any batch")
        {
            return reason::DESTINATION_REJECTED;
        }
        // The pipeline names a connection the tenant does not have.
        if has("connection ") && has(" not found") {
            return reason::INVALID_CONFIG;
        }
    }
    reason::INTERNAL
}

/// Why a scheduled source run failed, as a Lab `reason` code, from the run's error
/// (`source_runs.error`).
///
/// A fetch the source refused names its HTTP status (`fetch rejected: GET …: status
/// 404 Not Found`): 401/403 is `source_auth`, 404/410 `source_not_found`, 402
/// `insufficient_credits`. A fetch or DNS failure that outlived its retries is
/// `source_unreachable`. A run whose jobs failed (`job … failed: …`, `N of M jobs
/// failed; first: …`) takes the reason of the (first) failed job when
/// [`failure_reason`] finds one. Anything else is `internal`.
pub fn source_failure_reason(message: &str) -> &'static str {
    let m = message.to_lowercase();
    if m.contains("no lab credits") || m.contains("source run refused") {
        return reason::INSUFFICIENT_CREDITS;
    }
    if let Some(at) = m.find("fetch rejected: ") {
        let status = m[at..].rfind("status ").and_then(|i| {
            let start = at + i + "status ".len();
            m.get(start..start + 3)?.parse::<u16>().ok()
        });
        return match status {
            Some(401 | 403) => reason::SOURCE_AUTH,
            Some(404 | 410) => reason::SOURCE_NOT_FOUND,
            Some(402) => reason::INSUFFICIENT_CREDITS,
            _ => reason::INTERNAL,
        };
    }
    if m.contains("fetch: ") || m.contains("dns: ") {
        return reason::SOURCE_UNREACHABLE;
    }
    if m.starts_with("job ") || m.contains(" jobs failed; first: ") {
        return failure_reason(None, message);
    }
    reason::INTERNAL
}

/// A scheduled source changed status: what [`lab_source_status_event`] reports.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceStatusChange {
    /// The source id.
    pub source_id: Uuid,
    /// The source's name.
    pub source_name: String,
    /// The source's tenant: the Lab account. Empty for a global source.
    pub tenant_id: String,
    /// The pipeline the source runs.
    pub pipeline_uid: String,
    /// The run that changed the status.
    pub run_id: Uuid,
    /// The new status.
    pub status: SourceStatus,
    /// The run's error, when it failed.
    pub error: Option<String>,
    /// Failed runs in a row, this one included; 0 when ok.
    pub consecutive_failures: u64,
    /// When the run finished.
    pub occurred_at: DateTime<Utc>,
}

/// The deterministic id of a source's status change: one per run, so a retried
/// recording of the run reports it once.
pub fn lab_source_status_event_id(source_id: Uuid, run_id: Uuid) -> Uuid {
    Uuid::new_v5(
        &GLUTONY_LAB_NAMESPACE,
        format!("source:{source_id}:run:{run_id}:status").as_bytes(),
    )
}

/// Build a source's `source.status_changed` event, or say why there is none (the same
/// account rules as a job's events).
pub fn lab_source_status_event(change: &SourceStatusChange) -> Result<LabEvent, SkipReason> {
    let failing = change.status == SourceStatus::Failing;
    let error = change
        .error
        .as_deref()
        .and_then(non_blank)
        .filter(|_| failing);
    let data = SourceStatusData {
        source_id: change.source_id.to_string(),
        source_name: non_blank(&change.source_name),
        status: change.status,
        run_id: Some(change.run_id.to_string()),
        pipeline: non_blank(&change.pipeline_uid),
        reason: failing.then(|| source_failure_reason(error.as_deref().unwrap_or("")).to_string()),
        error_message: error,
        consecutive_failures: Some(if failing {
            change.consecutive_failures
        } else {
            0
        }),
    };
    envelope_at(
        &change.tenant_id,
        change.occurred_at,
        lab_source_status_event_id(change.source_id, change.run_id),
        "source.status_changed",
        LabEventData::SourceStatus(data),
    )
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
            reason: None,
            group: None,
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
        // One unpriced call: the priced part is still billed, and the gap is flagged.
        assert_eq!(v["data"]["provider_cost_micro_usd"], 210);
        assert!(
            v["data"]["description"]
                .as_str()
                .unwrap()
                .contains("provider cost incomplete")
        );
        let units = &v["data"]["units"];
        assert_eq!(
            units["documents"], 12,
            "a succeeded job without an indexer: the final step's documents"
        );
        assert_eq!(units["bytes_in"], 5_000, "the job's payload size");
        assert_eq!(units["step_seconds"], 4, "3.3 s of steps, rounded up");
        assert_eq!(units["llm_tokens_in"], 1_000);
        assert_eq!(units["llm_tokens_out"], 100);
        assert_eq!(units["audio_seconds"], 3, "2.4 s, rounded up");
        assert_eq!(units["ocr_pages"], 0, "no ocr step");
        // Extra units: stored by the Lab, ignored by pricing (spec §4.3).
        assert_eq!(units["pages"], 9);
        assert_eq!(units["llm_requests"], 1);
        assert_eq!(units["unpriced_provider_calls"], 1);
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
        assert_eq!(
            data.units.get("unpriced_provider_calls"),
            Some(&0),
            "always present, 0 when the cost is complete"
        );
    }

    fn ocr_units(steps: Vec<StepResult>) -> serde_json::Value {
        let mut i = input();
        i.steps = steps;
        let v = serde_json::to_value(lab_event_for_job(&i).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        v["data"]["units"].clone()
    }

    fn plugin_step(plugin: &str, docs: usize, usage: UsageUnits) -> StepResult {
        StepResult {
            plugin: plugin.into(),
            ..step(plugin, docs, 1, usage)
        }
    }

    #[test]
    fn an_ocr_step_reports_one_page_per_output_document() {
        let units = ocr_units(vec![
            plugin_step("pdf_extractor", 1, UsageUnits::pages(4)),
            plugin_step("ocr", 4, UsageUnits::none()),
        ]);
        assert_eq!(units["ocr_pages"], 4);
    }

    #[test]
    fn an_ocr_step_that_reports_pages_is_trusted() {
        let units = ocr_units(vec![plugin_step("ocr", 2, UsageUnits::pages(7))]);
        assert_eq!(units["ocr_pages"], 7);
    }

    #[test]
    fn ocr_steps_are_matched_by_plugin_not_step_id() {
        let scan = StepResult {
            step_id: "scan".into(),
            ..plugin_step("ocr", 5, UsageUnits::none())
        };
        let named_ocr = StepResult {
            step_id: "ocr".into(),
            ..plugin_step("chunker", 30, UsageUnits::none())
        };
        let units = ocr_units(vec![scan, named_ocr]);
        assert_eq!(units["ocr_pages"], 5, "only the step whose plugin is ocr");
    }

    #[test]
    fn several_ocr_steps_are_summed() {
        let units = ocr_units(vec![
            plugin_step("ocr", 3, UsageUnits::none()),
            plugin_step("ocr", 1, UsageUnits::pages(6)),
        ]);
        assert_eq!(units["ocr_pages"], 9, "3 documents + 6 reported pages");
    }

    #[test]
    fn pdf_extractor_pages_are_not_ocr_pages() {
        let units = ocr_units(vec![
            plugin_step("pdf_extractor", 3, UsageUnits::pages(9)),
            plugin_step("chunker", 12, UsageUnits::none()),
        ]);
        assert_eq!(units["ocr_pages"], 0, "no ocr step");
        assert_eq!(units["pages"], 9, "extracted pages stay an extra unit");
    }

    #[test]
    fn the_priced_part_of_an_incomplete_cost_is_still_billed() {
        let mut i = input();
        i.steps = vec![
            step(
                "enrich",
                2,
                1,
                UsageUnits {
                    cost_micro_usd: 210,
                    ..UsageUnits::llm(1_000, 100)
                },
            ),
            step(
                "caption",
                2,
                1,
                UsageUnits {
                    unpriced_calls: 1,
                    ..UsageUnits::llm(50, 5)
                },
            ),
        ];
        let v = serde_json::to_value(lab_event_for_job(&i).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["data"]["provider_cost_micro_usd"], 210);
        assert!(
            v["data"]["description"]
                .as_str()
                .unwrap()
                .contains(", provider cost incomplete")
        );
        assert_eq!(v["data"]["units"]["unpriced_provider_calls"], 1);
        assert_eq!(unpriced_provider_calls(&i), 1);
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
        assert_eq!(
            v["data"]["index_uid"], "documents",
            "the index, not the pipeline"
        );
        assert_eq!(v["data"]["pipeline"], "builtin.pdf");
        assert!(v["data"].get("group").is_none(), "a job of its own");
        assert_eq!(v["data"]["documents_indexed"], 12);
        assert_eq!(v["data"]["pages_crawled"], 0);
        assert_eq!(v["data"]["duration_secs"], 9);
        assert_eq!(v["data"]["job_id"], "11111111-2222-3333-4444-555555555555");
    }

    #[test]
    fn a_job_cancelled_before_indexing_bills_no_documents() {
        let mut cancelled = input();
        cancelled.status = JobStatus::Cancelled;
        let usage = lab_event_for_job(&cancelled).unwrap();
        let LabEventData::Usage(data) = &usage.data else {
            panic!("usage.recorded carries usage data");
        };
        assert_eq!(data.units["documents"], 0);
        assert!(
            data.description.contains(", 0 documents"),
            "{}",
            data.description
        );
    }

    #[test]
    fn documents_come_from_the_indexer_step() {
        let mut job = input();
        let mut index = step("index", 5, 300, UsageUnits::none());
        index.plugin = meili_ingest_plugin_sdk::INDEXER_PLUGIN.into();
        job.steps.push(index);
        job.steps.push(step("notify", 9, 10, UsageUnits::none()));
        let units = lab_units(&job, &totals(&job));
        assert_eq!(units["documents"], 5);
        let v = serde_json::to_value(lab_job_event_for_job(&job).unwrap()).unwrap();
        assert_eq!(v["data"]["documents_indexed"], 5);
    }

    #[test]
    fn a_failed_job_names_its_step_reason_index_and_pipeline() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        failed.error = Some("step enrich (p) failed: invalid input: not an image".into());
        failed.steps[1].status = JobStatus::Failed;
        failed.steps[1].error = Some("invalid input: not an image".into());
        let v = serde_json::to_value(lab_job_event_for_job(&failed).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "job.failed");
        assert_eq!(v["id"], lab_job_event_id(failed.job_id).to_string());
        let d = &v["data"];
        assert_eq!(
            d["error_message"],
            "step enrich (p) failed: invalid input: not an image"
        );
        assert_eq!(d["pages_crawled"], 0);
        assert_eq!(d["reason"], "invalid_input");
        assert_eq!(d["step"], "enrich");
        assert_eq!(d["index_uid"], "documents");
        assert_eq!(d["pipeline"], "builtin.pdf");
    }

    #[test]
    fn a_failed_job_without_a_failed_step_still_has_a_reason() {
        let mut failed = input();
        failed.status = JobStatus::Failed;
        failed.index_name = String::new();
        let v = serde_json::to_value(lab_job_event_for_job(&failed).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["data"]["error_message"], "failed");
        assert_eq!(v["data"]["reason"], "internal");
        assert!(v["data"].get("step").is_none());
        assert!(v["data"].get("index_uid").is_none(), "no index known");
    }

    #[test]
    fn an_invalid_pipeline_fails_with_invalid_config() {
        let mut invalid = input();
        invalid.status = JobStatus::Failed;
        invalid.steps.clear();
        invalid.error = Some("invalid pipeline acme: cycle between a and b".into());
        invalid.reason = Some(reason::INVALID_CONFIG.into());
        let v = serde_json::to_value(lab_job_event_for_job(&invalid).unwrap()).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "job.failed");
        assert_eq!(v["data"]["reason"], "invalid_config");
        // Even without the explicit reason, the message says so.
        invalid.reason = None;
        let v = serde_json::to_value(lab_job_event_for_job(&invalid).unwrap()).unwrap();
        assert_eq!(v["data"]["reason"], "invalid_config");
    }

    #[test]
    fn a_cancelled_job_emits_job_cancelled_with_the_lifecycle_id() {
        let mut cancelled = input();
        cancelled.status = JobStatus::Cancelled;
        let event = lab_job_event_for_job(&cancelled).unwrap();
        assert_eq!(event.id, lab_job_event_id(cancelled.job_id));
        let v = serde_json::to_value(&event).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "job.cancelled");
        assert_eq!(v["data"]["job_id"], "11111111-2222-3333-4444-555555555555");
        assert_eq!(v["data"]["index_uid"], "documents");
        assert_eq!(v["data"]["pipeline"], "builtin.pdf");
        assert_eq!(v["data"]["documents_indexed"], 0, "no indexer ran");
        assert!(v["data"].get("error_message").is_none());
        let mut bad = v.clone();
        bad["data"]["error_message"] = "cancelled".into();
        assert!(
            !validator().is_valid(&bad),
            "job.cancelled carries no message"
        );
    }

    #[test]
    fn every_job_event_carries_its_group() {
        let groups = [
            JobGroup::source_run(
                "99999999-2222-3333-4444-555555555555".parse().unwrap(),
                "nightly export",
            ),
            JobGroup::archive("88888888-2222-3333-4444-555555555555".parse().unwrap()),
        ];
        for group in groups {
            for status in [
                JobStatus::Succeeded,
                JobStatus::Failed,
                JobStatus::Cancelled,
            ] {
                let mut job = input();
                job.status = status;
                job.group = Some(group.clone());
                let v = serde_json::to_value(lab_job_event_for_job(&job).unwrap()).unwrap();
                assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
                assert_eq!(
                    v["data"]["group"],
                    serde_json::to_value(&group).unwrap(),
                    "{status:?}"
                );
            }
        }
        let mut job = input();
        job.group = Some(JobGroup::source_run(Uuid::nil(), "tmdb"));
        let v = serde_json::to_value(lab_job_event_for_job(&job).unwrap()).unwrap();
        assert_eq!(
            v["data"]["group"],
            serde_json::json!({"kind": "source_run", "id": Uuid::nil().to_string(), "name": "tmdb"})
        );
        job.group = Some(JobGroup::archive(Uuid::nil()));
        let v = serde_json::to_value(lab_job_event_for_job(&job).unwrap()).unwrap();
        assert_eq!(
            v["data"]["group"],
            serde_json::json!({"kind": "archive", "id": Uuid::nil().to_string()})
        );
    }

    #[test]
    fn job_failures_map_to_reason_codes() {
        let cases: &[(Option<&str>, &str, &str)] = &[
            (
                None,
                "invalid config: chunker: max_tokens must be > 0",
                "invalid_config",
            ),
            (
                None,
                "invalid pipeline acme: unknown plugin x",
                "invalid_config",
            ),
            (
                Some("pdf_extractor"),
                "invalid input: not a PDF",
                "invalid_input",
            ),
            (
                None,
                "3 of 4 branches failed; first error: invalid input: bad utf-8",
                "invalid_input",
            ),
            (
                None,
                "non-retryable: account 0192 has no Lab credits left; source run refused",
                "insufficient_credits",
            ),
            (
                Some("llm_enricher"),
                "non-retryable: the team's Gateway budget is exhausted: add Lab credits (HTTP 402): no credits",
                "gateway_budget_exhausted",
            ),
            (
                Some("llm_enricher"),
                "non-retryable: this team has no Gateway in the Lab: account has no Gateway (no_gateway)",
                "no_gateway",
            ),
            (
                Some("image_captioner"),
                "non-retryable: the control plane serves no Lab Gateway target without CONTROL_PLANE_TOKEN (HTTP 403)",
                "no_gateway",
            ),
            (
                Some(INDEXER_PLUGIN),
                "non-retryable: meili_indexer: add documents: unexpected HTTP 403 from Meilisearch (no body); authentication failed",
                "destination_auth",
            ),
            (
                Some(INDEXER_PLUGIN),
                "non-retryable: meili_indexer: get index: Meilisearch returned [InvalidApiKey] The provided API key is invalid. (check the API key injected by the gateway)",
                "destination_auth",
            ),
            (
                Some(INDEXER_PLUGIN),
                "retryable: meili_indexer: add documents: network error talking to Meilisearch: connection refused",
                "destination_unreachable",
            ),
            (
                Some(INDEXER_PLUGIN),
                "retryable: meili_indexer: get index: timed out waiting for Meilisearch",
                "destination_unreachable",
            ),
            (
                Some(INDEXER_PLUGIN),
                "non-retryable: meili_indexer: Meilisearch task 12 (add documents) failed: [InvalidDocumentId] bad id",
                "destination_rejected",
            ),
            (
                Some(INDEXER_PLUGIN),
                "non-retryable: meili_indexer: index \"docs\" does not exist and auto_create_index is false; create it first",
                "destination_rejected",
            ),
            (
                None,
                "step index (meili_indexer) failed: non-retryable: meili_indexer: Meilisearch task 3 (settings) failed: [InvalidSettings] x",
                "destination_rejected",
            ),
            (
                Some(INDEXER_PLUGIN),
                "non-retryable: connection \"prod\" not found (tenant_id=Some(\"t\")); a tenant job may only use its own connections",
                "invalid_config",
            ),
            (
                Some("pdf_extractor"),
                "non-retryable: pdf parser crashed",
                "internal",
            ),
            (None, "", "internal"),
        ];
        for (plugin, message, expected) in cases {
            assert_eq!(
                failure_reason(*plugin, message),
                *expected,
                "{plugin:?}: {message}"
            );
        }
    }

    #[test]
    fn source_failures_map_to_reason_codes() {
        let cases = [
            (
                "non-retryable: fetch rejected: GET https://x/a: status 401 Unauthorized",
                "source_auth",
            ),
            (
                "non-retryable: fetch rejected: GET https://x/a: status 403 Forbidden",
                "source_auth",
            ),
            (
                "non-retryable: fetch rejected: GET https://x/a: status 404 Not Found",
                "source_not_found",
            ),
            (
                "non-retryable: fetch rejected: GET https://x/a: status 410 Gone",
                "source_not_found",
            ),
            (
                "non-retryable: fetch rejected: GET https://x/a: status 402 Payment Required",
                "insufficient_credits",
            ),
            (
                "non-retryable: fetch rejected: GET https://x/a: status 418 I'm a teapot",
                "internal",
            ),
            (
                "retryable: fetch: GET https://x/a: status 503 Service Unavailable",
                "source_unreachable",
            ),
            (
                "retryable: fetch: GET https://x/a: error sending request",
                "source_unreachable",
            ),
            (
                "retryable: fetch: reading https://x/a: no data received for 30 s",
                "source_unreachable",
            ),
            (
                "retryable: dns: no address for x.invalid",
                "source_unreachable",
            ),
            (
                "non-retryable: source run refused: account a has no Lab credits left; top up",
                "insufficient_credits",
            ),
            (
                "non-retryable: account a has no Lab credits left; source run refused",
                "insufficient_credits",
            ),
            (
                "job 1 failed: step index (meili_indexer) failed: non-retryable: meili_indexer: add documents: unexpected HTTP 401 from Meilisearch (x); authentication failed",
                "destination_auth",
            ),
            (
                "2 of 3 jobs failed; first: job 1 failed: step parse (pdf_extractor) failed: invalid input: not a PDF (see the jobs for the others)",
                "invalid_input",
            ),
            (
                "2 of 3 jobs failed; first: job 1 ended cancelled",
                "internal",
            ),
            (
                "non-retryable: blocked url: 10.0.0.1 is not allowed",
                "internal",
            ),
            ("", "internal"),
        ];
        for (message, expected) in cases {
            assert_eq!(source_failure_reason(message), expected, "{message}");
        }
    }

    fn status_change(status: SourceStatus) -> SourceStatusChange {
        SourceStatusChange {
            source_id: "aaaaaaaa-2222-3333-4444-555555555555".parse().unwrap(),
            source_name: "nightly export".into(),
            tenant_id: ACCOUNT.into(),
            pipeline_uid: "movies".into(),
            run_id: "bbbbbbbb-2222-3333-4444-555555555555".parse().unwrap(),
            status,
            error: Some(
                "non-retryable: fetch rejected: GET https://x/a: status 404 Not Found".into(),
            ),
            consecutive_failures: 3,
            occurred_at: Utc.with_ymd_and_hms(2026, 10, 1, 10, 0, 9).unwrap(),
        }
    }

    #[test]
    fn a_failing_source_emits_a_valid_status_change() {
        let change = status_change(SourceStatus::Failing);
        let event = lab_source_status_event(&change).unwrap();
        assert_eq!(
            event.id,
            lab_source_status_event_id(change.source_id, change.run_id)
        );
        assert_eq!(event.id.get_version_num(), 5);
        assert_eq!(
            event.id,
            Uuid::new_v5(
                &GLUTONY_LAB_NAMESPACE,
                b"source:aaaaaaaa-2222-3333-4444-555555555555:run:bbbbbbbb-2222-3333-4444-555555555555:status"
            )
        );
        let v = serde_json::to_value(&event).unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["type"], "source.status_changed");
        assert_eq!(v["product"], "glutony");
        assert_eq!(v["account_id"], ACCOUNT);
        assert_eq!(v["occurred_at"], "2026-10-01T10:00:09.000Z");
        assert_eq!(
            v["data"],
            serde_json::json!({
                "source_id": "aaaaaaaa-2222-3333-4444-555555555555",
                "source_name": "nightly export",
                "status": "failing",
                "run_id": "bbbbbbbb-2222-3333-4444-555555555555",
                "pipeline": "movies",
                "reason": "source_not_found",
                "error_message": "non-retryable: fetch rejected: GET https://x/a: status 404 Not Found",
                "consecutive_failures": 3,
            })
        );
    }

    #[test]
    fn a_recovered_source_emits_ok_without_the_error() {
        let v = serde_json::to_value(
            lab_source_status_event(&status_change(SourceStatus::Ok)).unwrap(),
        )
        .unwrap();
        assert!(errors_of(&v).is_empty(), "{:?}\n{v:#}", errors_of(&v));
        assert_eq!(v["data"]["status"], "ok");
        assert_eq!(v["data"]["consecutive_failures"], 0);
        assert!(v["data"].get("reason").is_none());
        assert!(v["data"].get("error_message").is_none());
    }

    #[test]
    fn source_events_follow_the_account_rules() {
        let mut global = status_change(SourceStatus::Failing);
        global.tenant_id = String::new();
        assert_eq!(lab_source_status_event(&global), Err(SkipReason::NoTenant));
        let mut cloud = status_change(SourceStatus::Failing);
        cloud.tenant_id = "hackersearch".into();
        assert_eq!(lab_source_status_event(&cloud), Err(SkipReason::NotAUuid));
    }

    #[test]
    fn a_source_event_from_another_product_is_refused() {
        let mut v = serde_json::to_value(
            lab_source_status_event(&status_change(SourceStatus::Ok)).unwrap(),
        )
        .unwrap();
        v["product"] = "scrapix".into();
        assert!(!validator().is_valid(&v));
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
