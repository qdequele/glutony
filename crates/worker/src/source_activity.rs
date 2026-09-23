//! Activities of `SourceRunWorkflow` (spec *`SourceRunWorkflow`*).
//!
//! All of a run's I/O happens here so the workflow stays deterministic. One rule shapes
//! the split: **the fetch credential never crosses into the workflow.** An activity's
//! input and output are recorded in Temporal history, so [`SourceActivities::resolve_source`]
//! loads the source (credential still sealed), opens the credential, fetches, stages the
//! bytes into the blob store and returns only references — never the credential, and
//! never the bytes.

use std::sync::Arc;

use chrono::{DateTime, Utc};
use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{
    Blob, JobStatus, MeiliContext, PipelineDefinition, PipelineWorkflowInput, PluginError,
};
use meili_ingest_router::mime_to_default_index;
use meili_ingest_source::{
    FetchAuth, HostPolicy, IncrementalState, Location, Resolution, ResolveRuntime, RunOutcome,
    SecretKey, SourceConnector, SourceDefinition, SourceError, UrlConnector, open_json,
};
use serde::{Deserialize, Serialize};
use temporalio_macros::activities;
use temporalio_sdk::activities::{ActivityContext, ActivityError};
use url::Url;
use uuid::Uuid;

use crate::activity::to_activity_error;

/// Shared state of the source-run activities.
pub struct SourceActivities {
    /// HTTP client for the control plane.
    pub http: reqwest::Client,
    /// Control plane base URL, trailing slash trimmed.
    pub control_plane_url: Option<String>,
    /// Where fetched items are staged.
    pub blob: BlobStore,
    /// Opens sealed fetch credentials. `None` when `SOURCE_SECRET_KEY` is unset.
    pub key: Option<Arc<SecretKey>>,
    /// `SOURCE_FETCH_HOSTS`, re-applied at every fetch and every redirect.
    pub fetch_policy: HostPolicy,
    /// Index used when nothing more specific resolves (mirrors the gateway's
    /// `DEFAULT_INDEX`).
    pub default_index: String,
}

impl SourceActivities {
    /// Activities talking to `control_plane_url`.
    pub fn new(control_plane_url: Option<String>, blob: BlobStore) -> Self {
        Self {
            http: reqwest::Client::new(),
            control_plane_url: control_plane_url.map(|u| u.trim_end_matches('/').to_string()),
            blob,
            key: None,
            fetch_policy: HostPolicy::default(),
            default_index: "documents".into(),
        }
    }

    /// Set the key that opens fetch credentials and the fetch host policy.
    pub fn with_security(mut self, key: Option<Arc<SecretKey>>, policy: HostPolicy) -> Self {
        self.key = key;
        self.fetch_policy = policy;
        self
    }

    /// Set the fallback index.
    pub fn with_default_index(mut self, index: impl Into<String>) -> Self {
        self.default_index = index.into();
        self
    }

    fn base(&self) -> Result<&str, PluginError> {
        self.control_plane_url.as_deref().ok_or_else(|| {
            PluginError::NonRetryable("no control plane is configured on this worker".into())
        })
    }

    fn url(&self, segments: &[&str]) -> Result<Url, PluginError> {
        let mut url = Url::parse(&format!("{}/", self.base()?))
            .map_err(|e| PluginError::NonRetryable(format!("control plane url: {e}")))?;
        {
            let mut path = url.path_segments_mut().map_err(|()| {
                PluginError::NonRetryable("control plane url cannot be a base".into())
            })?;
            path.pop_if_empty();
            for s in segments {
                path.push(s);
            }
        }
        Ok(url)
    }
}

/// Input of [`SourceActivities::resolve_source`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveSourceInput {
    /// The source to run.
    pub source_id: Uuid,
    /// The run, so job rows can be traced back to it.
    pub run_id: Uuid,
    /// When this run was scheduled for; URL templates render against it (Decision 9).
    pub scheduled_at: DateTime<Utc>,
}

/// One pipeline job a run will start.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SourceJob {
    /// Everything the child `PipelineWorkflow` needs. `input` is always a staged
    /// reference and `context` carries no destination — the pipeline's connection is
    /// the destination (Decision 13).
    pub workflow_input: PipelineWorkflowInput,
}

/// Output of [`SourceActivities::resolve_source`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum ResolveSourceOutput {
    /// Upstream is unchanged: no job, nothing billed.
    Unchanged,
    /// Jobs to start, and the state to persist once they succeed.
    Ready {
        /// One job per fetched item.
        jobs: Vec<SourceJob>,
        /// What to save as the source's incremental state.
        state: IncrementalState,
    },
}

/// Input of [`SourceActivities::record_source_run`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordRunInput {
    /// Run id.
    pub run_id: Uuid,
    /// Source.
    pub source_id: Uuid,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When it finished.
    pub finished_at: DateTime<Utc>,
    /// Outcome.
    pub outcome: RunOutcome,
    /// Items ingested.
    pub items: i32,
    /// Jobs started.
    pub job_ids: Vec<Uuid>,
    /// Failure message.
    pub error: Option<String>,
    /// Incremental state to persist. Only set when every job succeeded, so a failed
    /// run is retried on the next tick instead of being mistaken for "unchanged".
    pub state: Option<IncrementalState>,
}

/// The fields of `GET /internal/sources-by-id/{id}` this worker reads.
#[derive(Debug, Deserialize)]
struct SourceRow {
    #[serde(flatten)]
    definition: SourceDefinition,
    #[serde(default)]
    fetch_auth: Option<Vec<u8>>,
    #[serde(default)]
    state: IncrementalState,
}

/// Map a connector error onto retry semantics: what retrying cannot fix fails now.
fn from_source_error(e: SourceError) -> PluginError {
    match e {
        SourceError::Fetch(m) | SourceError::Dns(m) => PluginError::Retryable(m),
        other => PluginError::NonRetryable(other.to_string()),
    }
}

/// The index a source's item lands in, in the gateway's order with the source's own
/// `index` in the request's place: pipeline `index_pattern`, then the source's index,
/// then the MIME default, then the deployment default. A `meili_indexer` step that pins
/// its own `index` still wins over all of these when the workflow injects the context.
pub fn resolve_index(
    pipeline: &PipelineDefinition,
    source_index: Option<&str>,
    mime: &str,
    default_index: &str,
) -> String {
    fn non_blank(s: Option<&str>) -> Option<&str> {
        s.map(str::trim).filter(|s| !s.is_empty())
    }
    if let Some(p) = non_blank(
        pipeline
            .trigger
            .as_ref()
            .and_then(|t| t.index_pattern.as_deref()),
    ) {
        return p.to_string();
    }
    if let Some(i) = non_blank(source_index) {
        return i.to_string();
    }
    match mime_to_default_index(mime) {
        "documents" => default_index.to_string(),
        other => other.to_string(),
    }
}

impl SourceActivities {
    async fn get_json<T: serde::de::DeserializeOwned>(
        &self,
        url: Url,
        what: &str,
    ) -> Result<Option<T>, PluginError> {
        let resp = self
            .http
            .get(url)
            .send()
            .await
            .map_err(|e| PluginError::Retryable(format!("control plane unreachable: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !resp.status().is_success() {
            return Err(PluginError::Retryable(format!(
                "control plane returned {} for {what}",
                resp.status()
            )));
        }
        resp.json::<T>()
            .await
            .map(Some)
            .map_err(|e| PluginError::NonRetryable(format!("{what}: unreadable response: {e}")))
    }

    async fn send_json(&self, req: reqwest::RequestBuilder, what: &str) -> Result<(), PluginError> {
        if self.send_json_unless_gone(req, what).await? {
            Ok(())
        } else {
            Err(PluginError::Retryable(format!(
                "control plane returned 404 Not Found for {what}"
            )))
        }
    }

    /// Like [`Self::send_json`], but a 404 is `Ok(false)` instead of an error: the
    /// thing written about no longer exists, and retrying will not bring it back.
    async fn send_json_unless_gone(
        &self,
        req: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<bool, PluginError> {
        let resp = req
            .send()
            .await
            .map_err(|e| PluginError::Retryable(format!("control plane unreachable: {e}")))?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(false);
        }
        if resp.status().is_success() {
            Ok(true)
        } else {
            Err(PluginError::Retryable(format!(
                "control plane returned {} for {what}",
                resp.status()
            )))
        }
    }

    /// Load, fetch and stage one run. See the module docs for why this is one activity.
    pub async fn resolve(
        &self,
        input: &ResolveSourceInput,
    ) -> Result<ResolveSourceOutput, PluginError> {
        let source_id = input.source_id;
        let row: SourceRow = self
            .get_json(
                self.url(&["internal", "sources-by-id", &source_id.to_string()])?,
                "load source",
            )
            .await?
            .ok_or_else(|| {
                PluginError::NonRetryable(format!(
                    "source {source_id} no longer exists or was archived"
                ))
            })?;
        let d = row.definition;

        // The pipeline as it is now, so an edit takes effect on the next tick.
        let mut pipeline_url = self.url(&["pipelines", &d.pipeline_uid])?;
        if let Some(p) = &d.project_id {
            pipeline_url.query_pairs_mut().append_pair("project_id", p);
        }
        let pipeline: PipelineDefinition = self
            .get_json(pipeline_url, "load pipeline")
            .await?
            .ok_or_else(|| {
                PluginError::NonRetryable(format!(
                    "source {:?}: its pipeline {:?} no longer exists",
                    d.uid, d.pipeline_uid
                ))
            })?;
        if !pipeline.pins_destination() {
            return Err(PluginError::NonRetryable(format!(
                "source {:?}: pipeline {:?} no longer names a Meilisearch connection on its \
                 meili_indexer step, and a scheduled run has no request to supply one",
                d.uid, d.pipeline_uid
            )));
        }

        // Opened here and dropped at the end of this activity: never returned.
        let auth: Option<FetchAuth> = match &row.fetch_auth {
            None => None,
            Some(sealed) => {
                let key = self.key.as_deref().ok_or_else(|| {
                    PluginError::NonRetryable(format!(
                        "source {:?} has a credential but SOURCE_SECRET_KEY is not set on \
                         this worker",
                        d.uid
                    ))
                })?;
                Some(open_json(key, sealed).map_err(|_| {
                    PluginError::NonRetryable(format!(
                        "source {:?}: its credential could not be decrypted (is \
                         SOURCE_SECRET_KEY the one it was saved with?)",
                        d.uid
                    ))
                })?)
            }
        };

        let rt = ResolveRuntime::new(
            self.fetch_policy.clone(),
            input.scheduled_at,
            d.timezone.clone(),
        );
        let connector = match &d.location {
            Location::Url { .. } => UrlConnector,
        };
        let resolution = connector
            .resolve(&d.location, auth.as_ref(), &row.state, &rt)
            .await
            .map_err(from_source_error)?;

        let (items, state) = match resolution {
            Resolution::Unchanged => return Ok(ResolveSourceOutput::Unchanged),
            Resolution::Items { items, state } => (items, state),
        };

        let mut jobs = Vec::with_capacity(items.len());
        for item in items {
            let job_id = Uuid::new_v4();
            let index = resolve_index(
                &pipeline,
                d.index_name.as_deref(),
                &item.mime,
                &self.default_index,
            );
            // Always staged: bytes must never enter workflow history.
            let staged = self
                .blob
                .stage_upload(job_id, Blob::new(item.bytes, item.mime, item.filename), 0)
                .await
                .map_err(|e| PluginError::Retryable(format!("staging a fetched item: {e}")))?;
            let context = MeiliContext {
                project_id: d.project_id.clone(),
                index: Some(index),
                ..Default::default()
            };
            self.record_job_row(job_id, &pipeline, &context, source_id)
                .await;
            jobs.push(SourceJob {
                workflow_input: PipelineWorkflowInput {
                    job_id,
                    pipeline: pipeline.clone(),
                    input: staged,
                    context,
                },
            });
        }
        tracing::info!(
            source = %d.uid,
            run_id = %input.run_id,
            jobs = jobs.len(),
            "source fetched"
        );
        Ok(ResolveSourceOutput::Ready { jobs, state })
    }

    /// Create the job row the jobs list shows. Best effort, like the gateway's: the row
    /// is a cache and Temporal is the source of truth.
    async fn record_job_row(
        &self,
        job_id: Uuid,
        pipeline: &PipelineDefinition,
        context: &MeiliContext,
        source_id: Uuid,
    ) {
        let now = Utc::now();
        let body = serde_json::json!({
            "job_id": job_id,
            "workflow_id": PipelineWorkflowInput::workflow_id(job_id),
            "pipeline_uid": pipeline.uid,
            "project_id": context.project_id,
            "index_name": context.index,
            "status": JobStatus::Queued.as_str(),
            "started_at": now,
            "updated_at": now,
            "source_id": source_id,
        });
        let result = match self.url(&["internal", "jobs"]) {
            Ok(url) => {
                self.send_json(self.http.post(url).json(&body), "create job")
                    .await
            }
            Err(e) => Err(e),
        };
        if let Err(e) = result {
            tracing::warn!(job_id = %job_id, "could not record the source's job row: {e}");
        }
    }

    /// Record a finished run and, when set, persist the new incremental state.
    ///
    /// A source deleted while its run was in flight has nowhere to record to: that is
    /// logged and treated as done, rather than retried for minutes against a 404.
    pub async fn record(&self, input: &RecordRunInput) -> Result<(), PluginError> {
        let run = serde_json::json!({
            "run_id": input.run_id,
            "source_id": input.source_id,
            "started_at": input.started_at,
            "finished_at": input.finished_at,
            "outcome": input.outcome,
            "items": input.items,
            "job_ids": input.job_ids,
            "error": input.error,
        });
        let recorded = self
            .send_json_unless_gone(
                self.http
                    .post(self.url(&["internal", "source-runs"])?)
                    .json(&run),
                "record run",
            )
            .await?;
        if !recorded {
            tracing::warn!(
                source_id = %input.source_id,
                run_id = %input.run_id,
                "the source was deleted during its run; the run is not recorded"
            );
            return Ok(());
        }
        if let Some(state) = &input.state {
            self.send_json_unless_gone(
                self.http
                    .put(self.url(&[
                        "internal",
                        "sources-by-id",
                        &input.source_id.to_string(),
                        "state",
                    ])?)
                    .json(state),
                "save state",
            )
            .await?;
        }
        Ok(())
    }
}

#[activities]
impl SourceActivities {
    /// Load, fetch and stage one scheduled run.
    #[activity]
    pub async fn resolve_source(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: ResolveSourceInput,
    ) -> Result<ResolveSourceOutput, ActivityError> {
        self.resolve(&input).await.map_err(to_activity_error)
    }

    /// Record a finished run (and its new state, on success).
    #[activity]
    pub async fn record_source_run(
        self: Arc<Self>,
        _ctx: ActivityContext,
        input: RecordRunInput,
    ) -> Result<(), ActivityError> {
        self.record(&input).await.map_err(to_activity_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::{ContentRef, PipelineTrigger, PluginInput, StepDefinition};
    use meili_ingest_source::seal_json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const TOKEN: &str = "sk-fetch-NEVER-IN-HISTORY";

    fn key() -> Arc<SecretKey> {
        Arc::new(SecretKey::from_bytes([8; 32]))
    }

    fn pipeline(pinned: bool) -> PipelineDefinition {
        let config = if pinned {
            serde_json::json!({ "connection": "prod-movies" })
        } else {
            serde_json::json!({})
        };
        PipelineDefinition {
            uid: "movies".into(),
            name: "movies".into(),
            description: None,
            version: 1,
            trigger: None,
            steps: vec![
                StepDefinition::new("parse", "json_parser"),
                StepDefinition::new("index", "meili_indexer")
                    .depends_on(["parse"])
                    .config(config),
            ],
            builtin: false,
            project_id: Some("tenant-1".into()),
        }
    }

    fn source_row(url: &str, sealed: Option<Vec<u8>>) -> serde_json::Value {
        let mut v = serde_json::json!({
            "id": "11111111-1111-1111-1111-111111111111",
            "uid": "tmdb",
            "name": "tmdb",
            "project_id": "tenant-1",
            "pipeline_uid": "movies",
            "location": { "kind": "url", "url": url },
            "cron": "30 0 * * *",
            "timezone": "UTC",
            "index_name": "movies",
            "schedule_id": "source-x",
        });
        if let Some(s) = sealed {
            v["fetch_auth"] = serde_json::json!(s);
        }
        v
    }

    fn input() -> ResolveSourceInput {
        ResolveSourceInput {
            source_id: Uuid::parse_str("11111111-1111-1111-1111-111111111111").expect("uuid"),
            run_id: Uuid::nil(),
            scheduled_at: chrono::TimeZone::with_ymd_and_hms(&Utc, 2026, 9, 13, 0, 30, 0)
                .single()
                .expect("instant"),
        }
    }

    /// A control plane serving `row` and `pipeline`, and accepting job rows.
    async fn control_plane(row: serde_json::Value, pipeline: PipelineDefinition) -> MockServer {
        let cp = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/internal/sources-by-id/11111111-1111-1111-1111-111111111111",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(row))
            .mount(&cp)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines/movies"))
            .respond_with(ResponseTemplate::new(200).set_body_json(pipeline))
            .mount(&cp)
            .await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&cp)
            .await;
        cp
    }

    fn activities(cp: &MockServer, files: &MockServer) -> SourceActivities {
        let host = files.uri().trim_start_matches("http://").to_string();
        SourceActivities::new(Some(cp.uri()), BlobStore::memory())
            .with_security(Some(key()), HostPolicy::parse(&host).expect("policy"))
    }

    #[tokio::test]
    async fn a_run_fetches_with_the_credential_and_returns_only_staged_refs() {
        let files = MockServer::start().await;
        // The template renders against the SCHEDULED time: 2026-09-13 00:30, minus a day.
        Mock::given(method("GET"))
            .and(path("/movie_ids_09_12_2026.json"))
            .and(header("authorization", format!("Bearer {TOKEN}").as_str()))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_raw("{\"id\":1}\n{\"id\":2}\n", "application/octet-stream"),
            )
            .mount(&files)
            .await;
        let sealed = seal_json(
            &key(),
            &FetchAuth::Bearer {
                token: TOKEN.into(),
            },
        )
        .expect("seal");
        let cp = control_plane(
            source_row(
                &format!("{}/movie_ids_{{{{ date-1d:%m_%d_%Y }}}}.json", files.uri()),
                Some(sealed),
            ),
            pipeline(true),
        )
        .await;

        let out = activities(&cp, &files)
            .resolve(&input())
            .await
            .expect("resolves");

        // What Temporal records for this activity's result.
        let recorded = serde_json::to_string(&out).expect("serialize");
        assert!(
            !recorded.contains(TOKEN),
            "the credential never leaves the activity"
        );
        assert!(
            !recorded.contains("\"id\":2"),
            "the fetched bytes are staged, not returned"
        );

        let ResolveSourceOutput::Ready { jobs, state } = out else {
            panic!("expected jobs");
        };
        assert_eq!(jobs.len(), 1, "one URL is one item");
        let job = &jobs[0].workflow_input;
        assert!(matches!(
            job.input,
            PluginInput::Ref(ContentRef::Staged { .. })
        ));
        assert_eq!(
            job.context.index.as_deref(),
            Some("movies"),
            "the source's index"
        );
        assert_eq!(job.context.project_id.as_deref(), Some("tenant-1"));
        assert!(
            job.context.host.is_none() && job.context.api_key.is_none(),
            "the destination is the pipeline's connection, not the context"
        );
        assert!(state.hash.is_some());
    }

    #[tokio::test]
    async fn an_unchanged_upstream_starts_no_job() {
        let files = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(304))
            .mount(&files)
            .await;
        let mut row = source_row(&format!("{}/feed.json", files.uri()), None);
        // The previous run's ETag, as the control plane returns it.
        row["state"] = serde_json::json!({ "etag": "\"v1\"" });
        let cp = control_plane(row, pipeline(true)).await;

        let out = activities(&cp, &files)
            .resolve(&input())
            .await
            .expect("resolves");
        assert_eq!(out, ResolveSourceOutput::Unchanged);
        let jobs_created = cp
            .received_requests()
            .await
            .expect("recorded")
            .iter()
            .filter(|r| r.url.path() == "/internal/jobs")
            .count();
        assert_eq!(jobs_created, 0, "no job, so nothing is billed");
    }

    #[tokio::test]
    async fn a_pipeline_that_lost_its_connection_fails_the_run_clearly() {
        let files = MockServer::start().await;
        let cp = control_plane(
            source_row(&format!("{}/feed.json", files.uri()), None),
            pipeline(false),
        )
        .await;
        let err = activities(&cp, &files)
            .resolve(&input())
            .await
            .expect_err("unpinned");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("connection")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn an_archived_or_deleted_source_is_non_retryable() {
        let files = MockServer::start().await;
        let cp = MockServer::start().await; // every path 404s
        let err = activities(&cp, &files)
            .resolve(&input())
            .await
            .expect_err("gone");
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_host_the_fetch_policy_forbids_is_non_retryable() {
        let files = MockServer::start().await;
        let cp = control_plane(
            source_row("http://169.254.169.254/latest/meta-data", None),
            pipeline(true),
        )
        .await;
        let err = activities(&cp, &files)
            .resolve(&input())
            .await
            .expect_err("forbidden host");
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err:?}");
    }

    #[tokio::test]
    async fn a_transient_fetch_failure_is_retryable() {
        let files = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&files)
            .await;
        let cp = control_plane(
            source_row(&format!("{}/feed.json", files.uri()), None),
            pipeline(true),
        )
        .await;
        let err = activities(&cp, &files)
            .resolve(&input())
            .await
            .expect_err("503");
        assert!(matches!(err, PluginError::Retryable(_)), "{err:?}");
    }

    #[test]
    fn the_index_follows_the_gateways_order() {
        let mut p = pipeline(true);
        assert_eq!(
            resolve_index(&p, Some("movies"), "application/json", "documents"),
            "movies",
            "the source's own index beats the MIME default"
        );
        // Without one, the MIME default applies; the generic "documents" defers to the
        // deployment default, exactly as the gateway does.
        let pdf = mime_to_default_index("application/pdf");
        let expected = if pdf == "documents" { "fallback" } else { pdf };
        assert_eq!(
            resolve_index(&p, None, "application/pdf", "fallback"),
            expected
        );
        assert_eq!(
            resolve_index(&p, Some("  "), "text/plain", "fallback"),
            "fallback"
        );
        p.trigger = Some(PipelineTrigger {
            content_types: vec![],
            filename_pattern: None,
            index_pattern: Some("from-pattern".into()),
        });
        assert_eq!(
            resolve_index(&p, Some("movies"), "application/json", "documents"),
            "from-pattern",
            "the pipeline's pattern wins, as it does for requests"
        );
    }

    #[tokio::test]
    async fn recording_a_successful_run_saves_its_state_and_a_failed_one_does_not() {
        let cp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/source-runs"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&cp)
            .await;
        Mock::given(method("PUT"))
            .and(path(
                "/internal/sources-by-id/11111111-1111-1111-1111-111111111111/state",
            ))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&cp)
            .await;
        let files = MockServer::start().await;
        let a = activities(&cp, &files);
        let base = RecordRunInput {
            run_id: Uuid::new_v4(),
            source_id: input().source_id,
            started_at: Utc::now(),
            finished_at: Utc::now(),
            outcome: RunOutcome::Ingested,
            items: 1,
            job_ids: vec![Uuid::new_v4()],
            error: None,
            state: Some(IncrementalState {
                etag: Some("\"v2\"".into()),
                ..Default::default()
            }),
        };
        a.record(&base).await.expect("recorded with state");

        let failed = RecordRunInput {
            run_id: Uuid::new_v4(),
            outcome: RunOutcome::Failed,
            error: Some("boom".into()),
            state: None,
            ..base
        };
        a.record(&failed).await.expect("recorded without state");
        // `expect(1)` on the state PUT is verified when `cp` drops.
    }

    #[tokio::test]
    async fn a_run_of_a_source_deleted_meanwhile_is_dropped_not_retried() {
        let cp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/source-runs"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&cp)
            .await;
        Mock::given(method("PUT"))
            .respond_with(ResponseTemplate::new(204))
            .expect(0)
            .mount(&cp)
            .await;
        let files = MockServer::start().await;
        let run = RecordRunInput {
            run_id: Uuid::new_v4(),
            source_id: input().source_id,
            started_at: Utc::now(),
            finished_at: Utc::now(),
            outcome: RunOutcome::Ingested,
            items: 1,
            job_ids: vec![],
            error: None,
            state: Some(IncrementalState::default()),
        };
        activities(&cp, &files)
            .record(&run)
            .await
            .expect("nothing left to record is not an error");
    }
}
