//! Shared application state: configuration, the Temporal workflow starter, the control
//! plane HTTP client and the blob store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{JobStatus, PipelineDefinition, PipelineWorkflowInput, PluginManifest, WorkflowProgress};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use temporalio_client::{
    Client, UntypedQuery, UntypedSignal, UntypedWorkflow, WorkflowCancelOptions, WorkflowDescribeOptions,
    WorkflowExecutionStatus, WorkflowQueryOptions, WorkflowSignalOptions, WorkflowStartOptions,
};
use temporalio_common::data_converters::{
    GenericPayloadConverter, PayloadConverter, RawValue, SerializationContext, SerializationContextData,
};
use uuid::Uuid;

use crate::error::GatewayError;

/// Temporal task queue the `PipelineWorkflow` itself runs on (plan Decision 2).
pub const WORKFLOW_TASK_QUEUE: &str = "workers-general";
/// Temporal workflow type name (plan Decision 2).
pub const WORKFLOW_TYPE: &str = "PipelineWorkflow";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

/// Gateway configuration (SPEC §13 "Gateway" plus `BLOB_STORE_URL` and `INLINE_MAX_BYTES`).
#[derive(Clone)]
pub struct GatewayConfig {
    /// Listen address (`BIND`, default `0.0.0.0:8080`).
    pub bind: String,
    /// Temporal frontend URL (`TEMPORAL_URL`).
    pub temporal_url: String,
    /// Temporal namespace (`TEMPORAL_NAMESPACE`, default `default`).
    pub temporal_namespace: String,
    /// Internal control plane base URL (`CONTROL_PLANE_URL`).
    pub control_plane_url: String,
    /// Standalone fallback Meilisearch host (`MEILI_URL`).
    pub meili_url: Option<String>,
    /// Standalone fallback Meilisearch API key (`MEILI_API_KEY`).
    pub meili_api_key: Option<String>,
    /// Fallback index name (`DEFAULT_INDEX`, default `documents`).
    pub default_index: String,
    /// Maximum upload size in MiB (`MAX_UPLOAD_MB`, default 500).
    pub max_upload_mb: usize,
    /// Shared secret that must be carried in `X-Meili-Envoy-Secret` for the `X-Meili-*`
    /// headers to be honoured (`ENVOY_TRUSTED_HEADER`). `None` = trust everyone (dev mode).
    pub envoy_trusted_header: Option<String>,
    /// Blob store URL (`BLOB_STORE_URL`, default `file://./blobs`).
    pub blob_store_url: String,
    /// Uploads up to this many bytes are inlined into the workflow input; bigger ones are
    /// staged in the blob store (`INLINE_MAX_BYTES`, default 1 MiB).
    pub inline_max_bytes: usize,
}

impl std::fmt::Debug for GatewayConfig {
    /// Redacts secrets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayConfig")
            .field("bind", &self.bind)
            .field("temporal_url", &self.temporal_url)
            .field("temporal_namespace", &self.temporal_namespace)
            .field("control_plane_url", &self.control_plane_url)
            .field("meili_url", &self.meili_url)
            .field("meili_api_key", &self.meili_api_key.as_ref().map(|_| "<redacted>"))
            .field("default_index", &self.default_index)
            .field("max_upload_mb", &self.max_upload_mb)
            .field("envoy_trusted_header", &self.envoy_trusted_header.as_ref().map(|_| "<redacted>"))
            .field("blob_store_url", &self.blob_store_url)
            .field("inline_max_bytes", &self.inline_max_bytes)
            .finish()
    }
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            bind: "0.0.0.0:8080".into(),
            temporal_url: "http://temporal-frontend:7233".into(),
            temporal_namespace: "default".into(),
            control_plane_url: "http://meili-control-plane:9000".into(),
            meili_url: None,
            meili_api_key: None,
            default_index: "documents".into(),
            max_upload_mb: 500,
            envoy_trusted_header: None,
            blob_store_url: "file://./blobs".into(),
            inline_max_bytes: 1_048_576,
        }
    }
}

impl GatewayConfig {
    /// Read the configuration from the environment (the only place env vars are read).
    pub fn from_env() -> anyhow::Result<Self> {
        let d = Self::default();
        Ok(Self {
            bind: env_or("BIND", &d.bind),
            temporal_url: env_or("TEMPORAL_URL", &d.temporal_url),
            temporal_namespace: env_or("TEMPORAL_NAMESPACE", &d.temporal_namespace),
            control_plane_url: env_or("CONTROL_PLANE_URL", &d.control_plane_url),
            meili_url: env_opt("MEILI_URL"),
            meili_api_key: env_opt("MEILI_API_KEY"),
            default_index: env_or("DEFAULT_INDEX", &d.default_index),
            max_upload_mb: env_parse("MAX_UPLOAD_MB", d.max_upload_mb)?,
            envoy_trusted_header: env_opt("ENVOY_TRUSTED_HEADER"),
            blob_store_url: env_or("BLOB_STORE_URL", &d.blob_store_url),
            inline_max_bytes: env_parse("INLINE_MAX_BYTES", d.inline_max_bytes)?,
        })
    }

    /// Maximum request body size in bytes.
    pub fn max_upload_bytes(&self) -> usize {
        self.max_upload_mb.saturating_mul(1024 * 1024)
    }
}

fn env_opt(name: &str) -> Option<String> {
    std::env::var(name).ok().map(|v| v.trim().to_string()).filter(|v| !v.is_empty())
}

fn env_or(name: &str, default: &str) -> String {
    env_opt(name).unwrap_or_else(|| default.to_string())
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match env_opt(name) {
        Some(v) => v.parse::<T>().map_err(|e| anyhow::anyhow!("invalid {name}={v:?}: {e}")),
        None => Ok(default),
    }
}

// ---------------------------------------------------------------------------
// Workflow starter
// ---------------------------------------------------------------------------

/// Identifiers of a started workflow execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StartedWorkflow {
    /// Temporal workflow id (`ingest-<job_id>`).
    pub workflow_id: String,
    /// Run id when the server returned one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub run_id: Option<String>,
}

/// Snapshot of a job as seen from Temporal: the execution status plus the `progress`
/// query result when the workflow answered it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobSnapshot {
    /// Overall status.
    pub status: JobStatus,
    /// Progress as reported by the workflow (`None` when the query failed, e.g. closed
    /// workflow with no worker to answer).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<WorkflowProgress>,
}

/// Abstraction over the Temporal client so handlers can be tested without a server.
#[async_trait]
pub trait WorkflowStarter: Send + Sync {
    /// Start a `PipelineWorkflow` for `input`.
    async fn start(&self, input: &PipelineWorkflowInput) -> Result<StartedWorkflow, GatewayError>;
    /// Describe + `progress`-query the workflow of `job_id`. `Ok(None)` when Temporal does
    /// not know the workflow.
    async fn progress(&self, job_id: Uuid) -> Result<Option<JobSnapshot>, GatewayError>;
    /// Send the `cancel` signal and request cancellation of the workflow of `job_id`.
    async fn cancel(&self, job_id: Uuid) -> Result<(), GatewayError>;
}

/// [`WorkflowStarter`] backed by a real Temporal client (untyped API, plan Decision 3).
#[derive(Clone)]
pub struct TemporalStarter(pub Client);

impl std::fmt::Debug for TemporalStarter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TemporalStarter")
    }
}

/// Decode a [`RawValue`] without panicking (unlike `RawValue::to_value`).
fn decode_raw<T: DeserializeOwned + 'static>(raw: RawValue, conv: &PayloadConverter) -> Result<T, GatewayError> {
    let payload = raw
        .payloads
        .into_iter()
        .next()
        .ok_or_else(|| GatewayError::Internal("empty query result".into()))?;
    let ctx = SerializationContext::new(&SerializationContextData::None, conv);
    conv.from_payload(&ctx, payload)
        .map_err(|e| GatewayError::Internal(format!("cannot decode workflow payload: {e}")))
}

/// Map Temporal's execution status onto a [`JobStatus`].
pub fn map_execution_status(status: WorkflowExecutionStatus) -> JobStatus {
    match status {
        WorkflowExecutionStatus::Completed => JobStatus::Succeeded,
        WorkflowExecutionStatus::Failed | WorkflowExecutionStatus::TimedOut => JobStatus::Failed,
        WorkflowExecutionStatus::Canceled | WorkflowExecutionStatus::Terminated => JobStatus::Cancelled,
        _ => JobStatus::Running,
    }
}

/// Combine the `describe` status with the `progress` query: terminal statuses from the
/// server win; while running, the workflow's own view (queued/running/...) is used.
pub fn combine_status(described: JobStatus, progress: Option<&WorkflowProgress>) -> JobStatus {
    match (described, progress) {
        (JobStatus::Running, Some(p)) => p.status,
        (s, _) => s,
    }
}

#[async_trait]
impl WorkflowStarter for TemporalStarter {
    async fn start(&self, input: &PipelineWorkflowInput) -> Result<StartedWorkflow, GatewayError> {
        let conv = PayloadConverter::default();
        let raw = RawValue::from_value(input, &conv);
        let handle = self
            .0
            .start_workflow(
                UntypedWorkflow::new(WORKFLOW_TYPE),
                raw,
                WorkflowStartOptions::new(WORKFLOW_TASK_QUEUE, PipelineWorkflowInput::workflow_id(input.job_id))
                    .build(),
            )
            .await
            .map_err(|e| GatewayError::Upstream(format!("cannot start workflow: {e}")))?;
        Ok(StartedWorkflow {
            workflow_id: handle.info().workflow_id.clone(),
            run_id: handle.info().run_id.clone(),
        })
    }

    async fn progress(&self, job_id: Uuid) -> Result<Option<JobSnapshot>, GatewayError> {
        use temporalio_client::errors::WorkflowInteractionError;
        let conv = PayloadConverter::default();
        let handle = self
            .0
            .get_workflow_handle::<UntypedWorkflow>(PipelineWorkflowInput::workflow_id(job_id));
        let described = match handle.describe(WorkflowDescribeOptions::default()).await {
            Ok(d) => map_execution_status(d.status()),
            Err(WorkflowInteractionError::NotFound(_)) => return Ok(None),
            Err(e) => return Err(GatewayError::Upstream(format!("cannot describe workflow: {e}"))),
        };
        let progress = match handle
            .query(UntypedQuery::new("progress"), RawValue::from_value(&(), &conv), WorkflowQueryOptions::default())
            .await
        {
            Ok(raw) => match decode_raw::<WorkflowProgress>(raw, &conv) {
                Ok(p) => Some(p),
                Err(e) => {
                    tracing::warn!(job_id = %job_id, "progress query returned an undecodable payload: {e}");
                    None
                }
            },
            Err(e) => {
                // Closed workflows (or ones without a live worker) cannot answer queries.
                tracing::debug!(job_id = %job_id, "progress query failed, using describe only: {e}");
                None
            }
        };
        Ok(Some(JobSnapshot { status: combine_status(described, progress.as_ref()), progress }))
    }

    async fn cancel(&self, job_id: Uuid) -> Result<(), GatewayError> {
        use temporalio_client::errors::WorkflowInteractionError;
        let conv = PayloadConverter::default();
        let handle = self
            .0
            .get_workflow_handle::<UntypedWorkflow>(PipelineWorkflowInput::workflow_id(job_id));
        let map = |e: WorkflowInteractionError| match e {
            WorkflowInteractionError::NotFound(_) => GatewayError::NotFound(format!("job {job_id} not found")),
            other => GatewayError::Upstream(format!("cannot cancel workflow: {other}")),
        };
        handle
            .signal(UntypedSignal::new("cancel"), RawValue::from_value(&(), &conv), WorkflowSignalOptions::default())
            .await
            .map_err(map)?;
        handle.cancel(WorkflowCancelOptions::default()).await.map_err(map)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Control plane client
// ---------------------------------------------------------------------------

/// Row of the control plane's `jobs` table (write-through cache of Temporal state).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    /// Job id.
    pub job_id: Uuid,
    /// Temporal workflow id.
    pub workflow_id: String,
    /// Pipeline that was run.
    pub pipeline_uid: String,
    /// Tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Target index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Last known status.
    #[serde(default)]
    pub status: JobStatus,
    /// Last known step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    /// Failure message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// Creation time.
    pub started_at: DateTime<Utc>,
    /// Last update time.
    pub updated_at: DateTime<Utc>,
}

/// Partial update of a [`JobRecord`] (`PATCH /internal/jobs/{job_id}`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobUpdate {
    /// New status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<JobStatus>,
    /// New current step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    /// New error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// New index name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
}

/// Body of `POST /internal/resolve`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveRequest {
    /// Detected MIME type.
    pub mime: String,
    /// Filename when known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// Tenant.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Explicit pipeline uid (skips MIME routing).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
}

/// Response of `POST /internal/resolve`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolveResponse {
    /// The selected pipeline.
    pub pipeline: PipelineDefinition,
    /// Index override from the matching trigger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_pattern: Option<String>,
}

/// Error body returned by the control plane.
#[derive(Debug, Clone, Default, Deserialize)]
struct UpstreamErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
}

/// Thin HTTP client for the control plane's internal API.
#[derive(Debug, Clone)]
pub struct ControlPlaneClient {
    /// Base URL without trailing slash.
    pub base_url: String,
    /// Shared HTTP client.
    pub http: reqwest::Client,
}

impl ControlPlaneClient {
    /// Build a client.
    pub fn new(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        let base_url: String = base_url.into();
        Self { base_url: base_url.trim_end_matches('/').to_string(), http }
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    fn project_query(project_id: Option<&str>) -> Vec<(&'static str, String)> {
        project_id.map(|p| vec![("project_id", p.to_string())]).unwrap_or_default()
    }

    /// Turn a non-2xx control plane response into a [`GatewayError`].
    async fn error_from(resp: reqwest::Response, what: &str) -> GatewayError {
        let status = resp.status();
        let body = resp.bytes().await.unwrap_or_default();
        let parsed: UpstreamErrorBody = serde_json::from_slice(&body).unwrap_or_default();
        let msg = parsed
            .error
            .or(parsed.message)
            .filter(|m| !m.is_empty())
            .unwrap_or_else(|| String::from_utf8_lossy(&body).trim().to_string());
        let msg = if msg.is_empty() { format!("{what} failed with status {status}") } else { msg };
        match status {
            StatusCode::NOT_FOUND => GatewayError::NotFound(msg),
            StatusCode::FORBIDDEN => GatewayError::Forbidden(msg),
            StatusCode::BAD_REQUEST => GatewayError::BadRequest(msg),
            StatusCode::UNPROCESSABLE_ENTITY => GatewayError::Invalid(msg),
            StatusCode::PAYLOAD_TOO_LARGE => GatewayError::TooLarge(msg),
            _ => GatewayError::Upstream(format!("control plane {what}: {status}: {msg}")),
        }
    }

    async fn send_json<T: DeserializeOwned>(&self, req: reqwest::RequestBuilder, what: &str) -> Result<T, GatewayError> {
        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp, what).await);
        }
        resp.json::<T>()
            .await
            .map_err(|e| GatewayError::Upstream(format!("control plane {what}: invalid response: {e}")))
    }

    async fn send_empty(&self, req: reqwest::RequestBuilder, what: &str) -> Result<(), GatewayError> {
        let resp = req.send().await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp, what).await);
        }
        Ok(())
    }

    /// `POST /internal/resolve` — pick a pipeline for a MIME type / filename / tenant.
    /// Returns [`GatewayError::NotFound`] when nothing matches (callers map it to 415).
    pub async fn resolve(
        &self,
        mime: &str,
        filename: Option<&str>,
        project_id: Option<&str>,
        pipeline: Option<&str>,
    ) -> Result<ResolveResponse, GatewayError> {
        let body = ResolveRequest {
            mime: mime.to_string(),
            filename: filename.map(str::to_string),
            project_id: project_id.map(str::to_string),
            pipeline: pipeline.map(str::to_string),
        };
        self.send_json(self.http.post(self.url("/internal/resolve")).json(&body), "resolve").await
    }

    /// `GET /pipelines/{uid}?project_id=`.
    pub async fn get_pipeline(&self, uid: &str, project_id: Option<&str>) -> Result<PipelineDefinition, GatewayError> {
        self.send_json(
            self.http.get(self.url(&format!("/pipelines/{uid}"))).query(&Self::project_query(project_id)),
            "get pipeline",
        )
        .await
    }

    /// `GET /pipelines?project_id=`.
    pub async fn list_pipelines(&self, project_id: Option<&str>) -> Result<Vec<PipelineDefinition>, GatewayError> {
        self.send_json(self.http.get(self.url("/pipelines")).query(&Self::project_query(project_id)), "list pipelines")
            .await
    }

    /// `POST /pipelines` (create or update).
    pub async fn upsert_pipeline(&self, def: &PipelineDefinition) -> Result<PipelineDefinition, GatewayError> {
        self.send_json(self.http.post(self.url("/pipelines")).json(def), "upsert pipeline").await
    }

    /// `DELETE /pipelines/{uid}?project_id=`.
    pub async fn delete_pipeline(&self, uid: &str, project_id: Option<&str>) -> Result<(), GatewayError> {
        self.send_empty(
            self.http.delete(self.url(&format!("/pipelines/{uid}"))).query(&Self::project_query(project_id)),
            "delete pipeline",
        )
        .await
    }

    /// `GET /plugins`.
    pub async fn list_plugins(&self) -> Result<Vec<PluginManifest>, GatewayError> {
        self.send_json(self.http.get(self.url("/plugins")), "list plugins").await
    }

    /// `POST /internal/jobs`.
    pub async fn create_job(&self, job: &JobRecord) -> Result<(), GatewayError> {
        self.send_empty(self.http.post(self.url("/internal/jobs")).json(job), "create job").await
    }

    /// `PATCH /internal/jobs/{job_id}`.
    pub async fn update_job(&self, job_id: Uuid, update: &JobUpdate) -> Result<JobRecord, GatewayError> {
        self.send_json(self.http.patch(self.url(&format!("/internal/jobs/{job_id}"))).json(update), "update job")
            .await
    }

    /// `GET /internal/jobs/{job_id}`.
    pub async fn get_job(&self, job_id: Uuid) -> Result<JobRecord, GatewayError> {
        self.send_json(self.http.get(self.url(&format!("/internal/jobs/{job_id}"))), "get job").await
    }
}

// ---------------------------------------------------------------------------
// AppState
// ---------------------------------------------------------------------------

/// Everything handlers need. Cheap to clone.
#[derive(Clone)]
pub struct AppState {
    /// Configuration.
    pub config: Arc<GatewayConfig>,
    /// Workflow starter (Temporal in production, a fake in tests).
    pub temporal: Arc<dyn WorkflowStarter>,
    /// Control plane client.
    pub control_plane: ControlPlaneClient,
    /// Blob store for staging large uploads.
    pub blob: BlobStore,
    /// Shared HTTP client.
    pub http: reqwest::Client,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("config", &self.config)
            .field("control_plane", &self.control_plane)
            .field("blob", &self.blob)
            .finish_non_exhaustive()
    }
}

impl AppState {
    /// Assemble the state.
    pub fn new(
        config: GatewayConfig,
        temporal: Arc<dyn WorkflowStarter>,
        blob: BlobStore,
        http: reqwest::Client,
    ) -> Self {
        let control_plane = ControlPlaneClient::new(config.control_plane_url.clone(), http.clone());
        Self { config: Arc::new(config), temporal, control_plane, blob, http }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_pipeline() -> PipelineDefinition {
        PipelineDefinition {
            uid: "builtin.pdf".into(),
            name: "PDF".into(),
            description: None,
            version: 1,
            trigger: None,
            steps: vec![meili_ingest_plugin_sdk::StepDefinition::new("index", "meili_indexer")],
            builtin: true,
            project_id: None,
        }
    }

    #[test]
    fn execution_status_mapping() {
        assert_eq!(map_execution_status(WorkflowExecutionStatus::Running), JobStatus::Running);
        assert_eq!(map_execution_status(WorkflowExecutionStatus::Completed), JobStatus::Succeeded);
        assert_eq!(map_execution_status(WorkflowExecutionStatus::Failed), JobStatus::Failed);
        assert_eq!(map_execution_status(WorkflowExecutionStatus::TimedOut), JobStatus::Failed);
        assert_eq!(map_execution_status(WorkflowExecutionStatus::Canceled), JobStatus::Cancelled);
        assert_eq!(map_execution_status(WorkflowExecutionStatus::Terminated), JobStatus::Cancelled);
    }

    #[test]
    fn combine_status_prefers_progress_while_running() {
        let p = WorkflowProgress { status: JobStatus::Queued, ..Default::default() };
        assert_eq!(combine_status(JobStatus::Running, Some(&p)), JobStatus::Queued);
        assert_eq!(combine_status(JobStatus::Running, None), JobStatus::Running);
        assert_eq!(combine_status(JobStatus::Succeeded, Some(&p)), JobStatus::Succeeded);
        assert_eq!(combine_status(JobStatus::Cancelled, Some(&p)), JobStatus::Cancelled);
    }

    #[test]
    fn raw_value_roundtrip_decodes_without_panic() {
        let conv = PayloadConverter::default();
        let p = WorkflowProgress { status: JobStatus::Running, total_steps: 3, ..Default::default() };
        let raw = RawValue::from_value(&p, &conv);
        let back: WorkflowProgress = decode_raw(raw, &conv).unwrap();
        assert_eq!(back, p);
        let bad = RawValue::new(vec![]);
        assert!(decode_raw::<WorkflowProgress>(bad, &conv).is_err());
    }

    #[test]
    fn config_debug_redacts_secrets() {
        let cfg = GatewayConfig {
            meili_api_key: Some("SUPERSECRET".into()),
            envoy_trusted_header: Some("ENVOYSECRET".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("SUPERSECRET"));
        assert!(!dbg.contains("ENVOYSECRET"));
        assert_eq!(cfg.max_upload_bytes(), 500 * 1024 * 1024);
    }

    #[tokio::test]
    async fn resolve_maps_404_to_not_found_and_502_on_connection_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .and(body_json(serde_json::json!({"mime": "video/x-foo"})))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({"error": "no pipeline matches video/x-foo"})))
            .mount(&server)
            .await;
        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new());
        let err = cp.resolve("video/x-foo", None, None, None).await.unwrap_err();
        assert_eq!(err, GatewayError::NotFound("no pipeline matches video/x-foo".into()));

        let dead = ControlPlaneClient::new("http://127.0.0.1:9", reqwest::Client::new());
        let err = dead.resolve("application/pdf", None, None, None).await.unwrap_err();
        assert!(matches!(err, GatewayError::Upstream(_)), "{err:?}");
    }

    #[tokio::test]
    async fn resolve_success_and_pipeline_crud() {
        let server = MockServer::start().await;
        let def = sample_pipeline();
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"pipeline": def, "index_pattern": "contracts"})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines/builtin.pdf"))
            .and(query_param("project_id", "tenant-a"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&def))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![def.clone()]))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(201).set_body_json(&def))
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pipelines/builtin.pdf"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({"error": "built-in pipelines cannot be deleted", "code": "forbidden"})))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/plugins"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![PluginManifest::new("chunker", "0.1.0")]))
            .mount(&server)
            .await;

        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new());
        let r = cp.resolve("application/pdf", Some("a.pdf"), Some("tenant-a"), None).await.unwrap();
        assert_eq!(r.pipeline.uid, "builtin.pdf");
        assert_eq!(r.index_pattern.as_deref(), Some("contracts"));
        assert_eq!(cp.get_pipeline("builtin.pdf", Some("tenant-a")).await.unwrap().uid, "builtin.pdf");
        assert_eq!(cp.list_pipelines(None).await.unwrap().len(), 1);
        assert_eq!(cp.upsert_pipeline(&def).await.unwrap().uid, "builtin.pdf");
        let err = cp.delete_pipeline("builtin.pdf", None).await.unwrap_err();
        assert_eq!(err, GatewayError::Forbidden("built-in pipelines cannot be deleted".into()));
        assert_eq!(cp.list_plugins().await.unwrap()[0].name, "chunker");
    }

    #[tokio::test]
    async fn job_endpoints() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        let record = JobRecord {
            job_id,
            workflow_id: format!("ingest-{job_id}"),
            pipeline_uid: "builtin.pdf".into(),
            project_id: None,
            index_name: Some("documents".into()),
            status: JobStatus::Queued,
            current_step: None,
            error: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        };
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .respond_with(ResponseTemplate::new(201).set_body_json(&record))
            .mount(&server)
            .await;
        Mock::given(method("PATCH"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&record))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({"error": "job not found"})))
            .mount(&server)
            .await;
        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new());
        cp.create_job(&record).await.unwrap();
        let updated = cp
            .update_job(job_id, &JobUpdate { status: Some(JobStatus::Running), ..Default::default() })
            .await
            .unwrap();
        assert_eq!(updated.job_id, job_id);
        assert!(matches!(cp.get_job(job_id).await, Err(GatewayError::NotFound(_))));
    }
}
