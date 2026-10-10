//! Shared application state: configuration, the Temporal workflow starter, the control
//! plane HTTP client and the blob store.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{
    JobStatus, PipelineDefinition, PipelineWorkflowInput, PluginManifest, WorkflowProgress,
};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use temporalio_client::{
    Client, UntypedQuery, UntypedSignal, UntypedWorkflow, WorkflowCancelOptions,
    WorkflowDescribeOptions, WorkflowExecutionStatus, WorkflowQueryOptions, WorkflowSignalOptions,
    WorkflowStartOptions,
};
use temporalio_common::data_converters::{
    GenericPayloadConverter, PayloadConverter, RawValue, SerializationContext,
    SerializationContextData,
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

/// Configuration of the read-side usage analytics API.
#[derive(Clone)]
pub struct UsageApiConfig {
    /// Tinybird API base URL for the workspace's region.
    pub base_url: String,
    /// Read token. Held server-side only; never sent to a browser.
    pub token: String,
    /// Endpoint pipe name backing the dashboard.
    pub pipe: String,
}

impl std::fmt::Debug for UsageApiConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UsageApiConfig")
            .field("base_url", &self.base_url)
            .field("pipe", &self.pipe)
            .field("token", &"<redacted>")
            .finish()
    }
}

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
    /// Bearer token for the control plane's `/internal/*` routes (`CONTROL_PLANE_TOKEN`).
    pub control_plane_token: Option<String>,
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
    /// Read-side usage analytics API, when configured. Absent means `GET /usage`
    /// answers 501 rather than failing.
    pub usage_api: Option<UsageApiConfig>,
    /// Browser origins allowed to call the API cross-origin
    /// (`CORS_ALLOW_ORIGINS`, comma-separated). Empty means no `CorsLayer` is
    /// mounted at all, which is what production wants: the embedded UI is
    /// same-origin. It exists for `next dev`, which serves the UI from another
    /// port and would otherwise be blocked by the browser.
    pub cors_allow_origins: Vec<String>,
    /// Check the request's Meilisearch key against the target index before queueing a
    /// job (`WRITE_PREFLIGHT`, default off). See [`crate::preflight`].
    pub write_preflight: bool,
    /// Bearer token the Meilisearch Lab uses on management routes (`LAB_SERVICE_TOKEN`).
    /// Setting it (or `admin_api_key`) closes open mode. See [`crate::auth`].
    pub lab_service_token: Option<String>,
    /// Bearer token an operator uses on management routes (`ADMIN_API_KEY`).
    pub admin_api_key: Option<String>,
}

impl std::fmt::Debug for GatewayConfig {
    /// Redacts secrets.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayConfig")
            .field("bind", &self.bind)
            .field("temporal_url", &self.temporal_url)
            .field("temporal_namespace", &self.temporal_namespace)
            .field("control_plane_url", &self.control_plane_url)
            .field(
                "control_plane_token",
                &self.control_plane_token.as_ref().map(|_| "<redacted>"),
            )
            .field("meili_url", &self.meili_url)
            .field(
                "meili_api_key",
                &self.meili_api_key.as_ref().map(|_| "<redacted>"),
            )
            .field("default_index", &self.default_index)
            .field("max_upload_mb", &self.max_upload_mb)
            .field(
                "envoy_trusted_header",
                &self.envoy_trusted_header.as_ref().map(|_| "<redacted>"),
            )
            .field("blob_store_url", &self.blob_store_url)
            .field("inline_max_bytes", &self.inline_max_bytes)
            .field("usage_api", &self.usage_api)
            .field("cors_allow_origins", &self.cors_allow_origins)
            .field("write_preflight", &self.write_preflight)
            .field(
                "lab_service_token",
                &self.lab_service_token.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "admin_api_key",
                &self.admin_api_key.as_ref().map(|_| "<redacted>"),
            )
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
            control_plane_token: None,
            meili_url: None,
            meili_api_key: None,
            default_index: "documents".into(),
            max_upload_mb: 500,
            envoy_trusted_header: None,
            blob_store_url: "file://./blobs".into(),
            inline_max_bytes: 1_048_576,
            usage_api: None,
            cors_allow_origins: Vec::new(),
            write_preflight: false,
            lab_service_token: None,
            admin_api_key: None,
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
            control_plane_token: env_opt("CONTROL_PLANE_TOKEN"),
            meili_url: env_opt("MEILI_URL"),
            meili_api_key: env_opt("MEILI_API_KEY"),
            default_index: env_or("DEFAULT_INDEX", &d.default_index),
            max_upload_mb: env_parse("MAX_UPLOAD_MB", d.max_upload_mb)?,
            envoy_trusted_header: env_opt("ENVOY_TRUSTED_HEADER"),
            blob_store_url: env_or("BLOB_STORE_URL", &d.blob_store_url),
            inline_max_bytes: env_parse("INLINE_MAX_BYTES", d.inline_max_bytes)?,
            // The dashboard proxy needs a READ token, which is a different token from
            // the append token the workers use to write usage events.
            usage_api: env_opt("TINYBIRD_READ_TOKEN").map(|token| UsageApiConfig {
                base_url: env_or("TINYBIRD_BASE_URL", "https://api.tinybird.co")
                    .trim_end_matches('/')
                    .to_string(),
                token,
                pipe: env_or("TINYBIRD_USAGE_PIPE", "tenant_usage"),
            }),
            cors_allow_origins: env_opt("CORS_ALLOW_ORIGINS")
                .map(|v| split_list(&v))
                .unwrap_or_default(),
            write_preflight: env_parse("WRITE_PREFLIGHT", d.write_preflight)?,
            lab_service_token: env_opt("LAB_SERVICE_TOKEN"),
            admin_api_key: env_opt("ADMIN_API_KEY"),
        })
    }

    /// Checks that must stop the process at boot.
    ///
    /// - The example Secret's `CHANGE_ME` placeholder is refused for every secret: once
    ///   published, it would let anyone through.
    /// - A Lab service token, or Lab instance credentials (`lab`), without the edge
    ///   secret would let any client claim a tenant with a bare `X-Meili-Tenant-Id`
    ///   header (Lab account ids are not secrets) and, on a Lab engine, bill any account.
    ///   Both combinations are refused.
    ///
    /// `ADMIN_API_KEY` alone stays allowed: operator mode may run without an edge.
    pub fn validate(&self, lab: Option<&meili_ingest_lab::LabCredentials>) -> anyhow::Result<()> {
        for (name, value) in [
            ("CONTROL_PLANE_TOKEN", &self.control_plane_token),
            ("ADMIN_API_KEY", &self.admin_api_key),
            ("ENVOY_TRUSTED_HEADER", &self.envoy_trusted_header),
        ] {
            if value.as_deref().is_some_and(is_placeholder) {
                anyhow::bail!(
                    "{name} is the example placeholder CHANGE_ME; set a real secret \
                     (openssl rand -hex 32) or leave it unset"
                );
            }
        }
        if lab.is_some() && self.envoy_trusted_header.is_none() {
            anyhow::bail!(
                "LAB_INSTANCE_ID / LAB_INSTANCE_SECRET are set but ENVOY_TRUSTED_HEADER is not: \
                 without the edge secret any client could bill any Lab account with \
                 X-Meili-Tenant-Id; set both"
            );
        }
        if self.lab_service_token.is_some() && self.envoy_trusted_header.is_none() {
            anyhow::bail!(
                "LAB_SERVICE_TOKEN is set but ENVOY_TRUSTED_HEADER is not: without the edge \
                 secret any client could claim a tenant with X-Meili-Tenant-Id; set both"
            );
        }
        Ok(())
    }

    /// Maximum request body size in bytes.
    pub fn max_upload_bytes(&self) -> usize {
        self.max_upload_mb.saturating_mul(1024 * 1024)
    }
}

/// The value the example Secret ships for every secret (`k8s/secrets.example.yaml`).
pub const PLACEHOLDER: &str = "CHANGE_ME";

/// Whether `value` is the example placeholder (trimmed, exact match).
fn is_placeholder(value: &str) -> bool {
    value.trim() == PLACEHOLDER
}

fn env_opt(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// Split a comma-separated env value, trimming each entry and dropping empty ones
/// (`"a, b,,"` → `["a", "b"]`), so the spacing people naturally type is harmless.
fn split_list(value: &str) -> Vec<String> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect()
}

fn env_or(name: &str, default: &str) -> String {
    env_opt(name).unwrap_or_else(|| default.to_string())
}

fn env_parse<T: std::str::FromStr>(name: &str, default: T) -> anyhow::Result<T>
where
    T::Err: std::fmt::Display,
{
    match env_opt(name) {
        Some(v) => v
            .parse::<T>()
            .map_err(|e| anyhow::anyhow!("invalid {name}={v:?}: {e}")),
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
fn decode_raw<T: DeserializeOwned + 'static>(
    raw: RawValue,
    conv: &PayloadConverter,
) -> Result<T, GatewayError> {
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
        WorkflowExecutionStatus::Canceled | WorkflowExecutionStatus::Terminated => {
            JobStatus::Cancelled
        }
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
                WorkflowStartOptions::new(
                    WORKFLOW_TASK_QUEUE,
                    PipelineWorkflowInput::workflow_id(input.job_id),
                )
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
            Err(e) => {
                return Err(GatewayError::Upstream(format!(
                    "cannot describe workflow: {e}"
                )));
            }
        };
        let progress = match handle
            .query(
                UntypedQuery::new("progress"),
                RawValue::from_value(&(), &conv),
                WorkflowQueryOptions::default(),
            )
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
        Ok(Some(JobSnapshot {
            status: combine_status(described, progress.as_ref()),
            progress,
        }))
    }

    async fn cancel(&self, job_id: Uuid) -> Result<(), GatewayError> {
        use temporalio_client::errors::WorkflowInteractionError;
        let conv = PayloadConverter::default();
        let handle = self
            .0
            .get_workflow_handle::<UntypedWorkflow>(PipelineWorkflowInput::workflow_id(job_id));
        let map = |e: WorkflowInteractionError| match e {
            WorkflowInteractionError::NotFound(_) => {
                GatewayError::NotFound(format!("job {job_id} not found"))
            }
            other => GatewayError::Upstream(format!("cannot cancel workflow: {other}")),
        };
        handle
            .signal(
                UntypedSignal::new("cancel"),
                RawValue::from_value(&(), &conv),
                WorkflowSignalOptions::default(),
            )
            .await
            .map_err(map)?;
        handle
            .cancel(WorkflowCancelOptions::default())
            .await
            .map_err(map)?;
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
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "project_id")]
    pub tenant_id: Option<String>,
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
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "project_id")]
    pub tenant_id: Option<String>,
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
#[derive(Clone)]
pub struct ControlPlaneClient {
    /// Base URL without trailing slash.
    pub base_url: String,
    /// Shared HTTP client.
    pub http: reqwest::Client,
    /// `CONTROL_PLANE_TOKEN`, presented as a bearer on every request.
    token: Option<String>,
}

impl std::fmt::Debug for ControlPlaneClient {
    /// Redacts the token.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlPlaneClient")
            .field("base_url", &self.base_url)
            .field("token", &self.token.as_ref().map(|_| "<redacted>"))
            .finish()
    }
}

impl ControlPlaneClient {
    /// Build a client.
    pub fn new(base_url: impl Into<String>, http: reqwest::Client) -> Self {
        let base_url: String = base_url.into();
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
            token: None,
        }
    }

    /// Present `CONTROL_PLANE_TOKEN` on every request.
    pub fn with_token(mut self, token: Option<String>) -> Self {
        self.token = token;
        self
    }

    fn authed(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => req.bearer_auth(t),
            None => req,
        }
    }

    pub(crate) fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }

    pub(crate) fn tenant_query(tenant_id: Option<&str>) -> Vec<(&'static str, String)> {
        tenant_id
            .map(|p| vec![("tenant_id", p.to_string())])
            .unwrap_or_default()
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
        let msg = if msg.is_empty() {
            format!("{what} failed with status {status}")
        } else {
            msg
        };
        match status {
            StatusCode::NOT_FOUND => GatewayError::NotFound(msg),
            StatusCode::FORBIDDEN => GatewayError::Forbidden(msg),
            StatusCode::BAD_REQUEST => GatewayError::BadRequest(msg),
            StatusCode::UNPROCESSABLE_ENTITY => GatewayError::Invalid(msg),
            StatusCode::PAYLOAD_TOO_LARGE => GatewayError::TooLarge(msg),
            _ => GatewayError::Upstream(format!("control plane {what}: {status}: {msg}")),
        }
    }

    pub(crate) async fn send_json<T: DeserializeOwned>(
        &self,
        req: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<T, GatewayError> {
        let resp = self.authed(req).send().await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp, what).await);
        }
        resp.json::<T>().await.map_err(|e| {
            GatewayError::Upstream(format!("control plane {what}: invalid response: {e}"))
        })
    }

    pub(crate) async fn send_empty(
        &self,
        req: reqwest::RequestBuilder,
        what: &str,
    ) -> Result<(), GatewayError> {
        let resp = self.authed(req).send().await?;
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
        tenant_id: Option<&str>,
        pipeline: Option<&str>,
    ) -> Result<ResolveResponse, GatewayError> {
        let body = ResolveRequest {
            mime: mime.to_string(),
            filename: filename.map(str::to_string),
            tenant_id: tenant_id.map(str::to_string),
            pipeline: pipeline.map(str::to_string),
        };
        self.send_json(
            self.http.post(self.url("/internal/resolve")).json(&body),
            "resolve",
        )
        .await
    }

    /// `GET /pipelines/{uid}?tenant_id=`.
    pub async fn get_pipeline(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
    ) -> Result<PipelineDefinition, GatewayError> {
        self.send_json(
            self.http
                .get(self.url(&format!("/pipelines/{uid}")))
                .query(&Self::tenant_query(tenant_id)),
            "get pipeline",
        )
        .await
    }

    /// `GET /pipelines?tenant_id=`.
    pub async fn list_pipelines(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<PipelineDefinition>, GatewayError> {
        self.send_json(
            self.http
                .get(self.url("/pipelines"))
                .query(&Self::tenant_query(tenant_id)),
            "list pipelines",
        )
        .await
    }

    /// `POST /pipelines` (create or update).
    pub async fn upsert_pipeline(
        &self,
        def: &PipelineDefinition,
    ) -> Result<PipelineDefinition, GatewayError> {
        self.send_json(
            self.http.post(self.url("/pipelines")).json(def),
            "upsert pipeline",
        )
        .await
    }

    /// `POST /pipelines/validate` — dry run, persists nothing.
    pub async fn validate_pipeline(
        &self,
        def: &PipelineDefinition,
    ) -> Result<serde_json::Value, GatewayError> {
        self.send_json(
            self.http.post(self.url("/pipelines/validate")).json(def),
            "validate pipeline",
        )
        .await
    }

    /// `GET /jobs?...` — one page of the denormalized job list.
    pub async fn list_jobs(
        &self,
        query: &[(String, String)],
    ) -> Result<serde_json::Value, GatewayError> {
        self.send_json(self.http.get(self.url("/jobs")).query(query), "list jobs")
            .await
    }

    /// `DELETE /pipelines/{uid}?tenant_id=`.
    ///
    /// Returns the ids of the sources the delete archived, whose Temporal schedules the
    /// caller must delete. An older control plane answering `204` reports none.
    pub async fn delete_pipeline(
        &self,
        uid: &str,
        tenant_id: Option<&str>,
    ) -> Result<Vec<Uuid>, GatewayError> {
        let resp = self
            .authed(
                self.http
                    .delete(self.url(&format!("/pipelines/{uid}")))
                    .query(&Self::tenant_query(tenant_id)),
            )
            .send()
            .await?;
        if !resp.status().is_success() {
            return Err(Self::error_from(resp, "delete pipeline").await);
        }
        if resp.status() == StatusCode::NO_CONTENT {
            return Ok(Vec::new());
        }
        #[derive(Deserialize)]
        struct Deleted {
            #[serde(default)]
            archived_sources: Vec<Uuid>,
        }
        let deleted: Deleted = resp.json().await.map_err(|e| {
            GatewayError::Upstream(format!(
                "control plane delete pipeline: invalid response: {e}"
            ))
        })?;
        Ok(deleted.archived_sources)
    }

    /// `GET /plugins`.
    pub async fn list_plugins(&self) -> Result<Vec<PluginManifest>, GatewayError> {
        self.send_json(self.http.get(self.url("/plugins")), "list plugins")
            .await
    }

    /// `POST /internal/jobs`.
    pub async fn create_job(&self, job: &JobRecord) -> Result<(), GatewayError> {
        self.send_empty(
            self.http.post(self.url("/internal/jobs")).json(job),
            "create job",
        )
        .await
    }

    /// `PATCH /internal/jobs/{job_id}`.
    pub async fn update_job(
        &self,
        job_id: Uuid,
        update: &JobUpdate,
    ) -> Result<JobRecord, GatewayError> {
        self.send_json(
            self.http
                .patch(self.url(&format!("/internal/jobs/{job_id}")))
                .json(update),
            "update job",
        )
        .await
    }

    /// `GET /internal/jobs/{job_id}`.
    pub async fn get_job(&self, job_id: Uuid) -> Result<JobRecord, GatewayError> {
        self.send_json(
            self.http.get(self.url(&format!("/internal/jobs/{job_id}"))),
            "get job",
        )
        .await
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
    /// Meilisearch connection settings: sealing key, host policy, probe client.
    pub connections: crate::connections::ConnectionConfig,
    /// Temporal Schedules for scheduled sources.
    pub schedules: Arc<dyn crate::schedules::ScheduleClient>,
    /// Which hosts a source may fetch from (`SOURCE_FETCH_HOSTS`), checked on save.
    pub fetch_policy: meili_ingest_source::HostPolicy,
    /// The Lab, when this deployment reports to one (`LAB_URL` + `LAB_INSTANCE_*`).
    pub lab: Option<Arc<crate::lab::LabClient>>,
}

impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("config", &self.config)
            .field("control_plane", &self.control_plane)
            .field("blob", &self.blob)
            .field("connections", &self.connections)
            .field("lab", &self.lab)
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
        let control_plane = ControlPlaneClient::new(config.control_plane_url.clone(), http.clone())
            .with_token(config.control_plane_token.clone());
        Self {
            config: Arc::new(config),
            temporal,
            control_plane,
            blob,
            http,
            connections: crate::connections::ConnectionConfig::default(),
            schedules: Arc::new(crate::schedules::DisabledSchedules),
            fetch_policy: meili_ingest_source::HostPolicy::default(),
            lab: None,
        }
    }

    /// Enable scheduled sources: the Temporal Schedule client and the fetch policy.
    /// Without it the `/sources` routes answer 501.
    pub fn with_sources(
        mut self,
        schedules: Arc<dyn crate::schedules::ScheduleClient>,
        fetch_policy: meili_ingest_source::HostPolicy,
    ) -> Self {
        self.schedules = schedules;
        self.fetch_policy = fetch_policy;
        self
    }

    /// Enable the `/connections` routes with a sealing key and host policy. Without it
    /// they answer 501, so a key is never stored unsealed by accident.
    pub fn with_connections(mut self, connections: crate::connections::ConnectionConfig) -> Self {
        self.connections = connections;
        self
    }

    /// Attach the Lab client: hosted deployments then pre-check credits before each job.
    pub fn with_lab(mut self, lab: Arc<crate::lab::LabClient>) -> Self {
        self.lab = Some(lab);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{body_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn split_list_trims_and_drops_empty_entries() {
        // The exact shape compose passes: a space after the comma must not leak into
        // an origin, where it would never match the browser's `Origin` header.
        assert_eq!(
            split_list("http://localhost:3000, http://ui.local:3000,,"),
            vec!["http://localhost:3000", "http://ui.local:3000"]
        );
        assert!(split_list(" , ").is_empty());
    }

    fn sample_pipeline() -> PipelineDefinition {
        PipelineDefinition {
            uid: "builtin.pdf".into(),
            name: "PDF".into(),
            description: None,
            version: 1,
            trigger: None,
            steps: vec![meili_ingest_plugin_sdk::StepDefinition::new(
                "index",
                "meili_indexer",
            )],
            builtin: true,
            tenant_id: None,
        }
    }

    #[test]
    fn execution_status_mapping() {
        assert_eq!(
            map_execution_status(WorkflowExecutionStatus::Running),
            JobStatus::Running
        );
        assert_eq!(
            map_execution_status(WorkflowExecutionStatus::Completed),
            JobStatus::Succeeded
        );
        assert_eq!(
            map_execution_status(WorkflowExecutionStatus::Failed),
            JobStatus::Failed
        );
        assert_eq!(
            map_execution_status(WorkflowExecutionStatus::TimedOut),
            JobStatus::Failed
        );
        assert_eq!(
            map_execution_status(WorkflowExecutionStatus::Canceled),
            JobStatus::Cancelled
        );
        assert_eq!(
            map_execution_status(WorkflowExecutionStatus::Terminated),
            JobStatus::Cancelled
        );
    }

    #[test]
    fn combine_status_prefers_progress_while_running() {
        let p = WorkflowProgress {
            status: JobStatus::Queued,
            ..Default::default()
        };
        assert_eq!(
            combine_status(JobStatus::Running, Some(&p)),
            JobStatus::Queued
        );
        assert_eq!(combine_status(JobStatus::Running, None), JobStatus::Running);
        assert_eq!(
            combine_status(JobStatus::Succeeded, Some(&p)),
            JobStatus::Succeeded
        );
        assert_eq!(
            combine_status(JobStatus::Cancelled, Some(&p)),
            JobStatus::Cancelled
        );
    }

    #[test]
    fn raw_value_roundtrip_decodes_without_panic() {
        let conv = PayloadConverter::default();
        let p = WorkflowProgress {
            status: JobStatus::Running,
            total_steps: 3,
            ..Default::default()
        };
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
            lab_service_token: Some("lab-secret-value".into()),
            admin_api_key: Some("admin-secret-value".into()),
            ..Default::default()
        };
        let dbg = format!("{cfg:?}");
        assert!(!dbg.contains("SUPERSECRET"));
        assert!(!dbg.contains("ENVOYSECRET"));
        assert!(!dbg.contains("lab-secret-value"));
        assert!(!dbg.contains("admin-secret-value"));
        assert_eq!(cfg.max_upload_bytes(), 500 * 1024 * 1024);
    }

    #[test]
    fn validate_requires_the_edge_secret_with_a_lab_token() {
        let lab_only = GatewayConfig {
            lab_service_token: Some("lab".into()),
            ..Default::default()
        };
        let err = lab_only.validate(None).unwrap_err().to_string();
        assert!(err.contains("LAB_SERVICE_TOKEN"), "{err}");
        assert!(err.contains("ENVOY_TRUSTED_HEADER"), "{err}");

        let both = GatewayConfig {
            lab_service_token: Some("lab".into()),
            envoy_trusted_header: Some("edge".into()),
            ..Default::default()
        };
        assert!(both.validate(None).is_ok());

        // Operator mode may run without an edge.
        let admin_only = GatewayConfig {
            admin_api_key: Some("admin".into()),
            ..Default::default()
        };
        assert!(admin_only.validate(None).is_ok());
        assert!(GatewayConfig::default().validate(None).is_ok());
    }

    #[test]
    fn validate_refuses_the_change_me_placeholder() {
        // The example Secret ships CHANGE_ME; booting on it would make these public.
        type Set = fn(&mut GatewayConfig, String);
        let cases: [(&str, Set); 3] = [
            ("CONTROL_PLANE_TOKEN", |c, v| {
                c.control_plane_token = Some(v)
            }),
            ("ADMIN_API_KEY", |c, v| c.admin_api_key = Some(v)),
            ("ENVOY_TRUSTED_HEADER", |c, v| {
                c.envoy_trusted_header = Some(v)
            }),
        ];
        for (name, set) in cases {
            for value in ["CHANGE_ME", " CHANGE_ME\n"] {
                let mut cfg = GatewayConfig::default();
                set(&mut cfg, value.to_string());
                let err = cfg.validate(None).unwrap_err().to_string();
                assert!(err.contains(name), "{name}: {err}");
                assert!(err.contains("CHANGE_ME"), "{name}: {err}");
            }
            // Only the exact placeholder is refused.
            let mut cfg = GatewayConfig::default();
            set(&mut cfg, "change_me_not".to_string());
            assert!(cfg.validate(None).is_ok(), "{name}");
        }
    }

    #[test]
    fn validate_requires_the_edge_secret_on_a_lab_engine() {
        // With Lab credentials every Lab-account job is billed: without the edge secret
        // any client could bill any account with X-Meili-Tenant-Id.
        let creds = meili_ingest_lab::LabCredentials::new(
            "https://lab.example",
            "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d",
            "s",
        )
        .unwrap();
        let err = GatewayConfig::default()
            .validate(Some(&creds))
            .unwrap_err()
            .to_string();
        assert!(err.contains("LAB_INSTANCE_ID"), "{err}");
        assert!(err.contains("ENVOY_TRUSTED_HEADER"), "{err}");
        let with_edge = GatewayConfig {
            envoy_trusted_header: Some("edge".into()),
            ..Default::default()
        };
        assert!(with_edge.validate(Some(&creds)).is_ok());
    }

    #[tokio::test]
    async fn resolve_maps_404_to_not_found_and_502_on_connection_error() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .and(body_json(serde_json::json!({"mime": "video/x-foo"})))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"error": "no pipeline matches video/x-foo"})),
            )
            .mount(&server)
            .await;
        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new());
        let err = cp
            .resolve("video/x-foo", None, None, None)
            .await
            .unwrap_err();
        assert_eq!(
            err,
            GatewayError::NotFound("no pipeline matches video/x-foo".into())
        );

        let dead = ControlPlaneClient::new("http://127.0.0.1:9", reqwest::Client::new());
        let err = dead
            .resolve("application/pdf", None, None, None)
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::Upstream(_)), "{err:?}");
    }

    #[tokio::test]
    async fn resolve_success_and_pipeline_crud() {
        let server = MockServer::start().await;
        let def = sample_pipeline();
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    serde_json::json!({"pipeline": def, "index_pattern": "contracts"}),
                ),
            )
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines/builtin.pdf"))
            .and(query_param("tenant_id", "tenant-a"))
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
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(vec![PluginManifest::new("chunker", "0.1.0")]),
            )
            .mount(&server)
            .await;

        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new());
        let r = cp
            .resolve("application/pdf", Some("a.pdf"), Some("tenant-a"), None)
            .await
            .unwrap();
        assert_eq!(r.pipeline.uid, "builtin.pdf");
        assert_eq!(r.index_pattern.as_deref(), Some("contracts"));
        assert_eq!(
            cp.get_pipeline("builtin.pdf", Some("tenant-a"))
                .await
                .unwrap()
                .uid,
            "builtin.pdf"
        );
        assert_eq!(cp.list_pipelines(None).await.unwrap().len(), 1);
        assert_eq!(cp.upsert_pipeline(&def).await.unwrap().uid, "builtin.pdf");
        let err = cp.delete_pipeline("builtin.pdf", None).await.unwrap_err();
        assert_eq!(
            err,
            GatewayError::Forbidden("built-in pipelines cannot be deleted".into())
        );
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
            tenant_id: None,
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
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"error": "job not found"})),
            )
            .mount(&server)
            .await;
        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new());
        cp.create_job(&record).await.unwrap();
        let updated = cp
            .update_job(
                job_id,
                &JobUpdate {
                    status: Some(JobStatus::Running),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(updated.job_id, job_id);
        assert!(matches!(
            cp.get_job(job_id).await,
            Err(GatewayError::NotFound(_))
        ));
    }

    #[tokio::test]
    async fn the_control_plane_token_is_sent_as_a_bearer() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .and(wiremock::matchers::header(
                "authorization",
                "Bearer cp-token",
            ))
            .respond_with(ResponseTemplate::new(201))
            .expect(1)
            .mount(&server)
            .await;
        let cp = ControlPlaneClient::new(server.uri(), reqwest::Client::new())
            .with_token(Some("cp-token".into()));
        let job_id = Uuid::new_v4();
        cp.create_job(&JobRecord {
            job_id,
            workflow_id: format!("ingest-{job_id}"),
            pipeline_uid: "builtin.pdf".into(),
            tenant_id: None,
            index_name: None,
            status: JobStatus::Queued,
            current_step: None,
            error: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
        assert!(!format!("{cp:?}").contains("cp-token"), "{cp:?}");
        let cfg = GatewayConfig {
            control_plane_token: Some("cp-token-value".into()),
            ..Default::default()
        };
        assert!(!format!("{cfg:?}").contains("cp-token-value"));
    }
}
