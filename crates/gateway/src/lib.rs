//! meili-ingest HTTP gateway (SPEC §4, §6).
//!
//! The gateway resolves the tenant [`MeiliContext`](meili_ingest_plugin_sdk::MeiliContext)
//! from Envoy headers or standalone credentials, detects what was uploaded, asks the
//! control plane which pipeline to run, stages large payloads in the blob store and
//! starts one Temporal `PipelineWorkflow` per job. The binary lives in `main.rs`; this
//! library exposes the router so it can be exercised in tests without a network.

pub mod context;
pub mod error;
pub mod extract;
pub mod handlers;
pub mod state;

use axum::extract::DefaultBodyLimit;
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub use error::GatewayError;
pub use state::{AppState, GatewayConfig, WorkflowStarter};

/// `GET /health`.
async fn health() -> Json<serde_json::Value> {
    Json(serde_json::json!({"status": "ok"}))
}

/// Build the axum router with every route of SPEC §4 plus `GET /health`, the body
/// limits (`MAX_UPLOAD_MB`) and request tracing.
pub fn router(state: AppState) -> Router {
    let limit = state.config.max_upload_bytes();
    Router::new()
        .route("/health", get(health))
        .route("/ingest", post(handlers::ingest::ingest))
        .route("/ingest/batch", post(handlers::ingest::ingest_batch))
        .route("/ingest/pipeline/{name}", post(handlers::pipeline::ingest_with_pipeline))
        .route("/jobs/{id}", get(handlers::jobs::get_job))
        .route("/jobs/{id}/cancel", post(handlers::jobs::cancel_job))
        .route("/pipelines", get(handlers::pipelines::list_pipelines).post(handlers::pipelines::create_pipeline))
        .route(
            "/pipelines/{name}",
            get(handlers::pipelines::get_pipeline).delete(handlers::pipelines::delete_pipeline),
        )
        .route("/plugins", get(handlers::plugins::list_plugins))
        .layer(DefaultBodyLimit::max(limit))
        .layer(RequestBodyLimitLayer::new(limit))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Shared fixtures for handler tests: a recording [`WorkflowStarter`], an in-memory blob
/// store and a `wiremock` control plane.
#[cfg(test)]
pub mod test_support {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::body::Body;
    use axum::http::header::CONTENT_TYPE;
    use axum::http::Request;
    use axum::response::Response;
    use axum::Router;
    use meili_ingest_blob::BlobStore;
    use meili_ingest_plugin_sdk::{PipelineDefinition, PipelineTrigger, PipelineWorkflowInput, StepDefinition};
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    pub use crate::state::GatewayConfig;
    use crate::state::{AppState, JobSnapshot, StartedWorkflow, WorkflowStarter};
    use crate::GatewayError;

    /// A small but valid PDF header (magic bytes for `infer`).
    pub const PDF_MAGIC: &[u8] = b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";

    /// [`WorkflowStarter`] that records inputs and serves canned snapshots.
    #[derive(Default)]
    pub struct FakeStarter {
        inputs: Mutex<Vec<PipelineWorkflowInput>>,
        cancelled: Mutex<Vec<Uuid>>,
        snapshots: Mutex<HashMap<Uuid, JobSnapshot>>,
        fail_start: Mutex<bool>,
    }

    impl FakeStarter {
        /// Every workflow input received so far.
        pub fn inputs(&self) -> Vec<PipelineWorkflowInput> {
            self.inputs.lock().unwrap().clone()
        }
        /// Every cancelled job id.
        pub fn cancelled(&self) -> Vec<Uuid> {
            self.cancelled.lock().unwrap().clone()
        }
        /// Make `progress(job_id)` return this snapshot.
        pub fn set_snapshot(&self, job_id: Uuid, snapshot: JobSnapshot) {
            self.snapshots.lock().unwrap().insert(job_id, snapshot);
        }
        /// Make `start` fail with an upstream error.
        pub fn fail_start(&self) {
            *self.fail_start.lock().unwrap() = true;
        }
    }

    #[async_trait]
    impl WorkflowStarter for FakeStarter {
        async fn start(&self, input: &PipelineWorkflowInput) -> Result<StartedWorkflow, GatewayError> {
            if *self.fail_start.lock().unwrap() {
                return Err(GatewayError::Upstream("temporal unavailable".into()));
            }
            self.inputs.lock().unwrap().push(input.clone());
            Ok(StartedWorkflow {
                workflow_id: PipelineWorkflowInput::workflow_id(input.job_id),
                run_id: Some("run-1".into()),
            })
        }
        async fn progress(&self, job_id: Uuid) -> Result<Option<JobSnapshot>, GatewayError> {
            Ok(self.snapshots.lock().unwrap().get(&job_id).cloned())
        }
        async fn cancel(&self, job_id: Uuid) -> Result<(), GatewayError> {
            self.cancelled.lock().unwrap().push(job_id);
            Ok(())
        }
    }

    /// Router + starter wired to the given control plane URL.
    pub async fn test_app_with_url(mut config: GatewayConfig, control_plane_url: &str) -> (Router, Arc<FakeStarter>) {
        config.control_plane_url = control_plane_url.to_string();
        let starter = Arc::new(FakeStarter::default());
        let state = AppState::new(config, starter.clone(), BlobStore::memory(), reqwest::Client::new());
        (crate::router(state), starter)
    }

    /// Router + starter wired to a `wiremock` control plane.
    pub async fn test_app(server: &MockServer, config: GatewayConfig) -> (Router, Arc<FakeStarter>) {
        test_app_with_url(config, &server.uri()).await
    }

    /// A one-step pipeline definition.
    pub fn sample_pipeline(uid: &str, index_pattern: Option<&str>) -> PipelineDefinition {
        PipelineDefinition {
            uid: uid.into(),
            name: uid.into(),
            description: None,
            version: 1,
            trigger: Some(PipelineTrigger {
                content_types: vec!["*/*".into()],
                filename_pattern: None,
                index_pattern: index_pattern.map(str::to_string),
            }),
            steps: vec![StepDefinition::new("index", "meili_indexer")],
            builtin: uid.starts_with("builtin."),
            project_id: None,
        }
    }

    /// Mount `POST /internal/resolve` → 200 with the given pipeline.
    pub async fn mount_resolve(server: &MockServer, uid: &str, index_pattern: Option<&str>) {
        let mut body = serde_json::json!({"pipeline": sample_pipeline(uid, None)});
        if let Some(p) = index_pattern {
            body["index_pattern"] = serde_json::Value::String(p.to_string());
        }
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(server)
            .await;
    }

    /// Mount `POST /internal/jobs` → 201.
    pub async fn mount_jobs_ok(server: &MockServer) {
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({})))
            .mount(server)
            .await;
    }

    /// Build a multipart body with one file part. Returns `(content_type, body)`.
    pub fn multipart_body(field: &str, filename: &str, content_type: &str, data: &[u8]) -> (String, Vec<u8>) {
        let boundary = "----meiliTestBoundary7MA4YWxk";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!("Content-Disposition: form-data; name=\"{field}\"; filename=\"{filename}\"\r\n").as_bytes(),
        );
        body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
        body.extend_from_slice(data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        (format!("multipart/form-data; boundary={boundary}"), body)
    }

    /// A POST request carrying standalone credentials (`X-Meili-Host` + bearer key).
    pub fn standalone_request(uri: &str, content_type: impl AsRef<str>, body: Vec<u8>) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(CONTENT_TYPE, content_type.as_ref())
            .header("x-meili-host", "http://localhost:7700")
            .header("authorization", "Bearer masterKey")
            .body(Body::from(body))
            .unwrap()
    }

    /// Read a JSON response body.
    pub async fn json_body(resp: Response) -> serde_json::Value {
        let bytes = axum::body::to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
        if bytes.is_empty() {
            return serde_json::Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("invalid JSON body {:?}: {e}", String::from_utf8_lossy(&bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use wiremock::MockServer;

    #[tokio::test]
    async fn health_is_ok() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app.oneshot(Request::get("/health").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(json_body(resp).await["status"], "ok");
    }

    #[tokio::test]
    async fn unknown_route_is_404() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app.oneshot(Request::get("/nope").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
