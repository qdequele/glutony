//! meili-ingest HTTP gateway (SPEC §4, §6).
//!
//! The gateway resolves the tenant [`MeiliContext`](meili_ingest_plugin_sdk::MeiliContext)
//! from Envoy headers or standalone credentials, detects what was uploaded, asks the
//! control plane which pipeline to run, stages large payloads in the blob store and
//! starts one Temporal `PipelineWorkflow` per job. The binary lives in `main.rs`; this
//! library exposes the router so it can be exercised in tests without a network.

pub mod connections;
pub mod context;
pub mod error;
pub mod extract;
pub mod handlers;
pub mod preflight;
pub mod schedules;
pub mod sources;
pub mod state;
pub mod ui;

use axum::extract::{DefaultBodyLimit, State};
use axum::routing::{get, post};
use axum::{Json, Router};
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::trace::TraceLayer;

pub use error::GatewayError;
pub use state::{AppState, GatewayConfig, WorkflowStarter};

/// `GET /health` — liveness plus the facts a client needs to render itself.
///
/// Reports the tenant resolved from this very request, so the admin UI can show who
/// it is acting as instead of guessing from the data it happens to have fetched. Also
/// reports which optional features this deployment has, so the UI can present an
/// honest "not enabled" state rather than an error.
async fn health(
    State(state): State<AppState>,
    headers: axum::http::HeaderMap,
) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "project_id": context::resolve_project_id(&headers, &state.config),
        "features": {
            "usage_analytics": state.config.usage_api.is_some(),
            "embedded_ui": ui::is_embedded(),
        },
    }))
}

/// Normalise one `CORS_ALLOW_ORIGINS` entry into the exact value a browser sends in
/// its `Origin` header, or `None` when it cannot be an origin.
///
/// Origins match byte-for-byte on scheme, host and port, so an entry that merely
/// parses as a header value is not enough: `localhost:3000` (no scheme) or
/// `http://host/app` (a path) would be accepted and then never match anything. A
/// trailing slash is the one typo that is safe to repair.
fn normalize_origin(raw: &str) -> Option<axum::http::HeaderValue> {
    let origin = raw.trim().trim_end_matches('/');
    let authority = origin
        .strip_prefix("http://")
        .or_else(|| origin.strip_prefix("https://"))?;
    if authority.is_empty() || authority.contains(['/', '?', '#']) {
        return None;
    }
    origin.parse().ok()
}

/// Cross-origin layer for `CORS_ALLOW_ORIGINS`, or `None` when it is unset.
///
/// Production embeds the UI in this binary and is same-origin, so no layer is
/// mounted and the browser's default same-origin policy stands. `next dev` serves
/// the UI from its own port, so the dev stack names that origin explicitly. An
/// entry that is not an origin is dropped with a warning rather than silently
/// widening the policy; if that leaves nothing, no layer is mounted.
fn cors_layer(origins: &[String]) -> Option<CorsLayer> {
    let mut accepted = Vec::new();
    for raw in origins {
        match normalize_origin(raw) {
            Some(origin) => accepted.push(origin),
            None => tracing::warn!(
                origin = %raw,
                "ignoring CORS_ALLOW_ORIGINS entry: expected scheme://host[:port]"
            ),
        }
    }
    if accepted.is_empty() {
        return None;
    }
    tracing::info!(origins = ?accepted, "CORS enabled");
    Some(
        CorsLayer::new()
            .allow_origin(accepted)
            .allow_methods(Any)
            .allow_headers(Any),
    )
}

/// Build the axum router with every route of SPEC §4 plus `GET /health`, the body
/// limits (`MAX_UPLOAD_MB`) and request tracing.
pub fn router(state: AppState) -> Router {
    let limit = state.config.max_upload_bytes();
    let cors = cors_layer(&state.config.cors_allow_origins);
    let router = Router::new()
        .route("/health", get(health))
        .route("/ingest", post(handlers::ingest::ingest))
        .route("/ingest/batch", post(handlers::ingest::ingest_batch))
        .route(
            "/ingest/pipeline/{name}",
            post(handlers::pipeline::ingest_with_pipeline),
        )
        // Index-scoped aliases, the Meilisearch-style URLs a Cloud project exposes: the
        // path names the index and nothing in the request can override it.
        .route(
            "/indexes/{index_uid}/ingest",
            post(handlers::ingest::ingest_into_index),
        )
        .route(
            "/indexes/{index_uid}/ingest/batch",
            post(handlers::ingest::ingest_batch_into_index),
        )
        .route(
            "/indexes/{index_uid}/ingest/pipeline/{name}",
            post(handlers::pipeline::ingest_into_index_with_pipeline),
        )
        .route("/jobs", get(handlers::jobs::list_jobs))
        .route("/jobs/{id}", get(handlers::jobs::get_job))
        .route("/jobs/{id}/cancel", post(handlers::jobs::cancel_job))
        .route(
            "/pipelines",
            get(handlers::pipelines::list_pipelines).post(handlers::pipelines::create_pipeline),
        )
        // Before the `{name}` route so the literal path wins.
        .route(
            "/pipelines/validate",
            post(handlers::pipelines::validate_pipeline),
        )
        .route(
            "/pipelines/{name}",
            get(handlers::pipelines::get_pipeline).delete(handlers::pipelines::delete_pipeline),
        )
        .route(
            "/connections",
            get(handlers::connections::list_connections)
                .post(handlers::connections::create_connection),
        )
        .route(
            "/connections/{uid}",
            get(handlers::connections::get_connection)
                .patch(handlers::connections::patch_connection)
                .delete(handlers::connections::delete_connection),
        )
        .route(
            "/sources",
            get(handlers::sources::list_sources).post(handlers::sources::create_source),
        )
        .route(
            "/sources/{uid}",
            get(handlers::sources::get_source)
                .patch(handlers::sources::patch_source)
                .delete(handlers::sources::delete_source),
        )
        .route(
            "/sources/{uid}/pause",
            post(handlers::sources::pause_source),
        )
        .route(
            "/sources/{uid}/unpause",
            post(handlers::sources::unpause_source),
        )
        .route("/sources/{uid}/run", post(handlers::sources::run_source))
        .route("/sources/{uid}/runs", get(handlers::sources::list_runs))
        .route("/plugins", get(handlers::plugins::list_plugins))
        .route("/catalog", get(handlers::catalog::get_catalog))
        .route("/usage", get(handlers::usage::get_usage))
        .merge(ui::router())
        .layer(DefaultBodyLimit::max(limit))
        .layer(RequestBodyLimitLayer::new(limit))
        .layer(TraceLayer::new_for_http());
    // Applied with a `match` rather than an `Option` layer so the gateway does not
    // need tower's `util` feature just for `option_layer`.
    match cors {
        Some(cors) => router.layer(cors),
        None => router,
    }
    .with_state(state)
}

/// Shared fixtures for handler tests: a recording [`WorkflowStarter`], an in-memory blob
/// store and a `wiremock` control plane.
#[cfg(test)]
pub mod test_support {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::header::CONTENT_TYPE;
    use axum::response::Response;
    use meili_ingest_blob::BlobStore;
    use meili_ingest_plugin_sdk::{
        PipelineDefinition, PipelineTrigger, PipelineWorkflowInput, StepDefinition,
    };
    use uuid::Uuid;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::GatewayError;
    pub use crate::state::GatewayConfig;
    use crate::state::{AppState, JobSnapshot, StartedWorkflow, WorkflowStarter};

    /// A small but valid PDF header (magic bytes for `infer`).
    pub const PDF_MAGIC: &[u8] =
        b"%PDF-1.4\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";

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
        async fn start(
            &self,
            input: &PipelineWorkflowInput,
        ) -> Result<StartedWorkflow, GatewayError> {
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
    pub async fn test_app_with_url(
        mut config: GatewayConfig,
        control_plane_url: &str,
    ) -> (Router, Arc<FakeStarter>) {
        config.control_plane_url = control_plane_url.to_string();
        let starter = Arc::new(FakeStarter::default());
        let state = AppState::new(
            config,
            starter.clone(),
            BlobStore::memory(),
            reqwest::Client::new(),
        );
        (crate::router(state), starter)
    }

    /// Router + starter wired to a `wiremock` control plane.
    pub async fn test_app(
        server: &MockServer,
        config: GatewayConfig,
    ) -> (Router, Arc<FakeStarter>) {
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
    pub fn multipart_body(
        field: &str,
        filename: &str,
        content_type: &str,
        data: &[u8],
    ) -> (String, Vec<u8>) {
        let boundary = "----meiliTestBoundary7MA4YWxk";
        let mut body = Vec::new();
        body.extend_from_slice(format!("--{boundary}\r\n").as_bytes());
        body.extend_from_slice(
            format!(
                "Content-Disposition: form-data; name=\"{field}\"; filename=\"{filename}\"\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(format!("Content-Type: {content_type}\r\n\r\n").as_bytes());
        body.extend_from_slice(data);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        (format!("multipart/form-data; boundary={boundary}"), body)
    }

    /// A POST request carrying standalone credentials (`X-Meili-Host` + bearer key).
    pub fn standalone_request(
        uri: &str,
        content_type: impl AsRef<str>,
        body: Vec<u8>,
    ) -> Request<Body> {
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
        let bytes = axum::body::to_bytes(resp.into_body(), 16 * 1024 * 1024)
            .await
            .unwrap();
        if bytes.is_empty() {
            return serde_json::Value::Null;
        }
        serde_json::from_slice(&bytes).unwrap_or_else(|e| {
            panic!(
                "invalid JSON body {:?}: {e}",
                String::from_utf8_lossy(&bytes)
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::{cors_layer, normalize_origin};
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use wiremock::MockServer;

    #[tokio::test]
    async fn health_is_ok() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(Request::get("/health").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let body = json_body(resp).await;
        assert_eq!(body["status"], "ok");
        // The UI reads these to render the tenant and to hide features that are off.
        assert_eq!(body["features"]["usage_analytics"], false);
        assert!(body["project_id"].is_null());
    }

    #[tokio::test]
    async fn health_reports_the_tenant_from_trusted_headers() {
        let server = MockServer::start().await;
        let config = GatewayConfig {
            envoy_trusted_header: Some("shhh".into()),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;
        let resp = app
            .oneshot(
                Request::get("/health")
                    .header("X-Meili-Envoy-Secret", "shhh")
                    .header("X-Meili-Project-Id", "acme")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(json_body(resp).await["project_id"], "acme");
    }

    #[tokio::test]
    async fn health_ignores_a_spoofed_tenant_header() {
        // Same rule as everywhere else: without the shared secret the X-Meili-*
        // headers are not trusted, so the UI cannot be tricked into showing another
        // tenant's identity.
        let server = MockServer::start().await;
        let config = GatewayConfig {
            envoy_trusted_header: Some("shhh".into()),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;
        let resp = app
            .oneshot(
                Request::get("/health")
                    .header("X-Meili-Project-Id", "attacker")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(json_body(resp).await["project_id"].is_null());
    }

    #[tokio::test]
    async fn no_cors_headers_without_an_allowed_origin() {
        // The production shape: the UI is embedded and same-origin, so the gateway
        // must not hand out cross-origin permission to anybody.
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(
                Request::get("/health")
                    .header("Origin", "http://evil.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !resp
                .headers()
                .contains_key(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN),
            "unconfigured gateway should not send Access-Control-Allow-Origin"
        );
    }

    #[tokio::test]
    async fn cors_allows_only_the_configured_origins() {
        let server = MockServer::start().await;
        let config = GatewayConfig {
            cors_allow_origins: vec!["http://localhost:3000".into()],
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;

        let resp = app
            .clone()
            .oneshot(
                Request::get("/health")
                    .header("Origin", "http://localhost:3000")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.headers()
                .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
                .unwrap(),
            "http://localhost:3000"
        );

        // An origin that was not listed gets no grant, so naming one dev origin
        // does not quietly open the API to every site the browser visits.
        let resp = app
            .oneshot(
                Request::get("/health")
                    .header("Origin", "http://evil.example")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert!(
            !resp
                .headers()
                .contains_key(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN),
            "unlisted origin should not be granted"
        );
    }

    #[tokio::test]
    async fn cors_preflight_grants_the_ui_json_writes() {
        // The UI's POST/DELETE calls send `Content-Type: application/json`, which
        // makes the browser preflight them. Without this grant, reads would work
        // and every pipeline save would fail.
        use axum::http::header;

        let server = MockServer::start().await;
        let config = GatewayConfig {
            cors_allow_origins: vec!["http://localhost:3000".into()],
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, config).await;
        let resp = app
            .oneshot(
                Request::options("/pipelines")
                    .header(header::ORIGIN, "http://localhost:3000")
                    .header(header::ACCESS_CONTROL_REQUEST_METHOD, "POST")
                    .header(header::ACCESS_CONTROL_REQUEST_HEADERS, "content-type")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let headers = resp.headers();
        assert_eq!(
            headers.get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "http://localhost:3000"
        );
        assert!(
            headers.contains_key(header::ACCESS_CONTROL_ALLOW_METHODS),
            "preflight must grant the method"
        );
        assert!(
            headers.contains_key(header::ACCESS_CONTROL_ALLOW_HEADERS),
            "preflight must grant content-type"
        );
    }

    #[test]
    fn origin_entries_are_normalised_or_rejected() {
        let ok = |raw: &str| normalize_origin(raw).map(|v| v.to_str().unwrap().to_string());
        assert_eq!(
            ok("http://localhost:3000").as_deref(),
            Some("http://localhost:3000")
        );
        // The trailing slash a browser never sends is repaired, not kept.
        assert_eq!(
            ok("http://localhost:3000/").as_deref(),
            Some("http://localhost:3000")
        );
        assert_eq!(
            ok(" https://ui.example ").as_deref(),
            Some("https://ui.example")
        );
        // These would parse as header values yet could never match an Origin.
        assert_eq!(ok("localhost:3000"), None);
        assert_eq!(ok("http://"), None);
        assert_eq!(ok("http://host/app"), None);
        assert_eq!(ok("http://host?x=1"), None);
        assert!(cors_layer(&["localhost:3000".into()]).is_none());
    }

    #[tokio::test]
    async fn unknown_route_is_404() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(Request::get("/nope").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[cfg(feature = "ui")]
    #[tokio::test]
    async fn embedded_ui_is_served_under_its_own_prefix() {
        use axum::http::header;

        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;

        // The shell, under both spellings a human might type.
        for path in ["/ui", "/ui/"] {
            let resp = app
                .clone()
                .oneshot(Request::get(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "{path} did not serve the shell"
            );
        }
        let resp = app
            .clone()
            .oneshot(Request::get("/ui").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(ct.starts_with("text/html"), "content type was {ct:?}");
        let body = axum::body::to_bytes(resp.into_body(), 4_000_000)
            .await
            .unwrap();
        let html = String::from_utf8_lossy(&body);
        assert!(
            html.contains("/ui/_next/"),
            "assets must be prefixed with /ui"
        );

        // A client-side route survives a refresh.
        let resp = app
            .clone()
            .oneshot(Request::get("/ui/pipelines/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // The API is not shadowed by the UI: /pipelines still reaches the handler,
        // which without a control plane fails upstream rather than returning HTML.
        let resp = app
            .oneshot(Request::get("/pipelines").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_ne!(resp.status(), StatusCode::OK);
        let ct = resp
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default();
        assert!(
            !ct.starts_with("text/html"),
            "API route was shadowed by the UI"
        );
    }
}
