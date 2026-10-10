//! # meili-ingest control plane
//!
//! Internal HTTP service (JSON only) backing the gateway:
//!
//! * pipeline registry (`/pipelines`) — user pipelines in Postgres + built-ins from
//!   the router crate;
//! * MIME/filename → pipeline resolution (`POST /internal/resolve`);
//! * plugin manifest registry (`/plugins`, `POST /internal/plugins`);
//! * denormalized job cache (`/internal/jobs`);
//! * Meilisearch connections (`/internal/connections`), keys held sealed;
//! * scheduled sources (`/internal/sources`, `/internal/sources-by-id`,
//!   `/internal/source-runs`);
//! * the Lab credit pre-check workers run before a source run
//!   (`/internal/lab/credits/{account_id}`).
//!
//! The binary lives in `main.rs`; everything else is exposed as a library so the
//! router can be exercised in tests without opening a socket.

pub mod boot;
pub mod builtin_pipelines;
pub mod connections;
pub mod db;
pub mod error;
pub mod jobs;
pub mod lab_credits;
pub mod lab_events;
pub mod lab_sender;
pub mod metrics;
pub mod pipelines;
pub mod plugins;
pub mod resolver;
pub mod sources;

use axum::extract::{FromRequest, Request, State};
use axum::http::StatusCode;
use axum::http::header::AUTHORIZATION;
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post, put};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use subtle::ConstantTimeEq;
use tower_http::trace::TraceLayer;

pub use error::CpError;
pub use resolver::PipelineCache;

/// Shared handler state: the Postgres pool and the in-memory user-pipeline cache.
#[derive(Clone)]
pub struct AppState {
    /// Postgres connection pool (may be lazily connected).
    pub pool: PgPool,
    /// Short-lived cache of user pipelines used by the resolver.
    pub cache: PipelineCache,
    /// Wakes the Lab events sender right after an insert.
    pub lab_notify: std::sync::Arc<tokio::sync::Notify>,
    /// Prometheus metrics served at `GET /metrics`.
    pub metrics: metrics::LabMetrics,
    /// Bearer token internal callers (gateway, workers) present on `/internal/*`; `None` leaves them open (dev only).
    pub internal_token: Option<String>,
    /// Lab account lookups behind `GET /internal/lab/credits/{account_id}`; `None`
    /// when the control plane has no Lab instance credentials (nothing is checked).
    pub lab_credits: Option<std::sync::Arc<meili_ingest_lab::AccountCreditCache>>,
}

impl AppState {
    /// State with a fresh cache using the default TTL.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: PipelineCache::default(),
            lab_notify: std::sync::Arc::new(tokio::sync::Notify::new()),
            metrics: metrics::LabMetrics::default(),
            internal_token: None,
            lab_credits: None,
        }
    }

    /// Answer the workers' credit checks from this cache (spec v2 §8.1).
    pub fn with_lab_credits(
        mut self,
        cache: std::sync::Arc<meili_ingest_lab::AccountCreditCache>,
    ) -> Self {
        self.lab_credits = Some(cache);
        self
    }

    /// Require `Authorization: Bearer <token>` on every `/internal/*` route.
    pub fn with_internal_token(mut self, token: Option<String>) -> Self {
        self.internal_token = token
            .map(|t| t.trim().to_string())
            .filter(|t| !t.is_empty());
        self
    }

    /// Pipeline repository bound to this state's pool.
    pub fn pipelines(&self) -> pipelines::PipelineRepo {
        pipelines::PipelineRepo::new(self.pool.clone())
    }

    /// Meilisearch connection repository bound to this state's pool.
    pub fn connections(&self) -> connections::ConnectionRepo {
        connections::ConnectionRepo::new(self.pool.clone())
    }
}

/// Decide what the control plane runs with: the token, or none only when the operator
/// said so explicitly (`CONTROL_PLANE_TOKEN_DISABLED=true`, dev). The example Secret's
/// `CHANGE_ME` placeholder is refused: it is a public value.
pub fn control_plane_token_policy(
    token: Option<String>,
    disabled: bool,
) -> anyhow::Result<Option<String>> {
    match token
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty())
    {
        Some(t) if t == "CHANGE_ME" => anyhow::bail!(
            "CONTROL_PLANE_TOKEN is the example placeholder CHANGE_ME; set a real token \
             (openssl rand -hex 32) on the control plane, the gateway and the workers"
        ),
        Some(t) => Ok(Some(t)),
        None if disabled => {
            tracing::warn!(
                "CONTROL_PLANE_TOKEN_DISABLED=true: /internal/* routes are open; never expose this port"
            );
            Ok(None)
        }
        None => anyhow::bail!(
            "CONTROL_PLANE_TOKEN is not set: the gateway and workers authenticate to /internal/* with it. \
             Set it (openssl rand -hex 32) on all three services, or set CONTROL_PLANE_TOKEN_DISABLED=true in dev"
        ),
    }
}

/// Middleware: `/internal/*` needs the configured bearer token; everything else passes.
pub async fn require_internal_token(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let Some(expected) = &state.internal_token else {
        return next.run(req).await;
    };
    if !req.uri().path().starts_with("/internal/") {
        return next.run(req).await;
    }
    let presented = req
        .headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim);
    match presented {
        Some(t) if bool::from(t.as_bytes().ct_eq(expected.as_bytes())) => next.run(req).await,
        _ => CpError::Unauthorized.into_response(),
    }
}

/// JSON body extractor whose rejection is a [`CpError::BadJson`] so malformed bodies
/// get the same `{"error","code"}` shape as every other error (400 `bad_json`).
#[derive(Debug, Clone)]
pub struct JsonBody<T>(pub T);

impl<S, T> FromRequest<S> for JsonBody<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = CpError;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(v)) => Ok(JsonBody(v)),
            Err(rej) => Err(CpError::BadJson(rej.body_text())),
        }
    }
}

/// `GET /health`: `{"status":"ok"}` when the database answers `SELECT 1`, otherwise
/// 503 `{"status":"unavailable","error":...}`.
pub async fn health(State(state): State<AppState>) -> Response {
    match db::ping(&state.pool).await {
        Ok(()) => Json(serde_json::json!({"status": "ok"})).into_response(),
        Err(e) => {
            tracing::warn!(error = %e, "health check: database unreachable");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(serde_json::json!({"status": "unavailable", "error": e.to_string()})),
            )
                .into_response()
        }
    }
}

/// `GET /metrics` in the Prometheus text format.
///
/// The outbox gauges are refreshed at scrape time so they are right even when no
/// sender runs (`LAB_URL` unset), which is exactly when the backlog alert must fire.
pub async fn serve_metrics(State(state): State<AppState>) -> Response {
    let repo = lab_events::LabEventRepo::new(state.pool.clone());
    match tokio::time::timeout(std::time::Duration::from_secs(2), repo.stats()).await {
        Ok(Ok(stats)) => {
            state.metrics.pending.set(stats.pending);
            state
                .metrics
                .oldest_pending_seconds
                .set(stats.oldest_pending_seconds);
        }
        Ok(Err(e)) => tracing::warn!(error = %e, "metrics: cannot read the lab_events stats"),
        Err(_) => tracing::warn!("metrics: reading the lab_events stats timed out"),
    }
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        metrics::render(&state.metrics),
    )
        .into_response()
}

/// Build the full axum router with request tracing.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/metrics", get(serve_metrics))
        .route(
            "/pipelines",
            get(pipelines::list_pipelines).post(pipelines::create_pipeline),
        )
        // Registered before `/pipelines/{uid}` so the literal path wins over the
        // wildcard and a pipeline can never be named "validate".
        .route("/pipelines/validate", post(pipelines::validate_pipeline))
        .route(
            "/pipelines/{uid}",
            get(pipelines::get_pipeline).delete(pipelines::delete_pipeline),
        )
        .route("/plugins", get(plugins::list_plugins))
        .route("/internal/plugins", post(plugins::register_plugins))
        .route("/internal/resolve", post(resolver::resolve))
        .route("/jobs", get(jobs::list_jobs))
        .route("/internal/jobs", post(jobs::create_job))
        .route(
            "/internal/jobs/{job_id}",
            get(jobs::get_job).patch(jobs::update_job),
        )
        .route(
            "/internal/connections",
            get(connections::list_connections).post(connections::create_connection),
        )
        .route(
            "/internal/connections/{uid}",
            get(connections::get_connection)
                .patch(connections::patch_connection)
                .delete(connections::delete_connection),
        )
        .route(
            "/internal/connections/{uid}/used_by",
            get(connections::connection_used_by),
        )
        .route(
            "/internal/sources",
            get(sources::list_sources).post(sources::create_source),
        )
        .route(
            "/internal/sources/{uid}",
            get(sources::get_source)
                .patch(sources::patch_source)
                .delete(sources::delete_source),
        )
        .route(
            "/internal/sources-by-id/{id}",
            get(sources::load_source_for_run),
        )
        .route(
            "/internal/sources-by-id/{id}/state",
            put(sources::save_source_state),
        )
        .route(
            "/internal/sources-by-id/{id}/paused",
            put(sources::set_source_paused),
        )
        .route(
            "/internal/sources-by-id/{id}/runs",
            get(sources::list_source_runs),
        )
        .route("/internal/source-runs", post(sources::record_source_run))
        .route("/internal/lab-events", post(lab_events::ingest_lab_events))
        .route(
            "/internal/lab/credits/{account_id}",
            get(lab_credits::check_credits),
        )
        .layer(middleware::from_fn_with_state(
            state.clone(),
            require_internal_token,
        ))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Resolve the tenant scope of a request: an explicit value (query param or body
/// field) wins over the `X-Meili-Project-Id` header. Empty strings count as absent.
pub fn tenant_scope(explicit: Option<&str>, headers: &axum::http::HeaderMap) -> Option<String> {
    explicit
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_owned)
        .or_else(|| {
            headers
                .get("x-meili-project-id")
                .and_then(|v| v.to_str().ok())
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_owned)
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::{HeaderMap, HeaderValue};

    #[test]
    fn tenant_scope_prefers_explicit_then_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-meili-project-id", HeaderValue::from_static("hdr"));
        assert_eq!(tenant_scope(Some("q"), &headers).as_deref(), Some("q"));
        assert_eq!(tenant_scope(None, &headers).as_deref(), Some("hdr"));
        assert_eq!(tenant_scope(Some("  "), &headers).as_deref(), Some("hdr"));
        assert_eq!(tenant_scope(None, &HeaderMap::new()), None);
    }
}
