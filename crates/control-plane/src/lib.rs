//! # meili-ingest control plane
//!
//! Internal HTTP service (JSON only) backing the gateway:
//!
//! * pipeline registry (`/pipelines`) — user pipelines in Postgres + built-ins from
//!   the router crate;
//! * MIME/filename → pipeline resolution (`POST /internal/resolve`);
//! * plugin manifest registry (`/plugins`, `POST /internal/plugins`);
//! * denormalized job cache (`/internal/jobs`).
//!
//! The binary lives in `main.rs`; everything else is exposed as a library so the
//! router can be exercised in tests without opening a socket.

pub mod builtin_pipelines;
pub mod connections;
pub mod db;
pub mod error;
pub mod jobs;
pub mod pipelines;
pub mod plugins;
pub mod resolver;
pub mod sources;

use axum::extract::{FromRequest, Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
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
}

impl AppState {
    /// State with a fresh cache using the default TTL.
    pub fn new(pool: PgPool) -> Self {
        Self {
            pool,
            cache: PipelineCache::default(),
        }
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

/// Build the full axum router with request tracing.
pub fn app(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
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
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

/// Resolve the tenant scope of a request: an explicit value (query param or body
/// field) wins over the `X-Meili-Project-Id` header. Empty strings count as absent.
pub fn project_scope(explicit: Option<&str>, headers: &axum::http::HeaderMap) -> Option<String> {
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
    fn project_scope_prefers_explicit_then_header() {
        let mut headers = HeaderMap::new();
        headers.insert("x-meili-project-id", HeaderValue::from_static("hdr"));
        assert_eq!(project_scope(Some("q"), &headers).as_deref(), Some("q"));
        assert_eq!(project_scope(None, &headers).as_deref(), Some("hdr"));
        assert_eq!(project_scope(Some("  "), &headers).as_deref(), Some("hdr"));
        assert_eq!(project_scope(None, &HeaderMap::new()), None);
    }
}
