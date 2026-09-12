//! `POST /internal/resolve`: pick the pipeline for an ingest request.
//!
//! The user-pipeline list is cached in memory for a few seconds so the gateway's
//! per-request resolve call does not turn into one Postgres round-trip per upload.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use meili_ingest_plugin_sdk::PipelineDefinition;
use meili_ingest_router::{PipelineRouter, RouteRequest};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::builtin_pipelines::{builtin_pipelines, is_builtin_uid};
use crate::error::CpError;
use crate::pipelines::PipelineRepo;
use crate::{AppState, JsonBody};

/// Default time-to-live of the cached user-pipeline list.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);

/// Cached list plus the instant it was loaded.
type CacheEntry = Option<(Instant, Vec<PipelineDefinition>)>;

/// Time-bounded in-memory copy of every user pipeline (all tenants).
#[derive(Debug, Clone)]
pub struct PipelineCache {
    inner: Arc<RwLock<CacheEntry>>,
    ttl: Duration,
}

impl Default for PipelineCache {
    fn default() -> Self {
        Self::new(DEFAULT_CACHE_TTL)
    }
}

impl PipelineCache {
    /// Empty cache with the given TTL.
    pub fn new(ttl: Duration) -> Self {
        Self {
            inner: Arc::new(RwLock::new(None)),
            ttl,
        }
    }

    /// Return the cached list when fresh, otherwise reload it from `repo`.
    pub async fn get_or_load(
        &self,
        repo: &PipelineRepo,
    ) -> Result<Vec<PipelineDefinition>, CpError> {
        if let Some((at, list)) = self.inner.read().await.as_ref()
            && at.elapsed() < self.ttl
        {
            return Ok(list.clone());
        }
        let list = repo.list_all().await?;
        *self.inner.write().await = Some((Instant::now(), list.clone()));
        Ok(list)
    }

    /// Replace the cached list (used after writes in tests and by seeding).
    pub async fn set(&self, list: Vec<PipelineDefinition>) {
        *self.inner.write().await = Some((Instant::now(), list));
    }

    /// Drop the cached list so the next read hits the database.
    pub async fn invalidate(&self) {
        *self.inner.write().await = None;
    }

    /// Whether a fresh list is currently cached.
    pub async fn is_fresh(&self) -> bool {
        matches!(self.inner.read().await.as_ref(), Some((at, _)) if at.elapsed() < self.ttl)
    }
}

/// Body of `POST /internal/resolve`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolveRequest {
    /// Detected MIME type (required).
    pub mime: String,
    /// Original filename, if any (used for `filename_pattern` triggers).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub filename: Option<String>,
    /// Tenant scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Explicit pipeline uid (`POST /ingest/pipeline/{name}`); bypasses routing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline: Option<String>,
}

/// Response of `POST /internal/resolve`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolveResponse {
    /// The selected pipeline (snapshot).
    pub pipeline: PipelineDefinition,
    /// `trigger.index_pattern` of the selected pipeline, when set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_pattern: Option<String>,
}

/// Keep only the pipelines visible to `project_id`: global ones plus that tenant's.
pub fn scope_pipelines(
    all: Vec<PipelineDefinition>,
    project_id: Option<&str>,
) -> Vec<PipelineDefinition> {
    all.into_iter()
        .filter(|p| match (&p.project_id, project_id) {
            (None, _) => true,
            (Some(pid), Some(req)) => pid == req,
            (Some(_), None) => false,
        })
        .collect()
}

/// Pure resolution over an already-loaded candidate list (user pipelines for the
/// tenant followed by the built-ins). Shared by the handler and the unit tests.
pub fn resolve_with(
    candidates: Vec<PipelineDefinition>,
    req: &ResolveRequest,
) -> Result<ResolveResponse, CpError> {
    let router = PipelineRouter::new(candidates);
    let project_id = req.project_id.as_deref();
    let pipeline = match &req.pipeline {
        Some(uid) => router.by_uid(uid, project_id).ok_or_else(|| {
            CpError::NotFound(format!(
                "pipeline {uid:?} not found (project_id={project_id:?})"
            ))
        })?,
        None => {
            let m = router
                .resolve(RouteRequest {
                    mime: &req.mime,
                    filename: req.filename.as_deref(),
                    project_id,
                })
                .ok_or_else(|| {
                    CpError::NoPipeline(format!(
                        "no pipeline matches mime {:?}{}",
                        req.mime,
                        req.filename
                            .as_deref()
                            .map(|f| format!(" (filename {f:?})"))
                            .unwrap_or_default()
                    ))
                })?;
            m.pipeline
        }
    };
    let index_pattern = pipeline
        .trigger
        .as_ref()
        .and_then(|t| t.index_pattern.clone());
    Ok(ResolveResponse {
        pipeline: pipeline.clone(),
        index_pattern,
    })
}

/// `POST /internal/resolve` → `{pipeline, index_pattern?}` | 404.
///
/// Explicit `builtin.*` uids are answered from the static table without a database
/// round-trip; everything else goes through the cached user list + built-ins.
pub async fn resolve(
    State(state): State<AppState>,
    JsonBody(req): JsonBody<ResolveRequest>,
) -> Result<Json<ResolveResponse>, CpError> {
    if let Some(uid) = req.pipeline.as_deref()
        && is_builtin_uid(uid)
    {
        return resolve_with(builtin_pipelines(), &req).map(Json);
    }
    let user = state.cache.get_or_load(&state.pipelines()).await?;
    let mut candidates = scope_pipelines(user, req.project_id.as_deref());
    candidates.extend(builtin_pipelines());
    let res = resolve_with(candidates, &req)?;
    tracing::debug!(
        mime = %req.mime,
        project_id = ?req.project_id,
        pipeline = %res.pipeline.uid,
        "resolved pipeline"
    );
    Ok(Json(res))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::{PipelineTrigger, StepDefinition};

    fn user_pdf(uid: &str, project_id: Option<&str>, index: Option<&str>) -> PipelineDefinition {
        PipelineDefinition {
            uid: uid.into(),
            name: uid.into(),
            description: None,
            version: 1,
            trigger: Some(PipelineTrigger {
                content_types: vec!["application/pdf".into()],
                filename_pattern: None,
                index_pattern: index.map(str::to_owned),
            }),
            steps: vec![
                StepDefinition::new("extract", "pdf_extractor"),
                StepDefinition::new("index", "meili_indexer").depends_on(["extract"]),
            ],
            builtin: false,
            project_id: project_id.map(str::to_owned),
        }
    }

    fn req(mime: &str) -> ResolveRequest {
        ResolveRequest {
            mime: mime.into(),
            filename: None,
            project_id: None,
            pipeline: None,
        }
    }

    #[test]
    fn request_body_parsing() {
        let r: ResolveRequest = serde_json::from_str(r#"{"mime":"application/pdf"}"#).unwrap();
        assert_eq!(r, req("application/pdf"));
        let r: ResolveRequest = serde_json::from_str(
            r#"{"mime":"text/csv","filename":"a.csv","project_id":"t1","pipeline":"x"}"#,
        )
        .unwrap();
        assert_eq!(r.filename.as_deref(), Some("a.csv"));
        assert_eq!(r.project_id.as_deref(), Some("t1"));
        assert_eq!(r.pipeline.as_deref(), Some("x"));
        assert!(serde_json::from_str::<ResolveRequest>(r#"{"filename":"a"}"#).is_err());
    }

    #[test]
    fn builtin_only_resolution() {
        let res = resolve_with(builtin_pipelines(), &req("application/pdf")).unwrap();
        assert_eq!(res.pipeline.uid, "builtin.pdf");
        assert_eq!(res.index_pattern, None);
        let err = resolve_with(builtin_pipelines(), &req("application/x-unknown")).unwrap_err();
        assert_eq!(err.code(), "no_pipeline");
        assert!(err.to_string().contains("no pipeline matches mime"));
    }

    #[test]
    fn user_pipeline_beats_builtin_and_tenant_beats_global() {
        let mut all = vec![
            user_pdf("global-pdf", None, Some("global_idx")),
            user_pdf("tenant-pdf", Some("t1"), Some("tenant_idx")),
        ];
        // Tenant scope: tenant > global > builtin.
        let mut c = scope_pipelines(all.clone(), Some("t1"));
        c.extend(builtin_pipelines());
        let mut r = req("application/pdf");
        r.project_id = Some("t1".into());
        let res = resolve_with(c, &r).unwrap();
        assert_eq!(res.pipeline.uid, "tenant-pdf");
        assert_eq!(res.index_pattern.as_deref(), Some("tenant_idx"));

        // Another tenant only sees global.
        let mut c = scope_pipelines(all.clone(), Some("t2"));
        assert_eq!(c.len(), 1);
        c.extend(builtin_pipelines());
        r.project_id = Some("t2".into());
        let res = resolve_with(c, &r).unwrap();
        assert_eq!(res.pipeline.uid, "global-pdf");

        // No user pipeline for this MIME → builtin.
        all.clear();
        let mut c = scope_pipelines(all, None);
        c.extend(builtin_pipelines());
        let res = resolve_with(c, &req("application/pdf")).unwrap();
        assert_eq!(res.pipeline.uid, "builtin.pdf");
    }

    #[test]
    fn explicit_pipeline_lookup() {
        let mut c = vec![user_pdf("mine", Some("t1"), None)];
        c.extend(builtin_pipelines());
        let mut r = req("application/octet-stream");
        r.pipeline = Some("builtin.csv".into());
        assert_eq!(
            resolve_with(c.clone(), &r).unwrap().pipeline.uid,
            "builtin.csv"
        );
        r.pipeline = Some("mine".into());
        r.project_id = Some("t1".into());
        assert_eq!(resolve_with(c.clone(), &r).unwrap().pipeline.uid, "mine");
        r.pipeline = Some("nope".into());
        assert_eq!(resolve_with(c, &r).unwrap_err().code(), "not_found");
    }

    #[tokio::test]
    async fn cache_set_and_invalidate() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        assert!(!cache.is_fresh().await);
        cache.set(vec![]).await;
        assert!(cache.is_fresh().await);
        cache.invalidate().await;
        assert!(!cache.is_fresh().await);
        let expired = PipelineCache::new(Duration::ZERO);
        expired.set(vec![]).await;
        assert!(!expired.is_fresh().await);
    }
}
