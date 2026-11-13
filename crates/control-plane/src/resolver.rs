//! `POST /internal/resolve`: pick the pipeline for an ingest request.
//!
//! The user-pipeline list is cached in memory for a few seconds so the gateway's
//! per-request resolve call does not turn into one Postgres round-trip per upload.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::State;
use meili_ingest_plugin_sdk::PipelineDefinition;
use meili_ingest_router::{PipelineRouter, RouteRequest};
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};

use crate::builtin_pipelines::{builtin_pipelines, is_builtin_uid};
use crate::error::CpError;
use crate::pipelines::PipelineRepo;
use crate::{AppState, JsonBody};

/// Default time-to-live of the cached user-pipeline list.
pub const DEFAULT_CACHE_TTL: Duration = Duration::from_secs(5);

/// The cached list and the generation it belongs to.
#[derive(Debug, Default)]
struct CacheState {
    /// Bumped by every [`PipelineCache::invalidate`] and [`PipelineCache::set`]. A
    /// load stores its result only when this did not move while it was reading the
    /// table, so a list read before a pipeline write can never replace the
    /// invalidation that write made.
    generation: u64,
    /// The list and the instant it was loaded. Shared, so a resolve takes a handle
    /// instead of a deep copy of every tenant's pipelines.
    entry: Option<(Instant, Arc<Vec<PipelineDefinition>>)>,
}

/// Time-bounded in-memory copy of every user pipeline (all tenants).
///
/// Per process: with several control-plane replicas, a write on one replica is seen
/// by the others when their copy expires (at most one TTL later).
#[derive(Debug, Clone)]
pub struct PipelineCache {
    inner: Arc<RwLock<CacheState>>,
    /// Held while one request reloads the list, so the requests that miss together
    /// run one query instead of one each. Never held across a read of a fresh list.
    reload: Arc<Mutex<()>>,
    /// Reloads that failed. A request that waited behind one fails with it instead
    /// of queueing a query of its own behind a database that just refused one.
    failed_reloads: Arc<AtomicU64>,
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
            inner: Arc::default(),
            reload: Arc::default(),
            failed_reloads: Arc::default(),
            ttl,
        }
    }

    /// Return the cached list when fresh, otherwise reload it from `repo`.
    pub async fn get_or_load(
        &self,
        repo: &PipelineRepo,
    ) -> Result<Arc<Vec<PipelineDefinition>>, CpError> {
        self.get_or_load_with(|| repo.list_all()).await
    }

    /// [`PipelineCache::get_or_load`] with the load as a closure, so tests can
    /// interleave a load with other calls.
    ///
    /// Concurrent misses share one load. The list is returned to the caller either
    /// way, but cached only when no [`PipelineCache::invalidate`] or
    /// [`PipelineCache::set`] ran during the load.
    pub async fn get_or_load_with<F, Fut>(
        &self,
        load: F,
    ) -> Result<Arc<Vec<PipelineDefinition>>, CpError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<Vec<PipelineDefinition>, CpError>>,
    {
        if let Some(list) = self.fresh().await {
            return Ok(list);
        }
        let failed_before = self.failed_reloads.load(Ordering::SeqCst);
        let _reloading = self.reload.lock().await;
        // Whoever held the gate may have loaded the list this request waited for.
        if let Some(list) = self.fresh().await {
            return Ok(list);
        }
        if self.failed_reloads.load(Ordering::SeqCst) != failed_before {
            return Err(CpError::Internal(
                "the pipeline list could not be loaded: the reload this request waited \
                 for failed"
                    .into(),
            ));
        }
        let generation = self.inner.read().await.generation;
        let list = match load().await {
            Ok(list) => Arc::new(list),
            Err(e) => {
                self.failed_reloads.fetch_add(1, Ordering::SeqCst);
                return Err(e);
            }
        };
        let mut state = self.inner.write().await;
        if state.generation == generation {
            state.entry = Some((Instant::now(), list.clone()));
        }
        Ok(list)
    }

    /// The cached list, when there is one and it has not expired.
    async fn fresh(&self) -> Option<Arc<Vec<PipelineDefinition>>> {
        match self.inner.read().await.entry.as_ref() {
            Some((at, list)) if at.elapsed() < self.ttl => Some(list.clone()),
            _ => None,
        }
    }

    /// Replace the cached list (used after writes in tests and by seeding).
    pub async fn set(&self, list: Vec<PipelineDefinition>) {
        let mut state = self.inner.write().await;
        state.generation += 1;
        state.entry = Some((Instant::now(), Arc::new(list)));
    }

    /// Drop the cached list so the next read hits the database.
    pub async fn invalidate(&self) {
        let mut state = self.inner.write().await;
        state.generation += 1;
        state.entry = None;
    }

    /// Whether a fresh list is currently cached.
    pub async fn is_fresh(&self) -> bool {
        self.fresh().await.is_some()
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
    #[serde(default, skip_serializing_if = "Option::is_none", alias = "project_id")]
    pub tenant_id: Option<String>,
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

/// Copy out the pipelines visible to `tenant_id`: global ones plus that tenant's, in
/// the order of `all`. Only those are cloned, never the other tenants' rows.
pub fn scope_pipelines(
    all: &[PipelineDefinition],
    tenant_id: Option<&str>,
) -> Vec<PipelineDefinition> {
    all.iter()
        .filter(|p| match (&p.tenant_id, tenant_id) {
            (None, _) => true,
            (Some(pid), Some(req)) => pid == req,
            (Some(_), None) => false,
        })
        .cloned()
        .collect()
}

/// Pure resolution over an already-loaded candidate list (user pipelines for the
/// tenant followed by the built-ins). Shared by the handler and the unit tests.
pub fn resolve_with(
    candidates: Vec<PipelineDefinition>,
    req: &ResolveRequest,
) -> Result<ResolveResponse, CpError> {
    let router = PipelineRouter::new(candidates);
    let tenant_id = req.tenant_id.as_deref();
    let pipeline = match &req.pipeline {
        Some(uid) => router.by_uid(uid, tenant_id).ok_or_else(|| {
            CpError::NotFound(format!(
                "pipeline {uid:?} not found (tenant_id={tenant_id:?})"
            ))
        })?,
        None => {
            let m = router
                .resolve(RouteRequest {
                    mime: &req.mime,
                    filename: req.filename.as_deref(),
                    tenant_id,
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
    let mut candidates = scope_pipelines(&user, req.tenant_id.as_deref());
    candidates.extend(builtin_pipelines());
    let res = resolve_with(candidates, &req)?;
    tracing::debug!(
        mime = %req.mime,
        tenant_id = ?req.tenant_id,
        pipeline = %res.pipeline.uid,
        "resolved pipeline"
    );
    Ok(Json(res))
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::{PipelineTrigger, StepDefinition};
    use std::sync::atomic::AtomicUsize;

    fn user_pdf(uid: &str, tenant_id: Option<&str>, index: Option<&str>) -> PipelineDefinition {
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
            tenant_id: tenant_id.map(str::to_owned),
        }
    }

    fn req(mime: &str) -> ResolveRequest {
        ResolveRequest {
            mime: mime.into(),
            filename: None,
            tenant_id: None,
            pipeline: None,
        }
    }

    #[test]
    fn request_body_parsing() {
        let r: ResolveRequest = serde_json::from_str(r#"{"mime":"application/pdf"}"#).unwrap();
        assert_eq!(r, req("application/pdf"));
        let r: ResolveRequest = serde_json::from_str(
            r#"{"mime":"text/csv","filename":"a.csv","tenant_id":"t1","pipeline":"x"}"#,
        )
        .unwrap();
        assert_eq!(r.filename.as_deref(), Some("a.csv"));
        assert_eq!(r.tenant_id.as_deref(), Some("t1"));
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
        let mut c = scope_pipelines(&all, Some("t1"));
        c.extend(builtin_pipelines());
        let mut r = req("application/pdf");
        r.tenant_id = Some("t1".into());
        let res = resolve_with(c, &r).unwrap();
        assert_eq!(res.pipeline.uid, "tenant-pdf");
        assert_eq!(res.index_pattern.as_deref(), Some("tenant_idx"));

        // Another tenant only sees global.
        let mut c = scope_pipelines(&all, Some("t2"));
        assert_eq!(c.len(), 1);
        c.extend(builtin_pipelines());
        r.tenant_id = Some("t2".into());
        let res = resolve_with(c, &r).unwrap();
        assert_eq!(res.pipeline.uid, "global-pdf");

        // No user pipeline for this MIME → builtin.
        all.clear();
        let mut c = scope_pipelines(&all, None);
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
        r.tenant_id = Some("t1".into());
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

    #[tokio::test]
    async fn fresh_reads_share_one_list() {
        // A resolve must not deep-copy every tenant's pipelines: fresh reads hand out
        // the same allocation.
        let cache = PipelineCache::new(Duration::from_secs(60));
        let load = || async {
            Ok(vec![
                user_pdf("a", Some("t1"), None),
                user_pdf("b", Some("t2"), None),
                user_pdf("g", None, None),
            ])
        };
        let first = cache.get_or_load_with(load).await.unwrap();
        let second = cache
            .get_or_load_with(|| async { panic!("the list is fresh") })
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&first, &second));

        // Scoping copies out the tenant's and the global rows only, in list order.
        let uids = |tenant| -> Vec<String> {
            scope_pipelines(&first, tenant)
                .into_iter()
                .map(|p| p.uid)
                .collect()
        };
        assert_eq!(uids(Some("t1")), ["a", "g"]);
        assert_eq!(uids(Some("t2")), ["b", "g"]);
        assert_eq!(uids(None), ["g"]);
    }

    #[tokio::test]
    async fn a_load_that_raced_an_invalidation_is_not_cached() {
        // A pipeline write commits and invalidates while a reload is reading the
        // table: the list that reload holds may predate the write, so it must not be
        // installed as fresh for a whole TTL.
        let cache = PipelineCache::new(Duration::from_secs(60));
        let before_write = vec![user_pdf("old", None, None)];
        cache
            .get_or_load_with(|| async {
                cache.invalidate().await;
                Ok(before_write)
            })
            .await
            .unwrap();
        assert!(
            !cache.is_fresh().await,
            "the pre-write list was cached over the invalidation"
        );

        // The next read loads again and that list is kept.
        let loads = AtomicUsize::new(0);
        for _ in 0..2 {
            cache
                .get_or_load_with(|| async {
                    loads.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![user_pdf("new", None, None)])
                })
                .await
                .unwrap();
        }
        assert_eq!(loads.load(Ordering::SeqCst), 1);
        assert!(cache.is_fresh().await);
    }

    #[tokio::test]
    async fn a_load_that_raced_a_set_does_not_replace_it() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        cache
            .get_or_load_with(|| async {
                cache.set(vec![user_pdf("seeded", None, None)]).await;
                Ok(vec![user_pdf("older", None, None)])
            })
            .await
            .unwrap();
        let list = cache
            .get_or_load_with(|| async { panic!("the seeded list is fresh") })
            .await
            .unwrap();
        assert_eq!(list[0].uid, "seeded");
    }

    #[tokio::test]
    async fn concurrent_misses_share_one_load() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let loads = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..20 {
            let (cache, loads) = (cache.clone(), loads.clone());
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load_with(|| async {
                        loads.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Ok(vec![user_pdf("p", None, None)])
                    })
                    .await
                    .map(|list| list.len())
            }));
        }
        for t in tasks {
            assert_eq!(t.await.unwrap().unwrap(), 1);
        }
        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "an expired cache must cost one query, not one per in-flight resolve"
        );
    }

    #[tokio::test]
    async fn a_failed_load_fails_its_waiters_without_a_query_each() {
        let cache = PipelineCache::new(Duration::from_secs(60));
        let loads = Arc::new(AtomicUsize::new(0));
        let mut tasks = Vec::new();
        for _ in 0..5 {
            let (cache, loads) = (cache.clone(), loads.clone());
            tasks.push(tokio::spawn(async move {
                cache
                    .get_or_load_with(|| async {
                        loads.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        Err(CpError::Db(sqlx::Error::PoolTimedOut))
                    })
                    .await
                    .map(|list| list.len())
            }));
        }
        for t in tasks {
            let err = t.await.unwrap().unwrap_err();
            assert_eq!(err.status(), axum::http::StatusCode::INTERNAL_SERVER_ERROR);
        }
        assert_eq!(
            loads.load(Ordering::SeqCst),
            1,
            "waiters must not queue one failing query each behind the first"
        );
        assert!(!cache.is_fresh().await);

        // A request that arrives after the failure tries again.
        let list = cache
            .get_or_load_with(|| async { Ok(vec![user_pdf("back", None, None)]) })
            .await
            .unwrap();
        assert_eq!(list[0].uid, "back");
        assert!(cache.is_fresh().await);
    }
}
