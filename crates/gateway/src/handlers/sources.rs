//! `/sources`: fetch a location on a cron and run it through a pipeline (spec *API*).
//!
//! Every route answers 501 until `SOURCE_SECRET_KEY` is configured (a source's fetch
//! credential is sealed with it) and until Temporal schedules are wired.
//!
//! Create is not atomic across Postgres and Temporal, so it runs in an order whose
//! every failure is visible and repairable: validate everything, write the row paused,
//! create the schedule, then unpause. If the schedule cannot be created the row is
//! removed again, so no half-created source is left behind.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use meili_ingest_source::{SecretKey, SourceRunInput, seal_json};
use serde::Deserialize;
use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::connections::valid_uid;
use crate::context::resolve_project_id;
use crate::error::GatewayError;
use crate::schedules::SourceSchedule;
use crate::sources::{
    CreateSource, NewSource, RunRecord, SourcePatch, SourceRecord, SourceView, UpdateSource,
    require_pinned, validate_location,
};
use crate::state::AppState;

fn parse_body<T: DeserializeOwned>(body: &Bytes) -> Result<T, GatewayError> {
    serde_json::from_slice(body)
        .map_err(|e| GatewayError::BadRequest(format!("invalid source body: {e}")))
}

fn check_uid(uid: &str) -> Result<(), GatewayError> {
    if valid_uid(uid) {
        Ok(())
    } else {
        Err(GatewayError::BadRequest(format!(
            "invalid source uid {uid:?}: must be 1-128 characters of [a-zA-Z0-9._-]"
        )))
    }
}

fn not_found(uid: &str) -> GatewayError {
    GatewayError::NotFound(format!("source {uid:?} not found"))
}

fn non_blank(s: Option<String>) -> Option<String> {
    s.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

/// Seal a fetch credential.
fn seal(key: &SecretKey, auth: &meili_ingest_source::FetchAuth) -> Result<Vec<u8>, GatewayError> {
    seal_json(key, auth).map_err(|e| GatewayError::Internal(e.to_string()))
}

/// Load the source in exactly the caller's scope, for a write. Reads fall back to the
/// global row; writes never do, so a tenant cannot reach a global source.
async fn owned(
    state: &AppState,
    uid: &str,
    project_id: Option<&str>,
) -> Result<SourceRecord, GatewayError> {
    state
        .control_plane
        .get_source(uid, project_id)
        .await?
        .filter(|r| r.definition.project_id.as_deref() == project_id)
        .ok_or_else(|| not_found(uid))
}

/// The pipeline a source will run, checked to supply its own destination.
async fn schedulable_pipeline(
    state: &AppState,
    pipeline: &str,
    project_id: Option<&str>,
) -> Result<(), GatewayError> {
    let def = match state.control_plane.get_pipeline(pipeline, project_id).await {
        Ok(def) => def,
        Err(GatewayError::NotFound(_)) => {
            return Err(GatewayError::Unprocessable(format!(
                "pipeline {pipeline:?} does not exist"
            )));
        }
        Err(e) => return Err(e),
    };
    require_pinned(&def)
}

fn schedule_of(r: &SourceRecord) -> SourceSchedule {
    let d = &r.definition;
    SourceSchedule {
        schedule_id: d.schedule_id.clone(),
        source_id: d.id,
        project_id: d.project_id.clone(),
        cron: d.cron.clone(),
        timezone: d.timezone.clone(),
        paused: d.paused,
    }
}

/// Query of `GET /sources`.
#[derive(Debug, Default, Deserialize)]
pub struct ListQuery {
    /// Include sources archived because their pipeline was deleted.
    #[serde(default)]
    pub include_archived: bool,
}

/// `GET /sources`.
pub async fn list_sources(
    State(state): State<AppState>,
    Query(q): Query<ListQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<SourceView>>, GatewayError> {
    let key = state.connections.key()?;
    let project_id = resolve_project_id(&headers, &state.config);
    let rows = state
        .control_plane
        .list_sources(project_id.as_deref(), q.include_archived)
        .await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| SourceView::from_record(r, key, None))
            .collect(),
    ))
}

/// `POST /sources`.
pub async fn create_source(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<SourceView>), GatewayError> {
    let key = state.connections.key()?;
    let req: CreateSource = parse_body(&body)?;
    let uid = req.uid.trim().to_owned();
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);

    validate_location(&req.location, &req.timezone, &state.fetch_policy).await?;
    schedulable_pipeline(&state, &req.pipeline, project_id.as_deref()).await?;
    let fetch_auth = req.auth.as_ref().map(|a| seal(key, a)).transpose()?;

    let id = Uuid::new_v4();
    let new = NewSource {
        id,
        uid: uid.clone(),
        name: non_blank(req.name).unwrap_or_else(|| uid.clone()),
        description: non_blank(req.description),
        project_id: project_id.clone(),
        pipeline_uid: req.pipeline,
        location: req.location,
        cron: req.cron,
        timezone: req.timezone,
        index_name: non_blank(req.index),
        fetch_auth,
        schedule_id: SourceRunInput::schedule_id(id),
    };

    // 1. The row, paused.
    let record = state.control_plane.create_source(&new).await?;

    // 2. The schedule, live. Temporal validates the cron here (Decision 10); if it or
    //    anything after fails, undo so no half-created source remains.
    let mut schedule = schedule_of(&record);
    schedule.paused = false;
    if let Err(e) = state.schedules.create(&schedule).await {
        undo_create(&state, &record, false).await;
        return Err(e);
    }

    // 3. Unpause the row so it mirrors the schedule.
    let patch = SourcePatch {
        paused: Some(false),
        ..Default::default()
    };
    let record = match state
        .control_plane
        .update_source(&uid, project_id.as_deref(), &patch)
        .await
    {
        Ok(Some(r)) => r,
        Ok(None) => {
            undo_create(&state, &record, true).await;
            return Err(GatewayError::Internal(format!(
                "source {uid:?} vanished while it was being created"
            )));
        }
        Err(e) => {
            undo_create(&state, &record, true).await;
            return Err(e);
        }
    };

    let next = state
        .schedules
        .describe(&record.definition.schedule_id)
        .await
        .ok()
        .flatten()
        .and_then(|i| i.next_run_at);
    tracing::info!(uid = %uid, project_id = ?project_id, "source created");
    Ok((
        StatusCode::CREATED,
        Json(SourceView::from_record(record, key, next)),
    ))
}

/// Best-effort rollback of a failed create. Failures are logged, not returned: the
/// caller is already reporting the error that caused the rollback.
async fn undo_create(state: &AppState, record: &SourceRecord, schedule_created: bool) {
    let d = &record.definition;
    if schedule_created && let Err(e) = state.schedules.delete(&d.schedule_id).await {
        tracing::error!(uid = %d.uid, schedule = %d.schedule_id, "rollback: could not delete schedule: {e}");
    }
    if let Err(e) = state
        .control_plane
        .delete_source(&d.uid, d.project_id.as_deref())
        .await
    {
        tracing::error!(uid = %d.uid, "rollback: could not delete source row: {e}");
    }
}

/// `GET /sources/{uid}` — includes the next run from Temporal.
pub async fn get_source(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<Json<SourceView>, GatewayError> {
    let key = state.connections.key()?;
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);
    let record = state
        .control_plane
        .get_source(&uid, project_id.as_deref())
        .await?
        .ok_or_else(|| not_found(&uid))?;
    // Temporal is the truth for whether it fires and when; the row only mirrors it.
    let info = state
        .schedules
        .describe(&record.definition.schedule_id)
        .await
        .unwrap_or_else(|e| {
            tracing::warn!(uid = %uid, "could not describe schedule: {e}");
            None
        });
    let mut view = SourceView::from_record(record, key, info.as_ref().and_then(|i| i.next_run_at));
    if let Some(i) = info {
        view.paused = i.paused;
    }
    Ok(Json(view))
}

/// `PATCH /sources/{uid}`.
pub async fn patch_source(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<SourceView>, GatewayError> {
    let key = state.connections.key()?;
    check_uid(&uid)?;
    let req: UpdateSource = parse_body(&body)?;
    let project_id = resolve_project_id(&headers, &state.config);
    let stored = owned(&state, &uid, project_id.as_deref()).await?;
    let d = &stored.definition;

    let timezone = req.timezone.clone().unwrap_or_else(|| d.timezone.clone());
    if req.location.is_some() || req.timezone.is_some() {
        let location = req.location.as_ref().unwrap_or(&d.location);
        validate_location(location, &timezone, &state.fetch_policy).await?;
    }
    if let Some(pipeline) = &req.pipeline {
        schedulable_pipeline(&state, pipeline, project_id.as_deref()).await?;
    }
    let fetch_auth = match &req.auth {
        None => None,
        Some(None) => Some(None),
        Some(Some(a)) => Some(Some(seal(key, a)?)),
    };

    // The schedule first: Temporal validates the cron, and a rejection leaves nothing
    // changed anywhere.
    if req.cron.is_some() || req.timezone.is_some() {
        let mut schedule = schedule_of(&stored);
        if let Some(c) = &req.cron {
            schedule.cron = c.clone();
        }
        schedule.timezone = timezone.clone();
        state.schedules.update(&schedule).await?;
    }

    let patch = SourcePatch {
        name: non_blank(req.name),
        description: req.description,
        pipeline_uid: req.pipeline,
        location: req.location,
        cron: req.cron,
        timezone: req.timezone,
        index_name: req.index,
        fetch_auth,
        paused: None,
    };
    let record = state
        .control_plane
        .update_source(&uid, project_id.as_deref(), &patch)
        .await?
        .ok_or_else(|| not_found(&uid))?;
    Ok(Json(SourceView::from_record(record, key, None)))
}

async fn set_paused(
    state: &AppState,
    uid: &str,
    headers: &HeaderMap,
    paused: bool,
) -> Result<StatusCode, GatewayError> {
    state.connections.key()?;
    check_uid(uid)?;
    let project_id = resolve_project_id(headers, &state.config);
    let stored = owned(state, uid, project_id.as_deref()).await?;
    state
        .schedules
        .set_paused(&stored.definition.schedule_id, paused)
        .await?;
    state
        .control_plane
        .set_source_paused(stored.definition.id, paused)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

/// `POST /sources/{uid}/pause`.
pub async fn pause_source(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, GatewayError> {
    set_paused(&state, &uid, &headers, true).await
}

/// `POST /sources/{uid}/unpause`.
pub async fn unpause_source(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, GatewayError> {
    set_paused(&state, &uid, &headers, false).await
}

/// `POST /sources/{uid}/run` — run now, off-schedule. Skipped by Temporal if a run is
/// already in flight.
pub async fn run_source(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<(StatusCode, Json<serde_json::Value>), GatewayError> {
    state.connections.key()?;
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);
    let stored = owned(&state, &uid, project_id.as_deref()).await?;
    if stored.definition.archived_at.is_some() {
        return Err(GatewayError::Unprocessable(format!(
            "source {uid:?} is archived: its pipeline was deleted"
        )));
    }
    state
        .schedules
        .trigger(&stored.definition.schedule_id)
        .await?;
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "status": "triggered" })),
    ))
}

/// `DELETE /sources/{uid}` — the schedule first, so nothing fires against a row that
/// is about to disappear.
pub async fn delete_source(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, GatewayError> {
    state.connections.key()?;
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);
    let stored = owned(&state, &uid, project_id.as_deref()).await?;
    state
        .schedules
        .delete(&stored.definition.schedule_id)
        .await?;
    state
        .control_plane
        .delete_source(&uid, project_id.as_deref())
        .await?;
    tracing::info!(uid = %uid, project_id = ?project_id, "source deleted");
    Ok(StatusCode::NO_CONTENT)
}

/// Query of `GET /sources/{uid}/runs`.
#[derive(Debug, Default, Deserialize)]
pub struct RunsQuery {
    /// Maximum runs, newest first (default 20, at most 200).
    #[serde(default)]
    pub limit: Option<i64>,
}

/// `GET /sources/{uid}/runs`.
pub async fn list_runs(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    Query(q): Query<RunsQuery>,
    headers: HeaderMap,
) -> Result<Json<Vec<RunRecord>>, GatewayError> {
    state.connections.key()?;
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);
    let record = state
        .control_plane
        .get_source(&uid, project_id.as_deref())
        .await?
        .ok_or_else(|| not_found(&uid))?;
    let limit = q.limit.unwrap_or(20).clamp(1, 200);
    Ok(Json(
        state
            .control_plane
            .list_source_runs(record.definition.id, limit)
            .await?,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use async_trait::async_trait;
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use meili_ingest_blob::BlobStore;
    use meili_ingest_source::{HostPolicy, SecretKey};
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::connections::ConnectionConfig;
    use crate::error::GatewayError;
    use crate::schedules::{ScheduleClient, ScheduleInfo, SourceSchedule};
    use crate::state::{AppState, GatewayConfig};
    use crate::test_support::{FakeStarter, sample_pipeline};

    /// Records every call; `reject_create` makes `create` fail like a bad cron would.
    #[derive(Default)]
    struct FakeSchedules {
        calls: Mutex<Vec<String>>,
        created: Mutex<Vec<SourceSchedule>>,
        reject_create: bool,
    }

    impl FakeSchedules {
        fn calls(&self) -> Vec<String> {
            self.calls.lock().expect("lock").clone()
        }
        fn log(&self, c: String) {
            self.calls.lock().expect("lock").push(c);
        }
    }

    #[async_trait]
    impl ScheduleClient for FakeSchedules {
        async fn create(&self, s: &SourceSchedule) -> Result<(), GatewayError> {
            self.log(format!(
                "create {} {} paused={}",
                s.schedule_id, s.cron, s.paused
            ));
            if self.reject_create {
                return Err(GatewayError::Unprocessable("invalid cron string".into()));
            }
            self.created.lock().expect("lock").push(s.clone());
            Ok(())
        }
        async fn update(&self, s: &SourceSchedule) -> Result<(), GatewayError> {
            self.log(format!(
                "update {} {} {}",
                s.schedule_id, s.cron, s.timezone
            ));
            Ok(())
        }
        async fn delete(&self, id: &str) -> Result<(), GatewayError> {
            self.log(format!("delete {id}"));
            Ok(())
        }
        async fn set_paused(&self, id: &str, paused: bool) -> Result<(), GatewayError> {
            self.log(format!("paused {id} {paused}"));
            Ok(())
        }
        async fn trigger(&self, id: &str) -> Result<(), GatewayError> {
            self.log(format!("trigger {id}"));
            Ok(())
        }
        async fn describe(&self, _id: &str) -> Result<Option<ScheduleInfo>, GatewayError> {
            Ok(Some(ScheduleInfo {
                paused: false,
                next_run_at: None,
            }))
        }
    }

    fn key() -> Arc<SecretKey> {
        Arc::new(SecretKey::from_bytes([6; 32]))
    }

    fn app(cp: &MockServer, schedules: Arc<FakeSchedules>, with_key: bool) -> Router {
        let config = GatewayConfig {
            control_plane_url: cp.uri(),
            ..GatewayConfig::default()
        };
        let state = AppState::new(
            config,
            Arc::new(FakeStarter::default()),
            BlobStore::memory(),
            reqwest::Client::new(),
        )
        .with_connections(ConnectionConfig::new(with_key.then(key), HostPolicy::Any))
        .with_sources(
            schedules,
            HostPolicy::parse("files.example").expect("policy"),
        );
        crate::router(state)
    }

    async fn call(app: &Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
        let res = app.clone().oneshot(req).await.expect("response");
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("body");
        (
            status,
            serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null),
        )
    }

    fn req(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    fn create_body() -> serde_json::Value {
        serde_json::json!({
            "uid": "tmdb",
            "pipeline": "movies",
            "location": {
                "kind": "url",
                "url": "http://files.example/movie_ids_{{ date-1d:%m_%d_%Y }}.json.gz"
            },
            "cron": "30 0 * * *",
            "auth": { "kind": "bearer", "token": "s3cret" }
        })
    }

    fn record(paused: bool) -> serde_json::Value {
        serde_json::json!({
            "id": "11111111-1111-1111-1111-111111111111",
            "uid": "tmdb",
            "name": "tmdb",
            "pipeline_uid": "movies",
            "location": {
                "kind": "url",
                "url": "http://files.example/movie_ids_{{ date-1d:%m_%d_%Y }}.json.gz"
            },
            "cron": "30 0 * * *",
            "timezone": "UTC",
            "paused": paused,
            "schedule_id": "source-11111111-1111-1111-1111-111111111111",
        })
    }

    async fn mount_pipeline(cp: &MockServer, pinned: bool) {
        let mut p = sample_pipeline("movies", None);
        if pinned {
            p.steps[0].config = serde_json::json!({ "connection": "prod-movies" });
        }
        Mock::given(method("GET"))
            .and(path("/pipelines/movies"))
            .respond_with(ResponseTemplate::new(200).set_body_json(p))
            .mount(cp)
            .await;
    }

    async fn mount_source(cp: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/internal/sources/tmdb"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(false)))
            .mount(cp)
            .await;
    }

    #[tokio::test]
    async fn every_route_is_501_without_a_secret_key() {
        let cp = MockServer::start().await;
        let schedules = Arc::new(FakeSchedules::default());
        let app = app(&cp, schedules.clone(), false);
        let (status, _) = call(&app, req("POST", "/sources", create_body())).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert!(schedules.calls().is_empty());
    }

    #[tokio::test]
    async fn create_writes_paused_then_schedules_then_unpauses() {
        let cp = MockServer::start().await;
        mount_pipeline(&cp, true).await;
        Mock::given(method("POST"))
            .and(path("/internal/sources"))
            .respond_with(ResponseTemplate::new(201).set_body_json(record(true)))
            .mount(&cp)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/internal/sources/tmdb"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(false)))
            .mount(&cp)
            .await;
        let schedules = Arc::new(FakeSchedules::default());
        let app = app(&cp, schedules.clone(), true);

        let (status, body) = call(&app, req("POST", "/sources", create_body())).await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert!(
            !body.to_string().contains("s3cret"),
            "the token is never returned"
        );

        // The schedule is created live, with the source's cron.
        let created = schedules.created.lock().expect("lock").clone();
        assert_eq!(created.len(), 1);
        assert_eq!(created[0].cron, "30 0 * * *");
        assert!(!created[0].paused);

        // The control plane saw a sealed credential, and was then asked to unpause.
        let requests = cp.received_requests().await.expect("recorded");
        let post = requests
            .iter()
            .find(|r| r.method.as_str() == "POST")
            .expect("row written");
        assert!(!String::from_utf8_lossy(&post.body).contains("s3cret"));
        let patch = requests
            .iter()
            .find(|r| r.method.as_str() == "PATCH")
            .expect("row unpaused");
        let patch: serde_json::Value = serde_json::from_slice(&patch.body).expect("json");
        assert_eq!(patch, serde_json::json!({ "paused": false }));
    }

    #[tokio::test]
    async fn a_pipeline_without_a_connection_is_422_and_nothing_is_created() {
        let cp = MockServer::start().await;
        mount_pipeline(&cp, false).await;
        Mock::given(method("POST"))
            .and(path("/internal/sources"))
            .respond_with(ResponseTemplate::new(201))
            .expect(0)
            .mount(&cp)
            .await;
        let schedules = Arc::new(FakeSchedules::default());
        let (status, body) = call(
            &app(&cp, schedules.clone(), true),
            req("POST", "/sources", create_body()),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            body["error"].as_str().unwrap_or("").contains("connection"),
            "{body}"
        );
        assert!(schedules.calls().is_empty());
    }

    #[tokio::test]
    async fn a_host_outside_the_fetch_policy_is_422() {
        let cp = MockServer::start().await;
        let mut body = create_body();
        body["location"]["url"] = serde_json::json!("http://169.254.169.254/latest/meta-data");
        let (status, _) = call(
            &app(&cp, Arc::new(FakeSchedules::default()), true),
            req("POST", "/sources", body),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    }

    #[tokio::test]
    async fn a_rejected_cron_rolls_the_row_back() {
        let cp = MockServer::start().await;
        mount_pipeline(&cp, true).await;
        Mock::given(method("POST"))
            .and(path("/internal/sources"))
            .respond_with(ResponseTemplate::new(201).set_body_json(record(true)))
            .mount(&cp)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/internal/sources/tmdb"))
            .respond_with(ResponseTemplate::new(204))
            .expect(1)
            .mount(&cp)
            .await;
        let schedules = Arc::new(FakeSchedules {
            reject_create: true,
            ..Default::default()
        });
        let (status, _) = call(
            &app(&cp, schedules.clone(), true),
            req("POST", "/sources", create_body()),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
        // `expect(1)` on the DELETE is verified when `cp` drops: the row was removed.
        assert!(
            !schedules.calls().iter().any(|c| c.starts_with("delete")),
            "no schedule was created, so none is deleted"
        );
    }

    #[tokio::test]
    async fn changing_the_cron_updates_the_schedule_and_a_rename_does_not() {
        let cp = MockServer::start().await;
        mount_source(&cp).await;
        Mock::given(method("PATCH"))
            .and(path("/internal/sources/tmdb"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(false)))
            .mount(&cp)
            .await;
        let schedules = Arc::new(FakeSchedules::default());
        let app = app(&cp, schedules.clone(), true);

        let (status, _) = call(
            &app,
            req(
                "PATCH",
                "/sources/tmdb",
                serde_json::json!({ "cron": "0 9 * * *" }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            schedules.calls(),
            vec!["update source-11111111-1111-1111-1111-111111111111 0 9 * * * UTC".to_string()]
        );

        let (status, _) = call(
            &app,
            req(
                "PATCH",
                "/sources/tmdb",
                serde_json::json!({ "name": "TMDB" }),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(schedules.calls().len(), 1, "a rename touches no schedule");
    }

    #[tokio::test]
    async fn run_triggers_and_delete_removes_the_schedule_first() {
        let cp = MockServer::start().await;
        mount_source(&cp).await;
        Mock::given(method("DELETE"))
            .and(path("/internal/sources/tmdb"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&cp)
            .await;
        let schedules = Arc::new(FakeSchedules::default());
        let app = app(&cp, schedules.clone(), true);

        let (status, body) = call(
            &app,
            req("POST", "/sources/tmdb/run", serde_json::json!({})),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{body}");
        let (status, _) = call(
            &app,
            Request::builder()
                .method("DELETE")
                .uri("/sources/tmdb")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(
            schedules.calls(),
            vec![
                "trigger source-11111111-1111-1111-1111-111111111111".to_string(),
                "delete source-11111111-1111-1111-1111-111111111111".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn a_tenant_cannot_trigger_a_global_source() {
        let cp = MockServer::start().await;
        mount_source(&cp).await; // the record has no project_id: it is global
        let schedules = Arc::new(FakeSchedules::default());
        let req = Request::builder()
            .method("POST")
            .uri("/sources/tmdb/run")
            .header("x-meili-project-id", "tenant-1")
            .body(Body::empty())
            .expect("request");
        let (status, _) = call(&app(&cp, schedules.clone(), true), req).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert!(schedules.calls().is_empty());
    }

    #[tokio::test]
    async fn deleting_a_pipeline_deletes_the_schedules_of_the_sources_it_archived() {
        let cp = MockServer::start().await;
        Mock::given(method("DELETE"))
            .and(path("/pipelines/movies"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "archived_sources": ["22222222-2222-2222-2222-222222222222"]
            })))
            .mount(&cp)
            .await;
        let schedules = Arc::new(FakeSchedules::default());
        let (status, _) = call(
            &app(&cp, schedules.clone(), true),
            Request::builder()
                .method("DELETE")
                .uri("/pipelines/movies")
                .body(Body::empty())
                .expect("request"),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NO_CONTENT,
            "the public contract is unchanged"
        );
        assert_eq!(
            schedules.calls(),
            vec!["delete source-22222222-2222-2222-2222-222222222222".to_string()]
        );
    }
}
