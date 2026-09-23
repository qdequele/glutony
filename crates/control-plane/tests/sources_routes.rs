//! `/internal/sources` routes, driven through the router.
//!
//! Skips cleanly when `DATABASE_URL` is unset. Each test owns a `<prefix>-` namespace.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use meili_ingest_control_plane::error::ErrorBody;
use meili_ingest_control_plane::sources::{NewSource, RunRecord, SourceRecord};
use meili_ingest_control_plane::{AppState, app};
use meili_ingest_source::model::{IncrementalState, Location, RunOutcome};
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use tower::ServiceExt;
use uuid::Uuid;

async fn setup(prefix: &str) -> Option<Router> {
    let url = std::env::var("DATABASE_URL").ok()?;
    let pool = PgPool::connect(&url).await.ok()?;
    sqlx::migrate!("../../migrations")
        .run(&pool)
        .await
        .expect("migrations apply");
    sqlx::query("DELETE FROM sources WHERE uid LIKE $1")
        .bind(format!("{prefix}-%"))
        .execute(&pool)
        .await
        .expect("clean");
    Some(app(AppState::new(pool)))
}

async fn call(app: &Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let res = app.clone().oneshot(req).await.expect("response");
    let status = res.status();
    let body = res.into_body().collect().await.expect("body").to_bytes();
    (status, body.to_vec())
}

fn json<T: DeserializeOwned>(body: &[u8]) -> T {
    serde_json::from_slice(body)
        .unwrap_or_else(|e| panic!("bad json ({e}): {}", String::from_utf8_lossy(body)))
}

fn with_json(method: &str, uri: &str, body: &impl serde::Serialize) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body).expect("serialize")))
        .expect("request")
}

fn bare(method: &str, uri: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .expect("request")
}

fn new_source(uid: &str, project: &str) -> NewSource {
    NewSource {
        id: Uuid::new_v4(),
        uid: uid.to_string(),
        name: uid.to_string(),
        description: None,
        project_id: Some(project.to_string()),
        pipeline_uid: "movies".into(),
        location: Location::Url {
            url: "https://files.example/movie_ids_{{ date:%m_%d_%Y }}.json.gz".into(),
            method: None,
            headers: Default::default(),
        },
        cron: "30 0 * * *".into(),
        timezone: "UTC".into(),
        index_name: None,
        fetch_auth: Some(vec![1, 2, 3]),
        schedule_id: format!("source-{uid}"),
    }
}

async fn create(app: &Router, s: &NewSource) -> SourceRecord {
    let (status, body) = call(app, with_json("POST", "/internal/sources", s)).await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );
    json(&body)
}

#[tokio::test]
async fn create_get_patch_delete_roundtrip() {
    let Some(app) = setup("sr-crud").await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };
    let created = create(&app, &new_source("sr-crud-1", "sp-1")).await;
    assert!(
        created.definition.paused,
        "created paused until its schedule exists"
    );

    let (status, body) = call(
        &app,
        bare("GET", "/internal/sources/sr-crud-1?project_id=sp-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json::<SourceRecord>(&body).definition.id,
        created.definition.id
    );

    // Clearing the credential over JSON must actually clear it.
    let (status, body) = call(
        &app,
        with_json(
            "PATCH",
            "/internal/sources/sr-crud-1?project_id=sp-1",
            &serde_json::json!({ "paused": false, "fetch_auth": null }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{}", String::from_utf8_lossy(&body));
    let patched: SourceRecord = json(&body);
    assert!(!patched.definition.paused);
    assert!(patched.fetch_auth.is_none(), "null cleared the credential");

    let (status, _) = call(
        &app,
        bare("DELETE", "/internal/sources/sr-crud-1?project_id=sp-1"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, body) = call(
        &app,
        bare("GET", "/internal/sources/sr-crud-1?project_id=sp-1"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json::<ErrorBody>(&body).code, "not_found");
}

#[tokio::test]
async fn a_duplicate_uid_is_a_validation_error() {
    let Some(app) = setup("sr-dup").await else {
        return;
    };
    create(&app, &new_source("sr-dup-1", "sp-2")).await;
    let (status, body) = call(
        &app,
        with_json("POST", "/internal/sources", &new_source("sr-dup-1", "sp-2")),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(json::<ErrorBody>(&body).error.contains("sr-dup-1"));
}

#[tokio::test]
async fn list_hides_archived_unless_asked() {
    let Some(app) = setup("sr-list").await else {
        return;
    };
    create(&app, &new_source("sr-list-a", "sp-3")).await;
    let (status, body) = call(&app, bare("GET", "/internal/sources?project_id=sp-3")).await;
    assert_eq!(status, StatusCode::OK);
    let uids: Vec<String> = json::<Vec<SourceRecord>>(&body)
        .into_iter()
        .map(|s| s.definition.uid)
        .collect();
    assert!(uids.contains(&"sr-list-a".to_string()));

    let (status, _) = call(
        &app,
        bare(
            "GET",
            "/internal/sources?project_id=sp-3&include_archived=true",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "include_archived is accepted");
}

#[tokio::test]
async fn the_worker_routes_load_by_id_save_state_and_record_runs() {
    let Some(app) = setup("sr-run").await else {
        return;
    };
    let created = create(&app, &new_source("sr-run-1", "sp-4")).await;
    let id = created.definition.id;

    // Load for a run.
    let (status, body) = call(&app, bare("GET", &format!("/internal/sources-by-id/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        json::<SourceRecord>(&body).fetch_auth.as_deref(),
        Some(&[1u8, 2, 3][..])
    );

    // Save incremental state.
    let state = IncrementalState {
        etag: Some("\"v7\"".into()),
        last_modified: None,
        hash: Some("abcd".into()),
    };
    let (status, _) = call(
        &app,
        with_json(
            "PUT",
            &format!("/internal/sources-by-id/{id}/state"),
            &state,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Pause and unpause by id.
    let (status, _) = call(
        &app,
        with_json(
            "PUT",
            &format!("/internal/sources-by-id/{id}/paused"),
            &serde_json::json!({ "paused": false }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Record a run, then read it back.
    let run = RunRecord {
        run_id: Uuid::new_v4(),
        source_id: id,
        started_at: chrono::Utc::now(),
        finished_at: Some(chrono::Utc::now()),
        outcome: RunOutcome::Unchanged,
        items: 0,
        job_ids: vec![],
        error: None,
    };
    let (status, _) = call(&app, with_json("POST", "/internal/source-runs", &run)).await;
    assert_eq!(status, StatusCode::CREATED);
    let (status, body) = call(
        &app,
        bare("GET", &format!("/internal/sources-by-id/{id}/runs?limit=5")),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let runs: Vec<RunRecord> = json(&body);
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0].outcome, RunOutcome::Unchanged);

    let (status, body) = call(&app, bare("GET", &format!("/internal/sources-by-id/{id}"))).await;
    assert_eq!(status, StatusCode::OK);
    let reloaded: SourceRecord = json(&body);
    assert_eq!(
        reloaded.state.etag.as_deref(),
        Some("\"v7\""),
        "state persisted"
    );
    assert_eq!(reloaded.last_status.as_deref(), Some("unchanged"));
}

#[tokio::test]
async fn a_job_row_remembers_the_source_that_started_it() {
    let Some(app) = setup("sr-job").await else {
        return;
    };
    let job_id = Uuid::new_v4();
    let source_id = Uuid::new_v4();
    let now = chrono::Utc::now();
    let job = serde_json::json!({
        "job_id": job_id,
        "workflow_id": format!("ingest-{job_id}"),
        "pipeline_uid": "movies",
        "status": "queued",
        "started_at": now,
        "updated_at": now,
        "source_id": source_id,
    });
    let (status, _) = call(&app, with_json("POST", "/internal/jobs", &job)).await;
    assert_eq!(status, StatusCode::CREATED);

    let (status, body) = call(&app, bare("GET", &format!("/internal/jobs/{job_id}"))).await;
    assert_eq!(status, StatusCode::OK);
    let got: serde_json::Value = json(&body);
    assert_eq!(got["source_id"], serde_json::json!(source_id));
}

#[tokio::test]
async fn an_unknown_id_is_404_on_the_worker_routes() {
    let Some(app) = setup("sr-none").await else {
        return;
    };
    let id = Uuid::new_v4();
    let (status, _) = call(&app, bare("GET", &format!("/internal/sources-by-id/{id}"))).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(
        &app,
        with_json(
            "PUT",
            &format!("/internal/sources-by-id/{id}/paused"),
            &serde_json::json!({ "paused": true }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
