//! `/internal/connections` routes, driven through the router like `db.rs` does.
//!
//! Skips cleanly when `DATABASE_URL` is unset. Each test owns a `<prefix>-` namespace.

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use meili_ingest_control_plane::connections::{ConnectionRecord, NewConnection};
use meili_ingest_control_plane::error::ErrorBody;
use meili_ingest_control_plane::{AppState, app};
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
    sqlx::query("DELETE FROM meili_connections WHERE uid LIKE $1")
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

fn new_connection(uid: &str, project_id: &str) -> NewConnection {
    NewConnection {
        id: Uuid::new_v4(),
        uid: uid.to_string(),
        name: uid.to_string(),
        project_id: Some(project_id.to_string()),
        host: "https://movies.example".into(),
        api_key: vec![1, 2, 3],
    }
}

#[tokio::test]
async fn create_get_patch_delete_roundtrip() {
    let Some(app) = setup("cr-crud").await else {
        eprintln!("DATABASE_URL unset; skipping");
        return;
    };

    let (status, body) = call(
        &app,
        with_json(
            "POST",
            "/internal/connections",
            &new_connection("cr-crud-1", "rp-1"),
        ),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::CREATED,
        "{}",
        String::from_utf8_lossy(&body)
    );
    let created: ConnectionRecord = json(&body);
    assert_eq!(
        created.api_key,
        vec![1, 2, 3],
        "sealed bytes pass through untouched"
    );

    let (status, body) = call(
        &app,
        bare("GET", "/internal/connections/cr-crud-1?project_id=rp-1"),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(json::<ConnectionRecord>(&body).id, created.id);

    let (status, body) = call(
        &app,
        with_json(
            "PATCH",
            "/internal/connections/cr-crud-1?project_id=rp-1",
            &serde_json::json!({ "name": "Movies" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let patched: ConnectionRecord = json(&body);
    assert_eq!(patched.name, "Movies");
    assert_eq!(patched.api_key, vec![1, 2, 3], "omitted key is kept");

    let (status, _) = call(
        &app,
        bare("DELETE", "/internal/connections/cr-crud-1?project_id=rp-1"),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (status, body) = call(
        &app,
        bare("GET", "/internal/connections/cr-crud-1?project_id=rp-1"),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(json::<ErrorBody>(&body).code, "not_found");
}

#[tokio::test]
async fn a_duplicate_uid_is_a_clear_validation_error_not_a_500() {
    let Some(app) = setup("cr-dup").await else {
        return;
    };
    let first = new_connection("cr-dup-1", "rp-2");
    let (status, _) = call(&app, with_json("POST", "/internal/connections", &first)).await;
    assert_eq!(status, StatusCode::CREATED);

    let mut second = new_connection("cr-dup-1", "rp-2");
    second.id = Uuid::new_v4();
    let (status, body) = call(&app, with_json("POST", "/internal/connections", &second)).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "validation");
    assert!(err.error.contains("cr-dup-1"), "{}", err.error);
}

#[tokio::test]
async fn missing_connections_are_404_for_patch_and_delete() {
    let Some(app) = setup("cr-miss").await else {
        return;
    };
    let (status, _) = call(
        &app,
        with_json(
            "PATCH",
            "/internal/connections/cr-miss-none?project_id=rp-3",
            &serde_json::json!({ "name": "x" }),
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = call(
        &app,
        bare(
            "DELETE",
            "/internal/connections/cr-miss-none?project_id=rp-3",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn used_by_and_list_are_exposed() {
    let Some(app) = setup("cr-list").await else {
        return;
    };
    for uid in ["cr-list-a", "cr-list-b"] {
        let (status, _) = call(
            &app,
            with_json(
                "POST",
                "/internal/connections",
                &new_connection(uid, "rp-4"),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED);
    }

    let (status, body) = call(&app, bare("GET", "/internal/connections?project_id=rp-4")).await;
    assert_eq!(status, StatusCode::OK);
    let uids: Vec<String> = json::<Vec<ConnectionRecord>>(&body)
        .into_iter()
        .map(|c| c.uid)
        .filter(|u| u.starts_with("cr-list-"))
        .collect();
    assert_eq!(uids, vec!["cr-list-a".to_string(), "cr-list-b".to_string()]);

    let (status, body) = call(
        &app,
        bare(
            "GET",
            "/internal/connections/cr-list-a/used_by?project_id=rp-4",
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(
        json::<Vec<String>>(&body).is_empty(),
        "no pipeline uses it yet"
    );
}
