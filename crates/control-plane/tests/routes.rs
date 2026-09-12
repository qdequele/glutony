//! Route tests that never touch the database. The pool is created with
//! `connect_lazy`, so no connection is opened unless a handler runs a query — the
//! paths exercised here all branch before that point (built-in lookups, validation,
//! JSON parsing, cache-seeded resolution).

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use meili_ingest_control_plane::error::ErrorBody;
use meili_ingest_control_plane::resolver::ResolveResponse;
use meili_ingest_control_plane::{AppState, app};
use meili_ingest_plugin_sdk::{PipelineDefinition, PipelineTrigger, StepDefinition};
use serde::de::DeserializeOwned;
use sqlx::postgres::PgPoolOptions;
use tower::ServiceExt;

fn state() -> AppState {
    // Port 1 on localhost: nothing listens there, and a lazy pool never dials until a
    // query runs. The short acquire timeout keeps the one test that *does* query
    // (health → 503) fast instead of waiting out sqlx's 30s default.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .acquire_timeout(std::time::Duration::from_millis(500))
        .connect_lazy("postgres://nobody:nothing@127.0.0.1:1/none")
        .unwrap();
    AppState::new(pool)
}

async fn call(app: Router, req: Request<Body>) -> (StatusCode, Vec<u8>) {
    let res = app.oneshot(req).await.unwrap();
    let status = res.status();
    let bytes = res.into_body().collect().await.unwrap().to_bytes().to_vec();
    (status, bytes)
}

fn json<T: DeserializeOwned>(bytes: &[u8]) -> T {
    serde_json::from_slice(bytes).unwrap_or_else(|e| {
        panic!(
            "invalid JSON body {:?}: {e}",
            String::from_utf8_lossy(bytes)
        )
    })
}

fn post_json(uri: &str, body: impl Into<Body>) -> Request<Body> {
    Request::post(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(body.into())
        .unwrap()
}

fn pipeline(uid: &str, plugins: &[&str]) -> PipelineDefinition {
    PipelineDefinition {
        uid: uid.into(),
        name: String::new(),
        description: None,
        version: 1,
        trigger: None,
        steps: plugins
            .iter()
            .enumerate()
            .map(|(i, p)| StepDefinition::new(format!("s{i}"), *p))
            .collect(),
        builtin: false,
        project_id: None,
    }
}

#[tokio::test]
async fn get_builtin_pipeline_is_served_without_db() {
    let (status, body) = call(
        app(state()),
        Request::get("/pipelines/builtin.pdf")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let def: PipelineDefinition = json(&body);
    assert_eq!(def.uid, "builtin.pdf");
    assert!(def.builtin);
    assert_eq!(
        def.steps
            .iter()
            .map(|s| s.plugin.as_str())
            .collect::<Vec<_>>(),
        ["pdf_extractor", "chunker", "meili_indexer"]
    );
}

#[tokio::test]
async fn unknown_builtin_uid_is_404() {
    let (status, body) = call(
        app(state()),
        Request::get("/pipelines/builtin.nope")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "not_found");
}

#[tokio::test]
async fn deleting_a_builtin_is_forbidden() {
    let (status, body) = call(
        app(state()),
        Request::delete("/pipelines/builtin.pdf")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "builtin");
}

#[tokio::test]
async fn creating_in_builtin_namespace_is_forbidden() {
    let def = pipeline("builtin.custom", &["pdf_extractor"]);
    let (status, body) = call(
        app(state()),
        post_json("/pipelines", serde_json::to_vec(&def).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "builtin");
}

#[tokio::test]
async fn invalid_pipeline_is_422_with_pipeline_error_message() {
    // Empty steps.
    let def = pipeline("empty", &[]);
    let (status, body) = call(
        app(state()),
        post_json("/pipelines", serde_json::to_vec(&def).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "validation");
    assert_eq!(err.error, "pipeline has no steps");

    // Cycle.
    let mut def = pipeline("cycle", &["chunker", "chunker"]);
    def.steps[0].depends_on = vec!["s1".into()];
    def.steps[1].depends_on = vec!["s0".into()];
    let (status, body) = call(
        app(state()),
        post_json("/pipelines", serde_json::to_vec(&def).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "validation");
    assert!(err.error.contains("cycle"), "{}", err.error);

    // Bad uid.
    let def = pipeline("has space", &["chunker"]);
    let (status, body) = call(
        app(state()),
        post_json("/pipelines", serde_json::to_vec(&def).unwrap()),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "validation");
    assert!(err.error.contains("invalid pipeline uid"), "{}", err.error);
}

#[tokio::test]
async fn yaml_body_is_rejected_as_bad_json() {
    // The control plane only speaks JSON (the gateway converts YAML).
    let yaml = "uid: my-pipeline\nsteps:\n  - id: a\n    plugin: chunker\n";
    let (status, body) = call(app(state()), post_json("/pipelines", yaml)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "bad_json");

    // Same for the other JSON endpoints.
    let (status, body) = call(app(state()), post_json("/internal/resolve", "mime: x")).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "bad_json");

    // Wrong content type is also a 400.
    let req = Request::post("/internal/jobs")
        .header(header::CONTENT_TYPE, "text/plain")
        .body(Body::from("{}"))
        .unwrap();
    let (status, body) = call(app(state()), req).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "bad_json");
}

#[tokio::test]
async fn resolve_explicit_builtin_without_db() {
    let (status, body) = call(
        app(state()),
        post_json(
            "/internal/resolve",
            r#"{"mime":"application/octet-stream","pipeline":"builtin.csv"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let res: ResolveResponse = json(&body);
    assert_eq!(res.pipeline.uid, "builtin.csv");
    assert_eq!(res.index_pattern, None);

    let (status, body) = call(
        app(state()),
        post_json(
            "/internal/resolve",
            r#"{"mime":"application/octet-stream","pipeline":"builtin.nope"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "not_found");
}

#[tokio::test]
async fn resolve_uses_cached_user_pipelines_with_precedence() {
    let st = state();
    let mut tenant = pipeline("tenant-pdf", &["pdf_extractor", "meili_indexer"]);
    tenant.project_id = Some("t1".into());
    tenant.trigger = Some(PipelineTrigger {
        content_types: vec!["application/pdf".into()],
        filename_pattern: None,
        index_pattern: Some("contracts".into()),
    });
    let mut global = pipeline("global-pdf", &["pdf_extractor", "meili_indexer"]);
    global.trigger = Some(PipelineTrigger {
        content_types: vec!["application/pdf".into()],
        filename_pattern: None,
        index_pattern: None,
    });
    st.cache.set(vec![tenant, global]).await;

    // Tenant t1: its own pipeline wins and carries the index pattern.
    let (status, body) = call(
        app(st.clone()),
        post_json(
            "/internal/resolve",
            r#"{"mime":"application/pdf","filename":"a.pdf","project_id":"t1"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let res: ResolveResponse = json(&body);
    assert_eq!(res.pipeline.uid, "tenant-pdf");
    assert_eq!(res.index_pattern.as_deref(), Some("contracts"));

    // Tenant t2: global user pipeline beats the builtin.
    let (status, body) = call(
        app(st.clone()),
        post_json(
            "/internal/resolve",
            r#"{"mime":"application/pdf","project_id":"t2"}"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let res: ResolveResponse = json(&body);
    assert_eq!(res.pipeline.uid, "global-pdf");

    // No tenant: still global user pipeline.
    let (status, body) = call(
        app(st.clone()),
        post_json("/internal/resolve", r#"{"mime":"application/pdf"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let res: ResolveResponse = json(&body);
    assert_eq!(res.pipeline.uid, "global-pdf");

    // Other MIME falls back to a builtin.
    let (status, body) = call(
        app(st.clone()),
        post_json("/internal/resolve", r#"{"mime":"text/csv; charset=utf-8"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let res: ResolveResponse = json(&body);
    assert_eq!(res.pipeline.uid, "builtin.csv");

    // Nothing matches → 404 no_pipeline.
    let (status, body) = call(
        app(st),
        post_json("/internal/resolve", r#"{"mime":"application/x-nothing"}"#),
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let err: ErrorBody = json(&body);
    assert_eq!(err.code, "no_pipeline");
    assert!(
        err.error.starts_with("no pipeline matches mime"),
        "{}",
        err.error
    );
}

#[tokio::test]
async fn health_reports_503_when_db_unreachable() {
    let (status, body) = call(
        app(state()),
        Request::get("/health").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    let v: serde_json::Value = json(&body);
    assert_eq!(v["status"], "unavailable");
}
