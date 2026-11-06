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
        tenant_id: None,
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
    tenant.tenant_id = Some("t1".into());
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
            r#"{"mime":"application/pdf","filename":"a.pdf","tenant_id":"t1"}"#,
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
            r#"{"mime":"application/pdf","tenant_id":"t2"}"#,
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

fn script_step(id: &str, config: serde_json::Value) -> StepDefinition {
    let mut step = StepDefinition::new(id, "document_script");
    step.config = config;
    step
}

fn script_pipeline(steps: Vec<StepDefinition>) -> PipelineDefinition {
    let mut def = pipeline("scripts", &[]);
    def.steps = steps;
    def
}

#[tokio::test]
async fn validate_reports_every_broken_document_script_step() {
    let def = script_pipeline(vec![
        script_step("a", serde_json::json!({"script": "doc.fields.x = ;"})),
        script_step(
            "b",
            serde_json::json!({"ops": [{"set": {"field": "t", "expr": "1 +"}}]}),
        ),
    ]);
    let (status, body) = call(
        app(state()),
        post_json("/pipelines/validate", serde_json::to_vec(&def).unwrap()),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = json(&body);
    assert_eq!(body["code"], "invalid_step_config");
    assert_eq!(body["error"], "2 steps have invalid configuration");
    let diagnostics = body["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 2);
    assert_eq!(diagnostics[0]["step"], "a");
    assert_eq!(diagnostics[0]["path"], "script");
    assert_eq!(diagnostics[0]["line"], 1);
    assert_eq!(diagnostics[1]["step"], "b");
    assert_eq!(diagnostics[1]["path"], "ops[0].expr");
}

#[tokio::test]
async fn saving_a_broken_script_is_rejected_before_the_database() {
    let def = script_pipeline(vec![script_step("a", serde_json::json!({"script": "1 +"}))]);
    let (status, body) = call(
        app(state()),
        post_json("/pipelines", serde_json::to_vec(&def).unwrap()),
    )
    .await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = json(&body);
    assert_eq!(body["code"], "invalid_step_config");
    assert_eq!(body["error"], "step `a` has invalid configuration");
}

#[tokio::test]
async fn metrics_are_served_in_the_prometheus_text_format() {
    let (status, body) = call(
        app(state()),
        Request::get("/metrics").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("glutony_lab_events_pending 0"), "{text}");
    assert!(
        text.contains("glutony_lab_events_delivered_total 0"),
        "{text}"
    );
}

#[tokio::test]
async fn internal_routes_need_the_control_plane_token() {
    let app = app(state().with_internal_token(Some("cp-token".into())));
    for req in [
        post_json("/internal/lab-events", r#"{"events":[]}"#),
        Request::post("/internal/lab-events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Bearer nope")
            .body(Body::from(r#"{"events":[]}"#))
            .unwrap(),
        Request::post("/internal/lab-events")
            .header(header::CONTENT_TYPE, "application/json")
            .header(header::AUTHORIZATION, "Basic cp-token")
            .body(Body::from(r#"{"events":[]}"#))
            .unwrap(),
        Request::get("/internal/connections/x")
            .body(Body::empty())
            .unwrap(),
    ] {
        let (status, body) = call(app.clone(), req).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
        let err: ErrorBody = json(&body);
        assert_eq!(err.code, "unauthorized");
    }
    // The right token reaches the handler. With the lazy pool that handler fails on
    // the database (500 `db`), which is exactly "past the auth gate".
    let (status, _) = call(
        app.clone(),
        Request::get("/internal/connections/x")
            .header(header::AUTHORIZATION, "Bearer cp-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
    // Non-internal routes are untouched: /metrics never needs the token.
    let (status, _) = call(app, Request::get("/metrics").body(Body::empty()).unwrap()).await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
async fn without_a_configured_token_internal_routes_stay_open() {
    let (status, _) = call(
        app(state()),
        Request::get("/metrics").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _) = call(
        app(state()),
        Request::get("/internal/connections/x")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_ne!(status, StatusCode::UNAUTHORIZED);
}

#[test]
fn control_plane_token_policy() {
    use meili_ingest_control_plane::control_plane_token_policy as policy;
    assert_eq!(
        policy(Some("t".into()), false).unwrap().as_deref(),
        Some("t")
    );
    assert_eq!(
        policy(Some("t".into()), true).unwrap().as_deref(),
        Some("t")
    );
    assert_eq!(policy(None, true).unwrap(), None);
    let err = policy(None, false).unwrap_err().to_string();
    assert!(
        err.contains("CONTROL_PLANE_TOKEN") && err.contains("CONTROL_PLANE_TOKEN_DISABLED=true"),
        "{err}"
    );
    assert!(policy(Some("  ".into()), false).is_err(), "blank is unset");
    // The example Secret's placeholder is refused even with the escape hatch on.
    for placeholder in ["CHANGE_ME", " CHANGE_ME\n"] {
        for disabled in [false, true] {
            let err = policy(Some(placeholder.into()), disabled)
                .unwrap_err()
                .to_string();
            assert!(
                err.contains("CONTROL_PLANE_TOKEN") && err.contains("CHANGE_ME"),
                "{err}"
            );
        }
    }
}

// --- `GET /internal/lab/credits/{account_id}`: the workers' credit pre-check ---

const LAB_ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
const LAB_INSTANCE: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

fn with_lab(lab_url: &str) -> AppState {
    let creds = meili_ingest_lab::LabCredentials::new(lab_url, LAB_INSTANCE, "s").unwrap();
    state().with_lab_credits(std::sync::Arc::new(
        meili_ingest_lab::AccountCreditCache::new(creds, reqwest::Client::new()),
    ))
}

async fn mount_lab_balance(lab: &wiremock::MockServer, balance: i64, expect: u64) {
    use wiremock::matchers::{method, path};
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}")))
        .respond_with(
            wiremock::ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": LAB_ACCOUNT, "tier": "pro",
                "credits": {"balance": balance}, "cache_ttl": 30
            })),
        )
        .expect(expect)
        .mount(lab)
        .await;
}

async fn credit_check(state: AppState, account: &str) -> (StatusCode, serde_json::Value) {
    let (status, body) = call(
        app(state),
        Request::get(format!("/internal/lab/credits/{account}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    (status, json(&body))
}

#[tokio::test]
async fn credit_checks_without_lab_credentials_are_not_checked() {
    let (status, body) = credit_check(state(), LAB_ACCOUNT).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"checked": false}));
}

#[tokio::test]
async fn credit_checks_of_a_non_lab_account_never_call_the_lab() {
    let lab = wiremock::MockServer::start().await;
    mount_lab_balance(&lab, 0, 0).await;
    for account in ["hackersearch", "0192F3C1-7C2E-7B1A-9F00-3C9D2E4A5B61"] {
        let (status, body) = credit_check(with_lab(&lab.uri()), account).await;
        assert_eq!(status, StatusCode::OK, "{account}");
        assert_eq!(body, serde_json::json!({"checked": false}), "{account}");
    }
    // `expect(0)` is verified when `lab` drops.
}

#[tokio::test]
async fn credit_checks_of_an_account_with_credits_pass() {
    let lab = wiremock::MockServer::start().await;
    mount_lab_balance(&lab, 10, 1).await;
    let (status, body) = credit_check(with_lab(&lab.uri()), LAB_ACCOUNT).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"checked": true}));
}

#[tokio::test]
async fn credit_checks_of_an_account_without_credits_are_402() {
    let lab = wiremock::MockServer::start().await;
    mount_lab_balance(&lab, 0, 1).await;
    let (status, body) = credit_check(with_lab(&lab.uri()), LAB_ACCOUNT).await;
    assert_eq!(status, StatusCode::PAYMENT_REQUIRED);
    assert_eq!(body["code"], "insufficient_credits");
    assert!(
        body["error"].as_str().unwrap().contains(LAB_ACCOUNT),
        "{body}"
    );
}

#[tokio::test]
async fn credit_checks_fail_closed_when_the_lab_cannot_answer() {
    // Nothing listens on port 1, and nothing is cached yet.
    let (status, body) = credit_check(with_lab("http://127.0.0.1:1"), LAB_ACCOUNT).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "lab_unavailable");
}

#[tokio::test]
async fn credit_checks_need_the_control_plane_token() {
    let (status, body) = credit_check(
        state().with_internal_token(Some("cp-token".into())),
        LAB_ACCOUNT,
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(body["code"], "unauthorized");
}

// --- `GET /internal/lab/ai/{account_id}`: the workers' Gateway target ---

const TEAM_KEY: &str = "sk-team-key-do-not-leak";

/// A state serving AI targets from `lab_url`, with the control plane token `cp-token`
/// (the route refuses without one).
fn with_lab_ai(lab_url: &str) -> AppState {
    let creds = meili_ingest_lab::LabCredentials::new(lab_url, LAB_INSTANCE, "s").unwrap();
    state()
        .with_internal_token(Some("cp-token".into()))
        .with_lab_ai(std::sync::Arc::new(meili_ingest_lab::AiTargetCache::new(
            creds,
            reqwest::Client::new(),
        )))
}

fn ai_body(key_id: &str) -> serde_json::Value {
    serde_json::json!({
        "base_url": "https://gateway.example/v1",
        "api_key": TEAM_KEY,
        "key_id": key_id,
        "default_model": "openai/gpt-4o-mini",
        "vision_model": "openai/gpt-4o",
        "cache_ttl": 60,
    })
}

/// `GET uri` with the `cp-token` bearer.
async fn ai_target(state: AppState, uri: &str) -> (StatusCode, serde_json::Value) {
    let req = Request::get(uri)
        .header(header::AUTHORIZATION, "Bearer cp-token")
        .body(Body::empty())
        .unwrap();
    let (status, body) = call(app(state), req).await;
    (status, json(&body))
}

fn ai_uri() -> String {
    format!("/internal/lab/ai/{LAB_ACCOUNT}")
}

#[tokio::test]
async fn ai_target_without_lab_credentials_is_null() {
    let state = state().with_internal_token(Some("cp-token".into()));
    let (status, body) = ai_target(state, &ai_uri()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"target": null}));
}

#[tokio::test]
async fn ai_target_of_a_non_lab_account_is_null_and_never_calls_the_lab() {
    let lab = wiremock::MockServer::start().await;
    let (status, body) = ai_target(with_lab_ai(&lab.uri()), "/internal/lab/ai/hackersearch").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, serde_json::json!({"target": null}));
    assert!(lab.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn ai_target_is_served_from_the_lab() {
    use wiremock::matchers::{method, path};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(ai_body("k1")))
        .expect(1)
        .mount(&lab)
        .await;
    let (status, body) = ai_target(with_lab_ai(&lab.uri()), &ai_uri()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["target"]["key_id"], "k1");
    // Workers need the key; the route is behind the control plane token.
    assert_eq!(body["target"]["api_key"], TEAM_KEY);
    assert_eq!(body["target"]["base_url"], "https://gateway.example/v1");
    assert_eq!(body["target"]["default_model"], "openai/gpt-4o-mini");
}

#[tokio::test]
async fn ai_target_passes_rejected_key_id() {
    use wiremock::matchers::{method, path, query_param};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .and(query_param("rejected_key_id", "k1"))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(ai_body("k2")))
        .expect(1)
        .mount(&lab)
        .await;
    let (status, body) = ai_target(
        with_lab_ai(&lab.uri()),
        &format!("{}?rejected_key_id=k1", ai_uri()),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["target"]["key_id"], "k2");
}

#[tokio::test]
async fn ai_target_refresh_asks_the_lab_past_a_fresh_entry() {
    use wiremock::matchers::{method, path};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(ai_body("k1")))
        .mount(&lab)
        .await;
    let state = with_lab_ai(&lab.uri());
    let lab_calls = || async { lab.received_requests().await.unwrap().len() };
    ai_target(state.clone(), &ai_uri()).await;
    // Cached (cache_ttl 60 s): no second Lab call.
    ai_target(state.clone(), &ai_uri()).await;
    assert_eq!(lab_calls().await, 1);
    // `refresh=true` (a retried step) skips the fresh entry.
    let (status, body) = ai_target(state.clone(), &format!("{}?refresh=true", ai_uri())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["target"]["key_id"], "k1");
    assert_eq!(lab_calls().await, 2);
    // `refresh=false` is the default.
    ai_target(state, &format!("{}?refresh=false", ai_uri())).await;
    assert_eq!(lab_calls().await, 2);
}

#[tokio::test]
async fn ai_target_is_refused_when_the_token_is_disabled() {
    use wiremock::matchers::{method, path};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(ai_body("k1")))
        .mount(&lab)
        .await;
    // CONTROL_PLANE_TOKEN_DISABLED=true: no token on the state.
    let state = with_lab_ai(&lab.uri()).with_internal_token(None);
    let (status, body) = call(
        app(state),
        Request::get(ai_uri()).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(json::<serde_json::Value>(&body)["code"], "token_required");
    assert!(!String::from_utf8_lossy(&body).contains(TEAM_KEY));
    assert!(lab.received_requests().await.unwrap().is_empty());
}

#[tokio::test]
async fn ai_target_lab_404_is_no_gateway() {
    use wiremock::matchers::{method, path};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .respond_with(wiremock::ResponseTemplate::new(404))
        .mount(&lab)
        .await;
    let (status, body) = ai_target(with_lab_ai(&lab.uri()), &ai_uri()).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["code"], "no_gateway");
}

#[tokio::test]
async fn ai_target_lab_503_is_lab_unavailable() {
    use wiremock::matchers::{method, path};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .respond_with(wiremock::ResponseTemplate::new(503))
        .mount(&lab)
        .await;
    let (status, body) = ai_target(with_lab_ai(&lab.uri()), &ai_uri()).await;
    assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(body["code"], "lab_unavailable");
    assert!(!body.to_string().contains(TEAM_KEY));
}

#[tokio::test]
async fn ai_target_needs_the_control_plane_token() {
    use wiremock::matchers::{method, path};
    let lab = wiremock::MockServer::start().await;
    wiremock::Mock::given(method("GET"))
        .and(path(format!("/internal/accounts/{LAB_ACCOUNT}/ai")))
        .respond_with(wiremock::ResponseTemplate::new(200).set_body_json(ai_body("k1")))
        .mount(&lab)
        .await;
    let state = with_lab_ai(&lab.uri());
    let (status, body) = call(
        app(state.clone()),
        Request::get(ai_uri()).body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    assert_eq!(json::<serde_json::Value>(&body)["code"], "unauthorized");
    assert!(!String::from_utf8_lossy(&body).contains(TEAM_KEY));
    // The refused request never reached the Lab.
    assert!(lab.received_requests().await.unwrap().is_empty());
    // With the bearer it goes through.
    let (status, _) = call(
        app(state),
        Request::get(ai_uri())
            .header(header::AUTHORIZATION, "Bearer cp-token")
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
}
