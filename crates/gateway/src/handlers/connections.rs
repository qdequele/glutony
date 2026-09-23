//! `/connections`: named Meilisearch destinations a pipeline's `meili_indexer` step can
//! pin (spec *Meilisearch connections and the indexer step*).
//!
//! Every route answers 501 until `SOURCE_SECRET_KEY` is configured, so a key is never
//! stored unsealed. Creating a connection, or changing its host or key, first proves it
//! against the target Meilisearch; responses always mask the key.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use serde::de::DeserializeOwned;

use crate::connections::{
    ConnectionView, CreateConnection, UpdateConnection, normalize_host, valid_uid,
    validate_destination,
};
use crate::context::resolve_project_id;
use crate::error::GatewayError;
use crate::state::AppState;

fn parse_body<T: DeserializeOwned>(body: &Bytes) -> Result<T, GatewayError> {
    serde_json::from_slice(body)
        .map_err(|e| GatewayError::BadRequest(format!("invalid connection body: {e}")))
}

fn check_uid(uid: &str) -> Result<(), GatewayError> {
    if valid_uid(uid) {
        Ok(())
    } else {
        Err(GatewayError::BadRequest(format!(
            "invalid connection uid {uid:?}: must be 1-128 characters of [a-zA-Z0-9._-]"
        )))
    }
}

fn non_blank(s: Option<String>) -> Option<String> {
    s.map(|v| v.trim().to_owned()).filter(|v| !v.is_empty())
}

fn not_found(uid: &str) -> GatewayError {
    GatewayError::NotFound(format!("connection {uid:?} not found"))
}

/// `GET /connections` — the tenant's connections plus global ones, keys masked.
pub async fn list_connections(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<ConnectionView>>, GatewayError> {
    state.connections.key()?;
    let project_id = resolve_project_id(&headers, &state.config);
    let rows = state
        .control_plane
        .list_connections(project_id.as_deref())
        .await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| ConnectionView::from_record(r, None))
            .collect(),
    ))
}

/// `POST /connections` — validate, seal and store a connection.
pub async fn create_connection(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<ConnectionView>), GatewayError> {
    let key = state.connections.key()?;
    let req: CreateConnection = parse_body(&body)?;
    let uid = req.uid.trim().to_owned();
    check_uid(&uid)?;
    let api_key = non_blank(Some(req.api_key))
        .ok_or_else(|| GatewayError::BadRequest("api_key must not be empty".into()))?;
    let (host, url) = normalize_host(&req.host)?;

    // Proven before anything is stored, so a typo'd key fails now, not in a cron run.
    validate_destination(&state.connections, &host, &url, &api_key).await?;

    let sealed = key
        .seal(api_key.as_bytes())
        .map_err(|e| GatewayError::Internal(e.to_string()))?;
    let name = non_blank(req.name).unwrap_or_else(|| uid.clone());
    let project_id = resolve_project_id(&headers, &state.config);
    let record = state
        .control_plane
        .create_connection(&uid, &name, project_id.as_deref(), &host, &sealed)
        .await?;
    tracing::info!(uid = %uid, host = %host, project_id = ?project_id, "connection created");
    Ok((
        StatusCode::CREATED,
        Json(ConnectionView::from_record(record, Some(Vec::new()))),
    ))
}

/// `GET /connections/{uid}` — one connection, key masked, with the pipelines using it.
pub async fn get_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<Json<ConnectionView>, GatewayError> {
    state.connections.key()?;
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);
    let record = state
        .control_plane
        .get_connection(&uid, project_id.as_deref())
        .await?
        .ok_or_else(|| not_found(&uid))?;
    let used_by = state
        .control_plane
        .connection_used_by(&uid, project_id.as_deref())
        .await?;
    Ok(Json(ConnectionView::from_record(record, Some(used_by))))
}

/// `PATCH /connections/{uid}` — rename, repoint or rekey. A change to `host` or
/// `api_key` is re-validated against Meilisearch; omitting `api_key` keeps it.
pub async fn patch_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<ConnectionView>, GatewayError> {
    let key = state.connections.key()?;
    check_uid(&uid)?;
    let req: UpdateConnection = parse_body(&body)?;
    let project_id = resolve_project_id(&headers, &state.config);

    // Reads fall back to the global row, writes never do: a tenant must not reach a
    // global connection, so anything outside the caller's exact scope is "not found".
    let stored = state
        .control_plane
        .get_connection(&uid, project_id.as_deref())
        .await?
        .filter(|r| r.project_id == project_id)
        .ok_or_else(|| not_found(&uid))?;

    let new_host = req.host.as_deref().map(normalize_host).transpose()?;
    let new_key = non_blank(req.api_key);
    let mut sealed = None;
    if new_host.is_some() || new_key.is_some() {
        // Validate the destination as it will be after this patch.
        let (host, url) = match &new_host {
            Some(h) => h.clone(),
            None => normalize_host(&stored.host)?,
        };
        let plaintext = match &new_key {
            Some(k) => k.clone(),
            None => {
                let opened = key.open(&stored.api_key).map_err(|_| {
                    GatewayError::Internal(format!(
                        "connection {uid:?}: its stored key cannot be decrypted with this \
                         SOURCE_SECRET_KEY; send a new api_key"
                    ))
                })?;
                String::from_utf8(opened).map_err(|_| {
                    GatewayError::Internal(format!("connection {uid:?}: stored key is not UTF-8"))
                })?
            }
        };
        validate_destination(&state.connections, &host, &url, &plaintext).await?;
        if let Some(k) = &new_key {
            sealed = Some(
                key.seal(k.as_bytes())
                    .map_err(|e| GatewayError::Internal(e.to_string()))?,
            );
        }
    }

    let name = non_blank(req.name);
    let record = state
        .control_plane
        .update_connection(
            &uid,
            project_id.as_deref(),
            name.as_deref(),
            new_host.as_ref().map(|(h, _)| h.as_str()),
            sealed.as_deref(),
        )
        .await?
        .ok_or_else(|| not_found(&uid))?;
    let used_by = state
        .control_plane
        .connection_used_by(&uid, project_id.as_deref())
        .await?;
    Ok(Json(ConnectionView::from_record(record, Some(used_by))))
}

/// `DELETE /connections/{uid}` — never blocked by pipelines that still name it (spec
/// Decision 15); they fail at run time naming the missing connection.
pub async fn delete_connection(
    State(state): State<AppState>,
    Path(uid): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, GatewayError> {
    state.connections.key()?;
    check_uid(&uid)?;
    let project_id = resolve_project_id(&headers, &state.config);
    state
        .control_plane
        .delete_connection(&uid, project_id.as_deref())
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use meili_ingest_blob::BlobStore;
    use meili_ingest_source::{HostPolicy, SecretKey};
    use tower::ServiceExt;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::connections::ConnectionConfig;
    use crate::state::{AppState, GatewayConfig};
    use crate::test_support::FakeStarter;

    const PLAINTEXT: &str = "sk-live-PLAINTEXT-KEY";

    fn secret() -> Arc<SecretKey> {
        Arc::new(SecretKey::from_bytes([4; 32]))
    }

    fn app(control_plane: &MockServer, key: Option<Arc<SecretKey>>) -> Router {
        let config = GatewayConfig {
            control_plane_url: control_plane.uri(),
            ..GatewayConfig::default()
        };
        let state = AppState::new(
            config,
            Arc::new(FakeStarter::default()),
            BlobStore::memory(),
            reqwest::Client::new(),
        )
        .with_connections(ConnectionConfig::new(key, HostPolicy::Any));
        crate::router(state)
    }

    async fn call(app: &Router, req: Request<Body>) -> (StatusCode, serde_json::Value) {
        let res = app.clone().oneshot(req).await.expect("response");
        let status = res.status();
        let bytes = axum::body::to_bytes(res.into_body(), usize::MAX)
            .await
            .expect("body");
        let json = serde_json::from_slice(&bytes).unwrap_or(serde_json::Value::Null);
        (status, json)
    }

    fn json_req(method: &str, uri: &str, body: serde_json::Value) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(uri)
            .header("content-type", "application/json")
            .body(Body::from(body.to_string()))
            .expect("request")
    }

    fn get(uri: &str) -> Request<Body> {
        Request::builder()
            .uri(uri)
            .body(Body::empty())
            .expect("request")
    }

    /// A Meilisearch that accepts `PLAINTEXT` and rejects anything else.
    async fn meili() -> MockServer {
        let s = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(200))
            .mount(&s)
            .await;
        Mock::given(method("GET"))
            .and(path("/indexes"))
            .and(header(
                "authorization",
                format!("Bearer {PLAINTEXT}").as_str(),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"results": []})),
            )
            .mount(&s)
            .await;
        Mock::given(method("GET"))
            .and(path("/indexes"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&s)
            .await;
        s
    }

    fn record(uid: &str, host: &str, sealed: &[u8]) -> serde_json::Value {
        serde_json::json!({
            "id": "11111111-1111-1111-1111-111111111111",
            "uid": uid,
            "name": uid,
            "host": host,
            "api_key": sealed,
            "created_at": "2026-09-23T00:00:00Z",
            "updated_at": "2026-09-23T00:00:00Z",
        })
    }

    #[tokio::test]
    async fn every_route_is_501_without_a_secret_key() {
        let cp = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(ResponseTemplate::new(500))
            .expect(0)
            .mount(&cp)
            .await;
        let app = app(&cp, None);
        let (status, body) = call(&app, get("/connections")).await;
        assert_eq!(status, StatusCode::NOT_IMPLEMENTED);
        assert_eq!(body["code"], "not_configured");
        let (status, _) = call(
            &app,
            json_req(
                "POST",
                "/connections",
                serde_json::json!({"uid": "x", "host": "https://m.example", "api_key": "k"}),
            ),
        )
        .await;
        assert_eq!(
            status,
            StatusCode::NOT_IMPLEMENTED,
            "nothing is stored unsealed"
        );
    }

    #[tokio::test]
    async fn create_validates_seals_and_masks() {
        let meili = meili().await;
        let cp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/connections"))
            .respond_with(ResponseTemplate::new(201).set_body_json(record(
                "prod",
                &meili.uri(),
                &[9, 9],
            )))
            .mount(&cp)
            .await;
        let app = app(&cp, Some(secret()));

        let (status, body) = call(
            &app,
            json_req(
                "POST",
                "/connections",
                serde_json::json!({"uid": "prod", "host": meili.uri(), "api_key": PLAINTEXT}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::CREATED, "{body}");
        assert_eq!(body["api_key"], "****");
        assert!(!body.to_string().contains(PLAINTEXT));

        // The control plane received sealed bytes, never the plaintext key.
        let sent = &cp.received_requests().await.expect("recorded")[0];
        let sent_body: serde_json::Value = serde_json::from_slice(&sent.body).expect("json");
        assert!(!String::from_utf8_lossy(&sent.body).contains(PLAINTEXT));
        let sealed: Vec<u8> = serde_json::from_value(sent_body["api_key"].clone()).expect("bytes");
        assert_eq!(
            secret().open(&sealed).expect("opens with the same key"),
            PLAINTEXT.as_bytes()
        );
    }

    #[tokio::test]
    async fn a_rejected_key_is_422_and_nothing_is_stored() {
        let meili = meili().await;
        let cp = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/connections"))
            .respond_with(ResponseTemplate::new(201))
            .expect(0)
            .mount(&cp)
            .await;
        let app = app(&cp, Some(secret()));
        let (status, body) = call(
            &app,
            json_req(
                "POST",
                "/connections",
                serde_json::json!({"uid": "prod", "host": meili.uri(), "api_key": "wrong"}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY, "{body}");
        assert_eq!(body["code"], "validation");
        assert!(
            body["error"].as_str().unwrap_or("").contains("rejected"),
            "{body}"
        );
    }

    #[tokio::test]
    async fn an_invalid_uid_is_400_before_any_probe() {
        let cp = MockServer::start().await;
        let app = app(&cp, Some(secret()));
        let (status, _) = call(
            &app,
            json_req(
                "POST",
                "/connections",
                serde_json::json!({"uid": "../x", "host": "https://m.example", "api_key": "k"}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn get_masks_the_key_and_reports_used_by() {
        let cp = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(
                "prod",
                "https://m.example",
                &[1],
            )))
            .mount(&cp)
            .await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod/used_by"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!(["tmdb"])))
            .mount(&cp)
            .await;
        let (status, body) = call(&app(&cp, Some(secret())), get("/connections/prod")).await;
        assert_eq!(status, StatusCode::OK, "{body}");
        assert_eq!(body["api_key"], "****");
        assert_eq!(body["used_by"], serde_json::json!(["tmdb"]));
    }

    #[tokio::test]
    async fn renaming_does_not_probe_meilisearch_or_touch_the_key() {
        let meili = MockServer::start().await;
        Mock::given(wiremock::matchers::any())
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&meili)
            .await;
        let cp = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(
                "prod",
                &meili.uri(),
                &[1],
            )))
            .mount(&cp)
            .await;
        Mock::given(method("PATCH"))
            .and(path("/internal/connections/prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(
                "prod",
                &meili.uri(),
                &[1],
            )))
            .mount(&cp)
            .await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod/used_by"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!([])))
            .mount(&cp)
            .await;

        let (status, body) = call(
            &app(&cp, Some(secret())),
            json_req(
                "PATCH",
                "/connections/prod",
                serde_json::json!({"name": "Movies"}),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{body}");
        let patch = cp
            .received_requests()
            .await
            .expect("recorded")
            .into_iter()
            .find(|r| r.method.as_str() == "PATCH")
            .expect("a patch was sent");
        let patch: serde_json::Value = serde_json::from_slice(&patch.body).expect("json");
        assert_eq!(
            patch,
            serde_json::json!({"name": "Movies"}),
            "no key, no host"
        );
    }

    #[tokio::test]
    async fn a_tenant_cannot_patch_a_global_connection() {
        let cp = MockServer::start().await;
        // The read falls back to the global row (no project_id)…
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod"))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(
                "prod",
                "https://m.example",
                &[1],
            )))
            .mount(&cp)
            .await;
        Mock::given(method("PATCH"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&cp)
            .await;
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
        .with_connections(ConnectionConfig::new(Some(secret()), HostPolicy::Any));
        let app = crate::router(state);
        let req = Request::builder()
            .method("PATCH")
            .uri("/connections/prod")
            .header("content-type", "application/json")
            .header("x-meili-project-id", "tenant-1")
            .body(Body::from(r#"{"host":"https://attacker.example"}"#))
            .expect("request");
        let (status, _) = call(&app, req).await;
        assert_eq!(
            status,
            StatusCode::NOT_FOUND,
            "…but the write must not reach it"
        );
    }
}
