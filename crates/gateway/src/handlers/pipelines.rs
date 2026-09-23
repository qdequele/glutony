//! CRUD proxy for pipelines (SPEC §4 "Other endpoints"). `POST /pipelines` accepts YAML
//! or JSON (plan Decision 9), normalizes and validates the definition, then forwards JSON
//! to the control plane. Context is optional here: only the tenant id is used, and a
//! request without any tenant creates a global pipeline.

use axum::Json;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::header::CONTENT_TYPE;
use axum::http::{HeaderMap, StatusCode};
use meili_ingest_plugin_sdk::PipelineDefinition;

use crate::context::resolve_project_id;
use crate::error::GatewayError;
use crate::state::AppState;

/// Whether a content type denotes YAML.
pub fn is_yaml(content_type: Option<&str>) -> bool {
    let Some(ct) = content_type else { return false };
    let essence = ct
        .split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase();
    matches!(
        essence.as_str(),
        "application/x-yaml" | "application/yaml" | "text/yaml" | "text/x-yaml"
    ) || essence.ends_with("+yaml")
}

/// Parse a pipeline definition from JSON or YAML text and normalize it.
///
/// With a YAML content type the body is parsed as YAML; otherwise JSON is tried first
/// and YAML second (YAML is a superset of JSON, so the JSON error is only reported when
/// both fail).
pub fn parse_pipeline(
    body: &[u8],
    content_type: Option<&str>,
) -> Result<PipelineDefinition, GatewayError> {
    let text = std::str::from_utf8(body)
        .map_err(|_| GatewayError::BadRequest("pipeline body must be UTF-8".into()))?;
    if text.trim().is_empty() {
        return Err(GatewayError::BadRequest("empty pipeline body".into()));
    }
    let mut def: PipelineDefinition = if is_yaml(content_type) {
        serde_yaml::from_str(text)
            .map_err(|e| GatewayError::BadRequest(format!("invalid pipeline YAML: {e}")))?
    } else {
        match serde_json::from_str::<PipelineDefinition>(text) {
            Ok(d) => d,
            Err(json_err) => serde_yaml::from_str(text).map_err(|yaml_err| {
                GatewayError::BadRequest(format!(
                    "pipeline body is neither valid JSON ({json_err}) nor valid YAML ({yaml_err})"
                ))
            })?,
        }
    };
    def.normalize();
    Ok(def)
}

/// `POST /pipelines` — create or update.
pub async fn create_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<(StatusCode, Json<PipelineDefinition>), GatewayError> {
    let content_type = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());
    let mut def = parse_pipeline(&body, content_type)?;
    def.validate()
        .map_err(|e| GatewayError::Invalid(e.to_string()))?;
    if def.builtin {
        return Err(GatewayError::Forbidden(
            "user pipelines cannot be marked builtin".into(),
        ));
    }
    if let Some(project_id) = resolve_project_id(&headers, &state.config) {
        def.project_id = Some(project_id);
    }
    let stored = state.control_plane.upsert_pipeline(&def).await?;
    tracing::info!(pipeline = %stored.uid, project_id = ?stored.project_id, version = stored.version, "pipeline upserted");
    Ok((StatusCode::CREATED, Json(stored)))
}

/// `GET /pipelines`.
pub async fn list_pipelines(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> Result<Json<Vec<PipelineDefinition>>, GatewayError> {
    let project_id = resolve_project_id(&headers, &state.config);
    Ok(Json(
        state
            .control_plane
            .list_pipelines(project_id.as_deref())
            .await?,
    ))
}

/// `POST /pipelines/validate` — check a definition without saving it.
///
/// Accepts the same YAML or JSON body as `POST /pipelines` and returns the control
/// plane's verdict, so an editor can surface cycles, unknown plugins and bad fan-out
/// while the author is still typing.
pub async fn validate_pipeline(
    State(state): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Result<Json<serde_json::Value>, GatewayError> {
    let content_type = headers.get(CONTENT_TYPE).and_then(|v| v.to_str().ok());
    let mut def = parse_pipeline(&body, content_type)?;
    if let Some(project_id) = resolve_project_id(&headers, &state.config) {
        def.project_id = Some(project_id);
    }
    Ok(Json(state.control_plane.validate_pipeline(&def).await?))
}

/// `GET /pipelines/{name}`.
pub async fn get_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<Json<PipelineDefinition>, GatewayError> {
    let project_id = resolve_project_id(&headers, &state.config);
    Ok(Json(
        state
            .control_plane
            .get_pipeline(&name, project_id.as_deref())
            .await?,
    ))
}

/// `DELETE /pipelines/{name}` → 204.
pub async fn delete_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
    headers: HeaderMap,
) -> Result<StatusCode, GatewayError> {
    let project_id = resolve_project_id(&headers, &state.config);
    let archived = state
        .control_plane
        .delete_pipeline(&name, project_id.as_deref())
        .await?;
    // The control plane archived the sources feeding this pipeline; stop their
    // schedules so they do not keep firing into a source that can no longer run. The
    // pipeline is already gone, so a failure here is logged rather than returned.
    for source_id in &archived {
        let schedule_id = meili_ingest_source::SourceRunInput::schedule_id(*source_id);
        if let Err(e) = state.schedules.delete(&schedule_id).await {
            tracing::error!(
                pipeline = %name,
                schedule = %schedule_id,
                "could not delete the schedule of an archived source: {e}"
            );
        }
    }
    tracing::info!(
        pipeline = %name,
        project_id = ?project_id,
        archived_sources = archived.len(),
        "pipeline deleted"
    );
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use axum::body::Body;
    use axum::http::Request;
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::{body_partial_json, method, path, query_param as wq};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const YAML: &str = r#"
uid: my-pdf
name: "PDF"
trigger:
  content_types: [application/pdf]
steps:
  - id: extract
    plugin: pdf_extractor
  - id: chunk
    plugin: chunker
  - id: index
    plugin: meili_indexer
"#;

    #[test]
    fn yaml_content_types() {
        for ct in [
            "application/x-yaml",
            "application/yaml; charset=utf-8",
            "text/yaml",
            "text/x-yaml",
            "application/vnd.x+yaml",
        ] {
            assert!(is_yaml(Some(ct)), "{ct}");
        }
        assert!(!is_yaml(Some("application/json")));
        assert!(!is_yaml(None));
    }

    #[test]
    fn parse_yaml_json_and_fallback() {
        let def = parse_pipeline(YAML.as_bytes(), Some("application/x-yaml")).unwrap();
        assert_eq!(def.uid, "my-pdf");
        assert_eq!(def.steps[1].depends_on, vec!["extract"], "normalized");
        assert_eq!(def.validate().unwrap(), vec!["extract", "chunk", "index"]);

        let json = json!({"uid": "j", "steps": [{"id": "a", "plugin": "chunker"}]}).to_string();
        let def = parse_pipeline(json.as_bytes(), Some("application/json")).unwrap();
        assert_eq!(def.uid, "j");
        assert_eq!(def.name, "j", "name defaults to uid");

        // YAML body without a content type still parses
        let def = parse_pipeline(YAML.as_bytes(), None).unwrap();
        assert_eq!(def.uid, "my-pdf");

        let err = parse_pipeline(b"::: not anything", None).unwrap_err();
        assert!(matches!(err, GatewayError::BadRequest(m) if m.contains("neither valid JSON")));
        assert!(matches!(
            parse_pipeline(b"   ", None),
            Err(GatewayError::BadRequest(_))
        ));
        assert!(matches!(
            parse_pipeline(b"{", Some("application/x-yaml")),
            Err(GatewayError::BadRequest(_))
        ));
    }

    #[tokio::test]
    async fn post_yaml_is_validated_forwarded_with_tenant_and_returns_201() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pipelines"))
            .and(body_partial_json(
                json!({"uid": "my-pdf", "project_id": "xxx"}),
            ))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "uid": "my-pdf", "name": "PDF", "version": 2, "project_id": "xxx",
                "steps": [{"id": "extract", "plugin": "pdf_extractor"}]
            })))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let req = Request::post("/pipelines")
            .header(CONTENT_TYPE, "application/x-yaml")
            .header("x-meili-project-id", "xxx")
            .body(Body::from(YAML))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let json = json_body(resp).await;
        assert_eq!(json["version"], 2);
        assert_eq!(json["project_id"], "xxx");
    }

    #[tokio::test]
    async fn post_without_context_is_global() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pipelines"))
            .respond_with(
                ResponseTemplate::new(201).set_body_json(
                    json!({"uid": "g", "steps": [{"id": "a", "plugin": "chunker"}]}),
                ),
            )
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"uid": "g", "steps": [{"id": "a", "plugin": "chunker"}]}).to_string();
        let req = Request::post("/pipelines")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::CREATED);
        let sent = server.received_requests().await.unwrap().remove(0);
        let sent: serde_json::Value = serde_json::from_slice(&sent.body).unwrap();
        assert!(sent.get("project_id").is_none());
    }

    #[tokio::test]
    async fn invalid_pipeline_is_422_with_message_and_bad_text_is_400() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"uid": "cyc", "steps": [
            {"id": "a", "plugin": "x", "depends_on": ["b"]},
            {"id": "b", "plugin": "y", "depends_on": ["a"]}
        ]})
        .to_string();
        let req = Request::post("/pipelines")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let json = json_body(resp).await;
        assert_eq!(json["code"], "invalid_pipeline");
        assert!(json["error"].as_str().unwrap().contains("cycle"));

        let req = Request::post("/pipelines")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{{{"))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(
            server.received_requests().await.unwrap().is_empty(),
            "nothing forwarded"
        );
    }

    #[tokio::test]
    async fn control_plane_422_passes_through() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pipelines"))
            .respond_with(ResponseTemplate::new(422).set_body_json(
                json!({"error": "unknown plugin \"nope\"", "code": "unknown_plugin"}),
            ))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"uid": "p", "steps": [{"id": "a", "plugin": "nope"}]}).to_string();
        let req = Request::post("/pipelines")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let json = json_body(resp).await;
        assert_eq!(json["code"], "invalid_pipeline");
        assert!(
            json["error"]
                .as_str()
                .unwrap()
                .contains("unknown plugin \"nope\"")
        );
    }

    #[tokio::test]
    async fn list_get_delete_are_proxied_with_tenant_and_403_passes_through() {
        let server = MockServer::start().await;
        let def = sample_pipeline("builtin.pdf", None);
        Mock::given(method("GET"))
            .and(path("/pipelines"))
            .and(wq("project_id", "xxx"))
            .respond_with(ResponseTemplate::new(200).set_body_json(vec![def.clone()]))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines/builtin.pdf"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&def))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/pipelines/ghost"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error": "pipeline ghost not found"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pipelines/builtin.pdf"))
            .respond_with(
                ResponseTemplate::new(403)
                    .set_body_json(json!({"error": "built-in pipelines cannot be deleted"})),
            )
            .mount(&server)
            .await;
        Mock::given(method("DELETE"))
            .and(path("/pipelines/mine"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;

        let req = Request::get("/pipelines")
            .header("x-meili-project-id", "xxx")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(json_body(resp).await.as_array().unwrap().len(), 1);

        let resp = app
            .clone()
            .oneshot(
                Request::get("/pipelines/builtin.pdf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(json_body(resp).await["uid"], "builtin.pdf");

        let resp = app
            .clone()
            .oneshot(
                Request::get("/pipelines/ghost")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        let resp = app
            .clone()
            .oneshot(
                Request::delete("/pipelines/builtin.pdf")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(json_body(resp).await["code"], "forbidden");

        let resp = app
            .oneshot(
                Request::delete("/pipelines/mine")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NO_CONTENT);
    }
}
