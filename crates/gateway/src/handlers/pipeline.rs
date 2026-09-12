//! `POST /ingest/pipeline/{name}` (SPEC §4 Tier 2): explicit pipeline, MIME routing
//! skipped; the pipeline trigger's `index_pattern` still applies.

use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use axum::Json;

use super::ingest::{context_for, submit_batch, submit_one, BatchResponse, IngestResponse, PipelineSelection};
use super::{query_param, read_payload, QueryParams};
use crate::context::resolve_context;
use crate::error::GatewayError;
use crate::extract::IngestPayload;
use crate::state::AppState;

/// Response of the explicit-pipeline route: a single job, or `{jobs: [...]}` when the
/// body was an `items` batch.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum PipelineIngestResponse {
    /// One job.
    Single(IngestResponse),
    /// One entry per item.
    Batch(BatchResponse),
}

/// `POST /ingest/pipeline/{name}`.
pub async fn ingest_with_pipeline(
    State(state): State<AppState>,
    Path(name): Path<String>,
    Query(query): Query<QueryParams>,
    headers: HeaderMap,
    req: Request,
) -> Result<(StatusCode, Json<PipelineIngestResponse>), GatewayError> {
    let pre = resolve_context(&headers, query_param(&query, "index"), &state.config)?;
    // 404 before reading the body when the pipeline does not exist.
    let pipeline = state.control_plane.get_pipeline(&name, pre.project_id.as_deref()).await?;
    let extracted = read_payload(&headers, &query, req).await?;
    let ctx = context_for(&state, &headers, &query, &extracted)?;
    let selection = PipelineSelection::Explicit(Box::new(pipeline));
    let resp = match extracted.payload {
        IngestPayload::Batch(items) => {
            if items.is_empty() {
                return Err(GatewayError::BadRequest("`items` must not be empty".into()));
            }
            PipelineIngestResponse::Batch(submit_batch(&state, items, &ctx, &selection).await)
        }
        single => PipelineIngestResponse::Single(submit_one(&state, single, ctx, &selection).await?),
    };
    Ok((StatusCode::ACCEPTED, Json(resp)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use meili_ingest_plugin_sdk::PluginInput;
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::{method, path, query_param as wq};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn explicit_pipeline_skips_routing_and_applies_index_pattern() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines/video-ingest-enriched"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_pipeline("video-ingest-enriched", Some("keynotes"))))
            .mount(&server)
            .await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let (ct, body) = multipart_body("file", "keynote.mp4", "video/mp4", b"\x00\x00\x00\x18ftypmp42\x00\x00\x00\x00mp42isom");
        let resp = app.oneshot(standalone_request("/ingest/pipeline/video-ingest-enriched?index=ignored", ct, body)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let json = json_body(resp).await;
        assert_eq!(json["pipeline_used"], "video-ingest-enriched");
        assert_eq!(json["target_index"], "keynotes");
        let inputs = starter.inputs();
        assert_eq!(inputs[0].pipeline.uid, "video-ingest-enriched");
        assert_eq!(inputs[0].context.index.as_deref(), Some("keynotes"));
        assert!(matches!(inputs[0].input, PluginInput::Bytes(_)));
        // no routing call
        assert!(server.received_requests().await.unwrap().iter().all(|r| r.url.path() != "/internal/resolve"));
    }

    #[tokio::test]
    async fn without_index_pattern_query_index_then_mime_default() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines/p"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_pipeline("p", None)))
            .mount(&server)
            .await;
        mount_jobs_ok(&server).await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let json = json_body(app.clone().oneshot(standalone_request("/ingest/pipeline/p?index=mine", ct, body)).await.unwrap()).await;
        assert_eq!(json["target_index"], "mine");
        let (ct, body) = multipart_body("file", "a.csv", "text/csv", b"a,b\n1,2\n");
        let json = json_body(app.oneshot(standalone_request("/ingest/pipeline/p", ct, body)).await.unwrap()).await;
        assert_eq!(json["target_index"], "datasets");
    }

    #[tokio::test]
    async fn unknown_pipeline_is_404_and_tenant_is_forwarded() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines/ghost"))
            .and(wq("project_id", "xxx"))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "pipeline ghost not found"})))
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let req = axum::http::Request::builder()
            .method("POST")
            .uri("/ingest/pipeline/ghost")
            .header(axum::http::header::CONTENT_TYPE, ct)
            .header("x-meili-host", "http://h")
            .header("x-meili-api-key", "k")
            .header("x-meili-project-id", "xxx")
            .body(axum::body::Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(json_body(resp).await["error"], "pipeline ghost not found");
        assert!(starter.inputs().is_empty());
    }

    #[tokio::test]
    async fn batch_body_on_pipeline_route() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/pipelines/p"))
            .respond_with(ResponseTemplate::new(200).set_body_json(sample_pipeline("p", None)))
            .mount(&server)
            .await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"items": [{"url": "https://e.com/a.pdf"}, {"url": "https://e.com/b.pdf"}]}).to_string();
        let resp = app.oneshot(standalone_request("/ingest/pipeline/p", "application/json", body.into())).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(json_body(resp).await["jobs"].as_array().unwrap().len(), 2);
        assert_eq!(starter.inputs().len(), 2);
    }
}
