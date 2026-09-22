//! `POST /ingest` and `POST /ingest/batch` (SPEC §4 Tier 1), plus the submission flow
//! shared with the explicit-pipeline route.

use axum::Json;
use axum::extract::{Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use chrono::Utc;
use meili_ingest_plugin_sdk::{
    ContentRef, JobStatus, MeiliContext, PipelineDefinition, PipelineWorkflowInput, PluginInput,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{QueryParams, query_param, read_payload};
use crate::context::{resolve_context, resolve_index};
use crate::error::{ErrorBody, GatewayError};
use crate::extract::{Extracted, IngestPayload};
use crate::state::{AppState, JobRecord};

/// Response of the ingest routes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestResponse {
    /// Job id (also the suffix of the Temporal workflow id).
    pub job_id: Uuid,
    /// Pipeline uid that will run.
    pub pipeline_used: String,
    /// Fully resolved index name.
    pub target_index: String,
    /// Always `"queued"`.
    pub status: String,
}

/// One entry of a batch response: a started job or a per-item error.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BatchEntry {
    /// Started.
    Job(IngestResponse),
    /// Failed; the other items are unaffected.
    Error(ErrorBody),
}

/// Response of `POST /ingest/batch`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BatchResponse {
    /// One entry per item, in request order.
    pub jobs: Vec<BatchEntry>,
}

/// How the pipeline is chosen for a submission.
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineSelection {
    /// MIME/filename routing by the control plane, optionally forcing a pipeline uid
    /// (`?pipeline=` / `pipeline` field).
    Auto {
        /// Explicit pipeline uid, if any.
        explicit: Option<String>,
    },
    /// Pipeline already fetched (`POST /ingest/pipeline/{name}`).
    Explicit(Box<PipelineDefinition>),
}

/// Resolve the tenant context from headers + query + body fields. The `?index=` query
/// param beats the `index` body field, which beats the `X-Meili-Index` header.
pub fn context_for(
    state: &AppState,
    headers: &HeaderMap,
    query: &QueryParams,
    extracted: &Extracted,
) -> Result<MeiliContext, GatewayError> {
    let query_index = query_param(query, "index").or(extracted.index.as_deref());
    resolve_context(headers, query_index, &state.config)
}

/// Submit one item: route → resolve index → stage bytes → start workflow → cache job.
pub async fn submit_one(
    state: &AppState,
    payload: IngestPayload,
    mut ctx: MeiliContext,
    selection: &PipelineSelection,
) -> Result<IngestResponse, GatewayError> {
    let filename = payload.filename();
    let mime = payload
        .mime()
        .ok_or_else(|| GatewayError::BadRequest("nested batches are not supported".into()))?;

    let (pipeline, index_pattern) = match selection {
        PipelineSelection::Auto { explicit } => {
            match state
                .control_plane
                .resolve(
                    &mime,
                    filename.as_deref(),
                    ctx.project_id.as_deref(),
                    explicit.as_deref(),
                )
                .await
            {
                Ok(r) => (r.pipeline, r.index_pattern),
                Err(GatewayError::NotFound(msg)) => {
                    return Err(match explicit {
                        Some(_) => GatewayError::NotFound(msg),
                        None => GatewayError::Unsupported(mime),
                    });
                }
                Err(e) => return Err(e),
            }
        }
        PipelineSelection::Explicit(def) => {
            let pattern = def.trigger.as_ref().and_then(|t| t.index_pattern.clone());
            ((**def).clone(), pattern)
        }
    };

    let target_index = resolve_index(
        &mut ctx,
        index_pattern.as_deref(),
        &mime,
        &state.config.default_index,
    );
    let job_id = Uuid::new_v4();

    let input = match payload {
        IngestPayload::File(blob) => {
            state
                .blob
                .stage_upload(job_id, blob, state.config.inline_max_bytes)
                .await?
        }
        IngestPayload::Url { url, filename } => PluginInput::Ref(ContentRef::Url {
            url,
            mime: Some(mime.clone()).filter(|m| m != "application/octet-stream"),
            filename,
        }),
        IngestPayload::S3 { uri, filename } => PluginInput::Ref(ContentRef::S3 {
            uri,
            mime: Some(mime.clone()).filter(|m| m != "application/octet-stream"),
            filename,
        }),
        IngestPayload::Documents(docs) => PluginInput::Documents(docs),
        IngestPayload::Batch(_) => {
            return Err(GatewayError::BadRequest(
                "nested batches are not supported".into(),
            ));
        }
    };

    let pipeline_uid = pipeline.uid.clone();
    let wf_input = PipelineWorkflowInput {
        job_id,
        pipeline,
        input,
        context: ctx.clone(),
    };
    let started = state.temporal.start(&wf_input).await?;
    tracing::info!(
        job_id = %job_id,
        workflow_id = %started.workflow_id,
        pipeline = %pipeline_uid,
        index = %target_index,
        mime = %mime,
        context = %ctx.redacted(),
        "job queued"
    );

    // Best effort: the control plane row is a cache, Temporal is the source of truth.
    let now = Utc::now();
    let record = JobRecord {
        job_id,
        workflow_id: started.workflow_id,
        pipeline_uid: pipeline_uid.clone(),
        project_id: ctx.project_id.clone(),
        index_name: Some(target_index.clone()),
        status: JobStatus::Queued,
        current_step: None,
        error: None,
        started_at: now,
        updated_at: now,
    };
    if let Err(e) = state.control_plane.create_job(&record).await {
        tracing::warn!(job_id = %job_id, "could not record job in the control plane: {e}");
    }

    Ok(IngestResponse {
        job_id,
        pipeline_used: pipeline_uid,
        target_index,
        status: "queued".into(),
    })
}

/// Submit every item of a batch, continuing past per-item failures.
pub async fn submit_batch(
    state: &AppState,
    items: Vec<IngestPayload>,
    ctx: &MeiliContext,
    selection: &PipelineSelection,
) -> BatchResponse {
    let mut jobs = Vec::with_capacity(items.len());
    for item in items {
        match submit_one(state, item, ctx.clone(), selection).await {
            Ok(r) => jobs.push(BatchEntry::Job(r)),
            Err(e) => jobs.push(BatchEntry::Error(ErrorBody {
                error: e.to_string(),
                code: e.code().to_string(),
            })),
        }
    }
    BatchResponse { jobs }
}

/// `POST /ingest` — auto-routing.
pub async fn ingest(
    State(state): State<AppState>,
    Query(query): Query<QueryParams>,
    headers: HeaderMap,
    req: Request,
) -> Result<(StatusCode, Json<IngestResponse>), GatewayError> {
    // Fail fast on a missing context before buffering the body.
    resolve_context(&headers, query_param(&query, "index"), &state.config)?;
    let extracted = read_payload(&headers, &query, req).await?;
    let ctx = context_for(&state, &headers, &query, &extracted)?;
    let explicit = query_param(&query, "pipeline")
        .map(str::to_string)
        .or(extracted.pipeline);
    if matches!(extracted.payload, IngestPayload::Batch(_)) {
        return Err(GatewayError::BadRequest(
            "`items` batches must be sent to POST /ingest/batch".into(),
        ));
    }
    let resp = submit_one(
        &state,
        extracted.payload,
        ctx,
        &PipelineSelection::Auto { explicit },
    )
    .await?;
    Ok((StatusCode::ACCEPTED, Json(resp)))
}

/// `POST /ingest/batch` — one job per item; a single-item body is accepted too.
pub async fn ingest_batch(
    State(state): State<AppState>,
    Query(query): Query<QueryParams>,
    headers: HeaderMap,
    req: Request,
) -> Result<(StatusCode, Json<BatchResponse>), GatewayError> {
    resolve_context(&headers, query_param(&query, "index"), &state.config)?;
    let extracted = read_payload(&headers, &query, req).await?;
    let ctx = context_for(&state, &headers, &query, &extracted)?;
    let explicit = query_param(&query, "pipeline")
        .map(str::to_string)
        .or(extracted.pipeline);
    let items = match extracted.payload {
        IngestPayload::Batch(items) => items,
        single => vec![single],
    };
    if items.is_empty() {
        return Err(GatewayError::BadRequest("`items` must not be empty".into()));
    }
    let resp = submit_batch(&state, items, &ctx, &PipelineSelection::Auto { explicit }).await;
    Ok((StatusCode::ACCEPTED, Json(resp)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::*;
    use axum::body::Body;
    use axum::http::Request;
    use axum::http::header::CONTENT_TYPE;
    use meili_ingest_plugin_sdk::{Blob, PluginInput};
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::{body_partial_json, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn multipart_pdf_starts_workflow_with_context_pipeline_and_index() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let (ct, body) = multipart_body("file", "report.pdf", "application/pdf", PDF_MAGIC);
        let req = Request::builder()
            .method("POST")
            .uri("/ingest?index=from-query")
            .header(CONTENT_TYPE, ct)
            .header("x-meili-host", "https://xxx.us-west.meilisearch.io")
            .header("x-meili-api-key", "envoyKey")
            .header("x-meili-project-id", "xxx")
            .header("x-meili-index", "from-header")
            .header("x-meili-region", "us-west")
            .body(Body::from(body))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let json = json_body(resp).await;
        assert_eq!(json["pipeline_used"], "builtin.pdf");
        assert_eq!(json["target_index"], "from-query");
        assert_eq!(json["status"], "queued");
        let job_id: Uuid = json["job_id"].as_str().unwrap().parse().unwrap();

        let inputs = starter.inputs();
        assert_eq!(inputs.len(), 1);
        let wf = &inputs[0];
        assert_eq!(wf.job_id, job_id);
        assert_eq!(wf.pipeline.uid, "builtin.pdf");
        assert_eq!(
            wf.context,
            MeiliContext {
                project_id: Some("xxx".into()),
                host: Some("https://xxx.us-west.meilisearch.io".into()),
                api_key: Some("envoyKey".into()),
                index: Some("from-query".into()),
                region: Some("us-west".into()),
            }
        );
        match &wf.input {
            PluginInput::Bytes(Blob {
                data,
                mime,
                filename,
            }) => {
                assert_eq!(data, PDF_MAGIC);
                assert_eq!(mime, "application/pdf");
                assert_eq!(filename.as_deref(), Some("report.pdf"));
            }
            other => panic!("expected inline bytes, got {other:?}"),
        }
        // resolve was called with the tenant and the filename
        let requests = server.received_requests().await.unwrap();
        let resolve = requests
            .iter()
            .find(|r| r.url.path() == "/internal/resolve")
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&resolve.body).unwrap();
        assert_eq!(body["mime"], "application/pdf");
        assert_eq!(body["filename"], "report.pdf");
        assert_eq!(body["project_id"], "xxx");
        // job was cached
        let created = requests
            .iter()
            .find(|r| r.url.path() == "/internal/jobs")
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&created.body).unwrap();
        assert_eq!(body["job_id"], job_id.to_string());
        assert_eq!(body["pipeline_uid"], "builtin.pdf");
        assert_eq!(body["index_name"], "from-query");
        assert_eq!(body["status"], "queued");
    }

    #[tokio::test]
    async fn large_upload_is_staged_as_ref() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let cfg = GatewayConfig {
            inline_max_bytes: 16,
            ..GatewayConfig::default()
        };
        let (app, starter) = test_app(&server, cfg).await;

        let (ct, body) = multipart_body("file", "big.pdf", "application/pdf", PDF_MAGIC);
        let resp = app
            .oneshot(standalone_request("/ingest", ct, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let inputs = starter.inputs();
        match &inputs[0].input {
            PluginInput::Ref(ContentRef::Staged {
                uri,
                mime,
                filename,
            }) => {
                assert!(uri.contains(&inputs[0].job_id.to_string()), "{uri}");
                assert_eq!(mime.as_deref(), Some("application/pdf"));
                assert_eq!(filename.as_deref(), Some("big.pdf"));
            }
            other => panic!("expected staged ref, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn index_pattern_from_trigger_overrides_query_and_mime_default_applies() {
        let server = MockServer::start().await;
        mount_resolve(&server, "contracts-pipe", Some("contracts")).await;
        mount_jobs_ok(&server).await;
        let (app, _starter) = test_app(&server, GatewayConfig::default()).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let resp = app
            .clone()
            .oneshot(standalone_request("/ingest?index=q", ct, body))
            .await
            .unwrap();
        let json = json_body(resp).await;
        assert_eq!(json["target_index"], "contracts");
        assert_eq!(json["pipeline_used"], "contracts-pipe");

        // no index anywhere → mime default for html
        let server2 = MockServer::start().await;
        mount_resolve(&server2, "builtin.html", None).await;
        mount_jobs_ok(&server2).await;
        let (app2, starter2) = test_app(&server2, GatewayConfig::default()).await;
        let req = Request::builder()
            .method("POST")
            .uri("/ingest?filename=page.html")
            .header(CONTENT_TYPE, "text/html")
            .header("authorization", "Bearer k")
            .header("x-meili-host", "http://localhost:7700")
            .body(Body::from("<html><body>hi</body></html>"))
            .unwrap();
        let json = json_body(app2.oneshot(req).await.unwrap()).await;
        assert_eq!(json["target_index"], "pages");
        assert_eq!(starter2.inputs()[0].context.index.as_deref(), Some("pages"));
    }

    #[tokio::test]
    async fn json_url_and_documents_bodies() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let body = json!({"url": "https://example.com/doc.pdf", "index": "from-body"}).to_string();
        let resp = app
            .clone()
            .oneshot(standalone_request(
                "/ingest",
                "application/json",
                body.into(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let json = json_body(resp).await;
        assert_eq!(json["target_index"], "from-body");
        match &starter.inputs()[0].input {
            PluginInput::Ref(ContentRef::Url {
                url,
                mime,
                filename,
            }) => {
                assert_eq!(url, "https://example.com/doc.pdf");
                assert_eq!(mime.as_deref(), Some("application/pdf"));
                assert_eq!(filename, &None);
            }
            other => panic!("{other:?}"),
        }

        let body =
            json!({"documents": [{"id": "1", "title": "Hello", "content": "world"}]}).to_string();
        let resp = app
            .oneshot(standalone_request(
                "/ingest",
                "application/json",
                body.into(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        match &starter.inputs()[1].input {
            PluginInput::Documents(d) => {
                assert_eq!(d.len(), 1);
                assert_eq!(d[0].id, "1");
                assert_eq!(d[0].content, "world");
            }
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn no_pipeline_for_mime_is_415_and_missing_context_is_400() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(
                ResponseTemplate::new(404).set_body_json(json!({"error": "no pipeline matches"})),
            )
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let req = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(CONTENT_TYPE, "application/x-unknown")
            .header("x-meili-host", "http://h")
            .header("x-meili-api-key", "k")
            .body(Body::from(vec![0u8, 1, 2, 3, 4, 5, 6, 7]))
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNSUPPORTED_MEDIA_TYPE);
        let json = json_body(resp).await;
        assert_eq!(json["code"], "unsupported_media_type");
        assert!(starter.inputs().is_empty());

        let req = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(CONTENT_TYPE, "application/pdf")
            .body(Body::from(PDF_MAGIC))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(resp).await["code"], "missing_context");
    }

    #[tokio::test]
    async fn explicit_pipeline_param_unknown_is_404() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .and(body_partial_json(json!({"pipeline": "ghost"})))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error": "pipeline ghost not found"})),
            )
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let resp = app
            .oneshot(standalone_request("/ingest?pipeline=ghost", ct, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn control_plane_down_is_502_and_job_cache_failure_is_tolerated() {
        // control plane unreachable → 502
        let cfg = GatewayConfig {
            control_plane_url: "http://127.0.0.1:9".into(),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app_with_url(cfg, "http://127.0.0.1:9").await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let resp = app
            .oneshot(standalone_request("/ingest", ct, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);

        // resolve works but POST /internal/jobs fails → still 202
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        Mock::given(method("POST"))
            .and(path("/internal/jobs"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let resp = app
            .oneshot(standalone_request("/ingest", ct, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(starter.inputs().len(), 1);
    }

    #[tokio::test]
    async fn temporal_failure_is_502() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.fail_start();
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let resp = app
            .oneshot(standalone_request("/ingest", ct, body))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        // no job row is written when the workflow did not start
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.url.path() != "/internal/jobs")
        );
    }

    #[tokio::test]
    async fn batch_continues_past_failures() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .and(body_partial_json(json!({"mime": "application/pdf"})))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"pipeline": sample_pipeline("builtin.pdf", None)})),
            )
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .and(body_partial_json(
                json!({"mime": "application/octet-stream"}),
            ))
            .respond_with(
                ResponseTemplate::new(404).set_body_json(json!({"error": "no pipeline matches"})),
            )
            .mount(&server)
            .await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"items": [
            {"url": "https://e.com/a.pdf"},
            {"s3": "s3://bucket/unknown-blob"},
            {"url": "https://e.com/b.pdf"}
        ]})
        .to_string();
        let resp = app
            .oneshot(standalone_request(
                "/ingest/batch",
                "application/json",
                body.into(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let json = json_body(resp).await;
        let jobs = json["jobs"].as_array().unwrap();
        assert_eq!(jobs.len(), 3);
        assert_eq!(jobs[0]["status"], "queued");
        assert_eq!(jobs[1]["code"], "unsupported_media_type");
        assert_eq!(jobs[2]["pipeline_used"], "builtin.pdf");
        assert_eq!(starter.inputs().len(), 2);
    }

    #[tokio::test]
    async fn batch_body_on_ingest_is_400_and_single_on_batch_is_wrapped() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"items": [{"url": "https://e.com/a.pdf"}]}).to_string();
        let resp = app
            .clone()
            .oneshot(standalone_request(
                "/ingest",
                "application/json",
                body.into(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let body = json!({"url": "https://e.com/a.pdf"}).to_string();
        let resp = app
            .oneshot(standalone_request(
                "/ingest/batch",
                "application/json",
                body.into(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(json_body(resp).await["jobs"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn body_over_limit_is_413() {
        let server = MockServer::start().await;
        let cfg = GatewayConfig {
            max_upload_mb: 1,
            ..GatewayConfig::default()
        };
        let (app, _) = test_app(&server, cfg).await;
        let big = vec![b'a'; 1024 * 1024 + 10];
        let resp = app
            .oneshot(standalone_request("/ingest", "text/plain", big))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
