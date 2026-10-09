//! `POST /ingest` and `POST /ingest/batch` (SPEC §4 Tier 1), plus the submission flow
//! shared with the explicit-pipeline route.

use axum::Json;
use axum::extract::{Path, Query, Request, State};
use axum::http::{HeaderMap, StatusCode};
use chrono::Utc;
use meili_ingest_plugin_sdk::{
    ContentRef, JobStatus, MeiliContext, PipelineDefinition, PipelineWorkflowInput, PluginInput,
};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::{QueryParams, query_param, read_payload};
use crate::context::{require_destination, resolve_index, resolve_request_context};
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
///
/// Never fails on a missing destination: whether one is needed depends on the pipeline,
/// which [`submit_one`] checks once it knows it.
pub fn context_for(
    state: &AppState,
    headers: &HeaderMap,
    query: &QueryParams,
    extracted: &Extracted,
) -> Result<MeiliContext, GatewayError> {
    let query_index = query_param(query, "index").or(extracted.index.as_deref());
    resolve_request_context(headers, query_index, &state.config)
}

/// Validate an index uid taken from the URL path with Meilisearch's own rule
/// (alphanumeric, `-` and `_`, at most 400 bytes), so a malformed one is a 400 here
/// rather than a job that fails at the indexer.
pub fn path_index(index_uid: &str) -> Result<&str, GatewayError> {
    let valid = !index_uid.is_empty()
        && index_uid.len() <= 400
        && index_uid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_');
    if valid {
        Ok(index_uid)
    } else {
        Err(GatewayError::BadRequest(format!(
            "invalid index uid {index_uid:?}: only alphanumeric characters, `-` and `_` \
             are allowed, up to 400 bytes"
        )))
    }
}

/// Submit one item: route → resolve index → preflight → stage bytes → start workflow →
/// cache job.
///
/// `locked_index` is the index named by an `/indexes/{index_uid}/…` path. It wins over
/// every other source — `?index=`, the `index` field, `X-Meili-Index` and the pipeline
/// trigger's `index_pattern` — the same way `/indexes/{uid}/documents` can only ever
/// write to `uid`.
pub async fn submit_one(
    state: &AppState,
    payload: IngestPayload,
    mut ctx: MeiliContext,
    selection: &PipelineSelection,
    locked_index: Option<&str>,
) -> Result<IngestResponse, GatewayError> {
    let filename = payload.filename();
    let mime = payload
        .mime()
        .ok_or_else(|| GatewayError::BadRequest("nested batches are not supported".into()))?;

    // The worker enforces SOURCE_FETCH_HOSTS on every hop regardless; checking here too
    // turns an obviously forbidden URL into a 422 now instead of a failed job later.
    // Only a definite refusal counts: a DNS failure here may be transient, and the
    // worker re-checks (and re-resolves) at fetch time anyway.
    if let IngestPayload::Url { url, .. } = &payload {
        let parsed = url::Url::parse(url)
            .map_err(|e| GatewayError::BadRequest(format!("invalid url {url:?}: {e}")))?;
        if let Err(e @ meili_ingest_source::SourceError::Blocked(_)) =
            state.fetch_policy.check(&parsed).await
        {
            return Err(GatewayError::Unprocessable(format!(
                "{e} (URL refs obey SOURCE_FETCH_HOSTS)"
            )));
        }
    }
    // `{"s3": …}` refs must name a cloud object store; the worker refuses the rest
    // (`file://` would read its own filesystem), so say so now instead of queueing.
    if let IngestPayload::S3 { uri, .. } = &payload {
        match meili_ingest_blob::check_object_ref(uri) {
            Ok(_) => {}
            Err(e @ meili_ingest_blob::BlobError::Blocked(_)) => {
                return Err(GatewayError::Unprocessable(e.to_string()));
            }
            Err(e) => return Err(GatewayError::BadRequest(e.to_string())),
        }
    }

    let (pipeline, index_pattern) = match selection {
        PipelineSelection::Auto { explicit } => {
            match state
                .control_plane
                .resolve(
                    &mime,
                    filename.as_deref(),
                    ctx.tenant_id.as_deref(),
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

    // Only now is it known whether the request must carry a destination: a pipeline
    // pinned to Meilisearch connections needs none (spec Decision 12).
    if !pipeline.pins_destination() {
        require_destination(&ctx)?;
    }

    // An indexer step that names its own `index` writes there whatever the request says
    // (the worker only fills an unset one). Under `/indexes/{uid}/…` that would write
    // somewhere other than the path promises, so refuse rather than queue; overriding
    // the step instead would let a caller aim a pinned connection's key at any index.
    if let Some(index) = locked_index
        && let Some(other) = pipeline.step_indexes().find(|named| *named != index)
    {
        return Err(GatewayError::Unprocessable(format!(
            "pipeline {:?} writes to index {other:?}, not {index:?}; send it to \
             /ingest/pipeline/{} instead",
            pipeline.uid, pipeline.uid
        )));
    }

    let target_index = match (locked_index, pipeline.fixed_index()) {
        (Some(index), _) => {
            ctx.index = Some(index.to_string());
            index.to_string()
        }
        // Report (and preflight) the index the job will actually write to.
        (None, Some(fixed)) => {
            ctx.index = Some(fixed.to_string());
            fixed.to_string()
        }
        (None, None) => resolve_index(
            &mut ctx,
            index_pattern.as_deref(),
            &mime,
            &state.config.default_index,
        ),
    };

    // A pinned pipeline writes with its connection's sealed key, which the caller never
    // sees; only the request's own key is the caller's to prove.
    if state.config.write_preflight
        && !pipeline.pins_destination()
        && let (Some(host), Some(key)) = (&ctx.host, &ctx.api_key)
    {
        crate::preflight::check_write(&state.http, host, key, &target_index).await?;
    }

    // A Lab engine refuses work for an account out of credits (spec v2 §8.1), and
    // refuses rather than runs unbilled work when the Lab cannot answer (decision A:
    // every Lab-account job is billed).
    if let (Some(lab), Some(account)) = (&state.lab, ctx.tenant_id.as_deref()) {
        lab.check_credits(account).await?;
    }

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
        tenant_id: ctx.tenant_id.clone(),
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
    locked_index: Option<&str>,
) -> BatchResponse {
    let mut jobs = Vec::with_capacity(items.len());
    for item in items {
        match submit_one(state, item, ctx.clone(), selection, locked_index).await {
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
    ingest_inner(state, query, headers, req, None).await
}

/// `POST /indexes/{index_uid}/ingest` — auto-routing into the index named by the path.
pub async fn ingest_into_index(
    State(state): State<AppState>,
    Path(index_uid): Path<String>,
    Query(query): Query<QueryParams>,
    headers: HeaderMap,
    req: Request,
) -> Result<(StatusCode, Json<IngestResponse>), GatewayError> {
    let index = path_index(&index_uid)?;
    ingest_inner(state, query, headers, req, Some(index)).await
}

async fn ingest_inner(
    state: AppState,
    query: QueryParams,
    headers: HeaderMap,
    req: Request,
    locked_index: Option<&str>,
) -> Result<(StatusCode, Json<IngestResponse>), GatewayError> {
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
        locked_index,
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
    ingest_batch_inner(state, query, headers, req, None).await
}

/// `POST /indexes/{index_uid}/ingest/batch` — every item goes to the path's index.
pub async fn ingest_batch_into_index(
    State(state): State<AppState>,
    Path(index_uid): Path<String>,
    Query(query): Query<QueryParams>,
    headers: HeaderMap,
    req: Request,
) -> Result<(StatusCode, Json<BatchResponse>), GatewayError> {
    let index = path_index(&index_uid)?;
    ingest_batch_inner(state, query, headers, req, Some(index)).await
}

async fn ingest_batch_inner(
    state: AppState,
    query: QueryParams,
    headers: HeaderMap,
    req: Request,
    locked_index: Option<&str>,
) -> Result<(StatusCode, Json<BatchResponse>), GatewayError> {
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
    let resp = submit_batch(
        &state,
        items,
        &ctx,
        &PipelineSelection::Auto { explicit },
        locked_index,
    )
    .await;
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

    /// A JSON-documents ingest the way the qdq Caddy (standing in for Envoy) forwards it:
    /// host and tenant injected, the caller's own key left in `Authorization`.
    fn index_scoped_request(uri: &str, host: &str, key: &str) -> Request<Body> {
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(CONTENT_TYPE, "application/json")
            .header("x-meili-host", host)
            .header("x-meili-project-id", "hackersearch")
            .header("authorization", format!("Bearer {key}"))
            .body(Body::from(
                json!({"documents": [{"id": "1"}], "index": "from-field"}).to_string(),
            ))
            .unwrap()
    }

    #[tokio::test]
    async fn path_index_beats_query_field_and_index_pattern() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.json", Some("from-pattern")).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let resp = app
            .oneshot(index_scoped_request(
                "/indexes/movies/ingest?index=from-query",
                "http://meili",
                "callerKey",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(json_body(resp).await["target_index"], "movies");
        let ctx = &starter.inputs()[0].context;
        assert_eq!(ctx.index.as_deref(), Some("movies"));
        assert_eq!(
            ctx.api_key.as_deref(),
            Some("callerKey"),
            "caller key forwarded"
        );
    }

    /// Mount `POST /internal/resolve` → a pipeline whose indexer step names `index`.
    async fn mount_resolve_step_index(server: &MockServer, index: &str) {
        let mut pipeline = sample_pipeline("p", None);
        pipeline.steps[0].config = json!({ "index": index });
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({"pipeline": pipeline})))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn path_index_refuses_a_pipeline_that_writes_elsewhere() {
        // The worker keeps a step's own `index`, so `/indexes/hn/…` would say `hn` and
        // write to `movies`: refuse instead of queueing.
        let server = MockServer::start().await;
        mount_resolve_step_index(&server, "movies").await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let resp = app
            .oneshot(index_scoped_request(
                "/indexes/hn/ingest",
                "http://meili",
                "k",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert!(
            json_body(resp).await["error"]
                .as_str()
                .unwrap()
                .contains("movies")
        );
        assert!(starter.inputs().is_empty());
    }

    #[tokio::test]
    async fn a_step_index_is_the_reported_target() {
        let server = MockServer::start().await;
        mount_resolve_step_index(&server, "movies").await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        for uri in ["/indexes/movies/ingest", "/ingest?index=from-query"] {
            let resp = app
                .clone()
                .oneshot(index_scoped_request(uri, "http://meili", "k"))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::ACCEPTED, "{uri}");
            assert_eq!(json_body(resp).await["target_index"], "movies", "{uri}");
        }
        assert_eq!(starter.inputs().len(), 2);
    }

    #[tokio::test]
    async fn path_index_applies_to_every_batch_item() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", Some("from-pattern")).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let body =
            json!({"items": [{"url": "https://e.com/a.pdf"}, {"url": "https://e.com/b.pdf"}]});
        let req = Request::builder()
            .method("POST")
            .uri("/indexes/movies/ingest/batch")
            .header(CONTENT_TYPE, "application/json")
            .header("x-meili-host", "http://meili")
            .header("authorization", "Bearer k")
            .body(Body::from(body.to_string()))
            .unwrap();
        assert_eq!(
            app.oneshot(req).await.unwrap().status(),
            StatusCode::ACCEPTED
        );
        let inputs = starter.inputs();
        assert_eq!(inputs.len(), 2);
        assert!(
            inputs
                .iter()
                .all(|i| i.context.index.as_deref() == Some("movies"))
        );
    }

    #[tokio::test]
    async fn invalid_path_index_is_rejected_before_anything_runs() {
        let server = MockServer::start().await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(index_scoped_request(
                "/indexes/bad.uid/ingest",
                "http://meili",
                "k",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert!(starter.inputs().is_empty());
    }

    #[tokio::test]
    async fn preflight_refuses_a_key_that_cannot_write_the_index() {
        let cp = MockServer::start().await;
        mount_resolve(&cp, "builtin.json", None).await;
        mount_jobs_ok(&cp).await;
        let meili = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/indexes/hn/documents"))
            .respond_with(ResponseTemplate::new(403).set_body_json(json!({
                "message": "The API key cannot acces the index `hn`, authorized indexes are [\"glutony-*\"].",
                "code": "invalid_api_key"
            })))
            .mount(&meili)
            .await;
        Mock::given(method("POST"))
            .and(path("/indexes/glutony-demo/documents"))
            .respond_with(ResponseTemplate::new(415))
            .mount(&meili)
            .await;
        Mock::given(method("GET"))
            .and(path("/indexes/glutony-demo"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&meili)
            .await;
        let config = GatewayConfig {
            write_preflight: true,
            ..Default::default()
        };
        let (app, starter) = test_app(&cp, config).await;

        let resp = app
            .clone()
            .oneshot(index_scoped_request(
                "/indexes/hn/ingest",
                &meili.uri(),
                "scoped",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert!(
            json_body(resp).await["error"]
                .as_str()
                .unwrap()
                .contains("authorized indexes")
        );
        assert!(starter.inputs().is_empty(), "no job for a refused key");

        let resp = app
            .oneshot(index_scoped_request(
                "/indexes/glutony-demo/ingest",
                &meili.uri(),
                "scoped",
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(starter.inputs().len(), 1);
    }

    #[tokio::test]
    async fn a_url_outside_the_fetch_policy_is_422_and_queues_nothing() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        for url in [
            "http://127.0.0.1:8123/?query=SELECT%201",
            "https://169.254.169.254/latest/meta-data/",
        ] {
            let resp = app
                .clone()
                .oneshot(standalone_request(
                    "/ingest",
                    "application/json",
                    json!({ "url": url }).to_string().into(),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY, "{url}");
            assert!(
                json_body(resp).await["error"]
                    .as_str()
                    .unwrap()
                    .contains("SOURCE_FETCH_HOSTS")
            );
        }
        assert!(starter.inputs().is_empty());
    }

    #[tokio::test]
    async fn an_object_ref_outside_cloud_stores_is_422_and_queues_nothing() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.text", None).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        for uri in ["file:///etc/passwd", "memory:///k"] {
            let resp = app
                .clone()
                .oneshot(standalone_request(
                    "/ingest",
                    "application/json",
                    json!({ "s3": uri, "filename": "x.txt" }).to_string().into(),
                ))
                .await
                .unwrap();
            assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY, "{uri}");
        }
        assert!(starter.inputs().is_empty());
    }

    #[tokio::test]
    async fn batch_items_are_checked_one_by_one() {
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"items": [
            {"url": "https://1.1.1.1/a.pdf"},
            {"url": "http://localhost:9090/api/v1/query"}
        ]});
        let resp = app
            .oneshot(standalone_request(
                "/indexes/movies/ingest/batch",
                "application/json",
                body.to_string().into(),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let jobs = json_body(resp).await["jobs"].clone();
        assert!(jobs[0]["job_id"].is_string(), "{jobs}");
        assert_eq!(jobs[1]["code"], "validation", "{jobs}");
        assert_eq!(starter.inputs().len(), 1);
    }

    #[test]
    fn path_index_follows_meilisearch_uid_rules() {
        assert!(path_index("glutony-demo_2").is_ok());
        assert!(path_index("").is_err());
        assert!(path_index("a b").is_err());
        assert!(path_index(&"a".repeat(401)).is_err());
    }

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
                tenant_id: Some("xxx".into()),
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
        assert_eq!(body["tenant_id"], "xxx");
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
    }

    #[tokio::test]
    async fn missing_context_is_400_for_a_pipeline_without_a_connection() {
        // The context is checked once the pipeline is known, so this request is routed
        // first; its pipeline pins no connection, so the request's context is required.
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        let req = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(CONTENT_TYPE, "application/pdf")
            .body(Body::from(PDF_MAGIC))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        assert_eq!(json_body(resp).await["code"], "missing_context");
        assert!(starter.inputs().is_empty(), "no workflow is started");
    }

    #[tokio::test]
    async fn a_pipeline_pinned_to_a_connection_needs_no_context() {
        let server = MockServer::start().await;
        let mut pinned = sample_pipeline("movies", None);
        pinned.steps[0].config = json!({ "connection": "prod-movies" });
        Mock::given(method("POST"))
            .and(path("/internal/resolve"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "pipeline": pinned })))
            .mount(&server)
            .await;
        mount_jobs_ok(&server).await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;

        // No X-Meili-* headers, no bearer token, no MEILI_URL / MEILI_API_KEY.
        let req = Request::builder()
            .method("POST")
            .uri("/ingest")
            .header(CONTENT_TYPE, "application/pdf")
            .body(Body::from(PDF_MAGIC))
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let inputs = starter.inputs();
        assert_eq!(inputs.len(), 1);
        assert!(
            inputs[0].context.host.is_none() && inputs[0].context.api_key.is_none(),
            "the destination comes from the connection, not the request"
        );
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

    #[tokio::test]
    async fn a_hosted_engine_refuses_a_lab_account_without_credits() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};
        const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
        let server = MockServer::start().await;
        mount_resolve(&server, "builtin.pdf", None).await;
        mount_jobs_ok(&server).await;
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": ACCOUNT, "tier": "free",
                "credits": {"balance": 0}, "cache_ttl": 30
            })))
            .mount(&lab)
            .await;
        let client = std::sync::Arc::new(crate::lab::LabClient::new(
            meili_ingest_lab::LabCredentials::new(
                &lab.uri(),
                "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d",
                "s",
            )
            .unwrap(),
            reqwest::Client::new(),
        ));
        client.set_identity(meili_ingest_lab::InstanceInfo {
            instance_id: "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d".into(),
            kind: meili_ingest_lab::InstanceKind::Hosted,
            product: "glutony".into(),
            region: None,
            lab_url: None,
        });
        let (app, starter) = test_app_with_lab(&server, GatewayConfig::default(), client).await;
        let (ct, body) = multipart_body("file", "a.pdf", "application/pdf", PDF_MAGIC);
        let mut req = standalone_request("/ingest", ct, body);
        req.headers_mut()
            .insert("x-meili-tenant-id", ACCOUNT.parse().unwrap());
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYMENT_REQUIRED);
        assert_eq!(json_body(resp).await["code"], "insufficient_credits");
        assert!(starter.inputs().is_empty(), "nothing was queued");
    }
}
