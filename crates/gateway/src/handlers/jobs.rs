//! `GET /jobs/{id}` and `POST /jobs/{id}/cancel`.
//!
//! Temporal is the source of truth (plan Decision 5): the status comes from
//! `describe` + the `progress` query; the control plane's `jobs` row is refreshed on
//! every read (write-through cache) and used as a fallback when Temporal no longer
//! knows the workflow.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use meili_ingest_plugin_sdk::{JobStatus, WorkflowProgress};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::auth::{Scope, authorize_job_read};
use crate::error::GatewayError;
use crate::state::{AppState, JobRecord, JobUpdate};

/// Response of `GET /jobs/{id}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct JobResponse {
    /// Job id.
    pub job_id: Uuid,
    /// Current status.
    pub status: JobStatus,
    /// Step currently executing (or last executed).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    /// Progress snapshot from the workflow (`None` when Temporal could not answer).
    #[serde(default)]
    pub progress: Option<WorkflowProgress>,
    /// Pipeline uid, from the cached job record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_used: Option<String>,
    /// Target index, from the cached job record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub target_index: Option<String>,
    /// Failure message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Response of `POST /jobs/{id}/cancel`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CancelResponse {
    /// Job id.
    pub job_id: Uuid,
    /// Always `"cancelling"`.
    pub status: String,
}

fn parse_job_id(id: &str) -> Result<Uuid, GatewayError> {
    Uuid::parse_str(id.trim())
        .map_err(|_| GatewayError::BadRequest(format!("invalid job id {id:?}: expected a UUID")))
}

/// `404` unless the job belongs to `tenant`. No tenant: no check (today's behavior,
/// and an admin's global view). A job without a control-plane row is hidden from
/// tenants rather than shown unchecked.
pub async fn ensure_job_visible(
    state: &AppState,
    job_id: Uuid,
    tenant: Option<&str>,
) -> Result<(), GatewayError> {
    let Some(tenant) = tenant else {
        return Ok(());
    };
    let not_found = || GatewayError::NotFound(format!("job {job_id} not found"));
    match state.control_plane.get_job(job_id).await {
        Ok(r) if r.tenant_id.as_deref() == Some(tenant) => Ok(()),
        Ok(_) | Err(GatewayError::NotFound(_)) => Err(not_found()),
        Err(e) => Err(e),
    }
}

/// `GET /jobs?status=&pipeline_uid=&limit=&offset=` — recent jobs, newest first.
///
/// Scoped to the caller's tenant: the `tenant_id` filter is taken from the resolved
/// context, never from the query string, so one tenant cannot list another's jobs.
pub async fn list_jobs(
    State(state): State<AppState>,
    scope: Scope,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, GatewayError> {
    let mut query: Vec<(String, String)> = Vec::new();
    for key in ["status", "pipeline_uid", "limit", "offset"] {
        if let Some(v) = params.get(key) {
            query.push((key.to_string(), v.clone()));
        }
    }
    if let Some(tenant_id) = scope.tenant_id.clone() {
        query.push(("tenant_id".to_string(), tenant_id));
    }
    Ok(Json(state.control_plane.list_jobs(&query).await?))
}

/// `GET /jobs/{id}`. Scoped by [`authorize_job_read`]: the management tenant or the
/// trusted edge tenant; with management auth on, a caller with neither is a `401`.
pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Result<Json<JobResponse>, GatewayError> {
    let tenant = authorize_job_read(&headers, &state.config)?;
    let job_id = parse_job_id(&id)?;
    ensure_job_visible(&state, job_id, tenant.as_deref()).await?;
    match state.temporal.progress(job_id).await? {
        Some(snapshot) => {
            let progress = snapshot.progress;
            let current_step = progress.as_ref().and_then(|p| p.current_step.clone());
            let error = progress.as_ref().and_then(|p| p.error.clone());
            // Best effort write-through; the PATCH response doubles as our cached record.
            let update = JobUpdate {
                status: Some(snapshot.status),
                current_step: current_step.clone(),
                error: error.clone(),
                index_name: None,
            };
            let record: Option<JobRecord> =
                match state.control_plane.update_job(job_id, &update).await {
                    Ok(r) => Some(r),
                    Err(e) => {
                        tracing::debug!(job_id = %job_id, "could not refresh job cache: {e}");
                        None
                    }
                };
            Ok(Json(JobResponse {
                job_id,
                status: snapshot.status,
                current_step,
                progress,
                pipeline_used: record.as_ref().map(|r| r.pipeline_uid.clone()),
                target_index: record.as_ref().and_then(|r| r.index_name.clone()),
                error,
            }))
        }
        None => {
            let record = state
                .control_plane
                .get_job(job_id)
                .await
                .map_err(|e| match e {
                    GatewayError::NotFound(_) => {
                        GatewayError::NotFound(format!("job {job_id} not found"))
                    }
                    other => other,
                })?;
            Ok(Json(JobResponse {
                job_id,
                status: record.status,
                current_step: record.current_step,
                progress: None,
                pipeline_used: Some(record.pipeline_uid),
                target_index: record.index_name,
                error: record.error,
            }))
        }
    }
}

/// Why Temporal refused to cancel `job_id`. It answers `NotFound` both for a workflow
/// that never existed and for one that already closed, so tell them apart: a job
/// Temporal still describes, or one with a control-plane row (kept past Temporal's
/// retention), has finished — `409 already_finished`; anything else is `404`.
async fn not_cancellable(state: &AppState, job_id: Uuid) -> GatewayError {
    let status = match state.temporal.progress(job_id).await {
        Ok(Some(snapshot)) => Some(snapshot.status),
        _ => state
            .control_plane
            .get_job(job_id)
            .await
            .ok()
            .map(|r| r.status),
    };
    match status {
        Some(status) if status.is_terminal() => GatewayError::AlreadyFinished(format!(
            "job {job_id} already finished ({}); there is nothing to cancel",
            status.as_str()
        )),
        // A row whose cached status never reached a terminal one, for a workflow
        // Temporal no longer knows: it is over all the same.
        Some(_) => GatewayError::AlreadyFinished(format!(
            "job {job_id} is no longer running; there is nothing to cancel"
        )),
        None => GatewayError::NotFound(format!("job {job_id} not found")),
    }
}

/// `POST /jobs/{id}/cancel` → `202`, `409 already_finished` when the job is over,
/// `404` when it does not exist or belongs to another tenant.
pub async fn cancel_job(
    State(state): State<AppState>,
    scope: Scope,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<CancelResponse>), GatewayError> {
    let job_id = parse_job_id(&id)?;
    ensure_job_visible(&state, job_id, scope.tenant()).await?;
    match state.temporal.cancel(job_id).await {
        Ok(()) => {}
        Err(GatewayError::NotFound(_)) => return Err(not_cancellable(&state, job_id).await),
        Err(e) => return Err(e),
    }
    tracing::info!(job_id = %job_id, "cancel requested");
    Ok((
        StatusCode::ACCEPTED,
        Json(CancelResponse {
            job_id,
            status: "cancelling".into(),
        }),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::JobSnapshot;
    use crate::test_support::*;
    use axum::body::Body;
    use axum::http::Request;
    use chrono::Utc;
    use serde_json::json;
    use tower::ServiceExt;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn record(job_id: Uuid) -> JobRecord {
        JobRecord {
            job_id,
            workflow_id: format!("ingest-{job_id}"),
            pipeline_uid: "builtin.pdf".into(),
            tenant_id: None,
            index_name: Some("documents".into()),
            status: JobStatus::Queued,
            current_step: None,
            error: None,
            started_at: Utc::now(),
            updated_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn running_job_reports_progress_and_refreshes_cache() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        Mock::given(method("PATCH"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(record(job_id)))
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.set_snapshot(
            job_id,
            JobSnapshot {
                status: JobStatus::Running,
                progress: Some(WorkflowProgress {
                    status: JobStatus::Running,
                    current_step: Some("chunk".into()),
                    completed_steps: 1,
                    total_steps: 3,
                    ..Default::default()
                }),
            },
        );
        let resp = app
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = json_body(resp).await;
        assert_eq!(json["job_id"], job_id.to_string());
        assert_eq!(json["status"], "running");
        assert_eq!(json["current_step"], "chunk");
        assert_eq!(json["progress"]["completed_steps"], 1);
        assert_eq!(json["pipeline_used"], "builtin.pdf");
        assert_eq!(json["target_index"], "documents");
        let patch = server
            .received_requests()
            .await
            .unwrap()
            .into_iter()
            .find(|r| r.method.as_str() == "PATCH")
            .unwrap();
        let body: serde_json::Value = serde_json::from_slice(&patch.body).unwrap();
        assert_eq!(body["status"], "running");
        assert_eq!(body["current_step"], "chunk");
    }

    #[tokio::test]
    async fn cache_failure_does_not_fail_the_request() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.set_snapshot(
            job_id,
            JobSnapshot {
                status: JobStatus::Succeeded,
                progress: None,
            },
        );
        let resp = app
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = json_body(resp).await;
        assert_eq!(json["status"], "succeeded");
        assert!(json["progress"].is_null());
        assert!(json.get("pipeline_used").is_none());
    }

    #[tokio::test]
    async fn unknown_in_temporal_falls_back_to_cache_then_404() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        let mut rec = record(job_id);
        rec.status = JobStatus::Failed;
        rec.error = Some("boom".into());
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&rec))
            .mount(&server)
            .await;
        let other = Uuid::new_v4();
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{other}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(json!({"error": "not found"})))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = json_body(resp).await;
        assert_eq!(json["status"], "failed");
        assert_eq!(json["error"], "boom");
        assert_eq!(json["pipeline_used"], "builtin.pdf");

        let resp = app
            .oneshot(
                Request::get(format!("/jobs/{other}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn invalid_uuid_is_400() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(
                Request::get("/jobs/not-a-uuid")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cancel_returns_202_and_records_the_call() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .clone()
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let json = json_body(resp).await;
        assert_eq!(json["status"], "cancelling");
        assert_eq!(json["job_id"], job_id.to_string());
        assert_eq!(starter.cancelled(), vec![job_id]);

        let resp = app
            .oneshot(
                Request::post("/jobs/xyz/cancel")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn cancelling_a_finished_job_is_409_not_404() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        let mut r = record(job_id);
        r.status = JobStatus::Failed;
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&r))
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.set_snapshot(
            job_id,
            JobSnapshot {
                status: JobStatus::Failed,
                progress: None,
            },
        );
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let json = json_body(resp).await;
        assert_eq!(json["code"], "already_finished");
        assert!(json["error"].as_str().unwrap().contains("failed"), "{json}");
        assert!(starter.cancelled().is_empty());
    }

    #[tokio::test]
    async fn cancelling_a_job_temporal_forgot_is_409_when_the_row_exists() {
        // Past Temporal's retention the workflow is gone, but the job row remains.
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        let mut r = record(job_id);
        r.status = JobStatus::Succeeded;
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&r))
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.forget(job_id);
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        assert_eq!(json_body(resp).await["code"], "already_finished");
    }

    #[tokio::test]
    async fn cancelling_an_unknown_job_is_still_404() {
        let server = MockServer::start().await; // every control-plane path 404s
        let job_id = Uuid::new_v4();
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.forget(job_id);
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert_eq!(json_body(resp).await["code"], "not_found");
    }

    fn lab_config() -> GatewayConfig {
        GatewayConfig {
            lab_service_token: Some("lab-secret".into()),
            ..GatewayConfig::default()
        }
    }

    async fn mount_job(server: &MockServer, job_id: Uuid, tenant: Option<&str>) {
        let mut r = record(job_id);
        r.tenant_id = tenant.map(str::to_string);
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(&r))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_tenant_cannot_read_or_cancel_another_tenants_job() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;

        // Data route: tenant from the trusted edge (ENVOY_TRUSTED_HEADER unset).
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .clone()
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .header("x-meili-tenant-id", "acct-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // Management route: tenant from the Lab principal.
        let (app, starter_lab) = test_app(&server, lab_config()).await;
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .header("authorization", "Bearer lab-secret")
                    .header("x-glutony-tenant-id", "acct-b")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        assert!(starter_lab.cancelled().is_empty());
        assert!(starter.cancelled().is_empty());
    }

    #[tokio::test]
    async fn the_owner_cancels_its_job() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;
        let (app, starter) = test_app(&server, lab_config()).await;
        let resp = app
            .oneshot(
                Request::post(format!("/jobs/{job_id}/cancel"))
                    .header("authorization", "Bearer lab-secret")
                    .header("x-glutony-tenant-id", "acct-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(starter.cancelled(), vec![job_id]);
    }

    #[tokio::test]
    async fn job_without_a_row_is_hidden_from_tenants() {
        // The gateway's best-effort job insert failed: Temporal knows the job, the
        // control plane does not.
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        Mock::given(method("GET"))
            .and(path(format!("/internal/jobs/{job_id}")))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(json!({"error": "not found", "code": "not_found"})),
            )
            .mount(&server)
            .await;
        let (app, starter) = test_app(&server, GatewayConfig::default()).await;
        starter.set_snapshot(
            job_id,
            JobSnapshot {
                status: JobStatus::Running,
                progress: None,
            },
        );
        let with_tenant = app
            .clone()
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .header("x-meili-tenant-id", "acct-a")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(with_tenant.status(), StatusCode::NOT_FOUND);
        // No tenant: unchanged, the Temporal snapshot answers.
        let without = app
            .oneshot(
                Request::get(format!("/jobs/{job_id}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(without.status(), StatusCode::OK);
    }

    fn get(job_id: Uuid, headers: &[(&str, &str)]) -> Request<Body> {
        let mut req = Request::get(format!("/jobs/{job_id}"));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        req.body(Body::empty()).unwrap()
    }

    /// Lab token, admin key and an edge secret, so trusted `X-Meili-*` needs the secret.
    fn auth_on_config() -> GatewayConfig {
        GatewayConfig {
            lab_service_token: Some("lab-secret".into()),
            admin_api_key: Some("admin-secret".into()),
            envoy_trusted_header: Some("edge-secret".into()),
            ..GatewayConfig::default()
        }
    }

    #[tokio::test]
    async fn auth_on_scopes_job_reads_to_the_callers_tenant() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;
        let (app, _) = test_app(&server, auth_on_config()).await;
        let read = |headers: &[(&str, &str)]| app.clone().oneshot(get(job_id, headers));

        // Another tenant: 404, through the management credential or the trusted edge.
        let resp = read(&[
            ("authorization", "Bearer lab-secret"),
            ("x-glutony-tenant-id", "acct-b"),
        ])
        .await
        .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        let resp = read(&[
            ("x-meili-tenant-id", "acct-b"),
            ("x-meili-envoy-secret", "edge-secret"),
        ])
        .await
        .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
        // An admin naming another tenant is scoped like the Lab.
        let resp = read(&[
            ("authorization", "Bearer admin-secret"),
            ("x-glutony-tenant-id", "acct-b"),
        ])
        .await
        .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        // The owner, both ways, and an admin without a tenant: visible.
        for headers in [
            &[
                ("authorization", "Bearer lab-secret"),
                ("x-glutony-tenant-id", "acct-a"),
            ][..],
            &[
                ("x-meili-tenant-id", "acct-a"),
                ("x-meili-envoy-secret", "edge-secret"),
            ][..],
            &[("authorization", "Bearer admin-secret")][..],
        ] {
            let resp = read(headers).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK, "{headers:?}");
            let json = json_body(resp).await;
            assert_eq!(json["job_id"], job_id.to_string());
            assert_eq!(json["pipeline_used"], "builtin.pdf");
        }
    }

    #[tokio::test]
    async fn auth_on_refuses_an_untenanted_unauthenticated_job_read() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;
        let (app, starter) = test_app(&server, auth_on_config()).await;
        starter.set_snapshot(
            job_id,
            JobSnapshot {
                status: JobStatus::Running,
                progress: None,
            },
        );
        for headers in [
            &[][..],
            &[("authorization", "Bearer nope")][..],
            // The owner's tenant, but no or the wrong edge secret.
            &[("x-meili-tenant-id", "acct-a")][..],
            &[
                ("x-meili-tenant-id", "acct-a"),
                ("x-meili-envoy-secret", "wrong"),
            ][..],
        ] {
            let resp = app.clone().oneshot(get(job_id, headers)).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "{headers:?}");
            assert_eq!(json_body(resp).await["code"], "unauthorized");
        }
        // Nothing was read on the caller's behalf.
        let reads = server.received_requests().await.unwrap();
        assert!(reads.is_empty(), "{reads:?}");
    }

    #[tokio::test]
    async fn open_mode_job_reads_are_unchanged() {
        let server = MockServer::start().await;
        let job_id = Uuid::new_v4();
        mount_job(&server, job_id, Some("acct-a")).await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        // No tenant: no check.
        let resp = app.clone().oneshot(get(job_id, &[])).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        // An edge tenant still scopes the read.
        let resp = app
            .clone()
            .oneshot(get(job_id, &[("x-meili-tenant-id", "acct-a")]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let resp = app
            .oneshot(get(job_id, &[("x-meili-tenant-id", "acct-b")]))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
