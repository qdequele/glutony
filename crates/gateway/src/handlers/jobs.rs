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

use crate::context::resolve_project_id;
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

/// `GET /jobs?status=&pipeline_uid=&limit=&offset=` — recent jobs, newest first.
///
/// Scoped to the caller's tenant: the `project_id` filter is taken from the resolved
/// context, never from the query string, so one tenant cannot list another's jobs.
pub async fn list_jobs(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> Result<Json<serde_json::Value>, GatewayError> {
    let mut query: Vec<(String, String)> = Vec::new();
    for key in ["status", "pipeline_uid", "limit", "offset"] {
        if let Some(v) = params.get(key) {
            query.push((key.to_string(), v.clone()));
        }
    }
    if let Some(project_id) = resolve_project_id(&headers, &state.config) {
        query.push(("project_id".to_string(), project_id));
    }
    Ok(Json(state.control_plane.list_jobs(&query).await?))
}

/// `GET /jobs/{id}`.
pub async fn get_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<JobResponse>, GatewayError> {
    let job_id = parse_job_id(&id)?;
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

/// `POST /jobs/{id}/cancel`.
pub async fn cancel_job(
    State(state): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<CancelResponse>), GatewayError> {
    let job_id = parse_job_id(&id)?;
    state.temporal.cancel(job_id).await?;
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
            project_id: None,
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
}
