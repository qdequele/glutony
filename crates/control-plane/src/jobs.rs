//! Denormalized job cache (`jobs` table). Temporal stays the source of truth; the
//! gateway writes through here so status polls do not always hit Temporal.

use axum::Json;
use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use chrono::{DateTime, Utc};
use meili_ingest_plugin_sdk::JobStatus;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use uuid::Uuid;

use crate::error::CpError;
use crate::{AppState, JsonBody};

/// One row of the `jobs` table, as exchanged with the gateway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    /// Job id (also the workflow input's `job_id`).
    pub job_id: Uuid,
    /// Temporal workflow id (`ingest-<job_id>`).
    pub workflow_id: String,
    /// Pipeline that was started.
    pub pipeline_uid: String,
    /// Tenant scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Resolved target index.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Last known status.
    #[serde(default)]
    pub status: JobStatus,
    /// Step currently running (or last seen).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    /// Failure message, when failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// When the job was accepted.
    pub started_at: DateTime<Utc>,
    /// Last write.
    pub updated_at: DateTime<Utc>,
    /// Scheduled source that started this job; `None` for request-driven jobs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_id: Option<Uuid>,
}

/// Partial update for `PATCH /internal/jobs/{job_id}`; absent fields are left as-is.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobUpdate {
    /// New status.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<JobStatus>,
    /// New current step.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub current_step: Option<String>,
    /// New error message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    /// New index name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
}

/// Parse the `status` column (stored via [`JobStatus::as_str`]).
pub fn parse_status(s: &str) -> Option<JobStatus> {
    match s {
        "queued" => Some(JobStatus::Queued),
        "running" => Some(JobStatus::Running),
        "succeeded" => Some(JobStatus::Succeeded),
        "failed" => Some(JobStatus::Failed),
        "cancelled" => Some(JobStatus::Cancelled),
        _ => None,
    }
}

/// Raw row; `status` is text in the database.
#[derive(Debug, sqlx::FromRow)]
struct JobRow {
    job_id: Uuid,
    workflow_id: String,
    pipeline_uid: String,
    project_id: Option<String>,
    index_name: Option<String>,
    status: String,
    current_step: Option<String>,
    error: Option<String>,
    started_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
    source_id: Option<Uuid>,
}

impl TryFrom<JobRow> for JobRecord {
    type Error = CpError;

    fn try_from(r: JobRow) -> Result<Self, CpError> {
        let status = parse_status(&r.status).ok_or_else(|| {
            CpError::Internal(format!(
                "job {} has unknown status {:?}",
                r.job_id, r.status
            ))
        })?;
        Ok(JobRecord {
            job_id: r.job_id,
            workflow_id: r.workflow_id,
            pipeline_uid: r.pipeline_uid,
            project_id: r.project_id,
            index_name: r.index_name,
            status,
            current_step: r.current_step,
            error: r.error,
            started_at: r.started_at,
            updated_at: r.updated_at,
            source_id: r.source_id,
        })
    }
}

/// Insert a job row (idempotent: a repeated insert of the same `job_id` overwrites).
pub async fn insert_job(pool: &PgPool, job: &JobRecord) -> Result<JobRecord, CpError> {
    let row: JobRow = sqlx::query_as(
        "INSERT INTO jobs (job_id, workflow_id, pipeline_uid, project_id, index_name, status, \
                           current_step, error, started_at, updated_at, source_id) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11) \
         ON CONFLICT (job_id) DO UPDATE SET \
            workflow_id = EXCLUDED.workflow_id, pipeline_uid = EXCLUDED.pipeline_uid, \
            project_id = EXCLUDED.project_id, index_name = EXCLUDED.index_name, \
            status = EXCLUDED.status, current_step = EXCLUDED.current_step, \
            error = EXCLUDED.error, started_at = EXCLUDED.started_at, updated_at = now(), \
            source_id = COALESCE(EXCLUDED.source_id, jobs.source_id) \
         RETURNING job_id, workflow_id, pipeline_uid, project_id, index_name, status, \
                   current_step, error, started_at, updated_at, source_id",
    )
    .bind(job.job_id)
    .bind(&job.workflow_id)
    .bind(&job.pipeline_uid)
    .bind(job.project_id.as_deref())
    .bind(job.index_name.as_deref())
    .bind(job.status.as_str())
    .bind(job.current_step.as_deref())
    .bind(job.error.as_deref())
    .bind(job.started_at)
    .bind(job.updated_at)
    .bind(job.source_id)
    .fetch_one(pool)
    .await?;
    JobRecord::try_from(row)
}

/// Apply a partial update; `None` when the job does not exist.
pub async fn update_job_row(
    pool: &PgPool,
    job_id: Uuid,
    upd: &JobUpdate,
) -> Result<Option<JobRecord>, CpError> {
    // Invariant enforced here rather than trusted from the caller: a job that
    // succeeded is not "on" a step, so the last running value must not stick and make
    // the job list read "succeeded, extract". Failed and cancelled jobs keep it,
    // because where they stopped is exactly what an operator wants to see.
    let clear_step = upd.status == Some(JobStatus::Succeeded);
    let row: Option<JobRow> = sqlx::query_as(
        "UPDATE jobs SET \
            status = COALESCE($2, status), \
            current_step = CASE WHEN $6 THEN NULL ELSE COALESCE($3, current_step) END, \
            error = COALESCE($4, error), \
            index_name = COALESCE($5, index_name), \
            updated_at = now() \
         WHERE job_id = $1 \
         RETURNING job_id, workflow_id, pipeline_uid, project_id, index_name, status, \
                   current_step, error, started_at, updated_at, source_id",
    )
    .bind(job_id)
    .bind(upd.status.map(|s| s.as_str()))
    .bind(upd.current_step.as_deref())
    .bind(upd.error.as_deref())
    .bind(upd.index_name.as_deref())
    .bind(clear_step)
    .fetch_optional(pool)
    .await?;
    row.map(JobRecord::try_from).transpose()
}

/// Fetch one job.
pub async fn fetch_job(pool: &PgPool, job_id: Uuid) -> Result<Option<JobRecord>, CpError> {
    let row: Option<JobRow> = sqlx::query_as(
        "SELECT job_id, workflow_id, pipeline_uid, project_id, index_name, status, \
                current_step, error, started_at, updated_at, source_id \
         FROM jobs WHERE job_id = $1",
    )
    .bind(job_id)
    .fetch_optional(pool)
    .await?;
    row.map(JobRecord::try_from).transpose()
}

/// `POST /internal/jobs` body `JobRecord` → 201 `JobRecord`.
pub async fn create_job(
    State(state): State<AppState>,
    JsonBody(job): JsonBody<JobRecord>,
) -> Result<Response, CpError> {
    let stored = insert_job(&state.pool, &job).await?;
    tracing::info!(job_id = %stored.job_id, pipeline = %stored.pipeline_uid, "job recorded");
    Ok((StatusCode::CREATED, Json(stored)).into_response())
}

/// `PATCH /internal/jobs/{job_id}` body `JobUpdate` → 200 `JobRecord` | 404.
pub async fn update_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
    JsonBody(upd): JsonBody<JobUpdate>,
) -> Result<Json<JobRecord>, CpError> {
    update_job_row(&state.pool, job_id, &upd)
        .await?
        .map(Json)
        .ok_or_else(|| CpError::NotFound(format!("job {job_id} not found")))
}

/// Filters accepted by [`list_jobs`].
#[derive(Debug, Clone, Default, Deserialize)]
pub struct JobListQuery {
    /// Tenant scope. Falls back to the `X-Meili-Project-Id` header.
    pub project_id: Option<String>,
    /// Only jobs in this status.
    pub status: Option<String>,
    /// Only jobs started by this pipeline.
    pub pipeline_uid: Option<String>,
    /// Page size, 1..=200, default 50.
    pub limit: Option<i64>,
    /// Rows to skip, for paging.
    pub offset: Option<i64>,
}

/// One page of jobs.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobList {
    /// The jobs, newest first.
    pub jobs: Vec<JobRecord>,
    /// Page size that was applied.
    pub limit: i64,
    /// Offset that was applied.
    pub offset: i64,
    /// Total rows matching the filters, ignoring paging. Lets a client show a page
    /// count and disable "next" on the real last page instead of guessing from
    /// whether the page came back full.
    pub total: i64,
}

/// `GET /jobs?project_id=&status=&pipeline_uid=&limit=&offset=` → newest first.
///
/// Reads the denormalized `jobs` table rather than Temporal: listing is a browsing
/// operation and must not fan out gRPC calls per row. Individual job detail still
/// comes from Temporal, which stays the source of truth for live status.
pub async fn list_jobs(
    State(state): State<AppState>,
    Query(q): Query<JobListQuery>,
    headers: HeaderMap,
) -> Result<Json<JobList>, CpError> {
    let project_id = crate::project_scope(q.project_id.as_deref(), &headers);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let offset = q.offset.unwrap_or(0).max(0);

    // A single statement with NULL-tolerant predicates keeps the SQL static, which
    // sqlx 0.9 requires, and lets Postgres use the (project_id, started_at) index.
    let rows: Vec<JobRow> = sqlx::query_as(
        "SELECT job_id, workflow_id, pipeline_uid, project_id, index_name, status, \
                current_step, error, started_at, updated_at, source_id \
         FROM jobs \
         WHERE ($1::text IS NULL OR project_id = $1) \
           AND ($2::text IS NULL OR status = $2) \
           AND ($3::text IS NULL OR pipeline_uid = $3) \
         ORDER BY started_at DESC \
         LIMIT $4 OFFSET $5",
    )
    .bind(project_id.as_deref())
    .bind(q.status.as_deref())
    .bind(q.pipeline_uid.as_deref())
    .bind(limit)
    .bind(offset)
    .fetch_all(&state.pool)
    .await?;

    let jobs = rows
        .into_iter()
        .map(JobRecord::try_from)
        .collect::<Result<Vec<_>, _>>()?;

    let (total,): (i64,) = sqlx::query_as(
        "SELECT count(*) FROM jobs \
         WHERE ($1::text IS NULL OR project_id = $1) \
           AND ($2::text IS NULL OR status = $2) \
           AND ($3::text IS NULL OR pipeline_uid = $3)",
    )
    .bind(project_id.as_deref())
    .bind(q.status.as_deref())
    .bind(q.pipeline_uid.as_deref())
    .fetch_one(&state.pool)
    .await?;

    Ok(Json(JobList {
        jobs,
        limit,
        offset,
        total,
    }))
}

/// `GET /internal/jobs/{job_id}` → `JobRecord` | 404.
pub async fn get_job(
    State(state): State<AppState>,
    Path(job_id): Path<Uuid>,
) -> Result<Json<JobRecord>, CpError> {
    fetch_job(&state.pool, job_id)
        .await?
        .map(Json)
        .ok_or_else(|| CpError::NotFound(format!("job {job_id} not found")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_round_trips_through_text_column() {
        for s in [
            JobStatus::Queued,
            JobStatus::Running,
            JobStatus::Succeeded,
            JobStatus::Failed,
            JobStatus::Cancelled,
        ] {
            assert_eq!(parse_status(s.as_str()), Some(s));
            // The JSON form and the column form are the same lowercase word.
            let json = serde_json::to_string(&s).unwrap();
            assert_eq!(json, format!("\"{}\"", s.as_str()));
        }
        assert_eq!(parse_status("QUEUED"), None);
        assert_eq!(parse_status(""), None);
    }

    #[test]
    fn row_with_unknown_status_is_an_internal_error() {
        let now = Utc::now();
        let row = JobRow {
            job_id: Uuid::nil(),
            workflow_id: "ingest-x".into(),
            pipeline_uid: "builtin.pdf".into(),
            project_id: None,
            index_name: None,
            status: "weird".into(),
            current_step: None,
            error: None,
            started_at: now,
            updated_at: now,
            source_id: None,
        };
        assert_eq!(JobRecord::try_from(row).unwrap_err().code(), "internal");
    }

    #[test]
    fn job_update_defaults_to_no_change() {
        let u: JobUpdate = serde_json::from_str("{}").unwrap();
        assert_eq!(u, JobUpdate::default());
        let u: JobUpdate = serde_json::from_str(r#"{"status":"failed","error":"boom"}"#).unwrap();
        assert_eq!(u.status, Some(JobStatus::Failed));
        assert_eq!(u.error.as_deref(), Some("boom"));
        assert_eq!(u.current_step, None);
    }

    #[test]
    fn job_record_json_shape() {
        let now = Utc::now();
        let rec = JobRecord {
            job_id: Uuid::nil(),
            workflow_id: "ingest-00000000-0000-0000-0000-000000000000".into(),
            pipeline_uid: "builtin.pdf".into(),
            project_id: Some("t1".into()),
            index_name: None,
            status: JobStatus::Queued,
            current_step: None,
            error: None,
            started_at: now,
            updated_at: now,
            source_id: None,
        };
        let v = serde_json::to_value(&rec).unwrap();
        assert!(
            v.get("source_id").is_none(),
            "request-driven jobs omit source_id, so older readers are unaffected"
        );
        assert_eq!(v["status"], "queued");
        assert_eq!(v["project_id"], "t1");
        assert!(v.get("index_name").is_none());
        let back: JobRecord = serde_json::from_value(v).unwrap();
        assert_eq!(back, rec);
    }
}
