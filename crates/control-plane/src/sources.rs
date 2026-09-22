//! Scheduled-source repository (`sources`, `source_runs`).
//!
//! The repository never decrypts anything: `fetch_auth` moves through it as opaque
//! sealed bytes. Only the gateway and the worker activity hold a `SecretKey`, which keeps
//! the blast radius of the control plane small.
//!
//! A source holds no Meilisearch destination. That lives on its pipeline's
//! `meili_indexer` step as a named connection, since a cron run has no request to carry
//! one (spec Decision 6).
//!
//! Temporal owns the schedule; `sources.last_*` and `source_runs` are a queryable mirror
//! for the UI, the same arrangement `jobs` has with workflow state.

use chrono::{DateTime, Utc};
use meili_ingest_source::model::{IncrementalState, Location, RunOutcome, SourceDefinition};
use serde::{Deserialize, Serialize};
// Every query below is built from the `SOURCE_COLUMNS` const plus string literals — no
// caller data is ever interpolated — which is the audit `AssertSqlSafe` asks for.
use sqlx::types::Json as SqlJson;
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

use crate::error::CpError;

/// Everything needed to create a source. Secrets arrive already sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NewSource {
    /// Surrogate id, chosen by the caller so it can name the Temporal schedule.
    pub id: Uuid,
    /// Handle, unique per project.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Optional description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Tenant scope; `None` = global.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Pipeline this source feeds.
    pub pipeline_uid: String,
    /// Where the content comes from.
    pub location: Location,
    /// Cron expression.
    pub cron: String,
    /// IANA timezone.
    pub timezone: String,
    /// Index override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// Sealed fetch credential; `None` for an unauthenticated source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_auth: Option<Vec<u8>>,
    /// Temporal schedule id.
    pub schedule_id: String,
}

/// Partial update. Absent fields are left untouched, which is what makes a `PATCH` that
/// omits `auth` preserve the stored credential.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourcePatch {
    /// New display name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// New description.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// New pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pipeline_uid: Option<String>,
    /// New location.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<Location>,
    /// New cron.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cron: Option<String>,
    /// New timezone.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timezone: Option<String>,
    /// New index override.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub index_name: Option<String>,
    /// `Some(Some(bytes))` replaces the credential, `Some(None)` clears it, `None`
    /// leaves it alone. That three-way distinction is the whole point of the type.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_auth: Option<Option<Vec<u8>>>,
    /// New paused flag.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paused: Option<bool>,
}

/// A source as stored, including its sealed secrets and incremental state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceRecord {
    /// The public shape.
    #[serde(flatten)]
    pub definition: SourceDefinition,
    /// Sealed fetch credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub fetch_auth: Option<Vec<u8>>,
    /// What the previous run learned.
    #[serde(default)]
    pub state: IncrementalState,
    /// When the last run started.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_at: Option<DateTime<Utc>>,
    /// Outcome of the last run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_status: Option<String>,
    /// Error of the last run, when it failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

/// One row of `source_runs`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RunRecord {
    /// Run id.
    pub run_id: Uuid,
    /// Source this run belongs to.
    pub source_id: Uuid,
    /// When the run started.
    pub started_at: DateTime<Utc>,
    /// When it finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at: Option<DateTime<Utc>>,
    /// Terminal outcome.
    pub outcome: RunOutcome,
    /// Number of source items ingested.
    #[serde(default)]
    pub items: i32,
    /// Jobs started by this run.
    #[serde(default)]
    pub job_ids: Vec<Uuid>,
    /// Failure message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Columns selected by every source query, in the order [`SourceRow`] declares them.
const SOURCE_COLUMNS: &str = "id, uid, name, description, project_id, pipeline_uid, location, \
     cron, timezone, paused, index_name, schedule_id, archived_at, fetch_auth, \
     last_etag, last_modified, last_hash, last_run_at, last_status, last_error";

/// Raw row as Postgres returns it.
#[derive(Debug, sqlx::FromRow)]
struct SourceRow {
    id: Uuid,
    uid: String,
    name: String,
    description: Option<String>,
    project_id: Option<String>,
    pipeline_uid: String,
    location: SqlJson<Location>,
    cron: String,
    timezone: String,
    paused: bool,
    index_name: Option<String>,
    schedule_id: String,
    archived_at: Option<DateTime<Utc>>,
    fetch_auth: Option<Vec<u8>>,
    last_etag: Option<String>,
    last_modified: Option<String>,
    last_hash: Option<String>,
    last_run_at: Option<DateTime<Utc>>,
    last_status: Option<String>,
    last_error: Option<String>,
}

impl From<SourceRow> for SourceRecord {
    fn from(r: SourceRow) -> Self {
        SourceRecord {
            definition: SourceDefinition {
                id: r.id,
                uid: r.uid,
                name: r.name,
                description: r.description,
                project_id: r.project_id,
                pipeline_uid: r.pipeline_uid,
                location: r.location.0,
                cron: r.cron,
                timezone: r.timezone,
                paused: r.paused,
                index_name: r.index_name,
                schedule_id: r.schedule_id,
                archived_at: r.archived_at,
            },
            fetch_auth: r.fetch_auth,
            state: IncrementalState {
                etag: r.last_etag,
                last_modified: r.last_modified,
                hash: r.last_hash,
            },
            last_run_at: r.last_run_at,
            last_status: r.last_status,
            last_error: r.last_error,
        }
    }
}

/// Raw `source_runs` row; `outcome` is text in the database.
#[derive(Debug, sqlx::FromRow)]
struct RunRow {
    run_id: Uuid,
    source_id: Uuid,
    started_at: DateTime<Utc>,
    finished_at: Option<DateTime<Utc>>,
    outcome: String,
    items: i32,
    job_ids: Vec<Uuid>,
    error: Option<String>,
}

impl TryFrom<RunRow> for RunRecord {
    type Error = CpError;

    fn try_from(r: RunRow) -> Result<Self, CpError> {
        let outcome: RunOutcome = r.outcome.parse().map_err(|_| {
            CpError::Internal(format!(
                "source run {} has unknown outcome {:?}",
                r.run_id, r.outcome
            ))
        })?;
        Ok(RunRecord {
            run_id: r.run_id,
            source_id: r.source_id,
            started_at: r.started_at,
            finished_at: r.finished_at,
            outcome,
            items: r.items,
            job_ids: r.job_ids,
            error: r.error,
        })
    }
}

/// Repository over `sources` and `source_runs`.
#[derive(Debug, Clone)]
pub struct SourceRepo {
    pool: PgPool,
}

impl SourceRepo {
    /// Repository over `pool`.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Global sources plus the ones scoped to `project_id`.
    ///
    /// Archived sources are hidden unless `include_archived`; they never fire, so a list
    /// that showed them by default would read as a set of broken schedules.
    pub async fn list(
        &self,
        project_id: Option<&str>,
        include_archived: bool,
    ) -> Result<Vec<SourceRecord>, CpError> {
        let sql = format!(
            "SELECT {SOURCE_COLUMNS} FROM sources \
             WHERE (project_id IS NULL OR project_id = $1) \
               AND ($2 OR archived_at IS NULL) \
             ORDER BY (project_id IS NULL), uid"
        );
        let rows: Vec<SourceRow> = sqlx::query_as(AssertSqlSafe(sql))
            .bind(project_id)
            .bind(include_archived)
            .fetch_all(&self.pool)
            .await?;
        Ok(rows.into_iter().map(SourceRecord::from).collect())
    }

    /// Fetch one source by uid: the tenant-scoped row when `project_id` is given and
    /// exists, otherwise the global row.
    pub async fn get(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<Option<SourceRecord>, CpError> {
        let sql = format!(
            "SELECT {SOURCE_COLUMNS} FROM sources \
             WHERE uid = $1 AND (project_id IS NULL OR project_id = $2) \
             ORDER BY (project_id IS NULL) \
             LIMIT 1"
        );
        let row: Option<SourceRow> = sqlx::query_as(AssertSqlSafe(sql))
            .bind(uid)
            .bind(project_id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(SourceRecord::from))
    }

    /// Fetch one source by id for a run. Archived sources are never returned: a schedule
    /// that outlived its archive must not ingest.
    pub async fn load_for_run(&self, id: Uuid) -> Result<Option<SourceRecord>, CpError> {
        let sql =
            format!("SELECT {SOURCE_COLUMNS} FROM sources WHERE id = $1 AND archived_at IS NULL");
        let row: Option<SourceRow> = sqlx::query_as(AssertSqlSafe(sql))
            .bind(id)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(SourceRecord::from))
    }

    /// Insert a source. It starts paused; the caller unpauses once the Temporal schedule
    /// exists, so a crash in between leaves something visible and repairable rather than
    /// a schedule firing against a row that does not exist.
    pub async fn insert(&self, new: &NewSource) -> Result<SourceRecord, CpError> {
        let sql = format!(
            "INSERT INTO sources (id, uid, name, description, project_id, pipeline_uid, \
                                  location, cron, timezone, paused, index_name, fetch_auth, \
                                  schedule_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, true, $10, $11, $12) \
             RETURNING {SOURCE_COLUMNS}"
        );
        let row: SourceRow = sqlx::query_as(AssertSqlSafe(sql))
            .bind(new.id)
            .bind(&new.uid)
            .bind(&new.name)
            .bind(new.description.as_deref())
            .bind(new.project_id.as_deref())
            .bind(&new.pipeline_uid)
            .bind(SqlJson(&new.location))
            .bind(&new.cron)
            .bind(&new.timezone)
            .bind(new.index_name.as_deref())
            .bind(new.fetch_auth.as_deref())
            .bind(&new.schedule_id)
            .fetch_one(&self.pool)
            .await?;
        Ok(SourceRecord::from(row))
    }

    /// Apply a partial update to the row in exactly `project_id`'s scope. `None` when no
    /// such row exists.
    ///
    /// Writes never fall back to the global row the way [`SourceRepo::get`] does: a
    /// tenant must not be able to rename, repoint or delete a global source.
    pub async fn update(
        &self,
        uid: &str,
        project_id: Option<&str>,
        patch: &SourcePatch,
    ) -> Result<Option<SourceRecord>, CpError> {
        // `$10` distinguishes "clear the credential" from "leave it alone": COALESCE
        // alone cannot express the former, because both arrive as SQL NULL.
        let (set_auth, auth_value) = match &patch.fetch_auth {
            None => (false, None),
            Some(v) => (true, v.as_deref()),
        };
        let sql = format!(
            "UPDATE sources SET \
                name = COALESCE($3, name), \
                description = COALESCE($4, description), \
                pipeline_uid = COALESCE($5, pipeline_uid), \
                location = COALESCE($6, location), \
                cron = COALESCE($7, cron), \
                timezone = COALESCE($8, timezone), \
                index_name = COALESCE($9, index_name), \
                fetch_auth = CASE WHEN $10 THEN $11 ELSE fetch_auth END, \
                paused = COALESCE($12, paused), \
                updated_at = now() \
             WHERE uid = $1 AND COALESCE(project_id, '') = COALESCE($2, '') \
             RETURNING {SOURCE_COLUMNS}"
        );
        let row: Option<SourceRow> = sqlx::query_as(AssertSqlSafe(sql))
            .bind(uid)
            .bind(project_id)
            .bind(patch.name.as_deref())
            .bind(patch.description.as_deref())
            .bind(patch.pipeline_uid.as_deref())
            .bind(patch.location.as_ref().map(SqlJson))
            .bind(patch.cron.as_deref())
            .bind(patch.timezone.as_deref())
            .bind(patch.index_name.as_deref())
            .bind(set_auth)
            .bind(auth_value)
            .bind(patch.paused)
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(SourceRecord::from))
    }

    /// Set the paused flag by id.
    pub async fn set_paused(&self, id: Uuid, paused: bool) -> Result<bool, CpError> {
        let done = sqlx::query("UPDATE sources SET paused = $2, updated_at = now() WHERE id = $1")
            .bind(id)
            .bind(paused)
            .execute(&self.pool)
            .await?;
        Ok(done.rows_affected() > 0)
    }

    /// Delete the source in exactly `project_id`'s scope. Its runs cascade.
    ///
    /// Exact scope matters: a looser `project_id IS NULL OR project_id = $2` would make a
    /// tenant's delete remove the global source of the same uid as well.
    pub async fn delete(&self, uid: &str, project_id: Option<&str>) -> Result<bool, CpError> {
        let done = sqlx::query(
            "DELETE FROM sources WHERE uid = $1 AND COALESCE(project_id, '') = COALESCE($2, '')",
        )
        .bind(uid)
        .bind(project_id)
        .execute(&self.pool)
        .await?;
        Ok(done.rows_affected() > 0)
    }

    /// Archive every source feeding `pipeline_uid`, returning their ids so the caller can
    /// delete the corresponding Temporal schedules.
    ///
    /// Archiving rather than deleting: the row holds credentials a tenant supplied by
    /// hand, and destroying them as a side effect of an unrelated pipeline delete is not
    /// recoverable.
    pub async fn archive_for_pipeline(
        &self,
        pipeline_uid: &str,
        project_id: Option<&str>,
    ) -> Result<Vec<Uuid>, CpError> {
        let rows: Vec<(Uuid,)> = sqlx::query_as(
            "UPDATE sources SET archived_at = now(), paused = true, updated_at = now() \
             WHERE pipeline_uid = $1 \
               AND COALESCE(project_id, '') = COALESCE($2, '') \
               AND archived_at IS NULL \
             RETURNING id",
        )
        .bind(pipeline_uid)
        .bind(project_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(id,)| id).collect())
    }

    /// Persist the incremental state learned by a run.
    pub async fn save_state(&self, id: Uuid, state: &IncrementalState) -> Result<(), CpError> {
        sqlx::query(
            "UPDATE sources SET last_etag = $2, last_modified = $3, last_hash = $4, \
                                updated_at = now() \
             WHERE id = $1",
        )
        .bind(id)
        .bind(state.etag.as_deref())
        .bind(state.last_modified.as_deref())
        .bind(state.hash.as_deref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Record one run and mirror its outcome onto the source's denormalized columns.
    pub async fn record_run(&self, run: &RunRecord) -> Result<RunRecord, CpError> {
        let mut tx = self.pool.begin().await?;
        let row: RunRow = sqlx::query_as(
            "INSERT INTO source_runs (run_id, source_id, started_at, finished_at, outcome, \
                                      items, job_ids, error) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
             ON CONFLICT (run_id) DO UPDATE SET \
                finished_at = EXCLUDED.finished_at, outcome = EXCLUDED.outcome, \
                items = EXCLUDED.items, job_ids = EXCLUDED.job_ids, error = EXCLUDED.error \
             RETURNING run_id, source_id, started_at, finished_at, outcome, items, job_ids, error",
        )
        .bind(run.run_id)
        .bind(run.source_id)
        .bind(run.started_at)
        .bind(run.finished_at)
        .bind(run.outcome.as_str())
        .bind(run.items)
        .bind(&run.job_ids)
        .bind(run.error.as_deref())
        .fetch_one(&mut *tx)
        .await?;

        sqlx::query(
            "UPDATE sources SET last_run_at = $2, last_status = $3, last_error = $4, \
                                updated_at = now() \
             WHERE id = $1",
        )
        .bind(run.source_id)
        .bind(run.started_at)
        .bind(run.outcome.as_str())
        .bind(run.error.as_deref())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        RunRecord::try_from(row)
    }

    /// Most recent runs of one source, newest first.
    pub async fn list_runs(&self, source_id: Uuid, limit: i64) -> Result<Vec<RunRecord>, CpError> {
        let rows: Vec<RunRow> = sqlx::query_as(
            "SELECT run_id, source_id, started_at, finished_at, outcome, items, job_ids, error \
             FROM source_runs WHERE source_id = $1 \
             ORDER BY started_at DESC, run_id DESC \
             LIMIT $2",
        )
        .bind(source_id)
        .bind(limit.clamp(1, 500))
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(RunRecord::try_from).collect()
    }
}
