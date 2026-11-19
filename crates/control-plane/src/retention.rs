//! Opt-in retention of job and source-run history (`JOBS_RETENTION_DAYS`).
//!
//! Every ingested file is a `jobs` row and every source tick a `source_runs` row, and
//! nothing else ever deletes them. With a retention set, an hourly pass deletes the
//! finished jobs (`succeeded`, `failed`, `cancelled`) and the source runs that started
//! more than that many days ago. Queued and running jobs are never touched, and an
//! archive job stays while any of its file jobs is still there, so
//! `GET /jobs?parent_job_id=` never lists the files of a job that is gone.
//!
//! Deletes run in small batches of `SKIP LOCKED` rows, so a pass never holds long
//! locks or a pooled connection, and several control-plane replicas can run it at
//! once without stepping on each other.

use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;

use crate::CpError;

/// Rows deleted per statement.
pub const BATCH: i64 = 1_000;
/// Statements per table per pass, so one pass is bounded; the rest waits an hour.
pub const MAX_BATCHES: usize = 100;
/// Time between passes.
pub const EVERY: Duration = Duration::from_secs(3_600);

/// What one pass deleted.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Pruned {
    /// Job rows.
    pub jobs: u64,
    /// Source-run rows.
    pub source_runs: u64,
}

/// The retention task.
#[derive(Debug, Clone)]
pub struct Retention {
    pool: PgPool,
    keep: Duration,
}

impl Retention {
    /// Keep `days` days of finished jobs and source runs.
    pub fn new(pool: PgPool, days: u32) -> Self {
        Self {
            pool,
            keep: Duration::from_secs(u64::from(days) * 86_400),
        }
    }

    /// One pass: delete up to [`MAX_BATCHES`] batches from each table.
    pub async fn prune_once(&self) -> Result<Pruned, CpError> {
        let secs = self.keep.as_secs_f64();
        let mut pruned = Pruned::default();
        for _ in 0..MAX_BATCHES {
            // A file job points at its archive job through `parent_job_id`; the archive
            // job goes only once none of its file jobs is left.
            let n = sqlx::query(
                "DELETE FROM jobs WHERE job_id IN ( \
                     SELECT j.job_id FROM jobs j \
                     WHERE j.status IN ('succeeded', 'failed', 'cancelled') \
                       AND j.started_at < now() - make_interval(secs => $1) \
                       AND NOT EXISTS (SELECT 1 FROM jobs c WHERE c.parent_job_id = j.job_id) \
                     ORDER BY j.started_at \
                     LIMIT $2 \
                     FOR UPDATE SKIP LOCKED)",
            )
            .bind(secs)
            .bind(BATCH)
            .execute(&self.pool)
            .await?
            .rows_affected();
            pruned.jobs += n;
            if n < BATCH as u64 {
                break;
            }
        }
        for _ in 0..MAX_BATCHES {
            let n = sqlx::query(
                "DELETE FROM source_runs WHERE run_id IN ( \
                     SELECT run_id FROM source_runs \
                     WHERE started_at < now() - make_interval(secs => $1) \
                     ORDER BY started_at \
                     LIMIT $2 \
                     FOR UPDATE SKIP LOCKED)",
            )
            .bind(secs)
            .bind(BATCH)
            .execute(&self.pool)
            .await?
            .rows_affected();
            pruned.source_runs += n;
            if n < BATCH as u64 {
                break;
            }
        }
        Ok(pruned)
    }

    /// Run a pass now, then every [`EVERY`], until `cancel`.
    pub async fn run(self, cancel: CancellationToken) {
        loop {
            match self.prune_once().await {
                Ok(p) if p != Pruned::default() => tracing::info!(
                    jobs = p.jobs,
                    source_runs = p.source_runs,
                    "old job and source-run history pruned"
                ),
                Ok(_) => {}
                Err(e) => tracing::warn!(error = %e, "cannot prune old job history"),
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = tokio::time::sleep(EVERY) => {}
            }
        }
        tracing::info!("job retention stopped");
    }
}
