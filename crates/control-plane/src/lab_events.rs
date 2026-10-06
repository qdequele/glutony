//! Outbox of Meilisearch Lab billing events (spec §5.3, §5.4).
//!
//! The worker posts each finished Lab job's event to `POST /internal/lab-events`;
//! the insert ignores duplicates, so a Temporal retry cannot bill twice. The sender
//! (`crate::lab_sender`) leases due rows with `claim_due`, so several control-plane
//! replicas never deliver the same row, and no transaction is held across HTTP.

use std::time::Duration;

use axum::Json;
use axum::extract::State;
use axum::http::StatusCode;
use serde::Deserialize;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{AppState, CpError, JsonBody};

/// A leased row, ready to send.
#[derive(Debug, Clone, PartialEq, sqlx::FromRow)]
pub struct PendingEvent {
    /// Event id.
    pub id: Uuid,
    /// The event as the worker built it.
    pub body: serde_json::Value,
    /// Failed deliveries so far.
    pub attempts: i32,
}

/// Outbox health, for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct LabEventStats {
    /// Rows not delivered yet.
    pub pending: i64,
    /// Age of the oldest undelivered row, 0 when none.
    pub oldest_pending_seconds: i64,
}

/// Queries over `lab_events`.
#[derive(Debug, Clone)]
pub struct LabEventRepo {
    pool: PgPool,
}

impl LabEventRepo {
    /// Wrap a pool.
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Insert events, ignoring ids already present. Every event needs a UUID `id`;
    /// otherwise nothing is inserted and the call fails.
    pub async fn insert_many(&self, events: &[serde_json::Value]) -> Result<u64, CpError> {
        let mut ids = Vec::with_capacity(events.len());
        for e in events {
            let id = e
                .get("id")
                .and_then(|v| v.as_str())
                .and_then(|s| Uuid::parse_str(s).ok())
                .ok_or_else(|| CpError::Validation("every lab event needs a UUID \"id\"".into()))?;
            ids.push(id);
        }
        let mut tx = self.pool.begin().await?;
        let mut inserted = 0;
        for (id, body) in ids.iter().zip(events) {
            inserted += sqlx::query(
                "INSERT INTO lab_events (id, body) VALUES ($1, $2) ON CONFLICT (id) DO NOTHING",
            )
            .bind(id)
            .bind(sqlx::types::Json(body))
            .execute(&mut *tx)
            .await?
            .rows_affected();
        }
        tx.commit().await?;
        Ok(inserted)
    }

    /// Lease up to `limit` due rows for `lease`: they stay undelivered but are not due
    /// again until the lease ends, so a sender that dies mid-batch loses nothing.
    pub async fn claim_due(
        &self,
        limit: i64,
        lease: Duration,
    ) -> Result<Vec<PendingEvent>, CpError> {
        Ok(sqlx::query_as(
            "UPDATE lab_events SET next_attempt = now() + make_interval(secs => $2) \
             WHERE id IN ( \
                 SELECT id FROM lab_events \
                 WHERE delivered_at IS NULL AND next_attempt <= now() \
                 ORDER BY created_at \
                 LIMIT $1 \
                 FOR UPDATE SKIP LOCKED) \
             RETURNING id, body, attempts",
        )
        .bind(limit)
        .bind(lease.as_secs_f64())
        .fetch_all(&self.pool)
        .await?)
    }

    /// Mark rows delivered.
    pub async fn mark_delivered(&self, ids: &[Uuid]) -> Result<u64, CpError> {
        Ok(sqlx::query(
            "UPDATE lab_events SET delivered_at = now() WHERE id = ANY($1) AND delivered_at IS NULL",
        )
        .bind(ids)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Count a failed delivery and back off: `min(2^attempts s, 300 s)` ± 20 %. The
    /// exponent is clamped at 9 (2^9 > 300) because Postgres' `power` errors on float
    /// overflow, which would otherwise make a long-failing row unschedulable.
    pub async fn reschedule(&self, ids: &[Uuid]) -> Result<u64, CpError> {
        Ok(sqlx::query(
            "UPDATE lab_events SET attempts = attempts + 1, \
                 next_attempt = now() + make_interval(secs => \
                     LEAST(power(2, LEAST(attempts + 1, 9)), 300) * (0.8 + random() * 0.4)) \
             WHERE id = ANY($1) AND delivered_at IS NULL",
        )
        .bind(ids)
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Delete delivered rows older than `older_than`. Never touches undelivered rows.
    pub async fn purge_delivered(&self, older_than: Duration) -> Result<u64, CpError> {
        Ok(sqlx::query(
            "DELETE FROM lab_events \
             WHERE delivered_at IS NOT NULL AND delivered_at < now() - make_interval(secs => $1)",
        )
        .bind(older_than.as_secs_f64())
        .execute(&self.pool)
        .await?
        .rows_affected())
    }

    /// Pending count and oldest pending age.
    pub async fn stats(&self) -> Result<LabEventStats, CpError> {
        let (pending, oldest): (i64, Option<f64>) = sqlx::query_as(
            "SELECT count(*), EXTRACT(EPOCH FROM now() - min(created_at))::float8 \
             FROM lab_events WHERE delivered_at IS NULL",
        )
        .fetch_one(&self.pool)
        .await?;
        Ok(LabEventStats {
            pending,
            oldest_pending_seconds: oldest.map(|s| s as i64).unwrap_or(0),
        })
    }
}

/// Body of `POST /internal/lab-events`.
#[derive(Debug, Deserialize)]
pub struct LabEventBatch {
    /// Events as the worker built them.
    pub events: Vec<serde_json::Value>,
}

/// `POST /internal/lab-events` → `202 {"inserted": n}`. Accepts whether or not
/// `LAB_URL` is configured: rows wait until it is (spec §5.3).
pub async fn ingest_lab_events(
    State(state): State<AppState>,
    JsonBody(batch): JsonBody<LabEventBatch>,
) -> Result<(StatusCode, Json<serde_json::Value>), CpError> {
    let inserted = LabEventRepo::new(state.pool.clone())
        .insert_many(&batch.events)
        .await?;
    if inserted > 0 {
        state.lab_notify.notify_one();
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(serde_json::json!({ "inserted": inserted })),
    ))
}
