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

/// Migration 0005: rewrites undelivered pre-v2 `usage.recorded` rows to the v2 shape.
/// Also run after every insert, so a released migration file must never change.
const CONVERT_PRE_V2: &str = include_str!("../../../migrations/0005_lab_events_v2.sql");

/// Whether `events` may hold a pre-v2 `usage.recorded` event: one with a `data` object
/// and no `data.operation`. A superset of what [`CONVERT_PRE_V2`] matches, so skipping
/// the conversion when this is false never leaves a pre-v2 row unconverted, and v2
/// batches never pay for a scan of the outbox.
pub fn needs_pre_v2_conversion(events: &[serde_json::Value]) -> bool {
    events.iter().any(|e| {
        e.get("type").and_then(|t| t.as_str()) == Some("usage.recorded")
            && e.get("data")
                .and_then(|d| d.as_object())
                .is_some_and(|d| !d.contains_key("operation"))
    })
}

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

    /// Insert events, ignoring ids already present, and convert pre-v2 usage events to
    /// the v2 shape. Every event needs a UUID `id`; otherwise nothing is inserted and
    /// the call fails.
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
        if inserted > 0 && needs_pre_v2_conversion(events) {
            // Workers not upgraded yet still post pre-v2 `usage.recorded` events, which a
            // v2 Lab accepts in the batch and then rejects: convert them like the
            // upgrade did (the migration is idempotent and only touches pre-v2 rows).
            // It scans the undelivered outbox, so v2 batches skip it.
            sqlx::raw_sql(CONVERT_PRE_V2).execute(&mut *tx).await?;
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

    /// Delete undelivered rows older than `older_than` and return their ids. Delivered
    /// rows are never touched here (see `purge_delivered`).
    pub async fn drop_stale(&self, older_than: Duration) -> Result<Vec<Uuid>, CpError> {
        Ok(sqlx::query_scalar(
            "DELETE FROM lab_events \
             WHERE delivered_at IS NULL AND created_at < now() - make_interval(secs => $1) \
             RETURNING id",
        )
        .bind(older_than.as_secs_f64())
        .fetch_all(&self.pool)
        .await?)
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::needs_pre_v2_conversion;

    #[test]
    fn only_a_usage_event_without_an_operation_needs_the_conversion() {
        let v2_usage =
            json!({"type": "usage.recorded", "data": {"operation": "ingest", "units": {}}});
        let lifecycle = json!({"type": "job.completed", "data": {"job_id": "j"}});
        let pre_v2 = json!({"type": "usage.recorded", "data": {"cost_complete": true}});
        assert!(!needs_pre_v2_conversion(&[]));
        assert!(!needs_pre_v2_conversion(&[
            v2_usage.clone(),
            lifecycle.clone()
        ]));
        assert!(needs_pre_v2_conversion(&[
            v2_usage,
            pre_v2.clone(),
            lifecycle
        ]));
        // Anything the SQL cannot convert does not trigger it either.
        assert!(!needs_pre_v2_conversion(&[
            json!({"type": "usage.recorded", "data": 1})
        ]));
        assert!(!needs_pre_v2_conversion(&[
            json!({"type": "usage.recorded"})
        ]));
        assert!(!needs_pre_v2_conversion(&[
            json!({"data": {"cost_complete": true}})
        ]));
        // A usage event without operation but also without cost_complete still counts:
        // the guard is a superset of the SQL predicate.
        assert!(needs_pre_v2_conversion(&[
            json!({"type": "usage.recorded", "data": {}})
        ]));
    }
}
