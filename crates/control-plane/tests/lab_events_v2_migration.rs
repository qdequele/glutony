//! Migration 0005: pre-v2 `usage.recorded` rows still in the outbox are rewritten to
//! the v2 shape. Needs DATABASE_URL (skips otherwise), e.g.
//! `DATABASE_URL=postgres://postgres:dev@localhost:55433/postgres cargo test -p meili-ingest-control-plane --test lab_events_v2_migration`.

use std::str::FromStr;

use chrono::{DateTime, Utc};
use meili_ingest_control_plane::db;
use serde_json::{Value, json};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;

const MIGRATION: &str = include_str!("../../../migrations/0005_lab_events_v2.sql");
const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";

struct TestDb {
    pool: PgPool,
    schema: String,
    admin: PgPool,
}

impl TestDb {
    async fn drop_schema(self) {
        self.pool.close().await;
        let _ = sqlx::query(AssertSqlSafe(format!(
            "DROP SCHEMA IF EXISTS {} CASCADE",
            self.schema
        )))
        .execute(&self.admin)
        .await;
        self.admin.close().await;
    }
}

async fn setup() -> Option<TestDb> {
    let Ok(url) = std::env::var("DATABASE_URL") else {
        eprintln!("skipping: DATABASE_URL not set");
        return None;
    };
    let schema = format!("cp_v2_{}", Uuid::new_v4().simple());
    let admin = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .unwrap();
    sqlx::query(AssertSqlSafe(format!("CREATE SCHEMA {schema}")))
        .execute(&admin)
        .await
        .unwrap();
    let opts = PgConnectOptions::from_str(&url)
        .unwrap()
        .options([("search_path", schema.as_str())]);
    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect_with(opts)
        .await
        .unwrap();
    db::migrate(&pool).await.unwrap();
    Some(TestDb {
        pool,
        schema,
        admin,
    })
}

/// A `usage.recorded` event as the worker wrote it before the v2 contract.
fn old_event(id: Uuid, job: Uuid, data: Value) -> Value {
    let mut data = data;
    data["job_id"] = json!(job);
    json!({
        "id": id,
        "type": "usage.recorded",
        "occurred_at": "2026-10-01T10:00:09.000Z",
        "account_id": ACCOUNT,
        "api_key_id": null,
        "product": "glutony",
        "data": data,
    })
}

/// Insert a row as the old worker would have left it: already attempted, backed off,
/// and written 30 h ago.
async fn insert(pool: &PgPool, body: &Value, delivered: bool) -> Uuid {
    let id: Uuid = body["id"].as_str().unwrap().parse().unwrap();
    sqlx::query(
        "INSERT INTO lab_events (id, body, created_at, attempts, next_attempt, delivered_at) \
         VALUES ($1, $2, now() - interval '30 hours', 7, now() + interval '5 minutes', \
                 CASE WHEN $3 THEN now() - interval '29 hours' END)",
    )
    .bind(id)
    .bind(body)
    .bind(delivered)
    .execute(pool)
    .await
    .unwrap();
    id
}

type Row = (
    Uuid,
    Value,
    i32,
    DateTime<Utc>,
    DateTime<Utc>,
    Option<DateTime<Utc>>,
);

async fn rows(pool: &PgPool) -> Vec<Row> {
    sqlx::query_as(
        "SELECT id, body, attempts, next_attempt, created_at, delivered_at \
         FROM lab_events ORDER BY id",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}

async fn row(pool: &PgPool, id: Uuid) -> Row {
    rows(pool)
        .await
        .into_iter()
        .find(|r| r.0 == id)
        .expect("row exists")
}

fn validator() -> jsonschema::Validator {
    let schema: Value = serde_json::from_str(include_str!(
        "../../../contracts/vendor/lab/lab-events.schema.json"
    ))
    .unwrap();
    jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)
        .unwrap()
}

fn assert_valid(event: &Value) {
    let errors: Vec<String> = validator()
        .iter_errors(event)
        .map(|e| e.to_string())
        .collect();
    assert!(errors.is_empty(), "{errors:?} in {event}");
}

#[test]
fn the_migration_is_embedded() {
    assert!(
        db::migrator()
            .iter()
            .any(|m| m.version == 5 && m.sql.as_str().contains("lab_events")),
        "migrations/0005_lab_events_v2.sql is not in the embedded migrator"
    );
}

#[tokio::test]
async fn pending_pre_v2_usage_events_are_rewritten_to_v2() {
    let Some(t) = setup().await else { return };
    let (incomplete_job, complete_job) = (Uuid::new_v4(), Uuid::new_v4());
    let incomplete = insert(
        &t.pool,
        &old_event(
            Uuid::new_v4(),
            incomplete_job,
            json!({
                "pipeline_uid": "builtin.pdf",
                "status": "succeeded",
                "duration_ms": 9001,
                "cost_micro_usd": 210,
                "cost_complete": false,
                "units": {
                    "documents_out": 12, "input_bytes": 5000, "pages": 9, "images": 2,
                    "audio_seconds": 12.25, "llm_input_tokens": 1000,
                    "llm_output_tokens": 100, "llm_requests": 1, "external_requests": 3
                }
            }),
        ),
        false,
    )
    .await;
    let complete = insert(
        &t.pool,
        &old_event(
            Uuid::new_v4(),
            complete_job,
            json!({
                "pipeline_uid": "my-json",
                "status": "failed",
                "duration_ms": 0,
                "cost_micro_usd": 0,
                "cost_complete": true,
                "units": {
                    "documents_out": 0, "input_bytes": 17, "pages": 0, "images": 0,
                    "audio_seconds": 0.0, "llm_input_tokens": 0,
                    "llm_output_tokens": 0, "llm_requests": 0, "external_requests": 0
                }
            }),
        ),
        false,
    )
    .await;
    let delivered_body = old_event(
        Uuid::new_v4(),
        Uuid::new_v4(),
        json!({
            "pipeline_uid": "builtin.pdf",
            "status": "succeeded",
            "duration_ms": 1000,
            "cost_micro_usd": 5,
            "cost_complete": true,
            "units": {
                "documents_out": 1, "input_bytes": 1, "pages": 0, "images": 0,
                "audio_seconds": 0.0, "llm_input_tokens": 0,
                "llm_output_tokens": 0, "llm_requests": 0, "external_requests": 0
            }
        }),
    );
    let delivered = insert(&t.pool, &delivered_body, true).await;
    let delivered_before = row(&t.pool, delivered).await;
    // A v2 lifecycle event already in the outbox is not a pre-v2 row.
    let lifecycle_body = json!({
        "id": Uuid::new_v4(), "type": "job.completed",
        "occurred_at": "2026-10-01T10:00:09.000Z", "account_id": ACCOUNT,
        "api_key_id": null, "product": "glutony",
        "data": {"job_id": "j", "index_uid": "p", "pages_crawled": 0,
                 "documents_indexed": 1, "duration_secs": 1}
    });
    let lifecycle = insert(&t.pool, &lifecycle_body, false).await;
    let lifecycle_before = row(&t.pool, lifecycle).await;

    sqlx::raw_sql(AssertSqlSafe(MIGRATION))
        .execute(&t.pool)
        .await
        .unwrap();

    let (_, body, attempts, next_attempt, created_at, delivered_at) =
        row(&t.pool, incomplete).await;
    assert_eq!(
        body,
        json!({
            "id": incomplete,
            "type": "usage.recorded",
            "occurred_at": "2026-10-01T10:00:09.000Z",
            "account_id": ACCOUNT,
            "api_key_id": null,
            "product": "glutony",
            "data": {
                "operation": "ingest",
                "units": {
                    "documents": 12, "bytes_in": 5000, "step_seconds": 10,
                    "llm_tokens_in": 1000, "llm_tokens_out": 100, "audio_seconds": 13,
                    "ocr_pages": 0, "pages": 9, "images": 2, "llm_requests": 1,
                    "external_requests": 3, "unpriced_provider_calls": 1
                },
                "provider_cost_micro_usd": 210,
                "description": format!(
                    "Job {incomplete_job} (builtin.pdf, succeeded, 12 documents, provider cost incomplete)"
                ),
                "job_id": incomplete_job.to_string(),
            }
        })
    );
    assert_valid(&body);
    // A fresh 24 h delivery window, due now.
    let now = Utc::now();
    assert_eq!(attempts, 0);
    assert!(next_attempt <= now, "{next_attempt} is not due");
    assert!(
        (now - created_at).num_seconds() < 60,
        "created_at {created_at} was not reset"
    );
    assert_eq!(delivered_at, None);

    let (_, body, attempts, ..) = row(&t.pool, complete).await;
    assert_eq!(
        body["data"],
        json!({
            "operation": "ingest",
            "units": {
                "documents": 0, "bytes_in": 17, "step_seconds": 0,
                "llm_tokens_in": 0, "llm_tokens_out": 0, "audio_seconds": 0,
                "ocr_pages": 0, "pages": 0, "images": 0, "llm_requests": 0,
                "external_requests": 0, "unpriced_provider_calls": 0
            },
            "provider_cost_micro_usd": 0,
            "description": format!("Job {complete_job} (my-json, failed, 0 documents)"),
            "job_id": complete_job.to_string(),
        })
    );
    assert_valid(&body);
    assert_eq!(attempts, 0);

    assert_eq!(
        row(&t.pool, delivered).await,
        delivered_before,
        "delivered rows are left alone"
    );
    assert_eq!(
        row(&t.pool, lifecycle).await,
        lifecycle_before,
        "v2 rows are left alone"
    );

    // Idempotent: a second run changes nothing.
    let after_first = rows(&t.pool).await;
    sqlx::raw_sql(AssertSqlSafe(MIGRATION))
        .execute(&t.pool)
        .await
        .unwrap();
    assert_eq!(rows(&t.pool).await, after_first);
    t.drop_schema().await;
}
