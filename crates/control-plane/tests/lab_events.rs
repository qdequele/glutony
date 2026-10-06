//! Lab events outbox. Needs DATABASE_URL (skips otherwise), e.g.
//! `DATABASE_URL=postgres://postgres:dev@localhost:55433/postgres cargo test -p meili-ingest-control-plane --test lab_events`.

use std::str::FromStr;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use meili_ingest_control_plane::lab_events::LabEventRepo;
use meili_ingest_control_plane::{AppState, app, db};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use tower::ServiceExt;
use uuid::Uuid;

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
    let schema = format!("cp_lab_{}", Uuid::new_v4().simple());
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

fn event(id: Uuid) -> serde_json::Value {
    serde_json::json!({"id": id, "type": "usage.recorded", "product": "glutony"})
}

#[tokio::test]
async fn inserting_twice_keeps_one_row() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let id = Uuid::new_v4();
    assert_eq!(repo.insert_many(&[event(id)]).await.unwrap(), 1);
    assert_eq!(repo.insert_many(&[event(id), event(id)]).await.unwrap(), 0);
    assert_eq!(repo.stats().await.unwrap().pending, 1);
    t.drop_schema().await;
}

#[tokio::test]
async fn an_event_without_a_uuid_id_is_rejected() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    assert!(
        repo.insert_many(&[serde_json::json!({"id": "nope"})])
            .await
            .is_err()
    );
    assert!(repo.insert_many(&[serde_json::json!({})]).await.is_err());
    assert_eq!(repo.stats().await.unwrap().pending, 0);
    t.drop_schema().await;
}

#[tokio::test]
async fn a_claim_leases_rows_so_two_senders_never_share_one() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
    let events: Vec<_> = ids.iter().copied().map(event).collect();
    repo.insert_many(&events).await.unwrap();

    let lease = Duration::from_secs(30);
    let (a, b) = tokio::join!(repo.claim_due(6, lease), repo.claim_due(6, lease));
    let (a, b) = (a.unwrap(), b.unwrap());
    let mut seen: Vec<Uuid> = a.iter().chain(b.iter()).map(|e| e.id).collect();
    seen.sort();
    seen.dedup();
    assert_eq!(seen.len(), a.len() + b.len(), "a row was claimed twice");
    assert_eq!(a.len() + b.len(), 10);
    assert!(
        repo.claim_due(10, lease).await.unwrap().is_empty(),
        "leased rows are not due"
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn delivered_rows_are_done_and_failed_rows_back_off() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let (ok, ko) = (Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(ok), event(ko)]).await.unwrap();
    let claimed = repo.claim_due(10, Duration::ZERO).await.unwrap();
    assert_eq!(claimed.len(), 2);
    assert_eq!(claimed[0].body["product"], "glutony");

    assert_eq!(repo.mark_delivered(&[ok]).await.unwrap(), 1);
    assert_eq!(repo.reschedule(&[ko]).await.unwrap(), 1);
    let (attempts, wait): (i32, f64) = sqlx::query_as(
        "SELECT attempts, EXTRACT(EPOCH FROM next_attempt - now())::float8 FROM lab_events WHERE id = $1",
    )
    .bind(ko)
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert_eq!(attempts, 1);
    assert!(
        (1.5..=2.5).contains(&wait),
        "first backoff is 2 s ± 20 %, got {wait}"
    );
    assert!(repo.claim_due(10, Duration::ZERO).await.unwrap().is_empty());

    let stats = repo.stats().await.unwrap();
    assert_eq!(
        stats.pending, 1,
        "delivered rows are not pending; failed ones are"
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn backoff_is_capped_at_five_minutes() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let id = Uuid::new_v4();
    repo.insert_many(&[event(id)]).await.unwrap();
    sqlx::query("UPDATE lab_events SET attempts = 30 WHERE id = $1")
        .bind(id)
        .execute(&t.pool)
        .await
        .unwrap();
    repo.reschedule(&[id]).await.unwrap();
    let wait: f64 = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM next_attempt - now())::float8 FROM lab_events WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert!((240.0..=360.0).contains(&wait), "{wait}");
    t.drop_schema().await;
}

#[tokio::test]
async fn backoff_does_not_overflow_after_thousands_of_failures() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let id = Uuid::new_v4();
    repo.insert_many(&[event(id)]).await.unwrap();
    sqlx::query("UPDATE lab_events SET attempts = 5000 WHERE id = $1")
        .bind(id)
        .execute(&t.pool)
        .await
        .unwrap();
    assert_eq!(repo.reschedule(&[id]).await.unwrap(), 1);
    let wait: f64 = sqlx::query_scalar(
        "SELECT EXTRACT(EPOCH FROM next_attempt - now())::float8 FROM lab_events WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert!((240.0..=360.0).contains(&wait), "{wait}");
    t.drop_schema().await;
}

#[tokio::test]
async fn purge_only_touches_old_delivered_rows() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let (old, recent, pending) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(old), event(recent), event(pending)])
        .await
        .unwrap();
    repo.mark_delivered(&[old, recent]).await.unwrap();
    sqlx::query("UPDATE lab_events SET delivered_at = now() - interval '8 days', created_at = now() - interval '9 days' WHERE id = $1")
        .bind(old)
        .execute(&t.pool)
        .await
        .unwrap();
    // A very old undelivered row must survive any purge.
    sqlx::query("UPDATE lab_events SET created_at = now() - interval '30 days' WHERE id = $1")
        .bind(pending)
        .execute(&t.pool)
        .await
        .unwrap();
    assert_eq!(
        repo.purge_delivered(Duration::from_secs(7 * 86_400))
            .await
            .unwrap(),
        1
    );
    let left: Vec<Uuid> = sqlx::query_scalar("SELECT id FROM lab_events ORDER BY created_at")
        .fetch_all(&t.pool)
        .await
        .unwrap();
    assert_eq!(left.len(), 2);
    assert!(left.contains(&pending) && left.contains(&recent));
    let stats = repo.stats().await.unwrap();
    assert!(stats.oldest_pending_seconds >= 30 * 86_400 - 5);
    t.drop_schema().await;
}

#[tokio::test]
async fn the_internal_route_inserts_and_is_idempotent() {
    let Some(t) = setup().await else { return };
    let app = app(AppState::new(t.pool.clone()));
    let id = Uuid::new_v4();
    let body = serde_json::json!({"events": [event(id)]});
    for expected in [1, 0] {
        let req = Request::post("/internal/lab-events")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&body).unwrap()))
            .unwrap();
        let res = app.clone().oneshot(req).await.unwrap();
        assert_eq!(res.status(), StatusCode::ACCEPTED);
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let v: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(v["inserted"], expected);
    }
    let bad = Request::post("/internal/lab-events")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"events":[{"id":"x"}]}"#))
        .unwrap();
    assert_eq!(
        app.oneshot(bad).await.unwrap().status(),
        StatusCode::UNPROCESSABLE_ENTITY
    );
    t.drop_schema().await;
}
