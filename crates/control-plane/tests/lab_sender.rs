//! Lab events sender. Needs DATABASE_URL (skips otherwise), e.g.
//! `DATABASE_URL=postgres://postgres:dev@localhost:55433/postgres cargo test -p meili-ingest-control-plane --test lab_sender`.

use std::str::FromStr;

use meili_ingest_control_plane::db;
use meili_ingest_control_plane::lab_events::LabEventRepo;
use meili_ingest_control_plane::lab_sender::{Delivery, LabConfig, LabSender};
use meili_ingest_control_plane::metrics::{LabMetrics, render};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request as WmRequest, ResponseTemplate};

const SECRET: &str = "lab-instance-secret";
const INSTANCE_ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

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
    let schema = format!("cp_send_{}", Uuid::new_v4().simple());
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

async fn sender(t: &TestDb, lab: &MockServer) -> (LabSender, LabEventRepo, LabMetrics) {
    let repo = LabEventRepo::new(t.pool.clone());
    let config = LabConfig::from_values(
        Some(lab.uri()),
        Some(INSTANCE_ID.into()),
        Some(SECRET.into()),
        None,
    )
    .unwrap()
    .unwrap();
    let metrics = LabMetrics::default();
    let s = LabSender::new(repo.clone(), config, metrics.clone(), Default::default()).unwrap();
    (s, repo, metrics)
}

fn event(id: Uuid) -> serde_json::Value {
    serde_json::json!({"id": id, "type": "usage.recorded", "product": "glutony"})
}

/// Answers 200 accepting every id it received.
fn accept_all(req: &WmRequest) -> ResponseTemplate {
    let v: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    let ids: Vec<_> = v["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].clone())
        .collect();
    ResponseTemplate::new(200).set_body_json(serde_json::json!({"accepted": ids}))
}

#[tokio::test]
async fn a_batch_is_signed_and_delivered() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/events"))
        .and(header("content-type", "application/json"))
        .respond_with(accept_all)
        .expect(1)
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    let id = Uuid::new_v4();
    repo.insert_many(&[event(id)]).await.unwrap();

    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 1,
            pending: 0
        }
    );
    let req = &lab.received_requests().await.unwrap()[0];
    let hdr = |n: &str| req.headers.get(n).unwrap().to_str().unwrap().to_string();
    assert_eq!(hdr("x-lab-instance-id"), INSTANCE_ID);
    let ts: u64 = hdr("x-lab-timestamp").parse().unwrap();
    let now = meili_ingest_lab::unix_now();
    assert!(
        now - 5 <= ts && ts <= now,
        "timestamp {ts} is not now ({now})"
    );
    assert_eq!(
        hdr("x-lab-signature"),
        meili_ingest_lab::sign_batch(SECRET.as_bytes(), ts, &req.body)
    );
    assert!(
        req.headers.get("authorization").is_none(),
        "event batches are signed, not bearer-authenticated"
    );
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    assert_eq!(body["events"][0]["id"], id.to_string());
    assert_eq!(repo.stats().await.unwrap().pending, 0);
    assert!(render(&metrics).contains("glutony_lab_events_delivered_total 1"));
    assert_eq!(s.deliver_once().await, Delivery::Idle);
    t.drop_schema().await;
}

#[tokio::test]
async fn partial_accept_leaves_the_rest_pending() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let (keep, skip) = (Uuid::new_v4(), Uuid::new_v4());
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"accepted": [keep]})),
        )
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(keep), event(skip)]).await.unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 1,
            pending: 1
        }
    );
    assert_eq!(repo.stats().await.unwrap().pending, 1);
    assert!(
        render(&metrics).contains("glutony_lab_events_failed_total{reason=\"not_accepted\"} 1")
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn a_failed_batch_counts_every_event_it_held() {
    // `failed_total` counts events for every reason, like `not_accepted` and the
    // sibling `delivered_total` / `dropped_total`: a refused batch of two is two.
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(401))
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4()), event(Uuid::new_v4())])
        .await
        .unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Failed {
            reason: "auth",
            pending: 2
        }
    );
    let text = render(&metrics);
    assert!(
        text.contains("glutony_lab_events_failed_total{reason=\"auth\"} 2"),
        "{text}"
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn events_that_could_not_be_marked_delivered_are_not_counted() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(accept_all)
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();

    // Only `mark_delivered` writes `delivered_at`: make exactly that statement fail,
    // as a pool timeout or a Postgres restart would.
    sqlx::raw_sql(
        "CREATE FUNCTION refuse_delivery() RETURNS trigger LANGUAGE plpgsql AS \
         $$ BEGIN RAISE EXCEPTION 'delivered_at is not writable'; END $$; \
         CREATE TRIGGER refuse_delivery BEFORE UPDATE OF delivered_at ON lab_events \
         FOR EACH ROW EXECUTE FUNCTION refuse_delivery();",
    )
    .execute(&t.pool)
    .await
    .unwrap();

    // The Lab accepted it, so the batch result still says so (the drain loop relies
    // on it), but the row is still pending and will be sent again: counting it now
    // would count it twice.
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 1,
            pending: 0
        }
    );
    assert_eq!(repo.stats().await.unwrap().pending, 1);
    let text = render(&metrics);
    assert!(
        text.contains("glutony_lab_events_delivered_total 0"),
        "{text}"
    );

    // Once the write works, the second delivery is the one that counts.
    sqlx::raw_sql("DROP TRIGGER refuse_delivery ON lab_events")
        .execute(&t.pool)
        .await
        .unwrap();
    sqlx::query("UPDATE lab_events SET next_attempt = now()")
        .execute(&t.pool)
        .await
        .unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 1,
            pending: 0
        }
    );
    assert_eq!(repo.stats().await.unwrap().pending, 0);
    assert!(render(&metrics).contains("glutony_lab_events_delivered_total 1"));
    t.drop_schema().await;
}

#[tokio::test]
async fn failures_back_off_and_never_drop() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();

    for (status, reason) in [(503, "status"), (401, "auth")] {
        lab.reset().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(status))
            .mount(&lab)
            .await;
        // Make the row due again despite the backoff.
        sqlx::query("UPDATE lab_events SET next_attempt = now()")
            .execute(&t.pool)
            .await
            .unwrap();
        assert_eq!(
            s.deliver_once().await,
            Delivery::Failed { reason, pending: 1 }
        );
    }
    lab.reset().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
        .mount(&lab)
        .await;
    sqlx::query("UPDATE lab_events SET next_attempt = now()")
        .execute(&t.pool)
        .await
        .unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Failed {
            reason: "malformed",
            pending: 1
        }
    );

    let attempts: i32 = sqlx::query_scalar("SELECT attempts FROM lab_events")
        .fetch_one(&t.pool)
        .await
        .unwrap();
    assert_eq!(attempts, 3);
    assert_eq!(
        repo.stats().await.unwrap().pending,
        1,
        "nothing is ever dropped"
    );
    let text = render(&metrics);
    assert!(text.contains("reason=\"auth\"} 1") && text.contains("reason=\"status\"} 1"));
    t.drop_schema().await;
}

#[tokio::test]
async fn redirect_is_not_followed() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let elsewhere = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(307)
                .insert_header("location", format!("{}/internal/events", elsewhere.uri())),
        )
        .mount(&lab)
        .await;
    Mock::given(method("POST"))
        .respond_with(accept_all)
        .expect(0)
        .mount(&elsewhere)
        .await;
    let (s, repo, _) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Failed {
            reason: "status",
            pending: 1
        }
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn an_unreachable_lab_is_a_connect_failure() {
    let Some(t) = setup().await else { return };
    let repo = LabEventRepo::new(t.pool.clone());
    let config = LabConfig::from_values(
        Some("http://127.0.0.1:1".into()),
        Some(INSTANCE_ID.into()),
        Some(SECRET.into()),
        None,
    )
    .unwrap()
    .unwrap();
    let s = LabSender::new(
        repo.clone(),
        config,
        LabMetrics::default(),
        Default::default(),
    )
    .unwrap();
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Failed {
            reason: "connect",
            pending: 1
        }
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn batches_hold_at_most_500_events() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(accept_all)
        .mount(&lab)
        .await;
    let (s, repo, _) = sender(&t, &lab).await;
    let events: Vec<_> = (0..501).map(|_| event(Uuid::new_v4())).collect();
    repo.insert_many(&events).await.unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 500,
            pending: 0
        }
    );
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 1,
            pending: 0
        }
    );
    t.drop_schema().await;
}

#[tokio::test]
async fn an_oversized_ack_is_malformed_and_stays_pending() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    let padding = "x".repeat(2 * 1024 * 1024);
    Mock::given(method("POST"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({"accepted": [], "note": padding})),
        )
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    repo.insert_many(&[event(Uuid::new_v4())]).await.unwrap();
    assert_eq!(
        s.deliver_once().await,
        Delivery::Failed {
            reason: "malformed",
            pending: 1
        }
    );
    assert_eq!(repo.stats().await.unwrap().pending, 1);
    assert!(render(&metrics).contains("glutony_lab_events_failed_total{reason=\"malformed\"} 1"));
    t.drop_schema().await;
}

async fn backdate(t: &TestDb, id: Uuid, interval: &str) {
    sqlx::query(
        "UPDATE lab_events SET created_at = now() - $2::interval, next_attempt = now() \
         WHERE id = $1",
    )
    .bind(id)
    .bind(interval)
    .execute(&t.pool)
    .await
    .unwrap();
}

async fn left(t: &TestDb) -> Vec<Uuid> {
    sqlx::query_scalar("SELECT id FROM lab_events ORDER BY created_at")
        .fetch_all(&t.pool)
        .await
        .unwrap()
}

/// A row the Lab never answered for (401, 5xx, unreachable) is no proof it was
/// rejected: it is kept and retried for 7 days, not dropped after 24 h.
#[tokio::test]
async fn rows_the_lab_never_answered_for_are_kept_for_7_days() {
    let Some(t) = setup().await else { return };
    for answer in [Some(401), Some(503), None] {
        let lab = MockServer::start().await;
        let (s, repo, metrics) = match answer {
            Some(status) => {
                Mock::given(method("POST"))
                    .respond_with(ResponseTemplate::new(status))
                    .mount(&lab)
                    .await;
                sender(&t, &lab).await
            }
            None => {
                // Nothing listens on the port of a server that was started and dropped.
                let gone = MockServer::start().await;
                let uri = gone.uri();
                drop(gone);
                let repo = LabEventRepo::new(t.pool.clone());
                let config = LabConfig::from_values(
                    Some(uri),
                    Some(INSTANCE_ID.into()),
                    Some(SECRET.into()),
                    None,
                )
                .unwrap()
                .unwrap();
                let metrics = LabMetrics::default();
                let s = LabSender::new(repo.clone(), config, metrics.clone(), Default::default())
                    .unwrap();
                (s, repo, metrics)
            }
        };
        let (day_old, week_old) = (Uuid::new_v4(), Uuid::new_v4());
        repo.insert_many(&[event(day_old), event(week_old)])
            .await
            .unwrap();
        backdate(&t, day_old, "25 hours").await;
        backdate(&t, week_old, "7 days 1 hour").await;
        assert!(
            matches!(s.deliver_once().await, Delivery::Failed { pending: 2, .. }),
            "{answer:?}"
        );
        s.maintain().await;
        assert_eq!(left(&t).await, vec![day_old], "{answer:?}");
        assert!(
            render(&metrics).contains("glutony_lab_events_dropped_total 1"),
            "{answer:?}"
        );
        sqlx::query("DELETE FROM lab_events")
            .execute(&t.pool)
            .await
            .unwrap();
    }
    t.drop_schema().await;
}

/// A row the Lab answered 200 for without accepting it was skipped on purpose: it is
/// dropped 24 h after the first skip, whatever its age when that skip happened.
#[tokio::test]
async fn rows_the_lab_skipped_are_dropped_24h_after_the_first_skip() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({"accepted": []})))
        .mount(&lab)
        .await;
    let (s, repo, metrics) = sender(&t, &lab).await;
    let (skipped_long_ago, skipped_now) = (Uuid::new_v4(), Uuid::new_v4());
    repo.insert_many(&[event(skipped_long_ago), event(skipped_now)])
        .await
        .unwrap();
    // Waited out a 30 h outage, then skipped once just now: kept for 24 h more.
    backdate(&t, skipped_now, "30 hours").await;
    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 0,
            pending: 2
        }
    );
    // A later skip does not restart the clock of the first one.
    sqlx::query(
        "UPDATE lab_events SET first_skipped_at = now() - interval '25 hours', next_attempt = now() \
         WHERE id = $1",
    )
    .bind(skipped_long_ago)
    .execute(&t.pool)
    .await
    .unwrap();
    s.deliver_once().await;
    s.maintain().await;
    assert_eq!(left(&t).await, vec![skipped_now]);
    assert!(render(&metrics).contains("glutony_lab_events_dropped_total 1"));
    t.drop_schema().await;
}

/// A `usage.recorded` row as an old control-plane replica stores it during the rollout:
/// pre-v2 shape, written straight to the table, due now.
async fn insert_pre_v2(t: &TestDb) -> (Uuid, Uuid) {
    let (id, job) = (Uuid::new_v4(), Uuid::new_v4());
    let body = serde_json::json!({
        "id": id, "type": "usage.recorded", "occurred_at": "2026-10-01T10:00:09.000Z",
        "account_id": "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61", "api_key_id": null,
        "product": "glutony",
        "data": {
            "job_id": job, "pipeline_uid": "builtin.json", "status": "succeeded",
            "duration_ms": 2500, "cost_micro_usd": 7, "cost_complete": true,
            "units": {"documents_out": 4, "input_bytes": 10, "pages": 0, "images": 0,
                      "audio_seconds": 0.0, "llm_input_tokens": 0, "llm_output_tokens": 0,
                      "llm_requests": 0, "external_requests": 0}
        }
    });
    sqlx::query("INSERT INTO lab_events (id, body, attempts) VALUES ($1, $2, 3)")
        .bind(id)
        .bind(&body)
        .execute(&t.pool)
        .await
        .unwrap();
    (id, job)
}

fn schema_errors(event: &serde_json::Value) -> Vec<String> {
    let schema: serde_json::Value = serde_json::from_str(include_str!(
        "../../../contracts/vendor/lab/lab-events.schema.json"
    ))
    .unwrap();
    let v = jsonschema::options()
        .should_validate_formats(true)
        .build(&schema)
        .unwrap();
    v.iter_errors(event).map(|e| e.to_string()).collect()
}

#[tokio::test]
async fn a_pre_v2_row_is_converted_before_it_is_sent() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/events"))
        .respond_with(accept_all)
        .expect(1)
        .mount(&lab)
        .await;
    let (s, repo, _) = sender(&t, &lab).await;
    let (id, job) = insert_pre_v2(&t).await;
    let v2 = event(Uuid::new_v4());
    repo.insert_many(std::slice::from_ref(&v2)).await.unwrap();

    assert_eq!(
        s.deliver_once().await,
        Delivery::Sent {
            delivered: 2,
            pending: 0
        }
    );
    let req = &lab.received_requests().await.unwrap()[0];
    let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
    let sent = body["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["id"] == id.to_string())
        .expect("the pre-v2 row is in the batch")
        .clone();
    assert_eq!(sent["data"]["operation"], "ingest");
    assert_eq!(sent["data"]["units"]["documents"], 4);
    assert_eq!(sent["data"]["units"]["step_seconds"], 3);
    assert_eq!(sent["data"]["job_id"], job.to_string());
    assert!(
        schema_errors(&sent).is_empty(),
        "{:?}",
        schema_errors(&sent)
    );
    let delivered: bool =
        sqlx::query_scalar("SELECT delivered_at IS NOT NULL FROM lab_events WHERE id = $1")
            .bind(id)
            .fetch_one(&t.pool)
            .await
            .unwrap();
    assert!(delivered);
    t.drop_schema().await;
}

#[tokio::test]
async fn a_converted_row_keeps_its_lease_and_backs_off_on_failure() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .expect(1)
        .mount(&lab)
        .await;
    let (s, _, _) = sender(&t, &lab).await;
    let (id, _) = insert_pre_v2(&t).await;
    assert_eq!(
        s.deliver_once().await,
        Delivery::Failed {
            reason: "status",
            pending: 1
        }
    );
    // Converted (fresh window: attempts back to 0), then the failure counted once
    // and backed off: not due again right away.
    let (body, attempts, due): (serde_json::Value, i32, bool) = sqlx::query_as(
        "SELECT body, attempts, next_attempt <= now() FROM lab_events WHERE id = $1",
    )
    .bind(id)
    .fetch_one(&t.pool)
    .await
    .unwrap();
    assert_eq!(body["data"]["operation"], "ingest");
    assert_eq!(attempts, 1);
    assert!(!due, "the row backs off after the failure");
    assert_eq!(s.deliver_once().await, Delivery::Idle);
    t.drop_schema().await;
}

/// A pre-v2 row the Lab skipped (it rejects that shape) gets a fresh window once
/// converted: the skips of the old shape must not drop the converted event.
#[tokio::test]
async fn a_converted_row_forgets_the_skips_of_its_pre_v2_shape() {
    let Some(t) = setup().await else { return };
    let lab = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&lab)
        .await;
    let (s, _, _) = sender(&t, &lab).await;
    let (id, _) = insert_pre_v2(&t).await;
    sqlx::query(
        "UPDATE lab_events SET first_skipped_at = now() - interval '25 hours' WHERE id = $1",
    )
    .bind(id)
    .execute(&t.pool)
    .await
    .unwrap();
    s.deliver_once().await;
    s.maintain().await;
    let skipped: Option<Option<chrono::DateTime<chrono::Utc>>> =
        sqlx::query_scalar("SELECT first_skipped_at FROM lab_events WHERE id = $1")
            .bind(id)
            .fetch_optional(&t.pool)
            .await
            .unwrap();
    assert_eq!(skipped, Some(None), "kept, with no skip on record");
    t.drop_schema().await;
}
