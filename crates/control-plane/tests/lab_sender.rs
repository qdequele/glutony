//! Lab events sender. Needs DATABASE_URL (skips otherwise), e.g.
//! `DATABASE_URL=postgres://postgres:dev@localhost:55433/postgres cargo test -p meili-ingest-control-plane --test lab_sender`.

use std::str::FromStr;

use meili_ingest_control_plane::db;
use meili_ingest_control_plane::lab_events::LabEventRepo;
use meili_ingest_control_plane::lab_sender::{Delivery, LabConfig, LabSender, sign};
use meili_ingest_control_plane::metrics::{LabMetrics, render};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{AssertSqlSafe, PgPool};
use uuid::Uuid;
use wiremock::matchers::{header, method, path};
use wiremock::{Mock, MockServer, Request as WmRequest, ResponseTemplate};

const SECRET: &str = "lab-events-secret";

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
    let config = LabConfig::from_values(Some(lab.uri()), Some(SECRET.into()))
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
    let sig = req
        .headers
        .get("x-lab-signature")
        .unwrap()
        .to_str()
        .unwrap();
    assert_eq!(sig, sign(SECRET.as_bytes(), &req.body));
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
    let config = LabConfig::from_values(Some("http://127.0.0.1:1".into()), Some(SECRET.into()))
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
