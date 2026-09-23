//! `TemporalSchedules` against a real Temporal server.
//!
//! Skips cleanly unless `TEMPORAL_TEST_URL` is set (e.g. `http://localhost:57233`), so
//! the suite stays green without a server. These tests exist to check what unit tests
//! cannot: that Temporal itself rejects a malformed cron with `InvalidArgument`, which
//! is what the gateway's 422 mapping relies on (spec Decision 10).

use std::str::FromStr;

use meili_ingest_gateway::GatewayError;
use meili_ingest_gateway::schedules::{ScheduleClient, SourceSchedule, TemporalSchedules};
use meili_ingest_source::SourceRunInput;
use temporalio_client::{Client, ClientOptions, Connection, ConnectionOptions, Url};
use uuid::Uuid;

async fn schedules() -> Option<TemporalSchedules> {
    let raw = std::env::var("TEMPORAL_TEST_URL").ok()?;
    let url = Url::from_str(&raw).expect("TEMPORAL_TEST_URL is a url");
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .expect("connect to Temporal");
    let client = Client::new(connection, ClientOptions::new("default").build()).expect("client");
    Some(TemporalSchedules(client))
}

fn schedule(cron: &str, timezone: &str) -> SourceSchedule {
    let source_id = Uuid::new_v4();
    SourceSchedule {
        schedule_id: SourceRunInput::schedule_id(source_id),
        source_id,
        project_id: Some("tenant-1".into()),
        cron: cron.into(),
        timezone: timezone.into(),
        paused: true,
    }
}

#[tokio::test]
async fn a_schedule_goes_through_its_whole_lifecycle() {
    let Some(t) = schedules().await else {
        eprintln!("TEMPORAL_TEST_URL unset; skipping");
        return;
    };
    let s = schedule("30 0 * * *", "Europe/Paris");

    t.create(&s).await.expect("create");
    let info = t
        .describe(&s.schedule_id)
        .await
        .expect("describe")
        .expect("exists");
    assert!(
        info.paused,
        "created paused, as the create-then-unpause order needs"
    );
    assert!(info.next_run_at.is_some(), "a daily cron has a next run");

    t.set_paused(&s.schedule_id, false).await.expect("unpause");
    let info = t
        .describe(&s.schedule_id)
        .await
        .expect("describe")
        .expect("exists");
    assert!(!info.paused);

    let mut changed = s.clone();
    changed.cron = "0 9 * * MON".into();
    t.update(&changed).await.expect("update the cron");

    // No worker polls in this test, so the run just waits in the queue; the point is
    // that Temporal accepts the trigger.
    t.trigger(&s.schedule_id).await.expect("trigger");

    t.delete(&s.schedule_id).await.expect("delete");
    assert!(
        t.describe(&s.schedule_id)
            .await
            .expect("describe")
            .is_none(),
        "gone after delete"
    );
    t.delete(&s.schedule_id)
        .await
        .expect("deleting a schedule that is already gone is not an error");
}

#[tokio::test]
async fn temporal_rejects_a_malformed_cron_as_a_422() {
    let Some(t) = schedules().await else {
        return;
    };
    let s = schedule("not a cron", "UTC");
    let err = t.create(&s).await.expect_err("Temporal must reject it");
    assert!(
        matches!(err, GatewayError::Unprocessable(_)),
        "a malformed cron is the caller's error, not an upstream failure: {err:?}"
    );
    // Nothing was left behind.
    assert!(
        t.describe(&s.schedule_id)
            .await
            .expect("describe")
            .is_none()
    );
}

#[tokio::test]
async fn creating_the_same_schedule_twice_is_a_422() {
    let Some(t) = schedules().await else {
        return;
    };
    let s = schedule("0 3 * * *", "UTC");
    t.create(&s).await.expect("first create");
    let err = t.create(&s).await.expect_err("duplicate");
    t.delete(&s.schedule_id).await.expect("cleanup");
    assert!(matches!(err, GatewayError::Unprocessable(_)), "{err:?}");
}

#[tokio::test]
async fn describing_an_unknown_schedule_is_none() {
    let Some(t) = schedules().await else {
        return;
    };
    let id = SourceRunInput::schedule_id(Uuid::new_v4());
    assert!(t.describe(&id).await.expect("describe").is_none());
}
