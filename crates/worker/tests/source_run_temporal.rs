//! `SourceRunWorkflow` against a real Temporal server, with the control plane, the
//! upstream file server and Meilisearch played by wiremock.
//!
//! Skips cleanly unless `TEMPORAL_TEST_URL` is set (e.g. `http://localhost:57233`).
//! What these check that the activity unit tests cannot: that the workflow is
//! registered under the name schedules start, that its activities round-trip through
//! history, that child `PipelineWorkflow`s start under the job's workflow id, and
//! that a run's state is saved only when its jobs succeed.

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{PipelineDefinition, StepDefinition};
use meili_ingest_source::{HostPolicy, SOURCE_RUN_WORKFLOW, SecretKey, SourceRunInput};
use meili_ingest_worker::connection::ConnectionSettings;
use meili_ingest_worker::source_workflow::SourceRunOutput;
use meili_ingest_worker::{
    PipelineWorkflow, PluginRegistry, SourceActivities, SourceRunWorkflow, StepActivities,
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowGetResultOptions,
    WorkflowStartOptions,
};
use temporalio_sdk::runtime::worker_tuner::{FixedSizeSlotSupplier, TunerHolder};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use uuid::Uuid;
use wiremock::matchers::{header, method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

const SOURCE_ID: &str = "22222222-2222-2222-2222-222222222222";

async fn client() -> Option<Client> {
    let raw = std::env::var("TEMPORAL_TEST_URL").ok()?;
    let url = Url::from_str(&raw).expect("TEMPORAL_TEST_URL is a url");
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .expect("connect to Temporal");
    Some(Client::new(connection, ClientOptions::new("default").build()).expect("client"))
}

fn pipeline(connection: Option<&str>) -> PipelineDefinition {
    let config = match connection {
        Some(c) => serde_json::json!({ "connection": c }),
        None => serde_json::json!({}),
    };
    PipelineDefinition {
        uid: "movies".into(),
        name: "movies".into(),
        description: None,
        version: 1,
        trigger: None,
        steps: vec![
            StepDefinition::new("parse", "json_flattener"),
            StepDefinition::new("index", "meili_indexer")
                .depends_on(["parse"])
                .config(config),
        ],
        builtin: false,
        project_id: Some("tenant-1".into()),
    }
}

fn source_row(url: &str, etag: Option<&str>) -> serde_json::Value {
    let mut row = serde_json::json!({
        "id": SOURCE_ID,
        "uid": "tmdb",
        "name": "tmdb",
        "project_id": "tenant-1",
        "pipeline_uid": "movies",
        "location": { "kind": "url", "url": url },
        "cron": "30 0 * * *",
        "timezone": "UTC",
        "index_name": "movies",
        "schedule_id": format!("source-{SOURCE_ID}"),
    });
    if let Some(e) = etag {
        row["state"] = serde_json::json!({ "etag": e });
    }
    row
}

/// A control plane serving the source and the pipeline and accepting every write.
async fn control_plane(row: serde_json::Value, pipeline: PipelineDefinition) -> MockServer {
    let cp = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/internal/sources-by-id/{SOURCE_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(row))
        .mount(&cp)
        .await;
    Mock::given(method("GET"))
        .and(path("/pipelines/movies"))
        .respond_with(ResponseTemplate::new(200).set_body_json(pipeline))
        .mount(&cp)
        .await;
    for verb in ["POST", "PUT", "PATCH"] {
        Mock::given(method(verb))
            .and(path_regex("^/internal/"))
            .respond_with(ResponseTemplate::new(201))
            .mount(&cp)
            .await;
    }
    cp
}

/// Requests `cp` received for `verb` on `path` about [`SOURCE_ID`], bodies parsed as
/// JSON. Filtered by source because the queue may hold runs other suites left behind
/// (a triggered schedule with no worker), and this worker picks those up too.
async fn bodies(cp: &MockServer, verb: &str, p: &str) -> Vec<serde_json::Value> {
    cp.received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method.as_str() == verb && r.url.path() == p)
        .map(|r| serde_json::from_slice::<serde_json::Value>(&r.body).expect("json body"))
        .filter(|b| b.get("source_id").is_none_or(|s| s == SOURCE_ID))
        .collect()
}

/// Run `SourceRunWorkflow` for [`SOURCE_ID`] on a worker polling `queue`, and return
/// its result. The worker also serves `PipelineWorkflow` and the step activities, so
/// a run's child jobs execute too.
async fn run_source(
    client: Client,
    queue: &str,
    cp: &MockServer,
    files: &MockServer,
    key: Arc<SecretKey>,
) -> Result<SourceRunOutput, String> {
    let blob = BlobStore::memory();
    let files_host = files.uri().trim_start_matches("http://").to_string();
    let cp_host = cp.uri().trim_start_matches("http://").to_string();
    let source_activities = SourceActivities::new(Some(cp.uri()), blob.clone()).with_security(
        Some(key.clone()),
        HostPolicy::parse(&files_host).expect("fetch policy"),
    );
    let step_activities = StepActivities::new(Arc::new(PluginRegistry::builtin()), blob, 1 << 20)
        .with_control_plane(Some(cp.uri()))
        .with_connections(ConnectionSettings {
            key: Some(key),
            policy: HostPolicy::parse(&cp_host).expect("connection policy"),
        });

    let runtime = Runtime::from_current_tokio(Default::default()).expect("runtime");
    let tuner = TunerHolder::builder()
        .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .activity_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(2))
        .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(2))
        .build();
    let options = WorkerOptions::new(queue.to_string())
        .register_workflow::<PipelineWorkflow>()
        .expect("register PipelineWorkflow")
        .register_workflow::<SourceRunWorkflow>()
        .expect("register SourceRunWorkflow")
        .register_activities(step_activities)
        .register_activities(source_activities)
        .tuner(tuner)
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), options).expect("worker");
    let shutdown = worker.shutdown_handle();

    let input = SourceRunInput {
        source_id: Uuid::parse_str(SOURCE_ID).expect("uuid"),
        project_id: Some("tenant-1".into()),
    };
    let workflow_id = format!(
        "{}-{}",
        SourceRunInput::workflow_id_prefix(input.source_id),
        Uuid::new_v4()
    );
    let body = async {
        let handle = client
            .start_workflow(
                SourceRunWorkflow::run,
                input,
                WorkflowStartOptions::new(queue.to_string(), workflow_id).build(),
            )
            .await
            .expect("start");
        let result = tokio::time::timeout(
            Duration::from_secs(90),
            handle.get_result(WorkflowGetResultOptions::builder().build()),
        )
        .await
        .expect("the run finishes in time")
        .map_err(|e| e.to_string());
        shutdown();
        result
    };
    let (ran, result) = tokio::join!(worker.run(), body);
    ran.expect("worker ran");
    result
}

#[test]
fn the_workflow_is_registered_under_the_name_schedules_start() {
    use temporalio_common::WorkflowDefinition;
    assert_eq!(SourceRunWorkflow::run.name(), SOURCE_RUN_WORKFLOW);
}

#[tokio::test]
async fn an_unchanged_upstream_records_an_unchanged_run_and_starts_nothing() {
    let Some(client) = client().await else {
        eprintln!("TEMPORAL_TEST_URL unset; skipping");
        return;
    };
    let files = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.json"))
        .and(header("if-none-match", "\"v1\""))
        .respond_with(ResponseTemplate::new(304))
        .mount(&files)
        .await;
    let cp = control_plane(
        source_row(&format!("{}/feed.json", files.uri()), Some("\"v1\"")),
        pipeline(Some("prod-movies")),
    )
    .await;

    let queue = format!("source-run-test-{}", Uuid::new_v4());
    let key = Arc::new(SecretKey::from_bytes([3; 32]));
    let out = run_source(client, &queue, &cp, &files, key)
        .await
        .expect("an unchanged run succeeds");

    assert_eq!(out.outcome, meili_ingest_source::RunOutcome::Unchanged);
    assert!(out.job_ids.is_empty());
    let runs = bodies(&cp, "POST", "/internal/source-runs").await;
    assert_eq!(runs.len(), 1, "exactly one run row");
    assert_eq!(runs[0]["outcome"], "unchanged");
    assert_eq!(runs[0]["run_id"], serde_json::json!(out.run_id));
    assert!(
        bodies(&cp, "POST", "/internal/jobs").await.is_empty(),
        "no job for an unchanged upstream"
    );
    assert!(
        bodies(
            &cp,
            "PUT",
            &format!("/internal/sources-by-id/{SOURCE_ID}/state")
        )
        .await
        .is_empty(),
        "nothing new to save"
    );
}

#[tokio::test]
async fn a_pipeline_without_a_connection_fails_the_run_and_records_why() {
    let Some(client) = client().await else {
        return;
    };
    let files = MockServer::start().await;
    let cp = control_plane(
        source_row(&format!("{}/feed.json", files.uri()), None),
        pipeline(None),
    )
    .await;

    let queue = format!("source-run-test-{}", Uuid::new_v4());
    let key = Arc::new(SecretKey::from_bytes([3; 32]));
    let err = run_source(client, &queue, &cp, &files, key)
        .await
        .expect_err("the run fails");
    assert!(err.contains("failed"), "{err}");

    let runs = bodies(&cp, "POST", "/internal/source-runs").await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["outcome"], "failed");
    let why = runs[0]["error"].as_str().unwrap_or_default();
    assert!(why.contains("connection"), "the run row says why: {why}");
    assert!(
        files
            .received_requests()
            .await
            .unwrap_or_default()
            .is_empty(),
        "nothing is fetched for a run that cannot be delivered"
    );
}

/// Child jobs poll `workers-general` for their step activities, so this test's worker
/// polls that queue; run it against a Temporal no other worker is attached to.
#[tokio::test]
async fn a_failed_job_fails_the_run_and_keeps_the_old_state() {
    let Some(client) = client().await else {
        return;
    };
    let files = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/feed.json"))
        .respond_with(
            ResponseTemplate::new(200)
                .insert_header("content-type", "application/x-ndjson")
                .insert_header("etag", "\"v2\"")
                .set_body_string("{\"id\":1,\"title\":\"Heat\"}\n{\"id\":2,\"title\":\"Ran\"}\n"),
        )
        .mount(&files)
        .await;
    // The pipeline names a connection the control plane does not have (it was deleted
    // after the pipeline was saved): the indexer step fails non-retryably.
    let cp = control_plane(
        source_row(&format!("{}/feed.json", files.uri()), Some("\"v1\"")),
        pipeline(Some("gone")),
    )
    .await;

    let key = Arc::new(SecretKey::from_bytes([3; 32]));
    let err = run_source(client, "workers-general", &cp, &files, key)
        .await
        .expect_err("the run fails");
    assert!(err.contains("failed"), "{err}");

    let jobs = bodies(&cp, "POST", "/internal/jobs").await;
    assert_eq!(jobs.len(), 1, "one job for the one fetched file");
    assert_eq!(jobs[0]["source_id"], SOURCE_ID);
    let runs = bodies(&cp, "POST", "/internal/source-runs").await;
    assert_eq!(runs.len(), 1);
    assert_eq!(runs[0]["outcome"], "failed");
    assert_eq!(runs[0]["job_ids"], serde_json::json!([jobs[0]["job_id"]]));
    assert!(
        bodies(
            &cp,
            "PUT",
            &format!("/internal/sources-by-id/{SOURCE_ID}/state")
        )
        .await
        .is_empty(),
        "a failed run keeps the old state, so the next tick fetches again"
    );
}
