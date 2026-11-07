//! `ArchiveWorkflow` against a real Temporal server, with the control plane played by
//! wiremock and each file's pipeline running for real.
//!
//! Skips cleanly unless `TEMPORAL_TEST_URL` is set (e.g. `http://localhost:57233`).
//! The worker polls `workers-general`, the queue step activities are routed to, so run
//! these against a Temporal server no other worker polls.
//!
//! What these check that the activity unit tests cannot: that the workflow is
//! registered under the name the gateway starts, that every file runs as a child job
//! linked to its archive, that continue-as-new carries the position and the counts,
//! and that one failed file fails the archive and is named.

use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use meili_ingest_archive::Limits;
use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{
    ARCHIVE_WORKFLOW_TYPE, ArchiveBatching, ArchiveWorkflowInput, ArchiveWorkflowOutput, Blob,
    MeiliContext, PipelineDefinition, PipelineTrigger, PipelineWorkflowInput, StepDefinition,
};
use meili_ingest_worker::{
    ArchiveActivities, ArchiveWorkflow, PipelineWorkflow, PluginRegistry, SourceActivities,
    StepActivities,
};
use temporalio_client::{
    Client, ClientOptions, Connection, ConnectionOptions, Url, WorkflowDescribeOptions,
    WorkflowGetResultOptions, WorkflowStartOptions,
};
use temporalio_sdk::runtime::worker_tuner::{FixedSizeSlotSupplier, TunerHolder};
use temporalio_sdk::{Runtime, Worker, WorkerOptions};
use uuid::Uuid;
use wiremock::matchers::{body_partial_json, method, path, path_regex};
use wiremock::{Mock, MockServer, ResponseTemplate};

/// Step activities are routed to this queue whatever queue the workflow runs on.
const QUEUE: &str = "workers-general";

/// One test at a time: every test's worker polls [`QUEUE`] with its own in-memory blob
/// store, so two at once would pick up each other's activities.
static SERIAL: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn client() -> Option<Client> {
    let raw = std::env::var("TEMPORAL_TEST_URL").ok()?;
    let url = Url::from_str(&raw).expect("TEMPORAL_TEST_URL is a url");
    let connection = Connection::connect(ConnectionOptions::new(url).build())
        .await
        .expect("connect to Temporal");
    Some(Client::new(connection, ClientOptions::new("default").build()).expect("client"))
}

fn zip_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
    let mut w = zip::ZipWriter::new(std::io::Cursor::new(Vec::new()));
    let opts = zip::write::SimpleFileOptions::default();
    for (name, data) in entries {
        w.start_file(*name, opts).unwrap();
        w.write_all(data).unwrap();
    }
    w.finish().unwrap().into_inner()
}

/// JSON files are parsed and nothing else: no indexer, so no Meilisearch is needed.
fn json_pipeline() -> PipelineDefinition {
    PipelineDefinition {
        uid: "json-only".into(),
        name: "json-only".into(),
        description: None,
        version: 1,
        trigger: Some(PipelineTrigger {
            content_types: vec!["application/json".into()],
            filename_pattern: None,
            index_pattern: None,
        }),
        steps: vec![StepDefinition::new("parse", "json_flattener")],
        builtin: false,
        tenant_id: Some("tenant-1".into()),
    }
}

/// A control plane routing JSON to [`json_pipeline`], nothing else, and accepting
/// every job write.
async fn control_plane() -> MockServer {
    let cp = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/internal/resolve"))
        .and(body_partial_json(
            serde_json::json!({"mime": "application/json"}),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "pipeline": json_pipeline(),
        })))
        .mount(&cp)
        .await;
    Mock::given(method("POST"))
        .and(path("/internal/resolve"))
        .respond_with(
            ResponseTemplate::new(404).set_body_json(serde_json::json!({"error": "no match"})),
        )
        .mount(&cp)
        .await;
    Mock::given(method("POST"))
        .and(path("/internal/jobs"))
        .respond_with(ResponseTemplate::new(201))
        .mount(&cp)
        .await;
    Mock::given(method("PATCH"))
        .and(path_regex("^/internal/jobs/"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({})))
        .mount(&cp)
        .await;
    cp
}

/// Requests `cp` received for `verb` on `p`, bodies parsed as JSON.
async fn bodies(cp: &MockServer, verb: &str, p: &str) -> Vec<serde_json::Value> {
    cp.received_requests()
        .await
        .unwrap_or_default()
        .into_iter()
        .filter(|r| r.method.as_str() == verb && r.url.path() == p)
        .map(|r| serde_json::from_slice::<serde_json::Value>(&r.body).expect("json body"))
        .collect()
}

/// Stage `zip`, run the archive job on a worker, and return its result and the run id
/// of its last run.
async fn run_archive(
    client: Client,
    cp: &MockServer,
    job_id: Uuid,
    zip: Vec<u8>,
    batching: ArchiveBatching,
) -> (Result<ArchiveWorkflowOutput, String>, String, String) {
    let blob = BlobStore::memory();
    let input = blob
        .stage_upload(
            job_id,
            Blob::new(zip, "application/zip", Some("bundle.zip".into())),
            0,
        )
        .await
        .expect("stage the archive");
    let archive_activities = ArchiveActivities::new(
        SourceActivities::new(Some(cp.uri()), blob.clone()),
        Limits::default(),
    );
    let step_activities = StepActivities::new(Arc::new(PluginRegistry::builtin()), blob, 1 << 20)
        .with_control_plane(Some(cp.uri()));

    let runtime = Runtime::from_current_tokio(Default::default()).expect("runtime");
    let tuner = TunerHolder::builder()
        .workflow_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .activity_task_slot_supplier(FixedSizeSlotSupplier::new(10))
        .local_activity_task_slot_supplier(FixedSizeSlotSupplier::new(2))
        .nexus_task_slot_supplier(FixedSizeSlotSupplier::new(2))
        .build();
    let options = WorkerOptions::new(QUEUE.to_string())
        .register_workflow::<PipelineWorkflow>()
        .expect("register PipelineWorkflow")
        .register_workflow::<ArchiveWorkflow>()
        .expect("register ArchiveWorkflow")
        .register_activities(step_activities)
        .register_activities(archive_activities)
        .tuner(tuner)
        .build();
    let mut worker = Worker::new(&runtime, client.clone(), options).expect("worker");
    let shutdown = worker.shutdown_handle();

    let input = ArchiveWorkflowInput {
        job_id,
        input,
        context: MeiliContext {
            tenant_id: Some("tenant-1".into()),
            host: Some("http://meili.invalid:7700".into()),
            api_key: Some("unused".into()),
            index: None,
            region: None,
        },
        forced_pipeline: None,
        locked_index: None,
        resume: None,
        batching: Some(batching),
    };
    let body = async {
        let handle = client
            .start_workflow(
                ArchiveWorkflow::run,
                input,
                WorkflowStartOptions::new(
                    QUEUE.to_string(),
                    PipelineWorkflowInput::workflow_id(job_id),
                )
                .build(),
            )
            .await
            .expect("start");
        let first_run = handle.info().run_id.clone().unwrap_or_default();
        let result = tokio::time::timeout(
            Duration::from_secs(120),
            handle.get_result(WorkflowGetResultOptions::builder().build()),
        )
        .await
        .expect("the archive finishes in time")
        .map_err(|e| format!("{e:?}"));
        let last_run = client
            .get_workflow_handle::<ArchiveWorkflow>(PipelineWorkflowInput::workflow_id(job_id))
            .describe(WorkflowDescribeOptions::default())
            .await
            .expect("describe")
            .run_id()
            .to_string();
        shutdown();
        (result, first_run, last_run)
    };
    let (ran, out) = tokio::join!(worker.run(), body);
    ran.expect("worker ran");
    out
}

#[test]
fn the_workflow_is_registered_under_the_name_the_gateway_starts() {
    use temporalio_common::WorkflowDefinition;
    assert_eq!(ArchiveWorkflow::run.name(), ARCHIVE_WORKFLOW_TYPE);
}

#[tokio::test]
async fn every_file_runs_as_its_own_job_across_continued_runs() {
    let Some(client) = client().await else {
        eprintln!("TEMPORAL_TEST_URL unset; skipping");
        return;
    };
    let _serial = SERIAL.lock().await;
    let cp = control_plane().await;
    let job_id = Uuid::new_v4();
    let zip = zip_of(&[
        ("d/1.json", br#"{"id": 1}"#),
        ("d/2.json", br#"{"id": 2}"#),
        ("d/3.json", br#"{"id": 3}"#),
        ("e/4.json", br#"{"id": 4}"#),
        ("e/5.json", br#"{"id": 5}"#),
        ("tool.bin", &[0, 1, 2, 0xff]),
        ("empty.json", b""),
    ]);
    // Two files per wave, two waves per run: the 6 staged files take two runs.
    let batching = ArchiveBatching {
        wave: 2,
        waves_per_run: 2,
    };
    let (result, first_run, last_run) = run_archive(client, &cp, job_id, zip, batching).await;
    let out = result.expect("the archive succeeds");

    assert_eq!(out.job_id, job_id);
    let a = &out.archive;
    assert_eq!(a.files_total, 6, "{a:?}");
    assert_eq!(a.files_started, 5, "{a:?}");
    assert_eq!(a.files_succeeded, 5, "{a:?}");
    assert_eq!(a.files_failed, 0, "{a:?}");
    assert_eq!(a.files_skipped, 2, "{a:?}");
    let skipped: Vec<&str> = a.skipped.iter().map(|i| i.path.as_str()).collect();
    assert_eq!(skipped, vec!["empty.json", "tool.bin"]);
    assert_ne!(first_run, last_run, "the workflow continued as new");

    let jobs = bodies(&cp, "POST", "/internal/jobs").await;
    let mine: Vec<_> = jobs
        .iter()
        .filter(|j| j["parent_job_id"] == serde_json::json!(job_id))
        .collect();
    assert_eq!(mine.len(), 5, "one job row per file, linked to the archive");
    assert!(mine.iter().all(|j| j["pipeline_uid"] == "json-only"));
    let finished = bodies(&cp, "PATCH", &format!("/internal/jobs/{job_id}")).await;
    assert_eq!(
        finished.last().map(|b| b["status"].clone()),
        Some(serde_json::json!("succeeded")),
        "{finished:?}"
    );
}

#[tokio::test]
async fn a_failed_file_fails_the_archive_and_is_named() {
    let Some(client) = client().await else {
        return;
    };
    let _serial = SERIAL.lock().await;
    let cp = control_plane().await;
    let job_id = Uuid::new_v4();
    let zip = zip_of(&[("good.json", br#"{"id": 1}"#), ("bad.json", b"{ not json")]);
    let batching = ArchiveBatching {
        wave: 10,
        waves_per_run: 10,
    };
    let (result, _, _) = run_archive(client, &cp, job_id, zip, batching).await;
    let err = result.expect_err("the archive fails");
    assert!(
        err.contains("1 of 2 files failed; first: bad.json"),
        "{err}"
    );

    let finished = bodies(&cp, "PATCH", &format!("/internal/jobs/{job_id}")).await;
    let last = finished.last().expect("the archive row was updated");
    assert_eq!(last["status"], "failed");
    assert!(
        last["error"]
            .as_str()
            .unwrap_or_default()
            .contains("bad.json"),
        "{last}"
    );
}
