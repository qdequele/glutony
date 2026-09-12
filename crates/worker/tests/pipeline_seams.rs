//! Integration checks for the seams between the worker, the blob store and the
//! plugins: a step output that was spilled to the blob store must reach the next
//! step as documents (never as a raw reference), including when it arrives inside a
//! multi-dependency `Many`, and inline JSON documents from the gateway must flow
//! through `builtin.json`'s first step unchanged.

use std::sync::Arc;

use meili_ingest_blob::BlobStore;
use meili_ingest_plugin_sdk::{Blob, Document, PluginInput, PluginOutput, StepActivityInput};
use meili_ingest_worker::{PluginRegistry, StepActivities};
use uuid::Uuid;

fn acts_t(t: usize) -> StepActivities {
    StepActivities::new(Arc::new(PluginRegistry::builtin()), BlobStore::memory(), t)
}
fn acts() -> StepActivities {
    acts_t(64)
}

fn ctx() -> meili_ingest_plugin_sdk::ActivityContext {
    meili_ingest_plugin_sdk::ActivityContext::noop()
}

fn step_in(job: Uuid, step: &str, plugin: &str, input: PluginInput) -> StepActivityInput {
    StepActivityInput {
        job_id: job,
        step_id: step.into(),
        plugin: plugin.into(),
        config: serde_json::json!({}),
        input,
        branch: None,
        branch_total: None,
        project_id: None,
    }
}

/// Spill → next step must hydrate back to Documents, never see a Ref.
#[tokio::test]
async fn spilled_output_round_trips_into_the_next_step() {
    let a = acts();
    let job = Uuid::new_v4();
    let big = "x ".repeat(500);
    let csv = format!("id,body\n1,{big}\n2,{big}\n");
    let out = a
        .run_step(
            &ctx(),
            step_in(
                job,
                "extract",
                "csv_parser",
                PluginInput::Bytes(Blob::new(
                    csv.into_bytes(),
                    "text/csv",
                    Some("a.csv".into()),
                )),
            ),
        )
        .await
        .expect("csv step");
    assert!(
        matches!(out.output, PluginOutput::Ref(_)),
        "expected the output to be spilled, got {:?}",
        out.output.kind()
    );

    // Feed the spilled ref to the chunker exactly as the workflow would.
    let next = a
        .run_step(
            &ctx(),
            step_in(job, "chunk", "chunker", PluginInput::from(out.output)),
        )
        .await
        .expect("chunker on spilled ref");
    let hydrated = a.blob.hydrate_output(next.output.clone()).await.unwrap();
    let docs = hydrated.into_documents().unwrap_or_default();
    assert!(!docs.is_empty(), "hydration lost the documents: {next:?}");
}

/// Multi-dependency Many carrying a spilled ref.
#[tokio::test]
async fn many_with_spilled_members_is_hydrated() {
    let a = acts();
    let job = Uuid::new_v4();
    let big: Vec<Document> = (0..40)
        .map(|i| Document::with_id(format!("d{i}"), "y".repeat(60)))
        .collect();
    let spilled = a
        .blob
        .spill_output(job, "up", None, PluginOutput::Documents(big), 64)
        .await
        .unwrap();
    assert!(matches!(spilled, PluginOutput::Ref(_)));
    let out = a
        .run_step(
            &ctx(),
            step_in(
                job,
                "chunk",
                "chunker",
                PluginInput::Many(vec![spilled, PluginOutput::Documents(vec![])]),
            ),
        )
        .await
        .expect("chunker on Many with spilled member");
    let hydrated = a.blob.hydrate_output(out.output.clone()).await.unwrap();
    assert!(hydrated.document_count() > 0, "got {:?}", out.output);
}

/// Inline JSON documents from the gateway through builtin.json's first step.
#[tokio::test]
async fn inline_documents_flow_through_json_flattener() {
    let a = acts_t(10_000_000);
    let job = Uuid::new_v4();
    let docs = vec![Document {
        id: "1".into(),
        title: Some("Hello".into()),
        content: String::new(),
        fields: serde_json::Map::new(),
        meta: Default::default(),
    }];
    let out = a
        .run_step(
            &ctx(),
            step_in(
                job,
                "extract",
                "json_flattener",
                PluginInput::Documents(docs),
            ),
        )
        .await
        .expect("json_flattener on inline documents");
    let d = out.output.into_documents().unwrap();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].id, "1");
    assert_eq!(d[0].title.as_deref(), Some("Hello"));
}
