//! Every example in the document_script reference (`tests/reference.json`, the data a
//! script editor shows) must run and produce what it claims — as a script (`snippet`)
//! and, where there is one, as an `ops` entry (`op`).

use meili_ingest_plugin_script::DocumentScriptPlugin;
use meili_ingest_plugin_sdk::prelude::*;
use serde_json::{Value, json};

const REFERENCE: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/reference.json");

async fn run(example: &Value, config: Value) -> Vec<Document> {
    let mut doc = Document::with_id("example", example["content"].as_str().unwrap_or(""));
    doc.fields = example["fields"].as_object().cloned().unwrap_or_default();
    match DocumentScriptPlugin::new()
        .execute(
            &ActivityContext::noop(),
            PluginInput::Documents(vec![doc]),
            config,
        )
        .await
    {
        Ok(PluginOutput::Documents(docs)) => docs,
        Ok(other) => panic!("expected documents, got {other:?}"),
        Err(e) => panic!("{e}"),
    }
}

fn assert_expected(id: &str, mode: &str, out: &[Document], expect: &Value) {
    if expect["dropped"] == json!(true) {
        assert!(
            out.is_empty(),
            "{id} ({mode}): expected the document to be dropped"
        );
        return;
    }
    assert_eq!(out.len(), 1, "{id} ({mode}): expected one document");
    assert_eq!(
        Value::Object(out[0].fields.clone()),
        expect["fields"],
        "{id} ({mode}): fields"
    );
    if let Some(title) = expect.get("title") {
        assert_eq!(
            out[0].title.as_deref(),
            title.as_str(),
            "{id} ({mode}): title"
        );
    }
}

#[tokio::test]
async fn every_reference_example_runs_as_documented() {
    let reference: Value =
        serde_json::from_str(&std::fs::read_to_string(REFERENCE).expect("reference.json"))
            .expect("reference.json is JSON");
    let entries = reference["entries"].as_array().expect("entries");
    assert!(!entries.is_empty());

    for entry in entries {
        let id = entry["id"].as_str().unwrap();
        let out = run(&entry["example"], json!({ "script": entry["snippet"] })).await;
        assert_expected(id, "snippet", &out, &entry["expect"]);

        if !entry["op"].is_null() {
            let out = run(&entry["example"], json!({ "ops": [entry["op"]] })).await;
            assert_expected(id, "op", &out, &entry["expect"]);
        }
    }
}

#[test]
fn every_builder_operation_has_a_reference_entry() {
    let reference: Value =
        serde_json::from_str(&std::fs::read_to_string(REFERENCE).unwrap()).unwrap();
    let kinds: Vec<String> = reference["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["op"].as_object().and_then(|op| op.keys().next().cloned()))
        .collect();
    for kind in [
        "rename",
        "drop",
        "keep",
        "copy",
        "set",
        "filter",
        "trim",
        "lowercase",
        "uppercase",
        "split",
        "join",
        "promote",
        "round",
        "cast",
        "default",
    ] {
        assert!(
            kinds.iter().any(|k| k == kind),
            "no reference entry for `{kind}`"
        );
    }
}
