//! End-to-end test: real WASM modules are loaded through `WasmPlugin` and called.
//!
//! The guests are tiny WebAssembly text modules built here and compiled by extism at
//! load, so there is no binary fixture to keep in sync. They talk to the host through
//! the extism kernel imports (`extism:host/env`), exactly like a module built with
//! `extism-pdk`, and reply with the JSON a guest built on
//! `meili_ingest_plugin_sdk::wasm` would produce.
#![cfg(feature = "wasm")]

use std::time::Duration;

use meili_ingest_plugin_runtime::{RuntimeError, WasmPlugin};
use meili_ingest_plugin_sdk::prelude::*;

/// What `ExecuteReply::Ok` looks like on the wire (`meili_ingest_plugin_sdk::wasm`).
const OK_REPLY: &str = r#"{"ok":{"type":"documents","value":[{"id":"a","content":"hello"}]}}"#;
/// What a guest sends back when the worker should retry.
const RETRYABLE_REPLY: &str = r#"{"error":{"message":"try later","retryable":true}}"#;
/// `retryable` left out: the host must read it as a permanent failure.
const FATAL_REPLY: &str = r#"{"error":{"message":"nope"}}"#;

/// Where the `i`-th reply lives in the guest's memory.
const REPLY_BASE: usize = 4096;
const REPLY_SLOT: usize = 1024;
/// Where the bytes the envelope must start with live.
const PREFIX_AT: usize = 2048;
/// How every envelope the host sends begins.
const ENVELOPE_PREFIX: &str = r#"{"input":"#;

/// A WAT string literal holding exactly `bytes`.
fn wat_bytes(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("\\{b:02x}")).collect()
}

/// WAT that returns the `i`-th reply to the host.
fn emit(replies: &[&str], i: usize) -> String {
    format!(
        "(call $emit (i32.const {}) (i32.const {}))",
        REPLY_BASE + i * REPLY_SLOT,
        replies[i].len()
    )
}

/// Build a guest module.
///
/// * `manifest`: what the `manifest` export returns, or `None` to leave it out.
/// * `execute`: the body of the `execute` export (locals first), or `None` to leave
///   the export out. It reaches its replies through [`emit`].
fn guest(manifest: Option<&str>, execute: Option<&str>, replies: &[&str]) -> Vec<u8> {
    let mut wat = String::from(
        r#"(module
  (import "extism:host/env" "input_length" (func $input_length (result i64)))
  (import "extism:host/env" "input_load_u8" (func $input_load_u8 (param i64) (result i32)))
  (import "extism:host/env" "alloc" (func $alloc (param i64) (result i64)))
  (import "extism:host/env" "store_u8" (func $store_u8 (param i64 i32)))
  (import "extism:host/env" "output_set" (func $output_set (param i64 i64)))
  (memory (export "memory") 1)
  ;; Copy `len` bytes of this module's memory into extism memory and return them.
  (func $emit (param $ptr i32) (param $len i32)
    (local $off i64) (local $i i32)
    (local.set $off (call $alloc (i64.extend_i32_u (local.get $len))))
    (block $done
      (loop $copy
        (br_if $done (i32.ge_u (local.get $i) (local.get $len)))
        (call $store_u8
          (i64.add (local.get $off) (i64.extend_i32_u (local.get $i)))
          (i32.load8_u (i32.add (local.get $ptr) (local.get $i))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $copy)))
    (call $output_set (local.get $off) (i64.extend_i32_u (local.get $len))))
"#,
    );
    wat.push_str(&format!(
        "  (data (i32.const {PREFIX_AT}) \"{}\")\n",
        wat_bytes(ENVELOPE_PREFIX.as_bytes())
    ));
    for (i, reply) in replies.iter().enumerate() {
        assert!(reply.len() <= REPLY_SLOT, "reply {i} does not fit its slot");
        wat.push_str(&format!(
            "  (data (i32.const {}) \"{}\")\n",
            REPLY_BASE + i * REPLY_SLOT,
            wat_bytes(reply.as_bytes())
        ));
    }
    if let Some(manifest) = manifest {
        assert!(manifest.len() <= PREFIX_AT, "manifest does not fit");
        wat.push_str(&format!(
            "  (data (i32.const 0) \"{}\")\n  (func (export \"manifest\") (result i32)\n    \
             (call $emit (i32.const 0) (i32.const {}))\n    (i32.const 0))\n",
            wat_bytes(manifest.as_bytes()),
            manifest.len()
        ));
    }
    if let Some(body) = execute {
        wat.push_str(&format!(
            "  (func (export \"execute\") (result i32)\n{body}\n    (i32.const 0))\n"
        ));
    }
    wat.push_str(")\n");
    wat.into_bytes()
}

/// The manifest a guest built on the SDK would return from `manifest_json`.
fn manifest_json() -> String {
    let manifest = PluginManifest::new("fixture", "1.2.3")
        .description("test guest")
        .accepts([InputKind::Documents])
        .produces(OutputKind::Documents);
    serde_json::to_string(&manifest).unwrap()
}

/// A guest that answers every call with `reply`.
fn fixed_reply(reply: &str) -> Vec<u8> {
    let replies = [reply];
    guest(Some(&manifest_json()), Some(&emit(&replies, 0)), &replies)
}

/// A guest that answers [`OK_REPLY`] when its input starts like an envelope, and a
/// permanent error otherwise.
fn envelope_checker() -> Vec<u8> {
    let replies = [
        OK_REPLY,
        r#"{"error":{"message":"not an envelope","retryable":false}}"#,
    ];
    let len = ENVELOPE_PREFIX.len();
    let body = format!(
        r#"    (local $i i32)
    (if (i64.lt_u (call $input_length) (i64.const {len}))
      (then {bad} (return (i32.const 0))))
    (block $same
      (loop $cmp
        (br_if $same (i32.ge_u (local.get $i) (i32.const {len})))
        (if (i32.ne
              (call $input_load_u8 (i64.extend_i32_u (local.get $i)))
              (i32.load8_u (i32.add (i32.const {PREFIX_AT}) (local.get $i))))
          (then {bad} (return (i32.const 0))))
        (local.set $i (i32.add (local.get $i) (i32.const 1)))
        (br $cmp)))
    {ok}"#,
        ok = emit(&replies, 0),
        bad = emit(&replies, 1),
    );
    guest(Some(&manifest_json()), Some(&body), &replies)
}

async fn execute(plugin: &WasmPlugin) -> Result<PluginOutput, PluginError> {
    let input = PluginInput::Documents(vec![Document::with_id("a", "hello")]);
    tokio::time::timeout(
        Duration::from_secs(30),
        plugin.execute(&ActivityContext::noop(), input, serde_json::json!({})),
    )
    .await
    .expect("the guest call must return")
}

#[tokio::test]
async fn manifest_comes_from_the_guest_export() {
    let plugin = WasmPlugin::from_bytes(fixed_reply(OK_REPLY), None).unwrap();
    let manifest = plugin.manifest();
    assert_eq!(manifest.name, "fixture");
    assert_eq!(manifest.version, "1.2.3");
    assert_eq!(manifest.accepts, vec![InputKind::Documents]);
    // Whatever the guest claims, a module loaded here is a WASM plugin.
    assert_eq!(manifest.kind, PluginKind::Wasm);
    assert_eq!(plugin.source(), "<bytes>");
}

#[tokio::test]
async fn guest_receives_the_envelope_and_its_ok_reply_becomes_the_output() {
    let plugin = WasmPlugin::from_bytes(envelope_checker(), None).unwrap();
    let docs = execute(&plugin).await.unwrap().into_documents().unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].id, "a");
    assert_eq!(docs[0].content, "hello");

    // One instance serves call after call.
    let again = execute(&plugin).await.unwrap().into_documents().unwrap();
    assert_eq!(again, docs);
}

#[tokio::test]
async fn error_replies_keep_their_retry_semantics() {
    let plugin = WasmPlugin::from_bytes(fixed_reply(RETRYABLE_REPLY), None).unwrap();
    let err = execute(&plugin).await.unwrap_err();
    assert!(
        matches!(&err, PluginError::Retryable(m) if m == "try later"),
        "got {err:?}"
    );

    let plugin = WasmPlugin::from_bytes(fixed_reply(FATAL_REPLY), None).unwrap();
    let err = execute(&plugin).await.unwrap_err();
    assert!(
        matches!(&err, PluginError::NonRetryable(m) if m == "nope"),
        "got {err:?}"
    );
}

#[tokio::test]
async fn invalid_reply_json_is_a_permanent_failure() {
    let plugin = WasmPlugin::from_bytes(fixed_reply("certainly not json"), None).unwrap();
    let err = execute(&plugin).await.unwrap_err();
    assert!(
        matches!(&err, PluginError::NonRetryable(m) if m.contains("invalid reply JSON")),
        "got {err:?}"
    );
}

#[tokio::test]
async fn a_trapping_guest_is_a_permanent_failure() {
    let module = guest(Some(&manifest_json()), Some("    (unreachable)"), &[]);
    let plugin = WasmPlugin::from_bytes(module, None).unwrap();
    let err = execute(&plugin).await.unwrap_err();
    assert!(matches!(err, PluginError::NonRetryable(_)), "got {err:?}");
}

#[tokio::test]
async fn module_without_manifest_needs_an_override() {
    let replies = [OK_REPLY];
    let module = guest(None, Some(&emit(&replies, 0)), &replies);

    let err = WasmPlugin::from_bytes(module.clone(), None).unwrap_err();
    assert!(
        matches!(err, RuntimeError::MissingManifest(_)),
        "got {err:?}"
    );

    let given = PluginManifest::new("renamed", "0.0.1").produces(OutputKind::Documents);
    let plugin = WasmPlugin::from_bytes(module, Some(given)).unwrap();
    assert_eq!(plugin.manifest().name, "renamed");
    assert_eq!(plugin.manifest().kind, PluginKind::Wasm);
    assert!(execute(&plugin).await.is_ok());
}

#[tokio::test]
async fn manifest_override_wins_over_the_guest_export() {
    let given = PluginManifest::new("renamed", "0.0.1");
    let plugin = WasmPlugin::from_bytes(fixed_reply(OK_REPLY), Some(given)).unwrap();
    assert_eq!(plugin.manifest().name, "renamed");
}

#[tokio::test]
async fn guest_manifest_that_is_not_json_is_rejected() {
    let replies = [OK_REPLY];
    let module = guest(Some("not a manifest"), Some(&emit(&replies, 0)), &replies);
    let err = WasmPlugin::from_bytes(module, None).unwrap_err();
    assert!(matches!(err, RuntimeError::Serde(_)), "got {err:?}");
}

#[tokio::test]
async fn module_without_execute_is_rejected() {
    let module = guest(Some(&manifest_json()), None, &[]);
    let err = WasmPlugin::from_bytes(module, None).unwrap_err();
    assert!(
        matches!(&err, RuntimeError::Wasm(m) if m.contains("does not export `execute`")),
        "got {err:?}"
    );
}
