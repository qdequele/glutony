//! # `document_script` — run a sandboxed script over every document
//!
//! Built-in `meili-ingest` plugin that runs one [rhai](https://rhai.rs) script per
//! incoming [`Document`]. It is the general-purpose reshaping step: rename a field,
//! drop fields, compute a new one from the others, promote a field into `title` or
//! `content`, or filter a document out entirely.
//!
//! The document is bound as `doc`, a map with:
//!
//! | binding | type | writable |
//! |---|---|---|
//! | `doc.id` | string | yes — re-sanitised afterwards |
//! | `doc.title` | string or `()` | yes |
//! | `doc.content` | string | yes |
//! | `doc.fields` | map | yes |
//! | `doc.meta` | map | no — writes are discarded |
//!
//! The script mutates `doc` in place. Returning `false` drops the document from the
//! output; any other result keeps it.
//!
//! ```text
//! doc.fields.price = doc.fields.prix;     // rename
//! doc.fields.remove("prix");
//! doc.fields.total = doc.fields.price * doc.fields.qty;
//! doc.title = doc.fields.name;            // promote into title
//! doc.content.trim();
//! if doc.content.is_empty() { return false; }     // drop this document
//! ```
//!
//! Rhai's string methods (`trim`, `to_upper`, `pad`, ...) take `&mut self` and
//! return `()`: they edit the string in place rather than returning a copy. So
//! `doc.content.trim() == ""` is always `false` — trim as its own statement, then
//! test `is_empty()`.
//!
//! Float arithmetic that produces NaN or an infinity (`x / 0.0`, which rhai does
//! not raise on) fails the document rather than writing `null` into the index.
//!
//! ## Sandbox
//!
//! Pipeline configs are tenant-authored, so the engine is locked down. `no_module`
//! removes `import`/`export`, `no_custom_syntax` removes syntax extension and
//! `no_time` removes the clock; rhai has no file, network or process API to begin
//! with, so a script can only reach `doc`. On top of that every run is bounded by an
//! operation budget (`max_ops`), a wall-clock budget (`timeout_ms`) and fixed caps on
//! expression depth, call depth, string, array and map sizes.
//!
//! Configuration:
//!
//! | key | default | meaning |
//! |---|---|---|
//! | `script` | required | the rhai script, run once per document |
//! | `on_error` | `fail` | what a per-document error does: `fail`, `skip` or `keep` |
//! | `max_ops` | `100000` | rhai operations one document may use |
//! | `timeout_ms` | `1000` | wall-clock milliseconds one document may use |

mod config;
mod convert;
mod runner;

use meili_ingest_plugin_sdk::prelude::*;

use crate::config::{Config, OnError};
use crate::runner::{Runner, Verdict};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "document_script";

/// Documents between two heartbeats.
const HEARTBEAT_EVERY: usize = 10;

/// The document script plugin. Stateless; construct with [`DocumentScriptPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct DocumentScriptPlugin;

impl DocumentScriptPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl Plugin for DocumentScriptPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Runs a sandboxed rhai script over every document: rename and drop fields, \
                 compute new ones from the others, promote a field into title or content, or \
                 return false to filter the document out.",
            )
            .accepts([InputKind::Documents, InputKind::Many])
            .produces(OutputKind::Documents)
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "required": ["script"],
                "properties": {
                    "script": {
                        "type": "string",
                        "minLength": 1,
                        "description": "Rhai script run once per document. The document is bound as `doc` (id, title, content, fields, and read-only meta); return false to drop it."
                    },
                    "on_error": {
                        "type": "string",
                        "enum": ["fail", "skip", "keep"],
                        "default": "fail",
                        "description": "What a per-document script error does: fail the step, skip that document, or keep it untouched."
                    },
                    "max_ops": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 100000,
                        "description": "Rhai operations one document may use before the script is aborted."
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 1000,
                        "description": "Wall-clock milliseconds one document may use before the script is aborted."
                    }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg = Config::parse(config)?;
        let docs = input.into_documents()?;
        let runner = Runner::new(&cfg)?;

        let mut out = Vec::with_capacity(docs.len());
        for (i, mut doc) in docs.into_iter().enumerate() {
            if i % HEARTBEAT_EVERY == 0 {
                ctx.check_cancelled()?;
                ctx.heartbeat(format!("document {i}"));
            }
            match runner.run(&mut doc) {
                Ok(Verdict::Keep) => out.push(doc),
                Ok(Verdict::Drop) => {}
                Err(e) => match cfg.on_error {
                    OnError::Fail => {
                        return Err(PluginError::NonRetryable(format!(
                            "{NAME}: document `{}`: {e}",
                            doc.id
                        )));
                    }
                    OnError::Skip => {
                        tracing::warn!(document = %doc.id, error = %e, "{NAME}: dropping document");
                    }
                    OnError::Keep => {
                        tracing::warn!(document = %doc.id, error = %e, "{NAME}: keeping document untouched");
                        out.push(doc);
                    }
                },
            }
        }
        Ok(PluginOutput::Documents(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::runner::Runner;
    use serde_json::{Value, json};
    use std::sync::atomic::Ordering;

    fn doc(id: &str, content: &str) -> Document {
        Document::with_id(id, content)
    }

    async fn run(docs: Vec<Document>, config: Value) -> Vec<Document> {
        match DocumentScriptPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(docs),
                config,
            )
            .await
            .unwrap()
        {
            PluginOutput::Documents(d) => d,
            other => panic!("expected documents, got {other:?}"),
        }
    }

    async fn try_run(docs: Vec<Document>, config: Value) -> Result<Vec<Document>, PluginError> {
        match DocumentScriptPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(docs),
                config,
            )
            .await?
        {
            PluginOutput::Documents(d) => Ok(d),
            other => panic!("expected documents, got {other:?}"),
        }
    }

    fn script(src: &str) -> Value {
        json!({ "script": src })
    }

    #[tokio::test]
    async fn renames_a_field() {
        let mut d = doc("a", "hello");
        d.fields.insert("prix".into(), json!(10));

        let out = run(
            vec![d],
            script("doc.fields.price = doc.fields.prix; doc.fields.remove(\"prix\");"),
        )
        .await;

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields.get("price"), Some(&json!(10)));
        assert!(!out[0].fields.contains_key("prix"));
    }

    #[tokio::test]
    async fn drops_several_fields() {
        let mut d = doc("a", "hello");
        d.fields.insert("keep".into(), json!("yes"));
        d.fields.insert("raw_html".into(), json!("<p>x</p>"));
        d.fields.insert("_internal_ref".into(), json!(7));

        let out = run(
            vec![d],
            script(r#"for k in ["raw_html", "_internal_ref"] { doc.fields.remove(k); }"#),
        )
        .await;

        assert_eq!(out[0].fields.len(), 1);
        assert_eq!(out[0].fields.get("keep"), Some(&json!("yes")));
    }

    #[tokio::test]
    async fn computes_a_field_from_integer_and_float_fields() {
        let mut d = doc("a", "hello");
        d.fields.insert("price".into(), json!(10));
        d.fields.insert("rate".into(), json!(1.2));

        let out = run(
            vec![d],
            script("doc.fields.total = doc.fields.price * doc.fields.rate;"),
        )
        .await;

        let total = out[0].fields.get("total").unwrap().as_f64().unwrap();
        assert!((total - 12.0).abs() < 1e-9, "got {total}");
    }

    #[tokio::test]
    async fn promotes_fields_into_title_and_content() {
        let mut d = doc("a", "");
        d.fields.insert("name".into(), json!("Widget"));
        d.fields.insert("body".into(), json!("a useful widget"));

        let out = run(
            vec![d],
            script("doc.title = doc.fields.name; doc.content = doc.fields.body;"),
        )
        .await;

        assert_eq!(out[0].title.as_deref(), Some("Widget"));
        assert_eq!(out[0].content, "a useful widget");
    }

    #[tokio::test]
    async fn clearing_the_title_leaves_no_title() {
        let mut d = doc("a", "hello");
        d.title = Some("old".into());

        let out = run(vec![d], script("doc.title = ();")).await;

        assert_eq!(out[0].title, None);
    }

    #[tokio::test]
    async fn returning_false_drops_the_document() {
        let out = run(
            vec![doc("a", "   "), doc("b", "kept")],
            script(r#"doc.content.trim(); if doc.content.is_empty() { return false; } true"#),
        )
        .await;

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "b");
    }

    /// Rhai's `trim` takes `&mut self` and returns `()`, so it edits the string in
    /// place rather than returning a trimmed copy. Authors reach for
    /// `doc.content.trim() == ""` and silently get `false`; this pins the behaviour
    /// the docs describe instead.
    #[tokio::test]
    async fn trim_edits_the_string_in_place() {
        let out = run(vec![doc("a", "  padded  ")], script("doc.content.trim();")).await;

        assert_eq!(out[0].content, "padded");
    }

    #[tokio::test]
    async fn a_script_with_no_return_value_keeps_the_document() {
        let out = run(vec![doc("a", "hello")], script("doc.fields.x = 1;")).await;

        assert_eq!(out.len(), 1);
    }

    #[tokio::test]
    async fn a_written_id_is_sanitised() {
        let out = run(vec![doc("a", "hello")], script(r#"doc.id = "a/b c";"#)).await;

        assert_eq!(out[0].id, "a_b_c");
    }

    #[tokio::test]
    async fn meta_is_readable_but_writes_are_discarded() {
        let mut d = doc("a", "hello");
        d.meta.source = Some("upload.csv".into());

        let out = run(
            vec![d],
            script(r#"doc.fields.came_from = doc.meta.source; doc.meta.source = "rewritten";"#),
        )
        .await;

        assert_eq!(out[0].fields.get("came_from"), Some(&json!("upload.csv")));
        assert_eq!(out[0].meta.source.as_deref(), Some("upload.csv"));
    }

    #[tokio::test]
    async fn a_script_that_does_not_compile_is_a_config_error() {
        let err = try_run(vec![doc("a", "hello")], script("doc.fields.x = ;"))
            .await
            .unwrap_err();

        assert!(
            matches!(&err, PluginError::InvalidConfig(m) if m.contains("does not compile")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn an_empty_script_is_a_config_error() {
        let err = try_run(vec![doc("a", "hello")], script("   "))
            .await
            .unwrap_err();

        assert!(
            matches!(&err, PluginError::InvalidConfig(m) if m.contains("must not be empty")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn a_missing_script_is_a_config_error() {
        let err = try_run(vec![doc("a", "hello")], json!({}))
            .await
            .unwrap_err();

        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn an_unknown_config_key_is_a_config_error() {
        let err = try_run(vec![doc("a", "hello")], json!({"script": "1", "nope": 1}))
            .await
            .unwrap_err();

        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn on_error_fail_names_the_document() {
        let err = try_run(vec![doc("bad-one", "hello")], script("1 / 0"))
            .await
            .unwrap_err();

        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("bad-one")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn on_error_skip_drops_only_the_failing_document() {
        let mut good = doc("good", "hello");
        good.fields.insert("n".into(), json!(2));
        let bad = doc("bad", "hello");

        let out = try_run(
            vec![bad, good],
            json!({"script": "doc.fields.half = 10 / doc.fields.n;", "on_error": "skip"}),
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "good");
    }

    #[tokio::test]
    async fn on_error_keep_passes_the_failing_document_through_untouched() {
        let mut bad = doc("bad", "hello");
        bad.fields.insert("original".into(), json!(true));

        let out = try_run(
            vec![bad],
            json!({"script": "doc.fields.boom = 1 / 0;", "on_error": "keep"}),
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].fields.get("original"), Some(&json!(true)));
        assert!(!out[0].fields.contains_key("boom"));
    }

    #[tokio::test]
    async fn an_endless_loop_is_stopped_by_the_operation_budget() {
        let started = std::time::Instant::now();
        let err = try_run(
            vec![doc("a", "hello")],
            json!({"script": "let i = 0; loop { i += 1; }", "max_ops": 5000, "timeout_ms": 600000}),
        )
        .await
        .unwrap_err();

        // The wall-clock budget is ten minutes here, so only the operation budget
        // can have stopped this — in both the message and the elapsed time.
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("Too many operations")),
            "got {err:?}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(10));
    }

    #[tokio::test]
    async fn an_endless_loop_is_stopped_by_the_wall_clock_budget() {
        let started = std::time::Instant::now();
        let err = try_run(
            vec![doc("a", "hello")],
            json!({"script": "let i = 0; loop { i += 1; }", "max_ops": 4000000000u64, "timeout_ms": 50}),
        )
        .await
        .unwrap_err();

        // Four billion operations would take far longer than this test allows, so
        // only the deadline can have stopped it.
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("Script terminated")),
            "got {err:?}"
        );
        assert!(
            started.elapsed() < std::time::Duration::from_secs(10),
            "took {:?}, the deadline did not fire",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn each_document_gets_a_fresh_deadline() {
        let runner =
            Runner::new(&Config::parse(json!({"script": "1", "timeout_ms": 50})).unwrap()).unwrap();

        runner.arm_deadline();
        let first = runner.deadline.load(Ordering::Relaxed);
        std::thread::sleep(std::time::Duration::from_millis(20));
        runner.arm_deadline();
        let second = runner.deadline.load(Ordering::Relaxed);

        assert!(
            second > first,
            "arming again must move the deadline forward, {first} -> {second}"
        );
    }

    #[tokio::test]
    async fn a_slow_document_does_not_eat_the_next_one_s_budget() {
        // Each document burns most of its own budget. With one shared deadline the
        // second document would be terminated; with a fresh one per document both
        // finish.
        let out = try_run(
            vec![doc("a", "hello"), doc("b", "hello")],
            json!({
                "script": "let i = 0; while i < 200000 { i += 1; } doc.fields.n = i;",
                "timeout_ms": 10000,
                "max_ops": 4000000000u64
            }),
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 2);
        assert_eq!(out[1].fields.get("n"), Some(&json!(200000)));
    }

    #[tokio::test]
    async fn scripts_cannot_import_modules() {
        let err = try_run(vec![doc("a", "hello")], script(r#"import "std" as s;"#))
            .await
            .unwrap_err();

        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn many_input_is_flattened() {
        let out = match DocumentScriptPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Many(vec![
                    PluginOutput::Documents(vec![doc("a", "hello")]),
                    PluginOutput::Documents(vec![doc("b", "world")]),
                ]),
                script("doc.fields.seen = true;"),
            )
            .await
            .unwrap()
        {
            PluginOutput::Documents(d) => d,
            other => panic!("expected documents, got {other:?}"),
        };

        assert_eq!(out.len(), 2);
        assert!(
            out.iter()
                .all(|d| d.fields.get("seen") == Some(&json!(true)))
        );
    }

    /// A float that JSON cannot represent must not reach the index. Rhai produces
    /// NaN and infinities from ordinary float arithmetic (`x / 0.0`), and
    /// `serde_json` turns every one of them into `null` — so without this check a
    /// divide-by-zero silently writes a null field instead of failing.
    #[tokio::test]
    async fn a_non_finite_number_is_an_error_not_a_null() {
        for expr in ["0.0/0.0", "1.0/0.0", "-1.0/0.0"] {
            let err = try_run(
                vec![doc("a", "x")],
                script(&format!("doc.fields.v = {expr};")),
            )
            .await
            .unwrap_err();

            assert!(
                matches!(&err, PluginError::NonRetryable(m) if m.contains("not a finite number")),
                "{expr} gave {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn a_non_finite_number_nested_in_a_field_is_caught() {
        let err = try_run(
            vec![doc("a", "x")],
            script("doc.fields.stats = #{ ratios: [1.0, 1.0/0.0] };"),
        )
        .await
        .unwrap_err();

        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("not a finite number")),
            "got {err:?}"
        );
    }

    #[tokio::test]
    async fn ordinary_floats_still_round_trip() {
        let out = run(vec![doc("a", "x")], script("doc.fields.v = 10.0 / 4.0;")).await;

        assert_eq!(out[0].fields.get("v"), Some(&json!(2.5)));
    }

    #[tokio::test]
    async fn manifest_is_correct() {
        let m = DocumentScriptPlugin.manifest();

        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Documents, InputKind::Many]);
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(!m.description.is_empty());
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in ["script", "on_error", "max_ops", "timeout_ms"] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
        assert_eq!(
            m.config_schema["required"],
            json!(["script"]),
            "script must be required"
        );
    }
}
