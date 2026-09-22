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

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use meili_ingest_plugin_sdk::prelude::*;
use rhai::{AST, Dynamic, Engine, Map, Scope};
use serde::Deserialize;
use serde_json::Value;

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "document_script";

/// Documents between two heartbeats.
const HEARTBEAT_EVERY: usize = 10;

/// Longest expression nesting a script may use, inside and outside functions.
const MAX_EXPR_DEPTH: usize = 64;
/// Deepest function call chain a script may build (bounds recursion).
const MAX_CALL_LEVELS: usize = 16;
/// Longest string a script may build, in characters.
const MAX_STRING_SIZE: usize = 512 * 1024;
/// Largest array a script may build.
const MAX_ARRAY_SIZE: usize = 10_000;
/// Largest map a script may build.
const MAX_MAP_SIZE: usize = 10_000;
/// Operations between two wall-clock checks in `on_progress`.
const PROGRESS_EVERY: u64 = 1_000;

/// The document script plugin. Stateless; construct with [`DocumentScriptPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct DocumentScriptPlugin;

impl DocumentScriptPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// What a per-document script error does to that document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
enum OnError {
    /// Fail the step, naming the document and the rhai error.
    #[default]
    Fail,
    /// Drop the document and carry on.
    Skip,
    /// Emit the document untouched and carry on.
    Keep,
}

/// Step configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    /// The rhai script, run once per document.
    script: String,
    /// What a per-document error does.
    #[serde(default)]
    on_error: OnError,
    /// Rhai operations one document may use.
    #[serde(default = "default_max_ops")]
    max_ops: u64,
    /// Wall-clock milliseconds one document may use.
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

fn default_max_ops() -> u64 {
    100_000
}

fn default_timeout_ms() -> u64 {
    1_000
}

impl Config {
    /// Validate a raw `config:` block. `script` is required, so `null` is an error.
    fn parse(value: Value) -> Result<Self, PluginError> {
        let cfg: Config = serde_json::from_value(value)
            .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))?;
        if cfg.script.trim().is_empty() {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `script` must not be empty"
            )));
        }
        if cfg.max_ops == 0 {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `max_ops` must be at least 1"
            )));
        }
        if cfg.timeout_ms == 0 {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `timeout_ms` must be at least 1"
            )));
        }
        Ok(cfg)
    }
}

/// One step's compiled script and the engine that runs it.
///
/// The engine is built once and reused for every document: compiling and installing
/// the sandbox per document would dominate the cost of a small script. Rhai's only
/// abort hook is `on_progress`, and an `Engine` cannot be cloned, so the per-document
/// wall-clock budget is carried in a shared deadline the closure reads and
/// [`Runner::run`] rewrites before each document.
struct Runner {
    engine: Engine,
    ast: AST,
    /// Start of the step; both the deadline and the progress closure measure from it.
    base: Instant,
    /// Nanoseconds after `base` at which the current document must stop.
    deadline: Arc<AtomicU64>,
    timeout_ms: u64,
}

impl Runner {
    /// Build the sandboxed engine and compile the script.
    ///
    /// A syntax error surfaces here — before any document is touched — so it fails
    /// the step as `InvalidConfig` whatever `on_error` says.
    fn new(cfg: &Config) -> Result<Self, PluginError> {
        let mut engine = Engine::new();
        engine.set_max_operations(cfg.max_ops);
        engine.set_max_expr_depths(MAX_EXPR_DEPTH, MAX_EXPR_DEPTH);
        engine.set_max_call_levels(MAX_CALL_LEVELS);
        engine.set_max_string_size(MAX_STRING_SIZE);
        engine.set_max_array_size(MAX_ARRAY_SIZE);
        engine.set_max_map_size(MAX_MAP_SIZE);

        let base = Instant::now();
        let deadline = Arc::new(AtomicU64::new(u64::MAX));
        let watch = Arc::clone(&deadline);
        engine.on_progress(move |ops| {
            if ops % PROGRESS_EVERY != 0 {
                return None;
            }
            let elapsed = base.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
            if elapsed >= watch.load(Ordering::Relaxed) {
                // Any `Some` aborts the script; the value is discarded by rhai.
                Some(Dynamic::UNIT)
            } else {
                None
            }
        });

        let ast = engine.compile(&cfg.script).map_err(|e| {
            PluginError::InvalidConfig(format!("{NAME}: script does not compile: {e}"))
        })?;

        Ok(Self {
            engine,
            ast,
            base,
            deadline,
            timeout_ms: cfg.timeout_ms,
        })
    }

    /// Give the next document a fresh wall-clock budget.
    fn arm_deadline(&self) {
        let now = self.base.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        let budget = self.timeout_ms.saturating_mul(1_000_000);
        self.deadline
            .store(now.saturating_add(budget), Ordering::Relaxed);
    }

    /// Run the script over one document, mutating it in place.
    fn run(&self, doc: &mut Document) -> Result<Verdict, PluginError> {
        let map = to_scope_map(doc)?;
        let mut scope = Scope::new();
        scope.push("doc", map);

        self.arm_deadline();
        let result: Dynamic = self
            .engine
            .eval_ast_with_scope(&mut scope, &self.ast)
            .map_err(|e| PluginError::InvalidInput(format!("{e}")))?;

        let map = scope.get_value::<Map>("doc").ok_or_else(|| {
            PluginError::InvalidInput(format!(
                "{NAME}: the script replaced `doc` with a non-map value"
            ))
        })?;
        from_scope_map(&map, doc)?;

        if result.as_bool() == Ok(false) {
            return Ok(Verdict::Drop);
        }
        Ok(Verdict::Keep)
    }
}

/// Bind a document as the rhai map the script mutates.
///
/// Built key by key rather than through `serde`: `Document` skips empty `title` and
/// `fields` when it serialises, and a script that cannot count on `doc.fields`
/// existing is unusable.
fn to_scope_map(doc: &Document) -> Result<Map, PluginError> {
    let mut map = Map::new();
    map.insert("id".into(), Dynamic::from(doc.id.clone()));
    map.insert(
        "title".into(),
        match &doc.title {
            Some(t) => Dynamic::from(t.clone()),
            None => Dynamic::UNIT,
        },
    );
    map.insert("content".into(), Dynamic::from(doc.content.clone()));
    map.insert(
        "fields".into(),
        json_to_dynamic(&Value::Object(doc.fields.clone()))?,
    );
    map.insert(
        "meta".into(),
        json_to_dynamic(
            &serde_json::to_value(&doc.meta).map_err(|e| {
                PluginError::NonRetryable(format!("{NAME}: cannot expose meta: {e}"))
            })?,
        )?,
    );
    Ok(map)
}

/// Convert JSON into rhai values.
fn json_to_dynamic(value: &Value) -> Result<Dynamic, PluginError> {
    rhai::serde::to_dynamic(value).map_err(|e| {
        PluginError::NonRetryable(format!("{NAME}: cannot convert to script value: {e}"))
    })
}

/// Convert rhai values back into JSON.
fn dynamic_to_json(value: &Dynamic) -> Result<Value, PluginError> {
    rhai::serde::from_dynamic(value).map_err(|e| {
        PluginError::InvalidInput(format!(
            "{NAME}: script produced a value that is not valid JSON: {e}"
        ))
    })
}

/// Read the mutated map back onto the document.
///
/// `meta` is deliberately not read back: it is provenance the pipeline owns, so the
/// script sees it but cannot rewrite it.
fn from_scope_map(map: &Map, doc: &mut Document) -> Result<(), PluginError> {
    if let Some(id) = map.get("id") {
        let id = match dynamic_to_json(id)? {
            Value::String(s) => s,
            other => other.to_string(),
        };
        doc.id = sanitize_id(&id);
    }
    doc.title = match map.get("title") {
        None => None,
        Some(t) if t.is_unit() => None,
        Some(t) => Some(match dynamic_to_json(t)? {
            Value::String(s) => s,
            other => other.to_string(),
        }),
    };
    if let Some(content) = map.get("content") {
        doc.content = match dynamic_to_json(content)? {
            Value::String(s) => s,
            Value::Null => String::new(),
            other => other.to_string(),
        };
    }
    doc.fields = match map.get("fields") {
        Some(f) => match dynamic_to_json(f)? {
            Value::Object(o) => o,
            other => {
                return Err(PluginError::InvalidInput(format!(
                    "{NAME}: `doc.fields` must stay a map, got {other}"
                )));
            }
        },
        None => Default::default(),
    };
    Ok(())
}

/// What the script decided about one document.
enum Verdict {
    /// Keep it, with the script's mutations applied.
    Keep,
    /// Drop it: the script returned `false`.
    Drop,
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
    use serde_json::{Value, json};

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
