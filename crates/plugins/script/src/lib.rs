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
//! with, so a script can only reach `doc`. `print` and `debug` are accepted but
//! their output is discarded, so a script cannot write to the worker's logs. On top
//! of that every run is bounded by an
//! operation budget (`max_ops`), a wall-clock budget (`timeout_ms`) and fixed caps on
//! expression depth, call depth, string, array and map sizes.
//!
//! Configuration:
//!
//! | key | default | meaning |
//! |---|---|---|
//! | `script` | required | the rhai script, run once per document |
//! | `on_error` | `fail` | what a per-document error does: `fail`, `skip` or `keep` |
//! | `max_ops` | `100000` | rhai operations one document may use, at most 100,000,000 |
//! | `timeout_ms` | `1000` | wall-clock milliseconds one document may use, at most 30,000 |
//!
//! Larger budgets are clamped, not rejected. The whole step also has a one-hour
//! budget; running past it, or being cancelled, stops the script that is running
//! and fails the step whatever `on_error` says. Scripts run on a blocking thread,
//! never on the async runtime.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

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
/// String bytes a script may build on top of the document it was given.
const MAX_STRING_SIZE: usize = 512 * 1024;
/// Array elements a script may build on top of the document it was given.
const MAX_ARRAY_SIZE: usize = 10_000;
/// Map entries a script may build on top of the document it was given.
const MAX_MAP_SIZE: usize = 10_000;
/// Operations between two wall-clock checks in `on_progress`.
const PROGRESS_EVERY: u64 = 1_000;
/// Highest `max_ops` honoured; larger values are clamped to it.
const MAX_OPS_CEILING: u64 = 100_000_000;
/// Highest `timeout_ms` honoured; larger values are clamped to it.
const MAX_TIMEOUT_MS: u64 = 30_000;
/// Wall-clock budget of the whole step, across all its documents. A backstop: the
/// per-document budgets alone still allow documents x `timeout_ms`.
const STEP_BUDGET: Duration = Duration::from_secs(60 * 60);

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
///
/// Three things stop a running script from the same closure: the document's own
/// deadline, the whole step's budget, and the activity's cancellation flag. The
/// closure is the only place that can, because the script runs on a blocking thread
/// that cannot be aborted from outside.
struct Runner {
    engine: Engine,
    ast: AST,
    /// Start of the step; both the deadline and the progress closure measure from it.
    base: Instant,
    /// Nanoseconds after `base` at which the current document must stop.
    deadline: Arc<AtomicU64>,
    /// Per-document wall-clock budget, after clamping to [`MAX_TIMEOUT_MS`].
    timeout_ms: u64,
    /// The whole step's wall-clock budget, measured from `base`.
    step_budget: Duration,
    /// The activity's cancellation flag.
    cancelled: Arc<AtomicBool>,
}

impl Runner {
    /// Build the sandboxed engine and compile the script, with the default step
    /// budget and a cancellation flag nothing sets.
    #[cfg(test)]
    fn new(cfg: &Config) -> Result<Self, PluginError> {
        Self::build(cfg, STEP_BUDGET, Arc::new(AtomicBool::new(false)))
    }

    /// [`Runner::new`] with another step budget.
    #[cfg(test)]
    fn with_step_budget(cfg: &Config, step_budget: Duration) -> Result<Self, PluginError> {
        Self::build(cfg, step_budget, Arc::new(AtomicBool::new(false)))
    }

    /// Build the sandboxed engine and compile the script.
    ///
    /// `max_ops` and `timeout_ms` are tenant-authored, so they are clamped to
    /// [`MAX_OPS_CEILING`] and [`MAX_TIMEOUT_MS`] rather than rejected: a pipeline
    /// saved with a larger value keeps working, within the server's bounds.
    ///
    /// A syntax error surfaces here — before any document is touched — so it fails
    /// the step as `InvalidConfig` whatever `on_error` says.
    fn build(
        cfg: &Config,
        step_budget: Duration,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, PluginError> {
        let max_ops = cfg.max_ops.min(MAX_OPS_CEILING);
        let timeout_ms = cfg.timeout_ms.min(MAX_TIMEOUT_MS);

        let mut engine = Engine::new();
        // Rhai's default handlers `println!` the script's text onto the worker's
        // stdout, next to its logs. Discard it rather than disable the symbols, so
        // scripts that call them keep compiling.
        engine.on_print(|_| {});
        engine.on_debug(|_, _, _| {});
        engine.set_max_operations(max_ops);
        engine.set_max_expr_depths(MAX_EXPR_DEPTH, MAX_EXPR_DEPTH);
        engine.set_max_call_levels(MAX_CALL_LEVELS);
        engine.set_max_string_size(MAX_STRING_SIZE);
        engine.set_max_array_size(MAX_ARRAY_SIZE);
        engine.set_max_map_size(MAX_MAP_SIZE);

        let base = Instant::now();
        let deadline = Arc::new(AtomicU64::new(u64::MAX));
        let watch = Arc::clone(&deadline);
        let stop = Arc::clone(&cancelled);
        let step_deadline = nanos(step_budget);
        engine.on_progress(move |ops| {
            if ops % PROGRESS_EVERY != 0 {
                return None;
            }
            let elapsed = nanos(base.elapsed());
            if elapsed >= watch.load(Ordering::Relaxed).min(step_deadline)
                || stop.load(Ordering::Relaxed)
            {
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
            timeout_ms,
            step_budget,
            cancelled,
        })
    }

    /// Fail the step once it is cancelled or past its whole-step budget.
    ///
    /// Checked between documents and after every per-document error, so that a
    /// script aborted for one of these reasons fails the step instead of going
    /// through `on_error` (which would skip or keep every remaining document).
    fn check_step(&self) -> Result<(), PluginError> {
        if self.cancelled.load(Ordering::Relaxed) {
            return Err(PluginError::Cancelled);
        }
        if self.base.elapsed() >= self.step_budget {
            return Err(PluginError::NonRetryable(format!(
                "{NAME}: the step ran past its step budget of {}s",
                self.step_budget.as_secs()
            )));
        }
        Ok(())
    }

    /// Give the next document a fresh wall-clock budget.
    fn arm_deadline(&self) {
        let now = nanos(self.base.elapsed());
        let budget = self.timeout_ms.saturating_mul(1_000_000);
        self.deadline
            .store(now.saturating_add(budget), Ordering::Relaxed);
    }

    /// Size the string, array and map caps for one document.
    ///
    /// Rhai checks the whole container a value lives in, not what the script added:
    /// after `doc.title = …` it sums every string, array element and map entry in
    /// `doc`, and after `doc.content.trim()` it measures all of `content`. Fixed caps
    /// would fail scripts on large documents they only received, so each cap is the
    /// document's own size plus the fixed headroom. The headroom is added, not
    /// multiplied, so a script on a large document can build no more than one on a
    /// small document.
    fn size_limits_for(&mut self, map: &Map) {
        let (arrays, maps, strings) = map_sizes(map);
        self.engine
            .set_max_string_size(strings.saturating_add(MAX_STRING_SIZE));
        self.engine
            .set_max_array_size(arrays.saturating_add(MAX_ARRAY_SIZE));
        self.engine
            .set_max_map_size(maps.saturating_add(MAX_MAP_SIZE));
    }

    /// Run the script over one document, mutating it in place.
    fn run(&mut self, doc: &mut Document) -> Result<Verdict, PluginError> {
        let map = to_scope_map(doc)?;
        self.size_limits_for(&map);
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

/// A duration in nanoseconds, saturating at `u64::MAX` (about 584 years).
fn nanos(d: Duration) -> u64 {
    d.as_nanos().min(u128::from(u64::MAX)) as u64
}

/// Run the script over every document, applying `on_error`.
///
/// Synchronous: [`DocumentScriptPlugin::execute`] runs it on a blocking thread so
/// the script never holds an async runtime thread, and heartbeats and cancellation
/// are handled here, between and inside documents.
fn run_all(
    mut runner: Runner,
    docs: Vec<Document>,
    on_error: OnError,
    ctx: &ActivityContext,
) -> Result<Vec<Document>, PluginError> {
    let mut out = Vec::with_capacity(docs.len());
    for (i, mut doc) in docs.into_iter().enumerate() {
        runner.check_step()?;
        if i % HEARTBEAT_EVERY == 0 {
            ctx.heartbeat(format!("document {i}"));
        }
        match runner.run(&mut doc) {
            Ok(Verdict::Keep) => out.push(doc),
            Ok(Verdict::Drop) => {}
            Err(e) => {
                // A script stopped by a cancel or the step budget is not this
                // document's fault.
                runner.check_step()?;
                match on_error {
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
                }
            }
        }
    }
    Ok(out)
}

/// Sizes of a map as rhai's data-size check counts them: `(array elements, map
/// entries, string bytes)`, summed over every nested container.
fn map_sizes(map: &Map) -> (usize, usize, usize) {
    let mut total = (0, 0, 0);
    for value in map.values() {
        total.1 += 1;
        add_sizes(&mut total, value);
    }
    total
}

/// Add the sizes of the containers and strings inside `value` to `total`, the way
/// rhai's `calc_data_sizes` does for an element of a map or an array.
fn add_sizes(total: &mut (usize, usize, usize), value: &Dynamic) {
    if let Ok(s) = value.as_immutable_string_ref() {
        total.2 += s.len();
    } else if let Ok(m) = value.as_map_ref() {
        let (a, m, s) = map_sizes(&m);
        total.0 += a;
        total.1 += m;
        total.2 += s;
    } else if let Ok(array) = value.as_array_ref() {
        for v in array.iter() {
            total.0 += 1;
            add_sizes(total, v);
        }
    } else if let Ok(blob) = value.as_blob_ref() {
        total.0 += 1 + blob.len();
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

/// Reject floats JSON cannot represent, before they are silently turned into `null`.
///
/// Rhai produces NaN and infinities from ordinary float arithmetic — `x / 0.0` does
/// not raise, unlike integer division — and `serde_json` maps every non-finite float
/// onto `null`. Left alone that writes a null field into the index and reports
/// success, so the conversion refuses instead and the step's `on_error` policy
/// decides what happens to the document.
fn reject_non_finite(value: &Dynamic, path: &str) -> Result<(), PluginError> {
    if value.is_float() {
        let f = value.as_float().unwrap_or(0.0);
        if !f.is_finite() {
            return Err(PluginError::NonRetryable(format!(
                "{NAME}: `{path}` is not a finite number ({f})"
            )));
        }
    } else if let Some(map) = value.read_lock::<Map>() {
        for (k, v) in map.iter() {
            reject_non_finite(v, &format!("{path}.{k}"))?;
        }
    } else if let Some(array) = value.read_lock::<rhai::Array>() {
        for (i, v) in array.iter().enumerate() {
            reject_non_finite(v, &format!("{path}[{i}]"))?;
        }
    }
    Ok(())
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
///
/// Every value is converted before any is written, so a failure leaves `doc` exactly
/// as it was: `on_error: keep` promises the document untouched.
fn from_scope_map(map: &Map, doc: &mut Document) -> Result<(), PluginError> {
    for key in ["id", "title", "content", "fields"] {
        if let Some(v) = map.get(key) {
            reject_non_finite(v, &format!("doc.{key}"))?;
        }
    }
    let id = match map.get("id") {
        Some(id) => Some(match dynamic_to_json(id)? {
            Value::String(s) => sanitize_id(&s),
            other => sanitize_id(&other.to_string()),
        }),
        None => None,
    };
    let title = match map.get("title") {
        None => None,
        Some(t) if t.is_unit() => None,
        Some(t) => Some(match dynamic_to_json(t)? {
            Value::String(s) => s,
            other => other.to_string(),
        }),
    };
    let content = match map.get("content") {
        Some(content) => Some(match dynamic_to_json(content)? {
            Value::String(s) => s,
            Value::Null => String::new(),
            other => other.to_string(),
        }),
        None => None,
    };
    let fields = match map.get("fields") {
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

    let mut fields = fields;
    for (key, value) in fields.iter_mut() {
        if let Some(original) = doc.fields.get(key) {
            restore_large_integers(value, original);
        }
    }

    if let Some(id) = id {
        doc.id = id;
    }
    doc.title = title;
    if let Some(content) = content {
        doc.content = content;
    }
    doc.fields = fields;
    Ok(())
}

/// Put back the integers above `i64::MAX` the script left alone.
///
/// Rhai integers are i64, so such a u64 reaches the script as a float and would be
/// written back as one (`18446744073709551615` becoming `1.8446744073709552e19`)
/// even when the script never touched it. Wherever the original held such an
/// integer at the same path and the result is the float it was converted to, the
/// original number is restored. A value the script moved to another key, or
/// computed with, stays the float rhai produced.
fn restore_large_integers(new: &mut Value, original: &Value) {
    match (new, original) {
        (Value::Number(n), Value::Number(o)) => {
            if let Some(big) = o.as_u64()
                && big > i64::MAX as u64
                && n.is_f64()
                && n.as_f64() == Some(big as f64)
            {
                *n = o.clone();
            }
        }
        (Value::Object(n), Value::Object(o)) => {
            for (key, value) in n.iter_mut() {
                if let Some(orig) = o.get(key) {
                    restore_large_integers(value, orig);
                }
            }
        }
        (Value::Array(n), Value::Array(o)) => {
            for (value, orig) in n.iter_mut().zip(o) {
                restore_large_integers(value, orig);
            }
        }
        _ => {}
    }
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
                        "description": "Rhai operations one document may use before the script is aborted. Values above 100000000 are clamped to it."
                    },
                    "timeout_ms": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 1000,
                        "description": "Wall-clock milliseconds one document may use before the script is aborted. Values above 30000 are clamped to it."
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
        let runner = Runner::build(&cfg, STEP_BUDGET, ctx.cancellation_flag())?;
        ctx.check_cancelled()?;

        let on_error = cfg.on_error;
        let ctx = ctx.clone();
        let out = run_blocking(move || run_all(runner, docs, on_error, &ctx)).await??;
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

    /// The script ran, but reading `doc` back failed after `id`, `title` and
    /// `content` would already have been converted: none of them may stick.
    #[tokio::test]
    async fn on_error_keep_is_untouched_when_the_read_back_fails() {
        let mut bad = doc("a", "hello");
        bad.fields.insert("original".into(), json!(true));

        let out = try_run(
            vec![bad],
            json!({
                "script": r#"doc.id = "other"; doc.title = "New"; doc.content = "changed"; doc.fields = 5;"#,
                "on_error": "keep"
            }),
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 1);
        assert_eq!(out[0].id, "a");
        assert_eq!(out[0].title, None);
        assert_eq!(out[0].content, "hello");
        assert_eq!(out[0].fields.get("original"), Some(&json!(true)));
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

    /// Budgets are tenant-authored: without a ceiling `u64::MAX` for both made a
    /// `loop {}` run for centuries.
    #[test]
    fn budgets_are_clamped_to_server_ceilings() {
        let runner = Runner::new(
            &Config::parse(json!({
                "script": "1",
                "max_ops": u64::MAX,
                "timeout_ms": u64::MAX
            }))
            .unwrap(),
        )
        .unwrap();

        assert_eq!(runner.engine.max_operations(), MAX_OPS_CEILING);
        assert_eq!(runner.timeout_ms, MAX_TIMEOUT_MS);

        // Values under the ceiling are kept as configured.
        let runner = Runner::new(
            &Config::parse(json!({"script": "1", "max_ops": 5000, "timeout_ms": 50})).unwrap(),
        )
        .unwrap();
        assert_eq!(runner.engine.max_operations(), 5000);
        assert_eq!(runner.timeout_ms, 50);
    }

    fn cancellable_context() -> (ActivityContext, Arc<std::sync::atomic::AtomicBool>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        // Keep the receiver alive for the whole test so heartbeats have somewhere to go.
        std::mem::forget(rx);
        let flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let job_id = ActivityContext::noop().job_id();
        let ctx = ActivityContext::new(job_id, "script", 1, tx, Arc::clone(&flag));
        (ctx, flag)
    }

    /// A cancel must stop a script that is still running, not wait for it to use
    /// up its own budget.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancellation_stops_a_running_script() {
        let (ctx, flag) = cancellable_context();
        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            flag.store(true, Ordering::Relaxed);
        });

        let started = std::time::Instant::now();
        let err = DocumentScriptPlugin::new()
            .execute(
                &ctx,
                PluginInput::Documents(vec![doc("a", "x")]),
                json!({
                    "script": "let i = 0; loop { i += 1; }",
                    "max_ops": 4000000000u64,
                    "timeout_ms": 5000,
                    "on_error": "keep"
                }),
            )
            .await
            .unwrap_err();

        assert!(matches!(err, PluginError::Cancelled), "got {err:?}");
        assert!(
            started.elapsed() < std::time::Duration::from_secs(3),
            "took {:?}",
            started.elapsed()
        );
    }

    /// A running script must not hold the async runtime: other tasks on the same
    /// (here single-threaded) runtime keep making progress meanwhile.
    #[tokio::test(flavor = "current_thread")]
    async fn a_running_script_does_not_block_the_runtime() {
        let ticks = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&ticks);
        let ticker = tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                counter.fetch_add(1, Ordering::Relaxed);
            }
        });

        let out = try_run(
            vec![doc("a", "x")],
            json!({
                "script": "let i = 0; loop { i += 1; }",
                "max_ops": 4000000000u64,
                "timeout_ms": 300,
                "on_error": "keep"
            }),
        )
        .await
        .unwrap();
        ticker.abort();

        assert_eq!(out.len(), 1);
        let ticks = ticks.load(Ordering::Relaxed);
        assert!(ticks >= 5, "the runtime only ticked {ticks} times");
    }

    /// A per-document budget alone still lets documents x timeout run; the whole
    /// step has its own, and running out of it fails the step whatever `on_error`
    /// says instead of skipping every remaining document.
    #[test]
    fn the_step_budget_fails_the_step() {
        let cfg = Config::parse(json!({
            "script": "let i = 0; loop { i += 1; }",
            "max_ops": 4000000000u64,
            "timeout_ms": 10000,
            "on_error": "skip"
        }))
        .unwrap();
        let runner = Runner::with_step_budget(&cfg, std::time::Duration::from_millis(200)).unwrap();

        let started = std::time::Instant::now();
        let err = run_all(
            runner,
            vec![doc("a", "x"), doc("b", "x"), doc("c", "x")],
            cfg.on_error,
            &ActivityContext::noop(),
        )
        .unwrap_err();

        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("step budget")),
            "got {err:?}"
        );
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
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

    /// Set in the child process [`print_and_debug_do_not_reach_stdout`] spawns.
    const PRINT_CHILD_ENV: &str = "DOCUMENT_SCRIPT_PRINT_CHILD";
    const PRINT_MARKER: &str = "forged-log-line-7f3a";

    /// Runs only inside the child process: a script that prints and debugs.
    #[tokio::test]
    async fn print_and_debug_child() {
        if std::env::var_os(PRINT_CHILD_ENV).is_none() {
            return;
        }
        let out = run(
            vec![doc("a", "x")],
            script(&format!(
                r#"print("{PRINT_MARKER}"); debug("{PRINT_MARKER}"); true"#
            )),
        )
        .await;
        assert_eq!(out.len(), 1);
    }

    /// Tenant scripts must not write to the worker's stdout, where they would
    /// interleave forged lines with the structured logs. Rhai's default `print` and
    /// `debug` handlers `println!`, so this runs the script in a child test process
    /// without output capture and checks what reached its stdout.
    #[test]
    fn print_and_debug_do_not_reach_stdout() {
        let out = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::print_and_debug_child",
                "--nocapture",
                "--test-threads=1",
            ])
            .env(PRINT_CHILD_ENV, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        assert!(out.status.success(), "child failed: {stdout}");
        assert!(
            stdout.contains("1 passed"),
            "the child test did not run: {stdout}"
        );
        assert!(
            !stdout.contains(PRINT_MARKER),
            "script output reached stdout: {stdout}"
        );
    }

    /// Rhai integers are i64, so a u64 above `i64::MAX` reaches the script as a
    /// float. One the script never touched must still come back as the same integer.
    #[tokio::test]
    async fn untouched_integers_above_i64_max_round_trip() {
        let big = json!(18446744073709551615u64);
        let mut d = doc("a", "x");
        d.fields.insert("snowflake".into(), big.clone());
        d.fields.insert(
            "nested".into(),
            json!({"b": big.clone(), "list": [1, big.clone()]}),
        );

        let out = run(
            vec![d],
            script("doc.fields.seen = true; doc.fields.nested.c = 1;"),
        )
        .await;

        assert_eq!(out[0].fields.get("snowflake"), Some(&big));
        assert_eq!(out[0].fields["nested"]["b"], big);
        assert_eq!(out[0].fields["nested"]["list"], json!([1, big.clone()]));
        assert_eq!(out[0].fields["nested"]["c"], json!(1));
        assert_eq!(out[0].fields["seen"], json!(true));
    }

    #[tokio::test]
    async fn a_large_integer_the_script_rewrites_is_not_restored() {
        let mut d = doc("a", "x");
        d.fields.insert("n".into(), json!(18446744073709551615u64));

        let out = run(vec![d], script("doc.fields.n = 2.5;")).await;

        assert_eq!(out[0].fields["n"], json!(2.5));
    }

    /// The size caps bound what a script builds, not the document it was handed:
    /// rhai checks the whole `doc` map after every assignment into it and the whole
    /// string after every method call on it.
    #[tokio::test]
    async fn a_large_document_is_not_rejected_by_the_size_caps() {
        let big = "x".repeat(600 * 1024);
        let mut d = doc("a", &big);
        d.fields.insert("name".into(), json!("Widget"));

        let out = try_run(vec![d.clone()], script("doc.title = doc.fields.name;"))
            .await
            .unwrap();
        assert_eq!(out[0].title.as_deref(), Some("Widget"));
        assert_eq!(out[0].content, big);

        let out = try_run(vec![d], script("doc.content.trim();"))
            .await
            .unwrap();
        assert_eq!(out[0].content, big);

        let mut d = doc("b", "x");
        d.fields
            .insert("links".into(), json!(vec!["l"; MAX_ARRAY_SIZE + 1]));
        let out = try_run(vec![d], script("doc.fields.x = 1;")).await.unwrap();
        assert_eq!(out[0].fields["x"], json!(1));
    }

    #[tokio::test]
    async fn a_script_still_cannot_build_an_oversized_string() {
        let err = try_run(
            vec![doc("a", "x")],
            script("let s = \"xxxxxxxx\"; loop { s += s; }"),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("too large")),
            "got {err:?}"
        );

        // On a large document the headroom is the same fixed cap, not a multiple.
        let err = try_run(
            vec![doc("a", &"x".repeat(600 * 1024))],
            script("doc.content += doc.content;"),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("too large")),
            "got {err:?}"
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
