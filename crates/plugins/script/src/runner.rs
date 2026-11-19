//! The sandboxed engine, the per-step runner and the run loop.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use meili_ingest_plugin_sdk::{ActivityContext, Document, PluginError};
use rhai::{AST, Dynamic, Engine, Map, Scope};

use crate::config::{Config, OnError, Source};
use crate::convert::{from_scope_map, to_scope_map};
use crate::resolver::install_field_resolver;
use crate::{HEARTBEAT_EVERY, NAME};

/// Longest expression nesting a script may use, inside and outside functions.
const MAX_EXPR_DEPTH: usize = 64;
/// Deepest function call chain a script may build (bounds recursion).
const MAX_CALL_LEVELS: usize = 16;
/// String bytes a script may build on top of the document it was given (rhai
/// measures `str::len`). Also the longest string a helper may build.
pub(crate) const MAX_STRING_SIZE: usize = 512 * 1024;
/// Array elements a script may build on top of the document it was given. Also the
/// largest array a helper may build.
pub(crate) const MAX_ARRAY_SIZE: usize = 10_000;
/// Map entries a script may build on top of the document it was given.
const MAX_MAP_SIZE: usize = 10_000;
/// Operations between two wall-clock checks in `on_progress`.
const PROGRESS_EVERY: u64 = 1_000;
/// Wall-clock budget of the whole step, across all its documents. A backstop: the
/// per-document budgets alone still allow documents x `timeout_ms`.
pub(crate) const STEP_BUDGET: Duration = Duration::from_secs(60 * 60);

/// An engine with every sandbox limit and the field resolver installed — everything
/// but the per-document deadline, which only a [`Runner`] needs. Also used to compile
/// `set` expressions and whole configs, so they are checked under the limits they run
/// under.
///
/// `max_ops` is expected already clamped by [`Config`].
pub(crate) fn sandboxed_engine(max_ops: u64) -> Engine {
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
    install_field_resolver(&mut engine);
    crate::helpers::register(&mut engine);
    engine
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
pub(crate) struct Runner {
    pub(crate) engine: Engine,
    ast: AST,
    /// Start of the step; both the deadline and the progress closure measure from it.
    base: Instant,
    /// Nanoseconds after `base` at which the current document must stop.
    pub(crate) deadline: Arc<AtomicU64>,
    /// Per-document wall-clock budget, already clamped by [`Config`].
    pub(crate) timeout_ms: u64,
    /// The whole step's wall-clock budget, measured from `base`.
    step_budget: Duration,
    /// The activity's cancellation flag.
    cancelled: Arc<AtomicBool>,
}

impl Runner {
    /// Build the sandboxed engine and compile the script, with the default step
    /// budget and a cancellation flag nothing sets.
    #[cfg(test)]
    pub(crate) fn new(cfg: &Config) -> Result<Self, PluginError> {
        Self::build(cfg, STEP_BUDGET, Arc::new(AtomicBool::new(false)))
    }

    /// [`Runner::new`] with another step budget.
    #[cfg(test)]
    pub(crate) fn with_step_budget(
        cfg: &Config,
        step_budget: Duration,
    ) -> Result<Self, PluginError> {
        Self::build(cfg, step_budget, Arc::new(AtomicBool::new(false)))
    }

    /// Build the sandboxed engine and compile the script or the program generated
    /// from `ops`.
    ///
    /// A syntax error surfaces here — before any document is touched — so it fails
    /// the step as `InvalidConfig` whatever `on_error` says.
    pub(crate) fn build(
        cfg: &Config,
        step_budget: Duration,
        cancelled: Arc<AtomicBool>,
    ) -> Result<Self, PluginError> {
        let mut engine = sandboxed_engine(cfg.max_ops);

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

        let source = match &cfg.source {
            Source::Script(script) => script.clone(),
            Source::Ops(ops) => crate::codegen::generate(ops),
        };
        let ast = engine.compile(&source).map_err(|e| {
            PluginError::InvalidConfig(format!("{NAME}: script does not compile: {e}"))
        })?;

        Ok(Self {
            engine,
            ast,
            base,
            deadline,
            timeout_ms: cfg.timeout_ms,
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
    pub(crate) fn arm_deadline(&self) {
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
    pub(crate) fn run(&mut self, doc: &mut Document) -> Result<Verdict, PluginError> {
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
/// Synchronous: [`crate::DocumentScriptPlugin`]'s `execute` runs it on a blocking
/// thread so the script never holds an async runtime thread, and heartbeats and
/// cancellation are handled here, between and inside documents.
pub(crate) fn run_all(
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

/// What the script decided about one document.
pub(crate) enum Verdict {
    /// Keep it, with the script's mutations applied.
    Keep,
    /// Drop it: the script returned `false`.
    Drop,
}
