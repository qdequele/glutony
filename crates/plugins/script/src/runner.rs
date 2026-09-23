//! The sandboxed engine and the per-step runner.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use meili_ingest_plugin_sdk::{Document, PluginError};
use rhai::{AST, Dynamic, Engine, Map, Scope};

use crate::NAME;
use crate::config::Config;
use crate::convert::{from_scope_map, to_scope_map};
use crate::resolver::install_field_resolver;

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

/// An engine with every sandbox limit and the field resolver installed — everything
/// but the per-document deadline, which only a [`Runner`] needs. Also used to compile
/// `set` expressions and whole configs, so they are checked under the limits they run
/// under.
pub(crate) fn sandboxed_engine(max_ops: u64) -> Engine {
    let mut engine = Engine::new();
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
pub(crate) struct Runner {
    engine: Engine,
    ast: AST,
    /// Start of the step; both the deadline and the progress closure measure from it.
    base: Instant,
    /// Nanoseconds after `base` at which the current document must stop.
    pub(crate) deadline: Arc<AtomicU64>,
    timeout_ms: u64,
}

impl Runner {
    /// Build the sandboxed engine and compile the script.
    ///
    /// A syntax error surfaces here — before any document is touched — so it fails
    /// the step as `InvalidConfig` whatever `on_error` says.
    pub(crate) fn new(cfg: &Config) -> Result<Self, PluginError> {
        let mut engine = sandboxed_engine(cfg.max_ops);

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
    pub(crate) fn arm_deadline(&self) {
        let now = self.base.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64;
        let budget = self.timeout_ms.saturating_mul(1_000_000);
        self.deadline
            .store(now.saturating_add(budget), Ordering::Relaxed);
    }

    /// Run the script over one document, mutating it in place.
    pub(crate) fn run(&self, doc: &mut Document) -> Result<Verdict, PluginError> {
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

/// What the script decided about one document.
pub(crate) enum Verdict {
    /// Keep it, with the script's mutations applied.
    Keep,
    /// Drop it: the script returned `false`.
    Drop,
}
