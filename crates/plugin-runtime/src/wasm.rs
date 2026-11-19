//! WASM plugins loaded with [extism](https://extism.org) (wasmtime under the hood).
//!
//! # Guest protocol
//!
//! The module must export:
//!
//! * `execute(bytes) -> bytes` — receives a JSON [`ExecuteEnvelope`]
//!   (`{"input": PluginInput, "config": {...}}`) and returns a JSON [`ExecuteReply`]
//!   (`{"ok": PluginOutput}` or `{"error": {"message": "...", "retryable": bool}}`).
//! * `manifest() -> bytes` — returns a JSON `PluginManifest`. The worker loads modules
//!   from `EXTERNAL_PLUGINS`, which has no way to supply a manifest, so a module it
//!   loads must export this. Only a host using this crate directly can do without it,
//!   by passing a manifest override to [`WasmPlugin::from_file`].
//!
//! The `meili-ingest-plugin-sdk` crate's `wasm` feature provides `run_plugin` /
//! `manifest_json` so a guest written in Rust is a dozen lines
//! (see `crates/plugin-runtime/examples/README.md`).
//!
//! # Execution
//!
//! extism calls are synchronous, so [`WasmPlugin::execute`] runs them on the blocking
//! thread pool. One instance executes one call at a time, which matches WASM's
//! single-threaded memory model; a call waits for the instance on the async runtime, so
//! waiting costs no thread. Fan-out steps still run in parallel across
//! activities/workers.
//!
//! A guest call is cut short when the activity is cancelled, when the caller stops
//! waiting for it (the worker abandons a step that ignores cancellation), or when it
//! outlives [`WasmLimits::timeout`]. The instance is then replaced by a fresh one, since
//! a guest stopped mid-call may hold broken state, and is free for the next call. The
//! interruption stops guest code; a guest blocked inside a host or WASI call is only
//! stopped once that call returns.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use meili_ingest_plugin_sdk::{
    ActivityContext, Plugin, PluginError, PluginInput, PluginKind, PluginManifest, PluginOutput,
    async_trait,
};

use crate::error::RuntimeError;
use crate::protocol::{ExecuteEnvelope, ExecuteReply};

/// Name of the guest export that runs the plugin.
pub const EXECUTE_FN: &str = "execute";
/// Name of the optional guest export that describes the plugin.
pub const MANIFEST_FN: &str = "manifest";

/// Env var read by [`WasmLimits::from_env`]: longest one guest call may run, in seconds.
pub const TIMEOUT_ENV: &str = "WASM_PLUGIN_TIMEOUT_SECS";
/// Env var read by [`WasmLimits::from_env`]: memory one instance may allocate, in MiB.
pub const MAX_MEMORY_ENV: &str = "WASM_PLUGIN_MAX_MEMORY_MB";

/// Size of a WebAssembly memory page.
const PAGE_BYTES: u64 = 64 * 1024;
/// WebAssembly pages per MiB.
const PAGES_PER_MIB: u64 = 1024 * 1024 / PAGE_BYTES;

/// How often a call looks at the activity's cancellation flag. The worker abandons a
/// cancelled step after a ten-second grace, so this must stay far below that.
const CANCEL_POLL: Duration = Duration::from_millis(50);

/// What extism reports when its timer or a cancel interrupts a call.
const EXTISM_TIMEOUT: &str = "timeout";
/// What extism reports when a call needs more memory than the instance may have.
const EXTISM_OOM: &str = "oom";

/// Bounds on what a WASM plugin may use. The default sets none.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WasmLimits {
    /// Longest one guest call may run. A call that outlives it fails the step for
    /// good: the same input would spin again.
    pub timeout: Option<Duration>,
    /// Most memory one instance may allocate, in 64 KiB WebAssembly pages. It covers
    /// the guest's own memory and the buffers holding its input and output, so it must
    /// be well above the largest input the plugin is given.
    pub max_memory_pages: Option<u32>,
}

impl WasmLimits {
    /// Read `WASM_PLUGIN_TIMEOUT_SECS` and `WASM_PLUGIN_MAX_MEMORY_MB`. Unset or blank
    /// means no limit; anything but a positive integer is an error.
    pub fn from_env() -> Result<Self, RuntimeError> {
        Self::from_lookup(|name| std::env::var(name).ok())
    }

    fn from_lookup(lookup: impl Fn(&str) -> Option<String>) -> Result<Self, RuntimeError> {
        let read = |name: &str| -> Result<Option<u64>, RuntimeError> {
            let Some(raw) = lookup(name) else {
                return Ok(None);
            };
            let raw = raw.trim();
            if raw.is_empty() {
                return Ok(None);
            }
            match raw.parse::<u64>() {
                Ok(n) if n > 0 => Ok(Some(n)),
                _ => Err(RuntimeError::Wasm(format!(
                    "{name} must be a positive integer, got {raw:?}"
                ))),
            }
        };
        let timeout = read(TIMEOUT_ENV)?.map(Duration::from_secs);
        let max_memory_pages = read(MAX_MEMORY_ENV)?
            .map(|mib| {
                mib.checked_mul(PAGES_PER_MIB)
                    .and_then(|pages| u32::try_from(pages).ok())
                    .ok_or_else(|| {
                        RuntimeError::Wasm(format!(
                            "{MAX_MEMORY_ENV} is too large: {mib} MiB does not fit a wasm memory"
                        ))
                    })
            })
            .transpose()?;
        Ok(Self {
            timeout,
            max_memory_pages,
        })
    }

    fn apply(&self, mut manifest: extism::Manifest) -> extism::Manifest {
        if let Some(timeout) = self.timeout {
            manifest = manifest.with_timeout(timeout);
        }
        if let Some(pages) = self.max_memory_pages {
            manifest = manifest.with_memory_max(pages);
        }
        manifest
    }
}

/// A plugin backed by a WASM module.
pub struct WasmPlugin {
    /// The compiled module, kept to build a fresh instance after an interrupted call.
    compiled: Arc<extism::CompiledPlugin>,
    /// The one instance. A tokio mutex, so a call waiting for it holds no thread and
    /// can give up when its activity is cancelled.
    instance: Arc<tokio::sync::Mutex<extism::Plugin>>,
    manifest: PluginManifest,
    source: String,
    limits: WasmLimits,
}

impl std::fmt::Debug for WasmPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmPlugin")
            .field("name", &self.manifest.name)
            .field("source", &self.source)
            .field("limits", &self.limits)
            .finish()
    }
}

impl WasmPlugin {
    /// Load a module from a `.wasm` file, without limits.
    ///
    /// `manifest_override`, when given, wins over the guest's own `manifest` export, so
    /// a host calling this directly can rename a plugin or run a module that does not
    /// export one. The worker never passes one: [`crate::parse_env`] has no syntax for it.
    pub fn from_file(
        path: impl AsRef<Path>,
        manifest_override: Option<PluginManifest>,
    ) -> Result<Self, RuntimeError> {
        Self::from_file_with_limits(path, manifest_override, WasmLimits::default())
    }

    /// Load a module from a `.wasm` file and bound what it may use.
    pub fn from_file_with_limits(
        path: impl AsRef<Path>,
        manifest_override: Option<PluginManifest>,
        limits: WasmLimits,
    ) -> Result<Self, RuntimeError> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("wasm module not found: {}", path.display()),
            )));
        }
        let manifest = extism::Manifest::new([extism::Wasm::file(path)]);
        Self::load(
            manifest,
            manifest_override,
            path.display().to_string(),
            limits,
        )
    }

    /// Load a module from its raw bytes, without limits.
    pub fn from_bytes(
        bytes: impl Into<Vec<u8>>,
        manifest_override: Option<PluginManifest>,
    ) -> Result<Self, RuntimeError> {
        Self::from_bytes_with_limits(bytes, manifest_override, WasmLimits::default())
    }

    /// Load a module from its raw bytes and bound what it may use.
    pub fn from_bytes_with_limits(
        bytes: impl Into<Vec<u8>>,
        manifest_override: Option<PluginManifest>,
        limits: WasmLimits,
    ) -> Result<Self, RuntimeError> {
        let manifest = extism::Manifest::new([extism::Wasm::data(bytes.into())]);
        Self::load(manifest, manifest_override, "<bytes>".to_string(), limits)
    }

    fn load(
        extism_manifest: extism::Manifest,
        manifest_override: Option<PluginManifest>,
        source: String,
        limits: WasmLimits,
    ) -> Result<Self, RuntimeError> {
        let wasm_error = |e: extism::Error| RuntimeError::Wasm(format!("{source}: {e:#}"));
        let builder = extism::PluginBuilder::new(limits.apply(extism_manifest)).with_wasi(true);
        let compiled = extism::CompiledPlugin::new(builder).map_err(wasm_error)?;
        let mut plugin = extism::Plugin::new_from_compiled(&compiled).map_err(wasm_error)?;

        if !plugin.function_exists(EXECUTE_FN) {
            return Err(RuntimeError::Wasm(format!(
                "{source}: module does not export `{EXECUTE_FN}`"
            )));
        }

        let mut manifest = match manifest_override {
            Some(m) => m,
            None if plugin.function_exists(MANIFEST_FN) => {
                let raw: Vec<u8> = plugin
                    .call(MANIFEST_FN, &[][..])
                    .map_err(|e| RuntimeError::Wasm(format!("{source}: manifest(): {e:#}")))?;
                serde_json::from_slice(&raw)?
            }
            None => return Err(RuntimeError::MissingManifest(source)),
        };
        manifest.kind = PluginKind::Wasm;

        tracing::info!(plugin = %manifest.name, version = %manifest.version, source = %source, ?limits, "loaded wasm plugin");
        Ok(Self {
            compiled: Arc::new(compiled),
            instance: Arc::new(tokio::sync::Mutex::new(plugin)),
            manifest,
            source,
            limits,
        })
    }

    /// Where the module was loaded from (path or `<bytes>`).
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The limits this plugin runs under.
    pub fn limits(&self) -> WasmLimits {
        self.limits
    }
}

/// One guest call in flight, shared by the thread running it and the task that may
/// have to interrupt it.
struct Call {
    cancel: extism::CancelHandle,
    /// Set when the caller's future is dropped: nobody waits for the result any more.
    abandoned: AtomicBool,
    progress: std::sync::Mutex<Progress>,
}

#[derive(Default)]
struct Progress {
    /// The guest call returned.
    finished: bool,
    /// An interruption was sent while it ran.
    interrupted: bool,
}

impl Call {
    fn progress(&self) -> std::sync::MutexGuard<'_, Progress> {
        // Two booleans cannot be left half-written, so a poisoned lock is still good.
        self.progress
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Interrupt the guest. Returns `false` once the call is over.
    ///
    /// The lock is held while the cancel is sent and the instance is only handed to the
    /// next caller after [`Call::finish`], so an interruption never reaches somebody
    /// else's call.
    fn interrupt(&self) -> bool {
        let mut progress = self.progress();
        if progress.finished {
            return false;
        }
        progress.interrupted = true;
        // Fails only if extism's timer thread is gone, and then nothing is running.
        let _ = self.cancel.cancel();
        true
    }

    /// Record that the guest call returned. Returns whether it had been interrupted.
    fn finish(&self) -> bool {
        let mut progress = self.progress();
        progress.finished = true;
        progress.interrupted
    }

    /// Interrupt the guest once the activity is cancelled or the caller is gone, and
    /// keep at it until the call returns: extism ignores a cancel that arrives before
    /// the guest has started.
    async fn watch(self: Arc<Self>, cancelled: Arc<AtomicBool>) {
        let mut tick = tokio::time::interval(CANCEL_POLL);
        loop {
            tick.tick().await;
            let wanted =
                cancelled.load(Ordering::Relaxed) || self.abandoned.load(Ordering::Relaxed);
            let over = if wanted {
                !self.interrupt()
            } else {
                self.progress().finished
            };
            if over {
                return;
            }
        }
    }
}

/// Marks the call abandoned when the caller's future goes away.
struct AbandonOnDrop(Arc<Call>);

impl Drop for AbandonOnDrop {
    fn drop(&mut self) {
        self.0.abandoned.store(true, Ordering::Relaxed);
    }
}

/// Why a guest call produced no reply.
enum CallFailure {
    /// Interrupted because the activity was cancelled or its caller went away.
    Cancelled,
    /// Anything else, already worded for the job's error.
    Failed(String),
}

/// Resolves once the activity is cancelled.
async fn cancelled(ctx: &ActivityContext) {
    let mut tick = tokio::time::interval(CANCEL_POLL);
    loop {
        tick.tick().await;
        if ctx.is_cancelled() {
            return;
        }
    }
}

#[async_trait]
impl Plugin for WasmPlugin {
    fn manifest(&self) -> PluginManifest {
        self.manifest.clone()
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        ctx.check_cancelled()?;
        let name = self.manifest.name.clone();
        let payload = serde_json::to_vec(&ExecuteEnvelope::for_step(ctx, input, config))?;

        // Wait for the instance without holding a thread, and stop waiting when the
        // activity is cancelled: another job's call may never finish.
        let mut instance = tokio::select! {
            biased;
            instance = Arc::clone(&self.instance).lock_owned() => instance,
            () = cancelled(ctx) => return Err(PluginError::Cancelled),
        };
        ctx.heartbeat(format!("wasm {name}: calling execute"));

        let call = Arc::new(Call {
            cancel: instance.cancel_handle(),
            abandoned: AtomicBool::new(false),
            progress: std::sync::Mutex::new(Progress::default()),
        });
        tokio::spawn(Arc::clone(&call).watch(ctx.cancellation_flag()));
        let _abandon = AbandonOnDrop(Arc::clone(&call));

        let compiled = Arc::clone(&self.compiled);
        let limits = self.limits;
        let raw: Vec<u8> = tokio::task::spawn_blocking(move || {
            let result = instance.call::<&[u8], Vec<u8>>(EXECUTE_FN, &payload);
            let interrupted = call.finish();
            let error = match result {
                Ok(raw) => return Ok(raw),
                Err(e) => e,
            };
            let cause = error.root_cause().to_string();
            let timed_out = cause == EXTISM_TIMEOUT;
            let out_of_memory = cause == EXTISM_OOM;
            if timed_out || out_of_memory {
                // The host stopped the guest mid-call, so whatever it was building is
                // half-done. Start the next call from a clean instance.
                match extism::Plugin::new_from_compiled(&compiled) {
                    Ok(fresh) => *instance = fresh,
                    Err(e) => {
                        tracing::error!(plugin = %name, error = %format!("{e:#}"), "could not replace the wasm instance after an interrupted call");
                    }
                }
            }
            Err(if timed_out && interrupted {
                CallFailure::Cancelled
            } else if let (true, Some(limit)) = (timed_out, limits.timeout) {
                CallFailure::Failed(format!(
                    "wasm {name}: execute() ran longer than the {limit:?} allowed by {TIMEOUT_ENV}"
                ))
            } else if let (true, Some(pages)) = (out_of_memory, limits.max_memory_pages) {
                CallFailure::Failed(format!(
                    "wasm {name}: execute() needed more memory than the {} MiB allowed by \
                     {MAX_MEMORY_ENV}",
                    u64::from(pages) / PAGES_PER_MIB
                ))
            } else {
                CallFailure::Failed(format!("wasm {name}: {error:#}"))
            })
        })
        .await
        .map_err(|e| PluginError::non_retryable(format!("wasm executor task failed: {e}")))?
        .map_err(|failure| match failure {
            CallFailure::Cancelled => PluginError::Cancelled,
            CallFailure::Failed(message) => PluginError::NonRetryable(message),
        })?;

        let reply: ExecuteReply = serde_json::from_slice(&raw).map_err(|e| {
            PluginError::non_retryable(format!(
                "wasm {}: execute() returned invalid reply JSON: {e}",
                self.manifest.name
            ))
        })?;
        reply.into_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn garbage_bytes_are_rejected() {
        let err = WasmPlugin::from_bytes(b"definitely not a wasm module".to_vec(), None)
            .expect_err("garbage must not load");
        assert!(matches!(err, RuntimeError::Wasm(_)), "got {err:?}");
    }

    #[test]
    fn missing_file_is_an_io_error() {
        let err = WasmPlugin::from_file("/nonexistent/plugin.wasm", None)
            .expect_err("missing file must not load");
        assert!(matches!(err, RuntimeError::Io(_)), "got {err:?}");
    }

    fn limits(timeout: Option<&str>, memory: Option<&str>) -> Result<WasmLimits, RuntimeError> {
        WasmLimits::from_lookup(|name| match name {
            TIMEOUT_ENV => timeout.map(str::to_string),
            MAX_MEMORY_ENV => memory.map(str::to_string),
            other => panic!("unexpected variable {other}"),
        })
    }

    #[test]
    fn limits_are_off_unless_set() {
        assert_eq!(limits(None, None).unwrap(), WasmLimits::default());
        assert_eq!(limits(Some(""), Some("  ")).unwrap(), WasmLimits::default());
    }

    #[test]
    fn limits_read_seconds_and_mebibytes() {
        let got = limits(Some(" 120 "), Some("256")).unwrap();
        assert_eq!(got.timeout, Some(Duration::from_secs(120)));
        // 256 MiB of 64 KiB pages.
        assert_eq!(got.max_memory_pages, Some(4096));
    }

    #[test]
    fn limits_refuse_what_is_not_a_positive_integer() {
        for bad in ["0", "-1", "2.5", "ten"] {
            let err = limits(Some(bad), None).expect_err(bad);
            assert!(
                matches!(&err, RuntimeError::Wasm(m) if m.contains(TIMEOUT_ENV)),
                "got {err:?}"
            );
            let err = limits(None, Some(bad)).expect_err(bad);
            assert!(
                matches!(&err, RuntimeError::Wasm(m) if m.contains(MAX_MEMORY_ENV)),
                "got {err:?}"
            );
        }
        // More than a wasm memory can address in pages.
        let err = limits(None, Some("300000000")).unwrap_err();
        assert!(
            matches!(&err, RuntimeError::Wasm(m) if m.contains("too large")),
            "got {err:?}"
        );
    }
}
