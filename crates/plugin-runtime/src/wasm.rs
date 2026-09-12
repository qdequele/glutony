//! WASM plugins loaded with [extism](https://extism.org) (wasmtime under the hood).
//!
//! # Guest protocol
//!
//! The module must export:
//!
//! * `execute(bytes) -> bytes` — receives a JSON [`ExecuteEnvelope`]
//!   (`{"input": PluginInput, "config": {...}}`) and returns a JSON [`ExecuteReply`]
//!   (`{"ok": PluginOutput}` or `{"error": {"message": "...", "retryable": bool}}`).
//! * `manifest() -> bytes` — *optional*, returns a JSON `PluginManifest`. When absent
//!   the caller must supply a manifest override.
//!
//! The `meili-ingest-plugin-sdk` crate's `wasm` feature provides `run_plugin` /
//! `manifest_json` so a guest written in Rust is a dozen lines
//! (see `crates/plugin-runtime/examples/README.md`).
//!
//! extism calls are synchronous, so [`WasmPlugin::execute`] runs them on the blocking
//! thread pool with the instance behind a mutex: one instance executes one call at a
//! time, which matches WASM's single-threaded memory model. Fan-out steps still run
//! in parallel across activities/workers.

use std::path::Path;
use std::sync::{Arc, Mutex};

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

/// A plugin backed by a WASM module.
pub struct WasmPlugin {
    plugin: Arc<Mutex<extism::Plugin>>,
    manifest: PluginManifest,
    source: String,
}

impl std::fmt::Debug for WasmPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WasmPlugin")
            .field("name", &self.manifest.name)
            .field("source", &self.source)
            .finish()
    }
}

impl WasmPlugin {
    /// Load a module from a `.wasm` file.
    ///
    /// `manifest_override`, when given, wins over the guest's own `manifest` export
    /// (useful to rename a plugin or to run modules that do not export one).
    pub fn from_file(
        path: impl AsRef<Path>,
        manifest_override: Option<PluginManifest>,
    ) -> Result<Self, RuntimeError> {
        let path = path.as_ref();
        if !path.is_file() {
            return Err(RuntimeError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("wasm module not found: {}", path.display()),
            )));
        }
        let manifest = extism::Manifest::new([extism::Wasm::file(path)]);
        Self::load(manifest, manifest_override, path.display().to_string())
    }

    /// Load a module from its raw bytes.
    pub fn from_bytes(
        bytes: impl Into<Vec<u8>>,
        manifest_override: Option<PluginManifest>,
    ) -> Result<Self, RuntimeError> {
        let manifest = extism::Manifest::new([extism::Wasm::data(bytes.into())]);
        Self::load(manifest, manifest_override, "<bytes>".to_string())
    }

    fn load(
        extism_manifest: extism::Manifest,
        manifest_override: Option<PluginManifest>,
        source: String,
    ) -> Result<Self, RuntimeError> {
        let mut plugin = extism::Plugin::new(
            &extism_manifest,
            std::iter::empty::<extism::Function>(),
            true,
        )
        .map_err(|e| RuntimeError::Wasm(format!("{source}: {e:#}")))?;

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

        tracing::info!(plugin = %manifest.name, version = %manifest.version, source = %source, "loaded wasm plugin");
        Ok(Self {
            plugin: Arc::new(Mutex::new(plugin)),
            manifest,
            source,
        })
    }

    /// Where the module was loaded from (path or `<bytes>`).
    pub fn source(&self) -> &str {
        &self.source
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
        let payload = serde_json::to_vec(&ExecuteEnvelope { input, config })?;
        ctx.heartbeat(format!("wasm {name}: calling execute"));

        let plugin = Arc::clone(&self.plugin);
        let raw: Vec<u8> = tokio::task::spawn_blocking(move || {
            let mut guard = plugin.lock().map_err(|_| {
                PluginError::non_retryable(format!("wasm {name}: instance mutex poisoned"))
            })?;
            guard
                .call::<&[u8], Vec<u8>>(EXECUTE_FN, &payload)
                .map_err(|e| PluginError::non_retryable(format!("wasm {name}: {e:#}")))
        })
        .await
        .map_err(|e| PluginError::non_retryable(format!("wasm executor task failed: {e}")))??;

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
}
