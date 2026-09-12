//! Errors raised while loading or talking to an external plugin.

use thiserror::Error;

/// Failures of the plugin runtime itself (loading a module, connecting to a container,
/// decoding a manifest). Errors raised *by* a plugin while executing are reported as
/// [`meili_ingest_plugin_sdk::PluginError`] through the `Plugin` trait instead.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// Reading a `.wasm` file or another filesystem operation failed.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// The WASM module could not be compiled or instantiated.
    #[error("wasm: {0}")]
    Wasm(String),

    /// The plugin did not provide a manifest and no override was supplied.
    #[error("plugin at {0} exports no `manifest` and no manifest override was given")]
    MissingManifest(String),

    /// A manifest or protocol payload was not valid JSON for the expected type.
    #[error("invalid manifest/protocol JSON: {0}")]
    Serde(#[from] serde_json::Error),

    /// The gRPC endpoint could not be reached or is not a valid URI.
    #[error("grpc transport: {0}")]
    Transport(String),

    /// A gRPC call failed with a status.
    #[error("grpc call failed: {0}")]
    Rpc(String),

    /// The requested plugin kind cannot be loaded by this build of the runtime
    /// (feature disabled, or `builtin` which is never external).
    #[error("unsupported external plugin kind: {0}")]
    UnsupportedKind(String),
}
