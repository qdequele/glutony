//! # meili-ingest plugin runtime
//!
//! Loads *external* plugins and exposes them through the plugin-sdk [`Plugin`] trait so
//! the worker's registry can treat them like built-ins:
//!
//! | kind | feature | type | location |
//! |---|---|---|---|
//! | WASM module (extism) | `wasm` | [`WasmPlugin`] | path to a `.wasm` file |
//! | gRPC container | `grpc` | [`GrpcPlugin`] | `http://host:port` |
//!
//! Operators list them in the `EXTERNAL_PLUGINS` env var and the worker calls
//! [`parse_env`] + [`load`]:
//!
//! ```text
//! EXTERNAL_PLUGINS="wasm:/plugins/upper.wasm,grpc:http://whisper:50051"
//! ```
//!
//! The wire protocol for both kinds is JSON (see [`protocol`] and `proto/plugin.proto`).

#![forbid(unsafe_code)]

pub mod error;
pub mod protocol;

#[cfg(feature = "grpc")]
pub mod grpc;
#[cfg(feature = "wasm")]
pub mod wasm;

use std::sync::Arc;

use meili_ingest_plugin_sdk::{Plugin, PluginKind, PluginManifest};
use serde::{Deserialize, Serialize};

pub use error::RuntimeError;
#[cfg(feature = "grpc")]
pub use grpc::GrpcPlugin;
#[cfg(feature = "wasm")]
pub use wasm::WasmPlugin;

/// One entry of the `EXTERNAL_PLUGINS` list.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExternalPluginSpec {
    /// `Wasm` or `Grpc` (`Builtin` is never produced by [`parse_env`]).
    pub kind: PluginKind,
    /// File path (WASM) or endpoint URI (gRPC).
    pub location: String,
    /// Optional manifest override (see [`WasmPlugin::from_file`] / [`GrpcPlugin::connect_with_manifest`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub manifest: Option<PluginManifest>,
}

impl ExternalPluginSpec {
    /// Build a spec without manifest override.
    pub fn new(kind: PluginKind, location: impl Into<String>) -> Self {
        Self {
            kind,
            location: location.into(),
            manifest: None,
        }
    }
}

/// Parse `EXTERNAL_PLUGINS="wasm:/plugins/foo.wasm,grpc:http://whisper:50051"`.
///
/// Entries are comma-separated `kind:location` pairs; whitespace around entries is
/// ignored, empty entries are skipped and entries with an unknown kind are skipped with
/// a warning (the worker should not refuse to boot because of one typo).
pub fn parse_env(value: &str) -> Vec<ExternalPluginSpec> {
    value
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .filter_map(|entry| {
            let Some((kind, location)) = entry.split_once(':') else {
                tracing::warn!(
                    entry,
                    "EXTERNAL_PLUGINS entry has no `kind:` prefix, skipping"
                );
                return None;
            };
            let location = location.trim();
            if location.is_empty() {
                tracing::warn!(
                    entry,
                    "EXTERNAL_PLUGINS entry has an empty location, skipping"
                );
                return None;
            }
            let kind = match kind.trim().to_ascii_lowercase().as_str() {
                "wasm" => PluginKind::Wasm,
                "grpc" => PluginKind::Grpc,
                other => {
                    tracing::warn!(
                        entry,
                        kind = other,
                        "unknown EXTERNAL_PLUGINS kind, skipping"
                    );
                    return None;
                }
            };
            Some(ExternalPluginSpec::new(kind, location))
        })
        .collect()
}

/// Load one external plugin.
pub async fn load(spec: &ExternalPluginSpec) -> Result<Arc<dyn Plugin>, RuntimeError> {
    match spec.kind {
        PluginKind::Wasm => load_wasm(spec),
        PluginKind::Grpc => load_grpc(spec).await,
        PluginKind::Builtin => Err(RuntimeError::UnsupportedKind("builtin".into())),
    }
}

#[cfg(feature = "wasm")]
fn load_wasm(spec: &ExternalPluginSpec) -> Result<Arc<dyn Plugin>, RuntimeError> {
    Ok(Arc::new(WasmPlugin::from_file(
        &spec.location,
        spec.manifest.clone(),
    )?))
}

#[cfg(not(feature = "wasm"))]
fn load_wasm(_spec: &ExternalPluginSpec) -> Result<Arc<dyn Plugin>, RuntimeError> {
    Err(RuntimeError::UnsupportedKind(
        "wasm (feature disabled)".into(),
    ))
}

#[cfg(feature = "grpc")]
async fn load_grpc(spec: &ExternalPluginSpec) -> Result<Arc<dyn Plugin>, RuntimeError> {
    Ok(Arc::new(
        GrpcPlugin::connect_with_manifest(spec.location.clone(), spec.manifest.clone()).await?,
    ))
}

#[cfg(not(feature = "grpc"))]
async fn load_grpc(_spec: &ExternalPluginSpec) -> Result<Arc<dyn Plugin>, RuntimeError> {
    Err(RuntimeError::UnsupportedKind(
        "grpc (feature disabled)".into(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_env_handles_kinds_whitespace_and_urls_with_colons() {
        let specs =
            parse_env(" wasm:/plugins/foo.wasm , grpc:http://whisper:50051,,bogus:x, nocolon ");
        assert_eq!(
            specs,
            vec![
                ExternalPluginSpec::new(PluginKind::Wasm, "/plugins/foo.wasm"),
                ExternalPluginSpec::new(PluginKind::Grpc, "http://whisper:50051"),
            ]
        );
        assert!(parse_env("").is_empty());
    }

    #[tokio::test]
    async fn builtin_kind_cannot_be_loaded_externally() {
        let err = load(&ExternalPluginSpec::new(PluginKind::Builtin, "x"))
            .await
            .err();
        assert!(matches!(err, Some(RuntimeError::UnsupportedKind(_))));
    }
}
