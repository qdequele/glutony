//! Guest-side helpers for plugins compiled to WASM (`--features wasm`, target
//! `wasm32-wasip1`, loaded by `meili-ingest-plugin-runtime` through extism).
//!
//! The host calls two exports on the module:
//!
//! * `execute(bytes) -> bytes`: input is a JSON [`ExecuteEnvelope`]
//!   (`{"input": PluginInput, "config": {...}}`), output is a JSON [`ExecuteReply`]
//!   (`{"ok": PluginOutput}` or `{"error": {"message": "...", "retryable": bool}}`).
//! * `manifest() -> bytes` (optional): a JSON `PluginManifest`.
//!
//! [`run_plugin`] and [`manifest_json`] implement both so a guest only has to wrap them
//! in `#[extism_pdk::plugin_fn]` exports:
//!
//! ```ignore
//! use extism_pdk::{plugin_fn, FnResult};
//! use meili_ingest_plugin_sdk::wasm::{manifest_json, run_plugin};
//!
//! #[plugin_fn]
//! pub fn manifest() -> FnResult<Vec<u8>> { Ok(manifest_json(&MyPlugin)) }
//!
//! #[plugin_fn]
//! pub fn execute(input: Vec<u8>) -> FnResult<Vec<u8>> { Ok(run_plugin(&MyPlugin, &input)) }
//! ```
//!
//! There is no tokio inside WASM: `execute` runs on `futures::executor::block_on`, so
//! plugins must not spawn tasks or use tokio I/O. Heartbeats are discarded (the host
//! heartbeats once per call) and cancellation is never observed.

use serde::{Deserialize, Serialize};

use crate::Plugin;
use crate::context::ActivityContext;
use crate::error::PluginError;
use crate::types::{PluginInput, PluginOutput};

/// Re-export so guests can name the PDK through the SDK if they prefer.
pub use extism_pdk;

/// Payload of the `execute` export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteEnvelope {
    /// The plugin input.
    pub input: PluginInput,
    /// The step `config:` block.
    #[serde(default)]
    pub config: serde_json::Value,
}

/// Structured failure in an [`ExecuteReply`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteError {
    /// Human-readable message.
    pub message: String,
    /// Whether the worker should retry the step.
    #[serde(default)]
    pub retryable: bool,
}

impl From<&PluginError> for RemoteError {
    fn from(e: &PluginError) -> Self {
        RemoteError {
            message: e.to_string(),
            retryable: e.is_retryable(),
        }
    }
}

/// Return value of the `execute` export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteReply {
    /// Success.
    Ok(PluginOutput),
    /// Failure.
    Error(RemoteError),
}

/// Fallback reply used if even the error reply cannot be serialized (cannot happen with
/// these types, but we never panic inside a guest).
const SERIALIZE_FAILURE: &str =
    r#"{"error":{"message":"plugin reply could not be serialized","retryable":false}}"#;

/// Decode an [`ExecuteEnvelope`], run `plugin.execute`, encode the [`ExecuteReply`].
///
/// Never panics: undecodable input yields a non-retryable error reply.
pub fn run_plugin<P: Plugin>(plugin: &P, input_json: &[u8]) -> Vec<u8> {
    let reply = match serde_json::from_slice::<ExecuteEnvelope>(input_json) {
        Ok(envelope) => {
            let ctx = ActivityContext::noop();
            match futures::executor::block_on(plugin.execute(&ctx, envelope.input, envelope.config))
            {
                Ok(out) => ExecuteReply::Ok(out),
                Err(e) => ExecuteReply::Error(RemoteError::from(&e)),
            }
        }
        Err(e) => ExecuteReply::Error(RemoteError {
            message: format!("invalid execute envelope: {e}"),
            retryable: false,
        }),
    };
    serde_json::to_vec(&reply).unwrap_or_else(|_| SERIALIZE_FAILURE.as_bytes().to_vec())
}

/// JSON-encode the plugin's manifest for the `manifest` export.
pub fn manifest_json<P: Plugin>(plugin: &P) -> Vec<u8> {
    serde_json::to_vec(&plugin.manifest()).unwrap_or_else(|_| b"{}".to_vec())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::async_trait;
    use crate::types::{Document, InputKind, OutputKind, PluginManifest};

    struct Upper;

    #[async_trait]
    impl Plugin for Upper {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("upper", "0.1.0")
                .accepts([InputKind::Documents])
                .produces(OutputKind::Documents)
        }
        async fn execute(
            &self,
            _ctx: &ActivityContext,
            input: PluginInput,
            config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            if config["fail"].as_bool() == Some(true) {
                return Err(PluginError::retryable("asked to fail"));
            }
            let docs = input.into_documents()?;
            Ok(PluginOutput::Documents(
                docs.into_iter()
                    .map(|mut d| {
                        d.content = d.content.to_uppercase();
                        d
                    })
                    .collect(),
            ))
        }
    }

    #[test]
    fn run_plugin_ok_reply() {
        let env = ExecuteEnvelope {
            input: PluginInput::Documents(vec![Document::with_id("a", "hi")]),
            config: serde_json::json!({}),
        };
        let out = run_plugin(&Upper, &serde_json::to_vec(&env).unwrap());
        let reply: ExecuteReply = serde_json::from_slice(&out).unwrap();
        match reply {
            ExecuteReply::Ok(PluginOutput::Documents(d)) => assert_eq!(d[0].content, "HI"),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn run_plugin_error_and_garbage_replies() {
        let env = serde_json::json!({"input": {"type": "empty"}, "config": {"fail": true}});
        let reply: ExecuteReply =
            serde_json::from_slice(&run_plugin(&Upper, &serde_json::to_vec(&env).unwrap()))
                .unwrap();
        assert_eq!(
            reply,
            ExecuteReply::Error(RemoteError {
                message: "retryable: asked to fail".into(),
                retryable: true
            })
        );

        let reply: ExecuteReply = serde_json::from_slice(&run_plugin(&Upper, b"garbage")).unwrap();
        assert!(matches!(
            reply,
            ExecuteReply::Error(RemoteError {
                retryable: false,
                ..
            })
        ));
    }

    #[test]
    fn manifest_json_encodes_manifest() {
        let v: serde_json::Value = serde_json::from_slice(&manifest_json(&Upper)).unwrap();
        assert_eq!(v["name"], "upper");
    }
}
