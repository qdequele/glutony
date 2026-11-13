//! JSON wire protocol shared by WASM guests and (partially) gRPC containers.
//!
//! These types mirror the ones in `meili_ingest_plugin_sdk::wasm` (guest side). They
//! are duplicated here so the worker never has to enable the SDK `wasm` feature (which
//! pulls in `extism-pdk`).

use meili_ingest_plugin_sdk::{PluginError, PluginInput, PluginOutput};
use serde::{Deserialize, Serialize};

/// What the host sends to a WASM guest's `execute` export.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExecuteEnvelope {
    /// The plugin input.
    pub input: PluginInput,
    /// The step `config:` block.
    pub config: serde_json::Value,
}

/// Structured failure returned by an external plugin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RemoteError {
    /// Human-readable message.
    pub message: String,
    /// Whether the worker should retry the step.
    #[serde(default)]
    pub retryable: bool,
}

impl From<RemoteError> for PluginError {
    fn from(e: RemoteError) -> Self {
        if e.retryable {
            PluginError::Retryable(e.message)
        } else {
            PluginError::NonRetryable(e.message)
        }
    }
}

/// What a WASM guest's `execute` export returns: `{"ok": PluginOutput}` or
/// `{"error": {"message": "...", "retryable": bool}}`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecuteReply {
    /// Success.
    Ok(PluginOutput),
    /// Failure.
    Error(RemoteError),
}

impl ExecuteReply {
    /// Convert into the `Plugin::execute` result type.
    pub fn into_result(self) -> Result<PluginOutput, PluginError> {
        match self {
            ExecuteReply::Ok(out) => Ok(out),
            ExecuteReply::Error(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::Document;

    #[test]
    fn reply_json_shape() {
        let ok = ExecuteReply::Ok(PluginOutput::Documents(vec![Document::with_id("a", "x")]));
        let v = serde_json::to_value(&ok).unwrap();
        assert_eq!(v["ok"]["type"], "documents");
        assert_eq!(v["ok"]["value"][0]["id"], "a");

        let err: ExecuteReply =
            serde_json::from_str(r#"{"error":{"message":"boom","retryable":true}}"#).unwrap();
        assert!(matches!(err.into_result(), Err(PluginError::Retryable(m)) if m == "boom"));
    }

    #[test]
    fn envelope_roundtrip() {
        let env = ExecuteEnvelope {
            input: PluginInput::Empty,
            config: serde_json::json!({"a": 1}),
        };
        let back: ExecuteEnvelope =
            serde_json::from_slice(&serde_json::to_vec(&env).unwrap()).unwrap();
        assert_eq!(back, env);
    }

    // The guest-side copies of these types live in `meili_ingest_plugin_sdk::wasm`,
    // behind a feature the workspace tests do not build. The literals below are the
    // wire format both sides must agree on; `tests/wasm_roundtrip.rs` sends them
    // through a real module.

    #[test]
    fn envelope_wire_format_is_pinned() {
        let env = ExecuteEnvelope {
            input: PluginInput::Documents(vec![Document::with_id("a", "x")]),
            config: serde_json::json!({"a": 1}),
        };
        assert_eq!(
            serde_json::to_value(&env).unwrap(),
            serde_json::json!({
                "input": {
                    "type": "documents",
                    "value": [{"id": "a", "content": "x", "meta": {}}],
                },
                "config": {"a": 1},
            })
        );
        // The guest reads `input` first: the envelope starts with it.
        assert!(
            serde_json::to_string(&env)
                .unwrap()
                .starts_with(r#"{"input":"#)
        );
    }

    #[test]
    fn reply_wire_format_is_pinned() {
        let ok: ExecuteReply = serde_json::from_str(r#"{"ok":{"type":"empty"}}"#).unwrap();
        assert_eq!(ok, ExecuteReply::Ok(PluginOutput::Empty));

        // What the SDK's `run_plugin` falls back to; `retryable` may also be left out.
        let err: ExecuteReply = serde_json::from_str(
            r#"{"error":{"message":"plugin reply could not be serialized","retryable":false}}"#,
        )
        .unwrap();
        assert!(matches!(
            err.into_result(),
            Err(PluginError::NonRetryable(_))
        ));
        let err: ExecuteReply = serde_json::from_str(r#"{"error":{"message":"m"}}"#).unwrap();
        assert!(matches!(err.into_result(), Err(PluginError::NonRetryable(m)) if m == "m"));
    }
}
