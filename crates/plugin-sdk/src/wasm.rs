//! Guest-side helpers for plugins compiled to WASM (`--features wasm`, target
//! `wasm32-wasip1`, loaded by `meili-ingest-plugin-runtime` through extism).
//!
//! The host calls two exports on the module:
//!
//! * `execute(bytes) -> bytes`: input is a JSON [`ExecuteEnvelope`]
//!   (`{"input": PluginInput, "config": {...}, "job_id": "...", "step_id": "...",
//!   "attempt": 1}`), output is a JSON [`ExecuteReply`]
//!   (`{"ok": PluginOutput}` or `{"error": {"message": "...", "retryable": bool}}`).
//! * `manifest() -> bytes`: a JSON `PluginManifest`. Required for modules the worker
//!   loads from `EXTERNAL_PLUGINS`, which has no way to supply a manifest: a module
//!   without this export fails to load. Only a host using `meili-ingest-plugin-runtime`
//!   directly can do without it, by passing a manifest override to `WasmPlugin::from_file`.
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
//! plugins must not spawn tasks or use tokio I/O. The context carries the real
//! `job_id`, `step_id` and `attempt`, but heartbeats are discarded (the host heartbeats
//! once per call), cancellation is never observed, `previous_progress` is always `None`
//! and `record_usage` is discarded: the step's usage is what the host measures, never
//! what a guest reports.

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
    /// The job the step belongs to ([`ActivityContext::job_id`]). `None` from a host
    /// that predates it: the context then has the nil id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub job_id: Option<uuid::Uuid>,
    /// The pipeline step id ([`ActivityContext::step_id`]), `"test"` when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub step_id: Option<String>,
    /// The attempt number, 1-based ([`ActivityContext::attempt`]), 1 when absent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt: Option<u32>,
}

impl ExecuteEnvelope {
    /// The context the guest runs the plugin with: the ids the host sent, no
    /// heartbeats, no cancellation, no usage reporting, no AI target.
    fn context(&self) -> ActivityContext {
        let noop = ActivityContext::noop();
        ActivityContext::detached(
            self.job_id.unwrap_or_else(|| noop.job_id()),
            self.step_id
                .clone()
                .unwrap_or_else(|| noop.step_id().into()),
            self.attempt.unwrap_or_else(|| noop.attempt()),
        )
    }
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
            let ctx = envelope.context();
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
            job_id: None,
            step_id: None,
            attempt: None,
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

    /// Answers with the ids its context carries.
    struct Ids;

    #[async_trait]
    impl Plugin for Ids {
        fn manifest(&self) -> PluginManifest {
            PluginManifest::new("ids", "0.1.0")
        }
        async fn execute(
            &self,
            ctx: &ActivityContext,
            _input: PluginInput,
            _config: serde_json::Value,
        ) -> Result<PluginOutput, PluginError> {
            Ok(PluginOutput::Documents(vec![Document::with_id(
                ctx.job_id().to_string(),
                format!("{}#{}", ctx.step_id(), ctx.attempt()),
            )]))
        }
    }

    fn ids_reply(envelope: serde_json::Value) -> Document {
        let out = run_plugin(&Ids, &serde_json::to_vec(&envelope).unwrap());
        match serde_json::from_slice(&out).unwrap() {
            ExecuteReply::Ok(PluginOutput::Documents(mut d)) => d.remove(0),
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn the_guest_context_carries_the_step_it_runs() {
        let doc = ids_reply(serde_json::json!({
            "input": {"type": "empty"},
            "config": {},
            "job_id": "6b0e8f4e-3c0a-4a5e-9d3f-1f2a3b4c5d6e",
            "step_id": "enrich",
            "attempt": 2,
        }));
        assert_eq!(doc.id, "6b0e8f4e-3c0a-4a5e-9d3f-1f2a3b4c5d6e");
        assert_eq!(doc.content, "enrich#2");
    }

    #[test]
    fn an_envelope_from_an_older_host_still_runs() {
        // Hosts before the ids were added send only `input` and `config`.
        let doc = ids_reply(serde_json::json!({"input": {"type": "empty"}, "config": {}}));
        assert_eq!(doc.id, uuid::Uuid::nil().to_string());
        assert_eq!(doc.content, "test#1");
    }

    #[test]
    fn manifest_json_encodes_manifest() {
        let v: serde_json::Value = serde_json::from_slice(&manifest_json(&Upper)).unwrap();
        assert_eq!(v["name"], "upper");
    }
}
