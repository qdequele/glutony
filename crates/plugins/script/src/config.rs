//! Step configuration: parsing and validation of the `config:` block.

use meili_ingest_plugin_sdk::PluginError;
use serde::Deserialize;
use serde_json::Value;

use crate::NAME;

/// What a per-document script error does to that document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub(crate) enum OnError {
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
pub(crate) struct Config {
    /// The rhai script, run once per document.
    pub(crate) script: String,
    /// What a per-document error does.
    #[serde(default)]
    pub(crate) on_error: OnError,
    /// Rhai operations one document may use.
    #[serde(default = "default_max_ops")]
    pub(crate) max_ops: u64,
    /// Wall-clock milliseconds one document may use.
    #[serde(default = "default_timeout_ms")]
    pub(crate) timeout_ms: u64,
}

fn default_max_ops() -> u64 {
    100_000
}

fn default_timeout_ms() -> u64 {
    1_000
}

impl Config {
    /// Validate a raw `config:` block. `script` is required, so `null` is an error.
    pub(crate) fn parse(value: Value) -> Result<Self, PluginError> {
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
