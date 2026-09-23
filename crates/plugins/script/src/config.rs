//! Step configuration: parsing and validation of the `config:` block.

use meili_ingest_plugin_sdk::PluginError;
use rhai::Engine;
use serde::Deserialize;
use serde_json::Value;

use crate::NAME;
use crate::diagnostic::Diagnostic;
use crate::ops::{Op, check_ops};
use crate::runner::sandboxed_engine;

/// Largest `script` accepted, in bytes.
pub(crate) const MAX_SCRIPT_BYTES: usize = 64 * 1024;

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

/// What the step runs.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    Script(String),
    Ops(Vec<Op>),
}

/// Step configuration, validated.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) source: Source,
    /// What a per-document error does.
    pub(crate) on_error: OnError,
    /// Rhai operations one document may use.
    pub(crate) max_ops: u64,
    /// Wall-clock milliseconds one document may use.
    pub(crate) timeout_ms: u64,
}

/// The `config:` block as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    script: Option<String>,
    #[serde(default)]
    ops: Option<Vec<Value>>,
    #[serde(default)]
    on_error: OnError,
    #[serde(default = "default_max_ops")]
    max_ops: u64,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

pub(crate) fn default_max_ops() -> u64 {
    100_000
}

fn default_timeout_ms() -> u64 {
    1_000
}

impl Config {
    /// Every problem with `value`, or the parsed config.
    pub(crate) fn check(value: Value, engine: &Engine) -> Result<Self, Vec<Diagnostic>> {
        let raw: RawConfig = serde_json::from_value(value)
            .map_err(|e| vec![Diagnostic::at("config", e.to_string())])?;
        let mut diagnostics = Vec::new();

        let source = match (raw.script, raw.ops) {
            (Some(_), Some(_)) => {
                diagnostics.push(Diagnostic::at(
                    "config",
                    "set exactly one of `script` or `ops`, not both",
                ));
                None
            }
            (None, None) => {
                diagnostics.push(Diagnostic::at(
                    "config",
                    "set exactly one of `script` or `ops`",
                ));
                None
            }
            (Some(script), None) => {
                if script.trim().is_empty() {
                    diagnostics.push(Diagnostic::at("script", "`script` must not be empty"));
                } else if script.len() > MAX_SCRIPT_BYTES {
                    diagnostics.push(Diagnostic::at(
                        "script",
                        format!(
                            "`script` is {} bytes; the limit is {MAX_SCRIPT_BYTES}",
                            script.len()
                        ),
                    ));
                }
                Some(Source::Script(script))
            }
            (None, Some(raw_ops)) => match check_ops(&raw_ops, engine) {
                Ok(ops) => Some(Source::Ops(ops)),
                Err(found) => {
                    diagnostics.extend(found);
                    None
                }
            },
        };
        if raw.max_ops == 0 {
            diagnostics.push(Diagnostic::at("max_ops", "`max_ops` must be at least 1"));
        }
        if raw.timeout_ms == 0 {
            diagnostics.push(Diagnostic::at(
                "timeout_ms",
                "`timeout_ms` must be at least 1",
            ));
        }

        match source {
            Some(source) if diagnostics.is_empty() => Ok(Config {
                source,
                on_error: raw.on_error,
                max_ops: raw.max_ops,
                timeout_ms: raw.timeout_ms,
            }),
            _ => Err(diagnostics),
        }
    }

    /// Validate a raw `config:` block, failing on the first problem.
    pub(crate) fn parse(value: Value) -> Result<Self, PluginError> {
        let engine = sandboxed_engine(default_max_ops());
        Self::check(value, &engine).map_err(|diagnostics| {
            let first = &diagnostics[0];
            PluginError::InvalidConfig(format!("{NAME}: {}: {}", first.path, first.message))
        })
    }
}
