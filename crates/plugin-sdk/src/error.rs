//! Error type returned by plugins.

use thiserror::Error;

/// Errors a plugin can return. The worker maps these onto Temporal failure
/// semantics: [`PluginError::Retryable`] and transport errors are retried according to
/// the step's retry policy, everything else fails the step immediately.
#[derive(Debug, Error)]
pub enum PluginError {
    /// Transient failure (network hiccup, rate limit, ...). The step will be retried.
    #[error("retryable: {0}")]
    Retryable(String),

    /// Permanent failure. The step will NOT be retried.
    #[error("non-retryable: {0}")]
    NonRetryable(String),

    /// The `config:` block of the step is invalid for this plugin. Never retried.
    #[error("invalid config: {0}")]
    InvalidConfig(String),

    /// The plugin received an input variant it does not accept. Never retried.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// The activity was cancelled (workflow cancel signal). Never retried.
    #[error("cancelled")]
    Cancelled,

    /// I/O failure. Retried.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),

    /// JSON (de)serialization failure. Never retried.
    #[error("serde: {0}")]
    Serde(#[from] serde_json::Error),

    /// Any other error. Retried (assumed transient unless proven otherwise).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl PluginError {
    /// Whether the worker should retry the step after this error.
    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            PluginError::Retryable(_) | PluginError::Io(_) | PluginError::Other(_)
        )
    }

    /// Build a retryable error from anything displayable.
    pub fn retryable(msg: impl std::fmt::Display) -> Self {
        PluginError::Retryable(msg.to_string())
    }

    /// Build a non-retryable error from anything displayable.
    pub fn non_retryable(msg: impl std::fmt::Display) -> Self {
        PluginError::NonRetryable(msg.to_string())
    }

    /// Build an invalid-config error from anything displayable.
    pub fn invalid_config(msg: impl std::fmt::Display) -> Self {
        PluginError::InvalidConfig(msg.to_string())
    }

    /// Build an invalid-input error from anything displayable.
    pub fn invalid_input(msg: impl std::fmt::Display) -> Self {
        PluginError::InvalidInput(msg.to_string())
    }
}
