//! A problem in a step's config, pinned to where it is.

use serde::{Deserialize, Serialize};

/// One problem found by [`crate::check_config`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    /// `script`, `ops`, `ops[3]`, `ops[2].expr`, `ops[1][0]`, `config`, …
    pub path: String,
    pub message: String,
    /// 1-based, from rhai's parse position; `None` for structural errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

impl Diagnostic {
    pub(crate) fn at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            message: message.into(),
            line: None,
            column: None,
        }
    }

    pub(crate) fn from_parse_error(path: impl Into<String>, error: &rhai::ParseError) -> Self {
        let position = error.position();
        Self {
            path: path.into(),
            message: error.err_type().to_string(),
            line: position.line().map(|l| l as u32),
            column: position.position().map(|c| c as u32),
        }
    }
}
