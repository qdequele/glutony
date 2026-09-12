//! Error type shared by every handler, rendered as `{"error": "...", "code": "..."}`.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// Control-plane error. Each variant maps to one HTTP status and a stable `code`.
#[derive(Debug, thiserror::Error)]
pub enum CpError {
    /// The request body is not valid JSON for the expected type (400, `bad_json`).
    #[error("invalid JSON body: {0}")]
    BadJson(String),
    /// Attempt to create, overwrite or delete a built-in pipeline (403, `builtin`).
    #[error("{0}")]
    Builtin(String),
    /// Resource not found (404, `not_found`).
    #[error("{0}")]
    NotFound(String),
    /// No pipeline matches the request (404, `no_pipeline`).
    #[error("{0}")]
    NoPipeline(String),
    /// Pipeline failed structural validation (422, `validation`).
    #[error("{0}")]
    Validation(String),
    /// Pipeline references a plugin nobody knows about (422, `unknown_plugin`).
    #[error("{0}")]
    UnknownPlugin(String),
    /// Database failure (500, `db`).
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),
    /// Anything else (500, `internal`).
    #[error("{0}")]
    Internal(String),
}

/// JSON error body.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Human-readable message.
    pub error: String,
    /// Stable machine-readable code (snake_case).
    pub code: String,
}

impl CpError {
    /// HTTP status for this error.
    pub fn status(&self) -> StatusCode {
        match self {
            CpError::BadJson(_) => StatusCode::BAD_REQUEST,
            CpError::Builtin(_) => StatusCode::FORBIDDEN,
            CpError::NotFound(_) | CpError::NoPipeline(_) => StatusCode::NOT_FOUND,
            CpError::Validation(_) | CpError::UnknownPlugin(_) => StatusCode::UNPROCESSABLE_ENTITY,
            CpError::Db(_) | CpError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Stable machine-readable code.
    pub fn code(&self) -> &'static str {
        match self {
            CpError::BadJson(_) => "bad_json",
            CpError::Builtin(_) => "builtin",
            CpError::NotFound(_) => "not_found",
            CpError::NoPipeline(_) => "no_pipeline",
            CpError::Validation(_) => "validation",
            CpError::UnknownPlugin(_) => "unknown_plugin",
            CpError::Db(_) => "db",
            CpError::Internal(_) => "internal",
        }
    }

    /// The JSON body sent to the client.
    pub fn body(&self) -> ErrorBody {
        ErrorBody {
            error: self.to_string(),
            code: self.code().to_string(),
        }
    }
}

impl From<serde_json::Error> for CpError {
    fn from(e: serde_json::Error) -> Self {
        CpError::Internal(format!("serialization error: {e}"))
    }
}

impl IntoResponse for CpError {
    fn into_response(self) -> Response {
        if matches!(self, CpError::Db(_) | CpError::Internal(_)) {
            tracing::error!(error = %self, code = self.code(), "request failed");
        } else {
            tracing::debug!(error = %self, code = self.code(), "request rejected");
        }
        (self.status(), Json(self.body())).into_response()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_and_code_mapping() {
        let cases: Vec<(CpError, StatusCode, &str)> = vec![
            (
                CpError::BadJson("x".into()),
                StatusCode::BAD_REQUEST,
                "bad_json",
            ),
            (
                CpError::Builtin("x".into()),
                StatusCode::FORBIDDEN,
                "builtin",
            ),
            (
                CpError::NotFound("x".into()),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                CpError::NoPipeline("x".into()),
                StatusCode::NOT_FOUND,
                "no_pipeline",
            ),
            (
                CpError::Validation("x".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
                "validation",
            ),
            (
                CpError::UnknownPlugin("x".into()),
                StatusCode::UNPROCESSABLE_ENTITY,
                "unknown_plugin",
            ),
            (
                CpError::Db(sqlx::Error::RowNotFound),
                StatusCode::INTERNAL_SERVER_ERROR,
                "db",
            ),
            (
                CpError::Internal("x".into()),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal",
            ),
        ];
        for (err, status, code) in cases {
            assert_eq!(err.status(), status, "{err:?}");
            assert_eq!(err.code(), code, "{err:?}");
            assert_eq!(err.body().code, code);
        }
    }

    #[test]
    fn body_carries_message() {
        let b = CpError::UnknownPlugin("unknown plugin \"foo\"".into()).body();
        assert_eq!(b.error, "unknown plugin \"foo\"");
        assert_eq!(b.code, "unknown_plugin");
    }
}
