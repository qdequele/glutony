//! Gateway error type and its HTTP mapping.
//!
//! Every handler returns `Result<_, GatewayError>`; the [`IntoResponse`] impl turns the
//! error into `{"error": "<message>", "code": "<snake_case>"}` with the status code from
//! the plan (400 missing context / bad request, 401 unauthorized, 403 forbidden, 404 not found, 413 too
//! large, 415 no pipeline for the MIME type, 422 invalid pipeline, 502 upstream, 500
//! internal). Messages never contain API keys: they are built from our own strings, from
//! control-plane error bodies, or from transport errors that carry URLs but no
//! credentials.

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

/// Errors produced by the gateway. Each variant maps to exactly one HTTP status.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error, Serialize, Deserialize)]
pub enum GatewayError {
    /// Neither Envoy headers nor standalone credentials were found (400).
    #[error("missing Meilisearch context: {0}")]
    MissingContext(String),
    /// Malformed request (400).
    #[error("{0}")]
    BadRequest(String),
    /// The caller's Meilisearch key is missing (401), from the write preflight.
    #[error("{0}")]
    Unauthorized(String),
    /// The control plane refused the operation, e.g. deleting a built-in pipeline, or the
    /// caller's Meilisearch key may not write the target index (403).
    #[error("{0}")]
    Forbidden(String),
    /// Unknown pipeline, job or resource (404).
    #[error("{0}")]
    NotFound(String),
    /// Upload larger than the configured limit (413).
    #[error("{0}")]
    TooLarge(String),
    /// No pipeline matches the detected MIME type (415).
    #[error("no pipeline matches content type {0:?}")]
    Unsupported(String),
    /// A pipeline definition failed validation (422).
    #[error("invalid pipeline: {0}")]
    Invalid(String),
    /// Any other request that is well-formed but cannot be accepted (422), e.g. a
    /// Meilisearch connection whose host is forbidden or whose key is rejected.
    #[error("{0}")]
    Unprocessable(String),
    /// A feature is not configured on this deployment (501).
    #[error("{0}")]
    NotImplemented(String),
    /// Control plane or Temporal unreachable / returned an unexpected error (502).
    #[error("upstream error: {0}")]
    Upstream(String),
    /// Anything else (500).
    #[error("internal error: {0}")]
    Internal(String),
}

impl GatewayError {
    /// HTTP status for this error.
    pub fn status(&self) -> StatusCode {
        match self {
            GatewayError::MissingContext(_) | GatewayError::BadRequest(_) => {
                StatusCode::BAD_REQUEST
            }
            GatewayError::Unauthorized(_) => StatusCode::UNAUTHORIZED,
            GatewayError::Forbidden(_) => StatusCode::FORBIDDEN,
            GatewayError::NotFound(_) => StatusCode::NOT_FOUND,
            GatewayError::TooLarge(_) => StatusCode::PAYLOAD_TOO_LARGE,
            GatewayError::Unsupported(_) => StatusCode::UNSUPPORTED_MEDIA_TYPE,
            GatewayError::Invalid(_) | GatewayError::Unprocessable(_) => {
                StatusCode::UNPROCESSABLE_ENTITY
            }
            GatewayError::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            GatewayError::Upstream(_) => StatusCode::BAD_GATEWAY,
            GatewayError::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Machine-readable error code (`snake_case`).
    pub fn code(&self) -> &'static str {
        match self {
            GatewayError::MissingContext(_) => "missing_context",
            GatewayError::BadRequest(_) => "bad_request",
            GatewayError::Unauthorized(_) => "unauthorized",
            GatewayError::Forbidden(_) => "forbidden",
            GatewayError::NotFound(_) => "not_found",
            GatewayError::TooLarge(_) => "payload_too_large",
            GatewayError::Unsupported(_) => "unsupported_media_type",
            GatewayError::Invalid(_) => "invalid_pipeline",
            GatewayError::Unprocessable(_) => "validation",
            GatewayError::NotImplemented(_) => "not_configured",
            GatewayError::Upstream(_) => "upstream_error",
            GatewayError::Internal(_) => "internal_error",
        }
    }

    /// Build an internal error from anything displayable.
    pub fn internal(e: impl std::fmt::Display) -> Self {
        GatewayError::Internal(e.to_string())
    }

    /// Build an upstream error from anything displayable.
    pub fn upstream(e: impl std::fmt::Display) -> Self {
        GatewayError::Upstream(e.to_string())
    }

    /// Build a bad-request error from anything displayable.
    pub fn bad_request(e: impl std::fmt::Display) -> Self {
        GatewayError::BadRequest(e.to_string())
    }
}

/// JSON body of an error response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorBody {
    /// Human-readable message.
    pub error: String,
    /// Machine-readable code.
    pub code: String,
}

impl IntoResponse for GatewayError {
    fn into_response(self) -> Response {
        let status = self.status();
        if status.is_server_error() {
            tracing::error!(code = self.code(), "{self}");
        } else {
            tracing::debug!(code = self.code(), "{self}");
        }
        let body = ErrorBody {
            error: self.to_string(),
            code: self.code().to_string(),
        };
        (status, Json(body)).into_response()
    }
}

impl From<reqwest::Error> for GatewayError {
    fn from(e: reqwest::Error) -> Self {
        // reqwest errors never include request headers, so no credential can leak here.
        GatewayError::Upstream(format!("control plane request failed: {e}"))
    }
}

impl From<meili_ingest_blob::BlobError> for GatewayError {
    fn from(e: meili_ingest_blob::BlobError) -> Self {
        GatewayError::Internal(format!("blob store: {e}"))
    }
}

impl From<axum::extract::multipart::MultipartError> for GatewayError {
    fn from(e: axum::extract::multipart::MultipartError) -> Self {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            GatewayError::TooLarge("multipart upload exceeds the configured size limit".into())
        } else {
            GatewayError::BadRequest(format!("invalid multipart body: {}", e.body_text()))
        }
    }
}

impl From<serde_json::Error> for GatewayError {
    fn from(e: serde_json::Error) -> Self {
        GatewayError::BadRequest(format!("invalid JSON: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn body_of(resp: Response) -> (StatusCode, ErrorBody) {
        let status = resp.status();
        let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20)
            .await
            .unwrap();
        let body: ErrorBody = serde_json::from_slice(&bytes).unwrap();
        (status, body)
    }

    #[test]
    fn status_mapping() {
        assert_eq!(
            GatewayError::MissingContext("x".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            GatewayError::BadRequest("x".into()).status(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            GatewayError::Forbidden("x".into()).status(),
            StatusCode::FORBIDDEN
        );
        assert_eq!(
            GatewayError::NotFound("x".into()).status(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            GatewayError::TooLarge("x".into()).status(),
            StatusCode::PAYLOAD_TOO_LARGE
        );
        assert_eq!(
            GatewayError::Unsupported("x".into()).status(),
            StatusCode::UNSUPPORTED_MEDIA_TYPE
        );
        assert_eq!(
            GatewayError::Invalid("x".into()).status(),
            StatusCode::UNPROCESSABLE_ENTITY
        );
        assert_eq!(
            GatewayError::Upstream("x".into()).status(),
            StatusCode::BAD_GATEWAY
        );
        assert_eq!(
            GatewayError::Internal("x".into()).status(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn codes_are_snake_case_and_unique() {
        let all = [
            GatewayError::MissingContext("x".into()),
            GatewayError::BadRequest("x".into()),
            GatewayError::Forbidden("x".into()),
            GatewayError::NotFound("x".into()),
            GatewayError::TooLarge("x".into()),
            GatewayError::Unsupported("x".into()),
            GatewayError::Invalid("x".into()),
            GatewayError::Unprocessable("x".into()),
            GatewayError::NotImplemented("x".into()),
            GatewayError::Upstream("x".into()),
            GatewayError::Internal("x".into()),
        ];
        let codes: std::collections::HashSet<&str> = all.iter().map(|e| e.code()).collect();
        assert_eq!(codes.len(), all.len());
        for c in codes {
            assert!(
                c.chars().all(|ch| ch.is_ascii_lowercase() || ch == '_'),
                "{c}"
            );
        }
    }

    #[test]
    fn unprocessable_is_422_without_the_pipeline_wording() {
        // `Invalid` reads "invalid pipeline: …", which is wrong for a rejected connection.
        let e = GatewayError::Unprocessable("the API key was rejected".into());
        assert_eq!(e.status(), StatusCode::UNPROCESSABLE_ENTITY);
        assert_eq!(e.code(), "validation");
        assert_eq!(e.to_string(), "the API key was rejected");
    }

    #[tokio::test]
    async fn response_body_shape() {
        let (status, body) =
            body_of(GatewayError::Unsupported("video/x-foo".into()).into_response()).await;
        assert_eq!(status, StatusCode::UNSUPPORTED_MEDIA_TYPE);
        assert_eq!(body.code, "unsupported_media_type");
        assert!(body.error.contains("video/x-foo"));
    }

    #[tokio::test]
    async fn missing_context_body() {
        let (status, body) =
            body_of(GatewayError::MissingContext("no host".into()).into_response()).await;
        assert_eq!(status, StatusCode::BAD_REQUEST);
        assert_eq!(body.code, "missing_context");
        assert_eq!(body.error, "missing Meilisearch context: no host");
    }

    #[test]
    fn serde_json_error_maps_to_bad_request() {
        let e: GatewayError = serde_json::from_str::<serde_json::Value>("{")
            .unwrap_err()
            .into();
        assert!(matches!(e, GatewayError::BadRequest(_)));
    }
}
