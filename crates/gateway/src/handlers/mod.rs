//! HTTP handlers, one module per route group (SPEC §6).

pub mod catalog;
pub mod connections;
pub mod ingest;
pub mod jobs;
pub mod pipeline;
pub mod pipelines;
pub mod plugins;
pub mod sources;
pub mod usage;

use std::collections::HashMap;

use axum::body::Bytes;
use axum::extract::{FromRequest, FromRequestParts, Multipart, Request};
use axum::http::header::{CONTENT_DISPOSITION, CONTENT_TYPE};
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use serde::de::DeserializeOwned;

use crate::error::GatewayError;
use crate::extract::{Extracted, extract_payload, is_multipart};

/// The query string, deserialized like [`axum::extract::Query`], but refused with the
/// gateway's JSON `400 bad_request` rather than axum's plain-text rejection, so every
/// error keeps the `{"error", "code"}` shape the API documents.
#[derive(Debug, Clone, Copy, Default)]
pub struct Query<T>(pub T);

impl<S, T> FromRequestParts<S> for Query<T>
where
    S: Send + Sync,
    T: DeserializeOwned,
{
    type Rejection = GatewayError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        match axum::extract::Query::<T>::from_request_parts(parts, state).await {
            Ok(axum::extract::Query(value)) => Ok(Self(value)),
            Err(rejection) => Err(GatewayError::BadRequest(format!(
                "invalid query string: {}",
                rejection.body_text()
            ))),
        }
    }
}

/// Query parameters accepted by the ingest routes.
pub type QueryParams = HashMap<String, String>;

/// Non-empty query parameter.
pub fn query_param<'a>(query: &'a QueryParams, name: &str) -> Option<&'a str> {
    query
        .get(name)
        .map(String::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
}

/// Refuse a `{name}` path parameter that cannot be a pipeline uid, before it is used to
/// address the control plane.
pub(crate) fn check_pipeline_name(name: &str) -> Result<(), GatewayError> {
    if crate::connections::valid_pipeline_uid(name) {
        Ok(())
    } else {
        Err(GatewayError::BadRequest(format!(
            "invalid pipeline name {name:?}: must be made of [a-zA-Z0-9._-]"
        )))
    }
}

/// `filename` from a `Content-Disposition` header (`filename="x"` or `filename=x`).
pub fn content_disposition_filename(headers: &HeaderMap) -> Option<String> {
    let raw = headers.get(CONTENT_DISPOSITION)?.to_str().ok()?;
    raw.split(';').map(str::trim).find_map(|part| {
        let (key, value) = part.split_once('=')?;
        if !key.trim().eq_ignore_ascii_case("filename") {
            return None;
        }
        let v = value.trim().trim_matches('"').trim();
        if v.is_empty() {
            None
        } else {
            Some(v.to_string())
        }
    })
}

/// Read the request body (multipart or bytes, honouring the body limit) and extract the
/// ingest payload. `filename_hint` comes from `?filename=` or `Content-Disposition`.
pub async fn read_payload(
    headers: &HeaderMap,
    query: &QueryParams,
    req: Request,
) -> Result<Extracted, GatewayError> {
    let content_type = headers
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let filename_hint = query_param(query, "filename")
        .map(str::to_string)
        .or_else(|| content_disposition_filename(headers));
    if is_multipart(content_type.as_deref()) {
        let mp = Multipart::from_request(req, &()).await.map_err(|e| {
            if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
                GatewayError::TooLarge("upload exceeds the configured size limit".into())
            } else {
                GatewayError::BadRequest(format!("invalid multipart request: {}", e.body_text()))
            }
        })?;
        return extract_payload(
            content_type.as_deref(),
            Bytes::new(),
            Some(mp),
            filename_hint.as_deref(),
        )
        .await;
    }
    let body = Bytes::from_request(req, &()).await.map_err(|e| {
        if e.status() == StatusCode::PAYLOAD_TOO_LARGE {
            GatewayError::TooLarge("request body exceeds the configured size limit".into())
        } else {
            GatewayError::BadRequest(format!("cannot read request body: {}", e.body_text()))
        }
    })?;
    extract_payload(
        content_type.as_deref(),
        body,
        None,
        filename_hint.as_deref(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    #[test]
    fn content_disposition_parsing() {
        let mut h = HeaderMap::new();
        h.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("attachment; filename=\"report.pdf\""),
        );
        assert_eq!(
            content_disposition_filename(&h).as_deref(),
            Some("report.pdf")
        );
        h.insert(
            CONTENT_DISPOSITION,
            HeaderValue::from_static("inline; filename=plain.txt"),
        );
        assert_eq!(
            content_disposition_filename(&h).as_deref(),
            Some("plain.txt")
        );
        h.insert(CONTENT_DISPOSITION, HeaderValue::from_static("attachment"));
        assert_eq!(content_disposition_filename(&h), None);
        assert_eq!(content_disposition_filename(&HeaderMap::new()), None);
    }

    #[test]
    fn query_param_ignores_blank() {
        let mut q = QueryParams::new();
        q.insert("index".into(), "  ".into());
        q.insert("pipeline".into(), " p ".into());
        assert_eq!(query_param(&q, "index"), None);
        assert_eq!(query_param(&q, "pipeline"), Some("p"));
        assert_eq!(query_param(&q, "nope"), None);
    }
}
