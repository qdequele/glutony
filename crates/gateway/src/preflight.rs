//! Write preflight (`WRITE_PREFLIGHT`): refuse an ingest at request time when the
//! caller's Meilisearch key could not write the job's result, instead of queueing a job
//! that fails at the `meili_indexer` step.
//!
//! Meilisearch has no "can this key do X?" endpoint, so the check asks the real routes
//! in a way that has no side effect:
//!
//! 1. `POST /indexes/{uid}/documents` with an empty body and a Content-Type Meilisearch
//!    does not accept. Authorization is an extractor that runs before the handler, so a
//!    key without `documents.add` on `uid` gets 401/403, while an allowed key reaches the
//!    handler and gets `415 invalid_content_type` — before any task is registered and
//!    before the index is auto-created. Verified against Meilisearch v1.49 and v1.54.
//! 2. `GET /indexes/{uid}` — `meili_indexer` reads the index before writing to it, so the
//!    key also needs `indexes.get`. 200 and 404 (not created yet) both pass.
//!
//! Only 401 and 403 fail the check. Anything unexpected (Meilisearch down, a version
//! that answers differently) is logged and let through: the indexer still enforces the
//! key at write time, so the preflight can only make a failure earlier, never allow one.

use std::time::Duration;

use reqwest::StatusCode;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;

use crate::error::GatewayError;

/// A Content-Type no Meilisearch version accepts for documents.
const PROBE_CONTENT_TYPE: &str = "application/x-meili-ingest-preflight";
/// Loopback in practice; generous enough for a Cloud project across a region.
const PROBE_TIMEOUT: Duration = Duration::from_secs(10);

/// Meilisearch error body; only the message is surfaced.
#[derive(Deserialize)]
struct MeiliErrorBody {
    message: String,
}

/// `{host}/indexes/{index}{suffix}`, with the index uid as one encoded path segment.
fn index_url(host: &str, index: &str, suffix: &[&str]) -> Result<url::Url, GatewayError> {
    let mut url = url::Url::parse(host)
        .map_err(|e| GatewayError::BadRequest(format!("invalid Meilisearch host {host:?}: {e}")))?;
    url.path_segments_mut()
        .map_err(|()| GatewayError::BadRequest(format!("invalid Meilisearch host {host:?}")))?
        .pop_if_empty()
        .push("indexes")
        .push(index)
        .extend(suffix);
    Ok(url)
}

/// Map an auth refusal onto the gateway error, keeping Meilisearch's own message (it
/// names the index the key is limited to, which is exactly what the caller needs).
async fn refusal(resp: reqwest::Response, what: &str) -> GatewayError {
    let status = resp.status();
    let message = resp
        .json::<MeiliErrorBody>()
        .await
        .map(|b| b.message)
        .unwrap_or_else(|_| format!("Meilisearch answered {status}"));
    let message = format!("the API key cannot {what}: {message}");
    if status == StatusCode::UNAUTHORIZED {
        GatewayError::Unauthorized(message)
    } else {
        GatewayError::Forbidden(message)
    }
}

fn is_refusal(status: StatusCode) -> bool {
    matches!(status, StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN)
}

/// Check that `api_key` may add documents to, and read, `index` on `host`.
pub async fn check_write(
    http: &reqwest::Client,
    host: &str,
    api_key: &str,
    index: &str,
) -> Result<(), GatewayError> {
    let add = http
        .post(index_url(host, index, &["documents"])?)
        .bearer_auth(api_key)
        .header(CONTENT_TYPE, PROBE_CONTENT_TYPE)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await;
    match add {
        Ok(resp) if is_refusal(resp.status()) => {
            return Err(refusal(resp, &format!("add documents to index {index:?}")).await);
        }
        Ok(resp) if resp.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE => {}
        Ok(resp) => tracing::warn!(
            index,
            status = %resp.status(),
            "write preflight: unexpected answer to the documents probe; letting the job \
             enforce the key"
        ),
        Err(e) => {
            tracing::warn!(
                index,
                "write preflight: Meilisearch unreachable ({e}); skipping"
            );
            return Ok(());
        }
    }

    let get = http
        .get(index_url(host, index, &[])?)
        .bearer_auth(api_key)
        .timeout(PROBE_TIMEOUT)
        .send()
        .await;
    match get {
        Ok(resp) if is_refusal(resp.status()) => {
            Err(refusal(resp, &format!("read index {index:?} (indexes.get)")).await)
        }
        Ok(_) => Ok(()),
        Err(e) => {
            tracing::warn!(
                index,
                "write preflight: Meilisearch unreachable ({e}); skipping"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    async fn mount(server: &MockServer, add_status: u16, get_status: u16) {
        Mock::given(method("POST"))
            .and(path("/indexes/movies/documents"))
            .and(header("content-type", PROBE_CONTENT_TYPE))
            .and(header("authorization", "Bearer k"))
            .respond_with(ResponseTemplate::new(add_status).set_body_json(json!({
                "message": "The API key cannot acces the index `movies`, authorized indexes are [\"glutony-*\"].",
                "code": "invalid_api_key"
            })))
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/indexes/movies"))
            .respond_with(ResponseTemplate::new(get_status).set_body_json(json!({
                "message": "no indexes.get", "code": "invalid_api_key"
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn allowed_key_passes_even_when_the_index_does_not_exist_yet() {
        let server = MockServer::start().await;
        mount(&server, 415, 404).await;
        check_write(&reqwest::Client::new(), &server.uri(), "k", "movies")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn key_scoped_to_other_indexes_is_forbidden_with_meilisearch_message() {
        let server = MockServer::start().await;
        mount(&server, 403, 200).await;
        let err = check_write(&reqwest::Client::new(), &server.uri(), "k", "movies")
            .await
            .unwrap_err();
        assert!(matches!(err, GatewayError::Forbidden(_)), "{err:?}");
        assert!(err.to_string().contains("authorized indexes"), "{err}");
    }

    #[tokio::test]
    async fn missing_key_is_unauthorized() {
        let server = MockServer::start().await;
        mount(&server, 401, 200).await;
        let err = check_write(&reqwest::Client::new(), &server.uri(), "k", "movies")
            .await
            .unwrap_err();
        assert_eq!(err.status(), axum::http::StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn key_without_indexes_get_is_forbidden() {
        let server = MockServer::start().await;
        mount(&server, 415, 403).await;
        let err = check_write(&reqwest::Client::new(), &server.uri(), "k", "movies")
            .await
            .unwrap_err();
        assert!(err.to_string().contains("indexes.get"), "{err}");
    }

    #[tokio::test]
    async fn unreachable_meilisearch_does_not_block() {
        check_write(&reqwest::Client::new(), "http://127.0.0.1:9", "k", "movies")
            .await
            .unwrap();
    }

    #[test]
    fn index_uid_is_one_encoded_segment_and_host_path_is_kept() {
        let u = index_url("https://h.example/base/", "a b", &["documents"]).unwrap();
        assert_eq!(u.as_str(), "https://h.example/base/indexes/a%20b/documents");
    }
}
