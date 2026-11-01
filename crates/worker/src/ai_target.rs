//! Resolving the Gateway target of a Lab tenant's AI step (`LAB_AI_GATEWAY`).
//!
//! Behind the Meilisearch Lab, an AI step of a Lab account runs on the team's hosted
//! Gateway with a per-team key, not on this worker's own provider key. Workers hold no
//! Lab credentials, so just before such a step the activity asks the control plane
//! (`GET /internal/lab/ai/{account_id}`, with `CONTROL_PLANE_TOKEN`), which asks the
//! Lab. Like a connection key (see [`crate::connection`]), the answer stays in this
//! process: it is attached to the step's plugin context and is never part of the
//! activity input, so the key never reaches Temporal history, a log line or an error.

use meili_ingest_plugin_sdk::{AiTarget, PluginError};
use serde::Deserialize;
use url::Url;

use crate::connection::{ControlPlane, authed};

/// Plugins that call the team's Gateway when the worker attaches a target.
pub const LAB_AI_PLUGINS: [&str; 3] = ["llm_enricher", "image_captioner", "jev_enricher"];

/// `200` answer of `GET /internal/lab/ai/{account_id}`. Declared here rather than
/// imported so the worker does not link the control plane.
#[derive(Deserialize)]
struct Answer {
    target: Option<AiTarget>,
}

/// Error body of the control plane (`{"error": "...", "code": "..."}`). Its messages
/// never carry a key.
#[derive(Deserialize, Default)]
struct ErrorBody {
    #[serde(default)]
    error: String,
    #[serde(default)]
    code: String,
}

/// Longest control plane error message carried into a step error, in chars.
const ERROR_CHARS: usize = 300;

/// The Gateway target of Lab account `tenant`, or `None` when the control plane has
/// none to serve (no Lab credentials, or not a Lab account).
///
/// `rejected_key_id` reports a key the Gateway just refused, so the Lab replaces it.
/// `refresh` (a retried step) makes the control plane ask the Lab even when it holds a
/// fresh cached target: with several replicas, the one answering may not have seen a
/// key rotation yet. A team without a Gateway (`404 no_gateway`) and a control plane
/// that refuses to serve keys without its token (`403 token_required`) are final
/// (non-retryable); any other failure, including a 404 without `no_gateway` (the route
/// missing during a rolling deploy), is retryable.
pub async fn fetch_ai_target(
    cp: &ControlPlane<'_>,
    tenant: &str,
    rejected_key_id: Option<&str>,
    refresh: bool,
) -> Result<Option<AiTarget>, PluginError> {
    let base = cp.base_url.ok_or_else(|| {
        PluginError::NonRetryable(
            "LAB_AI_GATEWAY needs CONTROL_PLANE_URL: no control plane is configured on this \
             worker"
                .into(),
        )
    })?;
    let mut url = Url::parse(&format!("{base}/internal/lab/ai/"))
        .map_err(|e| PluginError::NonRetryable(format!("control plane url: {e}")))?;
    // One path segment, so an id can never escape it.
    url.path_segments_mut()
        .map_err(|()| PluginError::NonRetryable("control plane url cannot be a base".into()))?
        .pop_if_empty()
        .push(tenant);
    if let Some(key_id) = rejected_key_id {
        url.query_pairs_mut().append_pair("rejected_key_id", key_id);
    }
    if refresh {
        url.query_pairs_mut().append_pair("refresh", "true");
    }

    let resp = authed(cp.http.get(url), cp.token)
        .send()
        .await
        .map_err(|e| {
            PluginError::Retryable(format!(
                "control plane unreachable for the Lab Gateway target: {}",
                e.without_url()
            ))
        })?;
    let status = resp.status();
    if !status.is_success() {
        // Only the `error` and `code` fields are kept: never the raw body.
        let body: ErrorBody = resp
            .bytes()
            .await
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let detail = if body.code.is_empty() && body.error.is_empty() {
            String::new()
        } else {
            format!(
                ": {} ({})",
                truncate_chars(&body.error, ERROR_CHARS),
                truncate_chars(&body.code, 64)
            )
        };
        return Err(match (status.as_u16(), body.code.as_str()) {
            (404, "no_gateway") => {
                PluginError::NonRetryable(format!("this team has no Gateway in the Lab{detail}"))
            }
            (403, "token_required") => PluginError::NonRetryable(format!(
                "the control plane serves no Lab Gateway target without CONTROL_PLANE_TOKEN \
                 (HTTP 403){detail}"
            )),
            (404, _) => PluginError::Retryable(format!(
                "control plane answered 404 for the Lab Gateway target (is \
                 /internal/lab/ai deployed on every replica?){detail}"
            )),
            _ => PluginError::Retryable(format!(
                "control plane returned {status} for the Lab Gateway target{detail}"
            )),
        });
    }
    // The body carries the key: never put the parse error (which can quote it) in
    // the message.
    let answer: Answer = resp.json().await.map_err(|_| {
        PluginError::Retryable("control plane sent an unreadable Lab Gateway target".into())
    })?;
    Ok(answer.target)
}

/// Truncate to at most `max` chars on a char boundary.
fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
    const KEY: &str = "sk-lab-SECRET";

    fn cp<'a>(http: &'a reqwest::Client, base: &'a str) -> ControlPlane<'a> {
        ControlPlane {
            http,
            base_url: Some(base),
            token: Some("cp-token"),
        }
    }

    async fn answering(status: u16, body: serde_json::Value) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/lab/ai/{ACCOUNT}")))
            .and(header("authorization", "Bearer cp-token"))
            .respond_with(ResponseTemplate::new(status).set_body_json(body))
            .mount(&server)
            .await;
        server
    }

    #[tokio::test]
    async fn a_target_is_returned_and_cache_ttl_ignored() {
        let server = answering(
            200,
            serde_json::json!({ "target": {
                "base_url": "http://lumen:8080/v1", "api_key": KEY, "key_id": "k1",
                "default_model": "m", "vision_model": "v", "decision_model": "d",
                "cache_ttl": 300,
            }}),
        )
        .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let t = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect("answer")
            .expect("a target");
        assert_eq!(t.key_id, "k1");
        assert_eq!(t.api_key, KEY);
        assert_eq!(t.decision_model.as_deref(), Some("d"));
        assert!(!t.is_rejected());
    }

    #[tokio::test]
    async fn a_null_target_is_none() {
        let server = answering(200, serde_json::json!({ "target": null })).await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let t = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect("answer");
        assert!(t.is_none());
    }

    #[tokio::test]
    async fn a_rejected_key_is_sent_as_a_query() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/lab/ai/{ACCOUNT}")))
            .and(query_param("rejected_key_id", "k1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "target": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        fetch_ai_target(&cp(&http, &base), ACCOUNT, Some("k1"), false)
            .await
            .expect("answer");
    }

    #[tokio::test]
    async fn status_mapping() {
        let http = reqwest::Client::new();
        let no_gateway = answering(
            404,
            serde_json::json!({"error": "account has no Gateway", "code": "no_gateway"}),
        )
        .await;
        let base = no_gateway.uri();
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect_err("404");
        assert!(
            matches!(&err, PluginError::NonRetryable(m)
                if m.starts_with("this team has no Gateway in the Lab")
                    && m.contains("account has no Gateway (no_gateway)")),
            "{err:?}"
        );

        let token_required = answering(
            403,
            serde_json::json!({"error": "no token", "code": "token_required"}),
        )
        .await;
        let base = token_required.uri();
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect_err("403");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("token_required")),
            "{err:?}"
        );

        for status in [503, 500, 401] {
            let server = answering(
                status,
                serde_json::json!({"error": "the Lab is down", "code": "lab_unavailable"}),
            )
            .await;
            let base = server.uri();
            let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
                .await
                .expect_err("failure");
            assert!(err.is_retryable(), "{status}: {err:?}");
            assert!(
                err.to_string()
                    .contains("the Lab is down (lab_unavailable)"),
                "{status}: {err}"
            );
        }
    }

    #[tokio::test]
    async fn a_404_without_no_gateway_is_retryable() {
        let http = reqwest::Client::new();
        // A replica without the route (rolling deploy): axum's bare 404, or another code.
        let empty = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&empty)
            .await;
        let base = empty.uri();
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect_err("404");
        assert!(err.is_retryable(), "{err:?}");
        assert!(err.to_string().contains("control plane"), "{err}");

        let other = answering(
            404,
            serde_json::json!({"error": "no such thing", "code": "not_found"}),
        )
        .await;
        let base = other.uri();
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect_err("404");
        assert!(err.is_retryable(), "{err:?}");
        assert!(err.to_string().contains("(not_found)"), "{err}");
    }

    #[tokio::test]
    async fn refresh_is_sent_only_when_asked() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/lab/ai/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "target": null
            })))
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect("answer");
        fetch_ai_target(&cp(&http, &base), ACCOUNT, None, true)
            .await
            .expect("answer");
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs[0].url.query(), None);
        assert_eq!(reqs[1].url.query(), Some("refresh=true"));
    }

    #[tokio::test]
    async fn an_unreadable_body_does_not_quote_it() {
        // `key_id` has the wrong type, so the parse fails after reading the key.
        let server = answering(
            200,
            serde_json::json!({ "target": {
                "base_url": "u", "api_key": KEY, "key_id": 7,
                "default_model": "m", "vision_model": "v",
            }}),
        )
        .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None, false)
            .await
            .expect_err("unreadable");
        assert!(err.is_retryable(), "{err:?}");
        assert!(!err.to_string().contains(KEY), "{err}");
    }

    #[tokio::test]
    async fn transport_errors_are_retryable_and_no_url_is_final() {
        let http = reqwest::Client::new();
        // Nothing listens on port 9 of the loopback.
        let err = fetch_ai_target(&cp(&http, "http://127.0.0.1:9"), ACCOUNT, None, false)
            .await
            .expect_err("unreachable");
        assert!(err.is_retryable(), "{err:?}");

        let none = ControlPlane {
            http: &http,
            base_url: None,
            token: None,
        };
        let err = fetch_ai_target(&none, ACCOUNT, None, false)
            .await
            .expect_err("no control plane");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("CONTROL_PLANE_URL")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn an_account_id_cannot_escape_its_path_segment() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/lab/ai/..%2Fjobs"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "target": null
            })))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let _ = fetch_ai_target(&cp(&http, &base), "../jobs", None, false).await;
    }
}
