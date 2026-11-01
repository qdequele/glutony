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

/// The Gateway target of Lab account `tenant`, or `None` when the control plane has
/// none to serve (no Lab credentials, or not a Lab account).
///
/// `rejected_key_id` reports a key the Gateway just refused, so the Lab replaces it.
/// A team without a Gateway is final (non-retryable); a Lab or control plane that
/// cannot answer is retryable.
pub async fn fetch_ai_target(
    cp: &ControlPlane<'_>,
    tenant: &str,
    rejected_key_id: Option<&str>,
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
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(PluginError::NonRetryable(
            "this team has no Gateway in the Lab".into(),
        ));
    }
    if !status.is_success() {
        return Err(PluginError::Retryable(format!(
            "control plane returned {status} for the Lab Gateway target"
        )));
    }
    // The body carries the key: never put the parse error (which can quote it) in
    // the message.
    let answer: Answer = resp.json().await.map_err(|_| {
        PluginError::Retryable("control plane sent an unreadable Lab Gateway target".into())
    })?;
    Ok(answer.target)
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
        let t = fetch_ai_target(&cp(&http, &base), ACCOUNT, None)
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
        let t = fetch_ai_target(&cp(&http, &base), ACCOUNT, None)
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
        fetch_ai_target(&cp(&http, &base), ACCOUNT, Some("k1"))
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
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None)
            .await
            .expect_err("404");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m == "this team has no Gateway in the Lab"),
            "{err:?}"
        );

        for status in [503, 500, 401] {
            let server = answering(
                status,
                serde_json::json!({"error": "x", "code": "lab_unavailable"}),
            )
            .await;
            let base = server.uri();
            let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None)
                .await
                .expect_err("failure");
            assert!(err.is_retryable(), "{status}: {err:?}");
        }
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
        let err = fetch_ai_target(&cp(&http, &base), ACCOUNT, None)
            .await
            .expect_err("unreadable");
        assert!(err.is_retryable(), "{err:?}");
        assert!(!err.to_string().contains(KEY), "{err}");
    }

    #[tokio::test]
    async fn transport_errors_are_retryable_and_no_url_is_final() {
        let http = reqwest::Client::new();
        // Nothing listens on port 9 of the loopback.
        let err = fetch_ai_target(&cp(&http, "http://127.0.0.1:9"), ACCOUNT, None)
            .await
            .expect_err("unreachable");
        assert!(err.is_retryable(), "{err:?}");

        let none = ControlPlane {
            http: &http,
            base_url: None,
            token: None,
        };
        let err = fetch_ai_target(&none, ACCOUNT, None)
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
        let _ = fetch_ai_target(&cp(&http, &base), "../jobs", None).await;
    }
}
