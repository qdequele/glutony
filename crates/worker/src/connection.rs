//! Resolving a `meili_indexer` step's Meilisearch connection (spec Decision 13).
//!
//! The workflow passes only the connection's *name* in the step config. This module
//! runs inside the activity, just before the plugin: it fetches the connection from the
//! control plane, opens its sealed key in memory, re-checks the host against the
//! deployment's policy, and merges `host`/`api_key` into a config that exists only in
//! this process. The key is therefore never part of the activity input, and never
//! recorded in Temporal history.
//!
//! Failure modes are deliberately split: anything that retrying cannot fix (unknown
//! connection, missing or wrong `SOURCE_SECRET_KEY`, a host the policy forbids) is
//! non-retryable and names the connection; transport and DNS trouble is retryable.

use std::sync::Arc;

use meili_ingest_plugin_sdk::{INDEXER_PLUGIN, PluginError, pinned_connection};
use meili_ingest_source::{HostPolicy, SecretKey, SourceError};
use serde::Deserialize;
use serde_json::Value;
use url::Url;

/// What this worker needs to resolve connections.
#[derive(Debug, Clone, Default)]
pub struct ConnectionSettings {
    /// Key that opens sealed connection keys. `None` when `SOURCE_SECRET_KEY` is unset;
    /// every step naming a connection then fails non-retryably.
    pub key: Option<Arc<SecretKey>>,
    /// Which hosts a connection may point at (`MEILI_CONNECTION_HOSTS`).
    pub policy: HostPolicy,
}

/// The fields of `GET /internal/connections/{uid}` this worker reads. Declared here
/// rather than imported so the worker does not link the control plane.
#[derive(Deserialize)]
struct ConnectionRow {
    host: String,
    api_key: Vec<u8>,
}

/// Where to reach the control plane, and how.
pub struct ControlPlane<'a> {
    /// Shared HTTP client.
    pub http: &'a reqwest::Client,
    /// Base URL, trailing slash trimmed. `None` when not configured.
    pub base_url: Option<&'a str>,
}

/// Return `config` with `host`/`api_key` resolved from the step's connection.
///
/// Configs that do not belong to `meili_indexer`, or that name no connection, are
/// returned untouched without any I/O.
pub async fn resolve_connection(
    control_plane: &ControlPlane<'_>,
    settings: &ConnectionSettings,
    plugin: &str,
    mut config: Value,
    project_id: Option<&str>,
) -> Result<Value, PluginError> {
    if plugin != INDEXER_PLUGIN {
        return Ok(config);
    }
    let Some(name) = pinned_connection(&config).map(str::to_owned) else {
        return Ok(config);
    };
    let fail = |why: String| PluginError::NonRetryable(format!("connection {name:?}: {why}"));

    let key = settings
        .key
        .as_ref()
        .ok_or_else(|| fail("SOURCE_SECRET_KEY is not set on this worker".into()))?;
    let base = control_plane
        .base_url
        .ok_or_else(|| fail("no control plane is configured on this worker".into()))?;

    let row = fetch(control_plane.http, base, &name, project_id).await?;

    let api_key = key.open(&row.api_key).map_err(|_| {
        fail(
            "its key could not be decrypted (is SOURCE_SECRET_KEY the one it was saved with?)"
                .into(),
        )
    })?;
    let api_key = String::from_utf8(api_key)
        .map_err(|_| fail("its decrypted key is not valid UTF-8".into()))?;

    let host = Url::parse(row.host.trim())
        .map_err(|e| fail(format!("its host {:?} is not a URL: {e}", row.host)))?;
    // Re-checked at use, not only at save: DNS can change in between.
    settings.policy.check(&host).await.map_err(|e| match e {
        SourceError::Dns(msg) => PluginError::Retryable(format!("connection {name:?}: {msg}")),
        other => fail(other.to_string()),
    })?;

    if let Some(obj) = config.as_object_mut() {
        obj.insert("host".into(), Value::String(row.host.trim().to_owned()));
        obj.insert("api_key".into(), Value::String(api_key));
    }
    Ok(config)
}

async fn fetch(
    http: &reqwest::Client,
    base: &str,
    name: &str,
    project_id: Option<&str>,
) -> Result<ConnectionRow, PluginError> {
    let mut url = Url::parse(&format!("{base}/internal/connections/"))
        .map_err(|e| PluginError::NonRetryable(format!("control plane url: {e}")))?;
    // Append the name as one path segment so a name can never escape it.
    url.path_segments_mut()
        .map_err(|()| PluginError::NonRetryable("control plane url cannot be a base".into()))?
        .pop_if_empty()
        .push(name);
    if let Some(p) = project_id {
        url.query_pairs_mut().append_pair("project_id", p);
    }

    let resp = http
        .get(url)
        .send()
        .await
        .map_err(|e| PluginError::Retryable(format!("control plane unreachable: {e}")))?;
    let status = resp.status();
    if status == reqwest::StatusCode::NOT_FOUND {
        return Err(PluginError::NonRetryable(format!(
            "connection {name:?} not found (project_id={project_id:?}); it may have been \
             deleted while this pipeline still names it"
        )));
    }
    if !status.is_success() {
        return Err(PluginError::Retryable(format!(
            "control plane returned {status} for connection {name:?}"
        )));
    }
    resp.json::<ConnectionRow>().await.map_err(|e| {
        PluginError::NonRetryable(format!("connection {name:?}: unreadable response: {e}"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const PLAINTEXT_KEY: &str = "sk-movies-PLAINTEXT";

    fn secret() -> Arc<SecretKey> {
        Arc::new(SecretKey::from_bytes([3u8; 32]))
    }

    fn settings(policy: HostPolicy) -> ConnectionSettings {
        ConnectionSettings {
            key: Some(secret()),
            policy,
        }
    }

    async fn control_plane_with(host: &str, sealed: Vec<u8>) -> MockServer {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/connections/prod-movies"))
            .and(query_param("project_id", "tenant-1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "id": "11111111-1111-1111-1111-111111111111",
                "uid": "prod-movies",
                "name": "Movies",
                "project_id": "tenant-1",
                "host": host,
                "api_key": sealed,
                "created_at": "2026-09-23T00:00:00Z",
                "updated_at": "2026-09-23T00:00:00Z",
            })))
            .mount(&server)
            .await;
        server
    }

    fn sealed_key() -> Vec<u8> {
        secret().seal(PLAINTEXT_KEY.as_bytes()).expect("seal")
    }

    #[tokio::test]
    async fn other_plugins_and_unpinned_indexers_are_untouched() {
        let http = reqwest::Client::new();
        let cp = ControlPlane {
            http: &http,
            base_url: Some("http://unused.invalid"),
        };
        let s = settings(HostPolicy::Any);
        let cfg = serde_json::json!({ "connection": "prod-movies" });

        let out = resolve_connection(&cp, &s, "json_parser", cfg.clone(), None)
            .await
            .expect("not an indexer");
        assert_eq!(out, cfg, "only meili_indexer steps are resolved");

        let unpinned = serde_json::json!({ "index": "movies" });
        let out = resolve_connection(&cp, &s, INDEXER_PLUGIN, unpinned.clone(), None)
            .await
            .expect("no connection");
        assert_eq!(out, unpinned);
    }

    #[tokio::test]
    async fn a_connection_resolves_to_its_host_and_decrypted_key() {
        let server = control_plane_with("http://meilisearch:7700", sealed_key()).await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
        };
        let out = resolve_connection(
            &cp,
            &settings(HostPolicy::parse("meilisearch:7700").expect("policy")),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies", "index": "movies" }),
            Some("tenant-1"),
        )
        .await
        .expect("resolves");
        assert_eq!(out["host"], "http://meilisearch:7700");
        assert_eq!(out["api_key"], PLAINTEXT_KEY);
        assert_eq!(out["index"], "movies", "other keys survive");
    }

    #[tokio::test]
    async fn a_missing_connection_is_non_retryable_and_named() {
        let server = MockServer::start().await; // every path 404s
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
        };
        let err = resolve_connection(
            &cp,
            &settings(HostPolicy::Any),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies" }),
            Some("tenant-1"),
        )
        .await
        .expect_err("missing");
        let PluginError::NonRetryable(msg) = err else {
            panic!("a deleted connection cannot be fixed by retrying: {err:?}");
        };
        assert!(
            msg.contains("prod-movies") && msg.contains("not found"),
            "{msg}"
        );
    }

    #[tokio::test]
    async fn a_worker_without_the_secret_key_fails_clearly() {
        let http = reqwest::Client::new();
        let cp = ControlPlane {
            http: &http,
            base_url: Some("http://unused.invalid"),
        };
        let err = resolve_connection(
            &cp,
            &ConnectionSettings::default(),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies" }),
            None,
        )
        .await
        .expect_err("no key");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("SOURCE_SECRET_KEY")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_key_sealed_with_another_secret_is_rejected_without_leaking_it() {
        let other = SecretKey::from_bytes([9u8; 32]);
        let foreign = other.seal(PLAINTEXT_KEY.as_bytes()).expect("seal");
        let server = control_plane_with("http://meilisearch:7700", foreign).await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
        };
        let err = resolve_connection(
            &cp,
            &settings(HostPolicy::Any),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies" }),
            Some("tenant-1"),
        )
        .await
        .expect_err("wrong key");
        let PluginError::NonRetryable(msg) = err else {
            panic!("{err:?}");
        };
        assert!(msg.contains("decrypted"), "{msg}");
        assert!(
            !msg.contains(PLAINTEXT_KEY),
            "an error must never carry the key"
        );
    }

    #[tokio::test]
    async fn the_host_policy_is_re_applied_at_use() {
        // Saved when the policy allowed it; the deployment has since gone strict.
        let server = control_plane_with("http://meilisearch:7700", sealed_key()).await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
        };
        let err = resolve_connection(
            &cp,
            &settings(HostPolicy::Public),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "prod-movies" }),
            Some("tenant-1"),
        )
        .await
        .expect_err("blocked");
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("prod-movies")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_connection_name_cannot_escape_its_path_segment() {
        let server = MockServer::start().await;
        // Only this exact, encoded path answers; a traversal would hit something else.
        Mock::given(method("GET"))
            .and(path("/internal/connections/..%2Fjobs"))
            .respond_with(ResponseTemplate::new(404))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let base = server.uri();
        let cp = ControlPlane {
            http: &http,
            base_url: Some(&base),
        };
        let _ = resolve_connection(
            &cp,
            &settings(HostPolicy::Any),
            INDEXER_PLUGIN,
            serde_json::json!({ "connection": "../jobs" }),
            None,
        )
        .await;
        // `expect(1)` is verified when the server drops.
    }
}
