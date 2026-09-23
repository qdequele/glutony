//! Meilisearch connections, gateway side: the control-plane calls, save-time validation,
//! and the redacted public shape.
//!
//! The gateway is one of only two places that ever see a connection's plaintext key —
//! here, briefly, to seal it and to prove it works against the target Meilisearch; and
//! in the worker activity, to use it. The control plane only stores sealed bytes, and
//! the API never returns a key (spec *Redaction*).

use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use meili_ingest_source::{HostPolicy, SecretKey, SourceError};
use serde::{Deserialize, Serialize};
use url::Url;
use uuid::Uuid;

use crate::error::GatewayError;
use crate::state::ControlPlaneClient;

/// What a key is rendered as in every response.
pub const MASK: &str = "****";

/// Settings the `/connections` routes need. Absent key → every route answers 501.
#[derive(Clone)]
pub struct ConnectionConfig {
    /// Seals keys on save and opens a stored key to re-validate a host change.
    pub key: Option<Arc<SecretKey>>,
    /// `MEILI_CONNECTION_HOSTS`.
    pub policy: HostPolicy,
    /// Client used to probe a connection on save: short timeout, **no redirects**, so an
    /// allowed host cannot bounce the probe onto an internal address.
    pub probe: reqwest::Client,
}

impl std::fmt::Debug for ConnectionConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionConfig")
            .field("key", &self.key.as_ref().map(|_| "<set>"))
            .field("policy", &self.policy)
            .finish_non_exhaustive()
    }
}

impl ConnectionConfig {
    /// Settings with the given key and policy, and a probe client that follows no
    /// redirects.
    pub fn new(key: Option<Arc<SecretKey>>, policy: HostPolicy) -> Self {
        let probe = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_default();
        Self { key, policy, probe }
    }

    /// The sealing key, or the 501 every connection route returns without one.
    pub fn key(&self) -> Result<&SecretKey, GatewayError> {
        self.key.as_deref().ok_or_else(|| {
            GatewayError::NotImplemented(
                "Meilisearch connections are disabled: set SOURCE_SECRET_KEY (32 bytes, \
                 base64) on the gateway and the workers"
                    .into(),
            )
        })
    }
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        Self::new(None, HostPolicy::default())
    }
}

// ---------------------------------------------------------------------------
// Control-plane wire types (mirrors `meili-ingest-control-plane::connections`; the
// gateway does not link the control plane).
// ---------------------------------------------------------------------------

/// A stored connection, key still sealed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionRecord {
    /// Surrogate id.
    pub id: Uuid,
    /// Name an indexer step references.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Tenant scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Meilisearch URL.
    pub host: String,
    /// Sealed key.
    pub api_key: Vec<u8>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last write.
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Serialize)]
struct NewConnection<'a> {
    id: Uuid,
    uid: &'a str,
    name: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_id: Option<&'a str>,
    host: &'a str,
    api_key: &'a [u8],
}

#[derive(Debug, Default, Serialize)]
struct ConnectionPatch<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    host: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key: Option<&'a [u8]>,
}

// ---------------------------------------------------------------------------
// Public API shapes.
// ---------------------------------------------------------------------------

/// `POST /connections` body.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateConnection {
    /// Name an indexer step will reference.
    pub uid: String,
    /// Display name; defaults to the uid.
    #[serde(default)]
    pub name: Option<String>,
    /// Meilisearch URL.
    pub host: String,
    /// Plaintext key. Sealed before it leaves the gateway.
    pub api_key: String,
}

/// `PATCH /connections/{uid}` body. Omitting `api_key` keeps the stored one.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct UpdateConnection {
    /// New display name.
    #[serde(default)]
    pub name: Option<String>,
    /// New Meilisearch URL.
    #[serde(default)]
    pub host: Option<String>,
    /// New plaintext key.
    #[serde(default)]
    pub api_key: Option<String>,
}

/// A connection as the API returns it: the key is always [`MASK`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConnectionView {
    /// Name an indexer step references.
    pub uid: String,
    /// Display name.
    pub name: String,
    /// Tenant scope.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    /// Meilisearch URL.
    pub host: String,
    /// Always [`MASK`].
    pub api_key: String,
    /// Pipelines whose indexer names this connection, on single-connection reads.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub used_by: Option<Vec<String>>,
    /// Creation time.
    pub created_at: DateTime<Utc>,
    /// Last write.
    pub updated_at: DateTime<Utc>,
}

impl ConnectionView {
    /// The redacted view of a stored record.
    pub fn from_record(r: ConnectionRecord, used_by: Option<Vec<String>>) -> Self {
        Self {
            uid: r.uid,
            name: r.name,
            project_id: r.project_id,
            host: r.host,
            api_key: MASK.into(),
            used_by,
            created_at: r.created_at,
            updated_at: r.updated_at,
        }
    }
}

// ---------------------------------------------------------------------------
// Validation.
// ---------------------------------------------------------------------------

/// Whether `uid` is a usable connection name: the same `[a-zA-Z0-9._-]+` rule as
/// pipeline uids, at most 128 bytes.
pub fn valid_uid(uid: &str) -> bool {
    !uid.is_empty()
        && uid.len() <= 128
        && uid
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// Normalize a host (trim whitespace and trailing slashes) and parse it.
pub fn normalize_host(raw: &str) -> Result<(String, Url), GatewayError> {
    let host = raw.trim().trim_end_matches('/').to_owned();
    let url = Url::parse(&host)
        .map_err(|e| GatewayError::Unprocessable(format!("host {host:?} is not a URL: {e}")))?;
    if url.query().is_some() || url.fragment().is_some() {
        return Err(GatewayError::Unprocessable(format!(
            "host {host:?} must not carry a query string or fragment"
        )));
    }
    Ok((host, url))
}

/// Prove a destination works before it is saved: the host policy, then
/// `GET /health`, then one authenticated call so a wrong key fails now rather than at
/// 3am inside a cron run (spec *Connection validation*).
///
/// Error messages name the host but never the key.
pub async fn validate_destination(
    cfg: &ConnectionConfig,
    host: &str,
    url: &Url,
    api_key: &str,
) -> Result<(), GatewayError> {
    cfg.policy.check(url).await.map_err(|e| match e {
        SourceError::Dns(msg) => {
            GatewayError::Unprocessable(format!("host {host:?} could not be resolved: {msg}"))
        }
        other => GatewayError::Unprocessable(other.to_string()),
    })?;

    let health = cfg
        .probe
        .get(format!("{host}/health"))
        .send()
        .await
        .map_err(|e| {
            GatewayError::Unprocessable(format!("Meilisearch at {host} is not reachable: {e}"))
        })?;
    if !health.status().is_success() {
        return Err(GatewayError::Unprocessable(format!(
            "Meilisearch at {host} answered /health with {}",
            health.status()
        )));
    }

    let authed = cfg
        .probe
        .get(format!("{host}/indexes"))
        .query(&[("limit", "1")])
        .bearer_auth(api_key)
        .send()
        .await
        .map_err(|e| {
            GatewayError::Unprocessable(format!("Meilisearch at {host} is not reachable: {e}"))
        })?;
    match authed.status() {
        s if s.is_success() => Ok(()),
        reqwest::StatusCode::UNAUTHORIZED | reqwest::StatusCode::FORBIDDEN => {
            Err(GatewayError::Unprocessable(format!(
                "the API key was rejected by Meilisearch at {host}"
            )))
        }
        s => Err(GatewayError::Unprocessable(format!(
            "Meilisearch at {host} answered GET /indexes with {s}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// Control-plane calls.
// ---------------------------------------------------------------------------

/// The control plane reports a duplicate uid as 422, which the shared mapping turns into
/// `Invalid` ("invalid pipeline: …"). Re-word it for connections.
fn reword(e: GatewayError) -> GatewayError {
    match e {
        GatewayError::Invalid(msg) => GatewayError::Unprocessable(msg),
        other => other,
    }
}

impl ControlPlaneClient {
    /// `GET /internal/connections`.
    pub async fn list_connections(
        &self,
        project_id: Option<&str>,
    ) -> Result<Vec<ConnectionRecord>, GatewayError> {
        let req = self
            .http
            .get(self.url("/internal/connections"))
            .query(&Self::project_query(project_id));
        self.send_json(req, "list connections").await
    }

    /// `GET /internal/connections/{uid}`; `Ok(None)` when it does not exist.
    pub async fn get_connection(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<Option<ConnectionRecord>, GatewayError> {
        let req = self
            .http
            .get(self.connection_url(uid, "")?)
            .query(&Self::project_query(project_id));
        match self.send_json(req, "get connection").await {
            Ok(r) => Ok(Some(r)),
            Err(GatewayError::NotFound(_)) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// `GET /internal/connections/{uid}/used_by`.
    pub async fn connection_used_by(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<Vec<String>, GatewayError> {
        let req = self
            .http
            .get(self.connection_url(uid, "used_by")?)
            .query(&Self::project_query(project_id));
        self.send_json(req, "list connection users").await
    }

    /// `POST /internal/connections`.
    pub async fn create_connection(
        &self,
        uid: &str,
        name: &str,
        project_id: Option<&str>,
        host: &str,
        sealed_key: &[u8],
    ) -> Result<ConnectionRecord, GatewayError> {
        let body = NewConnection {
            id: Uuid::new_v4(),
            uid,
            name,
            project_id,
            host,
            api_key: sealed_key,
        };
        let req = self
            .http
            .post(self.url("/internal/connections"))
            .json(&body);
        self.send_json(req, "create connection")
            .await
            .map_err(reword)
    }

    /// `PATCH /internal/connections/{uid}`; `Ok(None)` when it does not exist in exactly
    /// this scope.
    pub async fn update_connection(
        &self,
        uid: &str,
        project_id: Option<&str>,
        name: Option<&str>,
        host: Option<&str>,
        sealed_key: Option<&[u8]>,
    ) -> Result<Option<ConnectionRecord>, GatewayError> {
        let body = ConnectionPatch {
            name,
            host,
            api_key: sealed_key,
        };
        let req = self
            .http
            .patch(self.connection_url(uid, "")?)
            .query(&Self::project_query(project_id))
            .json(&body);
        match self.send_json(req, "update connection").await {
            Ok(r) => Ok(Some(r)),
            Err(GatewayError::NotFound(_)) => Ok(None),
            Err(e) => Err(reword(e)),
        }
    }

    /// `DELETE /internal/connections/{uid}`.
    pub async fn delete_connection(
        &self,
        uid: &str,
        project_id: Option<&str>,
    ) -> Result<(), GatewayError> {
        let req = self
            .http
            .delete(self.connection_url(uid, "")?)
            .query(&Self::project_query(project_id));
        self.send_empty(req, "delete connection").await
    }

    /// `{base}/internal/connections/{uid}[/{suffix}]`, with `uid` as one encoded path
    /// segment so a name can never escape it.
    fn connection_url(&self, uid: &str, suffix: &str) -> Result<Url, GatewayError> {
        let mut url = Url::parse(&self.url("/internal/connections/"))
            .map_err(|e| GatewayError::Internal(format!("control plane url: {e}")))?;
        {
            let mut segments = url.path_segments_mut().map_err(|()| {
                GatewayError::Internal("control plane url cannot be a base".into())
            })?;
            segments.pop_if_empty().push(uid);
            if !suffix.is_empty() {
                segments.push(suffix);
            }
        }
        Ok(url)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn any_policy() -> ConnectionConfig {
        ConnectionConfig::new(
            Some(Arc::new(SecretKey::from_bytes([1; 32]))),
            HostPolicy::Any,
        )
    }

    #[test]
    fn uids_follow_the_pipeline_rule() {
        for ok in ["prod-movies", "a", "tmdb.v2", "x_y-z"] {
            assert!(valid_uid(ok), "{ok}");
        }
        for bad in ["", "has space", "../x", "a/b", "é", &"x".repeat(129)] {
            assert!(!valid_uid(bad), "{bad:?}");
        }
    }

    #[test]
    fn hosts_are_normalized_and_must_be_bare() {
        let (h, _) = normalize_host("  https://m.example/  ").expect("ok");
        assert_eq!(h, "https://m.example");
        assert!(normalize_host("not a url").is_err());
        assert!(normalize_host("https://m.example/?x=1").is_err());
    }

    #[test]
    fn without_a_key_every_route_is_not_implemented() {
        let err = ConnectionConfig::default().key().expect_err("no key");
        assert!(matches!(err, GatewayError::NotImplemented(m) if m.contains("SOURCE_SECRET_KEY")));
    }

    async fn meili(health: u16, indexes: u16) -> MockServer {
        let s = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(ResponseTemplate::new(health))
            .mount(&s)
            .await;
        Mock::given(method("GET"))
            .and(path("/indexes"))
            .and(query_param("limit", "1"))
            .and(header("authorization", "Bearer good-key"))
            .respond_with(ResponseTemplate::new(indexes))
            .mount(&s)
            .await;
        s
    }

    #[tokio::test]
    async fn a_reachable_meilisearch_with_a_good_key_validates() {
        let s = meili(200, 200).await;
        let (host, url) = normalize_host(&s.uri()).expect("host");
        validate_destination(&any_policy(), &host, &url, "good-key")
            .await
            .expect("valid");
    }

    #[tokio::test]
    async fn a_rejected_key_fails_at_save_time_without_echoing_it() {
        let s = meili(200, 200).await;
        let (host, url) = normalize_host(&s.uri()).expect("host");
        // The mock only answers /indexes for "good-key"; anything else is 404 by default,
        // so mount an explicit 401 for the wrong key.
        Mock::given(method("GET"))
            .and(path("/indexes"))
            .and(header("authorization", "Bearer wrong-key"))
            .respond_with(ResponseTemplate::new(401))
            .mount(&s)
            .await;
        let err = validate_destination(&any_policy(), &host, &url, "wrong-key")
            .await
            .expect_err("rejected");
        let msg = err.to_string();
        assert!(matches!(err, GatewayError::Unprocessable(_)), "{err:?}");
        assert!(msg.contains("rejected"), "{msg}");
        assert!(
            !msg.contains("wrong-key"),
            "the key must never be echoed: {msg}"
        );
    }

    #[tokio::test]
    async fn an_unhealthy_meilisearch_fails_validation() {
        let s = meili(503, 200).await;
        let (host, url) = normalize_host(&s.uri()).expect("host");
        let err = validate_destination(&any_policy(), &host, &url, "good-key")
            .await
            .expect_err("unhealthy");
        assert!(err.to_string().contains("/health"), "{err}");
    }

    #[tokio::test]
    async fn the_policy_is_checked_before_any_request_is_made() {
        let s = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&s)
            .await;
        let (host, url) = normalize_host(&s.uri()).expect("host");
        let strict = ConnectionConfig::new(None, HostPolicy::Public);
        let err = validate_destination(&strict, &host, &url, "good-key")
            .await
            .expect_err("loopback http is not public");
        assert!(matches!(err, GatewayError::Unprocessable(_)), "{err:?}");
        // `expect(0)` is verified when `s` drops: no probe left the gateway.
    }

    #[tokio::test]
    async fn the_probe_does_not_follow_redirects() {
        // An allowed host redirecting the probe elsewhere must not be followed.
        let s = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/health"))
            .respond_with(
                ResponseTemplate::new(302).insert_header("location", "http://169.254.169.254/"),
            )
            .mount(&s)
            .await;
        let (host, url) = normalize_host(&s.uri()).expect("host");
        let err = validate_destination(&any_policy(), &host, &url, "good-key")
            .await
            .expect_err("a redirect is not healthy");
        assert!(err.to_string().contains("302"), "{err}");
    }

    #[test]
    fn the_view_always_masks_the_key() {
        let view = ConnectionView::from_record(
            ConnectionRecord {
                id: Uuid::nil(),
                uid: "prod".into(),
                name: "Prod".into(),
                project_id: None,
                host: "https://m.example".into(),
                api_key: vec![1, 2, 3],
                created_at: Utc::now(),
                updated_at: Utc::now(),
            },
            None,
        );
        assert_eq!(view.api_key, MASK);
        let json = serde_json::to_string(&view).expect("serialize");
        assert!(
            !json.contains("[1,2,3]"),
            "sealed bytes never leave either: {json}"
        );
    }
}
