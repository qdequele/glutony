//! The team's Gateway (Lumen) target for AI steps, as the Lab serves it at
//! `GET /internal/accounts/{id}/ai`: the base URL, a per-team key and the models to ask for.
//!
//! Workers hold no Lab credentials, so the control plane holds an [`AiTargetCache`] and
//! hands the answer to workers over its authenticated internal route. The key
//! (`api_key`) is a secret: [`LabAiTarget`]'s `Debug` redacts it and no error message or
//! log line here carries it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::{LabCredentials, LabError, STALE_FOR, is_lab_account};

/// The longest a target is reused without asking the Lab, whatever `cache_ttl` says.
pub const MAX_FRESH_FOR: Duration = Duration::from_secs(300);

/// `GET /internal/accounts/{id}/ai`, 200.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabAiTarget {
    /// The Gateway's OpenAI-compatible base URL.
    pub base_url: String,
    /// The team's Gateway key. Secret.
    pub api_key: String,
    /// The Lab's id of that key, sent back as `rejected_key_id` when the Gateway refuses it.
    pub key_id: String,
    /// Model for text steps.
    pub default_model: String,
    /// Model for vision steps.
    pub vision_model: String,
    /// Model for decision steps, when the team set one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_model: Option<String>,
    /// How long the Lab suggests reusing this answer, in seconds.
    pub cache_ttl: u64,
}

impl std::fmt::Debug for LabAiTarget {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabAiTarget")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("key_id", &self.key_id)
            .field("default_model", &self.default_model)
            .field("vision_model", &self.vision_model)
            .field("decision_model", &self.decision_model)
            .field("cache_ttl", &self.cache_ttl)
            .finish()
    }
}

/// What an AI target lookup concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AiLookup {
    /// The account's Gateway target.
    Target(LabAiTarget),
    /// The id is not a canonical Lab account id: it has no Gateway, and the Lab is not
    /// asked.
    NotALabAccount,
    /// The Lab does not know the account or its Gateway (404).
    NoGateway,
    /// The Lab cannot answer and no cached target is recent enough. Carries a message
    /// that never includes the key.
    Unavailable(String),
}

#[derive(Clone)]
struct Cached {
    fetched_at: Instant,
    target: LabAiTarget,
}

impl Cached {
    /// Reusable without asking the Lab: younger than `min(cache_ttl, MAX_FRESH_FOR)`.
    fn fresh(&self, now: Instant) -> bool {
        let ttl = Duration::from_secs(self.target.cache_ttl).min(MAX_FRESH_FOR);
        now.duration_since(self.fetched_at) < ttl
    }
}

/// Per-account cache of the Lab's Gateway targets. One per process, shared by every
/// request.
pub struct AiTargetCache {
    creds: LabCredentials,
    http: reqwest::Client,
    targets: Mutex<HashMap<String, Cached>>,
    /// Test-only offset added to the clock, so tests move the cache windows forward
    /// without waiting.
    #[cfg(test)]
    skew: Mutex<Duration>,
}

impl std::fmt::Debug for AiTargetCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AiTargetCache")
            .field("creds", &self.creds)
            .finish_non_exhaustive()
    }
}

/// What one request to the Lab answered.
enum Fetched {
    Target(LabAiTarget),
    NotFound,
}

impl AiTargetCache {
    /// An empty cache asking the Lab with `creds` over `http`. Give `http` short
    /// timeouts and no redirects: a step waits on it.
    pub fn new(creds: LabCredentials, http: reqwest::Client) -> Self {
        Self {
            creds,
            http,
            targets: Mutex::new(HashMap::new()),
            #[cfg(test)]
            skew: Mutex::new(Duration::ZERO),
        }
    }

    /// The clock the cache is aged by.
    fn now(&self) -> Instant {
        #[cfg(not(test))]
        let skew = Duration::ZERO;
        #[cfg(test)]
        let skew = *self.skew.lock().unwrap_or_else(|p| p.into_inner());
        Instant::now() + skew
    }

    /// Move this cache's clock forward (tests).
    #[cfg(test)]
    fn advance(&self, by: Duration) {
        *self.skew.lock().unwrap_or_else(|p| p.into_inner()) += by;
    }

    fn cached(&self, account_id: &str) -> Option<Cached> {
        self.targets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(account_id)
            .cloned()
    }

    fn forget(&self, account_id: &str) {
        self.targets
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(account_id);
    }

    /// The Gateway target of `account_id`.
    ///
    /// A cached target younger than `min(cache_ttl, 300 s)` is returned without asking
    /// the Lab, unless `refresh` is set: a retried step asks for it, because another
    /// control plane replica may have seen a key rotation this one has not, and a fresh
    /// entry here can still hold the revoked key. With `rejected_key_id` (the Gateway
    /// refused that key) the Lab is always asked, with `?rejected_key_id=`, so it can
    /// mint a new key; the cached entry is dropped first only when it holds that very
    /// key. When the Lab cannot answer, a cached target up to [`STALE_FOR`] old is
    /// returned instead (with `refresh` too), but never the rejected key.
    pub async fn get(
        &self,
        account_id: &str,
        rejected_key_id: Option<&str>,
        refresh: bool,
    ) -> AiLookup {
        if !is_lab_account(account_id) {
            return AiLookup::NotALabAccount;
        }
        let now = self.now();
        match rejected_key_id {
            None => {
                if !refresh
                    && let Some(c) = self.cached(account_id)
                    && c.fresh(now)
                {
                    return AiLookup::Target(c.target);
                }
            }
            Some(rejected) => {
                if self
                    .cached(account_id)
                    .is_some_and(|c| c.target.key_id == rejected)
                {
                    self.forget(account_id);
                }
            }
        }
        match self.fetch(account_id, rejected_key_id).await {
            Ok(Fetched::Target(target)) => {
                self.targets
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        account_id.to_string(),
                        Cached {
                            fetched_at: now,
                            target: target.clone(),
                        },
                    );
                AiLookup::Target(target)
            }
            Ok(Fetched::NotFound) => {
                self.forget(account_id);
                AiLookup::NoGateway
            }
            Err(e) => match self.cached(account_id) {
                Some(c) if now.duration_since(c.fetched_at) < STALE_FOR => {
                    tracing::warn!(error = %e, account_id, "Lab unreachable; using a stale AI target");
                    AiLookup::Target(c.target)
                }
                _ => {
                    tracing::error!(error = %e, account_id, "Lab cannot serve the AI target");
                    AiLookup::Unavailable(format!(
                        "cannot get the Gateway target of account {account_id}: {e}"
                    ))
                }
            },
        }
    }

    async fn fetch(
        &self,
        account_id: &str,
        rejected_key_id: Option<&str>,
    ) -> Result<Fetched, LabError> {
        let mut url = url::Url::parse(
            &self
                .creds
                .endpoint(&format!("/internal/accounts/{account_id}/ai")),
        )
        .map_err(|e| LabError::Malformed(format!("not a URL: {e}")))?;
        if let Some(id) = rejected_key_id {
            url.query_pairs_mut().append_pair("rejected_key_id", id);
        }
        let resp = self
            .creds
            .authorize(self.http.get(url))
            .send()
            .await
            .map_err(|e| LabError::Transport(e.to_string()))?;
        let status = resp.status();
        match status.as_u16() {
            401 => return Err(LabError::Unauthorized),
            403 => {
                return Err(LabError::Malformed(
                    "the Lab refused (403): these credentials are not a Pipelines engine's".into(),
                ));
            }
            404 => return Ok(Fetched::NotFound),
            _ if !status.is_success() => return Err(LabError::Status(status.as_u16())),
            _ => {}
        }
        let body = resp
            .bytes()
            .await
            .map_err(|e| LabError::Transport(e.to_string()))?;
        // serde's messages can quote the offending value, which may be the key: report
        // only the kind of failure and where.
        serde_json::from_slice::<LabAiTarget>(&body)
            .map(Fetched::Target)
            .map_err(|e| {
                LabError::Malformed(format!(
                    "{:?} at line {} column {}",
                    e.classify(),
                    e.line(),
                    e.column()
                ))
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
    const ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";
    const SECRET: &str = "lab-secret";
    const KEY: &str = "sk-team-key-do-not-leak";

    fn cache(lab: &MockServer) -> AiTargetCache {
        AiTargetCache::new(
            LabCredentials::new(&lab.uri(), ID, SECRET).unwrap(),
            reqwest::Client::new(),
        )
    }

    fn body(key_id: &str, cache_ttl: u64) -> serde_json::Value {
        serde_json::json!({
            "base_url": "https://gateway.example/v1",
            "api_key": KEY,
            "key_id": key_id,
            "default_model": "gpt-4o-mini",
            "vision_model": "gpt-4o",
            "cache_ttl": cache_ttl,
        })
    }

    fn target(key_id: &str) -> LabAiTarget {
        serde_json::from_value(body(key_id, 60)).unwrap()
    }

    fn ai_path() -> String {
        format!("/internal/accounts/{ACCOUNT}/ai")
    }

    async fn requests(lab: &MockServer) -> usize {
        lab.received_requests().await.unwrap().len()
    }

    fn is_unavailable(l: &AiLookup) -> bool {
        matches!(l, AiLookup::Unavailable(_))
    }

    #[tokio::test]
    async fn fetches_with_the_instance_credentials_and_caches() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .and(header("authorization", format!("Bearer {SECRET}").as_str()))
            .and(header("x-lab-instance-id", ID))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .expect(1)
            .mount(&lab)
            .await;
        let c = cache(&lab);
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1"))
        );
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1"))
        );
        lab.verify().await;
        assert_eq!(requests(&lab).await, 1);
    }

    #[tokio::test]
    async fn a_target_is_reused_for_min_of_cache_ttl_and_300s() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .mount(&lab)
            .await;
        let c = cache(&lab);
        c.get(ACCOUNT, None, false).await;
        c.advance(Duration::from_secs(59));
        c.get(ACCOUNT, None, false).await;
        assert_eq!(requests(&lab).await, 1);
        c.advance(Duration::from_secs(2));
        c.get(ACCOUNT, None, false).await;
        assert_eq!(requests(&lab).await, 2);

        // A cache_ttl above 300 is capped.
        lab.reset().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 3600)))
            .mount(&lab)
            .await;
        let c = cache(&lab);
        c.get(ACCOUNT, None, false).await;
        c.advance(Duration::from_secs(299));
        c.get(ACCOUNT, None, false).await;
        assert_eq!(requests(&lab).await, 1);
        c.advance(Duration::from_secs(2));
        c.get(ACCOUNT, None, false).await;
        assert_eq!(requests(&lab).await, 2);
    }

    #[tokio::test]
    async fn a_rejected_key_refetches_with_rejected_key_id() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .and(query_param("rejected_key_id", "k1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k2", 60)))
            .expect(1)
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .expect(1)
            .mount(&lab)
            .await;
        let c = cache(&lab);
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1"))
        );
        assert_eq!(
            c.get(ACCOUNT, Some("k1"), false).await,
            AiLookup::Target(target("k2"))
        );
        lab.verify().await;
        // The new key is what is cached now.
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k2"))
        );
        assert_eq!(requests(&lab).await, 2);
    }

    #[tokio::test]
    async fn a_rejected_key_that_is_not_cached_keeps_the_cache() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .and(query_param("rejected_key_id", "k1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k2", 60)))
            .expect(1)
            .mount(&lab)
            .await;
        let c = cache(&lab);
        c.targets.lock().unwrap().insert(
            ACCOUNT.to_string(),
            Cached {
                fetched_at: c.now(),
                target: target("k2"),
            },
        );
        assert_eq!(
            c.get(ACCOUNT, Some("k1"), false).await,
            AiLookup::Target(target("k2"))
        );
        lab.verify().await;
        assert_eq!(requests(&lab).await, 1);

        // k2 was not dropped first: when the Lab is down it is still served.
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        assert_eq!(
            c.get(ACCOUNT, Some("k1"), false).await,
            AiLookup::Target(target("k2"))
        );
        // A rejected key that is cached is never served, even stale.
        assert!(is_unavailable(&c.get(ACCOUNT, Some("k2"), false).await));
    }

    #[tokio::test]
    async fn refresh_asks_the_lab_past_a_fresh_entry() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .expect(1)
            .mount(&lab)
            .await;
        let c = cache(&lab);
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1"))
        );
        lab.verify().await;

        // Another replica rotated the key: this cache's entry is fresh but outdated.
        lab.reset().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k2", 60)))
            .expect(1)
            .mount(&lab)
            .await;
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1")),
            "without refresh the fresh entry is reused"
        );
        assert_eq!(requests(&lab).await, 0);
        assert_eq!(
            c.get(ACCOUNT, None, true).await,
            AiLookup::Target(target("k2"))
        );
        lab.verify().await;
        // The refreshed answer is what is cached now.
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k2"))
        );
        assert_eq!(requests(&lab).await, 1);
    }

    #[tokio::test]
    async fn refresh_with_the_lab_down_serves_the_cached_entry() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .mount(&lab)
            .await;
        let c = cache(&lab);
        c.get(ACCOUNT, None, false).await;
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        // Fresh, then stale (past cache_ttl, within 300 s): both are still served.
        assert_eq!(
            c.get(ACCOUNT, None, true).await,
            AiLookup::Target(target("k1"))
        );
        c.advance(Duration::from_secs(120));
        assert_eq!(
            c.get(ACCOUNT, None, true).await,
            AiLookup::Target(target("k1"))
        );
        assert_eq!(requests(&lab).await, 2, "refresh asked the Lab each time");
        // Past 300 s: nothing usable.
        c.advance(Duration::from_secs(181));
        assert!(is_unavailable(&c.get(ACCOUNT, None, true).await));
        // Refresh never serves a rejected key.
        let c = cache(&lab);
        c.targets.lock().unwrap().insert(
            ACCOUNT.to_string(),
            Cached {
                fetched_at: c.now(),
                target: target("k1"),
            },
        );
        assert!(is_unavailable(&c.get(ACCOUNT, Some("k1"), true).await));
    }

    #[tokio::test]
    async fn not_found_is_no_gateway_and_not_cached() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(
                ResponseTemplate::new(404).set_body_json(serde_json::json!({"error": "not_found"})),
            )
            .expect(2)
            .mount(&lab)
            .await;
        let c = cache(&lab);
        assert_eq!(c.get(ACCOUNT, None, false).await, AiLookup::NoGateway);
        assert_eq!(c.get(ACCOUNT, None, false).await, AiLookup::NoGateway);
        lab.verify().await;
    }

    #[tokio::test]
    async fn not_found_drops_a_cached_target() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .mount(&lab)
            .await;
        let c = cache(&lab);
        c.get(ACCOUNT, None, false).await;
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(404))
            .mount(&lab)
            .await;
        c.advance(Duration::from_secs(61));
        assert_eq!(c.get(ACCOUNT, None, false).await, AiLookup::NoGateway);
        // Gone for good: not served stale on the next hiccup.
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        assert!(is_unavailable(&c.get(ACCOUNT, None, false).await));
    }

    // The cache is aged by its own clock (`advance`), as `AccountCreditCache` is.
    #[tokio::test]
    async fn lab_down_serves_a_stale_entry_then_unavailable() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(ai_path()))
            .respond_with(ResponseTemplate::new(200).set_body_json(body("k1", 60)))
            .mount(&lab)
            .await;
        let c = cache(&lab);
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1"))
        );
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        // Past cache_ttl but within 300 s: the Lab is asked, fails, the entry is served.
        c.advance(Duration::from_secs(120)); // past cache_ttl (60 s)
        assert_eq!(
            c.get(ACCOUNT, None, false).await,
            AiLookup::Target(target("k1"))
        );
        assert_eq!(requests(&lab).await, 1);
        // Past 300 s: nothing usable.
        c.advance(Duration::from_secs(181));
        let l = c.get(ACCOUNT, None, false).await;
        assert!(is_unavailable(&l), "{l:?}");
    }

    #[tokio::test]
    async fn non_lab_ids_make_no_request() {
        let lab = MockServer::start().await;
        let c = cache(&lab);
        assert_eq!(
            c.get("tenant-1", None, false).await,
            AiLookup::NotALabAccount
        );
        assert_eq!(
            c.get(&ACCOUNT.to_uppercase(), Some("k1"), false).await,
            AiLookup::NotALabAccount
        );
        assert_eq!(requests(&lab).await, 0);
    }

    #[tokio::test]
    async fn forbidden_and_server_errors_are_unavailable() {
        for status in [401, 403, 502] {
            let lab = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(
                    ResponseTemplate::new(status)
                        .set_body_json(serde_json::json!({"error": "x", "api_key": KEY})),
                )
                .mount(&lab)
                .await;
            let l = cache(&lab).get(ACCOUNT, None, false).await;
            assert!(is_unavailable(&l), "{status}: {l:?}");
            assert!(!format!("{l:?}").contains(KEY), "{status}: {l:?}");
        }
    }

    #[tokio::test]
    async fn an_unreachable_lab_is_unavailable() {
        let c = AiTargetCache::new(
            LabCredentials::new("http://127.0.0.1:1", ID, SECRET).unwrap(),
            reqwest::Client::new(),
        );
        let l = c.get(ACCOUNT, None, false).await;
        assert!(is_unavailable(&l), "{l:?}");
    }

    #[tokio::test]
    async fn a_bad_body_is_unavailable_and_never_echoes_the_key() {
        let lab = MockServer::start().await;
        // The key sits in a field of the wrong type: serde's own message would quote it.
        let mut bad = body("k1", 60);
        bad["cache_ttl"] = serde_json::json!(KEY);
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(bad))
            .mount(&lab)
            .await;
        let l = cache(&lab).get(ACCOUNT, None, false).await;
        assert!(is_unavailable(&l), "{l:?}");
        assert!(!format!("{l:?}").contains(KEY), "{l:?}");

        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_string("not json"))
            .mount(&lab)
            .await;
        assert!(is_unavailable(&cache(&lab).get(ACCOUNT, None, false).await));
    }

    #[test]
    fn debug_redacts_the_key() {
        let t = target("k1");
        let shown = format!("{t:?}");
        assert!(!shown.contains(KEY), "{shown}");
        assert!(
            shown.contains("<redacted>") && shown.contains("k1"),
            "{shown}"
        );
        let lab_creds = LabCredentials::new("http://127.0.0.1:1", ID, SECRET).unwrap();
        let c = AiTargetCache::new(lab_creds, reqwest::Client::new());
        c.targets.lock().unwrap().insert(
            ACCOUNT.to_string(),
            Cached {
                fetched_at: c.now(),
                target: t.clone(),
            },
        );
        assert!(!format!("{c:?}").contains(KEY));
        assert!(!format!("{:?}", AiLookup::Target(t)).contains(KEY));
    }

    #[test]
    fn the_target_round_trips_and_decision_model_is_optional() {
        let mut v = body("k1", 60);
        let t: LabAiTarget = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(t.decision_model, None);
        assert_eq!(serde_json::to_value(&t).unwrap(), v);
        v["decision_model"] = serde_json::json!("gpt-4o");
        let t: LabAiTarget = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(t.decision_model.as_deref(), Some("gpt-4o"));
        assert_eq!(serde_json::to_value(&t).unwrap(), v);
    }
}
