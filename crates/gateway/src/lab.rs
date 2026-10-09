//! The deployment's Lab identity (spec v2 §3.6) and the credit pre-check (spec v2
//! §8.1). Engines are hosted by Meilisearch only (decision A): every Lab-account job
//! is billed, so the check runs whenever Lab credentials are configured and fails
//! closed (503 `lab_unavailable`) whenever it cannot be made: before the Lab has
//! confirmed the credentials at boot, and past the stale window of an outage.
//!
//! Lookups are cached per account for [`FRESH_FOR`]; when the Lab cannot answer, an
//! entry up to [`STALE_FOR`] old is used.

use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::Duration;

use meili_ingest_lab::{
    AccountLookup, InstanceInfo, LabCredentials, LabError, fetch_account, fetch_instance_info,
    is_lab_account,
};
use tokio::time::Instant;

use crate::error::GatewayError;

/// A lookup younger than this is reused without asking the Lab.
pub const FRESH_FOR: Duration = Duration::from_secs(30);
/// A lookup younger than this is reused when the Lab cannot answer.
pub const STALE_FOR: Duration = Duration::from_secs(300);

#[derive(Clone)]
struct Cached {
    fetched_at: Instant,
    lookup: AccountLookup,
}

/// Lab identity and account cache of this gateway.
pub struct LabClient {
    creds: LabCredentials,
    http: reqwest::Client,
    identity: RwLock<Option<InstanceInfo>>,
    accounts: Mutex<HashMap<String, Cached>>,
}

impl std::fmt::Debug for LabClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabClient")
            .field("creds", &self.creds)
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}

impl LabClient {
    /// Build with unknown identity; call [`LabClient::resolve_identity`] at boot.
    pub fn new(creds: LabCredentials, http: reqwest::Client) -> Self {
        Self {
            creds,
            http,
            identity: RwLock::new(None),
            accounts: Mutex::new(HashMap::new()),
        }
    }

    /// The credentials in use.
    pub fn credentials(&self) -> &LabCredentials {
        &self.creds
    }

    /// What the Lab said this deployment is, once known.
    pub fn identity(&self) -> Option<InstanceInfo> {
        self.identity
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// Set the identity directly (tests).
    pub fn set_identity(&self, info: InstanceInfo) {
        *self.identity.write().unwrap_or_else(|p| p.into_inner()) = Some(info);
    }

    /// Ask the Lab once and remember the answer.
    pub async fn refresh_identity(&self) -> Result<InstanceInfo, LabError> {
        let info = fetch_instance_info(&self.http, &self.creds).await?;
        self.set_identity(info.clone());
        Ok(info)
    }

    /// Ask until the Lab answers, waiting `every` between attempts. Spawned at boot
    /// (after one synchronous attempt in `main`, where a 401 aborts boot) so a Lab
    /// outage never blocks startup; until it returns, every Lab-account job is
    /// refused with 503.
    pub async fn resolve_identity(&self, every: Duration) {
        loop {
            match self.refresh_identity().await {
                Ok(info) => {
                    tracing::info!(kind = ?info.kind, product = %info.product, region = ?info.region, "Lab instance identity confirmed");
                    return;
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        "could not confirm this deployment's Lab identity; retrying (Lab-account jobs are refused until then)"
                    );
                    tokio::time::sleep(every).await;
                }
            }
        }
    }

    /// Refuse billable work for a Lab account out of credits. A tenant that is not a
    /// Lab account id (a Cloud project id) always passes: nothing is billed for it.
    pub async fn check_credits(&self, account_id: &str) -> Result<(), GatewayError> {
        if !is_lab_account(account_id) {
            return Ok(());
        }
        if self.identity().is_none() {
            return Err(GatewayError::LabUnavailable(
                "the Lab has not confirmed this deployment's credentials yet; retry shortly".into(),
            ));
        }
        match self.lookup(account_id).await {
            Some(lookup) if !lookup.can_spend() => Err(GatewayError::PaymentRequired(format!(
                "account {account_id} has no credits left; top up in the Lab console"
            ))),
            Some(_) => Ok(()),
            // Fail closed, as Scrapix does: money is checked or the job waits.
            None => Err(GatewayError::LabUnavailable(format!(
                "the Lab has been unreachable for more than {}s; cannot check account {account_id}'s credits",
                STALE_FOR.as_secs()
            ))),
        }
    }

    fn cached(&self, account_id: &str) -> Option<Cached> {
        self.accounts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(account_id)
            .cloned()
    }

    async fn lookup(&self, account_id: &str) -> Option<AccountLookup> {
        let now = Instant::now();
        if let Some(c) = self.cached(account_id)
            && now.duration_since(c.fetched_at) < FRESH_FOR
        {
            return Some(c.lookup);
        }
        match fetch_account(&self.http, &self.creds, account_id).await {
            Ok(lookup) => {
                self.accounts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .insert(
                        account_id.to_string(),
                        Cached {
                            fetched_at: now,
                            lookup: lookup.clone(),
                        },
                    );
                Some(lookup)
            }
            Err(e) => match self.cached(account_id) {
                Some(c) if now.duration_since(c.fetched_at) < STALE_FOR => {
                    tracing::warn!(error = %e, account_id, "Lab unreachable; using a stale account lookup");
                    Some(c.lookup)
                }
                _ => {
                    tracing::error!(
                        error = %e,
                        account_id,
                        "Lab unreachable and no usable cached lookup; refusing the job (503)"
                    );
                    None
                }
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_lab::InstanceKind;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
    const ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

    fn hosted() -> InstanceInfo {
        InstanceInfo {
            instance_id: ID.into(),
            kind: InstanceKind::Hosted,
            product: "glutony".into(),
            region: Some("eu-west-1".into()),
            lab_url: None,
        }
    }

    fn client(lab: &MockServer) -> LabClient {
        LabClient::new(
            LabCredentials::new(&lab.uri(), ID, "s").unwrap(),
            reqwest::Client::new(),
        )
    }

    async fn mount_balance(lab: &MockServer, balance: i64, expect: u64) {
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": ACCOUNT, "tier": "pro",
                "credits": {"balance": balance}, "cache_ttl": 30
            })))
            .expect(expect)
            .mount(lab)
            .await;
    }

    #[tokio::test]
    async fn an_account_without_credits_is_402_on_a_hosted_engine() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 0, 1).await;
        let c = client(&lab);
        c.set_identity(hosted());
        let err = c.check_credits(ACCOUNT).await.unwrap_err();
        assert_eq!(err.code(), "insufficient_credits");
        assert_eq!(err.status(), axum::http::StatusCode::PAYMENT_REQUIRED);
    }

    #[tokio::test]
    async fn an_inactive_account_is_402_too() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"active": false})),
            )
            .mount(&lab)
            .await;
        let c = client(&lab);
        c.set_identity(hosted());
        assert_eq!(
            c.check_credits(ACCOUNT).await.unwrap_err().code(),
            "insufficient_credits"
        );
    }

    #[tokio::test]
    async fn non_lab_tenants_never_call_the_lab() {
        // A Cloud project id or any other opaque tenant is not a Lab account: nothing
        // to bill, nothing to check.
        let lab = MockServer::start().await;
        mount_balance(&lab, 0, 0).await;
        let c = client(&lab);
        c.set_identity(hosted());
        c.check_credits("hackersearch").await.unwrap();
        c.check_credits(&ACCOUNT.to_uppercase()).await.unwrap();
    }

    #[tokio::test]
    async fn check_credits_fails_closed_until_the_identity_is_known() {
        // Until the Lab has confirmed the credentials, no Lab-account job runs: every
        // job is billed, so there is no availability argument for letting one through.
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 0).await;
        let c = client(&lab);
        assert!(c.identity().is_none());
        let err = c.check_credits(ACCOUNT).await.unwrap_err();
        assert_eq!(err.code(), "lab_unavailable");
        assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test(start_paused = true)]
    async fn lookups_are_cached_for_30s_and_served_stale_for_300s() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 1).await;
        let c = client(&lab);
        c.set_identity(hosted());
        c.check_credits(ACCOUNT).await.unwrap();
        tokio::time::advance(Duration::from_secs(29)).await;
        c.check_credits(ACCOUNT).await.unwrap(); // cached: expect(1) holds
        // The Lab goes down; the 29 s-old entry is stale but usable for 300 s.
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        tokio::time::advance(Duration::from_secs(2)).await;
        c.check_credits(ACCOUNT).await.unwrap();
        // A zero balance seen before the outage keeps refusing while stale.
        c.accounts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(ACCOUNT)
            .unwrap()
            .lookup
            .credits = Some(meili_ingest_lab::Credits { balance: 0 });
        assert_eq!(
            c.check_credits(ACCOUNT).await.unwrap_err().code(),
            "insufficient_credits"
        );
        // Past 300 s with no Lab: fail closed (503 lab_unavailable), like Scrapix.
        tokio::time::advance(Duration::from_secs(300)).await;
        assert_eq!(
            c.check_credits(ACCOUNT).await.unwrap_err().code(),
            "lab_unavailable"
        );
    }

    #[tokio::test]
    async fn resolve_identity_retries_until_the_lab_answers() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(2)
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instance_id": ID, "kind": "hosted", "product": "glutony"
            })))
            .mount(&lab)
            .await;
        let c = client(&lab);
        c.resolve_identity(Duration::from_millis(10)).await;
        assert_eq!(c.identity().map(|i| i.kind), Some(InstanceKind::Hosted));
    }
}
