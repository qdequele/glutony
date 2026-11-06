//! The deployment's Lab identity (spec v2 §3.6) and the credit pre-check (spec v2
//! §8.1). Engines are hosted by Meilisearch only (decision A): every Lab-account job
//! is billed, so the check runs whenever Lab credentials are configured and fails
//! closed (503 `lab_unavailable`) whenever it cannot be made: before the Lab has
//! confirmed the credentials at boot, and past the stale window of an outage.
//!
//! The account lookups, their cache windows ([`FRESH_FOR`], [`STALE_FOR`]) and the
//! failure backoff ([`FAILURE_BACKOFF`]) live in [`AccountCreditCache`], shared with
//! the control plane, which runs the same check for scheduled source runs.

use std::sync::RwLock;
use std::time::Duration;

use meili_ingest_lab::{
    AccountCreditCache, CreditDecision, InstanceInfo, LabCredentials, LabError, check_product,
    fetch_instance_info,
};
pub use meili_ingest_lab::{FAILURE_BACKOFF, FRESH_FOR, STALE_FOR};

use crate::error::GatewayError;

/// Lab identity and account cache of this gateway.
pub struct LabClient {
    credits: AccountCreditCache,
    identity: RwLock<Option<InstanceInfo>>,
}

impl std::fmt::Debug for LabClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabClient")
            .field("creds", self.credentials())
            .field("identity", &self.identity())
            .finish_non_exhaustive()
    }
}

impl LabClient {
    /// Build with unknown identity; call [`LabClient::resolve_identity`] at boot.
    pub fn new(creds: LabCredentials, http: reqwest::Client) -> Self {
        Self {
            credits: AccountCreditCache::new(creds, http),
            identity: RwLock::new(None),
        }
    }

    /// The credentials in use.
    pub fn credentials(&self) -> &LabCredentials {
        self.credits.credentials()
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

    /// Ask the Lab once and remember the answer. Credentials minted for another
    /// product's engine are refused ([`LabError::WrongProduct`]) and never remembered.
    pub async fn refresh_identity(&self) -> Result<InstanceInfo, LabError> {
        let info = fetch_instance_info(self.credits.http(), self.credentials()).await?;
        check_product(&info)?;
        self.set_identity(info.clone());
        Ok(info)
    }

    /// Ask until the Lab answers, waiting `every` between attempts. Spawned at boot
    /// (after one synchronous attempt in `main`, where a 401 aborts boot) so a Lab
    /// outage never blocks startup; until it returns, every Lab-account job is
    /// refused with 503. Credentials of another product stop the retries: the identity
    /// stays unknown, so every Lab-account job keeps being refused.
    pub async fn resolve_identity(&self, every: Duration) {
        loop {
            match self.refresh_identity().await {
                Ok(info) => {
                    tracing::info!(kind = ?info.kind, product = %info.product, region = ?info.region, "Lab instance identity confirmed");
                    return;
                }
                Err(e @ LabError::WrongProduct(_)) => {
                    tracing::error!(error = %e, "Lab-account jobs are refused until the credentials are fixed");
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
        if !meili_ingest_lab::is_lab_account(account_id) {
            return Ok(());
        }
        if self.identity().is_none() {
            return Err(GatewayError::LabUnavailable(
                "the Lab has not confirmed this deployment's credentials yet; retry shortly".into(),
            ));
        }
        match self.credits.check(account_id).await {
            CreditDecision::Allowed | CreditDecision::NotALabAccount => Ok(()),
            CreditDecision::NoCredits(m) => Err(GatewayError::PaymentRequired(m)),
            CreditDecision::Unavailable(m) => Err(GatewayError::LabUnavailable(m)),
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

    // The cache windows and the failure backoff are tested where they live, in
    // `meili_ingest_lab::credits`. Here: how the gateway maps each decision.
    #[tokio::test]
    async fn an_account_with_credits_passes_and_an_unreachable_lab_is_503() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 1).await;
        let c = client(&lab);
        c.set_identity(hosted());
        c.check_credits(ACCOUNT).await.unwrap();

        let dead = LabClient::new(
            LabCredentials::new("http://127.0.0.1:1", ID, "s").unwrap(),
            reqwest::Client::new(),
        );
        dead.set_identity(hosted());
        let err = dead.check_credits(ACCOUNT).await.unwrap_err();
        assert_eq!(err.code(), "lab_unavailable");
        assert_eq!(err.status(), axum::http::StatusCode::SERVICE_UNAVAILABLE);
        assert!(err.to_string().contains(ACCOUNT), "{err}");
    }

    #[tokio::test]
    async fn credentials_of_another_product_are_refused_and_never_confirmed() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instance_id": ID, "kind": "hosted", "product": "scrapix"
            })))
            .expect(2)
            .mount(&lab)
            .await;
        let c = client(&lab);
        assert_eq!(
            c.refresh_identity().await,
            Err(LabError::WrongProduct("scrapix".into()))
        );
        assert!(c.identity().is_none(), "Lab-account jobs stay refused");
        // The background resolver gives up instead of retrying forever.
        tokio::time::timeout(
            Duration::from_secs(5),
            c.resolve_identity(Duration::from_millis(10)),
        )
        .await
        .expect("resolve_identity returns on a wrong product");
        assert!(c.identity().is_none());
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
