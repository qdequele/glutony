//! The credit pre-check (spec v2 §8.1), shared by the gateway (ingest and manual
//! source runs) and the control plane (which answers it for the workers' scheduled
//! source runs, since workers hold no Lab credentials).
//!
//! Lookups are cached per account for [`FRESH_FOR`]; when the Lab cannot answer, an
//! entry up to [`STALE_FOR`] old is used, and past that the check fails closed
//! ([`CreditDecision::Unavailable`]). After a failed fetch the cache does not ask the
//! Lab again for [`FAILURE_BACKOFF`], so a hung Lab costs one timeout, not one per
//! check. A failure that says the Lab is in trouble pauses every account; one that is
//! about the account asked for pauses that account only.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::{AccountLookup, LabCredentials, LabError, fetch_account, is_lab_account};

/// A lookup younger than this is reused without asking the Lab.
pub const FRESH_FOR: Duration = Duration::from_secs(30);
/// A lookup younger than this is reused when the Lab cannot answer.
pub const STALE_FOR: Duration = Duration::from_secs(300);
/// After a failed fetch, the Lab is not asked again for this long: about any account
/// when the failure is the Lab's, about that account when it is the account's.
pub const FAILURE_BACKOFF: Duration = Duration::from_secs(10);

/// Does this failure say the Lab, or this engine's access to it, is in trouble? An
/// unreachable Lab, refused credentials (401, 403), a rate limit (429) and a 5xx do.
/// An unreadable body or another 4xx is about the account that was asked for.
fn is_lab_wide(e: &LabError) -> bool {
    match e {
        LabError::Transport(_) | LabError::Unauthorized | LabError::WrongProduct(_) => true,
        LabError::Status(code) => matches!(code, 403 | 429 | 500..),
        LabError::Malformed(_) => false,
    }
}

/// The failed Lab fetches of one cache, each remembered for [`FAILURE_BACKOFF`].
#[derive(Default)]
pub(crate) struct FailureBackoff {
    /// When the Lab last failed as a whole; cleared by the next success.
    lab: Mutex<Option<Instant>>,
    /// When and how the Lab last failed for one account; cleared by that account's
    /// next success, and pruned once older than [`FAILURE_BACKOFF`].
    accounts: Mutex<HashMap<String, (Instant, LabError)>>,
}

impl FailureBackoff {
    /// The failure to answer with instead of asking the Lab about `account_id`, when
    /// one is younger than [`FAILURE_BACKOFF`].
    pub(crate) fn blocked(&self, account_id: &str, now: Instant) -> Option<LabError> {
        let recent = |at: Instant| now.saturating_duration_since(at) < FAILURE_BACKOFF;
        if self
            .lab
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .is_some_and(recent)
        {
            return Some(LabError::Transport(format!(
                "not asked: the last Lab request failed less than {}s ago",
                FAILURE_BACKOFF.as_secs()
            )));
        }
        self.accounts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(account_id)
            .filter(|(at, _)| recent(*at))
            .map(|(_, e)| e.clone())
    }

    /// Remember what a fetch about `account_id` answered, seen at `now`. A success ends
    /// the Lab-wide backoff and that account's; an account's failure leaves the
    /// Lab-wide backoff as it is.
    pub(crate) fn record<T>(&self, account_id: &str, fetched: &Result<T, LabError>, now: Instant) {
        match fetched {
            Ok(_) => {
                *self.lab.lock().unwrap_or_else(|p| p.into_inner()) = None;
                self.accounts
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(account_id);
            }
            Err(e) if is_lab_wide(e) => {
                *self.lab.lock().unwrap_or_else(|p| p.into_inner()) = Some(now);
            }
            Err(e) => {
                let mut accounts = self.accounts.lock().unwrap_or_else(|p| p.into_inner());
                // Keeps the map bounded by the accounts that failed in the last
                // FAILURE_BACKOFF.
                accounts.retain(|_, (at, _)| now.saturating_duration_since(*at) < FAILURE_BACKOFF);
                accounts.insert(account_id.to_string(), (now, e.clone()));
            }
        }
    }
}

/// What a credit check concluded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CreditDecision {
    /// The account is active with a positive balance: the work may start.
    Allowed,
    /// The id is not a canonical Lab account id (a Cloud project id, any opaque
    /// tenant): nothing is billed for it, so nothing is checked and the Lab is not
    /// asked.
    NotALabAccount,
    /// The account is inactive or its balance is 0 or less (HTTP 402
    /// `insufficient_credits` for the callers). Carries a message for the client.
    NoCredits(String),
    /// The Lab cannot answer and no cached lookup is recent enough (HTTP 503
    /// `lab_unavailable`): fail closed. Carries a message for the client.
    Unavailable(String),
}

impl CreditDecision {
    /// The human message of a refusal.
    pub fn message(&self) -> Option<&str> {
        match self {
            Self::NoCredits(m) | Self::Unavailable(m) => Some(m),
            Self::Allowed | Self::NotALabAccount => None,
        }
    }
}

#[derive(Clone)]
struct Cached {
    fetched_at: Instant,
    lookup: AccountLookup,
}

/// Per-account cache of `GET /internal/accounts/{id}` answers, with the failure
/// backoff. One per process, shared by every request.
pub struct AccountCreditCache {
    creds: LabCredentials,
    http: reqwest::Client,
    accounts: Mutex<HashMap<String, Cached>>,
    failures: FailureBackoff,
    /// Test-only offset added to the clock, so tests move the cache windows forward
    /// without waiting.
    #[cfg(test)]
    skew: Mutex<Duration>,
}

impl std::fmt::Debug for AccountCreditCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AccountCreditCache")
            .field("creds", &self.creds)
            .finish_non_exhaustive()
    }
}

impl AccountCreditCache {
    /// An empty cache asking the Lab with `creds` over `http`. Give `http` short
    /// timeouts and no redirects: a check waits on it.
    pub fn new(creds: LabCredentials, http: reqwest::Client) -> Self {
        Self {
            creds,
            http,
            accounts: Mutex::new(HashMap::new()),
            failures: FailureBackoff::default(),
            #[cfg(test)]
            skew: Mutex::new(Duration::ZERO),
        }
    }

    /// The credentials in use.
    pub fn credentials(&self) -> &LabCredentials {
        &self.creds
    }

    /// The HTTP client the Lab is asked with.
    pub fn http(&self) -> &reqwest::Client {
        &self.http
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

    /// Can billable work start for `account_id`? An id that is not a Lab account
    /// passes without asking the Lab.
    pub async fn check(&self, account_id: &str) -> CreditDecision {
        if !is_lab_account(account_id) {
            return CreditDecision::NotALabAccount;
        }
        match self.lookup(account_id).await {
            Some(lookup) if !lookup.can_spend() => CreditDecision::NoCredits(format!(
                "account {account_id} has no credits left; top up in the Lab console"
            )),
            Some(_) => CreditDecision::Allowed,
            // Fail closed, as Scrapix does: money is checked or the work waits.
            None => CreditDecision::Unavailable(format!(
                "the Lab has been unreachable for more than {}s; cannot check account {account_id}'s credits",
                STALE_FOR.as_secs()
            )),
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
        let now = self.now();
        if let Some(c) = self.cached(account_id)
            && now.duration_since(c.fetched_at) < FRESH_FOR
        {
            return Some(c.lookup);
        }
        let fetched = if let Some(e) = self.failures.blocked(account_id, now) {
            Err(e)
        } else {
            let fetched = fetch_account(&self.http, &self.creds, account_id).await;
            // Timed from when the failure was seen, so a 5 s timeout still leaves the
            // full backoff before the next attempt.
            self.failures.record(account_id, &fetched, self.now());
            fetched
        };
        match fetched {
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
                    tracing::warn!(error = %e, account_id, "the Lab could not answer; using a stale account lookup");
                    Some(c.lookup)
                }
                _ => {
                    tracing::error!(
                        error = %e,
                        account_id,
                        "the Lab could not answer and no usable cached lookup; refusing the work (fail closed)"
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
    use crate::Credits;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ACCOUNT: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b61";
    const ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

    fn cache(lab: &MockServer) -> AccountCreditCache {
        AccountCreditCache::new(
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

    fn is_no_credits(d: &CreditDecision) -> bool {
        matches!(d, CreditDecision::NoCredits(_))
    }

    fn is_unavailable(d: &CreditDecision) -> bool {
        matches!(d, CreditDecision::Unavailable(_))
    }

    #[tokio::test]
    async fn an_account_with_credits_is_allowed() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 1).await;
        assert_eq!(cache(&lab).check(ACCOUNT).await, CreditDecision::Allowed);
    }

    #[tokio::test]
    async fn an_account_without_credits_is_refused() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 0, 1).await;
        let d = cache(&lab).check(ACCOUNT).await;
        assert!(is_no_credits(&d), "{d:?}");
        assert!(d.message().unwrap().contains(ACCOUNT), "{d:?}");
    }

    #[tokio::test]
    async fn an_inactive_account_is_refused_too() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"active": false})),
            )
            .mount(&lab)
            .await;
        assert!(is_no_credits(&cache(&lab).check(ACCOUNT).await));
    }

    #[tokio::test]
    async fn non_lab_accounts_never_call_the_lab() {
        // A Cloud project id or any other opaque tenant is not a Lab account: nothing
        // to bill, nothing to check.
        let lab = MockServer::start().await;
        mount_balance(&lab, 0, 0).await;
        let c = cache(&lab);
        assert_eq!(
            c.check("hackersearch").await,
            CreditDecision::NotALabAccount
        );
        assert_eq!(
            c.check(&ACCOUNT.to_uppercase()).await,
            CreditDecision::NotALabAccount
        );
        assert_eq!(CreditDecision::NotALabAccount.message(), None);
    }

    #[tokio::test]
    async fn an_unreachable_lab_without_a_cached_lookup_is_unavailable() {
        let c = AccountCreditCache::new(
            LabCredentials::new("http://127.0.0.1:1", ID, "s").unwrap(),
            reqwest::Client::new(),
        );
        let d = c.check(ACCOUNT).await;
        assert!(is_unavailable(&d), "{d:?}");
        assert!(d.message().unwrap().contains(ACCOUNT), "{d:?}");
    }

    // The cache is aged by its own clock (`advance`), not by pausing tokio's: a paused
    // clock auto-advances to the next pending timer whenever the runtime waits on the
    // mock server's I/O, which made the windows jump by hundreds of seconds.
    #[tokio::test]
    async fn lookups_are_cached_for_30s_and_served_stale_for_300s() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 1).await;
        let c = cache(&lab);
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed);
        c.advance(Duration::from_secs(29));
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed); // cached: expect(1) holds
        // Exactly one fetch so far: the 29 s-old entry was served from the cache.
        // (`reset` below drops mocks without checking their `expect`.)
        lab.verify().await;
        // The Lab goes down; the 29 s-old entry is stale but usable for 300 s.
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .mount(&lab)
            .await;
        c.advance(Duration::from_secs(2));
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed);
        // A zero balance seen before the outage keeps refusing while stale.
        c.accounts
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(ACCOUNT)
            .unwrap()
            .lookup
            .credits = Some(Credits { balance: 0 });
        assert!(is_no_credits(&c.check(ACCOUNT).await));
        // Past 300 s with no Lab: fail closed, like Scrapix.
        c.advance(Duration::from_secs(300));
        assert!(is_unavailable(&c.check(ACCOUNT).await));
    }

    #[tokio::test]
    async fn a_failed_fetch_is_remembered_for_10s() {
        // A hung or failing Lab must not cost every request a timeout: after one
        // failure the cache stops asking for FAILURE_BACKOFF and answers from what it
        // has (a stale entry, or Unavailable without one).
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&lab)
            .await;
        let c = cache(&lab);
        assert!(is_unavailable(&c.check(ACCOUNT).await));
        c.advance(Duration::from_secs(9));
        assert!(is_unavailable(&c.check(ACCOUNT).await));
        // Only the first lookup reached the Lab.
        lab.verify().await;

        // Past the backoff the cache asks again, and a success clears it.
        lab.reset().await;
        mount_balance(&lab, 10, 1).await;
        c.advance(Duration::from_secs(2));
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed);
        lab.verify().await;
    }

    #[tokio::test]
    async fn during_the_failure_backoff_a_stale_entry_is_served_without_asking() {
        let lab = MockServer::start().await;
        mount_balance(&lab, 10, 1).await;
        let c = cache(&lab);
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed);
        lab.verify().await;
        lab.reset().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(503))
            .expect(1)
            .mount(&lab)
            .await;
        // Entry is 31 s old: not fresh, so the Lab is asked once and fails.
        c.advance(Duration::from_secs(31));
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed);
        // Within the backoff: served stale, no second request.
        c.advance(Duration::from_secs(5));
        assert_eq!(c.check(ACCOUNT).await, CreditDecision::Allowed);
        lab.verify().await;
    }

    const OTHER: &str = "0192f3c1-7c2e-7b1a-9f00-3c9d2e4a5b62";

    /// `OTHER` has credits; `ACCOUNT` gets `broken` from the Lab.
    async fn mount_one_broken_account(lab: &MockServer, broken: ResponseTemplate, expect: u64) {
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ACCOUNT}")))
            .respond_with(broken)
            .expect(expect)
            .mount(lab)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{OTHER}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": OTHER, "tier": "pro",
                "credits": {"balance": 10}, "cache_ttl": 30
            })))
            .expect(1)
            .mount(lab)
            .await;
    }

    #[tokio::test]
    async fn one_account_s_bad_answer_does_not_pause_the_other_accounts() {
        // A body the contract does not allow (a fractional balance) and a 4xx are about
        // that account, not about the Lab: the others are still asked.
        let unreadable = ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "active": true, "account_id": ACCOUNT, "credits": {"balance": 1.5}
        }));
        for broken in [unreadable, ResponseTemplate::new(422)] {
            let lab = MockServer::start().await;
            mount_one_broken_account(&lab, broken, 2).await;
            let c = cache(&lab);
            assert!(is_unavailable(&c.check(ACCOUNT).await));
            assert_eq!(c.check(OTHER).await, CreditDecision::Allowed);
            // The broken account itself is not asked about again for 10 s, even though
            // another account's lookup succeeded meanwhile.
            c.advance(Duration::from_secs(9));
            assert!(is_unavailable(&c.check(ACCOUNT).await));
            assert_eq!(lab.received_requests().await.unwrap().len(), 2);
            c.advance(Duration::from_secs(2));
            assert!(is_unavailable(&c.check(ACCOUNT).await));
            lab.verify().await;
        }
    }

    #[test]
    fn an_account_s_failure_does_not_end_a_lab_wide_backoff() {
        let b = FailureBackoff::default();
        let t0 = Instant::now();
        b.record::<()>(ACCOUNT, &Err(LabError::Transport("timeout".into())), t0);
        b.record::<()>(ACCOUNT, &Err(LabError::Malformed("bad".into())), t0);
        assert!(b.blocked(OTHER, t0 + Duration::from_secs(9)).is_some());
        assert!(b.blocked(OTHER, t0 + FAILURE_BACKOFF).is_none());
        // A success ends both.
        b.record(ACCOUNT, &Ok(()), t0);
        assert!(b.blocked(ACCOUNT, t0).is_none());
        assert!(b.blocked(OTHER, t0).is_none());
    }

    #[test]
    fn remembered_account_failures_are_pruned() {
        let b = FailureBackoff::default();
        let t0 = Instant::now();
        b.record::<()>(ACCOUNT, &Err(LabError::Status(422)), t0);
        assert_eq!(b.blocked(ACCOUNT, t0), Some(LabError::Status(422)));
        b.record::<()>(OTHER, &Err(LabError::Status(422)), t0 + FAILURE_BACKOFF);
        assert_eq!(b.accounts.lock().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn a_lab_wide_failure_pauses_every_account() {
        // 5xx, 429, 401 and 403 say the Lab (or this engine's access to it) is in
        // trouble: no account is asked about for 10 s.
        for status in [500, 503, 429, 401, 403] {
            let lab = MockServer::start().await;
            Mock::given(method("GET"))
                .respond_with(ResponseTemplate::new(status))
                .expect(1)
                .mount(&lab)
                .await;
            let c = cache(&lab);
            assert!(is_unavailable(&c.check(ACCOUNT).await), "{status}");
            assert!(is_unavailable(&c.check(OTHER).await), "{status}");
            lab.verify().await;
        }
    }
}
