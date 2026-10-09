//! Meilisearch Lab client (platform contract v2, spec §3).
//!
//! What the gateway and the control plane share about the Lab: the per-instance
//! credentials (`LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET`), the headers of
//! service calls and signed event batches, `GET /internal/instances/me` (are these
//! credentials good, and which product and region is this?) and
//! `GET /internal/accounts/{id}` (can this account still spend?). No business logic
//! lives here; the callers decide what to do.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use std::time::{SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use sha2::Sha256;

/// Header naming the reporting deployment on every engine-to-Lab request.
pub const H_INSTANCE_ID: &str = "x-lab-instance-id";
/// Header carrying the unix timestamp an event batch was signed with.
pub const H_TIMESTAMP: &str = "x-lab-timestamp";
/// Header carrying `sha256=<hex HMAC>` of an event batch.
pub const H_SIGNATURE: &str = "x-lab-signature";

/// Accept a Lab base URL: `https`, or `http` when the host is loopback or private.
/// Returns it without a trailing slash.
pub fn validate_lab_url(url: &str) -> anyhow::Result<String> {
    let parsed = url::Url::parse(url).map_err(|e| anyhow::anyhow!("LAB_URL is not a URL: {e}"))?;
    let private = match parsed.host() {
        Some(url::Host::Domain(d)) => d == "localhost",
        Some(url::Host::Ipv4(ip)) => ip.is_loopback() || ip.is_private(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback() || ip.is_unique_local(),
        None => false,
    };
    match parsed.scheme() {
        "https" => {}
        "http" if private => {}
        "http" => anyhow::bail!("LAB_URL must be https unless its host is loopback or private"),
        other => anyhow::bail!("LAB_URL has unsupported scheme {other:?}"),
    }
    Ok(url.trim_end_matches('/').to_string())
}

/// `LAB_URL` + `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET`.
#[derive(Clone)]
pub struct LabCredentials {
    url: String,
    instance_id: String,
    secret: String,
}

impl std::fmt::Debug for LabCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabCredentials")
            .field("url", &self.url)
            .field("instance_id", &self.instance_id)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl LabCredentials {
    /// Validate and build. The id and secret are trimmed and must be non-empty.
    pub fn new(url: &str, instance_id: &str, secret: &str) -> anyhow::Result<Self> {
        let url = validate_lab_url(url)?;
        let instance_id = instance_id.trim();
        if instance_id.is_empty() {
            anyhow::bail!("LAB_INSTANCE_ID is empty");
        }
        let secret = secret.trim();
        if secret.is_empty() {
            anyhow::bail!("LAB_INSTANCE_SECRET is empty");
        }
        Ok(Self {
            url,
            instance_id: instance_id.to_string(),
            secret: secret.to_string(),
        })
    }

    /// All three values or none.
    pub fn from_values(
        url: Option<String>,
        instance_id: Option<String>,
        secret: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        match (url, instance_id, secret) {
            (None, None, None) => Ok(None),
            (Some(u), Some(i), Some(s)) => Self::new(&u, &i, &s).map(Some),
            (u, i, s) => anyhow::bail!(
                "LAB_URL, LAB_INSTANCE_ID and LAB_INSTANCE_SECRET go together \
                 (set: LAB_URL={}, LAB_INSTANCE_ID={}, LAB_INSTANCE_SECRET={})",
                u.is_some(),
                i.is_some(),
                s.is_some()
            ),
        }
    }

    /// Read `LAB_URL`, `LAB_INSTANCE_ID` and `LAB_INSTANCE_SECRET` (blank counts as unset).
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(
            get("LAB_URL"),
            get("LAB_INSTANCE_ID"),
            get("LAB_INSTANCE_SECRET"),
        )
    }

    /// Lab base URL, without trailing slash.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// This deployment's id in the Lab.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// `{url}{path}`.
    pub fn endpoint(&self, path: &str) -> String {
        format!("{}{path}", self.url)
    }

    /// Service-call headers (spec §3.3): `Authorization: Bearer <secret>` and the instance id.
    pub fn authorize(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        req.bearer_auth(&self.secret)
            .header(H_INSTANCE_ID, &self.instance_id)
    }

    /// Event-batch headers (spec §3.3): instance id, timestamp and signature over
    /// `"{timestamp}.{body}"`.
    pub fn sign_batch(
        &self,
        req: reqwest::RequestBuilder,
        timestamp: u64,
        body: &[u8],
    ) -> reqwest::RequestBuilder {
        req.header(H_INSTANCE_ID, &self.instance_id)
            .header(H_TIMESTAMP, timestamp.to_string())
            .header(
                H_SIGNATURE,
                sign_batch(self.secret.as_bytes(), timestamp, body),
            )
    }
}

/// `sha256=<hex HMAC-SHA256(secret, "{timestamp}.{body}")>`.
pub fn sign_batch(secret: &[u8], timestamp: u64, body: &[u8]) -> String {
    // HMAC accepts keys of any length; new_from_slice cannot fail for Hmac<Sha256>.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC takes any key length");
    mac.update(timestamp.to_string().as_bytes());
    mac.update(b".");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// Current unix time in seconds (0 if the clock is before 1970, which the Lab rejects
/// anyway).
pub fn unix_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Whether `id` is a Lab account id: the canonical lower-case hyphenated UUID form,
/// the only spelling the Lab keys accounts by. Also what makes it safe in a URL path.
pub fn is_lab_account(id: &str) -> bool {
    let b = id.as_bytes();
    b.len() == 36
        && b.iter().enumerate().all(|(i, c)| match i {
            8 | 13 | 18 | 23 => *c == b'-',
            _ => c.is_ascii_digit() || (b'a'..=b'f').contains(c),
        })
}

/// Which kind of reporting deployment this is (spec §3.1). Engines are hosted by
/// Meilisearch only (decision A), so there is exactly one kind; the enum keeps the
/// wire shape explicit and refuses anything else.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InstanceKind {
    /// Meilisearch-run: reports for any active account, every job debited.
    Hosted,
}

/// `GET /internal/instances/me` (spec §3.6).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InstanceInfo {
    /// This deployment's id.
    pub instance_id: String,
    /// Always `hosted`.
    pub kind: InstanceKind,
    /// `scrapix`, `lumen` or `glutony`.
    pub product: String,
    /// Region of the engine.
    #[serde(default)]
    pub region: Option<String>,
    /// The Lab's public base URL.
    #[serde(default)]
    pub lab_url: Option<String>,
}

/// `credits` of an account lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Credits {
    /// Spendable balance; 0 or less means no more work.
    pub balance: i64,
}

/// `GET /internal/accounts/{id}`: `{"active": false}` for an unknown or inactive account.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AccountLookup {
    /// Whether the account exists and is active.
    pub active: bool,
    /// The account id, when active.
    #[serde(default)]
    pub account_id: Option<String>,
    /// Plan tier, when active.
    #[serde(default)]
    pub tier: Option<String>,
    /// Balance, when active.
    #[serde(default)]
    pub credits: Option<Credits>,
    /// How long the Lab suggests caching this answer, in seconds.
    #[serde(default)]
    pub cache_ttl: Option<u64>,
}

impl AccountLookup {
    /// Active with a positive balance (spec §8: refuse when `credits.balance <= 0`).
    pub fn can_spend(&self) -> bool {
        self.active && self.credits.is_some_and(|c| c.balance > 0)
    }
}

/// Why a Lab service call failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LabError {
    /// Connect, DNS, TLS or timeout.
    #[error("Lab unreachable: {0}")]
    Transport(String),
    /// The Lab refused the instance credentials.
    #[error("the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401)")]
    Unauthorized,
    /// Any other non-2xx.
    #[error("the Lab answered {0}")]
    Status(u16),
    /// A 2xx whose body is not what the contract says, or an id that cannot be a path segment.
    #[error("the Lab answered with an unreadable body: {0}")]
    Malformed(String),
}

async fn get_json<T: DeserializeOwned>(
    http: &reqwest::Client,
    creds: &LabCredentials,
    path: &str,
) -> Result<T, LabError> {
    let resp = creds
        .authorize(http.get(creds.endpoint(path)))
        .send()
        .await
        .map_err(|e| LabError::Transport(e.to_string()))?;
    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        return Err(LabError::Unauthorized);
    }
    if !status.is_success() {
        return Err(LabError::Status(status.as_u16()));
    }
    resp.json::<T>()
        .await
        .map_err(|e| LabError::Malformed(e.to_string()))
}

/// Ask the Lab what this deployment is (spec §3.5).
pub async fn fetch_instance_info(
    http: &reqwest::Client,
    creds: &LabCredentials,
) -> Result<InstanceInfo, LabError> {
    get_json(http, creds, "/internal/instances/me").await
}

/// Look an account up (spec §8). `account_id` must be a canonical UUID: anything else
/// is refused here rather than sent as a path segment.
pub async fn fetch_account(
    http: &reqwest::Client,
    creds: &LabCredentials,
    account_id: &str,
) -> Result<AccountLookup, LabError> {
    if !is_lab_account(account_id) {
        return Err(LabError::Malformed(format!(
            "{account_id:?} is not a Lab account id"
        )));
    }
    get_json(http, creds, &format!("/internal/accounts/{account_id}")).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const ID: &str = "7b4a2c1e-5d6f-4a8b-9c0d-1e2f3a4b5c6d";

    #[test]
    fn the_batch_signature_matches_a_known_vector() {
        // printf '1700000000.{"events":[]}' | openssl dgst -sha256 -hmac secret
        assert_eq!(
            sign_batch(b"secret", 1_700_000_000, br#"{"events":[]}"#),
            "sha256=3947de27ec923573170fccda604ddfb25583ff98dc51e96cca2e11c59545026a"
        );
        // Not the v1 signature over the bare body.
        assert_ne!(
            sign_batch(b"secret", 1_700_000_000, br#"{"events":[]}"#),
            "sha256=a642b59553c93e227ec0f2f38910fbf71231a2197c00899833c00478cec86f34"
        );
    }

    #[test]
    fn credentials_go_together_and_redact_the_secret() {
        assert!(
            LabCredentials::from_values(None, None, None)
                .unwrap()
                .is_none()
        );
        for (u, i, s) in [
            (Some("https://lab.example"), None, None),
            (Some("https://lab.example"), Some(ID), None),
            (None, Some(ID), Some("s")),
            (Some("https://lab.example"), Some(" "), Some("s")),
            (Some("https://lab.example"), Some(ID), Some("")),
            (Some("http://lab.example"), Some(ID), Some("s")),
        ] {
            assert!(
                LabCredentials::from_values(
                    u.map(String::from),
                    i.map(String::from),
                    s.map(String::from)
                )
                .is_err(),
                "{u:?} {i:?} {s:?}"
            );
        }
        let c = LabCredentials::new("https://lab.example/", ID, "topsecret").unwrap();
        assert_eq!(c.url(), "https://lab.example");
        assert_eq!(
            c.endpoint("/internal/events"),
            "https://lab.example/internal/events"
        );
        assert!(!format!("{c:?}").contains("topsecret"));
        assert!(LabCredentials::new("http://127.0.0.1:8091", ID, "s").is_ok());
    }

    #[test]
    fn only_canonical_lowercase_uuids_are_lab_accounts() {
        assert!(is_lab_account(ID));
        assert!(!is_lab_account(&ID.to_uppercase()));
        assert!(!is_lab_account(&ID.replace('-', "")));
        assert!(!is_lab_account("hackersearch"));
        assert!(!is_lab_account(""));
        assert!(!is_lab_account("../internal/ping"));
    }

    #[tokio::test]
    async fn service_calls_carry_the_bearer_secret_and_the_instance_id() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/internal/instances/me"))
            .and(header("authorization", "Bearer topsecret"))
            .and(header("x-lab-instance-id", ID))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "instance_id": ID, "kind": "hosted", "product": "glutony",
                "region": "eu-west-1", "lab_url": lab.uri()
            })))
            .expect(1)
            .mount(&lab)
            .await;
        let creds = LabCredentials::new(&lab.uri(), ID, "topsecret").unwrap();
        let info = fetch_instance_info(&reqwest::Client::new(), &creds)
            .await
            .unwrap();
        assert_eq!(info.kind, InstanceKind::Hosted);
        assert_eq!(info.region.as_deref(), Some("eu-west-1"));
        assert_eq!(info.product, "glutony");
    }

    #[tokio::test]
    async fn account_lookups_parse_and_errors_are_classified() {
        let lab = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/internal/accounts/{ID}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "active": true, "account_id": ID, "tier": "pro",
                "credits": {"balance": 42}, "cache_ttl": 30
            })))
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/internal/accounts/00000000-0000-0000-0000-000000000000",
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"active": false})),
            )
            .mount(&lab)
            .await;
        Mock::given(method("GET"))
            .and(path(
                "/internal/accounts/11111111-1111-1111-1111-111111111111",
            ))
            .respond_with(ResponseTemplate::new(401))
            .mount(&lab)
            .await;
        let creds = LabCredentials::new(&lab.uri(), ID, "s").unwrap();
        let http = reqwest::Client::new();
        let ok = fetch_account(&http, &creds, ID).await.unwrap();
        assert!(ok.can_spend());
        assert_eq!(ok.credits.unwrap().balance, 42);
        let inactive = fetch_account(&http, &creds, "00000000-0000-0000-0000-000000000000")
            .await
            .unwrap();
        assert!(!inactive.can_spend());
        assert_eq!(
            fetch_account(&http, &creds, "11111111-1111-1111-1111-111111111111").await,
            Err(LabError::Unauthorized)
        );
        assert!(matches!(
            fetch_account(&http, &creds, "not-an-account").await,
            Err(LabError::Malformed(_))
        ));
        let dead = LabCredentials::new("http://127.0.0.1:1", ID, "s").unwrap();
        assert!(matches!(
            fetch_account(&http, &dead, ID).await,
            Err(LabError::Transport(_))
        ));
    }

    #[test]
    fn zero_balance_cannot_spend() {
        let a = AccountLookup {
            active: true,
            credits: Some(Credits { balance: 0 }),
            ..Default::default()
        };
        assert!(!a.can_spend());
        let b = AccountLookup {
            active: true,
            credits: None,
            ..Default::default()
        };
        assert!(!b.can_spend());
    }
}
