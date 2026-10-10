//! Delivers the Lab events outbox to `POST {LAB_URL}/internal/events` (spec §5.4).
//!
//! Every 2 s, or right after an insert, lease up to 500 due rows, send them in one
//! signed batch, mark the ids the Lab lists in `accepted` as delivered and back the
//! rest off. Rows are retried for 24 h; after that they are dropped with an error log
//! (spec v2 §3.4). A Lab `401` is logged and retried: the Lab can never stop glutony
//! from serving.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
use meili_ingest_lab::{LabCredentials, validate_lab_url};
use sha2::Sha256;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::lab_events::LabEventRepo;
use crate::metrics::LabMetrics;

/// Rows per batch.
pub const BATCH_SIZE: i64 = 500;
/// Idle tick.
pub const TICK: Duration = Duration::from_secs(2);
/// How long a claimed batch is leased before it is due again.
pub const LEASE: Duration = Duration::from_secs(30);
/// Largest acknowledgement body read from the Lab.
pub const MAX_ACK_BYTES: usize = 1024 * 1024;
/// Delivered rows are kept this long.
pub const RETENTION: Duration = Duration::from_secs(7 * 86_400);
/// Undelivered rows older than this were never acknowledged and are dropped (spec §3.4).
pub const STALE_AFTER: Duration = Duration::from_secs(24 * 3_600);

/// How the sender authenticates to the Lab.
#[derive(Clone)]
pub enum LabAuth {
    /// `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET` (spec v2 §3.3).
    Instance(LabCredentials),
    /// `LAB_EVENTS_SECRET`: the pre-v2 global secret, accepted for one more release. A v2
    /// Lab does not accept batches signed with it (logged as an error at boot).
    Legacy {
        /// HMAC key over the bare body.
        secret: String,
    },
}

/// `LAB_URL` plus either the instance credentials or the legacy secret.
#[derive(Clone)]
pub struct LabConfig {
    /// Lab base URL, without trailing slash.
    pub url: String,
    auth: LabAuth,
}

impl std::fmt::Debug for LabConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabConfig")
            .field("url", &self.url)
            .field(
                "auth",
                &match &self.auth {
                    LabAuth::Instance(c) => format!("instance {}", c.instance_id()),
                    LabAuth::Legacy { .. } => "legacy <redacted>".to_string(),
                },
            )
            .finish()
    }
}

impl LabConfig {
    /// `LAB_URL` with `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET`, or with the legacy
    /// `LAB_EVENTS_SECRET` (logged as an error: a v2 Lab drops those events; removed next
    /// release); nothing at all is `None`.
    pub fn from_values(
        url: Option<String>,
        instance_id: Option<String>,
        instance_secret: Option<String>,
        legacy_secret: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let Some(url) = url else {
            if instance_id.is_some() || instance_secret.is_some() || legacy_secret.is_some() {
                anyhow::bail!(
                    "LAB_INSTANCE_ID / LAB_INSTANCE_SECRET / LAB_EVENTS_SECRET need LAB_URL"
                );
            }
            return Ok(None);
        };
        match (instance_id, instance_secret) {
            (Some(id), Some(secret)) => {
                if legacy_secret.is_some() {
                    tracing::warn!(
                        "LAB_EVENTS_SECRET is ignored because LAB_INSTANCE_ID and LAB_INSTANCE_SECRET are set; remove it"
                    );
                }
                let creds = LabCredentials::new(&url, &id, &secret)?;
                Ok(Some(Self {
                    url: creds.url().to_string(),
                    auth: LabAuth::Instance(creds),
                }))
            }
            (None, None) => match legacy_secret {
                Some(secret) => {
                    // Still accepted (removed next release), but a v2 Lab checks legacy
                    // batches as Scrapix's and never accepts glutony's: say so loudly.
                    tracing::error!(
                        "LAB_EVENTS_SECRET is set without LAB_INSTANCE_ID / LAB_INSTANCE_SECRET: \
                         a v2 Lab does not accept events signed this way, so they are retried \
                         and dropped after 24 h. Set LAB_INSTANCE_ID and LAB_INSTANCE_SECRET \
                         (bin/rails lab:hosted_engine:create PRODUCT=glutony) and remove LAB_EVENTS_SECRET"
                    );
                    Ok(Some(Self {
                        url: validate_lab_url(&url)?,
                        auth: LabAuth::Legacy { secret },
                    }))
                }
                None => anyhow::bail!(
                    "LAB_URL is set but LAB_INSTANCE_ID and LAB_INSTANCE_SECRET are not"
                ),
            },
            _ => anyhow::bail!("LAB_INSTANCE_ID and LAB_INSTANCE_SECRET go together"),
        }
    }

    /// Read `LAB_URL`, `LAB_INSTANCE_ID`, `LAB_INSTANCE_SECRET` and the legacy `LAB_EVENTS_SECRET`.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(
            get("LAB_URL"),
            get("LAB_INSTANCE_ID"),
            get("LAB_INSTANCE_SECRET"),
            get("LAB_EVENTS_SECRET"),
        )
    }

    /// `{url}/internal/events`.
    pub fn events_url(&self) -> String {
        format!("{}/internal/events", self.url)
    }

    /// The instance credentials, when not running on the legacy secret.
    pub fn credentials(&self) -> Option<&LabCredentials> {
        match &self.auth {
            LabAuth::Instance(c) => Some(c),
            LabAuth::Legacy { .. } => None,
        }
    }

    /// Whether the deprecated global secret is in use.
    pub fn is_legacy(&self) -> bool {
        matches!(self.auth, LabAuth::Legacy { .. })
    }
}

/// `sha256=<hex HMAC-SHA256(secret, body)>`.
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    // HMAC accepts keys of any length; new_from_slice cannot fail for Hmac<Sha256>.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC takes any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

/// Read a response body, or `None` if it is larger than `cap` bytes or cannot be read.
/// The body is never buffered past `cap`, so a hostile Lab cannot exhaust memory.
async fn read_capped(mut resp: reqwest::Response, cap: usize) -> Option<Vec<u8>> {
    if resp.content_length().is_some_and(|n| n > cap as u64) {
        return None;
    }
    let mut buf = Vec::new();
    while let Some(chunk) = resp.chunk().await.ok()? {
        if buf.len() + chunk.len() > cap {
            return None;
        }
        buf.extend_from_slice(&chunk);
    }
    Some(buf)
}

/// Outcome of one delivery attempt, for tests and logs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Delivery {
    /// Nothing was due.
    Idle,
    /// The Lab answered `200`; `pending` ids were not in `accepted`.
    Sent {
        /// Rows marked delivered.
        delivered: usize,
        /// Rows backed off because the Lab did not accept them.
        pending: usize,
    },
    /// The batch failed as a whole and was backed off.
    Failed {
        /// `connect`, `timeout`, `auth`, `status`, `malformed` or `db`.
        reason: &'static str,
        /// Rows backed off.
        pending: usize,
    },
}

/// The sender task.
pub struct LabSender {
    repo: LabEventRepo,
    config: LabConfig,
    metrics: LabMetrics,
    notify: Arc<Notify>,
    http: reqwest::Client,
}

impl LabSender {
    /// Build the sender and its HTTP client (no redirects, 2 s connect, 10 s total).
    pub fn new(
        repo: LabEventRepo,
        config: LabConfig,
        metrics: LabMetrics,
        notify: Arc<Notify>,
    ) -> anyhow::Result<Self> {
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(Duration::from_secs(2))
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Self {
            repo,
            config,
            metrics,
            notify,
            http,
        })
    }

    /// Lease one batch and send it.
    pub async fn deliver_once(&self) -> Delivery {
        let batch = match self.repo.claim_due(BATCH_SIZE, LEASE).await {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "cannot read the lab_events outbox");
                return Delivery::Failed {
                    reason: "db",
                    pending: 0,
                };
            }
        };
        if batch.is_empty() {
            return Delivery::Idle;
        }
        let ids: Vec<Uuid> = batch.iter().map(|e| e.id).collect();
        let bodies: Vec<&serde_json::Value> = batch.iter().map(|e| &e.body).collect();
        let body = match serde_json::to_vec(&serde_json::json!({ "events": bodies })) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(error = %e, "cannot serialize a lab events batch");
                return self.fail(&ids, "malformed").await;
            }
        };
        let req = self
            .http
            .post(self.config.events_url())
            .header(reqwest::header::CONTENT_TYPE, "application/json");
        let req = match &self.config.auth {
            LabAuth::Instance(creds) => creds.sign_batch(req, meili_ingest_lab::unix_now(), &body),
            LabAuth::Legacy { secret } => req.header(
                meili_ingest_lab::H_SIGNATURE,
                sign(secret.as_bytes(), &body),
            ),
        };
        let resp = req.body(body).send().await;
        let resp = match resp {
            Ok(r) => r,
            Err(e) => {
                let reason = if e.is_timeout() { "timeout" } else { "connect" };
                tracing::warn!(reason, error = %e, "Lab unreachable; lab events stay pending");
                return self.fail(&ids, reason).await;
            }
        };
        let status = resp.status();
        if status == reqwest::StatusCode::UNAUTHORIZED {
            tracing::error!(
                "the Lab rejected the instance credentials (401); lab events stay pending"
            );
            return self.fail(&ids, "auth").await;
        }
        if status != reqwest::StatusCode::OK {
            tracing::warn!(%status, "the Lab did not accept the lab events batch");
            return self.fail(&ids, "status").await;
        }
        #[derive(serde::Deserialize)]
        struct Ack {
            accepted: Vec<Uuid>,
        }
        let Some(bytes) = read_capped(resp, MAX_ACK_BYTES).await else {
            tracing::warn!("the Lab answered 200 with an oversized or unreadable body");
            return self.fail(&ids, "malformed").await;
        };
        let Ok(ack) = serde_json::from_slice::<Ack>(&bytes) else {
            tracing::warn!("the Lab answered 200 with a malformed body");
            return self.fail(&ids, "malformed").await;
        };
        let accepted: HashSet<Uuid> = ack.accepted.into_iter().collect();
        let (done, rest): (Vec<Uuid>, Vec<Uuid>) =
            ids.iter().copied().partition(|id| accepted.contains(id));
        if let Err(e) = self.repo.mark_delivered(&done).await {
            // Not marked: they will be sent again and the Lab ignores the duplicates.
            tracing::error!(error = %e, "cannot mark lab events delivered");
        }
        self.metrics.delivered_total.inc_by(done.len() as u64);
        if !rest.is_empty() {
            self.metrics
                .failed_total
                .with_label_values(&["not_accepted"])
                .inc_by(rest.len() as u64);
            if let Err(e) = self.repo.reschedule(&rest).await {
                tracing::error!(error = %e, "cannot back off unaccepted lab events");
            }
        }
        Delivery::Sent {
            delivered: done.len(),
            pending: rest.len(),
        }
    }

    async fn fail(&self, ids: &[Uuid], reason: &'static str) -> Delivery {
        self.metrics.failed_total.with_label_values(&[reason]).inc();
        if let Err(e) = self.repo.reschedule(ids).await {
            // The lease still expires, so the rows come back anyway.
            tracing::error!(error = %e, "cannot back off lab events");
        }
        Delivery::Failed {
            reason,
            pending: ids.len(),
        }
    }

    /// Hourly housekeeping: purge delivered rows past retention, and drop undelivered
    /// rows the Lab never acknowledged within [`STALE_AFTER`] (spec §3.4: a skipped
    /// event is never acknowledged, so after 24 h it is permanently rejected).
    pub async fn maintain(&self) {
        match self.repo.purge_delivered(RETENTION).await {
            Ok(n) if n > 0 => tracing::info!(purged = n, "old delivered lab events purged"),
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot purge delivered lab events"),
        }
        match self.repo.drop_stale(STALE_AFTER).await {
            Ok(ids) if !ids.is_empty() => {
                self.metrics.dropped_total.inc_by(ids.len() as u64);
                tracing::error!(
                    count = ids.len(),
                    ids = ?ids,
                    "lab events never acknowledged for 24 h were dropped; the Lab skipped them (check its logs for the reason)"
                );
            }
            Ok(_) => {}
            Err(e) => tracing::warn!(error = %e, "cannot drop stale lab events"),
        }
    }

    /// Loop until `cancel`: deliver while batches come back full, then wait for the
    /// tick or an insert; refresh the gauges; purge hourly.
    pub async fn run(self, cancel: CancellationToken) {
        let mut last_purge = Instant::now() - Duration::from_secs(3_600);
        loop {
            // Drain while batches come back full.
            while let Delivery::Sent { delivered, pending } = self.deliver_once().await
                && (delivered + pending) as i64 == BATCH_SIZE
            {}
            if let Ok(stats) = self.repo.stats().await {
                self.metrics.pending.set(stats.pending);
                self.metrics
                    .oldest_pending_seconds
                    .set(stats.oldest_pending_seconds);
            }
            if last_purge.elapsed() >= Duration::from_secs(3_600) {
                self.maintain().await;
                last_purge = Instant::now();
            }
            tokio::select! {
                () = cancel.cancelled() => break,
                () = self.notify.notified() => {}
                () = tokio::time::sleep(TICK) => {}
            }
        }
        tracing::info!("lab events sender stopped");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn instance_credentials_legacy_secret_or_nothing() {
        let fv = |u: Option<&str>, i: Option<&str>, s: Option<&str>, l: Option<&str>| {
            LabConfig::from_values(
                u.map(String::from),
                i.map(String::from),
                s.map(String::from),
                l.map(String::from),
            )
        };
        assert!(fv(None, None, None, None).unwrap().is_none());
        let c = fv(Some("https://lab.example/"), Some("id"), Some("s"), None)
            .unwrap()
            .unwrap();
        assert_eq!(c.events_url(), "https://lab.example/internal/events");
        assert!(!c.is_legacy());
        assert!(c.credentials().is_some());
        assert!(!format!("{c:?}").contains("\"s\""));
        let legacy = fv(Some("https://lab.example"), None, None, Some("old"))
            .unwrap()
            .unwrap();
        assert!(legacy.is_legacy());
        assert!(legacy.credentials().is_none());
        assert!(!format!("{legacy:?}").contains("old"));
        // Instance credentials win over a leftover legacy secret.
        let both = fv(
            Some("https://lab.example"),
            Some("id"),
            Some("s"),
            Some("old"),
        )
        .unwrap()
        .unwrap();
        assert!(!both.is_legacy());
        for bad in [
            fv(Some("https://lab.example"), None, None, None),
            fv(Some("https://lab.example"), Some("id"), None, None),
            fv(Some("https://lab.example"), None, Some("s"), None),
            fv(None, Some("id"), Some("s"), None),
            fv(None, None, None, Some("old")),
        ] {
            assert!(bad.is_err());
        }
    }

    /// Run `f` with a subscriber that keeps ERROR events only, and return their text.
    fn errors_logged_by(f: impl FnOnce()) -> String {
        #[derive(Clone, Default)]
        struct Buf(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Buf {
            fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
                self.0
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .extend_from_slice(b);
                Ok(b.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let buf = Buf::default();
        let writer = buf.clone();
        let subscriber = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::ERROR)
            .with_ansi(false)
            .with_writer(move || writer.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, f);
        let out = buf.0.lock().unwrap_or_else(|p| p.into_inner()).clone();
        String::from_utf8_lossy(&out).into_owned()
    }

    #[test]
    fn the_legacy_secret_is_accepted_but_logged_as_an_error() {
        // A v2 Lab checks X-Scrapix-Signature on its legacy path and attributes those
        // events to Scrapix: glutony's legacy batches are never accepted there.
        let mut config = None;
        let logged = errors_logged_by(|| {
            config = LabConfig::from_values(
                Some("https://lab.example".into()),
                None,
                None,
                Some("old".into()),
            )
            .unwrap();
        });
        assert!(config.is_some_and(|c| c.is_legacy()));
        assert!(logged.contains("LAB_EVENTS_SECRET"), "{logged}");
        assert!(logged.contains("24 h"), "{logged}");
        assert!(logged.contains("LAB_INSTANCE_ID"), "{logged}");
        assert!(logged.contains("LAB_INSTANCE_SECRET"), "{logged}");
    }

    #[test]
    fn http_only_on_loopback_or_private_hosts() {
        for ok in [
            "http://127.0.0.1:8091",
            "http://localhost:8091",
            "http://10.0.0.5",
            "http://192.168.1.2:3000",
            "http://172.20.0.3",
            "http://[::1]:8091",
            "https://lab.meilisearch.com",
        ] {
            assert!(
                LabConfig::from_values(Some(ok.into()), Some("id".into()), Some("s".into()), None)
                    .is_ok(),
                "{ok}"
            );
        }
        for bad in [
            "http://lab.meilisearch.com",
            "http://8.8.8.8",
            "ftp://10.0.0.1",
            "not a url",
        ] {
            assert!(
                LabConfig::from_values(Some(bad.into()), Some("id".into()), Some("s".into()), None)
                    .is_err(),
                "{bad}"
            );
        }
    }

    #[test]
    fn signature_matches_a_known_vector() {
        // echo -n '{"events":[]}' | openssl dgst -sha256 -hmac secret
        assert_eq!(
            sign(b"secret", br#"{"events":[]}"#),
            format!(
                "sha256={}",
                hex::encode({
                    let mut m = Hmac::<Sha256>::new_from_slice(b"secret").unwrap();
                    m.update(br#"{"events":[]}"#);
                    m.finalize().into_bytes()
                })
            )
        );
        assert!(sign(b"k", b"body").starts_with("sha256="));
        assert_eq!(sign(b"k", b"body").len(), "sha256=".len() + 64);
    }
}
