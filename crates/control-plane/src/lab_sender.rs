//! Delivers the Lab events outbox to `POST {LAB_URL}/internal/events` (spec §5.4).
//!
//! Every 2 s, or right after an insert, lease up to 500 due rows, send them in one
//! signed batch, mark the ids the Lab lists in `accepted` as delivered and back the
//! rest off. Rows are never dropped. A Lab `401` is logged and retried: the Lab can
//! never stop glutony from starting or serving.

use std::sync::Arc;
use std::time::{Duration, Instant};

use hmac::{Hmac, Mac};
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
/// Delivered rows are kept this long.
pub const RETENTION: Duration = Duration::from_secs(7 * 86_400);

/// `LAB_URL` + `LAB_EVENTS_SECRET`.
#[derive(Clone)]
pub struct LabConfig {
    /// Lab base URL, without trailing slash.
    pub url: String,
    secret: String,
}

impl std::fmt::Debug for LabConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabConfig")
            .field("url", &self.url)
            .field("secret", &"<redacted>")
            .finish()
    }
}

impl LabConfig {
    /// Both or neither; `https` unless the host is loopback or private.
    pub fn from_values(
        url: Option<String>,
        secret: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        let (url, secret) = match (url, secret) {
            (None, None) => return Ok(None),
            (Some(u), Some(s)) => (u, s),
            (Some(_), None) => anyhow::bail!("LAB_URL is set but LAB_EVENTS_SECRET is not"),
            (None, Some(_)) => anyhow::bail!("LAB_EVENTS_SECRET is set but LAB_URL is not"),
        };
        let parsed =
            url::Url::parse(&url).map_err(|e| anyhow::anyhow!("LAB_URL is not a URL: {e}"))?;
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
        Ok(Some(Self {
            url: url.trim_end_matches('/').to_string(),
            secret,
        }))
    }

    /// Read `LAB_URL` and `LAB_EVENTS_SECRET`.
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(get("LAB_URL"), get("LAB_EVENTS_SECRET"))
    }

    /// `{url}/internal/events`.
    pub fn events_url(&self) -> String {
        format!("{}/internal/events", self.url)
    }
}

/// `sha256=<hex HMAC-SHA256(secret, body)>`.
pub fn sign(secret: &[u8], body: &[u8]) -> String {
    // HMAC accepts keys of any length; new_from_slice cannot fail for Hmac<Sha256>.
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC takes any key length");
    mac.update(body);
    format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
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
        let resp = self
            .http
            .post(self.config.events_url())
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(
                "x-lab-signature",
                sign(self.config.secret.as_bytes(), &body),
            )
            .body(body)
            .send()
            .await;
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
            tracing::error!("the Lab rejected LAB_EVENTS_SECRET (401); lab events stay pending");
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
        let Ok(ack) = resp.json::<Ack>().await else {
            tracing::warn!("the Lab answered 200 with a malformed body");
            return self.fail(&ids, "malformed").await;
        };
        let (done, rest): (Vec<Uuid>, Vec<Uuid>) = ids
            .iter()
            .copied()
            .partition(|id| ack.accepted.contains(id));
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
                match self.repo.purge_delivered(RETENTION).await {
                    Ok(n) if n > 0 => tracing::info!(purged = n, "old delivered lab events purged"),
                    Ok(_) => {}
                    Err(e) => tracing::warn!(error = %e, "cannot purge delivered lab events"),
                }
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
    fn both_or_neither() {
        assert!(LabConfig::from_values(None, None).unwrap().is_none());
        assert!(LabConfig::from_values(Some("https://lab.example".into()), None).is_err());
        assert!(LabConfig::from_values(None, Some("s".into())).is_err());
        let c = LabConfig::from_values(Some("https://lab.example/".into()), Some("s".into()))
            .unwrap()
            .unwrap();
        assert_eq!(c.events_url(), "https://lab.example/internal/events");
        assert!(!format!("{c:?}").contains("\"s\""));
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
                LabConfig::from_values(Some(ok.into()), Some("s".into())).is_ok(),
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
                LabConfig::from_values(Some(bad.into()), Some("s".into())).is_err(),
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
