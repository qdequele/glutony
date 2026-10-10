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

use meili_ingest_lab::LabCredentials;
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

/// `LAB_URL` + `LAB_INSTANCE_ID` + `LAB_INSTANCE_SECRET`: where the sender delivers and
/// how it signs (spec v2 §3.3). The control plane's credit check uses the same
/// credentials.
#[derive(Clone, Debug)]
pub struct LabConfig {
    creds: LabCredentials,
}

impl LabConfig {
    /// All three instance values, or none (`None`: no Lab). `legacy_secret` is the
    /// pre-v2 `LAB_EVENTS_SECRET`: when it is set (non-blank) boot is refused, because
    /// glutony has no working legacy Lab route (a v2 Lab checks legacy batches as
    /// Scrapix's and never accepts glutony's).
    pub fn from_values(
        url: Option<String>,
        instance_id: Option<String>,
        instance_secret: Option<String>,
        legacy_secret: Option<String>,
    ) -> anyhow::Result<Option<Self>> {
        if legacy_secret.is_some_and(|s| !s.trim().is_empty()) {
            anyhow::bail!(
                "LAB_EVENTS_SECRET is set, but glutony has no working legacy Lab route: the Lab \
                 only accepts glutony events signed with instance credentials. Remove \
                 LAB_EVENTS_SECRET and set LAB_INSTANCE_ID and LAB_INSTANCE_SECRET (mint them \
                 with `bin/rails lab:hosted_engine:create PRODUCT=glutony ...`)"
            );
        }
        Ok(
            LabCredentials::from_values(url, instance_id, instance_secret)?
                .map(|creds| Self { creds }),
        )
    }

    /// Read `LAB_URL`, `LAB_INSTANCE_ID` and `LAB_INSTANCE_SECRET`, and refuse a leftover
    /// `LAB_EVENTS_SECRET` (blank counts as unset).
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let get = |n: &str| std::env::var(n).ok().filter(|v| !v.trim().is_empty());
        Self::from_values(
            get("LAB_URL"),
            get("LAB_INSTANCE_ID"),
            get("LAB_INSTANCE_SECRET"),
            get("LAB_EVENTS_SECRET"),
        )
    }

    /// Lab base URL, without trailing slash.
    pub fn url(&self) -> &str {
        self.creds.url()
    }

    /// `{url}/internal/events`.
    pub fn events_url(&self) -> String {
        self.creds.endpoint("/internal/events")
    }

    /// The instance credentials.
    pub fn credentials(&self) -> &LabCredentials {
        &self.creds
    }
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
        let req = self.config.credentials().sign_batch(
            self.http
                .post(self.config.events_url())
                .header(reqwest::header::CONTENT_TYPE, "application/json"),
            meili_ingest_lab::unix_now(),
            &body,
        );
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
                "the Lab rejected LAB_INSTANCE_ID / LAB_INSTANCE_SECRET (401); lab events stay pending until the credentials are fixed"
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
    fn all_three_instance_values_or_nothing() {
        let fv = |u: Option<&str>, i: Option<&str>, s: Option<&str>| {
            LabConfig::from_values(
                u.map(String::from),
                i.map(String::from),
                s.map(String::from),
                None,
            )
        };
        assert!(fv(None, None, None).unwrap().is_none());
        let c = fv(Some("https://lab.example/"), Some("id"), Some("s3cret"))
            .unwrap()
            .unwrap();
        assert_eq!(c.url(), "https://lab.example");
        assert_eq!(c.events_url(), "https://lab.example/internal/events");
        assert_eq!(c.credentials().instance_id(), "id");
        assert!(!format!("{c:?}").contains("s3cret"));
        for bad in [
            fv(Some("https://lab.example"), None, None),
            fv(Some("https://lab.example"), Some("id"), None),
            fv(Some("https://lab.example"), None, Some("s")),
            fv(None, Some("id"), Some("s")),
            fv(None, Some("id"), None),
            fv(None, None, Some("s")),
        ] {
            assert!(bad.is_err());
        }
    }

    #[test]
    fn a_legacy_events_secret_refuses_to_boot() {
        // A v2 Lab checks X-Scrapix-Signature on its legacy path and attributes those
        // events to Scrapix: glutony has no working legacy route, so the secret is an
        // error whatever else is set.
        let url = Some("https://lab.example");
        for (u, i, s) in [
            (None, None, None),
            (url, None, None),
            (url, Some("id"), Some("s")),
        ] {
            let err = LabConfig::from_values(
                u.map(String::from),
                i.map(String::from),
                s.map(String::from),
                Some("leftover-v1-secret".into()),
            )
            .unwrap_err()
            .to_string();
            for needle in [
                "LAB_EVENTS_SECRET",
                "no working legacy Lab route",
                "LAB_INSTANCE_ID",
                "LAB_INSTANCE_SECRET",
                "bin/rails lab:hosted_engine:create PRODUCT=glutony",
            ] {
                assert!(err.contains(needle), "{needle:?} missing from {err}");
            }
            assert!(
                !err.contains("leftover-v1-secret"),
                "the secret is never echoed: {err}"
            );
        }
        // Blank counts as unset, as in from_env.
        assert!(
            LabConfig::from_values(
                url.map(String::from),
                Some("id".into()),
                Some("s".into()),
                Some("  ".into()),
            )
            .unwrap()
            .is_some()
        );
    }

    #[test]
    fn http_only_on_loopback_or_private_hosts() {
        for ok in [
            "http://127.0.0.1:8091",
            "http://localhost:8091",
            "http://10.0.0.5",
            "http://192.168.1.2:3000",
            "http://172.20.0.3",
            // The Lab's dev stack hands engines this one.
            "http://172.29.81.10:8081",
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
}
