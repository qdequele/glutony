//! Prometheus metrics of the control plane (spec §5.5), served at `GET /metrics`.

use prometheus::{Encoder, IntCounter, IntCounterVec, IntGauge, Opts, Registry, TextEncoder};

/// Lab events outbox metrics.
#[derive(Clone)]
pub struct LabMetrics {
    registry: Registry,
    /// `glutony_lab_events_pending`.
    pub pending: IntGauge,
    /// `glutony_lab_events_oldest_pending_seconds`.
    pub oldest_pending_seconds: IntGauge,
    /// `glutony_lab_events_delivered_total`.
    pub delivered_total: IntCounter,
    /// `glutony_lab_events_failed_total{reason}`.
    pub failed_total: IntCounterVec,
}

impl Default for LabMetrics {
    fn default() -> Self {
        let registry = Registry::new();
        let pending = IntGauge::new("glutony_lab_events_pending", "Lab events not delivered yet")
            .expect("valid metric");
        let oldest_pending_seconds = IntGauge::new(
            "glutony_lab_events_oldest_pending_seconds",
            "Age of the oldest undelivered Lab event",
        )
        .expect("valid metric");
        let delivered_total =
            IntCounter::new("glutony_lab_events_delivered_total", "Lab events delivered")
                .expect("valid metric");
        let failed_total = IntCounterVec::new(
            Opts::new(
                "glutony_lab_events_failed_total",
                "Lab event deliveries that failed",
            ),
            &["reason"],
        )
        .expect("valid metric");
        for m in [
            Box::new(pending.clone()) as Box<dyn prometheus::core::Collector>,
            Box::new(oldest_pending_seconds.clone()),
            Box::new(delivered_total.clone()),
            Box::new(failed_total.clone()),
        ] {
            registry.register(m).expect("unique metric");
        }
        Self {
            registry,
            pending,
            oldest_pending_seconds,
            delivered_total,
            failed_total,
        }
    }
}

impl std::fmt::Debug for LabMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LabMetrics").finish_non_exhaustive()
    }
}

/// Render every metric in the Prometheus text format.
pub fn render(metrics: &LabMetrics) -> String {
    let mut buf = Vec::new();
    let _ = TextEncoder::new().encode(&metrics.registry.gather(), &mut buf);
    String::from_utf8(buf).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_the_lab_metrics() {
        let m = LabMetrics::default();
        m.failed_total.with_label_values(&["auth"]).inc();
        let text = render(&m);
        for name in [
            "glutony_lab_events_pending",
            "glutony_lab_events_oldest_pending_seconds",
            "glutony_lab_events_delivered_total",
            "glutony_lab_events_failed_total{reason=\"auth\"} 1",
        ] {
            assert!(text.contains(name), "{name} missing from\n{text}");
        }
    }
}
