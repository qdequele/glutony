//! [`ActivityContext`]: what a plugin can do with the activity it runs inside of —
//! heartbeat, observe cancellation, and report billable usage.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::types::UsageUnits;

/// Handle a plugin uses to heartbeat and observe cancellation.
///
/// Heartbeats are fire-and-forget: they are pushed into a channel that the worker
/// drains and forwards to Temporal, so calling [`ActivityContext::heartbeat`] is cheap and
/// never blocks. When constructed with [`ActivityContext::noop`] (tests, CLI runs)
/// heartbeats are discarded.
#[derive(Debug, Clone)]
pub struct ActivityContext {
    job_id: Uuid,
    step_id: String,
    attempt: u32,
    heartbeat_tx: Option<mpsc::UnboundedSender<String>>,
    cancelled: Arc<AtomicBool>,
    usage: Arc<Mutex<UsageUnits>>,
}

impl ActivityContext {
    /// Build a context wired to a heartbeat channel and a cancellation flag.
    pub fn new(
        job_id: Uuid,
        step_id: impl Into<String>,
        attempt: u32,
        heartbeat_tx: mpsc::UnboundedSender<String>,
        cancelled: Arc<AtomicBool>,
    ) -> Self {
        Self {
            job_id,
            step_id: step_id.into(),
            attempt,
            heartbeat_tx: Some(heartbeat_tx),
            cancelled,
            usage: Arc::new(Mutex::new(UsageUnits::default())),
        }
    }

    /// A context that discards heartbeats and is never cancelled. Useful in tests.
    pub fn noop() -> Self {
        Self {
            job_id: Uuid::nil(),
            step_id: "test".into(),
            attempt: 1,
            heartbeat_tx: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            usage: Arc::new(Mutex::new(UsageUnits::default())),
        }
    }

    /// The job (workflow) this activity belongs to.
    pub fn job_id(&self) -> Uuid {
        self.job_id
    }

    /// The pipeline step id being executed.
    pub fn step_id(&self) -> &str {
        &self.step_id
    }

    /// Current attempt number (1-based).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Record progress. Cheap and non-blocking; call every ~10 items in long loops.
    pub fn heartbeat(&self, details: impl Into<String>) {
        if let Some(tx) = &self.heartbeat_tx {
            // A closed receiver just means the worker stopped listening; ignore.
            let _ = tx.send(details.into());
        }
    }

    /// Whether cancellation has been requested. Plugins should return
    /// [`crate::PluginError::Cancelled`] promptly when this turns true.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Returns `Err(PluginError::Cancelled)` if cancellation was requested.
    pub fn check_cancelled(&self) -> Result<(), crate::PluginError> {
        if self.is_cancelled() {
            Err(crate::PluginError::Cancelled)
        } else {
            Ok(())
        }
    }

    /// Shared cancellation flag (the worker flips it when Temporal cancels the activity).
    pub fn cancellation_flag(&self) -> Arc<AtomicBool> {
        self.cancelled.clone()
    }

    /// Report billable work: tokens spent, audio transcribed, pages rendered.
    ///
    /// Only the plugin knows what it spent on an external service, so anything that
    /// calls one should record it. Units accumulate across calls, so a plugin looping
    /// over documents can record each response as it arrives. The worker reads the
    /// total back after `execute` returns and ships it with the step's usage event.
    ///
    /// This never blocks and never fails: if the accumulator is poisoned by a panic in
    /// another thread the units are dropped rather than propagating the panic into an
    /// unrelated plugin.
    pub fn record_usage(&self, units: UsageUnits) {
        if units.is_empty() {
            return;
        }
        if let Ok(mut total) = self.usage.lock() {
            total.merge(units);
        }
    }

    /// Total units recorded so far.
    pub fn usage(&self) -> UsageUnits {
        self.usage.lock().map(|u| *u).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_accumulates_across_calls() {
        let ctx = ActivityContext::noop();
        assert!(ctx.usage().is_empty());
        ctx.record_usage(UsageUnits::llm(10, 4));
        ctx.record_usage(UsageUnits::llm(6, 2));
        // Empty reports are ignored rather than counted as activity.
        ctx.record_usage(UsageUnits::none());
        let total = ctx.usage();
        assert_eq!(total.llm_input_tokens, 16);
        assert_eq!(total.llm_output_tokens, 6);
        assert_eq!(total.llm_requests, 2);
    }

    #[test]
    fn usage_is_shared_by_clones() {
        // Plugins clone the context into concurrent tasks; every clone must report
        // into the same accumulator or fan-out usage would be undercounted.
        let ctx = ActivityContext::noop();
        let clone = ctx.clone();
        clone.record_usage(UsageUnits::transcription(2.0));
        assert_eq!(ctx.usage().audio_seconds, 2.0);
    }
}
