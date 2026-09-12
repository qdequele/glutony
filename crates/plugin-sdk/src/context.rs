//! [`ActivityContext`]: what a plugin can do with the activity it runs inside of
//! (heartbeat and observe cancellation).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::sync::mpsc;
use uuid::Uuid;

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
}
