//! [`ActivityContext`]: what a plugin can do with the activity it runs inside of —
//! heartbeat, checkpoint progress across retries, observe cancellation, report billable
//! usage, and reach the team's AI Gateway ([`AiTarget`]).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::mpsc;
use uuid::Uuid;

use crate::ai::AiTarget;
use crate::types::UsageUnits;

/// Handle a plugin uses to heartbeat, observe cancellation, record usage and, for AI plugins,
/// read the step's Gateway [`AiTarget`] (set by the worker, in memory only).
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
    ai_target: Arc<OnceLock<AiTarget>>,
    /// What the previous attempt of this activity checkpointed (see
    /// [`ActivityContext::previous_progress`]).
    previous_progress: Option<Arc<serde_json::Value>>,
    /// The latest checkpoint, repeated in every heartbeat of this attempt.
    progress: Arc<Mutex<Option<serde_json::Value>>>,
}

/// Key under which [`ActivityContext::heartbeat_details`] nests a checkpoint.
const PROGRESS_KEY: &str = "progress";
/// Key under which [`ActivityContext::heartbeat_details`] puts the free-text message
/// next to a checkpoint.
const MESSAGE_KEY: &str = "message";

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
            ai_target: Arc::new(OnceLock::new()),
            previous_progress: None,
            progress: Arc::new(Mutex::new(None)),
        }
    }

    /// A context that discards heartbeats and is never cancelled. Useful in tests.
    pub fn noop() -> Self {
        Self::detached(Uuid::nil(), "test", 1)
    }

    /// A context carrying the real ids of a step, but that discards heartbeats and is
    /// never cancelled. Used where no worker is listening, such as inside a WASM guest.
    pub fn detached(job_id: Uuid, step_id: impl Into<String>, attempt: u32) -> Self {
        Self {
            job_id,
            step_id: step_id.into(),
            attempt,
            heartbeat_tx: None,
            cancelled: Arc::new(AtomicBool::new(false)),
            usage: Arc::new(Mutex::new(UsageUnits::default())),
            ai_target: Arc::new(OnceLock::new()),
            previous_progress: None,
            progress: Arc::new(Mutex::new(None)),
        }
    }

    /// Hand this attempt what the previous attempt checkpointed with
    /// [`ActivityContext::record_progress`]. The worker calls it when it builds the
    /// context of a retried activity; `None` leaves the context as it is.
    ///
    /// The checkpoint also becomes this attempt's current one, so the heartbeats of an
    /// attempt that records nothing new keep carrying it to the next attempt.
    pub fn with_previous_progress(mut self, previous: Option<serde_json::Value>) -> Self {
        if let Some(previous) = previous {
            if let Ok(mut current) = self.progress.lock() {
                *current = Some(previous.clone());
            }
            self.previous_progress = Some(Arc::new(previous));
        }
        self
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

    /// Checkpoint progress, so that a retry of this activity can pick up where this
    /// attempt stopped instead of starting over (read it back with
    /// [`ActivityContext::previous_progress`]).
    ///
    /// The value is opaque to the worker and replaces the previous checkpoint. It
    /// travels in the Temporal heartbeat details, so keep it small (ids and counters,
    /// never document content or secrets): it is visible in Temporal and bound by its
    /// payload size limit. Cheap and non-blocking; it also sends a heartbeat.
    ///
    /// Heartbeats are throttled and best effort, so the next attempt may see an older
    /// checkpoint, or none. Only skip work on a retry whose repetition would be harmless.
    pub fn record_progress(&self, progress: serde_json::Value) {
        if let Ok(mut current) = self.progress.lock() {
            *current = Some(progress);
        }
        self.heartbeat("progress");
    }

    /// The checkpoint this attempt would hand to a retry: the last value given to
    /// [`ActivityContext::record_progress`], else the one inherited from the previous
    /// attempt.
    pub fn progress(&self) -> Option<serde_json::Value> {
        self.progress.lock().ok().and_then(|p| p.clone())
    }

    /// What the previous attempt of this activity checkpointed with
    /// [`ActivityContext::record_progress`]. `None` on the first attempt, when the
    /// previous attempt recorded nothing (or its last heartbeat was lost), in
    /// [`ActivityContext::noop`] contexts and for WASM and gRPC plugins.
    pub fn previous_progress(&self) -> Option<&serde_json::Value> {
        self.previous_progress.as_deref()
    }

    /// The heartbeat details the worker sends to Temporal for `message`: the bare
    /// message while nothing was checkpointed (as before checkpoints existed), else an
    /// object holding the checkpoint and the message. Read them back with
    /// [`ActivityContext::progress_from_heartbeat_details`].
    pub fn heartbeat_details(&self, message: impl Into<String>) -> serde_json::Value {
        match self.progress() {
            None => serde_json::Value::String(message.into()),
            Some(progress) => serde_json::json!({
                PROGRESS_KEY: progress,
                MESSAGE_KEY: message.into(),
            }),
        }
    }

    /// The checkpoint carried by heartbeat details that
    /// [`ActivityContext::heartbeat_details`] produced, or `None` for a plain message.
    pub fn progress_from_heartbeat_details(
        details: &serde_json::Value,
    ) -> Option<serde_json::Value> {
        details.as_object()?.get(PROGRESS_KEY).cloned()
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

    /// Attach the Gateway target AI plugins call for this step. The worker sets it before
    /// `execute`; the first call wins and later ones are ignored. Clones of the context see it.
    ///
    /// The target holds the team's API key, so it stays in memory: it is never serialized
    /// into Temporal history and its `Debug` output redacts the key.
    pub fn set_ai_target(&self, target: AiTarget) {
        let _ = self.ai_target.set(target);
    }

    /// The Gateway target for this step, if the worker attached one (hosted runs behind the
    /// Lab). `None` means the plugin falls back to its own configuration.
    pub fn ai_target(&self) -> Option<&AiTarget> {
        self.ai_target.get()
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
    fn a_noop_context_has_no_previous_progress() {
        let ctx = ActivityContext::noop();
        assert!(ctx.previous_progress().is_none());
        assert!(ctx.progress().is_none());
        // Recording works, it just has nowhere to go.
        ctx.record_progress(serde_json::json!({ "sent": 2 }));
        assert_eq!(ctx.progress(), Some(serde_json::json!({ "sent": 2 })));
        assert!(ctx.previous_progress().is_none());
    }

    #[test]
    fn heartbeat_details_stay_a_plain_message_until_progress_is_recorded() {
        let ctx = ActivityContext::noop();
        let details = ctx.heartbeat_details("keepalive");
        assert_eq!(details, serde_json::json!("keepalive"));
        assert!(ActivityContext::progress_from_heartbeat_details(&details).is_none());
    }

    #[test]
    fn every_heartbeat_carries_the_latest_progress() {
        let (tx, mut rx) = mpsc::unbounded_channel();
        let ctx = ActivityContext::new(Uuid::nil(), "s", 1, tx, Arc::default());
        ctx.record_progress(serde_json::json!({ "tasks": [1] }));
        ctx.record_progress(serde_json::json!({ "tasks": [1, 2] }));
        // Recording heartbeats, so the worker forwards the checkpoint promptly.
        assert!(rx.try_recv().is_ok());
        // A later free-text heartbeat (or the worker's keepalive) must not lose it:
        // Temporal keeps only the last details.
        let details = ctx.clone().heartbeat_details("waiting for task 2");
        assert_eq!(details["message"], "waiting for task 2");
        assert_eq!(
            ActivityContext::progress_from_heartbeat_details(&details),
            Some(serde_json::json!({ "tasks": [1, 2] }))
        );
    }

    #[test]
    fn previous_progress_is_carried_into_this_attempt() {
        let previous = serde_json::json!({ "tasks": [7] });
        let ctx = ActivityContext::detached(Uuid::nil(), "s", 2)
            .with_previous_progress(Some(previous.clone()));
        assert_eq!(ctx.previous_progress(), Some(&previous));
        // An attempt that records nothing hands the same checkpoint to the next one.
        assert_eq!(
            ActivityContext::progress_from_heartbeat_details(&ctx.heartbeat_details("keepalive")),
            Some(previous.clone())
        );
        // New progress replaces it for the next attempt, not for this one.
        ctx.record_progress(serde_json::json!({ "tasks": [7, 8] }));
        assert_eq!(ctx.previous_progress(), Some(&previous));
        assert_eq!(ctx.progress(), Some(serde_json::json!({ "tasks": [7, 8] })));
        // None changes nothing.
        let first = ActivityContext::noop().with_previous_progress(None);
        assert!(first.previous_progress().is_none() && first.progress().is_none());
    }

    #[test]
    fn a_detached_context_keeps_its_ids() {
        let job = Uuid::from_u128(42);
        let ctx = ActivityContext::detached(job, "enrich", 3);
        assert_eq!(ctx.job_id(), job);
        assert_eq!(ctx.step_id(), "enrich");
        assert_eq!(ctx.attempt(), 3);
        ctx.heartbeat("discarded");
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

    fn ai_target() -> crate::AiTarget {
        serde_json::from_str(
            r#"{"base_url":"http://l/v1","api_key":"k","key_id":"id","default_model":"m","vision_model":"v"}"#,
        )
        .unwrap()
    }

    #[test]
    fn ai_target_is_shared_by_clones() {
        let ctx = ActivityContext::noop();
        assert!(ctx.ai_target().is_none());
        ctx.clone().set_ai_target(ai_target());
        assert_eq!(ctx.ai_target().unwrap().key_id, "id");
        // First call wins.
        let mut other = ai_target();
        other.key_id = "other".into();
        ctx.set_ai_target(other);
        assert_eq!(ctx.ai_target().unwrap().key_id, "id");
    }
}
