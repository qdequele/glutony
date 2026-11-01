//! [`AiTarget`]: where an AI plugin sends its requests, and how a Gateway refusal maps to a
//! [`PluginError`].
//!
//! Behind the Meilisearch Lab, every team's AI calls go through the team's own hosted Gateway
//! (Lumen) with a per-team key. The worker fetches that key from the Lab at step time and
//! attaches it to the step's [`crate::ActivityContext`]. It lives in memory only: it is never
//! written to Temporal history, logs or error messages, and the `Debug` output redacts it.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use serde::Deserialize;

use crate::error::PluginError;

/// Longest body excerpt carried into an error message, in chars.
const SNIPPET_CHARS: usize = 512;

/// The Gateway endpoint, key and models an AI plugin uses for one step.
///
/// Deserializes from the Lab's answer (`base_url`, `api_key`, `key_id`, `default_model`,
/// `vision_model`, optional `decision_model`); other fields such as `cache_ttl` are ignored.
/// Clones share the "rejected" flag, so a failure seen by one concurrent task is seen by all.
///
/// Deliberately not `Serialize`: the key must never end up in Temporal inputs or results.
#[derive(Clone, Deserialize)]
pub struct AiTarget {
    /// Base URL of the Gateway's OpenAI-compatible API (e.g. `https://gw.example/v1`).
    pub base_url: String,
    /// The team's Gateway key. Never log it or put it in an error.
    pub api_key: String,
    /// Lab-side id of that key, safe to log; the worker uses it to ask for a replacement.
    pub key_id: String,
    /// Model for text completions.
    pub default_model: String,
    /// Model for requests that carry images.
    pub vision_model: String,
    /// Model for decision-style (classification, routing) calls, when the team sets one.
    #[serde(default)]
    pub decision_model: Option<String>,
    /// Set once the Gateway refused the key; shared by every clone.
    #[serde(skip)]
    rejected: Arc<AtomicBool>,
}

impl AiTarget {
    /// Build a target in code (plugins' tests, the worker). Starts not rejected.
    pub fn new(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        key_id: impl Into<String>,
        default_model: impl Into<String>,
        vision_model: impl Into<String>,
        decision_model: Option<String>,
    ) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            key_id: key_id.into(),
            default_model: default_model.into(),
            vision_model: vision_model.into(),
            decision_model,
            rejected: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Record that the Gateway refused this key, so the worker fetches a new one.
    pub fn mark_rejected(&self) {
        self.rejected.store(true, Ordering::Relaxed);
    }

    /// Whether the Gateway refused this key (on this target or any clone of it).
    pub fn is_rejected(&self) -> bool {
        self.rejected.load(Ordering::Relaxed)
    }
}

impl fmt::Debug for AiTarget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AiTarget")
            .field("base_url", &self.base_url)
            .field("api_key", &"<redacted>")
            .field("key_id", &self.key_id)
            .field("default_model", &self.default_model)
            .field("vision_model", &self.vision_model)
            .field("decision_model", &self.decision_model)
            .field("rejected", &self.is_rejected())
            .finish()
    }
}

/// Map a Gateway-specific HTTP failure to a [`PluginError`], or `None` when the status is not
/// the Gateway's own (the plugin then applies its usual mapping).
///
/// * 401 / 403: the key was revoked or rotated. Marks `target` rejected and returns a
///   retryable error, so the retry runs with a freshly fetched key.
/// * 402: the team's Gateway budget is spent. Retrying cannot help until the team adds Lab
///   credits, so the error is final.
///
/// The error never contains the API key.
pub fn gateway_failure(target: &AiTarget, code: u16, body: &str) -> Option<PluginError> {
    match code {
        401 | 403 => {
            target.mark_rejected();
            Some(PluginError::Retryable(format!(
                "the Gateway refused the Lab key (HTTP {code}); retrying with a new key"
            )))
        }
        402 => Some(PluginError::NonRetryable(format!(
            "the team's Gateway budget is exhausted: add Lab credits (HTTP 402): {}",
            truncate_chars(body, SNIPPET_CHARS)
        ))),
        _ => None,
    }
}

/// Truncate to at most `max` chars on a char boundary.
fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> AiTarget {
        serde_json::from_str(
            r#"{"base_url":"http://l/v1","api_key":"sk-test","key_id":"id","default_model":"m","vision_model":"v","cache_ttl":300}"#,
        )
        .unwrap()
    }

    #[test]
    fn debug_redacts_the_api_key() {
        let t = target();
        let shown = format!("{t:?}");
        assert!(!shown.contains("sk-test"), "{shown}");
        assert!(shown.contains("<redacted>"), "{shown}");
    }

    #[test]
    fn new_starts_not_rejected() {
        let t = AiTarget::new("http://l/v1", "k", "id", "m", "v", Some("d".into()));
        assert_eq!(t.base_url, "http://l/v1");
        assert_eq!(t.api_key, "k");
        assert_eq!(t.key_id, "id");
        assert_eq!(t.default_model, "m");
        assert_eq!(t.vision_model, "v");
        assert_eq!(t.decision_model.as_deref(), Some("d"));
        assert!(!t.is_rejected());
    }

    #[test]
    fn deserializes_the_lab_answer() {
        let t: AiTarget = serde_json::from_str(
            r#"{"base_url":"http://l/v1","api_key":"k","key_id":"id","default_model":"m","vision_model":"v","cache_ttl":300}"#,
        )
        .unwrap();
        assert_eq!(t.base_url, "http://l/v1");
        assert_eq!(t.decision_model, None);
        assert!(!t.is_rejected());
    }

    #[test]
    fn gateway_failure_401_marks_rejected_and_retries() {
        let t = target();
        let clone = t.clone();
        let err = gateway_failure(&t, 401, "bad key").expect("401 is ours");
        assert!(err.is_retryable());
        assert!(clone.is_rejected());
        assert!(!err.to_string().contains("sk-test"));
    }

    #[test]
    fn gateway_failure_402_is_final() {
        let t = target();
        let err = gateway_failure(&t, 402, "no credits").expect("402 is ours");
        assert!(matches!(&err, PluginError::NonRetryable(m) if m.contains("budget is exhausted")));
        assert!(!t.is_rejected());
    }

    #[test]
    fn gateway_failure_500_is_not_ours() {
        assert!(gateway_failure(&target(), 500, "boom").is_none());
    }
}
