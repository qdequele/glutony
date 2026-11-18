//! Resume a batch across activity retries.
//!
//! When one document still fails after the in-plugin retry, the step fails and Temporal
//! retries the whole activity. Without a checkpoint that retry re-sends every document,
//! paying again for the completions the failed attempt already had. So each finished
//! document's result is checkpointed ([`ActivityContext::record_progress`]) and the
//! retry applies those results instead of calling the provider again.
//!
//! The checkpoint rides in the Temporal heartbeat details, so it is capped
//! ([`MAX_CHECKPOINT_BYTES`]): once full, further documents are not recorded and a
//! retry simply redoes them. Heartbeats are best effort, so a retry may also see an
//! older checkpoint, or none; redoing a document is always safe.
//!
//! An entry is kept only when the retried attempt has the same plugin, the same
//! number of documents and the same id at that index: anything else is redone.

use std::collections::HashMap;

use meili_ingest_plugin_sdk::prelude::*;

/// Most bytes of results one checkpoint holds: well under Temporal's 2 MB payload
/// limit (and its 512 KB warning), leaving room for the heartbeat message.
pub(crate) const MAX_CHECKPOINT_BYTES: usize = 256 * 1024;

/// The documents of one batch finished so far, with their results.
pub(crate) struct Checkpoint {
    plugin: &'static str,
    total: usize,
    /// `[index, id, result]` per finished document.
    done: Vec<serde_json::Value>,
    bytes: usize,
    budget: usize,
    /// Whether `done` changed since it was last recorded.
    dirty: bool,
}

impl Checkpoint {
    /// A checkpoint for `docs`, seeded with what `previous` (the previous attempt's
    /// checkpoint) recorded for them. Returns it with those results by index.
    pub(crate) fn resume(
        plugin: &'static str,
        previous: Option<&serde_json::Value>,
        docs: &[Document],
        budget: usize,
    ) -> (Self, HashMap<usize, serde_json::Value>) {
        let mut checkpoint = Self {
            plugin,
            total: docs.len(),
            done: Vec::new(),
            bytes: 0,
            budget,
            dirty: false,
        };
        let mut resumed = HashMap::new();
        let entries = previous
            .filter(|p| p["plugin"] == plugin && p["total"] == docs.len())
            .and_then(|p| p["done"].as_array());
        for entry in entries.into_iter().flatten() {
            let (Some(index), Some(id), Some(result)) = (
                entry[0].as_u64().and_then(|i| usize::try_from(i).ok()),
                entry[1].as_str(),
                entry.get(2),
            ) else {
                continue;
            };
            if docs.get(index).is_some_and(|d| d.id == id) && !resumed.contains_key(&index) {
                resumed.insert(index, result.clone());
                checkpoint.push(index, id, result.clone());
            }
        }
        // The context already carries the previous checkpoint into this attempt's
        // heartbeats: nothing new to record yet.
        checkpoint.dirty = false;
        (checkpoint, resumed)
    }

    /// Note that document `index` (with id `id`) finished with `result`. Skipped when
    /// it would take the checkpoint over its budget: a retry then redoes it.
    pub(crate) fn push(&mut self, index: usize, id: &str, result: serde_json::Value) {
        let entry = serde_json::json!([index, id, result]);
        let size = entry.to_string().len() + 1;
        if self.bytes + size > self.budget {
            return;
        }
        self.bytes += size;
        self.done.push(entry);
        self.dirty = true;
    }

    /// Hand the checkpoint to the worker, if it changed since the last call.
    pub(crate) fn record(&mut self, ctx: &ActivityContext) {
        if !self.dirty {
            return;
        }
        self.dirty = false;
        ctx.record_progress(serde_json::json!({
            "plugin": self.plugin,
            "total": self.total,
            "done": self.done,
        }));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn docs(n: usize) -> Vec<Document> {
        (0..n)
            .map(|i| Document::with_id(format!("d{i}"), "x"))
            .collect()
    }

    fn recorded(c: &mut Checkpoint) -> serde_json::Value {
        let ctx = ActivityContext::noop();
        c.record(&ctx);
        ctx.progress().expect("recorded")
    }

    #[test]
    fn a_retry_gets_back_what_was_recorded() {
        let docs = docs(3);
        let (mut c, resumed) = Checkpoint::resume("p", None, &docs, MAX_CHECKPOINT_BYTES);
        assert!(resumed.is_empty());
        c.push(2, "d2", serde_json::json!({"reply": "two"}));
        c.push(0, "d0", serde_json::json!({"reply": "zero"}));
        let previous = recorded(&mut c);

        let (mut c, resumed) =
            Checkpoint::resume("p", Some(&previous), &docs, MAX_CHECKPOINT_BYTES);
        assert_eq!(resumed.len(), 2);
        assert_eq!(resumed[&2], serde_json::json!({"reply": "two"}));
        assert_eq!(resumed[&0], serde_json::json!({"reply": "zero"}));
        // The resumed entries carry on to a further retry.
        c.push(1, "d1", serde_json::json!({"reply": "one"}));
        let (_, resumed) =
            Checkpoint::resume("p", Some(&recorded(&mut c)), &docs, MAX_CHECKPOINT_BYTES);
        assert_eq!(resumed.len(), 3);
    }

    #[test]
    fn a_checkpoint_for_other_documents_is_ignored() {
        let (mut c, _) = Checkpoint::resume("p", None, &docs(2), MAX_CHECKPOINT_BYTES);
        c.push(0, "d0", serde_json::json!(1));
        c.push(1, "d1", serde_json::json!(1));
        let previous = recorded(&mut c);
        // Another plugin, another batch size, or another id at the index.
        let (_, r) = Checkpoint::resume("q", Some(&previous), &docs(2), MAX_CHECKPOINT_BYTES);
        assert!(r.is_empty());
        let (_, r) = Checkpoint::resume("p", Some(&previous), &docs(3), MAX_CHECKPOINT_BYTES);
        assert!(r.is_empty());
        let mut other = docs(2);
        other[1].id = "elsewhere".into();
        let (_, r) = Checkpoint::resume("p", Some(&previous), &other, MAX_CHECKPOINT_BYTES);
        assert_eq!(r.keys().copied().collect::<Vec<_>>(), vec![0]);
        // Garbage is no checkpoint.
        let garbage =
            serde_json::json!({"plugin": "p", "total": 2, "done": [["x"], 3, [9, "d9", 1]]});
        let (_, r) = Checkpoint::resume("p", Some(&garbage), &docs(2), MAX_CHECKPOINT_BYTES);
        assert!(r.is_empty());
    }

    #[test]
    fn results_past_the_budget_are_left_to_redo() {
        let docs = docs(3);
        let (mut c, _) = Checkpoint::resume("p", None, &docs, 40);
        c.push(0, "d0", serde_json::json!("short"));
        c.push(1, "d1", serde_json::json!("x".repeat(100)));
        c.push(2, "d2", serde_json::json!("ok"));
        let (_, r) = Checkpoint::resume("p", Some(&recorded(&mut c)), &docs, 40);
        let mut kept: Vec<_> = r.keys().copied().collect();
        kept.sort_unstable();
        assert_eq!(kept, vec![0, 2]);
    }

    #[test]
    fn nothing_new_is_not_recorded_again() {
        let (mut c, _) = Checkpoint::resume("p", None, &docs(1), MAX_CHECKPOINT_BYTES);
        let ctx = ActivityContext::noop();
        c.record(&ctx);
        assert!(
            ctx.progress().is_none(),
            "an empty checkpoint is not recorded"
        );
    }
}
