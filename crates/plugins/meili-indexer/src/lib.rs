//! # `meili_indexer` — push documents into Meilisearch
//!
//! The only built-in plugin that talks to Meilisearch. It never reads
//! `MEILI_URL` / `MEILI_API_KEY` from the environment: the tenant
//! [`MeiliContext`] (host, API key, resolved index) is injected into the step
//! `config` by the workflow (see SPEC §7.4 and
//! [`meili_ingest_plugin_sdk::inject_meili_context`]) and deserialized here as
//! an [`IndexerConfig`].
//!
//! Behaviour:
//! 1. validate the config (`host`, `api_key` and `index` are mandatory);
//! 2. look the index up with `GET /indexes/{uid}`. When it does not exist and
//!    `auto_create_index` is `false`, fail with a [`PluginError::NonRetryable`] naming
//!    the index and send nothing: an addition would make Meilisearch create it
//!    implicitly. Otherwise (the default) enqueue its creation with `primary_key`,
//!    without waiting: Meilisearch runs tasks in enqueue order, so the
//!    document additions sent next always run after it, and waiting would only double
//!    the time spent behind other tenants' tasks on a busy instance. Its outcome is
//!    checked once the documents are done;
//! 3. flatten every [`Document`] with [`Document::to_index_json`] and send it via
//!    `addOrReplace` with `?primaryKey=<primary_key>`, except when the index already
//!    has a primary key: then no `primaryKey` is sent and Meilisearch uses the index's
//!    own (any other value is rejected with `index_primary_key_already_exists`).
//!    Batches are cut at whichever of `batch_size` (documents, default
//!    1000) and `max_batch_bytes` (serialized bytes, default 50 MiB) comes first. A
//!    document too large for any batch is a [`PluginError::NonRetryable`] naming it;
//!    sending it would earn a `413` the retry policy would repeat forever;
//! 4. when `wait_for_completion` (default `true`), poll each task until it
//!    finishes; a `failed` task is a [`PluginError::NonRetryable`] carrying the
//!    Meilisearch error message;
//! 5. with `prune` (default `false`, needs `wait_for_completion`), once every addition
//!    succeeded, delete by filter the documents that share a `_meta.parent_id` with a
//!    document of this step but were not sent by it: the stale chunks of a document
//!    that got shorter. The index must have `_meta.parent_id` and its primary key
//!    filterable and the key must be allowed to delete documents; the indexer never
//!    changes index settings, so a missing one fails the step (non-retryable) saying so;
//!    a parent whose documents carry `_meta.chunk_total` is pruned only when the step
//!    holds all of them (a pipeline that fans the indexer out with `prune` is rejected);
//! 6. return [`PluginOutput::Indexed`] with the [`IndexReport`].
//!
//! Network failures, timeouts and 5xx responses are [`PluginError::Retryable`];
//! authentication problems (401/403, `auth` error type) and invalid requests are
//! [`PluginError::NonRetryable`].
//!
//! Every enqueued batch is checkpointed ([`ActivityContext::record_progress`]) with its
//! task uid. A retry of the step (a task that did not finish within
//! `task_timeout_secs`, a 5xx on a later batch) waits on the tasks the previous
//! attempt enqueued instead of sending those batches again, and only sends the rest.
//! It trusts a checkpoint only for the same destination and the same documents, and
//! only the leading batches whose task Meilisearch still knows (same index, same
//! enqueue time); from the first one it cannot confirm, every batch is sent again, so
//! the additions keep their order. Checkpoints are best effort: without one, the
//! retry sends everything, which `addOrReplace` makes harmless.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use meili_ingest_plugin_sdk::IndexerConfig;
use meili_ingest_plugin_sdk::prelude::*;
use meilisearch_sdk::client::Client;
use meilisearch_sdk::documents::DocumentDeletionQuery;
use meilisearch_sdk::errors::{Error as MeiliError, ErrorCode, ErrorType, MeilisearchError};
use meilisearch_sdk::task_info::TaskInfo;
use meilisearch_sdk::tasks::Task;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Plugin name referenced by `steps[].plugin`. Equals [`INDEXER_PLUGIN`].
pub const NAME: &str = INDEXER_PLUGIN;

/// How often Meilisearch is polled while waiting for a task.
#[cfg(not(test))]
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Waiting is done in slices so a heartbeat is emitted between them.
#[cfg(not(test))]
const WAIT_SLICE: Duration = Duration::from_secs(10);
// Short in tests, so the give-up path of `wait_for_task` runs in about a second.
#[cfg(test)]
const POLL_INTERVAL: Duration = Duration::from_millis(10);
#[cfg(test)]
const WAIT_SLICE: Duration = Duration::from_millis(100);
/// Default time to wait for one task before giving up (retryable), when neither the
/// step's `task_timeout_secs` nor the worker's [`TASK_TIMEOUT_ENV`] says otherwise.
const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(120);
/// Worker-wide default for the per-task wait. A deployment whose Meilisearch is shared
/// with a busy writer raises it (and keeps it below the index step's timeout).
pub const TASK_TIMEOUT_ENV: &str = "INDEXER_TASK_TIMEOUT_SECS";

/// The per-task wait: step config, then [`TASK_TIMEOUT_ENV`], then the default.
fn task_timeout(configured: Option<u64>) -> Duration {
    configured
        .or_else(|| {
            std::env::var(TASK_TIMEOUT_ENV)
                .ok()
                .and_then(|v| v.trim().parse().ok())
        })
        .filter(|s| *s > 0)
        .map(Duration::from_secs)
        .unwrap_or(DEFAULT_TASK_TIMEOUT)
}

/// The Meilisearch indexer plugin. Stateless; construct with [`MeiliIndexerPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct MeiliIndexerPlugin;

impl MeiliIndexerPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// A validated indexer config, with the destination resolved to plain strings.
struct ParsedConfig {
    cfg: IndexerConfig,
    index: String,
    host: String,
    api_key: String,
}

/// Validate and deserialize the step config into an [`IndexerConfig`].
fn parse_config(config: Value) -> Result<ParsedConfig, PluginError> {
    if !config.is_object() {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: config must be a JSON object"
        )));
    }
    let cfg: IndexerConfig = serde_json::from_value(config)
        .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))?;
    let non_blank = |v: &Option<String>| {
        v.as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
    };
    let (Some(host), Some(api_key)) = (non_blank(&cfg.meili.host), non_blank(&cfg.meili.api_key))
    else {
        let why = match cfg.connection.as_deref() {
            // The worker activity resolves a connection before calling this plugin, so
            // reaching here means that step was skipped (an older worker, say).
            Some(name) => format!(
                "{NAME}: connection {name:?} was not resolved into a host and API key \
                 before this step ran"
            ),
            None => format!(
                "{NAME}: config has no `host`/`api_key`. Name a Meilisearch `connection` \
                 on this step, or send the tenant context (MeiliContext) with the request; \
                 this plugin never reads MEILI_URL/MEILI_API_KEY from the environment \
                 (SPEC §7.4)"
            ),
        };
        return Err(PluginError::InvalidConfig(why));
    };
    if cfg.batch_size == 0 {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: `batch_size` must be at least 1"
        )));
    }
    if cfg.max_batch_bytes == 0 {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: `max_batch_bytes` must be at least 1"
        )));
    }
    if cfg.prune && !cfg.wait_for_completion {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: `prune` needs `wait_for_completion`: stale documents are only \
             deleted once the additions are known to have succeeded"
        )));
    }
    if cfg.primary_key.trim().is_empty() {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: `primary_key` must not be empty"
        )));
    }
    let index = cfg
        .meili
        .index
        .clone()
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| {
            PluginError::InvalidConfig(format!(
                "{NAME}: no target index resolved. Set `index` in the step config, the \
                 X-Meili-Index header, the `?index=` query or MEILI_INDEX on the gateway (SPEC §3.4)"
            ))
        })?;
    Ok(ParsedConfig {
        cfg,
        index,
        host,
        api_key,
    })
}

/// A single document whose serialized size exceeds `max_batch_bytes` on its own.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct OversizedDoc {
    /// Position of the document in the input.
    index: usize,
    /// Its serialized size in bytes.
    bytes: usize,
}

/// Split documents into consecutive batches, given each document's serialized size.
///
/// A batch is cut at whichever of `max_docs` and `max_bytes` is reached first. Bytes are
/// counted as the JSON array Meilisearch actually receives: the documents, the commas
/// between them, and the enclosing brackets.
///
/// Pure and I/O-free so the splitting is testable without a Meilisearch. A document that
/// does not fit even alone is an error rather than a batch of one: sending it would earn
/// a `413` that the retry policy would repeat forever.
fn plan_batches(
    sizes: &[usize],
    max_docs: usize,
    max_bytes: u64,
) -> Result<Vec<std::ops::Range<usize>>, OversizedDoc> {
    const BRACKETS: u64 = 2;
    let mut batches = Vec::new();
    let mut start = 0usize;
    let mut bytes = BRACKETS;
    for (i, &size) in sizes.iter().enumerate() {
        let size64 = size as u64;
        if BRACKETS + size64 > max_bytes {
            return Err(OversizedDoc {
                index: i,
                bytes: size,
            });
        }
        let in_batch = i - start;
        let comma = u64::from(in_batch > 0);
        if in_batch > 0 && (in_batch >= max_docs || bytes + comma + size64 > max_bytes) {
            batches.push(start..i);
            start = i;
            bytes = BRACKETS + size64;
        } else {
            bytes += comma + size64;
        }
    }
    if start < sizes.len() {
        batches.push(start..sizes.len());
    }
    Ok(batches)
}

/// Whether an SDK error means "the index does not exist".
fn is_index_not_found(err: &MeiliError) -> bool {
    match err {
        MeiliError::Meilisearch(e) => matches!(e.error_code, ErrorCode::IndexNotFound),
        MeiliError::MeilisearchCommunication(c) => c.status_code == 404,
        _ => false,
    }
}

/// Map an SDK error onto the plugin error semantics (retryable vs not).
fn map_error(err: MeiliError, what: &str) -> PluginError {
    match err {
        MeiliError::Meilisearch(e) => {
            let msg = format!(
                "{NAME}: {what}: Meilisearch returned [{:?}] {}",
                e.error_code, e.error_message
            );
            match e.error_type {
                ErrorType::Auth => PluginError::NonRetryable(format!(
                    "{msg} (check the API key injected by the gateway)"
                )),
                ErrorType::Internal => PluginError::Retryable(msg),
                _ => PluginError::NonRetryable(msg),
            }
        }
        MeiliError::MeilisearchCommunication(c) => {
            let msg = format!(
                "{NAME}: {what}: unexpected HTTP {} from Meilisearch ({})",
                c.status_code,
                c.message.as_deref().unwrap_or("no body")
            );
            match c.status_code {
                401 | 403 => PluginError::NonRetryable(format!("{msg}; authentication failed")),
                408 | 425 | 429 | 500..=599 => PluginError::Retryable(msg),
                _ => PluginError::NonRetryable(msg),
            }
        }
        MeiliError::HttpError(e) => PluginError::Retryable(format!(
            "{NAME}: {what}: network error talking to Meilisearch: {e}"
        )),
        MeiliError::Timeout => {
            PluginError::Retryable(format!("{NAME}: {what}: timed out waiting for Meilisearch"))
        }
        MeiliError::InvalidRequest => PluginError::InvalidConfig(format!(
            "{NAME}: {what}: the api_key cannot be sent as an HTTP header (invalid characters)"
        )),
        MeiliError::ParseError(e) => PluginError::NonRetryable(format!(
            "{NAME}: {what}: unexpected response body from Meilisearch: {e}"
        )),
        other => PluginError::NonRetryable(format!("{NAME}: {what}: {other}")),
    }
}

/// 64-bit FNV-1a: a small, stable hash for checkpoint fingerprints. Not
/// cryptographic; it only has to tell one step input from another.
#[derive(Debug, Clone, Copy)]
struct Fnv(u64);

impl Fnv {
    fn new() -> Self {
        Self(0xcbf2_9ce4_8422_2325)
    }

    fn write(&mut self, bytes: &[u8]) {
        for b in bytes {
            self.0 ^= u64::from(*b);
            self.0 = self.0.wrapping_mul(0x0100_0000_01b3);
        }
    }

    /// Hash `part` so that the parts of a sequence cannot run into each other.
    fn part(mut self, part: &[u8]) -> Self {
        self.write(&(part.len() as u64).to_le_bytes());
        self.write(part);
        self
    }

    fn hex(self) -> String {
        format!("{:016x}", self.0)
    }
}

/// A payload's serialized size and a hash of its serialization.
#[derive(Debug, Clone, Copy)]
struct Measured {
    bytes: usize,
    hash: u64,
}

/// Measure `payload` serialized, without building the serialization.
fn measure(payload: &Value) -> Measured {
    struct Counter(usize, Fnv);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len();
            self.1.write(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter(0, Fnv::new());
    match serde_json::to_writer(&mut counter, payload) {
        Ok(()) => Measured {
            bytes: counter.0,
            hash: counter.1.0,
        },
        Err(_) => Measured {
            bytes: usize::MAX,
            hash: 0,
        },
    }
}

/// The attribute `prune` deletes by: the parent a derived document (a chunk, a
/// transcript segment) was cut from.
const PRUNE_PARENT_ATTRIBUTE: &str = "_meta.parent_id";
/// Most current documents one prune filter lists, so a large run is pruned with
/// several bounded deletions instead of one huge filter.
const PRUNE_IDS_PER_FILTER: usize = 1000;

/// `v` written as a Meilisearch filter value, or `None` when it cannot be written
/// safely: the filter syntax only unescapes `\"`, so a backslash could change what a
/// quoted string means.
fn filter_value(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if !s.contains('\\') => Some(format!("\"{}\"", s.replace('"', "\\\""))),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// The delete filters `prune` sends for `children`, the `(parent id, primary key
/// value)` of every document of this run that has a parent, in input order. Each
/// filter matches the documents of some parents that are not among this run's
/// documents of those parents:
/// `_meta.parent_id IN [<parents>] AND NOT <primary key> IN [<their current ids>]`.
///
/// Parents are grouped, in first-seen order, so that a filter lists at most `max_ids`
/// current ids, except a parent with more children than that, which gets a filter
/// of its own (a parent is never split: its current ids must all be excluded at once).
/// A parent with a value that cannot be written safely is left out entirely: excluding
/// a wrong set of ids would delete current documents.
fn prune_filters(children: &[(String, Value)], primary_key: &str, max_ids: usize) -> Vec<String> {
    let mut order: Vec<&str> = Vec::new();
    let mut ids: HashMap<&str, Vec<String>> = HashMap::new();
    let mut unsafe_parents: HashSet<&str> = HashSet::new();
    for (parent, id) in children {
        let parent = parent.as_str();
        if !ids.contains_key(parent) && !unsafe_parents.contains(parent) {
            order.push(parent);
        }
        match (
            filter_value(&Value::String(parent.to_owned())),
            filter_value(id),
        ) {
            (Some(_), Some(id)) if !unsafe_parents.contains(parent) => {
                ids.entry(parent).or_default().push(id);
            }
            (Some(_), Some(_)) => {}
            _ => {
                if unsafe_parents.insert(parent) {
                    tracing::warn!(
                        plugin = NAME,
                        parent_id = %parent,
                        "prune: an id of this parent cannot be written in a filter; \
                         its stale documents are left in place"
                    );
                }
                ids.remove(parent);
            }
        }
    }

    let mut filters = Vec::new();
    let mut group_parents: Vec<String> = Vec::new();
    let mut group_ids: Vec<String> = Vec::new();
    let flush = |parents: &mut Vec<String>, current: &mut Vec<String>, out: &mut Vec<String>| {
        if !parents.is_empty() {
            out.push(format!(
                "{PRUNE_PARENT_ATTRIBUTE} IN [{}] AND NOT {primary_key} IN [{}]",
                parents.join(", "),
                current.join(", ")
            ));
            parents.clear();
            current.clear();
        }
    };
    for parent in order {
        let Some(current) = ids.remove(parent) else {
            continue;
        };
        if group_ids.len() + current.len() > max_ids {
            flush(&mut group_parents, &mut group_ids, &mut filters);
        }
        // Checked safe above.
        group_parents.extend(filter_value(&Value::String(parent.to_owned())));
        group_ids.extend(current);
    }
    flush(&mut group_parents, &mut group_ids, &mut filters);
    filters
}

/// The `(parent id, primary key value)` of every document of `payloads` that has a
/// parent, in input order, leaving out the parents this step does not see whole.
///
/// A parent whose documents carry `_meta.chunk_total` is kept only when they all
/// agree on it and this step holds exactly that many distinct ids: otherwise some of
/// its current documents went to another invocation (a split input), and pruning
/// would delete them. Such a parent is skipped with a warning. A parent without
/// `chunk_total` cannot be checked and is kept.
fn complete_children(payloads: &[Value], primary_key: &str) -> Vec<(String, Value)> {
    let children: Vec<(&str, Value, Option<u64>)> = payloads
        .iter()
        .filter_map(|p| {
            let meta = p.get("_meta")?;
            let parent = meta.get("parent_id")?.as_str()?;
            let id = p.get(primary_key).cloned().unwrap_or(Value::Null);
            let total = meta.get("chunk_total").and_then(Value::as_u64);
            Some((parent, id, total))
        })
        .collect();

    // Per parent: the totals its documents announce, and the distinct ids present.
    let mut seen: HashMap<&str, (HashSet<Option<u64>>, HashSet<String>)> = HashMap::new();
    for (parent, id, total) in &children {
        let (totals, ids) = seen.entry(parent).or_default();
        totals.insert(*total);
        ids.insert(id.to_string());
    }
    let partial: HashSet<&str> = seen
        .into_iter()
        .filter(
            |(_, (totals, ids))| match totals.iter().collect::<Vec<_>>()[..] {
                [None] => false,
                [Some(total)] => usize::try_from(*total).ok() != Some(ids.len()),
                _ => true,
            },
        )
        .map(|(parent, (totals, ids))| {
            tracing::warn!(
                plugin = NAME,
                parent_id = %parent,
                present = ids.len(),
                chunk_total = ?totals,
                "prune: this step does not hold every document of this parent; \
                 its stale documents are left in place"
            );
            parent
        })
        .collect();

    children
        .into_iter()
        .filter(|(parent, _, _)| !partial.contains(parent))
        .map(|(parent, id, _)| (parent.to_owned(), id))
        .collect()
}

/// Add what `prune` needs to a non-retryable error of its deletion.
fn with_prune_hint(err: PluginError, primary_key: &str) -> PluginError {
    match err {
        PluginError::NonRetryable(msg) => PluginError::NonRetryable(format!(
            "{msg}. `prune` needs an API key allowed to delete documents \
             (documents.delete), and `{PRUNE_PARENT_ATTRIBUTE}` and `{primary_key}` in \
             the index's filterableAttributes"
        )),
        other => other,
    }
}

/// Delete the derived documents of the parents in `payloads` that this run did not
/// send (see [`prune_filters`]). Runs once the additions succeeded, so a failed run
/// never loses the previous version of a document.
async fn prune(
    client: &Client,
    ctx: &ActivityContext,
    index: &meilisearch_sdk::indexes::Index,
    payloads: &[Value],
    primary_key: &str,
    timeout: Duration,
) -> Result<(), PluginError> {
    let children = complete_children(payloads, primary_key);
    let mut tasks = Vec::new();
    for filter in prune_filters(&children, primary_key, PRUNE_IDS_PER_FILTER) {
        ctx.check_cancelled()?;
        let mut query = DocumentDeletionQuery::new(index);
        query.with_filter(&filter);
        let info = index.delete_documents_with(&query).await.map_err(|e| {
            with_prune_hint(
                map_error(e, "deleting stale derived documents"),
                primary_key,
            )
        })?;
        tracing::debug!(
            plugin = NAME,
            task_uid = info.task_uid,
            "stale derived documents deletion enqueued"
        );
        tasks.push(info.task_uid);
    }
    for uid in tasks {
        if let Some(err) =
            wait_for_task(client, ctx, uid, "stale document deletion", timeout).await?
        {
            return Err(with_prune_hint(
                task_failed(uid, "stale document deletion", &err),
                primary_key,
            ));
        }
    }
    Ok(())
}

/// A task uid the SDK can wait on or fetch.
struct TaskUid(u32);

impl AsRef<u32> for TaskUid {
    fn as_ref(&self) -> &u32 {
        &self.0
    }
}

/// Version of [`Checkpoint`], also how the indexer recognises its own checkpoint.
const CHECKPOINT_VERSION: u32 = 1;

/// What an attempt checkpoints so that a retry resumes instead of resending: the
/// batches it enqueued, in send order. Small on purpose (it travels in the Temporal
/// heartbeat details): fingerprints and task uids, never documents or the API key.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Checkpoint {
    /// Always [`CHECKPOINT_VERSION`].
    meili_indexer: u32,
    /// Fingerprint of the host and index the tasks were enqueued on.
    destination: String,
    /// Fingerprint of the serialized documents, in order.
    documents: String,
    /// The batches enqueued so far, a prefix of the batch plan.
    batches: Vec<EnqueuedBatch>,
}

/// One enqueued batch: the documents it covers and the task Meilisearch gave it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct EnqueuedBatch {
    start: usize,
    end: usize,
    task: u32,
    /// When Meilisearch enqueued the task, in Unix milliseconds: a task uid alone could
    /// name another task after the instance was reset.
    enqueued_at_ms: i64,
}

impl EnqueuedBatch {
    fn new(range: &std::ops::Range<usize>, info: &TaskInfo) -> Self {
        Self {
            start: range.start,
            end: range.end,
            task: info.task_uid,
            enqueued_at_ms: unix_ms(info.enqueued_at.unix_timestamp_nanos()),
        }
    }
}

fn unix_ms(nanos: i128) -> i64 {
    i64::try_from(nanos / 1_000_000).unwrap_or(i64::MAX)
}

impl Checkpoint {
    /// The previous attempt's checkpoint, when it is this plugin's and describes the
    /// same destination and documents.
    fn matching(previous: Option<&Value>, destination: &str, documents: &str) -> Option<Self> {
        let cp: Self = serde_json::from_value(previous?.clone()).ok()?;
        (cp.meili_indexer == CHECKPOINT_VERSION
            && cp.destination == destination
            && cp.documents == documents)
            .then_some(cp)
    }

    fn to_value(&self) -> Value {
        serde_json::to_value(self).unwrap_or(Value::Null)
    }
}

/// The leading batches of `checkpoint` whose task Meilisearch still has as enqueued
/// by the previous attempt: same position in the plan, same index, same enqueue time.
/// Stops at the first one it cannot confirm, so every batch after it is sent again
/// and the additions keep their order.
async fn confirmed_batches(
    client: &Client,
    index_uid: &str,
    plan: &[std::ops::Range<usize>],
    checkpoint: Checkpoint,
) -> Result<Vec<EnqueuedBatch>, PluginError> {
    let mut confirmed = Vec::new();
    for (range, batch) in plan.iter().zip(checkpoint.batches) {
        if range.start != batch.start || range.end != batch.end {
            break;
        }
        let task = match client.get_task(TaskUid(batch.task)).await {
            Ok(task) => task,
            // Pruned, from a reset instance, or a status this SDK does not know
            // (`canceled`): resend rather than trust it.
            Err(e) if is_task_not_found(&e) || matches!(e, MeiliError::ParseError(_)) => break,
            Err(e) => return Err(map_error(e, &format!("checking task {}", batch.task))),
        };
        let (index, enqueued_at) = match &task {
            Task::Enqueued { content } => (&content.index_uid, content.enqueued_at),
            Task::Processing { content } => (&content.index_uid, content.enqueued_at),
            Task::Succeeded { content } => (&content.index_uid, content.enqueued_at),
            Task::Failed { content } => (&content.task.index_uid, content.task.enqueued_at),
        };
        if index.as_deref() != Some(index_uid)
            || unix_ms(enqueued_at.unix_timestamp_nanos()) != batch.enqueued_at_ms
        {
            break;
        }
        confirmed.push(batch);
    }
    Ok(confirmed)
}

/// Whether an SDK error means "no such task".
fn is_task_not_found(err: &MeiliError) -> bool {
    match err {
        MeiliError::Meilisearch(e) => matches!(e.error_code, ErrorCode::TaskNotFound),
        MeiliError::MeilisearchCommunication(c) => c.status_code == 404,
        _ => false,
    }
}

/// Non-retryable error for a task that Meilisearch processed and marked `failed`.
fn task_failed(uid: u32, what: &str, err: &MeilisearchError) -> PluginError {
    PluginError::NonRetryable(format!(
        "{NAME}: Meilisearch task {uid} ({what}) failed: [{:?}] {}",
        err.error_code, err.error_message
    ))
}

/// Poll a task until it finishes. `Ok(None)` = succeeded, `Ok(Some(err))` = failed.
/// Heartbeats between polling slices; gives up (retryable) after `timeout`.
async fn wait_for_task(
    client: &Client,
    ctx: &ActivityContext,
    uid: u32,
    what: &str,
    timeout: Duration,
) -> Result<Option<MeilisearchError>, PluginError> {
    let mut waited = Duration::ZERO;
    loop {
        ctx.check_cancelled()?;
        ctx.heartbeat(format!("{NAME}: waiting for task {uid} ({what})"));
        match client
            .wait_for_task(TaskUid(uid), Some(POLL_INTERVAL), Some(WAIT_SLICE))
            .await
        {
            Ok(Task::Succeeded { .. }) => return Ok(None),
            Ok(Task::Failed { content }) => return Ok(Some(content.error)),
            Ok(Task::Enqueued { .. } | Task::Processing { .. }) => {}
            Err(MeiliError::Timeout) => {}
            Err(e) => return Err(map_error(e, &format!("polling task {uid} ({what})"))),
        }
        waited += WAIT_SLICE;
        if waited >= timeout {
            return Err(PluginError::Retryable(format!(
                "{NAME}: task {uid} ({what}) did not finish within {}s; Meilisearch may \
                 be busy with other tasks (raise task_timeout_secs or {TASK_TIMEOUT_ENV})",
                timeout.as_secs()
            )));
        }
    }
}

/// What [`ensure_index`] found (or did) for the target index.
#[derive(Debug)]
enum IndexState {
    /// The index did not exist; its creation (with the configured key) was enqueued.
    Creating(TaskInfo),
    /// The index exists, with its own primary key or none yet.
    Existing { primary_key: Option<String> },
}

impl IndexState {
    /// The `?primaryKey` to send on document additions. An index that already has a
    /// primary key gets none: Meilisearch rejects any other key with
    /// `index_primary_key_already_exists`, and without the parameter it uses its own.
    fn addition_primary_key<'a>(&self, configured: &'a str) -> Option<&'a str> {
        match self {
            Self::Existing {
                primary_key: Some(_),
            } => None,
            Self::Creating(_) | Self::Existing { primary_key: None } => Some(configured),
        }
    }

    /// The primary key the index keys documents by: its own, else the configured one
    /// (declared at creation or by the first addition).
    fn primary_key<'a>(&'a self, configured: &'a str) -> &'a str {
        match self {
            Self::Existing {
                primary_key: Some(own),
            } => own,
            Self::Creating(_) | Self::Existing { primary_key: None } => configured,
        }
    }

    fn creation(&self) -> Option<&TaskInfo> {
        match self {
            Self::Creating(task) => Some(task),
            Self::Existing { .. } => None,
        }
    }
}

/// Look the index up. When it does not exist, enqueue its creation (with
/// `primary_key`) without waiting for it if `create` is set, else fail: sending
/// documents to a missing index would have Meilisearch create it anyway.
async fn ensure_index(
    client: &Client,
    uid: &str,
    primary_key: &str,
    create: bool,
) -> Result<IndexState, PluginError> {
    match client.get_index(uid).await {
        Ok(index) => {
            if let Some(existing) = index.primary_key.as_deref()
                && existing != primary_key
            {
                tracing::info!(
                    plugin = NAME,
                    index = %uid,
                    primary_key = %existing,
                    configured_primary_key = %primary_key,
                    "index already has a primary key; using it instead of the configured one"
                );
            }
            Ok(IndexState::Existing {
                primary_key: index.primary_key,
            })
        }
        Err(e) if is_index_not_found(&e) && !create => Err(PluginError::NonRetryable(format!(
            "{NAME}: index {uid:?} does not exist and auto_create_index is false; create \
             it first, or check the index uid"
        ))),
        Err(e) if is_index_not_found(&e) => {
            tracing::info!(plugin = NAME, index = %uid, primary_key = %primary_key, "creating index");
            client
                .create_index(uid, Some(primary_key))
                .await
                .map(IndexState::Creating)
                .map_err(|e| map_error(e, &format!("creating index {uid:?}")))
        }
        Err(e) => Err(map_error(e, &format!("fetching index {uid:?}"))),
    }
}

/// Wait for the index creation and fail on anything but success or "already exists"
/// (another job, or an earlier attempt of this one, created it first).
async fn check_creation(
    client: &Client,
    ctx: &ActivityContext,
    task: &TaskInfo,
    timeout: Duration,
) -> Result<(), PluginError> {
    match wait_for_task(client, ctx, task.task_uid, "index creation", timeout).await? {
        None => Ok(()),
        Some(err) if matches!(err.error_code, ErrorCode::IndexAlreadyExists) => Ok(()),
        Some(err) => Err(task_failed(task.task_uid, "index creation", &err)),
    }
}

#[async_trait]
impl Plugin for MeiliIndexerPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Pushes documents into Meilisearch. The destination is the named `connection` \
                 when the step sets one, otherwise the tenant MeiliContext sent with the \
                 request. Creates the index when missing, batches addOrReplace calls by \
                 document count and serialized size, and waits for the tasks.",
            )
            .accepts([InputKind::Documents, InputKind::Many, InputKind::Empty])
            .produces(OutputKind::Indexed)
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                // `host`, `api_key`, `tenant_id` and `region` are NOT listed as
                // required and are marked `readOnly`: they are injected just before the
                // step runs, from the named `connection` or from the tenant's
                // MeiliContext. A pipeline author must never type them, least of all the
                // API key, and a schema-driven editor is expected to skip `readOnly`
                // properties rather than render an empty required field for a secret.
                // To pin a destination, name a `connection` instead.
                "required": [],
                "properties": {
                    "connection": {
                        "type": "string",
                        "format": "meili-connection",
                        "description": "Optional: name of a Meilisearch connection. When set it is the destination, and it wins over any Meilisearch context sent with the request. Required for pipelines run by a scheduled source, which have no request."
                    },
                    "host": {
                        "type": "string",
                        "readOnly": true,
                        "description": "Injected: Meilisearch base URL from the connection or the tenant MeiliContext. Never set this by hand."
                    },
                    "api_key": {
                        "type": "string",
                        "readOnly": true,
                        "writeOnly": true,
                        "description": "Injected: Meilisearch API key from the connection or the tenant MeiliContext. Never set this by hand."
                    },
                    "index": {
                        "type": "string",
                        "description": "Optional: pin the target index uid. When unset it is resolved from the pipeline's `trigger.index_pattern`, then the request, then the deployment default (SPEC §3.4)."
                    },
                    "tenant_id": {
                        "type": ["string", "null"],
                        "default": null,
                        "readOnly": true,
                        "description": "Injected: tenant id, for logging only."
                    },
                    "region": {
                        "type": ["string", "null"],
                        "default": null,
                        "readOnly": true,
                        "description": "Injected: region tag, for logging only."
                    },
                    "primary_key": {
                        "type": "string",
                        "default": "id",
                        "description": "Primary key declared when creating the index, and sent as ?primaryKey on document additions while the index has none. An index that already has a primary key keeps its own: additions then carry no ?primaryKey."
                    },
                    "auto_create_index": {
                        "type": "boolean",
                        "default": true,
                        "description": "Create the index when GET /indexes/{uid} returns 404. When false, a missing index fails the step (non-retryable) and no documents are sent, since Meilisearch would otherwise create the index on the first addition. The lookup runs either way, so an existing primary key is respected."
                    },
                    "batch_size": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 1000,
                        "description": "Maximum documents per addOrReplace request."
                    },
                    "max_batch_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "default": DEFAULT_MAX_BATCH_BYTES,
                        "description": "Maximum serialized bytes per addOrReplace request. A batch is cut at whichever of this and batch_size is reached first. Keep it under Meilisearch's http_payload_size_limit (100 MB by default)."
                    },
                    "wait_for_completion": {
                        "type": "boolean",
                        "default": true,
                        "description": "Poll every task until it succeeds; a failed task fails the step (non-retryable)."
                    },
                    "task_timeout_secs": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "How long to wait for one Meilisearch task before retrying the step. Defaults to the worker's INDEXER_TASK_TIMEOUT_SECS, else 120. Keep it below the step's timeout_secs."
                    },
                    "prune": {
                        "type": "boolean",
                        "default": false,
                        "description": "After the additions succeed, delete the documents that share a _meta.parent_id with a document of this step but were not sent by it (stale chunks of a document that got shorter). Needs wait_for_completion, a key allowed to delete documents, and _meta.parent_id plus the primary key in the index's filterableAttributes. Cannot be combined with a fan_out other than $ on this step; a parent whose chunks are not all in the step (per meta.chunk_total) is skipped."
                    }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: Value,
    ) -> Result<PluginOutput, PluginError> {
        let ParsedConfig {
            cfg,
            index: index_uid,
            host,
            api_key,
        } = parse_config(config)?;
        let docs = input.into_documents()?;
        tracing::info!(
            plugin = NAME,
            job_id = %ctx.job_id(),
            step_id = %ctx.step_id(),
            index = %index_uid,
            documents = docs.len(),
            context = %cfg.meili.redacted(),
            "indexing documents"
        );

        let host = host.trim_end_matches('/').to_owned();
        let client = Client::new(host, Some(api_key.as_str()))
            .map_err(|e| map_error(e, "building the Meilisearch client"))?;

        let timeout = task_timeout(cfg.task_timeout_secs);
        let state =
            ensure_index(&client, &index_uid, &cfg.primary_key, cfg.auto_create_index).await?;
        let creation = state.creation();
        let addition_primary_key = state.addition_primary_key(&cfg.primary_key);

        if docs.is_empty() {
            if let Some(task) = creation
                && cfg.wait_for_completion
            {
                check_creation(&client, ctx, task, timeout).await?;
            }
            return Ok(PluginOutput::Indexed(IndexReport {
                index: index_uid,
                document_count: 0,
                task_uids: Vec::new(),
            }));
        }

        let index = client.index(index_uid.clone());
        let document_count = docs.len();
        // Consumes the documents: their strings move into the payloads, so the input
        // is held once rather than twice while the batches are sent.
        let payloads: Vec<Value> = docs.into_iter().map(Document::into_index_json).collect();
        let measured: Vec<Measured> = payloads.iter().map(measure).collect();
        let sizes: Vec<usize> = measured.iter().map(|m| m.bytes).collect();
        let batches = plan_batches(&sizes, cfg.batch_size, cfg.max_batch_bytes).map_err(
            |OversizedDoc { index, bytes }| {
                PluginError::NonRetryable(format!(
                    "{NAME}: document {:?} is {bytes} bytes serialized, over \
                     `max_batch_bytes` ({}); it cannot be sent in any batch. Raise \
                     `max_batch_bytes` or chunk the document upstream",
                    payloads[index]["id"].as_str().unwrap_or_default(),
                    cfg.max_batch_bytes
                ))
            },
        )?;
        let batch_total = batches.len();

        // Resume what a previous attempt already enqueued.
        let destination = Fnv::new()
            .part(client.get_host().as_bytes())
            .part(index_uid.as_bytes())
            .hex();
        let documents = measured
            .iter()
            .fold(Fnv::new(), |h, m| h.part(&m.hash.to_le_bytes()))
            .hex();
        let mut checkpoint = Checkpoint {
            meili_indexer: CHECKPOINT_VERSION,
            destination,
            documents,
            batches: Vec::with_capacity(batch_total),
        };
        if let Some(previous) = Checkpoint::matching(
            ctx.previous_progress(),
            &checkpoint.destination,
            &checkpoint.documents,
        ) {
            checkpoint.batches = confirmed_batches(&client, &index_uid, &batches, previous).await?;
            tracing::info!(
                plugin = NAME,
                index = %index_uid,
                attempt = ctx.attempt(),
                resumed = checkpoint.batches.len(),
                batches = batch_total,
                "resuming: not sending again the batches the previous attempt enqueued"
            );
        }
        if ctx.previous_progress().is_some() {
            // Replace the inherited checkpoint now: it may describe other documents, or
            // batches that were just found stale.
            ctx.record_progress(checkpoint.to_value());
        }

        let resumed = checkpoint.batches.len();
        for (batch_no, range) in batches.into_iter().enumerate().skip(resumed) {
            ctx.check_cancelled()?;
            let payload = &payloads[range.clone()];
            ctx.heartbeat(format!(
                "{NAME}: sending batch {}/{batch_total} ({} documents)",
                batch_no + 1,
                payload.len()
            ));
            let info = index
                .add_or_replace(payload, addition_primary_key)
                .await
                .map_err(|e| map_error(e, &format!("adding documents (batch {})", batch_no + 1)))?;
            tracing::debug!(
                plugin = NAME,
                index = %index_uid,
                task_uid = info.task_uid,
                batch = batch_no + 1,
                documents = payload.len(),
                "batch enqueued"
            );
            checkpoint.batches.push(EnqueuedBatch::new(&range, &info));
            ctx.record_progress(checkpoint.to_value());
        }
        let task_uids: Vec<u32> = checkpoint.batches.iter().map(|b| b.task).collect();

        if cfg.wait_for_completion {
            for &uid in &task_uids {
                if let Some(err) =
                    wait_for_task(&client, ctx, uid, "document addition", timeout).await?
                {
                    return Err(task_failed(uid, "document addition", &err));
                }
            }
            // Enqueued before the additions, so it finished before them: no extra wait.
            if let Some(task) = creation {
                check_creation(&client, ctx, task, timeout).await?;
            }
        }
        // Only after every addition succeeded (`prune` requires waiting for them).
        if cfg.prune {
            let primary_key = state.primary_key(&cfg.primary_key);
            prune(&client, ctx, &index, &payloads, primary_key, timeout).await?;
        }

        Ok(PluginOutput::Indexed(IndexReport {
            index: index_uid,
            document_count,
            task_uids,
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{
        body_json, header, method, path, query_param, query_param_is_missing,
    };
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const INDEX: &str = "documents";

    fn config(host: &str) -> Value {
        json!({
            "host": host,
            "api_key": "test-key",
            "index": INDEX,
            "tenant_id": "proj-1",
            "region": "eu"
        })
    }

    fn docs() -> Vec<Document> {
        let mut a = Document::with_id("a", "hello world");
        a.title = Some("A".into());
        a.fields.insert("price".into(), json!(3));
        a.meta.page = Some(1);
        let b = Document::with_id("b", "second");
        vec![a, b]
    }

    fn task_info(uid: u32, ty: &str) -> Value {
        json!({
            "taskUid": uid,
            "indexUid": INDEX,
            "status": "enqueued",
            "type": ty,
            "enqueuedAt": "2026-01-01T00:00:00Z"
        })
    }

    fn task(uid: u32, status: &str, ty: &str, details: Value, error: Option<Value>) -> Value {
        json!({
            "uid": uid,
            "indexUid": INDEX,
            "status": status,
            "type": ty,
            "canceledBy": null,
            "details": details,
            "error": error,
            "duration": "PT0.001S",
            "enqueuedAt": "2026-01-01T00:00:00Z",
            "startedAt": "2026-01-01T00:00:00Z",
            "finishedAt": "2026-01-01T00:00:01Z"
        })
    }

    fn meili_error(code: &str, ty: &str, message: &str) -> Value {
        json!({
            "message": message,
            "code": code,
            "type": ty,
            "link": format!("https://docs.meilisearch.com/errors#{code}")
        })
    }

    fn index_not_found() -> Value {
        meili_error(
            "index_not_found",
            "invalid_request",
            "Index `documents` not found.",
        )
    }

    fn existing_index() -> Value {
        json!({
            "uid": INDEX,
            "primaryKey": "id",
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z"
        })
    }

    async fn mount_index_creation(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(index_not_found()))
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/indexes"))
            .and(body_json(json!({"uid": INDEX, "primaryKey": "id"})))
            .respond_with(ResponseTemplate::new(202).set_body_json(task_info(1, "indexCreation")))
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                1,
                "succeeded",
                "indexCreation",
                json!({"primaryKey": "id"}),
                None,
            )))
            // The SDK re-fetches a terminal task once more before returning it.
            .expect(1..)
            .mount(server)
            .await;
    }

    /// Accept document additions sent with `?primaryKey=<primary_key>`, or without the
    /// parameter at all when `primary_key` is `None`.
    async fn mount_documents_add(
        server: &MockServer,
        task_uid: u32,
        expected_calls: u64,
        primary_key: Option<&str>,
    ) {
        let mock = Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .and(header("authorization", "Bearer test-key"));
        let mock = match primary_key {
            Some(pk) => mock.and(query_param("primaryKey", pk)),
            None => mock.and(query_param_is_missing("primaryKey")),
        };
        mock.respond_with(
            ResponseTemplate::new(202)
                .set_body_json(task_info(task_uid, "documentAdditionOrUpdate")),
        )
        .expect(expected_calls)
        .mount(server)
        .await;
    }

    async fn run(
        server: &MockServer,
        input: PluginInput,
        cfg: Value,
    ) -> Result<PluginOutput, PluginError> {
        let _ = server;
        MeiliIndexerPlugin::new()
            .execute(&ActivityContext::noop(), input, cfg)
            .await
    }

    #[test]
    fn manifest_is_correct() {
        let m = MeiliIndexerPlugin.manifest();
        assert_eq!(m.name, "meili_indexer");
        assert_eq!(NAME, INDEXER_PLUGIN);
        assert!(m.accepts_kind(InputKind::Documents));
        assert!(m.accepts_kind(InputKind::Many));
        assert_eq!(m.produces, OutputKind::Indexed);
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in [
            "host",
            "api_key",
            "index",
            "primary_key",
            "auto_create_index",
            "batch_size",
            "wait_for_completion",
        ] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
    }

    #[tokio::test]
    async fn creates_index_then_adds_documents_and_waits() {
        let server = MockServer::start().await;
        mount_index_creation(&server).await;
        mount_documents_add(&server, 2, 1, Some("id")).await;
        Mock::given(method("GET"))
            .and(path("/tasks/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                2,
                "succeeded",
                "documentAdditionOrUpdate",
                json!({"receivedDocuments": 2, "indexedDocuments": 2}),
                None,
            )))
            .expect(1..)
            .mount(&server)
            .await;

        let input_docs = docs();
        let out = run(
            &server,
            PluginInput::Documents(input_docs.clone()),
            config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 2,
                task_uids: vec![2],
            })
        );

        // The body sent to Meilisearch is exactly `Document::to_index_json` for every doc.
        let requests = server.received_requests().await.unwrap();
        let add = requests
            .iter()
            .find(|r| r.url.path() == format!("/indexes/{INDEX}/documents"))
            .expect("documents request");
        let sent: Vec<Value> = add.body_json().unwrap();
        let expected: Vec<Value> = input_docs.iter().map(Document::to_index_json).collect();
        assert_eq!(sent, expected);
        assert_eq!(sent[0]["id"], "a");
        assert_eq!(sent[0]["title"], "A");
        assert_eq!(sent[0]["content"], "hello world");
        assert_eq!(sent[0]["price"], 3);
        assert_eq!(sent[0]["_meta"]["page"], 1);
        assert_eq!(add.headers.get("content-type").unwrap(), "application/json");
    }

    #[tokio::test]
    async fn documents_are_sent_before_the_index_creation_is_awaited() {
        // On a busy Meilisearch the creation can sit behind other tasks for minutes.
        // Tasks run in enqueue order, so the additions must be enqueued right away,
        // not after the creation finished.
        let server = MockServer::start().await;
        mount_index_creation(&server).await;
        mount_documents_add(&server, 2, 1, Some("id")).await;
        Mock::given(method("GET"))
            .and(path("/tasks/2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                2,
                "succeeded",
                "documentAdditionOrUpdate",
                json!({"receivedDocuments": 2, "indexedDocuments": 2}),
                None,
            )))
            .mount(&server)
            .await;
        run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap();
        let order: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| format!("{} {}", r.method, r.url.path()))
            .collect();
        let add = order
            .iter()
            .position(|r| r == &format!("POST /indexes/{INDEX}/documents"))
            .expect("documents sent");
        let first_creation_poll = order
            .iter()
            .position(|r| r == "GET /tasks/1")
            .expect("creation checked");
        assert!(
            add < first_creation_poll,
            "documents must not wait for the creation: {order:?}"
        );
    }

    #[test]
    fn measure_matches_the_serialization() {
        for doc in docs() {
            let payload = doc.into_index_json();
            let bytes = serde_json::to_vec(&payload).unwrap();
            let measured = measure(&payload);
            assert_eq!(measured.bytes, bytes.len());
            let mut hash = Fnv::new();
            hash.write(&bytes);
            assert_eq!(measured.hash, hash.0);
        }
        assert_ne!(
            measure(&json!({"id": "a"})).hash,
            measure(&json!({"id": "b"})).hash
        );
    }

    #[test]
    fn task_timeout_prefers_the_step_config() {
        assert_eq!(task_timeout(Some(900)), Duration::from_secs(900));
        // 0 is not a usable budget: fall back rather than time out instantly.
        if std::env::var(TASK_TIMEOUT_ENV).is_err() {
            assert_eq!(task_timeout(Some(0)), DEFAULT_TASK_TIMEOUT);
            assert_eq!(task_timeout(None), DEFAULT_TASK_TIMEOUT);
        }
    }

    /// Mount a missing index whose creation (task 1) ends `failed` with `code`.
    async fn mount_failed_index_creation(server: &MockServer, code: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(index_not_found()))
            .mount(server)
            .await;
        Mock::given(method("POST"))
            .and(path("/indexes"))
            .respond_with(ResponseTemplate::new(202).set_body_json(task_info(1, "indexCreation")))
            .expect(1)
            .mount(server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                1,
                "failed",
                "indexCreation",
                json!({"primaryKey": "id"}),
                Some(meili_error(
                    code,
                    "invalid_request",
                    "index creation failed",
                )),
            )))
            .expect(1..)
            .mount(server)
            .await;
    }

    async fn mount_task(server: &MockServer, uid: u32, status: &str, error: Option<Value>) {
        Mock::given(method("GET"))
            .and(path(format!("/tasks/{uid}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                uid,
                status,
                "documentAdditionOrUpdate",
                json!({"receivedDocuments": 1, "indexedDocuments": 1}),
                error,
            )))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn a_task_that_never_finishes_times_out_as_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(&server)
            .await;
        mount_documents_add(&server, 7, 1, None).await;
        mount_task(&server, 7, "processing", None).await;

        let mut cfg = config(&server.uri());
        cfg["task_timeout_secs"] = json!(1);
        let started = std::time::Instant::now();
        let err = tokio::time::timeout(
            Duration::from_secs(30),
            run(&server, PluginInput::Documents(docs()), cfg),
        )
        .await
        .expect("the wait loop must give up")
        .unwrap_err();
        match err {
            PluginError::Retryable(msg) => {
                assert!(msg.contains("task 7"), "{msg}");
                assert!(msg.contains("did not finish within 1s"), "{msg}");
            }
            other => panic!("expected Retryable, got {other:?}"),
        }
        assert!(started.elapsed() >= Duration::from_secs(1), "gave up early");
    }

    // ----- prune: deleting the stale derived documents of re-indexed parents -----

    /// Chunks `a_0`, `a_1` of parent `a`, chunk `b_0` of parent `b`, and `x`, which has
    /// no parent.
    fn chunked_docs() -> Vec<Document> {
        let chunk = |id: &str, parent: &str| {
            let mut d = Document::with_id(id, "text");
            d.meta.parent_id = Some(parent.into());
            d
        };
        vec![
            chunk("a_0", "a"),
            chunk("a_1", "a"),
            chunk("b_0", "b"),
            Document::with_id("x", "no parent"),
        ]
    }

    fn prune_config(host: &str) -> Value {
        let mut cfg = config(host);
        cfg["prune"] = json!(true);
        cfg
    }

    async fn mount_prune(server: &MockServer, filter: &str, task_uid: u32, calls: u64) {
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents/delete")))
            .and(body_json(json!({ "filter": filter })))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(task_uid, "documentDeletion")),
            )
            .expect(calls)
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn prune_deletes_the_derived_documents_of_each_parent_not_sent_again() {
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        mount_prune(
            &server,
            r#"_meta.parent_id IN ["a", "b"] AND NOT id IN ["a_0", "a_1", "b_0"]"#,
            3,
            1,
        )
        .await;
        mount_task(&server, 3, "succeeded", None).await;
        let out = run(
            &server,
            PluginInput::Documents(chunked_docs()),
            prune_config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(out.document_count(), 4);
        // The deletion is enqueued only once the additions succeeded.
        let order: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| format!("{} {}", r.method, r.url.path()))
            .collect();
        let delete = order
            .iter()
            .position(|r| r.ends_with("/documents/delete"))
            .unwrap();
        let addition_done = order.iter().position(|r| r == "GET /tasks/2").unwrap();
        assert!(addition_done < delete, "{order:?}");
    }

    #[tokio::test]
    async fn prune_skips_a_parent_whose_chunks_did_not_all_reach_the_step() {
        // Parent `a` was cut into 3 chunks but only 2 are here (the input was split
        // across invocations): pruning it would delete the third, fresh chunk. Parent
        // `b` is complete (its one chunk, sent twice) and is pruned.
        let chunk = |id: &str, parent: &str, total: usize| {
            let mut d = Document::with_id(id, "text");
            d.meta.parent_id = Some(parent.into());
            d.meta.chunk_total = Some(total);
            d
        };
        let docs = vec![
            chunk("a_0", "a", 3),
            chunk("a_2", "a", 3),
            chunk("b_0", "b", 1),
            chunk("b_0", "b", 1),
        ];
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        mount_prune(
            &server,
            r#"_meta.parent_id IN ["b"] AND NOT id IN ["b_0", "b_0"]"#,
            3,
            1,
        )
        .await;
        mount_task(&server, 3, "succeeded", None).await;
        run(
            &server,
            PluginInput::Documents(docs),
            prune_config(&server.uri()),
        )
        .await
        .unwrap();
        let deletions = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.url.path().ends_with("/documents/delete"))
            .count();
        assert_eq!(deletions, 1, "only parent b is pruned");

        // Chunks of one parent that disagree on the total are not a complete view
        // either, whatever their count.
        let disagreeing = vec![chunk("c_0", "c", 2), chunk("c_1", "c", 3)];
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents/delete")))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(3, "documentDeletion")),
            )
            .expect(0)
            .mount(&server)
            .await;
        run(
            &server,
            PluginInput::Documents(disagreeing),
            prune_config(&server.uri()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn prune_filters_on_the_index_primary_key_and_splits_large_runs() {
        // The index keys on `uid`: that is the attribute the current documents are
        // kept by.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(index_with_primary_key(json!("uid"))),
            )
            .mount(&server)
            .await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        let input: Vec<Document> = chunked_docs()
            .into_iter()
            .map(|mut d| {
                d.fields.insert("uid".into(), json!(format!("u-{}", d.id)));
                d
            })
            .collect();
        mount_prune(
            &server,
            r#"_meta.parent_id IN ["a", "b"] AND NOT uid IN ["u-a_0", "u-a_1", "u-b_0"]"#,
            3,
            1,
        )
        .await;
        mount_task(&server, 3, "succeeded", None).await;
        run(
            &server,
            PluginInput::Documents(input),
            prune_config(&server.uri()),
        )
        .await
        .unwrap();

        // Filters stay bounded: parents are grouped so a filter lists at most
        // PRUNE_IDS_PER_FILTER current documents (a parent is never split).
        let groups = prune_filters(
            &(0..5)
                .flat_map(|p| (0..3).map(move |c| (format!("p{p}"), json!(format!("p{p}_{c}")))))
                .collect::<Vec<_>>(),
            "id",
            6,
        );
        assert_eq!(
            groups,
            vec![
                r#"_meta.parent_id IN ["p0", "p1"] AND NOT id IN ["p0_0", "p0_1", "p0_2", "p1_0", "p1_1", "p1_2"]"#,
                r#"_meta.parent_id IN ["p2", "p3"] AND NOT id IN ["p2_0", "p2_1", "p2_2", "p3_0", "p3_1", "p3_2"]"#,
                r#"_meta.parent_id IN ["p4"] AND NOT id IN ["p4_0", "p4_1", "p4_2"]"#,
            ]
        );
        // A quote is escaped; a value with a backslash cannot be written safely, so its
        // parent is left alone rather than risk deleting the wrong documents.
        let groups = prune_filters(
            &[
                ("q\"uote".to_owned(), json!("q_0")),
                ("back\\slash".to_owned(), json!("s_0")),
                ("n".to_owned(), json!(7)),
            ],
            "id",
            100,
        );
        assert_eq!(
            groups,
            vec![r#"_meta.parent_id IN ["q\"uote", "n"] AND NOT id IN ["q_0", 7]"#]
        );
    }

    #[tokio::test]
    async fn prune_is_off_by_default_and_has_nothing_to_do_without_parents() {
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 2, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents/delete")))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(3, "documentDeletion")),
            )
            .expect(0)
            .mount(&server)
            .await;
        run(
            &server,
            PluginInput::Documents(chunked_docs()),
            config(&server.uri()),
        )
        .await
        .unwrap();
        run(
            &server,
            PluginInput::Documents(docs()),
            prune_config(&server.uri()),
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn prune_never_runs_after_a_failed_addition() {
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(
            &server,
            2,
            "failed",
            Some(meili_error(
                "invalid_document_id",
                "invalid_request",
                "Document identifier is invalid.",
            )),
        )
        .await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents/delete")))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(3, "documentDeletion")),
            )
            .expect(0)
            .mount(&server)
            .await;
        let err = run(
            &server,
            PluginInput::Documents(chunked_docs()),
            prune_config(&server.uri()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err}");
    }

    #[tokio::test]
    async fn prune_fails_clearly_without_a_filterable_parent_or_delete_permission() {
        // The attribute is not filterable: the deletion task fails.
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents/delete")))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(3, "documentDeletion")),
            )
            .mount(&server)
            .await;
        mount_task(
            &server,
            3,
            "failed",
            Some(meili_error(
                "invalid_document_filter",
                "invalid_request",
                "Attribute `_meta.parent_id` is not filterable.",
            )),
        )
        .await;
        match run(
            &server,
            PluginInput::Documents(chunked_docs()),
            prune_config(&server.uri()),
        )
        .await
        {
            Err(PluginError::NonRetryable(msg)) => {
                assert!(msg.contains("is not filterable"), "{msg}");
                assert!(msg.contains("filterableAttributes"), "{msg}");
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }

        // The key may add documents but not delete them.
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_documents_add(&server, 2, 1, None).await;
        mount_task(&server, 2, "succeeded", None).await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents/delete")))
            .respond_with(ResponseTemplate::new(403).set_body_json(meili_error(
                "invalid_api_key",
                "auth",
                "The provided API key is invalid.",
            )))
            .mount(&server)
            .await;
        match run(
            &server,
            PluginInput::Documents(chunked_docs()),
            prune_config(&server.uri()),
        )
        .await
        {
            Err(PluginError::NonRetryable(msg)) => {
                assert!(msg.contains("documents.delete"), "{msg}")
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    #[test]
    fn prune_needs_wait_for_completion() {
        let Err(PluginError::InvalidConfig(msg)) = parse_config(json!({
            "host": "http://m", "api_key": "k", "index": "i",
            "prune": true, "wait_for_completion": false
        })) else {
            panic!("prune without waiting must be rejected");
        };
        assert!(msg.contains("wait_for_completion"), "{msg}");
    }

    // ----- resuming after a retry (checkpointed task uids) -----

    async fn mount_existing_index(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(server)
            .await;
    }

    /// Accept `n` document additions, answering them with task uids `first..first+n`.
    async fn mount_numbered_additions(server: &MockServer, first: u32, n: u32) {
        for uid in first..first + n {
            Mock::given(method("POST"))
                .and(path(format!("/indexes/{INDEX}/documents")))
                .respond_with(
                    ResponseTemplate::new(202)
                        .set_body_json(task_info(uid, "documentAdditionOrUpdate")),
                )
                .up_to_n_times(1)
                .expect(1)
                .mount(server)
                .await;
        }
    }

    /// The ids of the documents in every addition the server received, per request.
    async fn sent_batches(server: &MockServer) -> Vec<Vec<String>> {
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST" && r.url.path().ends_with("/documents"))
            .map(|r| {
                r.body_json::<Vec<Value>>()
                    .unwrap()
                    .iter()
                    .map(|d| d["id"].as_str().unwrap().to_owned())
                    .collect()
            })
            .collect()
    }

    fn three_docs() -> Vec<Document> {
        let mut input = docs();
        input.push(Document::with_id("c", "third"));
        input
    }

    async fn run_attempt(
        attempt: u32,
        previous: Option<Value>,
        input: Vec<Document>,
        cfg: Value,
    ) -> (Result<PluginOutput, PluginError>, Option<Value>) {
        let ctx = ActivityContext::detached(ActivityContext::noop().job_id(), "index", attempt)
            .with_previous_progress(previous);
        let out = MeiliIndexerPlugin::new()
            .execute(&ctx, PluginInput::Documents(input), cfg)
            .await;
        (out, ctx.progress())
    }

    #[tokio::test]
    async fn a_retry_after_a_wait_timeout_waits_on_the_enqueued_tasks_instead_of_resending() {
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 10, 3).await;
        // Meilisearch is busy: the first task never leaves the queue during attempt 1.
        for uid in [10, 11, 12] {
            mount_task(&server, uid, "enqueued", None).await;
        }
        let mut cfg = config(&server.uri());
        cfg["batch_size"] = json!(1);
        cfg["task_timeout_secs"] = json!(1);
        let (out, progress) = run_attempt(1, None, three_docs(), cfg.clone()).await;
        assert!(matches!(out, Err(PluginError::Retryable(_))), "{out:?}");
        assert_eq!(sent_batches(&server).await.len(), 3);
        let progress = progress.expect("the enqueued tasks are checkpointed");

        // Attempt 2: the tasks of attempt 1 have finished meanwhile. Nothing is sent
        // again; the step waits on those tasks and reports them.
        server.reset().await;
        mount_existing_index(&server).await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(99, "documentAdditionOrUpdate")),
            )
            .expect(0)
            .mount(&server)
            .await;
        for uid in [10, 11, 12] {
            mount_task(&server, uid, "succeeded", None).await;
        }
        let (out, _) = run_attempt(2, Some(progress), three_docs(), cfg).await;
        assert_eq!(
            out.unwrap(),
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 3,
                task_uids: vec![10, 11, 12],
            })
        );
    }

    #[tokio::test]
    async fn a_retry_after_a_partial_send_only_sends_the_batches_not_yet_enqueued() {
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 10, 1).await;
        // Batch 2 hits a 5xx: retryable, batch 1 is already in the queue.
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .respond_with(ResponseTemplate::new(503).set_body_string("unavailable"))
            .mount(&server)
            .await;
        let mut cfg = config(&server.uri());
        cfg["batch_size"] = json!(1);
        let (out, progress) = run_attempt(1, None, three_docs(), cfg.clone()).await;
        assert!(matches!(out, Err(PluginError::Retryable(_))), "{out:?}");
        assert_eq!(sent_batches(&server).await, vec![vec!["a"], vec!["b"]]);

        server.reset().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 20, 2).await;
        for uid in [10, 20, 21] {
            mount_task(&server, uid, "succeeded", None).await;
        }
        let (out, _) = run_attempt(2, progress, three_docs(), cfg).await;
        assert_eq!(
            out.unwrap(),
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 3,
                task_uids: vec![10, 20, 21],
            })
        );
        assert_eq!(sent_batches(&server).await, vec![vec!["b"], vec!["c"]]);
    }

    #[tokio::test]
    async fn a_checkpoint_that_cannot_be_trusted_falls_back_to_resending() {
        // First attempt: everything enqueued, nothing finished in time.
        let server = MockServer::start().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 10, 3).await;
        for uid in [10, 11, 12] {
            mount_task(&server, uid, "enqueued", None).await;
        }
        let mut cfg = config(&server.uri());
        cfg["batch_size"] = json!(1);
        cfg["task_timeout_secs"] = json!(1);
        let (_, progress) = run_attempt(1, None, three_docs(), cfg.clone()).await;
        let progress = progress.expect("checkpoint");

        // Task 11 is gone (pruned, or the instance was reset): it and every later batch
        // are sent again, so the additions keep their order.
        server.reset().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 30, 2).await;
        mount_task(&server, 10, "succeeded", None).await;
        Mock::given(method("GET"))
            .and(path("/tasks/11"))
            .respond_with(ResponseTemplate::new(404).set_body_json(meili_error(
                "task_not_found",
                "invalid_request",
                "Task `11` not found.",
            )))
            .mount(&server)
            .await;
        for uid in [30, 31] {
            mount_task(&server, uid, "succeeded", None).await;
        }
        let (out, _) = run_attempt(2, Some(progress.clone()), three_docs(), cfg.clone()).await;
        assert_eq!(out.unwrap().document_count(), 3);
        assert_eq!(sent_batches(&server).await, vec![vec!["b"], vec!["c"]]);

        // Different input than the checkpoint describes: everything is sent.
        server.reset().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 40, 2).await;
        for uid in [40, 41] {
            mount_task(&server, uid, "succeeded", None).await;
        }
        let (out, _) = run_attempt(2, Some(progress.clone()), docs(), cfg.clone()).await;
        assert_eq!(out.unwrap().document_count(), 2);
        assert_eq!(sent_batches(&server).await, vec![vec!["a"], vec!["b"]]);

        // Another index on the same host: everything is sent.
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/indexes/other"))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/indexes/other/documents"))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(50, "documentAdditionOrUpdate")),
            )
            .expect(3)
            .mount(&server)
            .await;
        mount_task(&server, 50, "succeeded", None).await;
        let mut other = cfg.clone();
        other["index"] = json!("other");
        let (out, _) = run_attempt(2, Some(progress), three_docs(), other).await;
        assert_eq!(out.unwrap().document_count(), 3);

        // A checkpoint this plugin does not understand is ignored.
        server.reset().await;
        mount_existing_index(&server).await;
        mount_numbered_additions(&server, 60, 3).await;
        for uid in [60, 61, 62] {
            mount_task(&server, uid, "succeeded", None).await;
        }
        let (out, _) = run_attempt(2, Some(json!({"something": "else"})), three_docs(), cfg).await;
        assert_eq!(out.unwrap().document_count(), 3);
        assert_eq!(sent_batches(&server).await.len(), 3);
    }

    #[tokio::test]
    async fn index_already_exists_on_creation_is_not_an_error() {
        let server = MockServer::start().await;
        mount_failed_index_creation(&server, "index_already_exists").await;
        mount_documents_add(&server, 2, 1, Some("id")).await;
        mount_task(&server, 2, "succeeded", None).await;
        let out = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 2,
                task_uids: vec![2],
            })
        );

        // Same with no documents to add.
        let server = MockServer::start().await;
        mount_failed_index_creation(&server, "index_already_exists").await;
        let out = run(
            &server,
            PluginInput::Documents(vec![]),
            config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 0,
                task_uids: vec![],
            })
        );
    }

    #[tokio::test]
    async fn any_other_index_creation_failure_is_non_retryable() {
        let server = MockServer::start().await;
        mount_failed_index_creation(&server, "invalid_index_uid").await;
        mount_documents_add(&server, 2, 1, Some("id")).await;
        mount_task(&server, 2, "succeeded", None).await;
        let err = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        match err {
            PluginError::NonRetryable(msg) => {
                assert!(msg.contains("task 1 (index creation)"), "{msg}")
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }

        // Same with no documents to add.
        let server = MockServer::start().await;
        mount_failed_index_creation(&server, "invalid_index_uid").await;
        let err = run(
            &server,
            PluginInput::Documents(vec![]),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        match err {
            PluginError::NonRetryable(msg) => {
                assert!(msg.contains("task 1 (index creation)"), "{msg}")
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_failed_later_batch_fails_the_step_naming_its_task() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(&server)
            .await;
        // One batch per document; each addition gets its own task uid, in order.
        for uid in [10, 11, 12] {
            Mock::given(method("POST"))
                .and(path(format!("/indexes/{INDEX}/documents")))
                .respond_with(
                    ResponseTemplate::new(202)
                        .set_body_json(task_info(uid, "documentAdditionOrUpdate")),
                )
                .up_to_n_times(1)
                .expect(1)
                .mount(&server)
                .await;
        }
        mount_task(&server, 10, "succeeded", None).await;
        mount_task(
            &server,
            11,
            "failed",
            Some(meili_error(
                "invalid_document_id",
                "invalid_request",
                "Document identifier is invalid.",
            )),
        )
        .await;
        mount_task(&server, 12, "succeeded", None).await;

        let mut input = docs();
        input.push(Document::with_id("c", "third"));
        let mut cfg = config(&server.uri());
        cfg["batch_size"] = json!(1);
        let err = run(&server, PluginInput::Documents(input), cfg)
            .await
            .unwrap_err();
        match err {
            PluginError::NonRetryable(msg) => {
                assert!(msg.contains("task 11 (document addition)"), "{msg}");
                assert!(msg.contains("Document identifier is invalid."), "{msg}");
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn failed_task_is_non_retryable_with_meilisearch_message() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(&server)
            .await;
        mount_documents_add(&server, 7, 1, None).await;
        Mock::given(method("GET"))
            .and(path("/tasks/7"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                7,
                "failed",
                "documentAdditionOrUpdate",
                json!({"receivedDocuments": 2, "indexedDocuments": 0}),
                Some(meili_error(
                    "missing_document_id",
                    "invalid_request",
                    "Document doesn't have a `id` attribute.",
                )),
            )))
            .mount(&server)
            .await;

        let err = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        match err {
            PluginError::NonRetryable(msg) => {
                assert!(msg.contains("task 7"), "{msg}");
                assert!(
                    msg.contains("Document doesn't have a `id` attribute."),
                    "{msg}"
                );
                assert!(msg.contains("MissingDocumentId"), "{msg}");
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn missing_context_or_index_is_invalid_config() {
        let server = MockServer::start().await;
        let err = run(
            &server,
            PluginInput::Documents(docs()),
            json!({"api_key": "k", "index": INDEX}),
        )
        .await
        .unwrap_err();
        match err {
            PluginError::InvalidConfig(msg) => assert!(msg.contains("MeiliContext"), "{msg}"),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }

        let err = run(
            &server,
            PluginInput::Documents(docs()),
            json!({"host": "http://x", "api_key": ""}),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");

        let err = run(
            &server,
            PluginInput::Documents(docs()),
            json!({"host": "http://x", "api_key": "k"}),
        )
        .await
        .unwrap_err();
        match err {
            PluginError::InvalidConfig(msg) => assert!(msg.contains("index"), "{msg}"),
            other => panic!("expected InvalidConfig, got {other:?}"),
        }

        let mut cfg = config(&server.uri());
        cfg["batch_size"] = json!(0);
        let err = run(&server, PluginInput::Documents(docs()), cfg)
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");

        // Nothing was sent to Meilisearch.
        assert!(server.received_requests().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn empty_input_only_ensures_the_index() {
        let server = MockServer::start().await;
        mount_index_creation(&server).await;
        mount_documents_add(&server, 2, 0, Some("id")).await;

        let out = run(
            &server,
            PluginInput::Documents(vec![]),
            config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 0,
                task_uids: vec![],
            })
        );

        // `Empty` input (e.g. after a step that produced nothing) behaves the same.
        let server2 = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .expect(1)
            .mount(&server2)
            .await;
        let out = run(&server2, PluginInput::Empty, config(&server2.uri()))
            .await
            .unwrap();
        assert_eq!(out.document_count(), 0);
    }

    #[tokio::test]
    async fn batches_documents_and_skips_waiting_when_disabled() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .expect(1)
            .mount(&server)
            .await;
        mount_documents_add(&server, 9, 3, None).await;
        Mock::given(method("GET"))
            .and(path("/tasks/9"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                9,
                "succeeded",
                "documentAdditionOrUpdate",
                Value::Null,
                None,
            )))
            .expect(0)
            .mount(&server)
            .await;

        let many: Vec<Document> = (0..5)
            .map(|i| Document::with_id(format!("d{i}"), "x"))
            .collect();
        let input = PluginInput::Many(vec![
            PluginOutput::Documents(many[..3].to_vec()),
            PluginOutput::Documents(many[3..].to_vec()),
        ]);
        let mut cfg = config(&server.uri());
        cfg["batch_size"] = json!(2);
        cfg["wait_for_completion"] = json!(false);
        let out = run(&server, input, cfg).await.unwrap();
        assert_eq!(
            out,
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 5,
                task_uids: vec![9, 9, 9],
            })
        );
        let sizes: Vec<usize> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .filter(|r| r.method == "POST")
            .map(|r| r.body_json::<Vec<Value>>().unwrap().len())
            .collect();
        assert_eq!(sizes, vec![2, 2, 1]);
    }

    #[tokio::test]
    async fn auto_create_index_false_fails_on_a_missing_index_without_sending_anything() {
        // Sending the additions anyway would make Meilisearch create the index
        // implicitly (with a key that may create indexes), which is what the flag is
        // there to prevent: a typo in the uid must fail the step, not add an index.
        for input in [PluginInput::Documents(docs()), PluginInput::Empty] {
            let server = MockServer::start().await;
            Mock::given(method("GET"))
                .and(path(format!("/indexes/{INDEX}")))
                .respond_with(ResponseTemplate::new(404).set_body_json(index_not_found()))
                .expect(1)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path("/indexes"))
                .respond_with(
                    ResponseTemplate::new(202).set_body_json(task_info(1, "indexCreation")),
                )
                .expect(0)
                .mount(&server)
                .await;
            Mock::given(method("POST"))
                .and(path(format!("/indexes/{INDEX}/documents")))
                .respond_with(
                    ResponseTemplate::new(202)
                        .set_body_json(task_info(3, "documentAdditionOrUpdate")),
                )
                .expect(0)
                .mount(&server)
                .await;
            let mut cfg = config(&server.uri());
            cfg["auto_create_index"] = json!(false);
            cfg["wait_for_completion"] = json!(false);
            match run(&server, input, cfg).await {
                Err(PluginError::NonRetryable(msg)) => {
                    assert!(msg.contains(&format!("{INDEX:?}")), "{msg}");
                    assert!(msg.contains("auto_create_index"), "{msg}");
                }
                other => panic!("expected NonRetryable, got {other:?}"),
            }
        }
    }

    fn index_with_primary_key(primary_key: Value) -> Value {
        json!({
            "uid": INDEX,
            "primaryKey": primary_key,
            "createdAt": "2026-01-01T00:00:00Z",
            "updatedAt": "2026-01-01T00:00:00Z"
        })
    }

    async fn mount_succeeded_addition(server: &MockServer, task_uid: u32) {
        Mock::given(method("GET"))
            .and(path(format!("/tasks/{task_uid}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                task_uid,
                "succeeded",
                "documentAdditionOrUpdate",
                json!({"receivedDocuments": 2, "indexedDocuments": 2}),
                None,
            )))
            .mount(server)
            .await;
    }

    /// Documents as a Scrapix crawl writes them: the index key is `uid`, not `id`.
    fn docs_with_uid() -> Vec<Document> {
        docs()
            .into_iter()
            .map(|mut d| {
                d.fields.insert("uid".into(), json!(format!("u-{}", d.id)));
                d
            })
            .collect()
    }

    #[tokio::test]
    async fn an_existing_index_keeps_its_own_primary_key() {
        // An index created elsewhere (a Scrapix crawl) with `uid` as its key. Sending
        // `?primaryKey=id` makes Meilisearch reject every addition with
        // `index_primary_key_already_exists`, so the parameter must be left out.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(index_with_primary_key(json!("uid"))),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path("/indexes"))
            .respond_with(ResponseTemplate::new(202).set_body_json(task_info(1, "indexCreation")))
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .and(query_param("primaryKey", "id"))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(8, "documentAdditionOrUpdate")),
            )
            .expect(0)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/tasks/8"))
            .respond_with(ResponseTemplate::new(200).set_body_json(task(
                8,
                "failed",
                "documentAdditionOrUpdate",
                json!({"receivedDocuments": 2, "indexedDocuments": 0}),
                Some(meili_error(
                    "index_primary_key_already_exists",
                    "invalid_request",
                    "Index already has a primary key: `uid`.",
                )),
            )))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .and(query_param_is_missing("primaryKey"))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(2, "documentAdditionOrUpdate")),
            )
            .expect(1)
            .mount(&server)
            .await;
        mount_succeeded_addition(&server, 2).await;

        let input_docs = docs_with_uid();
        let out = run(
            &server,
            PluginInput::Documents(input_docs.clone()),
            config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(
            out,
            PluginOutput::Indexed(IndexReport {
                index: INDEX.into(),
                document_count: 2,
                task_uids: vec![2],
            })
        );
        // The documents themselves are unchanged.
        let requests = server.received_requests().await.unwrap();
        let add = requests
            .iter()
            .find(|r| r.method == "POST")
            .expect("documents request");
        let sent: Vec<Value> = add.body_json().unwrap();
        let expected: Vec<Value> = input_docs.iter().map(Document::to_index_json).collect();
        assert_eq!(sent, expected);
        assert_eq!(sent[0]["uid"], "u-a");
    }

    #[tokio::test]
    async fn an_existing_index_without_a_primary_key_gets_the_configured_one() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(index_with_primary_key(Value::Null)),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .and(query_param_is_missing("primaryKey"))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(5, "documentAdditionOrUpdate")),
            )
            .expect(0)
            .mount(&server)
            .await;
        mount_documents_add(&server, 2, 1, Some("id")).await;
        mount_succeeded_addition(&server, 2).await;

        let out = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap();
        assert_eq!(out.document_count(), 2);
    }

    #[tokio::test]
    async fn auto_create_index_false_still_respects_an_existing_primary_key() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(index_with_primary_key(json!("uid"))),
            )
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .and(query_param_is_missing("primaryKey"))
            .respond_with(
                ResponseTemplate::new(202).set_body_json(task_info(3, "documentAdditionOrUpdate")),
            )
            .expect(1)
            .mount(&server)
            .await;
        let mut cfg = config(&server.uri());
        cfg["auto_create_index"] = json!(false);
        cfg["wait_for_completion"] = json!(false);
        let out = run(&server, PluginInput::Documents(docs_with_uid()), cfg)
            .await
            .unwrap();
        assert_eq!(out.document_count(), 2);
    }

    #[tokio::test]
    async fn auth_errors_are_non_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(403).set_body_json(meili_error(
                "invalid_api_key",
                "auth",
                "The provided API key is invalid.",
            )))
            .mount(&server)
            .await;
        let err = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        match err {
            PluginError::NonRetryable(msg) => {
                assert!(msg.contains("The provided API key is invalid."), "{msg}");
                assert!(msg.contains("API key"), "{msg}");
            }
            other => panic!("expected NonRetryable, got {other:?}"),
        }

        // A bare 401 without a Meilisearch error body is also non-retryable.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(401))
            .mount(&server)
            .await;
        let err = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err}");
    }

    #[tokio::test]
    async fn network_and_server_errors_are_retryable() {
        // Nothing listens on port 1: connection refused.
        let err = MeiliIndexerPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(docs()),
                config("http://127.0.0.1:1"),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Retryable(_)), "{err}");

        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .respond_with(ResponseTemplate::new(503).set_body_string("upstream unavailable"))
            .mount(&server)
            .await;
        let err = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::Retryable(_)), "{err}");

        // A Meilisearch `internal` error is retryable too.
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(500).set_body_json(meili_error(
                "internal",
                "internal",
                "An internal error has occurred.",
            )))
            .mount(&server)
            .await;
        let err = run(
            &server,
            PluginInput::Documents(docs()),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::Retryable(_)), "{err}");
    }

    #[tokio::test]
    async fn rejects_bytes_input_and_honours_cancellation() {
        let server = MockServer::start().await;
        let err = run(
            &server,
            PluginInput::Bytes(Blob::new(vec![1], "application/pdf", None)),
            config(&server.uri()),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");

        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = MeiliIndexerPlugin::new()
            .execute(&ctx, PluginInput::Documents(docs()), config(&server.uri()))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled), "{err}");
        // No documents were sent.
        assert!(
            server
                .received_requests()
                .await
                .unwrap()
                .iter()
                .all(|r| r.method != "POST")
        );
    }

    #[test]
    fn injected_context_fields_are_not_author_editable() {
        // A pipeline editor generates its form from this schema. host/api_key/index
        // come from the tenant context at runtime, so they must not be required (the
        // author cannot know them) and must be flagged readOnly so no editor renders
        // an input for a secret.
        let m = MeiliIndexerPlugin::new().manifest();
        let required = m.config_schema["required"]
            .as_array()
            .expect("required array");
        assert!(
            required.is_empty(),
            "no config key should be author-required: {required:?}"
        );
        let props = m.config_schema["properties"]
            .as_object()
            .expect("properties");
        // Destination credentials and tenant tags are injected, never typed.
        for key in ["host", "api_key", "tenant_id", "region"] {
            assert_eq!(
                props[key]["readOnly"],
                serde_json::json!(true),
                "{key} must be marked readOnly"
            );
        }
        // Author-controlled knobs stay editable, including the connection that pins the
        // destination and the index that may be pinned next to it.
        for key in [
            "connection",
            "index",
            "primary_key",
            "batch_size",
            "max_batch_bytes",
        ] {
            assert!(
                props[key].get("readOnly").is_none(),
                "{key} should remain editable"
            );
        }
    }

    // ----- batch planning (max_batch_bytes) -----

    #[test]
    fn plan_batches_splits_on_count() {
        let got = plan_batches(&[10; 5], 2, u64::MAX).expect("fits");
        assert_eq!(got, vec![0..2, 2..4, 4..5]);
    }

    #[test]
    fn plan_batches_splits_on_bytes() {
        // Each batch is "[" + docs joined by "," + "]". Two 10-byte docs = 2 + 10 + 1 + 10
        // = 23 bytes, so a 23-byte cap fits exactly two and a 22-byte cap only one.
        assert_eq!(
            plan_batches(&[10; 4], 1000, 23).expect("fits"),
            vec![0..2, 2..4]
        );
        assert_eq!(
            plan_batches(&[10; 3], 1000, 22).expect("fits"),
            vec![0..1, 1..2, 2..3]
        );
    }

    #[test]
    fn plan_batches_cuts_at_whichever_limit_comes_first() {
        // Bytes bind first: a 25-byte cap fits two 10-byte docs, count would allow 3.
        assert_eq!(
            plan_batches(&[10; 4], 3, 25).expect("fits"),
            vec![0..2, 2..4]
        );
        // Count binds first: bytes would allow many, count caps at 2.
        assert_eq!(
            plan_batches(&[1; 5], 2, 10_000).expect("fits"),
            vec![0..2, 2..4, 4..5]
        );
    }

    #[test]
    fn plan_batches_rejects_a_document_that_fits_no_batch() {
        let err = plan_batches(&[10, 500, 10], 1000, 100).expect_err("oversized");
        assert_eq!(
            err,
            OversizedDoc {
                index: 1,
                bytes: 500
            }
        );
    }

    #[test]
    fn plan_batches_of_nothing_is_no_batches() {
        assert!(plan_batches(&[], 1000, 100).expect("fits").is_empty());
    }

    #[test]
    fn plan_batches_covers_every_document_exactly_once() {
        let sizes: Vec<usize> = (1..=97).map(|i| (i * 37) % 200 + 1).collect();
        let batches = plan_batches(&sizes, 7, 600).expect("fits");
        let mut next = 0;
        for r in &batches {
            assert_eq!(r.start, next, "batches are contiguous");
            assert!(!r.is_empty(), "no empty batch");
            assert!(r.len() <= 7, "count cap respected");
            let bytes = 2 + r.len() - 1 + sizes[r.clone()].iter().sum::<usize>();
            assert!(bytes <= 600, "byte cap respected: {bytes}");
            next = r.end;
        }
        assert_eq!(next, sizes.len(), "every document is sent");
    }

    #[test]
    fn max_batch_bytes_defaults_and_rejects_zero() {
        let cfg: IndexerConfig = serde_json::from_value(serde_json::json!({
            "host": "http://m", "api_key": "k", "index": "i"
        }))
        .expect("config");
        assert_eq!(cfg.max_batch_bytes, DEFAULT_MAX_BATCH_BYTES);

        let zero = serde_json::json!({
            "host": "http://m", "api_key": "k", "index": "i", "max_batch_bytes": 0
        });
        assert!(parse_config(zero).is_err());
    }

    #[test]
    fn an_unresolved_connection_is_named_in_the_error() {
        // The worker activity resolves `connection` into host/api_key before the plugin
        // runs; if that did not happen, the error must say which connection, not just
        // "no host".
        let Err(PluginError::InvalidConfig(msg)) = parse_config(serde_json::json!({
            "connection": "prod-movies", "index": "movies"
        })) else {
            panic!("an unresolved connection must be rejected");
        };
        assert!(msg.contains("prod-movies"), "{msg}");
        assert!(msg.contains("not resolved"), "{msg}");
    }

    #[test]
    fn a_resolved_connection_parses_to_its_host_and_key() {
        let parsed = parse_config(serde_json::json!({
            "connection": "prod-movies",
            "host": " https://movies.example ",
            "api_key": "k",
            "index": "movies"
        }))
        .unwrap_or_else(|e| panic!("resolved config must parse: {e}"));
        assert_eq!(parsed.host, "https://movies.example", "trimmed");
        assert_eq!(parsed.api_key, "k");
        assert_eq!(parsed.cfg.connection.as_deref(), Some("prod-movies"));
    }
}
