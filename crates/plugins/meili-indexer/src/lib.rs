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
//! 2. when `auto_create_index` (default `true`), enqueue the index creation with
//!    `primary_key` if `GET /indexes/{uid}` says it does not exist — without waiting:
//!    Meilisearch runs tasks in enqueue order, so the document additions sent next
//!    always run after it, and waiting would only double the time spent behind other
//!    tenants' tasks on a busy instance. Its outcome is checked once the documents are
//!    done
//!    for that task;
//! 3. flatten every [`Document`] with [`Document::to_index_json`] and send it via
//!    `addOrReplace` in batches cut at whichever of `batch_size` (documents, default
//!    1000) and `max_batch_bytes` (serialized bytes, default 50 MiB) comes first. A
//!    document too large for any batch is a [`PluginError::NonRetryable`] naming it;
//!    sending it would earn a `413` the retry policy would repeat forever;
//! 4. when `wait_for_completion` (default `true`), poll each task until it
//!    finishes; a `failed` task is a [`PluginError::NonRetryable`] carrying the
//!    Meilisearch error message;
//! 5. return [`PluginOutput::Indexed`] with the [`IndexReport`].
//!
//! Network failures, timeouts and 5xx responses are [`PluginError::Retryable`];
//! authentication problems (401/403, `auth` error type) and invalid requests are
//! [`PluginError::NonRetryable`].

use std::time::Duration;

use meili_ingest_plugin_sdk::IndexerConfig;
use meili_ingest_plugin_sdk::prelude::*;
use meilisearch_sdk::client::Client;
use meilisearch_sdk::errors::{Error as MeiliError, ErrorCode, ErrorType, MeilisearchError};
use meilisearch_sdk::task_info::TaskInfo;
use meilisearch_sdk::tasks::Task;
use serde_json::Value;

/// Plugin name referenced by `steps[].plugin`. Equals [`INDEXER_PLUGIN`].
pub const NAME: &str = INDEXER_PLUGIN;

/// How often Meilisearch is polled while waiting for a task.
const POLL_INTERVAL: Duration = Duration::from_millis(200);
/// Waiting is done in slices so a heartbeat is emitted between them.
const WAIT_SLICE: Duration = Duration::from_secs(10);
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
    task: &TaskInfo,
    what: &str,
    timeout: Duration,
) -> Result<Option<MeilisearchError>, PluginError> {
    let uid = task.task_uid;
    let mut waited = Duration::ZERO;
    loop {
        ctx.check_cancelled()?;
        ctx.heartbeat(format!("{NAME}: waiting for task {uid} ({what})"));
        match client
            .wait_for_task(task, Some(POLL_INTERVAL), Some(WAIT_SLICE))
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

/// Enqueue the index creation (with `primary_key`) when it does not exist yet, without
/// waiting for it. Returns the creation task, if one was enqueued.
async fn ensure_index(
    client: &Client,
    uid: &str,
    primary_key: &str,
) -> Result<Option<TaskInfo>, PluginError> {
    match client.get_index(uid).await {
        Ok(_) => Ok(None),
        Err(e) if is_index_not_found(&e) => {
            tracing::info!(plugin = NAME, index = %uid, primary_key = %primary_key, "creating index");
            client
                .create_index(uid, Some(primary_key))
                .await
                .map(Some)
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
    match wait_for_task(client, ctx, task, "index creation", timeout).await? {
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
                // `host`, `api_key`, `project_id` and `region` are NOT listed as
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
                    "project_id": {
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
                        "description": "Primary key declared when creating the index and sent as ?primaryKey on document additions."
                    },
                    "auto_create_index": {
                        "type": "boolean",
                        "default": true,
                        "description": "Create the index when GET /indexes/{uid} returns 404."
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
        let creation = if cfg.auto_create_index {
            ensure_index(&client, &index_uid, &cfg.primary_key).await?
        } else {
            None
        };

        if docs.is_empty() {
            if let Some(task) = &creation
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
        let payloads: Vec<Value> = docs.iter().map(Document::to_index_json).collect();
        let sizes: Vec<usize> = payloads
            .iter()
            .map(|p| serde_json::to_vec(p).map(|b| b.len()).unwrap_or(usize::MAX))
            .collect();
        let batches = plan_batches(&sizes, cfg.batch_size, cfg.max_batch_bytes).map_err(
            |OversizedDoc { index, bytes }| {
                PluginError::NonRetryable(format!(
                    "{NAME}: document {:?} is {bytes} bytes serialized, over \
                     `max_batch_bytes` ({}); it cannot be sent in any batch. Raise \
                     `max_batch_bytes` or chunk the document upstream",
                    docs[index].id, cfg.max_batch_bytes
                ))
            },
        )?;
        let batch_total = batches.len();
        let mut tasks: Vec<TaskInfo> = Vec::with_capacity(batch_total);
        for (batch_no, range) in batches.into_iter().enumerate() {
            ctx.check_cancelled()?;
            let payload = &payloads[range];
            ctx.heartbeat(format!(
                "{NAME}: sending batch {}/{batch_total} ({} documents)",
                batch_no + 1,
                payload.len()
            ));
            let info = index
                .add_or_replace(payload, Some(&cfg.primary_key))
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
            tasks.push(info);
        }

        if cfg.wait_for_completion {
            for info in &tasks {
                if let Some(err) =
                    wait_for_task(&client, ctx, info, "document addition", timeout).await?
                {
                    return Err(task_failed(info.task_uid, "document addition", &err));
                }
            }
            // Enqueued before the additions, so it finished before them: no extra wait.
            if let Some(task) = &creation {
                check_creation(&client, ctx, task, timeout).await?;
            }
        }

        Ok(PluginOutput::Indexed(IndexReport {
            index: index_uid,
            document_count: docs.len(),
            task_uids: tasks.iter().map(|t| t.task_uid).collect(),
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_json, header, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const INDEX: &str = "documents";

    fn config(host: &str) -> Value {
        json!({
            "host": host,
            "api_key": "test-key",
            "index": INDEX,
            "project_id": "proj-1",
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

    async fn mount_documents_add(server: &MockServer, task_uid: u32, expected_calls: u64) {
        Mock::given(method("POST"))
            .and(path(format!("/indexes/{INDEX}/documents")))
            .and(query_param("primaryKey", "id"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(
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
        mount_documents_add(&server, 2, 1).await;
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
        mount_documents_add(&server, 2, 1).await;
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
    fn task_timeout_prefers_the_step_config() {
        assert_eq!(task_timeout(Some(900)), Duration::from_secs(900));
        // 0 is not a usable budget: fall back rather than time out instantly.
        if std::env::var(TASK_TIMEOUT_ENV).is_err() {
            assert_eq!(task_timeout(Some(0)), DEFAULT_TASK_TIMEOUT);
            assert_eq!(task_timeout(None), DEFAULT_TASK_TIMEOUT);
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
        mount_documents_add(&server, 7, 1).await;
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
        mount_documents_add(&server, 2, 0).await;

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
        mount_documents_add(&server, 9, 3).await;
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
    async fn auto_create_index_false_skips_the_lookup() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/indexes/{INDEX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(existing_index()))
            .expect(0)
            .mount(&server)
            .await;
        mount_documents_add(&server, 3, 1).await;
        let mut cfg = config(&server.uri());
        cfg["auto_create_index"] = json!(false);
        cfg["wait_for_completion"] = json!(false);
        let out = run(&server, PluginInput::Documents(docs()), cfg)
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
        for key in ["host", "api_key", "project_id", "region"] {
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
