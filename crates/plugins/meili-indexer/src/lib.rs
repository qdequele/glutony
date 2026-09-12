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
//! 2. when `auto_create_index` (default `true`), create the index with
//!    `primary_key` if `GET /indexes/{uid}` says it does not exist, and wait
//!    for that task;
//! 3. flatten every [`Document`] with [`Document::to_index_json`] and send it
//!    in batches of `batch_size` (default 1000) via `addOrReplace`;
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
/// Total time to wait for one task before giving up (retryable).
const TASK_TIMEOUT: Duration = Duration::from_secs(120);

/// The Meilisearch indexer plugin. Stateless; construct with [`MeiliIndexerPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct MeiliIndexerPlugin;

impl MeiliIndexerPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Validate and deserialize the step config into an [`IndexerConfig`].
fn parse_config(config: Value) -> Result<(IndexerConfig, String), PluginError> {
    let obj = config.as_object().ok_or_else(|| {
        PluginError::InvalidConfig(format!("{NAME}: config must be a JSON object"))
    })?;
    let present = |key: &str| {
        obj.get(key)
            .and_then(Value::as_str)
            .is_some_and(|s| !s.trim().is_empty())
    };
    if !present("host") || !present("api_key") {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: config has no `host`/`api_key`. The Meilisearch tenant context \
             (MeiliContext) must be injected into this step's config by the gateway/workflow; \
             this plugin never reads MEILI_URL/MEILI_API_KEY from the environment (SPEC §7.4)"
        )));
    }
    let cfg: IndexerConfig = serde_json::from_value(config)
        .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))?;
    if cfg.batch_size == 0 {
        return Err(PluginError::InvalidConfig(format!(
            "{NAME}: `batch_size` must be at least 1"
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
    Ok((cfg, index))
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
/// Heartbeats between polling slices; gives up (retryable) after [`TASK_TIMEOUT`].
async fn wait_for_task(
    client: &Client,
    ctx: &ActivityContext,
    task: &TaskInfo,
    what: &str,
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
        if waited >= TASK_TIMEOUT {
            return Err(PluginError::Retryable(format!(
                "{NAME}: task {uid} ({what}) did not finish within {}s",
                TASK_TIMEOUT.as_secs()
            )));
        }
    }
}

/// Create the index (with `primary_key`) when it does not exist yet.
async fn ensure_index(
    client: &Client,
    ctx: &ActivityContext,
    uid: &str,
    primary_key: &str,
) -> Result<(), PluginError> {
    match client.get_index(uid).await {
        Ok(_) => Ok(()),
        Err(e) if is_index_not_found(&e) => {
            tracing::info!(plugin = NAME, index = %uid, primary_key = %primary_key, "creating index");
            let info = client
                .create_index(uid, Some(primary_key))
                .await
                .map_err(|e| map_error(e, &format!("creating index {uid:?}")))?;
            match wait_for_task(client, ctx, &info, "index creation").await? {
                None => Ok(()),
                // Another job created it concurrently: fine.
                Some(err) if matches!(err.error_code, ErrorCode::IndexAlreadyExists) => Ok(()),
                Some(err) => Err(task_failed(info.task_uid, "index creation", &err)),
            }
        }
        Err(e) => Err(map_error(e, &format!("fetching index {uid:?}"))),
    }
}

#[async_trait]
impl Plugin for MeiliIndexerPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Pushes documents into Meilisearch using the tenant MeiliContext injected into \
                 the step config (host, api_key, index). Creates the index when missing, batches \
                 addOrReplace calls and waits for the tasks.",
            )
            .accepts([InputKind::Documents, InputKind::Many, InputKind::Empty])
            .produces(OutputKind::Indexed)
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "required": ["host", "api_key", "index"],
                "properties": {
                    "host": {
                        "type": "string",
                        "description": "Meilisearch base URL. Injected from the tenant MeiliContext; never read from env."
                    },
                    "api_key": {
                        "type": "string",
                        "description": "Meilisearch API key with documents.add / indexes.create rights. Injected from the tenant MeiliContext."
                    },
                    "index": {
                        "type": "string",
                        "description": "Target index uid, fully resolved by the gateway (SPEC §3.4)."
                    },
                    "project_id": {
                        "type": ["string", "null"],
                        "default": null,
                        "description": "Tenant id, for logging only."
                    },
                    "region": {
                        "type": ["string", "null"],
                        "default": null,
                        "description": "Region tag, for logging only."
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
                        "description": "Documents per addOrReplace request."
                    },
                    "wait_for_completion": {
                        "type": "boolean",
                        "default": true,
                        "description": "Poll every task until it succeeds; a failed task fails the step (non-retryable)."
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
        let (cfg, index_uid) = parse_config(config)?;
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

        let host = cfg.meili.host.trim().trim_end_matches('/').to_owned();
        let client = Client::new(host, Some(cfg.meili.api_key.as_str()))
            .map_err(|e| map_error(e, "building the Meilisearch client"))?;

        if cfg.auto_create_index {
            ensure_index(&client, ctx, &index_uid, &cfg.primary_key).await?;
        }

        if docs.is_empty() {
            return Ok(PluginOutput::Indexed(IndexReport {
                index: index_uid,
                document_count: 0,
                task_uids: Vec::new(),
            }));
        }

        let index = client.index(index_uid.clone());
        let batch_total = docs.len().div_ceil(cfg.batch_size);
        let mut tasks: Vec<TaskInfo> = Vec::with_capacity(batch_total);
        for (batch_no, batch) in docs.chunks(cfg.batch_size).enumerate() {
            ctx.check_cancelled()?;
            ctx.heartbeat(format!(
                "{NAME}: sending batch {}/{batch_total} ({} documents)",
                batch_no + 1,
                batch.len()
            ));
            let payload: Vec<Value> = batch.iter().map(Document::to_index_json).collect();
            let info = index
                .add_or_replace(&payload, Some(&cfg.primary_key))
                .await
                .map_err(|e| map_error(e, &format!("adding documents (batch {})", batch_no + 1)))?;
            tracing::debug!(
                plugin = NAME,
                index = %index_uid,
                task_uid = info.task_uid,
                batch = batch_no + 1,
                documents = batch.len(),
                "batch enqueued"
            );
            tasks.push(info);
        }

        if cfg.wait_for_completion {
            for info in &tasks {
                if let Some(err) = wait_for_task(&client, ctx, info, "document addition").await? {
                    return Err(task_failed(info.task_uid, "document addition", &err));
                }
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
}
