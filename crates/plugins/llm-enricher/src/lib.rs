//! # `llm_enricher` — the canonical built-in plugin
//!
//! Enriches documents through an OpenAI-compatible `POST /chat/completions` endpoint:
//! for every document the model is asked (in JSON mode) for a title, a summary,
//! keywords and the language, plus anything else the operator's prompt requests, and
//! the reply is merged back into the document.
//!
//! This crate is the reference implementation for writing a built-in plugin
//! (SPEC §7.3). The things to replicate are called out in the code:
//!
//! 1. [`NAME`] + a `*Plugin` struct with `new()` / `Default` / `from_env()` /
//!    `with_client()` constructors. Env vars are read **only** in `from_env`.
//! 2. A typed, serde-deserialized config struct with defaults for every field, and a
//!    matching JSON Schema in [`Plugin::manifest`] (`config_schema`).
//! 3. Accept `Documents` *and* `Many` (fan-in of a previous fan-out) by calling
//!    [`PluginInput::into_documents`].
//! 4. Bounded concurrency with `futures::stream::...buffer_unordered(n)` while keeping
//!    the output in input order.
//! 5. `ctx.heartbeat(..)` every 10 documents and `ctx.check_cancelled()?` in the loop.
//! 6. Precise error mapping: transient upstream failures → [`PluginError::Retryable`],
//!    everything the operator must fix → `NonRetryable` / `InvalidConfig` /
//!    `InvalidInput`. Never leak secrets into errors or logs.
//!
//! ## Pipeline usage
//!
//! ```yaml
//! - id: enrich
//!   plugin: llm_enricher
//!   config:
//!     model: gpt-4o-mini
//!     prompt: "Extract as JSON: title, summary, keywords (array), language (BCP-47), topics (array)."
//!     max_concurrent: 8
//!     merge_strategy: merge
//! ```
//!
//! ## Environment
//!
//! | var | required | default |
//! |---|---|---|
//! | `LLM_API_KEY` | yes | — |
//! | `LLM_BASE_URL` | no | `https://api.openai.com/v1` |
//! | `LLM_MODEL` | no | `gpt-4o-mini` |
//!
//! ## Reply mapping
//!
//! | reply key | goes to |
//! |---|---|
//! | `title` (string) | `Document::title` (only if the document has none, unless `merge_strategy: replace`) |
//! | `language` (string) | `DocumentMeta::language` (same rule) |
//! | `summary`, `keywords`, any other key | `Document::fields` (existing keys kept under `merge`, overwritten under `replace`) |
//!
//! A reply that is not a JSON object is stored verbatim in `fields.llm_raw` and the
//! document is otherwise left unchanged; a single bad reply never fails the batch.

#![forbid(unsafe_code)]

use std::time::Duration;

use futures::StreamExt;
use meili_ingest_plugin_sdk::prelude::*;
use serde::{Deserialize, Serialize};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "llm_enricher";

/// Default OpenAI-compatible base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// Default model.
pub const DEFAULT_MODEL: &str = "gpt-4o-mini";
/// Default user prompt.
pub const DEFAULT_PROMPT: &str =
    "Extract as JSON: title, summary, keywords (array), language (BCP-47).";
/// Default system prompt when the config does not set one.
pub const DEFAULT_SYSTEM_PROMPT: &str = "You are a precise information extraction engine. Reply with a single JSON object and nothing else.";
/// HTTP timeout of the default client built by [`LlmEnricherPlugin::from_env`].
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How reply keys are written into the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Keep existing values (`fields`, `title`, `meta.language`); only fill gaps.
    #[default]
    Merge,
    /// The model's reply overwrites existing values.
    Replace,
}

/// The step `config:` block. Every field has a default so `config: {}` is valid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmEnricherConfig {
    /// Model name; overrides the plugin's default (`LLM_MODEL`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// User prompt, prepended to the document content.
    #[serde(default = "default_prompt")]
    pub prompt: String,
    /// Maximum in-flight requests for one plugin invocation.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// Sampling temperature.
    #[serde(default)]
    pub temperature: f32,
    /// `max_tokens` for the completion (omitted when `None`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// Content longer than this (in chars) is truncated before being sent.
    #[serde(default = "default_max_input_chars")]
    pub max_input_chars: usize,
    /// How to write reply keys into the document.
    #[serde(default)]
    pub merge_strategy: MergeStrategy,
    /// System prompt; defaults to [`DEFAULT_SYSTEM_PROMPT`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_prompt: Option<String>,
}

fn default_prompt() -> String {
    DEFAULT_PROMPT.to_string()
}
fn default_max_concurrent() -> usize {
    8
}
fn default_max_input_chars() -> usize {
    12_000
}

impl Default for LlmEnricherConfig {
    fn default() -> Self {
        Self {
            model: None,
            prompt: default_prompt(),
            max_concurrent: default_max_concurrent(),
            temperature: 0.0,
            max_tokens: None,
            max_input_chars: default_max_input_chars(),
            merge_strategy: MergeStrategy::Merge,
            system_prompt: None,
        }
    }
}

impl LlmEnricherConfig {
    fn validate(&self) -> Result<(), PluginError> {
        if self.max_concurrent == 0 {
            return Err(PluginError::invalid_config("max_concurrent must be >= 1"));
        }
        if self.max_input_chars == 0 {
            return Err(PluginError::invalid_config("max_input_chars must be >= 1"));
        }
        if !(0.0..=2.0).contains(&self.temperature) {
            return Err(PluginError::invalid_config(
                "temperature must be within 0.0..=2.0",
            ));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/// Connection details for the OpenAI-compatible API.
#[derive(Clone)]
struct LlmClient {
    base_url: String,
    api_key: String,
    default_model: String,
    http: reqwest::Client,
}

/// The `llm_enricher` plugin. See the crate docs.
///
/// A plugin built with [`LlmEnricherPlugin::new`] when `LLM_API_KEY` is unset is
/// *disabled*: its manifest is still available (so `GET /plugins` lists it) but
/// `execute` fails with `InvalidConfig("LLM_API_KEY not set")`.
#[derive(Clone)]
pub struct LlmEnricherPlugin {
    client: Option<LlmClient>,
}

impl std::fmt::Debug for LlmEnricherPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the API key.
        let mut d = f.debug_struct("LlmEnricherPlugin");
        match &self.client {
            Some(c) => d
                .field("base_url", &c.base_url)
                .field("default_model", &c.default_model)
                .field("api_key", &"<redacted>"),
            None => d.field("enabled", &false),
        }
        .finish()
    }
}

impl Default for LlmEnricherPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl LlmEnricherPlugin {
    /// [`Self::from_env`], falling back to a disabled instance when `LLM_API_KEY` is
    /// missing. Prefer `from_env()` when you want the error.
    pub fn new() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::disabled())
    }

    /// An instance whose `execute` always fails with `InvalidConfig("LLM_API_KEY not set")`.
    pub fn disabled() -> Self {
        Self { client: None }
    }

    /// Read `LLM_API_KEY` (required), `LLM_BASE_URL`, `LLM_MODEL` from the environment.
    /// This is the only place the crate touches env vars.
    pub fn from_env() -> Result<Self, PluginError> {
        let api_key = std::env::var("LLM_API_KEY")
            .ok()
            .filter(|k| !k.trim().is_empty())
            .ok_or_else(|| PluginError::invalid_config("LLM_API_KEY not set"))?;
        let base_url = std::env::var("LLM_BASE_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let model = std::env::var("LLM_MODEL")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let http = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(|e| PluginError::non_retryable(format!("cannot build HTTP client: {e}")))?;
        Ok(Self::with_client(base_url, api_key, model, http))
    }

    /// Build with explicit connection details (tests, custom HTTP client).
    pub fn with_client(
        base_url: impl Into<String>,
        api_key: impl Into<String>,
        default_model: impl Into<String>,
        http: reqwest::Client,
    ) -> Self {
        Self {
            client: Some(LlmClient {
                base_url: base_url.into().trim_end_matches('/').to_string(),
                api_key: api_key.into(),
                default_model: default_model.into(),
                http,
            }),
        }
    }

    /// Whether credentials are configured.
    pub fn is_enabled(&self) -> bool {
        self.client.is_some()
    }

    fn client(&self) -> Result<&LlmClient, PluginError> {
        self.client
            .as_ref()
            .ok_or_else(|| PluginError::invalid_config("LLM_API_KEY not set"))
    }
}

#[async_trait]
impl Plugin for LlmEnricherPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Enriches documents (title, summary, keywords, language, custom fields) through an OpenAI-compatible chat completions API",
            )
            .accepts([InputKind::Documents, InputKind::Many])
            .produces(OutputKind::Documents)
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "model": { "type": "string", "description": "Model name; defaults to LLM_MODEL" },
                    "prompt": { "type": "string", "default": DEFAULT_PROMPT },
                    "max_concurrent": { "type": "integer", "minimum": 1, "default": 8 },
                    "temperature": { "type": "number", "minimum": 0.0, "maximum": 2.0, "default": 0.0 },
                    "max_tokens": { "type": "integer", "minimum": 1 },
                    "max_input_chars": { "type": "integer", "minimum": 1, "default": 12000 },
                    "merge_strategy": { "type": "string", "enum": ["merge", "replace"], "default": "merge" },
                    "system_prompt": { "type": "string", "default": DEFAULT_SYSTEM_PROMPT }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        // 1. Config: deserialize with defaults, then validate.
        let cfg: LlmEnricherConfig = serde_json::from_value(config)
            .map_err(|e| PluginError::invalid_config(e.to_string()))?;
        cfg.validate()?;
        let client = self.client()?;

        // 2. Input: Documents or Many, flattened.
        let docs = input.into_documents()?;
        let total = docs.len();
        if total == 0 {
            return Ok(PluginOutput::Documents(vec![]));
        }
        tracing::debug!(job_id = %ctx.job_id(), plugin = NAME, documents = total, "enriching documents");

        // 3. Bounded concurrency; results are re-ordered by their original index.
        let mut slots: Vec<Option<Document>> =
            std::iter::repeat_with(|| None).take(total).collect();
        let mut stream = futures::stream::iter(docs.into_iter().enumerate())
            .map(|(i, doc)| {
                let cfg = &cfg;
                async move { (i, client.enrich(cfg, doc).await) }
            })
            .buffer_unordered(cfg.max_concurrent);

        let mut done = 0usize;
        while let Some((i, result)) = stream.next().await {
            ctx.check_cancelled()?;
            let doc = result?;
            if let Some(slot) = slots.get_mut(i) {
                *slot = Some(doc);
            }
            done += 1;
            if done.is_multiple_of(10) || done == total {
                ctx.heartbeat(format!("{NAME}: {done}/{total} documents enriched"));
            }
        }

        let out = slots
            .into_iter()
            .map(|s| {
                s.ok_or_else(|| PluginError::non_retryable("internal: missing enriched document"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PluginOutput::Documents(out))
    }
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChatCompletion {
    #[serde(default)]
    choices: Vec<Choice>,
}
#[derive(Deserialize)]
struct Choice {
    message: ChatMessage,
}
#[derive(Deserialize)]
struct ChatMessage {
    #[serde(default)]
    content: Option<String>,
}

impl LlmClient {
    /// Ask the model about one document and merge the reply into it.
    async fn enrich(
        &self,
        cfg: &LlmEnricherConfig,
        mut doc: Document,
    ) -> Result<Document, PluginError> {
        let content = truncate_chars(&doc.content, cfg.max_input_chars);
        let user = format!("{}\n\n{}", cfg.prompt, content);
        let system = cfg
            .system_prompt
            .as_deref()
            .unwrap_or(DEFAULT_SYSTEM_PROMPT);
        let mut body = serde_json::json!({
            "model": cfg.model.as_deref().unwrap_or(&self.default_model),
            "temperature": cfg.temperature,
            "response_format": { "type": "json_object" },
            "messages": [
                { "role": "system", "content": system },
                { "role": "user", "content": user },
            ],
        });
        if let Some(max) = cfg.max_tokens {
            body["max_tokens"] = serde_json::json!(max);
        }

        let raw = chat_completion(&self.http, &self.base_url, &self.api_key, &body).await?;
        apply_reply(&mut doc, &raw, cfg.merge_strategy);
        Ok(doc)
    }
}

/// POST `{base_url}/chat/completions` and return `choices[0].message.content`.
///
/// Shared error mapping for OpenAI-compatible APIs:
/// 429 / 5xx / transport errors → `Retryable`; 401 / 403 → `NonRetryable`
/// (credentials); 400 → `NonRetryable` with the body; other statuses → `NonRetryable`.
pub async fn chat_completion(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    body: &serde_json::Value,
) -> Result<String, PluginError> {
    let url = format!("{}/chat/completions", base_url.trim_end_matches('/'));
    let resp = http
        .post(&url)
        .bearer_auth(api_key)
        .json(body)
        .send()
        .await
        .map_err(|e| PluginError::retryable(format!("LLM request to {url} failed: {e}")))?;

    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| PluginError::retryable(format!("LLM response body unreadable: {e}")))?;
    if !status.is_success() {
        return Err(map_http_failure(status.as_u16(), &text));
    }

    let parsed: ChatCompletion = serde_json::from_str(&text).map_err(|e| {
        PluginError::non_retryable(format!(
            "LLM response is not a chat completion ({e}): {}",
            truncate_chars(&text, 512)
        ))
    })?;
    parsed
        .choices
        .into_iter()
        .next()
        .and_then(|c| c.message.content)
        .ok_or_else(|| PluginError::non_retryable("LLM response has no choices[0].message.content"))
}

fn map_http_failure(code: u16, body: &str) -> PluginError {
    let snippet = truncate_chars(body, 512);
    match code {
        429 | 500..=599 => {
            PluginError::Retryable(format!("LLM API returned HTTP {code}: {snippet}"))
        }
        401 | 403 => PluginError::NonRetryable(format!(
            "LLM API rejected the credentials (HTTP {code}); check LLM_API_KEY: {snippet}"
        )),
        400 => PluginError::NonRetryable(format!(
            "LLM API rejected the request (HTTP 400): {snippet}"
        )),
        _ => PluginError::NonRetryable(format!(
            "LLM API returned unexpected HTTP {code}: {snippet}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Reply handling
// ---------------------------------------------------------------------------

/// Merge the model's raw reply into `doc` (see the crate docs for the mapping).
pub fn apply_reply(doc: &mut Document, raw: &str, strategy: MergeStrategy) {
    let Some(obj) = parse_json_object(raw) else {
        doc.fields
            .insert("llm_raw".into(), serde_json::Value::String(raw.to_string()));
        return;
    };
    let replace = strategy == MergeStrategy::Replace;
    for (key, value) in obj {
        match (key.as_str(), &value) {
            ("title", serde_json::Value::String(t)) => {
                if replace || doc.title.is_none() {
                    doc.title = Some(t.clone());
                }
            }
            ("language", serde_json::Value::String(l)) => {
                if replace || doc.meta.language.is_none() {
                    doc.meta.language = Some(l.clone());
                }
            }
            _ => {
                if replace {
                    doc.fields.insert(key, value);
                } else {
                    doc.fields.entry(key).or_insert(value);
                }
            }
        }
    }
}

/// Parse a JSON object from model output, tolerating ```` ```json ```` fences.
pub fn parse_json_object(raw: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    match serde_json::from_str::<serde_json::Value>(strip_code_fences(raw)) {
        Ok(serde_json::Value::Object(m)) => Some(m),
        _ => None,
    }
}

/// Remove a surrounding Markdown code fence (with optional language tag).
pub fn strip_code_fences(s: &str) -> &str {
    let t = s.trim();
    let Some(rest) = t.strip_prefix("```") else {
        return t;
    };
    let rest = match rest.find('\n') {
        Some(i) => &rest[i + 1..],
        None => rest,
    };
    rest.trim_end().strip_suffix("```").unwrap_or(rest).trim()
}

/// Truncate to at most `max` chars on a char boundary.
pub fn truncate_chars(s: &str, max: usize) -> &str {
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::AtomicBool;
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

    fn chat_reply(content: &str) -> serde_json::Value {
        serde_json::json!({
            "id": "chatcmpl-1", "object": "chat.completion",
            "choices": [{ "index": 0, "finish_reason": "stop",
                          "message": { "role": "assistant", "content": content } }]
        })
    }

    fn plugin(server: &MockServer) -> LlmEnricherPlugin {
        LlmEnricherPlugin::with_client(
            server.uri(),
            "test-key",
            "test-model",
            reqwest::Client::new(),
        )
    }

    async fn run(
        p: &LlmEnricherPlugin,
        docs: Vec<Document>,
        cfg: serde_json::Value,
    ) -> Result<Vec<Document>, PluginError> {
        p.execute(&ActivityContext::noop(), PluginInput::Documents(docs), cfg)
            .await?
            .into_documents()
    }

    #[tokio::test]
    async fn happy_path_merges_reply_without_overwriting() {
        let server = MockServer::start().await;
        let reply = r#"{"title":"Model Title","summary":"S","keywords":["a","b"],"language":"fr","topic":"model"}"#;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("authorization", "Bearer test-key"))
            .and(body_partial_json(serde_json::json!({
                "model": "test-model",
                "response_format": { "type": "json_object" },
                "temperature": 0.0
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply(reply)))
            .expect(1)
            .mount(&server)
            .await;

        let mut doc = Document::with_id("d1", "Bonjour le monde");
        doc.fields
            .insert("topic".into(), serde_json::json!("original"));
        let out = run(&plugin(&server), vec![doc], serde_json::json!({}))
            .await
            .unwrap();

        assert_eq!(out.len(), 1);
        let d = &out[0];
        assert_eq!(d.id, "d1");
        assert_eq!(d.content, "Bonjour le monde");
        assert_eq!(d.title.as_deref(), Some("Model Title"));
        assert_eq!(d.fields["summary"], "S");
        assert_eq!(d.fields["keywords"], serde_json::json!(["a", "b"]));
        assert_eq!(d.fields["topic"], "original", "merge must not overwrite");
        assert_eq!(d.meta.language.as_deref(), Some("fr"));
        assert!(!d.fields.contains_key("title"));
        assert!(!d.fields.contains_key("language"));

        let reqs = server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        assert_eq!(body["messages"][0]["role"], "system");
        let user = body["messages"][1]["content"].as_str().unwrap();
        assert!(user.starts_with(DEFAULT_PROMPT));
        assert!(user.ends_with("Bonjour le monde"));
        assert!(body.get("max_tokens").is_none());
    }

    #[tokio::test]
    async fn replace_strategy_overwrites_and_model_override_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({"model": "other", "max_tokens": 50}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply(
                r#"{"title":"New","topic":"model","language":"en"}"#,
            )))
            .expect(1)
            .mount(&server)
            .await;

        let mut doc = Document::with_id("d1", "x");
        doc.title = Some("Old".into());
        doc.meta.language = Some("fr".into());
        doc.fields
            .insert("topic".into(), serde_json::json!("original"));
        let out = run(
            &plugin(&server),
            vec![doc],
            serde_json::json!({"merge_strategy": "replace", "model": "other", "max_tokens": 50}),
        )
        .await
        .unwrap();
        assert_eq!(out[0].title.as_deref(), Some("New"));
        assert_eq!(out[0].fields["topic"], "model");
        assert_eq!(out[0].meta.language.as_deref(), Some("en"));
    }

    #[tokio::test]
    async fn rate_limit_is_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .mount(&server)
            .await;
        let err = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, PluginError::Retryable(m) if m.contains("429") && m.contains("slow down")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn server_error_is_retryable_and_auth_error_is_not() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(503))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("bad key"))
            .mount(&server)
            .await;
        let p = plugin(&server);
        let err = run(&p, vec![Document::with_id("a", "x")], serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Retryable(_)), "{err:?}");
        let err = run(&p, vec![Document::with_id("a", "x")], serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("401")),
            "{err:?}"
        );
        assert!(!err.to_string().contains("test-key"));
    }

    #[tokio::test]
    async fn bad_request_is_non_retryable_with_body() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(400).set_body_string(r#"{"error":"unknown model"}"#),
            )
            .mount(&server)
            .await;
        let err = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("unknown model")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn unparsable_reply_keeps_document_and_stores_llm_raw() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_reply("Sorry, I cannot do that.")),
            )
            .mount(&server)
            .await;
        let mut doc = Document::with_id("a", "content");
        doc.title = Some("T".into());
        let out = run(&plugin(&server), vec![doc.clone()], serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(out[0].fields["llm_raw"], "Sorry, I cannot do that.");
        assert_eq!(out[0].title, doc.title);
        assert_eq!(out[0].content, doc.content);
        assert_eq!(out[0].fields.len(), 1);
    }

    #[tokio::test]
    async fn fenced_json_is_accepted() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(chat_reply("```json\n{\"summary\": \"fenced\"}\n```")),
            )
            .mount(&server)
            .await;
        let out = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            serde_json::json!({}),
        )
        .await
        .unwrap();
        assert_eq!(out[0].fields["summary"], "fenced");
        assert!(!out[0].fields.contains_key("llm_raw"));
    }

    /// Replies with a summary derived from the document content, slower for early
    /// documents so completion order differs from input order.
    struct EchoSummary;
    impl Respond for EchoSummary {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let user = body["messages"][1]["content"].as_str().unwrap();
            let content = user.rsplit("\n\n").next().unwrap();
            let idx: u64 = content.trim_start_matches("doc number ").parse().unwrap();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis((25 - idx) * 4))
                .set_body_json(chat_reply(&format!(
                    r#"{{"summary":"summary of {content}"}}"#
                )))
        }
    }

    #[tokio::test]
    async fn concurrency_all_requests_arrive_and_order_is_preserved() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(EchoSummary)
            .expect(25)
            .mount(&server)
            .await;

        let docs: Vec<Document> = (0..25)
            .map(|i| Document::with_id(format!("doc-{i}"), format!("doc number {i}")))
            .collect();
        let out = run(
            &plugin(&server),
            docs,
            serde_json::json!({"max_concurrent": 5}),
        )
        .await
        .unwrap();

        assert_eq!(out.len(), 25);
        for (i, d) in out.iter().enumerate() {
            assert_eq!(d.id, format!("doc-{i}"), "order must be preserved");
            assert_eq!(d.fields["summary"], format!("summary of doc number {i}"));
        }
        assert_eq!(server.received_requests().await.unwrap().len(), 25);
    }

    #[tokio::test]
    async fn many_input_is_flattened_and_content_is_truncated() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .expect(2)
            .mount(&server)
            .await;
        let input = PluginInput::Many(vec![
            PluginOutput::Documents(vec![Document::with_id("a", "é".repeat(100))]),
            PluginOutput::Documents(vec![Document::with_id("b", "short")]),
        ]);
        let out = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                input,
                serde_json::json!({"max_input_chars": 10}),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        assert_eq!(
            out.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(
            out[0].content.chars().count(),
            100,
            "the document itself is not truncated"
        );

        let reqs = server.received_requests().await.unwrap();
        let sent: Vec<String> = reqs
            .iter()
            .map(|r| {
                let b: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
                b["messages"][1]["content"]
                    .as_str()
                    .unwrap()
                    .rsplit("\n\n")
                    .next()
                    .unwrap()
                    .to_string()
            })
            .collect();
        assert!(sent.contains(&"é".repeat(10)));
        assert!(sent.contains(&"short".to_string()));
    }

    #[tokio::test]
    async fn cancellation_is_observed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .mount(&server)
            .await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ActivityContext::new(
            uuid::Uuid::nil(),
            "enrich",
            1,
            tx,
            Arc::new(AtomicBool::new(true)),
        );
        let err = plugin(&server)
            .execute(
                &ctx,
                PluginInput::Documents(vec![Document::with_id("a", "x")]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled));
    }

    #[tokio::test]
    async fn disabled_plugin_and_invalid_config_and_input() {
        let p = LlmEnricherPlugin::disabled();
        assert!(!p.is_enabled());
        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidConfig(m) if m.contains("LLM_API_KEY")),
            "{err:?}"
        );

        let server = MockServer::start().await;
        let p = plugin(&server);
        let err = run(&p, vec![], serde_json::json!({"max_concurrent": 0}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)));
        let err = run(&p, vec![], serde_json::json!({"merge_strategy": "yolo"}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)));
        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(vec![1], "application/pdf", None)),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)));
        assert!(!format!("{p:?}").contains("test-key"));
    }

    #[test]
    fn manifest_and_helpers() {
        let m = LlmEnricherPlugin::disabled().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Documents) && m.accepts_kind(InputKind::Many));
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(
            m.config_schema["properties"]["max_concurrent"]["default"],
            8
        );

        assert_eq!(strip_code_fences("```json\n{\"a\":1}\n```"), "{\"a\":1}");
        assert_eq!(strip_code_fences("```{\"a\":1}```"), "{\"a\":1}");
        assert_eq!(strip_code_fences("  {\"a\":1} "), "{\"a\":1}");
        assert_eq!(truncate_chars("héllo", 2), "hé");
        assert_eq!(truncate_chars("hi", 10), "hi");
        assert!(parse_json_object("[1,2]").is_none());
        let cfg: LlmEnricherConfig = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(cfg, LlmEnricherConfig::default());
    }
}
