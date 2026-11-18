//! # `image_captioner`
//!
//! Turns an image into a searchable document by asking an OpenAI-compatible vision
//! chat model for a caption, tags and any text visible in the image.
//!
//! Follows the built-in plugin pattern of `llm_enricher` (the canonical example):
//! `new()` / `Default` / `from_env()` / `with_client()`, typed config with defaults,
//! JSON Schema in the manifest, precise retry semantics.
//!
//! ## Pipeline usage
//!
//! ```yaml
//! - id: caption
//!   plugin: image_captioner
//!   config:
//!     detail: high
//!     max_tokens: 400
//! ```
//!
//! ## Environment
//!
//! | var | required | default |
//! |---|---|---|
//! | `LLM_API_KEY` | yes (unless the Lab provides a target, see below) | — |
//! | `LLM_BASE_URL` | no | `https://api.openai.com/v1` |
//! | `LLM_MODEL` | no | `gpt-4o-mini` |
//!
//! ## Behind the Lab
//!
//! Behind the Meilisearch Lab the worker attaches the team's [`AiTarget`] (its hosted
//! Gateway and a per-team key) to the step's [`ActivityContext`]. When one is present the
//! plugin calls it instead of the env-configured API, with the target's `vision_model`
//! (a step's `config.model` still wins), and even a [`ImageCaptionerPlugin::disabled`]
//! instance works. A Gateway 401 marks the target rejected and is retried with a fresh
//! key; a 402 means the team's budget is spent and is final; a 403 is final. Without a target
//! nothing changes: the env client is used, and `LLM_API_KEY not set` is the error.
//!
//! ## Output
//!
//! One document. With `json: true` (default) the model must reply with
//! `{"caption", "tags", "text_in_image"}`: `caption` → `content`, `tags` →
//! `fields.tags`, `text_in_image` → `fields.ocr_text`, other keys → `fields`, except
//! keys starting with `_` (`_geo`, `_vectors`, …), which are dropped unless the step
//! lists them in `allow_reserved`. If the
//! reply is not a JSON object the whole text becomes `content`. With `json: false` the
//! model's full text reply is the `content`. `meta.mime` / `meta.filename` /
//! `meta.source` come from the input blob; the id derives from the filename when known.

#![forbid(unsafe_code)]

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use base64::Engine;
use meili_ingest_plugin_sdk::ai::gateway_failure;
use meili_ingest_plugin_sdk::cost::ProviderCosts;
use meili_ingest_plugin_sdk::prelude::*;
use meili_ingest_plugin_sdk::vendor::direct_model;
use serde::{Deserialize, Serialize};

mod retry;
use retry::{Failure, RetryPolicy, with_retry};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "image_captioner";

/// Default OpenAI-compatible base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// Default (vision-capable) model.
pub const DEFAULT_MODEL: &str = "gpt-4o-mini";
/// Default prompt.
pub const DEFAULT_PROMPT: &str = "Describe this image in detail for search indexing. Reply with JSON: {\"caption\": ..., \"tags\": [...], \"text_in_image\": ...}";
/// HTTP timeout of the default client built by [`ImageCaptionerPlugin::from_env`].
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(120);
/// Default `max_bytes`: 20 MiB, OpenAI's limit for one image. A larger image would be
/// refused by the provider anyway, after costing several base64 copies of itself.
pub const DEFAULT_MAX_BYTES: usize = 20 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Vision `detail` hint (OpenAI semantics).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Detail {
    /// Low-resolution pass (cheapest).
    Low,
    /// High-resolution tiles.
    High,
    /// Let the model decide.
    #[default]
    Auto,
}

/// The step `config:` block. Every field has a default so `config: {}` is valid.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ImageCaptionerConfig {
    /// Model name; overrides the plugin's default (`LLM_MODEL`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Instruction sent alongside the image.
    #[serde(default = "default_prompt")]
    pub prompt: String,
    /// Vision detail level.
    #[serde(default)]
    pub detail: Detail,
    /// `max_tokens` of the completion.
    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,
    /// Request JSON mode and map `caption`/`tags`/`text_in_image` (default). When false
    /// the raw text reply becomes the document content.
    #[serde(default = "default_true")]
    pub json: bool,
    /// Largest image accepted, in bytes; a larger one fails as invalid input before
    /// anything is encoded or sent.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,
    /// Reply keys starting with `_` that may reach the document (for example
    /// `["_geo"]`). Every other `_` key is dropped: Meilisearch reserves them, and a
    /// malformed one makes it reject the whole batch.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allow_reserved: Vec<String>,
}

fn default_prompt() -> String {
    DEFAULT_PROMPT.to_string()
}
fn default_max_tokens() -> u32 {
    400
}
fn default_true() -> bool {
    true
}
fn default_max_bytes() -> usize {
    DEFAULT_MAX_BYTES
}

impl Default for ImageCaptionerConfig {
    fn default() -> Self {
        Self {
            model: None,
            prompt: default_prompt(),
            detail: Detail::Auto,
            max_tokens: default_max_tokens(),
            json: true,
            max_bytes: DEFAULT_MAX_BYTES,
            allow_reserved: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct LlmClient {
    base_url: String,
    api_key: String,
    default_model: String,
    http: reqwest::Client,
    /// Set when this client was built from the Lab's target: Gateway failures then map
    /// through [`gateway_failure`], and a rejected key is flagged on the shared target.
    lab: Option<AiTarget>,
}

/// The `image_captioner` plugin. See the crate docs.
///
/// Built with [`ImageCaptionerPlugin::new`] without `LLM_API_KEY`, the plugin is
/// *disabled*: the manifest is available but `execute` fails with
/// `InvalidConfig("LLM_API_KEY not set")`.
#[derive(Clone)]
pub struct ImageCaptionerPlugin {
    client: Option<LlmClient>,
    /// HTTP client for the Lab's target when there is no env client, built on first use
    /// and shared by clones, so a keyless worker keeps one connection pool across steps.
    /// The team key travels per request, so sharing the pool shares no credential.
    lab_http: Arc<OnceLock<reqwest::Client>>,
    /// In-plugin retry of the request's transient failures (see [`retry`]).
    retry: RetryPolicy,
}

impl std::fmt::Debug for ImageCaptionerPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("ImageCaptionerPlugin");
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

impl Default for ImageCaptionerPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl ImageCaptionerPlugin {
    /// [`Self::from_env`], falling back to a disabled instance when `LLM_API_KEY` is missing.
    pub fn new() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::disabled())
    }

    /// An instance whose `execute` always fails with `InvalidConfig("LLM_API_KEY not set")`.
    pub fn disabled() -> Self {
        Self {
            client: None,
            lab_http: Arc::default(),
            retry: RetryPolicy::DEFAULT,
        }
    }

    /// Read `LLM_API_KEY` (required), `LLM_BASE_URL`, `LLM_MODEL` from the environment.
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
                lab: None,
            }),
            lab_http: Arc::default(),
            retry: RetryPolicy::DEFAULT,
        }
    }

    /// Whether credentials are configured.
    pub fn is_enabled(&self) -> bool {
        self.client.is_some()
    }

    /// The client for one step: the Lab's target when the context carries one, else the
    /// env client, else `InvalidConfig("LLM_API_KEY not set")`.
    fn client_for(&self, ctx: &ActivityContext) -> Result<LlmClient, PluginError> {
        if let Some(target) = ctx.ai_target() {
            let http = match &self.client {
                Some(c) => c.http.clone(),
                None => self.lab_http()?,
            };
            return Ok(LlmClient {
                base_url: target.base_url.trim_end_matches('/').to_string(),
                api_key: target.api_key.clone(),
                default_model: target.vision_model.clone(),
                http,
                lab: Some(target.clone()),
            });
        }
        self.client
            .clone()
            .ok_or_else(|| PluginError::invalid_config("LLM_API_KEY not set"))
    }

    /// The shared client for the Lab's target (see `lab_http`), built on first use.
    fn lab_http(&self) -> Result<reqwest::Client, PluginError> {
        if let Some(http) = self.lab_http.get() {
            return Ok(http.clone());
        }
        let built = reqwest::Client::builder()
            .timeout(DEFAULT_TIMEOUT)
            .build()
            .map_err(|e| PluginError::non_retryable(format!("cannot build HTTP client: {e}")))?;
        // Two first steps racing both build; one wins and both use the winner.
        Ok(self.lab_http.get_or_init(|| built).clone())
    }
}

#[async_trait]
impl Plugin for ImageCaptionerPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description("Captions an image with a vision chat model (caption, tags, text in image) and emits one searchable document")
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["image/*"])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "model": { "type": "string", "description": "Vision-capable model; defaults to LLM_MODEL" },
                    "prompt": { "type": "string", "default": DEFAULT_PROMPT },
                    "detail": { "type": "string", "enum": ["low", "high", "auto"], "default": "auto" },
                    "max_tokens": { "type": "integer", "minimum": 1, "default": 400 },
                    "json": { "type": "boolean", "default": true },
                    "max_bytes": { "type": "integer", "minimum": 1, "default": DEFAULT_MAX_BYTES },
                    "allow_reserved": {
                        "type": "array",
                        "items": { "type": "string", "pattern": "^_" },
                        "description": "Reply keys starting with _ to keep, e.g. [\"_geo\"]; any other is dropped",
                        "default": []
                    }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg: ImageCaptionerConfig = serde_json::from_value(config)
            .map_err(|e| PluginError::invalid_config(e.to_string()))?;
        if cfg.max_tokens == 0 {
            return Err(PluginError::invalid_config("max_tokens must be >= 1"));
        }
        if cfg.max_bytes == 0 {
            return Err(PluginError::invalid_config("max_bytes must be >= 1"));
        }
        let client = self.client_for(ctx)?;

        let mut blob = input.into_bytes()?;
        let mime = blob
            .mime
            .split(';')
            .next()
            .unwrap_or("")
            .trim()
            .to_ascii_lowercase();
        if !mime.starts_with("image/") {
            return Err(PluginError::invalid_input(format!(
                "{NAME} expects an image/* blob, got {:?}",
                blob.mime
            )));
        }
        if blob.data.is_empty() {
            return Err(PluginError::invalid_input("empty image"));
        }
        if blob.data.len() > cfg.max_bytes {
            return Err(PluginError::invalid_input(format!(
                "image is {} bytes, over max_bytes ({})",
                blob.data.len(),
                cfg.max_bytes
            )));
        }
        ctx.check_cancelled()?;
        ctx.heartbeat(format!(
            "{NAME}: sending {} bytes to the vision model",
            blob.data.len()
        ));

        // The data URL is the one base64 copy of the image: encoded straight into a
        // String sized for it, the raw bytes dropped right after, and the String moved
        // (not cloned) into the request body below.
        let data_url = {
            let data = std::mem::take(&mut blob.data);
            let prefix = format!("data:{mime};base64,");
            let mut url = String::with_capacity(prefix.len() + data.len().div_ceil(3) * 4);
            url.push_str(&prefix);
            base64::engine::general_purpose::STANDARD.encode_string(&data, &mut url);
            url
        };
        let model = cfg.model.as_deref().unwrap_or(&client.default_model);
        // The Lab's Gateway takes the id as written; a vendor called directly gets it
        // without its own `<provider>/` prefix.
        let wire_model = if client.lab.is_some() {
            model
        } else {
            direct_model(&client.base_url, model)
        };
        let mut body = serde_json::json!({
            "model": wire_model,
            "max_tokens": cfg.max_tokens,
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": prompt_text(&cfg) },
                    { "type": "image_url", "image_url": { "url": "", "detail": cfg.detail } }
                ]
            }],
        });
        // Assigned rather than interpolated: `json!` would clone the whole data URL.
        body["messages"][0]["content"][1]["image_url"]["url"] = serde_json::Value::String(data_url);
        if cfg.json {
            body["response_format"] = serde_json::json!({ "type": "json_object" });
        }

        // A failed call records nothing: `?` returns before `record_usage`. A transient
        // failure (429, 5xx, dropped connection) is retried here first.
        let (raw, mut units) = with_retry(&self.retry, ctx, NAME, || {
            chat_completion(
                &client.http,
                &client.base_url,
                &client.api_key,
                &body,
                client.lab.as_ref(),
            )
        })
        .await?;
        // One image went to the vision model, and its tokens are billed on top.
        units.images = 1;
        ProviderCosts::global().apply(NAME, model, &mut units);
        ctx.record_usage(units);

        let mut doc = match blob.filename.as_deref() {
            Some(name) if !name.is_empty() => Document::with_id(blob.tag_id(sanitize_id(name)), ""),
            _ if blob.origin.is_some() => Document::with_id(blob.tag_id("image"), ""),
            _ => Document::new(""),
        };
        doc.meta.mime = Some(blob.mime.clone());
        doc.meta.filename = blob.filename.clone();
        doc.meta.source = blob.filename.clone();
        apply_reply(&mut doc, &raw, cfg.json, &cfg.allow_reserved);

        tracing::debug!(job_id = %ctx.job_id(), plugin = NAME, id = %doc.id, "captioned image");
        Ok(PluginOutput::Documents(vec![doc]))
    }
}

/// Sentence appended to a JSON-mode prompt that does not mention JSON.
const JSON_REPLY_HINT: &str = "Reply with a single JSON object.";

/// The text part sent with the image. OpenAI refuses `response_format: json_object`
/// unless a message names JSON, so in JSON mode a prompt that does not gets
/// [`JSON_REPLY_HINT`] appended; any other prompt goes out exactly as configured.
fn prompt_text(cfg: &ImageCaptionerConfig) -> String {
    if cfg.json && !cfg.prompt.to_ascii_lowercase().contains("json") {
        format!("{}\n\n{JSON_REPLY_HINT}", cfg.prompt.trim_end())
    } else {
        cfg.prompt.clone()
    }
}

// ---------------------------------------------------------------------------
// Reply handling
// ---------------------------------------------------------------------------

/// Map the model reply onto the document (see the crate docs).
///
/// A reply key starting with `_` is dropped unless `allow_reserved` lists it.
pub fn apply_reply(doc: &mut Document, raw: &str, json: bool, allow_reserved: &[String]) {
    let parsed = if json { parse_json_object(raw) } else { None };
    let Some(obj) = parsed else {
        doc.content = raw.trim().to_string();
        return;
    };
    for (key, value) in obj {
        match (key.as_str(), &value) {
            ("caption", serde_json::Value::String(c)) => doc.content = c.clone(),
            ("tags", _) => {
                doc.fields.insert("tags".into(), value);
            }
            ("text_in_image", _) => {
                if !value.is_null() {
                    doc.fields.insert("ocr_text".into(), value);
                }
            }
            // Meilisearch reserves `_` keys (`_geo`, `_vectors`, ...): only the ones the
            // step asked for get through.
            (k, _) if k.starts_with('_') && !allow_reserved.iter().any(|a| a == k) => {
                tracing::debug!(plugin = NAME, key = k, "dropping reserved reply key");
            }
            _ => {
                doc.fields.insert(key, value);
            }
        }
    }
    if doc.content.is_empty() {
        // The model ignored the caption key; keep the whole reply searchable.
        doc.content = raw.trim().to_string();
    }
}

// ---------------------------------------------------------------------------
// HTTP (same semantics as llm_enricher)
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct ChatCompletion {
    #[serde(default)]
    choices: Vec<Choice>,
    /// Absent on gateways that do not report token accounting.
    #[serde(default)]
    usage: Option<ApiUsage>,
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

/// The `usage` object of an OpenAI-compatible chat completion.
#[derive(Debug, Clone, Copy, Default, Deserialize)]
struct ApiUsage {
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
}

/// POST `{base_url}/chat/completions` and return `choices[0].message.content` plus the
/// units the call cost.
///
/// The `usage` object is optional — some gateways omit it — and when it is missing the
/// returned units still count one request, with the token counts left at zero rather
/// than estimated.
///
/// 429 / 5xx / transport → `Retryable`; 401 / 403 / 400 / others → `NonRetryable`. When
/// `lab` is the Lab's Gateway target, [`gateway_failure`] gets the first say (see
/// [`map_http_failure`]). A failure says whether it is transient (see
/// [`transport_failure`] and [`http_failure`]), for the in-plugin retry.
async fn chat_completion(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    body: &serde_json::Value,
    lab: Option<&AiTarget>,
) -> Result<(String, UsageUnits), Failure> {
    let url = format!("{base_url}/chat/completions");
    let resp = http
        .post(&url)
        .bearer_auth(api_key)
        .json(body)
        .send()
        .await
        .map_err(|e| transport_failure(&e, format!("vision request to {url} failed: {e}")))?;
    let status = resp.status();
    let retry_after = retry::retry_after(resp.headers());
    let text = resp
        .text()
        .await
        .map_err(|e| transport_failure(&e, format!("vision response body unreadable: {e}")))?;
    if !status.is_success() {
        return Err(http_failure(status, retry_after, &text, lab));
    }
    let parsed: ChatCompletion = serde_json::from_str(&text).map_err(|e| {
        Failure::Other(PluginError::non_retryable(format!(
            "vision response is not a chat completion ({e}): {}",
            truncate_chars(&text, 512)
        )))
    })?;
    let usage = match parsed.usage {
        Some(u) => UsageUnits::llm(u.prompt_tokens, u.completion_tokens),
        None => UsageUnits {
            llm_requests: 1,
            ..Default::default()
        },
    };
    let content = parsed
        .choices
        .into_iter()
        .next()
        .and_then(|c| c.message.content)
        .ok_or_else(|| {
            Failure::Other(PluginError::non_retryable(
                "vision response has no choices[0].message.content",
            ))
        })?;
    Ok((content, usage))
}

/// A request that never got a full answer: `Retryable` either way, and retried in the
/// plugin unless it timed out (another full timeout would eat the step's budget).
fn transport_failure(e: &reqwest::Error, message: String) -> Failure {
    let error = PluginError::retryable(message);
    if e.is_timeout() {
        Failure::Other(error)
    } else {
        Failure::Transient {
            error,
            retry_after: None,
        }
    }
}

/// A non-2xx answer: 429 and 5xx are transient; everything else, including the Lab
/// Gateway's 401 (only a fresh key helps, and the worker fetches it), is not.
fn http_failure(
    status: reqwest::StatusCode,
    retry_after: Option<Duration>,
    body: &str,
    lab: Option<&AiTarget>,
) -> Failure {
    let error = map_http_failure(status, body, lab);
    if (status.as_u16() == 429 || status.is_server_error()) && error.is_retryable() {
        Failure::Transient { error, retry_after }
    } else {
        Failure::Other(error)
    }
}

/// Map a non-2xx reply to a [`PluginError`]. With the Lab's target, a Gateway refusal
/// (401 / 402) is mapped by [`gateway_failure`] first.
fn map_http_failure(
    status: reqwest::StatusCode,
    body: &str,
    lab: Option<&AiTarget>,
) -> PluginError {
    if let Some(err) = lab.and_then(|t| gateway_failure(t, status.as_u16(), body)) {
        return err;
    }
    let snippet = truncate_chars(body, 512);
    match status.as_u16() {
        429 | 500..=599 => {
            PluginError::Retryable(format!("vision API returned HTTP {status}: {snippet}"))
        }
        401 | 403 => PluginError::NonRetryable(format!(
            "vision API rejected the credentials (HTTP {status}); check LLM_API_KEY: {snippet}"
        )),
        400 => PluginError::NonRetryable(format!(
            "vision API rejected the request (HTTP 400): {snippet}"
        )),
        _ => PluginError::NonRetryable(format!(
            "vision API returned unexpected HTTP {status}: {snippet}"
        )),
    }
}

fn parse_json_object(raw: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    match serde_json::from_str::<serde_json::Value>(strip_code_fences(raw)) {
        Ok(serde_json::Value::Object(m)) => Some(m),
        _ => None,
    }
}

fn strip_code_fences(s: &str) -> &str {
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

fn truncate_chars(s: &str, max: usize) -> &str {
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
    use std::sync::atomic::{AtomicUsize, Ordering};
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const PNG: &[u8] = b"\x89PNG\r\n\x1a\n fake image bytes";

    fn chat_reply(content: &str) -> serde_json::Value {
        serde_json::json!({ "choices": [{ "message": { "role": "assistant", "content": content } }] })
    }

    /// The env-configured plugin, retrying transient failures without waiting.
    fn plugin(server: &MockServer) -> ImageCaptionerPlugin {
        let mut p = ImageCaptionerPlugin::with_client(
            server.uri(),
            "test-key",
            "vision-model",
            reqwest::Client::new(),
        );
        p.retry = RetryPolicy::IMMEDIATE;
        p
    }

    fn image() -> PluginInput {
        PluginInput::Bytes(Blob::new(
            PNG.to_vec(),
            "image/png",
            Some("cat photo.png".into()),
        ))
    }

    #[tokio::test]
    async fn happy_path_maps_json_reply() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .and(header("authorization", "Bearer test-key"))
            .and(body_partial_json(serde_json::json!({
                "model": "vision-model",
                "max_tokens": 400,
                "response_format": { "type": "json_object" }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply(
                r#"{"caption":"A cat on a sofa","tags":["cat","sofa"],"text_in_image":"MEOW","mood":"calm"}"#,
            )))
            .expect(1)
            .mount(&server)
            .await;

        let out = plugin(&server)
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 1);
        let d = &docs[0];
        assert_eq!(d.id, "cat_photo_png");
        assert_eq!(d.content, "A cat on a sofa");
        assert_eq!(d.fields["tags"], serde_json::json!(["cat", "sofa"]));
        assert_eq!(d.fields["ocr_text"], "MEOW");
        assert_eq!(d.fields["mood"], "calm");
        assert!(!d.fields.contains_key("caption"));
        assert_eq!(d.meta.mime.as_deref(), Some("image/png"));
        assert_eq!(d.meta.filename.as_deref(), Some("cat photo.png"));

        let reqs = server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        let parts = body["messages"][0]["content"].as_array().unwrap();
        assert_eq!(parts[0]["type"], "text");
        assert_eq!(parts[0]["text"], DEFAULT_PROMPT);
        assert_eq!(parts[1]["type"], "image_url");
        assert_eq!(parts[1]["image_url"]["detail"], "auto");
        let url = parts[1]["image_url"]["url"].as_str().unwrap();
        let expected = format!(
            "data:image/png;base64,{}",
            base64::engine::general_purpose::STANDARD.encode(PNG)
        );
        assert_eq!(url, expected);
    }

    #[tokio::test]
    async fn plain_text_mode_uses_full_reply_as_content() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({"model": "gpt-4o", "max_tokens": 50}),
            ))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(chat_reply("  A long free-form description. ")),
            )
            .expect(1)
            .mount(&server)
            .await;
        let docs = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                image(),
                serde_json::json!({"json": false, "model": "gpt-4o", "max_tokens": 50, "detail": "high"}),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        assert_eq!(docs[0].content, "A long free-form description.");
        assert!(docs[0].fields.is_empty());

        let reqs = server.received_requests().await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&reqs[0].body).unwrap();
        assert!(body.get("response_format").is_none());
        assert_eq!(
            body["messages"][0]["content"][1]["image_url"]["detail"],
            "high"
        );
    }

    #[tokio::test]
    async fn reserved_reply_keys_are_dropped_unless_allowed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply(
                r#"{"caption":"c","_geo":{"lat":48.8,"lng":2.3},"_vectors":{"x":[1]},"mood":"calm"}"#,
            )))
            .mount(&server)
            .await;
        let p = plugin(&server);
        let caption = |cfg: serde_json::Value| {
            let p = p.clone();
            async move {
                p.execute(&ActivityContext::noop(), image(), cfg)
                    .await
                    .unwrap()
                    .into_documents()
                    .unwrap()
                    .remove(0)
            }
        };
        let d = caption(serde_json::json!({})).await;
        assert_eq!(d.fields["mood"], "calm");
        assert!(
            d.fields.keys().all(|k| !k.starts_with('_')),
            "{:?}",
            d.fields
        );
        let d = caption(serde_json::json!({ "allow_reserved": ["_geo"] })).await;
        assert_eq!(
            d.fields["_geo"],
            serde_json::json!({"lat": 48.8, "lng": 2.3})
        );
        assert!(!d.fields.contains_key("_vectors"));
    }

    #[tokio::test]
    async fn non_json_reply_in_json_mode_falls_back_to_text() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("just words")))
            .mount(&server)
            .await;
        let docs = plugin(&server)
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        assert_eq!(docs[0].content, "just words");
    }

    #[tokio::test]
    async fn non_image_input_is_rejected_without_calling_the_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let p = plugin(&server);
        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(b"%PDF-1.4".to_vec(), "application/pdf", None)),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("application/pdf")),
            "{err:?}"
        );
        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)));
    }

    #[tokio::test]
    async fn an_image_over_max_bytes_is_rejected_without_calling_the_api() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .expect(0)
            .mount(&server)
            .await;
        let p = plugin(&server);
        let five_bytes = || PluginInput::Bytes(Blob::new(vec![1; 5], "image/png", None));
        let err = p
            .execute(
                &ActivityContext::noop(),
                five_bytes(),
                serde_json::json!({"max_bytes": 4}),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("max_bytes")),
            "{err:?}"
        );
        let err = p
            .execute(
                &ActivityContext::noop(),
                five_bytes(),
                serde_json::json!({"max_bytes": 0}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err:?}");
        assert_eq!(ImageCaptionerConfig::default().max_bytes, DEFAULT_MAX_BYTES);
        assert_eq!(
            p.manifest().config_schema["properties"]["max_bytes"]["default"],
            DEFAULT_MAX_BYTES
        );
    }

    #[tokio::test]
    async fn an_image_at_max_bytes_is_sent() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .expect(1)
            .mount(&server)
            .await;
        plugin(&server)
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(vec![1; 5], "image/png", None)),
                serde_json::json!({"max_bytes": 5}),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn json_mode_names_json_even_when_the_prompt_does_not() {
        // OpenAI rejects `response_format: json_object` unless a message mentions JSON.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_reply(r#"{"caption":"A shoe"}"#)),
            )
            .expect(2)
            .mount(&server)
            .await;
        let p = plugin(&server);
        let prompt = "Describe the product shown in this photo.";
        p.execute(
            &ActivityContext::noop(),
            image(),
            serde_json::json!({ "prompt": prompt }),
        )
        .await
        .unwrap();
        p.execute(
            &ActivityContext::noop(),
            image(),
            serde_json::json!({ "prompt": prompt, "json": false }),
        )
        .await
        .unwrap();

        let reqs = server.received_requests().await.unwrap();
        let text = |i: usize| -> (serde_json::Value, String) {
            let body: serde_json::Value = serde_json::from_slice(&reqs[i].body).unwrap();
            let text = body["messages"][0]["content"][0]["text"]
                .as_str()
                .unwrap()
                .to_string();
            (body, text)
        };
        let (body, sent) = text(0);
        assert_eq!(body["response_format"]["type"], "json_object");
        assert!(sent.starts_with(prompt), "{sent}");
        assert!(sent.to_ascii_lowercase().contains("json"), "{sent}");
        // Without JSON mode the operator's prompt goes out untouched.
        let (body, sent) = text(1);
        assert!(body.get("response_format").is_none());
        assert_eq!(sent, prompt);
    }

    #[tokio::test]
    async fn one_rate_limit_is_retried_in_the_plugin() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_reply(r#"{"caption":"A cat"}"#)),
            )
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        let docs = plugin(&server)
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        assert_eq!(docs[0].content, "A cat");
        assert_eq!(server.received_requests().await.unwrap().len(), 2);
        // Only the call that answered is metered.
        assert_eq!(ctx.usage().llm_requests, 1);
        assert_eq!(ctx.usage().images, 1);
    }

    #[tokio::test]
    async fn server_error_is_retryable_and_auth_error_is_not() {
        let server = MockServer::start().await;
        // Enough 500s to spend the in-plugin retry budget of the first run.
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .up_to_n_times(u64::from(RetryPolicy::IMMEDIATE.attempts))
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(403))
            .mount(&server)
            .await;
        let p = plugin(&server);
        let err = p
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::Retryable(m) if m.contains("500")),
            "{err:?}"
        );
        let err = p
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("403")),
            "{err:?}"
        );
        assert!(!err.to_string().contains("test-key"));
    }

    #[tokio::test]
    async fn usage_records_tokens_and_one_image() {
        let server = MockServer::start().await;
        let mut reply = chat_reply(r#"{"caption":"A cat on a sofa"}"#);
        reply["usage"] = serde_json::json!({
            "prompt_tokens": 1105, "completion_tokens": 37, "total_tokens": 1142
        });
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(reply))
            .expect(1)
            .mount(&server)
            .await;

        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap();

        let usage = ctx.usage();
        assert_eq!(usage.llm_input_tokens, 1105);
        assert_eq!(usage.llm_output_tokens, 37);
        assert_eq!(usage.llm_requests, 1);
        assert_eq!(usage.images, 1);
    }

    #[tokio::test]
    async fn provider_calls_are_priced_from_the_cost_table() {
        let server = MockServer::start().await;
        let mut reply = chat_reply(r#"{"caption":"A cat on a sofa"}"#);
        reply["usage"] = serde_json::json!({
            "prompt_tokens": 1105, "completion_tokens": 37, "total_tokens": 1142
        });
        Mock::given(method("POST"))
            .and(path("/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(reply))
            .expect(2)
            .mount(&server)
            .await;

        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, image(), serde_json::json!({"model": "gpt-4o-mini"}))
            .await
            .unwrap();
        let usage = ctx.usage();
        // 1105 * 0.15 + 37 * 0.6 = 165.75 + 22.2 -> 188 micro-USD
        assert_eq!(usage.cost_micro_usd, 188);
        assert_eq!(usage.unpriced_calls, 0);

        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(
            ctx.usage().unpriced_calls,
            1,
            "test-model is not in the table"
        );
    }

    #[tokio::test]
    async fn usage_without_tokens_still_counts_the_image_and_the_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("a caption")))
            .expect(1)
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap();
        let usage = ctx.usage();
        assert_eq!(usage.images, 1);
        assert_eq!(usage.llm_requests, 1);
        assert_eq!(usage.llm_input_tokens, 0);
    }

    #[tokio::test]
    async fn a_failed_call_records_no_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(ctx.usage().is_empty());
    }

    #[tokio::test]
    async fn disabled_plugin_and_manifest() {
        let p = ImageCaptionerPlugin::disabled();
        assert!(!p.is_enabled());
        let err = p
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::InvalidConfig(m) if m.contains("LLM_API_KEY")));
        let m = p.manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.content_types, vec!["image/*"]);
        let err = p
            .execute(
                &ActivityContext::noop(),
                image(),
                serde_json::json!({"detail": "ultra"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)));
        assert!(!format!("{p:?}").contains("test-key"));
    }

    // -- Behind the Lab: the target on the context wins over the env client --------------

    fn lab_target(server: &MockServer) -> AiTarget {
        AiTarget::new(
            format!("{}/v1", server.uri()),
            "lab-key",
            "key-1",
            "lab-model",
            "lab-vision",
            Some("lab-decision".to_string()),
        )
    }

    fn ctx_with_target(target: AiTarget) -> ActivityContext {
        let ctx = ActivityContext::noop();
        ctx.set_ai_target(target);
        ctx
    }

    #[tokio::test]
    async fn image_captioner_uses_the_lab_target_over_env() {
        let env = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .expect(0)
            .mount(&env)
            .await;
        let lab = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("authorization", "Bearer lab-key"))
            .and(body_partial_json(
                serde_json::json!({"model": "lab-vision"}),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_reply(r#"{"caption":"A cat"}"#)),
            )
            .expect(1)
            .mount(&lab)
            .await;

        let ctx = ctx_with_target(lab_target(&lab));
        let docs = plugin(&env)
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        assert_eq!(docs[0].content, "A cat");
    }

    #[tokio::test]
    async fn image_captioner_step_model_wins_over_the_lab_vision_model() {
        let lab = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(
                serde_json::json!({"model": "step-model"}),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .expect(1)
            .mount(&lab)
            .await;
        let ctx = ctx_with_target(lab_target(&lab));
        ImageCaptionerPlugin::disabled()
            .execute(&ctx, image(), serde_json::json!({"model": "step-model"}))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn image_captioner_disabled_works_with_a_lab_target() {
        let lab = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(header("authorization", "Bearer lab-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(chat_reply("{}")))
            .expect(1)
            .mount(&lab)
            .await;
        let ctx = ctx_with_target(lab_target(&lab));
        ImageCaptionerPlugin::disabled()
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap();
    }

    /// A keep-alive HTTP/1.1 server that answers every request with `reply` and counts
    /// the TCP connections it accepted. wiremock does not expose connection counts.
    async fn counting_server(reply: serde_json::Value) -> (String, Arc<AtomicUsize>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accepted = Arc::new(AtomicUsize::new(0));
        let body = reply.to_string();
        let count = accepted.clone();
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                count.fetch_add(1, Ordering::SeqCst);
                let body = body.clone();
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut chunk = [0u8; 4096];
                    loop {
                        // One request: headers, then `Content-Length` bytes of body.
                        let head_end = loop {
                            if let Some(i) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                                break i + 4;
                            }
                            match sock.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        };
                        let head = String::from_utf8_lossy(&buf[..head_end]).to_ascii_lowercase();
                        let len: usize = head
                            .lines()
                            .find_map(|l| l.strip_prefix("content-length:"))
                            .map_or(0, |v| v.trim().parse().unwrap());
                        while buf.len() < head_end + len {
                            match sock.read(&mut chunk).await {
                                Ok(0) | Err(_) => return,
                                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                            }
                        }
                        buf.drain(..head_end + len);
                        let resp = format!(
                            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{body}",
                            body.len()
                        );
                        if sock.write_all(resp.as_bytes()).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (format!("http://{addr}/v1"), accepted)
    }

    #[tokio::test]
    async fn a_disabled_plugin_reuses_its_lab_connection_across_steps() {
        let (base_url, accepted) = counting_server(chat_reply(r#"{"caption":"A cat"}"#)).await;
        let target = AiTarget::new(
            base_url,
            "lab-key",
            "key-1",
            "lab-model",
            "lab-vision",
            None,
        );
        let p = ImageCaptionerPlugin::disabled();
        for _ in 0..2 {
            // Each image is its own activity, with a fresh context.
            p.execute(
                &ctx_with_target(target.clone()),
                image(),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        }
        assert_eq!(accepted.load(Ordering::SeqCst), 1, "one pooled connection");
    }

    #[tokio::test]
    async fn image_captioner_disabled_without_target_names_the_env_key() {
        let err = ImageCaptionerPlugin::disabled()
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidConfig(m) if m.contains("LLM_API_KEY")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn image_captioner_lab_401_marks_the_target_rejected() {
        let lab = MockServer::start().await;
        // Only a fresh key helps, which the worker fetches: never retried in-plugin.
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(401).set_body_string(r#"{"error":{"code":"LM-4004"}}"#),
            )
            .expect(1)
            .mount(&lab)
            .await;
        let ctx = ctx_with_target(lab_target(&lab));
        let err = ImageCaptionerPlugin::disabled()
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(err.is_retryable(), "{err:?}");
        assert!(ctx.ai_target().unwrap().is_rejected());
        assert!(!err.to_string().contains("lab-key"));
    }

    #[tokio::test]
    async fn image_captioner_lab_402_is_final() {
        let lab = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(402).set_body_string("no budget"))
            .mount(&lab)
            .await;
        let ctx = ctx_with_target(lab_target(&lab));
        let err = ImageCaptionerPlugin::disabled()
            .execute(&ctx, image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("budget is exhausted")),
            "{err:?}"
        );
        assert!(!ctx.ai_target().unwrap().is_rejected());
    }

    #[tokio::test]
    async fn image_captioner_env_401_is_unchanged() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("bad key"))
            .mount(&server)
            .await;
        let err = plugin(&server)
            .execute(&ActivityContext::noop(), image(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("LLM_API_KEY")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_direct_call_to_the_vendor_drops_its_gateway_prefix() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(body_partial_json(serde_json::json!({"model": "gpt-4o"})))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_reply(r#"{"caption":"A cat"}"#)),
            )
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::builder()
            .resolve("api.openai.com", *server.address())
            .build()
            .unwrap();
        let base = format!("http://api.openai.com:{}/v1", server.address().port());
        ImageCaptionerPlugin::with_client(base, "test-key", "vision-model", http)
            .execute(
                &ActivityContext::noop(),
                image(),
                serde_json::json!({"model": "openai/gpt-4o"}),
            )
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn the_lab_gateway_gets_the_prefixed_id() {
        let lab = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(body_partial_json(
                serde_json::json!({"model": "openai/gpt-4o"}),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(chat_reply(r#"{"caption":"A cat"}"#)),
            )
            .expect(1)
            .mount(&lab)
            .await;
        let ctx = ctx_with_target(lab_target(&lab));
        ImageCaptionerPlugin::disabled()
            .execute(&ctx, image(), serde_json::json!({"model": "openai/gpt-4o"}))
            .await
            .unwrap();
    }
}
