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
//! | `LLM_API_KEY` | yes | — |
//! | `LLM_BASE_URL` | no | `https://api.openai.com/v1` |
//! | `LLM_MODEL` | no | `gpt-4o-mini` |
//!
//! ## Output
//!
//! One document. With `json: true` (default) the model must reply with
//! `{"caption", "tags", "text_in_image"}`: `caption` → `content`, `tags` →
//! `fields.tags`, `text_in_image` → `fields.ocr_text`, other keys → `fields`. If the
//! reply is not a JSON object the whole text becomes `content`. With `json: false` the
//! model's full text reply is the `content`. `meta.mime` / `meta.filename` /
//! `meta.source` come from the input blob; the id derives from the filename when known.

#![forbid(unsafe_code)]

use std::time::Duration;

use base64::Engine;
use meili_ingest_plugin_sdk::prelude::*;
use serde::{Deserialize, Serialize};

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

impl Default for ImageCaptionerConfig {
    fn default() -> Self {
        Self {
            model: None,
            prompt: default_prompt(),
            detail: Detail::Auto,
            max_tokens: default_max_tokens(),
            json: true,
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
}

/// The `image_captioner` plugin. See the crate docs.
///
/// Built with [`ImageCaptionerPlugin::new`] without `LLM_API_KEY`, the plugin is
/// *disabled*: the manifest is available but `execute` fails with
/// `InvalidConfig("LLM_API_KEY not set")`.
#[derive(Clone)]
pub struct ImageCaptionerPlugin {
    client: Option<LlmClient>,
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
        Self { client: None }
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
            }),
        }
    }

    /// Whether credentials are configured.
    pub fn is_enabled(&self) -> bool {
        self.client.is_some()
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
                    "json": { "type": "boolean", "default": true }
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
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| PluginError::invalid_config("LLM_API_KEY not set"))?;

        let blob = input.into_bytes()?;
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
        ctx.check_cancelled()?;
        ctx.heartbeat(format!(
            "{NAME}: sending {} bytes to the vision model",
            blob.data.len()
        ));

        let data_url = format!(
            "data:{mime};base64,{}",
            base64::engine::general_purpose::STANDARD.encode(&blob.data)
        );
        let mut body = serde_json::json!({
            "model": cfg.model.as_deref().unwrap_or(&client.default_model),
            "max_tokens": cfg.max_tokens,
            "messages": [{
                "role": "user",
                "content": [
                    { "type": "text", "text": cfg.prompt },
                    { "type": "image_url", "image_url": { "url": data_url, "detail": cfg.detail } }
                ]
            }],
        });
        if cfg.json {
            body["response_format"] = serde_json::json!({ "type": "json_object" });
        }

        // A failed call records nothing: `?` returns before `record_usage`.
        let (raw, mut units) =
            chat_completion(&client.http, &client.base_url, &client.api_key, &body).await?;
        // One image went to the vision model, and its tokens are billed on top.
        units.images = 1;
        ctx.record_usage(units);

        let mut doc = match blob.filename.as_deref() {
            Some(name) if !name.is_empty() => Document::with_id(name, ""),
            _ => Document::new(""),
        };
        doc.meta.mime = Some(blob.mime.clone());
        doc.meta.filename = blob.filename.clone();
        doc.meta.source = blob.filename.clone();
        apply_reply(&mut doc, &raw, cfg.json);

        tracing::debug!(job_id = %ctx.job_id(), plugin = NAME, id = %doc.id, "captioned image");
        Ok(PluginOutput::Documents(vec![doc]))
    }
}

// ---------------------------------------------------------------------------
// Reply handling
// ---------------------------------------------------------------------------

/// Map the model reply onto the document (see the crate docs).
pub fn apply_reply(doc: &mut Document, raw: &str, json: bool) {
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
/// 429 / 5xx / transport → `Retryable`; 401 / 403 / 400 / others → `NonRetryable`.
async fn chat_completion(
    http: &reqwest::Client,
    base_url: &str,
    api_key: &str,
    body: &serde_json::Value,
) -> Result<(String, UsageUnits), PluginError> {
    let url = format!("{base_url}/chat/completions");
    let resp = http
        .post(&url)
        .bearer_auth(api_key)
        .json(body)
        .send()
        .await
        .map_err(|e| PluginError::retryable(format!("vision request to {url} failed: {e}")))?;
    let status = resp.status();
    let text = resp
        .text()
        .await
        .map_err(|e| PluginError::retryable(format!("vision response body unreadable: {e}")))?;
    if !status.is_success() {
        let snippet = truncate_chars(&text, 512);
        return Err(match status.as_u16() {
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
        });
    }
    let parsed: ChatCompletion = serde_json::from_str(&text).map_err(|e| {
        PluginError::non_retryable(format!(
            "vision response is not a chat completion ({e}): {}",
            truncate_chars(&text, 512)
        ))
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
            PluginError::non_retryable("vision response has no choices[0].message.content")
        })?;
    Ok((content, usage))
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
    use wiremock::matchers::{body_partial_json, header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const PNG: &[u8] = b"\x89PNG\r\n\x1a\n fake image bytes";

    fn chat_reply(content: &str) -> serde_json::Value {
        serde_json::json!({ "choices": [{ "message": { "role": "assistant", "content": content } }] })
    }

    fn plugin(server: &MockServer) -> ImageCaptionerPlugin {
        ImageCaptionerPlugin::with_client(
            server.uri(),
            "test-key",
            "vision-model",
            reqwest::Client::new(),
        )
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
    async fn server_error_is_retryable_and_auth_error_is_not() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
            .up_to_n_times(1)
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
}
