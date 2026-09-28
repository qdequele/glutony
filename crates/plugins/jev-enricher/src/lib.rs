//! # `jev_enricher` — typed classification with TypeSafe's Jev
//!
//! [Jev](https://docs.typesafe.ai/api) is a "System One" decision model: instead of
//! generating prose it answers a set of named, typed questions about a piece of
//! content, each with a calibrated confidence. This plugin sends every document to
//! `POST {base_url}/systemone` with the questions from the step config and writes
//! each answer into `Document::fields` under the question's key.
//!
//! It follows the built-in plugin conventions of `llm_enricher` (the canonical
//! example): env vars read only in [`JevEnricherPlugin::from_env`], a typed config
//! with a matching JSON Schema, `Documents` and `Many` input, bounded concurrency with
//! input order preserved, heartbeats, cancellation, per-call usage and precise error
//! mapping.
//!
//! ## Pipeline usage
//!
//! ```yaml
//! - id: classify
//!   plugin: jev_enricher
//!   config:
//!     questions:
//!       category:
//!         type: choice
//!         instructions: "What is this page about?"
//!         criteria: { billing: "Invoices, plans, payments", api: "API reference", guide: "Tutorials" }
//!       is_outdated:
//!         type: noul
//!         instructions: "Does this describe a deprecated feature?"
//!       quality:
//!         type: score
//!         instructions: "How complete is this documentation?"
//!         criteria: ["stub", "partial", "complete"]
//!     include_confidence: true
//! ```
//!
//! ## Environment
//!
//! | var | required | default |
//! |---|---|---|
//! | `TYPESAFE_API_KEY` | yes | — |
//! | `TYPESAFE_BASE_URL` | no | `https://api.typesafe.ai/v1` |
//! | `JEV_MODEL` | no | `jev-latest` |
//!
//! ## Answer mapping
//!
//! | question `type` | `fields.<key>` |
//! |---|---|
//! | `choice` | the chosen option key (string) |
//! | `noul` | `true` when Jev's probability of "yes" is `>= noul_threshold`, else `false` |
//! | `score` | Jev's probability-weighted position on the `criteria` scale (number) |
//!
//! With `include_confidence: true`, the answer's `confidence` is also written to
//! `fields.<key>_confidence` whenever Jev returns one. Under `merge_strategy: merge`
//! (the default) existing fields are kept; `replace` overwrites them.
//!
//! The state sent to Jev is the document's title (when set) and content, truncated to
//! `max_input_chars`. Jev's answers are schema-constrained, so a reply that lacks one
//! of the configured questions is a protocol violation and fails the step.

#![forbid(unsafe_code)]

use std::collections::BTreeMap;
use std::time::Duration;

use futures::StreamExt;
use meili_ingest_plugin_sdk::prelude::*;
use serde::{Deserialize, Serialize};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "jev_enricher";

/// Default TypeSafe API base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.typesafe.ai/v1";
/// Default model: TypeSafe's alias that tracks the latest Jev release.
pub const DEFAULT_MODEL: &str = "jev-latest";
/// HTTP timeout of the default client built by [`JevEnricherPlugin::from_env`].
/// Jev answers in well under a second; this only bounds a stuck connection.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// Most options Jev accepts in one `choice` question.
pub const MAX_CHOICE_OPTIONS: usize = 255;
/// Fewest and most levels Jev accepts in one `score` rubric.
pub const SCORE_LEVELS: std::ops::RangeInclusive<usize> = 2..=10;

const MISSING_KEY: &str = "TYPESAFE_API_KEY not set";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// How answers are written into the document.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MergeStrategy {
    /// Keep existing fields; only fill gaps.
    #[default]
    Merge,
    /// Jev's answers overwrite existing fields.
    Replace,
}

/// Descriptions of what "yes" and "no" mean for a `noul` question.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NoulCriteria {
    #[serde(rename = "true")]
    pub yes: String,
    #[serde(rename = "false")]
    pub no: String,
}

/// One typed question, serialized exactly as Jev's `questions.<key>` object.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum Question {
    /// Yes/no: "does this condition hold?".
    Noul {
        instructions: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        criteria: Option<NoulCriteria>,
    },
    /// Pick one option; `criteria` maps each option key to its description.
    Choice {
        instructions: String,
        criteria: BTreeMap<String, String>,
    },
    /// Rate on an ordered scale; `criteria` lists the levels, lowest first.
    Score {
        instructions: String,
        criteria: Vec<String>,
    },
}

impl Question {
    fn instructions(&self) -> &str {
        match self {
            Question::Noul { instructions, .. }
            | Question::Choice { instructions, .. }
            | Question::Score { instructions, .. } => instructions,
        }
    }

    fn validate(&self, key: &str) -> Result<(), PluginError> {
        if self.instructions().trim().is_empty() {
            return Err(PluginError::invalid_config(format!(
                "questions.{key}.instructions must not be empty"
            )));
        }
        match self {
            Question::Noul { .. } => {}
            Question::Choice { criteria, .. } => {
                if !(2..=MAX_CHOICE_OPTIONS).contains(&criteria.len()) {
                    return Err(PluginError::invalid_config(format!(
                        "questions.{key}.criteria must have 2..={MAX_CHOICE_OPTIONS} options, got {}",
                        criteria.len()
                    )));
                }
            }
            Question::Score { criteria, .. } => {
                if !SCORE_LEVELS.contains(&criteria.len()) {
                    return Err(PluginError::invalid_config(format!(
                        "questions.{key}.criteria must have {}..={} levels, got {}",
                        SCORE_LEVELS.start(),
                        SCORE_LEVELS.end(),
                        criteria.len()
                    )));
                }
            }
        }
        Ok(())
    }
}

/// The step `config:` block. `questions` is required; everything else has a default.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JevEnricherConfig {
    /// Named questions; each key is also the output field name.
    pub questions: BTreeMap<String, Question>,
    /// Model name; overrides the plugin's default (`JEV_MODEL`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// Also write `<key>_confidence` for every answer Jev reports a confidence for.
    #[serde(default)]
    pub include_confidence: bool,
    /// Probability at or above which a `noul` answer is `true`.
    #[serde(default = "default_noul_threshold")]
    pub noul_threshold: f64,
    /// How answers are written into the document.
    #[serde(default)]
    pub merge_strategy: MergeStrategy,
    /// Maximum in-flight requests for one plugin invocation.
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent: usize,
    /// State longer than this (in chars) is truncated before being sent. The default
    /// stays under Jev's 32k-token context with room for the questions.
    #[serde(default = "default_max_input_chars")]
    pub max_input_chars: usize,
}

fn default_noul_threshold() -> f64 {
    0.5
}
fn default_max_concurrent() -> usize {
    8
}
fn default_max_input_chars() -> usize {
    48_000
}

impl JevEnricherConfig {
    fn validate(&self) -> Result<(), PluginError> {
        if self.questions.is_empty() {
            return Err(PluginError::invalid_config(
                "questions must contain at least one question",
            ));
        }
        for (key, question) in &self.questions {
            if key.trim().is_empty() {
                return Err(PluginError::invalid_config(
                    "question keys must not be empty",
                ));
            }
            question.validate(key)?;
        }
        if !(0.0..=1.0).contains(&self.noul_threshold) {
            return Err(PluginError::invalid_config(
                "noul_threshold must be within 0.0..=1.0",
            ));
        }
        if self.max_concurrent == 0 {
            return Err(PluginError::invalid_config("max_concurrent must be >= 1"));
        }
        if self.max_input_chars == 0 {
            return Err(PluginError::invalid_config("max_input_chars must be >= 1"));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/// Connection details for the TypeSafe API.
#[derive(Clone)]
struct JevClient {
    base_url: String,
    api_key: String,
    default_model: String,
    http: reqwest::Client,
}

/// The `jev_enricher` plugin. See the crate docs.
///
/// A plugin built with [`JevEnricherPlugin::new`] when `TYPESAFE_API_KEY` is unset is
/// *disabled*: its manifest is still available (so `GET /plugins` lists it) but
/// `execute` fails with `InvalidConfig("TYPESAFE_API_KEY not set")`.
#[derive(Clone)]
pub struct JevEnricherPlugin {
    client: Option<JevClient>,
}

impl std::fmt::Debug for JevEnricherPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print the API key.
        let mut d = f.debug_struct("JevEnricherPlugin");
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

impl Default for JevEnricherPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl JevEnricherPlugin {
    /// [`Self::from_env`], falling back to a disabled instance when `TYPESAFE_API_KEY`
    /// is missing. Prefer `from_env()` when you want the error.
    pub fn new() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::disabled())
    }

    /// An instance whose `execute` always fails with `InvalidConfig("TYPESAFE_API_KEY not set")`.
    pub fn disabled() -> Self {
        Self { client: None }
    }

    /// Read `TYPESAFE_API_KEY` (required), `TYPESAFE_BASE_URL`, `JEV_MODEL` from the
    /// environment. This is the only place the crate touches env vars.
    pub fn from_env() -> Result<Self, PluginError> {
        let env_var = |name: &str| std::env::var(name).ok().filter(|v| !v.trim().is_empty());
        let api_key =
            env_var("TYPESAFE_API_KEY").ok_or_else(|| PluginError::invalid_config(MISSING_KEY))?;
        let base_url = env_var("TYPESAFE_BASE_URL").unwrap_or_else(|| DEFAULT_BASE_URL.into());
        let model = env_var("JEV_MODEL").unwrap_or_else(|| DEFAULT_MODEL.into());
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
            client: Some(JevClient {
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

    fn client(&self) -> Result<&JevClient, PluginError> {
        self.client
            .as_ref()
            .ok_or_else(|| PluginError::invalid_config(MISSING_KEY))
    }
}

#[async_trait]
impl Plugin for JevEnricherPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Classifies documents with TypeSafe's Jev decision model (yes/no, choice and score questions) and writes the typed answers into fields",
            )
            .accepts([InputKind::Documents, InputKind::Many])
            .produces(OutputKind::Documents)
            .config_schema(config_schema())
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: serde_json::Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg: JevEnricherConfig = serde_json::from_value(config)
            .map_err(|e| PluginError::invalid_config(e.to_string()))?;
        cfg.validate()?;
        let client = self.client()?;

        let docs = input.into_documents()?;
        let total = docs.len();
        if total == 0 {
            return Ok(PluginOutput::Documents(vec![]));
        }
        tracing::debug!(job_id = %ctx.job_id(), plugin = NAME, documents = total, questions = cfg.questions.len(), "classifying documents");

        // Bounded concurrency; results are re-ordered by their original index.
        let mut slots: Vec<Option<Document>> =
            std::iter::repeat_with(|| None).take(total).collect();
        let mut stream = futures::stream::iter(docs.into_iter().enumerate())
            .map(|(i, doc)| {
                let cfg = &cfg;
                async move { (i, client.enrich(ctx, cfg, doc).await) }
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
                ctx.heartbeat(format!("{NAME}: {done}/{total} documents classified"));
            }
        }

        let out = slots
            .into_iter()
            .map(|s| {
                s.ok_or_else(|| PluginError::non_retryable("internal: missing classified document"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(PluginOutput::Documents(out))
    }
}

/// JSON Schema of [`JevEnricherConfig`], served in the manifest.
fn config_schema() -> serde_json::Value {
    let instructions = serde_json::json!({ "type": "string", "minLength": 1 });
    serde_json::json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["questions"],
        "properties": {
            "questions": {
                "type": "object",
                "description": "Named Jev questions; each key is the output field name",
                "minProperties": 1,
                "additionalProperties": {
                    "oneOf": [
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["type", "instructions"],
                            "properties": {
                                "type": { "const": "noul" },
                                "instructions": instructions,
                                "criteria": {
                                    "type": "object",
                                    "additionalProperties": false,
                                    "required": ["true", "false"],
                                    "properties": {
                                        "true": { "type": "string" },
                                        "false": { "type": "string" }
                                    }
                                }
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["type", "instructions", "criteria"],
                            "properties": {
                                "type": { "const": "choice" },
                                "instructions": instructions,
                                "criteria": {
                                    "type": "object",
                                    "description": "Option key → description",
                                    "minProperties": 2,
                                    "maxProperties": MAX_CHOICE_OPTIONS,
                                    "additionalProperties": { "type": "string" }
                                }
                            }
                        },
                        {
                            "type": "object",
                            "additionalProperties": false,
                            "required": ["type", "instructions", "criteria"],
                            "properties": {
                                "type": { "const": "score" },
                                "instructions": instructions,
                                "criteria": {
                                    "type": "array",
                                    "description": "Scale levels, lowest first",
                                    "minItems": SCORE_LEVELS.start(),
                                    "maxItems": SCORE_LEVELS.end(),
                                    "items": { "type": "string" }
                                }
                            }
                        }
                    ]
                }
            },
            "model": { "type": "string", "description": "Jev model; defaults to JEV_MODEL" },
            "include_confidence": { "type": "boolean", "default": false },
            "noul_threshold": { "type": "number", "minimum": 0.0, "maximum": 1.0, "default": 0.5 },
            "merge_strategy": { "type": "string", "enum": ["merge", "replace"], "default": "merge" },
            "max_concurrent": { "type": "integer", "minimum": 1, "default": 8 },
            "max_input_chars": { "type": "integer", "minimum": 1, "default": 48000 }
        }
    })
}

// ---------------------------------------------------------------------------
// HTTP
// ---------------------------------------------------------------------------

/// The body of a `POST /systemone` reply.
#[derive(Debug, Deserialize)]
struct SystemOneResponse {
    #[serde(default)]
    answers: BTreeMap<String, Answer>,
    #[serde(default)]
    usage: Option<ApiUsage>,
}

/// One entry of `answers`. Only the field matching the question type is read.
#[derive(Debug, Default, Deserialize)]
struct Answer {
    #[serde(default)]
    noul: Option<f64>,
    #[serde(default)]
    choice: Option<String>,
    #[serde(default)]
    score: Option<f64>,
    #[serde(default)]
    confidence: Option<f64>,
}

#[derive(Debug, Clone, Copy, Default, Deserialize)]
struct ApiUsage {
    #[serde(default)]
    input_tokens: u64,
    #[serde(default)]
    output_tokens: u64,
}

impl JevClient {
    /// Ask Jev about one document and write its answers into it.
    ///
    /// Records the call's usage on `ctx` before returning; the accumulator behind
    /// `ctx` is shared by clones, so concurrent documents sum into one total.
    async fn enrich(
        &self,
        ctx: &ActivityContext,
        cfg: &JevEnricherConfig,
        mut doc: Document,
    ) -> Result<Document, PluginError> {
        let body = serde_json::json!({
            "model": cfg.model.as_deref().unwrap_or(&self.default_model),
            "state": state_for(&doc, cfg.max_input_chars),
            "questions": cfg.questions,
        });
        // A failed call records nothing: `?` returns before `record_usage`.
        let reply = self.systemone(&body).await?;
        ctx.record_usage(match reply.usage {
            Some(u) => UsageUnits::llm(u.input_tokens, u.output_tokens),
            None => UsageUnits {
                llm_requests: 1,
                ..Default::default()
            },
        });
        apply_answers(&mut doc, cfg, &reply.answers)?;
        Ok(doc)
    }

    /// POST `{base_url}/systemone`.
    ///
    /// 429 / 5xx (incl. Jev's 529 "overloaded") / transport errors → `Retryable`;
    /// 401 / 403 → `NonRetryable` (credentials); 400 / 422 → `NonRetryable` with the
    /// body; other statuses → `NonRetryable`.
    async fn systemone(&self, body: &serde_json::Value) -> Result<SystemOneResponse, PluginError> {
        let url = format!("{}/systemone", self.base_url);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .map_err(|e| PluginError::retryable(format!("Jev request to {url} failed: {e}")))?;

        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| PluginError::retryable(format!("Jev response body unreadable: {e}")))?;
        if !status.is_success() {
            return Err(map_http_failure(status.as_u16(), &text));
        }
        serde_json::from_str(&text).map_err(|e| {
            PluginError::non_retryable(format!(
                "Jev response is not a systemone reply ({e}): {}",
                truncate_chars(&text, 512)
            ))
        })
    }
}

fn map_http_failure(code: u16, body: &str) -> PluginError {
    let snippet = truncate_chars(body, 512);
    match code {
        429 | 500..=599 => {
            PluginError::Retryable(format!("Jev API returned HTTP {code}: {snippet}"))
        }
        401 | 403 => PluginError::NonRetryable(format!(
            "Jev API rejected the credentials (HTTP {code}); check TYPESAFE_API_KEY: {snippet}"
        )),
        400 | 422 => PluginError::NonRetryable(format!(
            "Jev API rejected the request (HTTP {code}): {snippet}"
        )),
        _ => PluginError::NonRetryable(format!(
            "Jev API returned unexpected HTTP {code}: {snippet}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Answer handling
// ---------------------------------------------------------------------------

/// The text Jev evaluates: the title (when set) and content, truncated to `max` chars.
pub fn state_for(doc: &Document, max: usize) -> String {
    let full = match doc.title.as_deref().filter(|t| !t.trim().is_empty()) {
        Some(title) => format!("{title}\n\n{}", doc.content),
        None => doc.content.clone(),
    };
    truncate_chars(&full, max).to_string()
}

/// Write one value per configured question (plus confidences when asked) into `doc`.
fn apply_answers(
    doc: &mut Document,
    cfg: &JevEnricherConfig,
    answers: &BTreeMap<String, Answer>,
) -> Result<(), PluginError> {
    let replace = cfg.merge_strategy == MergeStrategy::Replace;
    let mut write = |key: String, value: serde_json::Value| {
        if replace {
            doc.fields.insert(key, value);
        } else {
            doc.fields.entry(key).or_insert(value);
        }
    };
    for (key, question) in &cfg.questions {
        let answer = answers.get(key).ok_or_else(|| {
            PluginError::non_retryable(format!("Jev response has no answer for question {key:?}"))
        })?;
        let value = match question {
            Question::Noul { .. } => answer
                .noul
                .map(|p| serde_json::Value::Bool(p >= cfg.noul_threshold)),
            Question::Choice { .. } => answer.choice.clone().map(serde_json::Value::String),
            Question::Score { .. } => answer.score.and_then(json_number),
        }
        .ok_or_else(|| {
            PluginError::non_retryable(format!(
                "Jev answer for question {key:?} is missing its value"
            ))
        })?;
        write(key.clone(), value);
        if cfg.include_confidence
            && let Some(c) = answer.confidence.and_then(json_number)
        {
            write(format!("{key}_confidence"), c);
        }
    }
    Ok(())
}

fn json_number(n: f64) -> Option<serde_json::Value> {
    serde_json::Number::from_f64(n).map(serde_json::Value::Number)
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

    fn plugin(server: &MockServer) -> JevEnricherPlugin {
        JevEnricherPlugin::with_client(server.uri(), "test-key", "jev-test", reqwest::Client::new())
    }

    /// A config asking one question of each type.
    fn three_questions() -> serde_json::Value {
        serde_json::json!({
            "questions": {
                "category": {
                    "type": "choice",
                    "instructions": "What is this page about?",
                    "criteria": { "billing": "Invoices", "api": "API reference" }
                },
                "is_outdated": {
                    "type": "noul",
                    "instructions": "Is this deprecated?",
                    "criteria": { "true": "Deprecated", "false": "Current" }
                },
                "quality": {
                    "type": "score",
                    "instructions": "How complete is it?",
                    "criteria": ["stub", "partial", "complete"]
                }
            }
        })
    }

    fn three_answers() -> serde_json::Value {
        serde_json::json!({
            "model": "jev-1.13.0",
            "answers": {
                "category": { "type": "choice", "choice": "api",
                              "probabilities": { "api": 0.9, "billing": 0.1 }, "confidence": 0.9 },
                "is_outdated": { "type": "noul", "noul": 0.2 },
                "quality": { "type": "score", "score": 1.5, "confidence": 0.7 }
            },
            "usage": { "input_tokens": 296, "output_tokens": 20 }
        })
    }

    fn with_config(extra: serde_json::Value) -> serde_json::Value {
        let mut cfg = three_questions();
        for (k, v) in extra.as_object().unwrap() {
            cfg[k] = v.clone();
        }
        cfg
    }

    async fn run(
        p: &JevEnricherPlugin,
        docs: Vec<Document>,
        cfg: serde_json::Value,
    ) -> Result<Vec<Document>, PluginError> {
        p.execute(&ActivityContext::noop(), PluginInput::Documents(docs), cfg)
            .await?
            .into_documents()
    }

    #[tokio::test]
    async fn happy_path_sends_typed_questions_and_writes_answers() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/systemone"))
            .and(header("authorization", "Bearer test-key"))
            .and(body_partial_json(serde_json::json!({
                "model": "jev-test",
                "state": "Old API\n\nThe v1 endpoint",
                "questions": {
                    "category": {
                        "type": "choice",
                        "instructions": "What is this page about?",
                        "criteria": { "billing": "Invoices", "api": "API reference" }
                    },
                    "is_outdated": {
                        "type": "noul",
                        "criteria": { "true": "Deprecated", "false": "Current" }
                    },
                    "quality": { "type": "score", "criteria": ["stub", "partial", "complete"] }
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
            .expect(1)
            .mount(&server)
            .await;

        let mut doc = Document::with_id("d1", "The v1 endpoint");
        doc.title = Some("Old API".into());
        let out = run(&plugin(&server), vec![doc], three_questions())
            .await
            .unwrap();

        let d = &out[0];
        assert_eq!(d.id, "d1");
        assert_eq!(d.content, "The v1 endpoint");
        assert_eq!(d.title.as_deref(), Some("Old API"));
        assert_eq!(d.fields["category"], "api");
        assert_eq!(d.fields["is_outdated"], false);
        assert_eq!(d.fields["quality"], 1.5);
        assert_eq!(d.fields.len(), 3, "no confidences unless asked");
    }

    #[tokio::test]
    async fn noul_threshold_decides_the_boolean() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
            .mount(&server)
            .await;
        let out = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            with_config(serde_json::json!({ "noul_threshold": 0.2 })),
        )
        .await
        .unwrap();
        assert_eq!(out[0].fields["is_outdated"], true, "0.2 >= 0.2");
    }

    #[tokio::test]
    async fn include_confidence_writes_reported_confidences_only() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
            .mount(&server)
            .await;
        let out = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            with_config(serde_json::json!({ "include_confidence": true })),
        )
        .await
        .unwrap();
        let f = &out[0].fields;
        assert_eq!(f["category_confidence"], 0.9);
        assert_eq!(f["quality_confidence"], 0.7);
        assert!(
            !f.contains_key("is_outdated_confidence"),
            "Jev reported none; nothing is invented"
        );
    }

    #[tokio::test]
    async fn merge_keeps_existing_fields_and_replace_overwrites_them() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(body_partial_json(serde_json::json!({ "model": "other" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
            .expect(2)
            .mount(&server)
            .await;
        let mut doc = Document::with_id("a", "x");
        doc.fields
            .insert("category".into(), serde_json::json!("original"));
        let p = plugin(&server);

        let merged = run(
            &p,
            vec![doc.clone()],
            with_config(serde_json::json!({ "model": "other" })),
        )
        .await
        .unwrap();
        assert_eq!(merged[0].fields["category"], "original");
        assert_eq!(merged[0].fields["quality"], 1.5, "gaps are still filled");

        let replaced = run(
            &p,
            vec![doc],
            with_config(serde_json::json!({ "model": "other", "merge_strategy": "replace" })),
        )
        .await
        .unwrap();
        assert_eq!(replaced[0].fields["category"], "api");
    }

    #[tokio::test]
    async fn a_missing_answer_is_non_retryable() {
        let server = MockServer::start().await;
        let mut reply = three_answers();
        reply["answers"].as_object_mut().unwrap().remove("quality");
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(reply))
            .mount(&server)
            .await;
        let err = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            three_questions(),
        )
        .await
        .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("quality")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn rate_limit_and_overload_are_retryable() {
        for code in [429u16, 529, 503] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .respond_with(ResponseTemplate::new(code).set_body_string("slow down"))
                .mount(&server)
                .await;
            let err = run(
                &plugin(&server),
                vec![Document::with_id("a", "x")],
                three_questions(),
            )
            .await
            .unwrap_err();
            assert!(
                matches!(&err, PluginError::Retryable(m) if m.contains(&code.to_string()) && m.contains("slow down")),
                "{code}: {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn auth_and_validation_errors_are_non_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("bad key"))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(
                ResponseTemplate::new(422).set_body_string(r#"{"error":"too many options"}"#),
            )
            .mount(&server)
            .await;
        let p = plugin(&server);
        let err = run(&p, vec![Document::with_id("a", "x")], three_questions())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("401") && m.contains("TYPESAFE_API_KEY")),
            "{err:?}"
        );
        assert!(!err.to_string().contains("test-key"));
        let err = run(&p, vec![Document::with_id("a", "x")], three_questions())
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("too many options")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn a_non_json_reply_is_non_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>proxy</html>"))
            .mount(&server)
            .await;
        let err = run(
            &plugin(&server),
            vec![Document::with_id("a", "x")],
            three_questions(),
        )
        .await
        .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "{err:?}");
    }

    /// Answers with a choice derived from the document content and token counts
    /// derived from its index; early documents answer last, so completion order
    /// differs from input order.
    struct EchoPerDocument;
    impl Respond for EchoPerDocument {
        fn respond(&self, req: &Request) -> ResponseTemplate {
            let body: serde_json::Value = serde_json::from_slice(&req.body).unwrap();
            let state = body["state"].as_str().unwrap();
            let idx: u64 = state.trim_start_matches("doc number ").parse().unwrap();
            ResponseTemplate::new(200)
                .set_delay(Duration::from_millis((25 - idx) * 4))
                .set_body_json(serde_json::json!({
                    "answers": {
                        "parity": { "type": "choice", "choice": if idx.is_multiple_of(2) { "even" } else { "odd" } }
                    },
                    "usage": { "input_tokens": 10 + idx, "output_tokens": 1 }
                }))
        }
    }

    #[tokio::test]
    async fn concurrency_preserves_order_and_usage_sums_into_one_total() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/systemone"))
            .respond_with(EchoPerDocument)
            .expect(25)
            .mount(&server)
            .await;
        let docs: Vec<Document> = (0..25)
            .map(|i| Document::with_id(format!("doc-{i}"), format!("doc number {i}")))
            .collect();
        let ctx = ActivityContext::noop();
        let out = plugin(&server)
            .execute(
                &ctx,
                PluginInput::Documents(docs),
                serde_json::json!({
                    "max_concurrent": 5,
                    "questions": { "parity": {
                        "type": "choice", "instructions": "Even or odd?",
                        "criteria": { "even": "even", "odd": "odd" }
                    } }
                }),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();

        assert_eq!(out.len(), 25);
        for (i, d) in out.iter().enumerate() {
            assert_eq!(d.id, format!("doc-{i}"), "order must be preserved");
            assert_eq!(
                d.fields["parity"],
                if i.is_multiple_of(2) { "even" } else { "odd" }
            );
        }
        let usage = ctx.usage();
        // input: sum(10..=34) = 550, output: 1 per document.
        assert_eq!(usage.llm_input_tokens, 550);
        assert_eq!(usage.llm_output_tokens, 25);
        assert_eq!(usage.llm_requests, 25);
    }

    #[tokio::test]
    async fn missing_usage_counts_the_request_and_failures_record_nothing() {
        let server = MockServer::start().await;
        let mut reply = three_answers();
        reply.as_object_mut().unwrap().remove("usage");
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(reply))
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429))
            .mount(&server)
            .await;
        let p = plugin(&server);

        let ctx = ActivityContext::noop();
        p.execute(
            &ctx,
            PluginInput::Documents(vec![Document::with_id("a", "x")]),
            three_questions(),
        )
        .await
        .unwrap();
        let usage = ctx.usage();
        assert_eq!(usage.llm_requests, 1);
        assert_eq!(usage.llm_input_tokens, 0, "tokens are unknown, not guessed");

        let ctx = ActivityContext::noop();
        p.execute(
            &ctx,
            PluginInput::Documents(vec![Document::with_id("a", "x")]),
            three_questions(),
        )
        .await
        .unwrap_err();
        assert!(ctx.usage().is_empty());
    }

    #[tokio::test]
    async fn many_input_is_flattened_and_state_is_truncated() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
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
                with_config(serde_json::json!({ "max_input_chars": 10 })),
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
            "the document is untouched"
        );

        let sent: Vec<String> = server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .map(|r| {
                let b: serde_json::Value = serde_json::from_slice(&r.body).unwrap();
                b["state"].as_str().unwrap().to_string()
            })
            .collect();
        assert!(sent.contains(&"é".repeat(10)));
        assert!(sent.contains(&"short".to_string()));
    }

    #[tokio::test]
    async fn cancellation_is_observed() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
            .mount(&server)
            .await;
        let (tx, _rx) = tokio::sync::mpsc::unbounded_channel();
        let ctx = ActivityContext::new(
            uuid::Uuid::nil(),
            "classify",
            1,
            tx,
            Arc::new(AtomicBool::new(true)),
        );
        let err = plugin(&server)
            .execute(
                &ctx,
                PluginInput::Documents(vec![Document::with_id("a", "x")]),
                three_questions(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled));
    }

    #[tokio::test]
    async fn disabled_plugin_reports_the_missing_key() {
        let p = JevEnricherPlugin::disabled();
        assert!(!p.is_enabled());
        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                three_questions(),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidConfig(m) if m.contains("TYPESAFE_API_KEY")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn invalid_configs_and_input_are_rejected_before_any_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_json(three_answers()))
            .expect(0)
            .mount(&server)
            .await;
        let p = plugin(&server);
        let doc = || vec![Document::with_id("a", "x")];
        let choice = |n: usize| {
            let criteria: serde_json::Map<String, serde_json::Value> = (0..n)
                .map(|i| (format!("o{i}"), serde_json::json!("opt")))
                .collect();
            serde_json::json!({ "questions": { "c": {
                "type": "choice", "instructions": "pick", "criteria": criteria
            } } })
        };
        let score = |n: usize| {
            serde_json::json!({ "questions": { "s": {
                "type": "score", "instructions": "rate", "criteria": vec!["lvl"; n]
            } } })
        };

        for (label, cfg) in [
            ("no questions field", serde_json::json!({})),
            ("empty questions", serde_json::json!({ "questions": {} })),
            (
                "unknown type",
                serde_json::json!({ "questions": { "q": { "type": "rank", "instructions": "x" } } }),
            ),
            (
                "blank instructions",
                serde_json::json!({ "questions": { "q": { "type": "noul", "instructions": " " } } }),
            ),
            ("choice with 1 option", choice(1)),
            ("choice with 256 options", choice(256)),
            ("score with 1 level", score(1)),
            ("score with 11 levels", score(11)),
            (
                "threshold > 1",
                with_config(serde_json::json!({ "noul_threshold": 1.5 })),
            ),
            (
                "max_concurrent 0",
                with_config(serde_json::json!({ "max_concurrent": 0 })),
            ),
            (
                "max_input_chars 0",
                with_config(serde_json::json!({ "max_input_chars": 0 })),
            ),
            (
                "bad merge strategy",
                with_config(serde_json::json!({ "merge_strategy": "yolo" })),
            ),
            (
                "unknown key",
                with_config(serde_json::json!({ "prompt": "hi" })),
            ),
        ] {
            let err = run(&p, doc(), cfg).await.unwrap_err();
            assert!(
                matches!(err, PluginError::InvalidConfig(_)),
                "{label}: {err:?}"
            );
        }

        // The limits themselves are accepted.
        for cfg in [choice(2), choice(255), score(2), score(10)] {
            let cfg: JevEnricherConfig = serde_json::from_value(cfg).unwrap();
            cfg.validate().unwrap();
        }

        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(vec![1], "application/pdf", None)),
                three_questions(),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)));
        assert!(!format!("{p:?}").contains("test-key"));
    }

    #[test]
    fn manifest_and_helpers() {
        let m = JevEnricherPlugin::disabled().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Documents) && m.accepts_kind(InputKind::Many));
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(
            m.config_schema["required"],
            serde_json::json!(["questions"])
        );
        assert_eq!(
            m.config_schema["properties"]["max_input_chars"]["default"],
            48000
        );

        let cfg: JevEnricherConfig = serde_json::from_value(three_questions()).unwrap();
        assert_eq!(cfg.noul_threshold, 0.5);
        assert_eq!(cfg.max_concurrent, 8);
        assert_eq!(cfg.merge_strategy, MergeStrategy::Merge);
        assert!(!cfg.include_confidence);

        let mut doc = Document::with_id("a", "body");
        assert_eq!(state_for(&doc, 100), "body");
        doc.title = Some("Title".into());
        assert_eq!(state_for(&doc, 100), "Title\n\nbody");
        assert_eq!(state_for(&doc, 3), "Tit");
        assert_eq!(truncate_chars("héllo", 2), "hé");
    }
}
