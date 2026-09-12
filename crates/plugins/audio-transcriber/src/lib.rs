//! # `whisper_transcriber`
//!
//! Turns spoken audio into searchable documents by POSTing the bytes to an
//! **OpenAI-compatible `/audio/transcriptions` endpoint**.
//!
//! ## Why HTTP and not gRPC
//!
//! SPEC §15 item 21 assumed `whisper_transcriber` would be a GPU gRPC container living
//! outside this repo. This crate implements it instead as a pure-Rust client of the
//! OpenAI audio-transcription API shape, which is spoken by OpenAI, Groq and by
//! self-hosted `whisper.cpp` (`whisper-server`) and `faster-whisper` /
//! `speaches` servers. That keeps the plugin a normal built-in — no CUDA in the worker
//! image, no extra deployment — while anyone who prefers a local GPU container can
//! still point `TRANSCRIBE_BASE_URL` at it, or register a gRPC plugin under the same
//! name through `plugin-runtime`. The gRPC path is therefore *available*, not required.
//!
//! ## Pipeline usage
//!
//! ```yaml
//! - id: transcribe
//!   plugin: whisper_transcriber
//!   config:
//!     language: en
//!     segment_documents: true
//! ```
//!
//! ## Environment
//!
//! | var | required | default |
//! |---|---|---|
//! | `TRANSCRIBE_API_KEY`, else `LLM_API_KEY` | yes | — |
//! | `TRANSCRIBE_BASE_URL`, else `LLM_BASE_URL` | no | `https://api.openai.com/v1` |
//! | `TRANSCRIBE_MODEL` | no | `whisper-1` |
//!
//! ## Input
//!
//! One [`Blob`] whose MIME is `audio/*` or `video/*`. A video blob is tolerated because
//! several endpoints happily demux a container themselves, but the normal pipeline puts
//! `video_audio_extractor` in front so only the audio track travels over the wire.
//!
//! ## Output
//!
//! With `segment_documents: false` (default) a single [`Document`] whose `content` is
//! the whole transcript. With `segment_documents: true` the request asks for
//! `verbose_json` and one document is emitted per segment, carrying `fields.start` /
//! `fields.end` (seconds), `meta.chunk_index` / `meta.chunk_total` / `meta.parent_id`
//! and the id `<stem>_t<index>`. An endpoint that ignores `verbose_json` and returns no
//! segments degrades to the single-document form instead of failing.
//!
//! `meta.source` / `meta.filename` / `meta.mime` come from the blob and `meta.language`
//! from the response (falling back to the configured `language`).
//!
//! ## Usage reporting
//!
//! Every successful call reports one `external_requests`. The seconds of audio it was
//! billed for come from the response's `duration` (`verbose_json` only), or — when the
//! response format does not carry one — from the exact header of an uncompressed WAV
//! payload. For a compressed payload with no `duration` the seconds are genuinely
//! unknowable and are reported as zero rather than guessed from the byte length; see
//! `transcription_usage`.

#![forbid(unsafe_code)]

use std::time::Duration;

use meili_ingest_plugin_sdk::prelude::*;
use serde::{Deserialize, Serialize};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "whisper_transcriber";

/// Default OpenAI-compatible base URL.
pub const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";
/// Default transcription model.
pub const DEFAULT_MODEL: &str = "whisper-1";
/// Default per-request timeout, in seconds. Transcription is slow.
pub const DEFAULT_TIMEOUT_SECS: u64 = 600;
/// Default upload ceiling: the 25 MiB limit enforced by the OpenAI endpoint.
pub const DEFAULT_MAX_BYTES: usize = 26_214_400;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// The step `config:` block. Every field has a default so `config: {}` is valid;
/// unknown keys are rejected as [`PluginError::InvalidConfig`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WhisperTranscriberConfig {
    /// Transcription model; overrides the plugin default (`TRANSCRIBE_MODEL`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    /// ISO-639-1 hint (`en`, `fr`, ...). Improves accuracy and latency when known.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    /// Optional prompt biasing the decoder (glossary, spelling of proper nouns).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
    /// Sampling temperature forwarded to the endpoint.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f32>,
    /// Emit one document per timestamped segment instead of one per file.
    pub segment_documents: bool,
    /// Per-request HTTP timeout, in seconds.
    pub timeout_secs: u64,
    /// Reject blobs larger than this many bytes before uploading anything.
    pub max_bytes: usize,
}

impl Default for WhisperTranscriberConfig {
    fn default() -> Self {
        Self {
            model: None,
            language: None,
            prompt: None,
            temperature: None,
            segment_documents: false,
            timeout_secs: DEFAULT_TIMEOUT_SECS,
            max_bytes: DEFAULT_MAX_BYTES,
        }
    }
}

impl WhisperTranscriberConfig {
    /// `verbose_json` when segments are requested, `json` otherwise.
    fn response_format(&self) -> &'static str {
        if self.segment_documents {
            "verbose_json"
        } else {
            "json"
        }
    }
}

// ---------------------------------------------------------------------------
// Plugin
// ---------------------------------------------------------------------------

/// Resolved connection details. Never `Debug`-printed as-is (it holds the key).
#[derive(Clone)]
struct TranscribeClient {
    base_url: String,
    api_key: String,
    default_model: String,
    http: reqwest::Client,
}

/// The `whisper_transcriber` plugin. See the crate docs.
///
/// Built with [`WhisperTranscriberPlugin::new`] without `TRANSCRIBE_API_KEY` /
/// `LLM_API_KEY`, the plugin is *disabled*: the manifest is still available (so the
/// worker can advertise it) but `execute` fails with `InvalidConfig`.
#[derive(Clone)]
pub struct WhisperTranscriberPlugin {
    client: Option<TranscribeClient>,
}

impl std::fmt::Debug for WhisperTranscriberPlugin {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut d = f.debug_struct("WhisperTranscriberPlugin");
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

impl Default for WhisperTranscriberPlugin {
    fn default() -> Self {
        Self::new()
    }
}

impl WhisperTranscriberPlugin {
    /// [`Self::from_env`], falling back to a disabled instance when no API key is set.
    pub fn new() -> Self {
        Self::from_env().unwrap_or_else(|_| Self::disabled())
    }

    /// An instance whose `execute` always fails with `InvalidConfig`.
    pub fn disabled() -> Self {
        Self { client: None }
    }

    /// Read `TRANSCRIBE_API_KEY` (falling back to `LLM_API_KEY`, required),
    /// `TRANSCRIBE_BASE_URL` (falling back to `LLM_BASE_URL`) and `TRANSCRIBE_MODEL`.
    pub fn from_env() -> Result<Self, PluginError> {
        let api_key = env_var("TRANSCRIBE_API_KEY")
            .or_else(|| env_var("LLM_API_KEY"))
            .ok_or_else(|| PluginError::invalid_config(MISSING_KEY))?;
        let base_url = env_var("TRANSCRIBE_BASE_URL")
            .or_else(|| env_var("LLM_BASE_URL"))
            .unwrap_or_else(|| DEFAULT_BASE_URL.to_string());
        let model = env_var("TRANSCRIBE_MODEL").unwrap_or_else(|| DEFAULT_MODEL.to_string());
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_TIMEOUT_SECS))
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
            client: Some(TranscribeClient {
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

/// Message used everywhere the plugin is disabled; kept in one place so tests and
/// operators see the same wording.
const MISSING_KEY: &str = "TRANSCRIBE_API_KEY (or LLM_API_KEY) not set";

fn env_var(name: &str) -> Option<String> {
    std::env::var(name)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

#[async_trait]
impl Plugin for WhisperTranscriberPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Transcribes audio (or an audio-bearing container) through an \
                 OpenAI-compatible /audio/transcriptions endpoint and emits the \
                 transcript as one document, or one document per timestamped segment",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types([
                "audio/mpeg",
                "audio/wav",
                "audio/ogg",
                "audio/mp4",
                "audio/x-wav",
                "audio/webm",
                "video/mp4",
            ])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "model": {
                        "type": "string",
                        "description": "Transcription model; defaults to TRANSCRIBE_MODEL",
                        "default": DEFAULT_MODEL
                    },
                    "language": {
                        "type": "string",
                        "description": "ISO-639-1 language hint (en, fr, ...)"
                    },
                    "prompt": {
                        "type": "string",
                        "description": "Decoder prompt biasing spelling and vocabulary"
                    },
                    "temperature": {
                        "type": "number",
                        "minimum": 0.0,
                        "maximum": 1.0,
                        "description": "Sampling temperature forwarded to the endpoint"
                    },
                    "segment_documents": {
                        "type": "boolean",
                        "default": false,
                        "description": "Emit one document per timestamped segment (requests verbose_json)"
                    },
                    "timeout_secs": {
                        "type": "integer",
                        "minimum": 1,
                        "default": DEFAULT_TIMEOUT_SECS,
                        "description": "Per-request HTTP timeout in seconds"
                    },
                    "max_bytes": {
                        "type": "integer",
                        "minimum": 1,
                        "default": DEFAULT_MAX_BYTES,
                        "description": "Reject larger blobs before uploading (OpenAI caps uploads at 25 MiB)"
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
        let cfg: WhisperTranscriberConfig = serde_json::from_value(config)
            .map_err(|e| PluginError::invalid_config(e.to_string()))?;
        if cfg.timeout_secs == 0 {
            return Err(PluginError::invalid_config("timeout_secs must be >= 1"));
        }
        if cfg.max_bytes == 0 {
            return Err(PluginError::invalid_config("max_bytes must be >= 1"));
        }
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| PluginError::invalid_config(MISSING_KEY))?;

        let blob = input.into_bytes()?;
        let mime = normalize_mime(&blob.mime);
        if !(mime.starts_with("audio/") || mime.starts_with("video/")) {
            return Err(PluginError::invalid_input(format!(
                "{NAME} expects an audio/* (or video/*) blob, got {:?}",
                blob.mime
            )));
        }
        if blob.data.is_empty() {
            return Err(PluginError::invalid_input("empty audio"));
        }
        if blob.data.len() > cfg.max_bytes {
            return Err(PluginError::NonRetryable(format!(
                "{NAME}: {} bytes exceeds max_bytes ({}); split the audio into shorter \
                 parts or point TRANSCRIBE_BASE_URL at a self-hosted endpoint without \
                 the 25 MiB upload limit",
                blob.data.len(),
                cfg.max_bytes
            )));
        }
        ctx.check_cancelled()?;
        ctx.heartbeat(format!("{NAME}: uploading {} bytes", blob.data.len()));

        let model = cfg.model.as_deref().unwrap_or(&client.default_model);
        let form = build_form(&blob, &mime, model, &cfg)?;
        let body = post_transcription(client, form, Duration::from_secs(cfg.timeout_secs)).await?;
        let parsed = parse_transcription(&body)?;

        let TranscriptionResponse {
            text,
            segments,
            language: detected,
            duration,
        } = parsed;
        // The endpoint bills per second of audio, so report what it actually got.
        ctx.record_usage(transcription_usage(duration, &blob.data));
        let language = detected
            .filter(|l| !l.trim().is_empty())
            .or_else(|| cfg.language.clone());
        let full_text = match text {
            Some(t) if !t.trim().is_empty() => t.trim().to_string(),
            _ => join_segments(&segments),
        };
        if full_text.is_empty() && segments.is_empty() {
            return Err(PluginError::non_retryable(format!(
                "{NAME}: transcription response carries neither `text` nor `segments`: {}",
                truncate_chars(&body, 512)
            )));
        }
        ctx.heartbeat(format!(
            "{NAME}: transcribed {} chars",
            full_text.chars().count()
        ));

        let base = base_id(&blob);
        let docs = if cfg.segment_documents && !segments.is_empty() {
            segment_documents(&base, &blob, language.as_deref(), &segments)
        } else {
            let mut doc = Document::with_id(&base, full_text);
            apply_common_meta(&mut doc, &blob, language.as_deref());
            vec![doc]
        };

        tracing::debug!(
            job_id = %ctx.job_id(),
            plugin = NAME,
            documents = docs.len(),
            segmented = cfg.segment_documents,
            "transcribed audio"
        );
        Ok(PluginOutput::Documents(docs))
    }
}

// ---------------------------------------------------------------------------
// Request building
// ---------------------------------------------------------------------------

/// Lowercased MIME with any `; codecs=...` parameters removed.
fn normalize_mime(raw: &str) -> String {
    raw.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

/// A file extension the endpoint will recognise, derived from the MIME type.
fn extension_for_mime(mime: &str) -> &'static str {
    match mime {
        "audio/mpeg" | "audio/mp3" | "audio/x-mpeg" => "mp3",
        "audio/wav" | "audio/x-wav" | "audio/wave" | "audio/vnd.wave" => "wav",
        "audio/ogg" | "audio/vorbis" | "audio/x-ogg" => "ogg",
        "audio/mp4" | "audio/m4a" | "audio/x-m4a" => "m4a",
        "audio/aac" => "aac",
        "audio/flac" | "audio/x-flac" => "flac",
        "audio/webm" => "webm",
        "video/mp4" => "mp4",
        "video/webm" => "webm",
        "video/quicktime" => "mov",
        "video/x-matroska" => "mkv",
        _ => "bin",
    }
}

/// The blob's own filename, or a synthetic `audio.<ext>` derived from the MIME.
fn upload_filename(blob: &Blob, mime: &str) -> String {
    match blob.filename.as_deref().map(str::trim) {
        Some(name) if !name.is_empty() => name.to_string(),
        _ => format!("audio.{}", extension_for_mime(mime)),
    }
}

/// Meilisearch-safe id derived from the filename stem, or a generated one.
fn base_id(blob: &Blob) -> String {
    let stem = blob
        .filename
        .as_deref()
        .map(str::trim)
        .filter(|n| !n.is_empty())
        .map(|name| match name.rsplit_once('.') {
            Some((stem, _)) if !stem.is_empty() => stem,
            _ => name,
        })
        .unwrap_or("");
    // `sanitize_id("")` generates a fresh UUID, which is exactly the fallback we want.
    sanitize_id(stem)
}

/// Assemble the `multipart/form-data` body (`file`, `model`, `response_format` and the
/// optional `language` / `prompt` / `temperature`).
fn build_form(
    blob: &Blob,
    mime: &str,
    model: &str,
    cfg: &WhisperTranscriberConfig,
) -> Result<reqwest::multipart::Form, PluginError> {
    let part = reqwest::multipart::Part::bytes(blob.data.clone())
        .file_name(upload_filename(blob, mime))
        .mime_str(mime)
        .map_err(|e| {
            PluginError::invalid_input(format!("{NAME}: unusable MIME {:?}: {e}", blob.mime))
        })?;
    let mut form = reqwest::multipart::Form::new()
        .part("file", part)
        .text("model", model.to_string())
        .text("response_format", cfg.response_format());
    if let Some(language) = cfg
        .language
        .as_deref()
        .map(str::trim)
        .filter(|l| !l.is_empty())
    {
        form = form.text("language", language.to_string());
    }
    if let Some(prompt) = cfg.prompt.as_deref().filter(|p| !p.trim().is_empty()) {
        form = form.text("prompt", prompt.to_string());
    }
    if let Some(temperature) = cfg.temperature {
        form = form.text("temperature", temperature.to_string());
    }
    Ok(form)
}

// ---------------------------------------------------------------------------
// HTTP (same error semantics as llm_enricher / image_captioner)
// ---------------------------------------------------------------------------

/// POST `{base_url}/audio/transcriptions` and return the raw response body.
///
/// 429 / 5xx / transport / timeout → [`PluginError::Retryable`]; 401 / 403 / 400 and
/// anything else → [`PluginError::NonRetryable`]. The API key is never echoed.
async fn post_transcription(
    client: &TranscribeClient,
    form: reqwest::multipart::Form,
    timeout: Duration,
) -> Result<String, PluginError> {
    let url = format!("{}/audio/transcriptions", client.base_url);
    let resp = client
        .http
        .post(&url)
        .bearer_auth(&client.api_key)
        .timeout(timeout)
        .multipart(form)
        .send()
        .await
        .map_err(|e| {
            PluginError::retryable(format!("transcription request to {url} failed: {e}"))
        })?;
    let status = resp.status();
    let text = resp.text().await.map_err(|e| {
        PluginError::retryable(format!("transcription response body unreadable: {e}"))
    })?;
    if !status.is_success() {
        let snippet = truncate_chars(&text, 512);
        return Err(match status.as_u16() {
            429 | 500..=599 => PluginError::Retryable(format!(
                "transcription API returned HTTP {status}: {snippet}"
            )),
            401 | 403 => PluginError::NonRetryable(format!(
                "transcription API rejected the credentials (HTTP {status}); check \
                 TRANSCRIBE_API_KEY: {snippet}"
            )),
            400 => PluginError::NonRetryable(format!(
                "transcription API rejected the request (HTTP 400): {snippet}"
            )),
            _ => PluginError::NonRetryable(format!(
                "transcription API returned unexpected HTTP {status}: {snippet}"
            )),
        });
    }
    Ok(text)
}

/// The `json` / `verbose_json` response shape.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct TranscriptionResponse {
    #[serde(default)]
    text: Option<String>,
    #[serde(default)]
    segments: Vec<TranscriptionSegment>,
    #[serde(default)]
    language: Option<String>,
    /// Length of the audio in seconds. Only `verbose_json` carries it.
    #[serde(default)]
    duration: Option<f64>,
}

/// One timestamped segment of a `verbose_json` response.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
struct TranscriptionSegment {
    #[serde(default)]
    start: Option<f64>,
    #[serde(default)]
    end: Option<f64>,
    #[serde(default)]
    text: Option<String>,
}

// ---------------------------------------------------------------------------
// Usage
// ---------------------------------------------------------------------------

/// Billable units for one transcription call.
///
/// `verbose_json` reports the audio length as a top-level `duration`, which is the
/// figure the provider bills on, so it is used verbatim when present. The plain `json`
/// response format does not carry it; the only honest fallback is an *uncompressed*
/// payload, whose header states the byte rate exactly (see [`wav_duration_secs`]) —
/// and that is the common case here, since `video_audio_extractor` upstream emits
/// 16 kHz WAV.
///
/// For anything else — MP3, AAC, Opus, a WAV whose header we cannot read — the number
/// of seconds is genuinely unknown: a compressed file's duration cannot be derived
/// from its byte length (variable bitrate makes any such figure wrong, and a wrong
/// figure would silently misbill). Those calls report `external_requests: 1` with
/// zero seconds, so the request is still counted and the seconds are visibly absent
/// rather than invented.
fn transcription_usage(duration: Option<f64>, data: &[u8]) -> UsageUnits {
    let seconds = duration
        .filter(|d| d.is_finite() && *d >= 0.0)
        .or_else(|| wav_duration_secs(data));
    match seconds {
        Some(secs) => UsageUnits::transcription(secs),
        None => UsageUnits {
            external_requests: 1,
            ..Default::default()
        },
    }
}

/// Exact duration of an uncompressed RIFF/WAVE payload, read from its header.
///
/// This is not an estimate: for PCM and IEEE-float WAV the `fmt ` chunk states the
/// byte rate, so `data chunk length / byte rate` is the true length of the audio.
/// Returns `None` for anything that is not such a file — in particular for every
/// compressed format, where byte length says nothing reliable about duration.
fn wav_duration_secs(data: &[u8]) -> Option<f64> {
    /// Uncompressed formats whose `fmt ` byte rate is exact.
    const PCM: u16 = 1;
    const IEEE_FLOAT: u16 = 3;
    const EXTENSIBLE: u16 = 0xFFFE;

    let le_u16 = |b: &[u8]| -> Option<u16> { Some(u16::from_le_bytes([*b.first()?, *b.get(1)?])) };
    let le_u32 = |b: &[u8]| -> Option<u32> {
        Some(u32::from_le_bytes([
            *b.first()?,
            *b.get(1)?,
            *b.get(2)?,
            *b.get(3)?,
        ]))
    };

    if data.len() < 12 || &data[0..4] != b"RIFF" || &data[8..12] != b"WAVE" {
        return None;
    }

    let mut byte_rate: Option<u32> = None;
    let mut data_len: Option<u64> = None;
    let mut pos = 12usize;
    while pos + 8 <= data.len() {
        let id = &data[pos..pos + 4];
        let declared = le_u32(&data[pos + 4..])? as usize;
        let body = &data[pos + 8..];
        // A stream-written WAV can declare a length past what is actually there;
        // clamp so a truncated upload still yields the length of the bytes we hold.
        let len = declared.min(body.len());
        match id {
            b"fmt " if len >= 16 => {
                let format: u16 = le_u16(body)?;
                let uncompressed = match format {
                    PCM | IEEE_FLOAT => true,
                    // WAVE_FORMAT_EXTENSIBLE names its real format in the GUID that
                    // starts 24 bytes into the chunk.
                    EXTENSIBLE if len >= 26 => {
                        matches!(le_u16(&body[24..])?, PCM | IEEE_FLOAT)
                    }
                    _ => false,
                };
                if !uncompressed {
                    return None;
                }
                byte_rate = le_u32(&body[8..]);
            }
            b"data" => data_len = Some(len as u64),
            _ => {}
        }
        // Chunks are word-aligned: an odd length is followed by a pad byte.
        pos += 8 + len + (len & 1);
    }

    match (byte_rate, data_len) {
        (Some(rate), Some(len)) if rate > 0 => Some(len as f64 / f64::from(rate)),
        _ => None,
    }
}

fn parse_transcription(body: &str) -> Result<TranscriptionResponse, PluginError> {
    serde_json::from_str(body).map_err(|e| {
        PluginError::non_retryable(format!(
            "transcription response is not a transcription object ({e}): {}",
            truncate_chars(body, 512)
        ))
    })
}

// ---------------------------------------------------------------------------
// Document building
// ---------------------------------------------------------------------------

fn join_segments(segments: &[TranscriptionSegment]) -> String {
    segments
        .iter()
        .filter_map(|s| s.text.as_deref())
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>()
        .join(" ")
}

fn apply_common_meta(doc: &mut Document, blob: &Blob, language: Option<&str>) {
    doc.meta.source = blob.filename.clone();
    doc.meta.filename = blob.filename.clone();
    doc.meta.mime = Some(blob.mime.clone());
    doc.meta.language = language.map(str::to_string);
}

fn segment_documents(
    base: &str,
    blob: &Blob,
    language: Option<&str>,
    segments: &[TranscriptionSegment],
) -> Vec<Document> {
    let total = segments.len();
    segments
        .iter()
        .enumerate()
        .map(|(index, seg)| {
            let content = seg.text.as_deref().unwrap_or("").trim();
            let mut doc = Document::with_id(format!("{base}_t{index}"), content);
            if let Some(start) = seg.start.and_then(serde_json::Number::from_f64) {
                doc.fields
                    .insert("start".into(), serde_json::Value::Number(start));
            }
            if let Some(end) = seg.end.and_then(serde_json::Number::from_f64) {
                doc.fields
                    .insert("end".into(), serde_json::Value::Number(end));
            }
            doc.meta.chunk_index = Some(index);
            doc.meta.chunk_total = Some(total);
            doc.meta.parent_id = Some(base.to_string());
            apply_common_meta(&mut doc, blob, language);
            doc
        })
        .collect()
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
    use wiremock::matchers::{header, method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    const MP3: &[u8] = b"ID3\x03\x00\x00\x00 fake audio bytes";

    fn plugin(server: &MockServer) -> WhisperTranscriberPlugin {
        WhisperTranscriberPlugin::with_client(
            server.uri(),
            "test-key",
            "whisper-1",
            reqwest::Client::new(),
        )
    }

    fn audio() -> PluginInput {
        PluginInput::Bytes(Blob::new(
            MP3.to_vec(),
            "audio/mpeg",
            Some("team standup.mp3".into()),
        ))
    }

    /// Headers of the named multipart part (everything before the blank line).
    fn part_headers(body: &[u8], name: &str) -> String {
        let s = String::from_utf8_lossy(body).into_owned();
        let start = s
            .find(&format!("name=\"{name}\""))
            .unwrap_or_else(|| panic!("no part named {name} in body"));
        let rest = &s[start..];
        let end = rest
            .find("\r\n\r\n")
            .expect("part has no header terminator");
        rest[..end].to_string()
    }

    /// Body of the named multipart part.
    fn part_value(body: &[u8], name: &str) -> String {
        let s = String::from_utf8_lossy(body).into_owned();
        let start = s
            .find(&format!("name=\"{name}\""))
            .unwrap_or_else(|| panic!("no part named {name} in body"));
        let rest = &s[start..];
        let head_end = rest
            .find("\r\n\r\n")
            .expect("part has no header terminator")
            + 4;
        let value = &rest[head_end..];
        let end = value.find("\r\n--").expect("part is not terminated");
        value[..end].to_string()
    }

    fn has_part(body: &[u8], name: &str) -> bool {
        String::from_utf8_lossy(body).contains(&format!("name=\"{name}\""))
    }

    #[tokio::test]
    async fn happy_path_uploads_multipart_and_uses_the_transcript() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .and(header("authorization", "Bearer test-key"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "text": "  Good morning everyone.  ",
                "language": "english"
            })))
            .expect(1)
            .mount(&server)
            .await;

        let docs = plugin(&server)
            .execute(&ActivityContext::noop(), audio(), serde_json::json!({}))
            .await
            .unwrap()
            .into_documents()
            .unwrap();

        assert_eq!(docs.len(), 1);
        let d = &docs[0];
        assert_eq!(d.id, "team_standup");
        assert_eq!(d.content, "Good morning everyone.");
        assert_eq!(d.meta.mime.as_deref(), Some("audio/mpeg"));
        assert_eq!(d.meta.filename.as_deref(), Some("team standup.mp3"));
        assert_eq!(d.meta.source.as_deref(), Some("team standup.mp3"));
        assert_eq!(d.meta.language.as_deref(), Some("english"));
        assert!(d.meta.chunk_index.is_none());

        let reqs = server.received_requests().await.unwrap();
        assert_eq!(reqs.len(), 1);
        let body = &reqs[0].body;
        assert!(
            reqs[0].headers["content-type"]
                .to_str()
                .unwrap()
                .starts_with("multipart/form-data")
        );
        assert_eq!(part_value(body, "model"), "whisper-1");
        assert_eq!(part_value(body, "response_format"), "json");
        assert!(!has_part(body, "language"));
        assert!(!has_part(body, "prompt"));
        assert!(!has_part(body, "temperature"));
        let file_headers = part_headers(body, "file");
        assert!(
            file_headers.contains("filename=\"team standup.mp3\""),
            "{file_headers}"
        );
        assert!(file_headers.contains("audio/mpeg"), "{file_headers}");
        assert_eq!(part_value(body, "file").as_bytes(), MP3);
    }

    #[tokio::test]
    async fn optional_fields_are_forwarded_and_model_is_overridable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "text": "bonjour" })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let docs = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(MP3.to_vec(), "audio/wav", None)),
                serde_json::json!({
                    "model": "whisper-large-v3",
                    "language": "fr",
                    "prompt": "Meilisearch, glutony",
                    "temperature": 0.25
                }),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        // No filename on the blob: the id is generated and the upload gets a synthetic one.
        assert_eq!(docs[0].content, "bonjour");
        assert_eq!(docs[0].meta.language.as_deref(), Some("fr"));
        assert!(docs[0].meta.filename.is_none());

        let reqs = server.received_requests().await.unwrap();
        let body = &reqs[0].body;
        assert_eq!(part_value(body, "model"), "whisper-large-v3");
        assert_eq!(part_value(body, "language"), "fr");
        assert_eq!(part_value(body, "prompt"), "Meilisearch, glutony");
        assert_eq!(part_value(body, "temperature"), "0.25");
        assert!(
            part_headers(body, "file").contains("filename=\"audio.wav\""),
            "{}",
            part_headers(body, "file")
        );
    }

    #[tokio::test]
    async fn segment_documents_emits_one_document_per_segment() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "text": "Hello there. General Kenobi.",
                "language": "en",
                "segments": [
                    { "id": 0, "start": 0.0, "end": 2.5, "text": " Hello there." },
                    { "id": 1, "start": 2.5, "end": 4.25, "text": " General Kenobi." }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let docs = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                audio(),
                serde_json::json!({ "segment_documents": true }),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();

        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, "team_standup_t0");
        assert_eq!(docs[0].content, "Hello there.");
        assert_eq!(docs[0].fields["start"], serde_json::json!(0.0));
        assert_eq!(docs[0].fields["end"], serde_json::json!(2.5));
        assert_eq!(docs[0].meta.chunk_index, Some(0));
        assert_eq!(docs[0].meta.chunk_total, Some(2));
        assert_eq!(docs[0].meta.parent_id.as_deref(), Some("team_standup"));
        assert_eq!(docs[1].id, "team_standup_t1");
        assert_eq!(docs[1].content, "General Kenobi.");
        assert_eq!(docs[1].fields["start"], serde_json::json!(2.5));
        assert_eq!(docs[1].meta.chunk_index, Some(1));
        assert_eq!(docs[1].meta.language.as_deref(), Some("en"));

        let reqs = server.received_requests().await.unwrap();
        assert_eq!(part_value(&reqs[0].body, "response_format"), "verbose_json");
    }

    #[tokio::test]
    async fn verbose_json_without_segments_falls_back_to_one_document() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(
                serde_json::json!({ "text": "one shot transcript", "segments": [] }),
            ))
            .expect(1)
            .mount(&server)
            .await;

        let docs = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                audio(),
                serde_json::json!({ "segment_documents": true }),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "team_standup");
        assert_eq!(docs[0].content, "one shot transcript");
        assert!(docs[0].meta.chunk_index.is_none());
    }

    #[tokio::test]
    async fn rate_limited_response_is_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .expect(1)
            .mount(&server)
            .await;
        let err = plugin(&server)
            .execute(&ActivityContext::noop(), audio(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::Retryable(m) if m.contains("429") && m.contains("slow down")),
            "{err:?}"
        );
        assert!(err.is_retryable());
    }

    #[tokio::test]
    async fn auth_error_is_non_retryable_and_never_echoes_the_key() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(401).set_body_string("invalid api key"))
            .expect(1)
            .mount(&server)
            .await;
        let err = plugin(&server)
            .execute(&ActivityContext::noop(), audio(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("401")),
            "{err:?}"
        );
        assert!(!err.to_string().contains("test-key"));
        assert!(!err.is_retryable());
    }

    #[tokio::test]
    async fn unparseable_body_is_non_retryable() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_string("<html>gateway</html>"))
            .expect(1)
            .mount(&server)
            .await;
        let err = plugin(&server)
            .execute(&ActivityContext::noop(), audio(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("<html>gateway</html>")),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn non_audio_mime_is_rejected_without_any_request() {
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
        // Documents input is not accepted either.
        let err = p
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err:?}");
    }

    #[tokio::test]
    async fn oversized_blob_is_rejected_without_any_request() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        let err = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                audio(),
                serde_json::json!({ "max_bytes": 4 }),
            )
            .await
            .unwrap_err();
        let PluginError::NonRetryable(msg) = &err else {
            panic!("expected NonRetryable, got {err:?}");
        };
        assert!(msg.contains(&MP3.len().to_string()), "{msg}");
        assert!(msg.contains("split"), "{msg}");
    }

    #[tokio::test]
    async fn disabled_plugin_reports_invalid_config_and_hides_nothing() {
        let p = WhisperTranscriberPlugin::disabled();
        assert!(!p.is_enabled());
        let err = p
            .execute(&ActivityContext::noop(), audio(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidConfig(m) if m.contains("TRANSCRIBE_API_KEY")),
            "{err:?}"
        );
        assert!(format!("{p:?}").contains("enabled"));

        // Unknown config keys are refused before anything else.
        let err = p
            .execute(
                &ActivityContext::noop(),
                audio(),
                serde_json::json!({ "segments": true }),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err:?}");
    }

    /// A 16 kHz mono 16-bit PCM WAV of `secs` seconds (silence: only the header and
    /// the data length matter here).
    fn wav_bytes(secs: f64) -> Vec<u8> {
        const RATE: u32 = 16_000;
        const BLOCK_ALIGN: u32 = 2; // mono, 16-bit
        let byte_rate = RATE * BLOCK_ALIGN;
        let data_len = (secs * f64::from(byte_rate)) as u32;
        let mut out = Vec::with_capacity(44 + data_len as usize);
        out.extend_from_slice(b"RIFF");
        out.extend_from_slice(&(36 + data_len).to_le_bytes());
        out.extend_from_slice(b"WAVEfmt ");
        out.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
        out.extend_from_slice(&1u16.to_le_bytes()); // PCM
        out.extend_from_slice(&1u16.to_le_bytes()); // channels
        out.extend_from_slice(&RATE.to_le_bytes());
        out.extend_from_slice(&byte_rate.to_le_bytes());
        out.extend_from_slice(&(BLOCK_ALIGN as u16).to_le_bytes());
        out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
        out.extend_from_slice(b"data");
        out.extend_from_slice(&data_len.to_le_bytes());
        out.resize(44 + data_len as usize, 0);
        out
    }

    async fn usage_for(
        response: serde_json::Value,
        blob: Blob,
        cfg: serde_json::Value,
    ) -> UsageUnits {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(ResponseTemplate::new(200).set_body_json(response))
            .expect(1)
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, PluginInput::Bytes(blob), cfg)
            .await
            .unwrap();
        ctx.usage()
    }

    #[tokio::test]
    async fn duration_in_the_response_is_recorded_as_audio_seconds() {
        let usage = usage_for(
            serde_json::json!({
                "text": "Hello there. General Kenobi.",
                "language": "en",
                "duration": 137.25,
                "segments": [{ "id": 0, "start": 0.0, "end": 137.25, "text": "Hello there." }]
            }),
            Blob::new(MP3.to_vec(), "audio/mpeg", Some("call.mp3".into())),
            serde_json::json!({ "segment_documents": true }),
        )
        .await;
        assert_eq!(usage.audio_seconds, 137.25);
        assert_eq!(usage.external_requests, 1);
        assert_eq!(usage.llm_requests, 0);
    }

    #[tokio::test]
    async fn a_wav_upload_without_a_duration_is_measured_from_its_header() {
        // `json` responses carry no duration, but an uncompressed payload states its
        // own byte rate, so the seconds are exact rather than estimated.
        let wav = wav_bytes(3.5);
        let usage = usage_for(
            serde_json::json!({ "text": "three and a half seconds" }),
            Blob::new(wav, "audio/wav", Some("clip.wav".into())),
            serde_json::json!({}),
        )
        .await;
        assert!(
            (usage.audio_seconds - 3.5).abs() < 1e-9,
            "got {}",
            usage.audio_seconds
        );
        assert_eq!(usage.external_requests, 1);
    }

    #[tokio::test]
    async fn a_compressed_upload_without_a_duration_counts_the_request_only() {
        // MP3 byte length says nothing about duration under variable bitrate, so the
        // seconds stay at zero instead of being invented.
        let usage = usage_for(
            serde_json::json!({ "text": "some words" }),
            Blob::new(MP3.to_vec(), "audio/mpeg", Some("x.mp3".into())),
            serde_json::json!({}),
        )
        .await;
        assert_eq!(usage.audio_seconds, 0.0);
        assert_eq!(usage.external_requests, 1);
    }

    #[test]
    fn wav_duration_is_read_only_from_uncompressed_headers() {
        assert_eq!(wav_duration_secs(&wav_bytes(1.0)), Some(1.0));
        assert_eq!(wav_duration_secs(&wav_bytes(0.25)), Some(0.25));
        // Not a RIFF/WAVE file at all.
        assert_eq!(wav_duration_secs(MP3), None);
        assert_eq!(wav_duration_secs(b"RIFF____WAVE"), None);
        assert_eq!(wav_duration_secs(&[]), None);
        // A compressed payload wrapped in RIFF (format 0x0055 = MP3) is refused.
        let mut compressed = wav_bytes(1.0);
        compressed[20..22].copy_from_slice(&0x0055u16.to_le_bytes());
        assert_eq!(wav_duration_secs(&compressed), None);
        // A truncated upload reports the audio it actually holds, never more.
        let mut truncated = wav_bytes(2.0);
        truncated.truncate(44 + 16_000);
        assert_eq!(wav_duration_secs(&truncated), Some(0.5));
    }

    #[tokio::test]
    async fn a_failed_call_records_no_usage() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(429).set_body_string("slow down"))
            .mount(&server)
            .await;
        let ctx = ActivityContext::noop();
        plugin(&server)
            .execute(&ctx, audio(), serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(ctx.usage().is_empty());
    }

    #[tokio::test]
    async fn manifest_shape() {
        let server = MockServer::start().await;
        let p = plugin(&server);
        assert!(p.is_enabled());
        assert!(!format!("{p:?}").contains("test-key"));

        let m = p.manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.version, env!("CARGO_PKG_VERSION"));
        assert!(!m.description.is_empty());
        assert!(m.accepts_kind(InputKind::Bytes));
        assert!(!m.accepts_kind(InputKind::Documents));
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(
            m.content_types,
            vec![
                "audio/mpeg",
                "audio/wav",
                "audio/ogg",
                "audio/mp4",
                "audio/x-wav",
                "audio/webm",
                "video/mp4"
            ]
        );
        let schema = &m.config_schema;
        assert_eq!(schema["additionalProperties"], serde_json::json!(false));
        let props = schema["properties"].as_object().unwrap();
        for key in [
            "model",
            "language",
            "prompt",
            "temperature",
            "segment_documents",
            "timeout_secs",
            "max_bytes",
        ] {
            assert!(props.contains_key(key), "missing schema key {key}");
        }
        assert_eq!(props["timeout_secs"]["default"], serde_json::json!(600));
        assert_eq!(props["max_bytes"]["default"], serde_json::json!(26_214_400));
        assert_eq!(
            props["segment_documents"]["default"],
            serde_json::json!(false)
        );
        // Every schema key round-trips through the config struct.
        let cfg: WhisperTranscriberConfig = serde_json::from_value(serde_json::json!({
            "model": "m", "language": "en", "prompt": "p", "temperature": 0.1,
            "segment_documents": true, "timeout_secs": 30, "max_bytes": 10
        }))
        .unwrap();
        assert_eq!(cfg.response_format(), "verbose_json");
        assert_eq!(
            WhisperTranscriberConfig::default().response_format(),
            "json"
        );
    }
}
