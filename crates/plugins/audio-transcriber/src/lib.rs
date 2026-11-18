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
//! A blob over `max_bytes` (the 25 MiB OpenAI upload limit by default) is rejected,
//! unless it is an uncompressed WAV, which is what `video_audio_extractor` emits. That
//! one is cut on frame boundaries into consecutive windows that each fit, sent one
//! after the other, and the responses are put back together as if one request had
//! been made: the texts are joined, segment timestamps are offset by the window's
//! start and segment indexes run across windows.
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
//! Every successful call (one per window) reports one `external_requests`. The seconds
//! of audio it was billed for come from the response's `duration` (`verbose_json` only), or — when the
//! response format does not carry one — from the exact header of an uncompressed WAV
//! payload. For a compressed payload with no `duration` the seconds are genuinely
//! unknowable and are reported as zero rather than guessed from the byte length; see
//! `transcription_usage`.

#![forbid(unsafe_code)]

use std::time::Duration;

use meili_ingest_plugin_sdk::cost::ProviderCosts;
use meili_ingest_plugin_sdk::prelude::*;
use meili_ingest_plugin_sdk::vendor::direct_model;
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
    /// Largest upload per request. A larger uncompressed WAV is sent in windows that
    /// fit; any other larger blob is rejected before uploading anything.
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
                        "description": "Largest upload per request (OpenAI caps uploads at 25 MiB). A larger uncompressed WAV is split into windows that fit; any other larger blob is rejected"
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
        // An uncompressed WAV over the limit goes up in windows that each fit; any
        // other payload over it is refused before anything is uploaded.
        let (layout, windows) = if blob.data.len() <= cfg.max_bytes {
            let whole = Window {
                bytes: 0..blob.data.len(),
                start_secs: 0.0,
                secs: wav_duration_secs(&blob.data),
            };
            (None, vec![whole])
        } else if let Some((layout, windows)) = wav_windows(&blob.data, cfg.max_bytes) {
            (Some(layout), windows)
        } else {
            return Err(PluginError::NonRetryable(format!(
                "{NAME}: {} bytes exceeds max_bytes ({}); only an uncompressed WAV is \
                 split into requests that fit, so split the audio into shorter parts or \
                 point TRANSCRIBE_BASE_URL at a self-hosted endpoint without the 25 MiB \
                 upload limit",
                blob.data.len(),
                cfg.max_bytes
            )));
        };

        let model = cfg.model.as_deref().unwrap_or(&client.default_model);
        // Always a direct call: a `<provider>/` prefix naming the vendor is stripped.
        let wire_model = direct_model(&client.base_url, model);
        let total = windows.len();
        let mut texts: Vec<String> = Vec::new();
        let mut segments: Vec<TranscriptionSegment> = Vec::new();
        let mut any_segments = false;
        let mut language: Option<String> = None;
        let mut last_body = String::new();
        for (index, window) in windows.iter().enumerate() {
            ctx.check_cancelled()?;
            let file = match &layout {
                None => blob.data.clone(),
                Some(layout) => wav_window(&blob.data, layout, &blob.data[window.bytes.clone()]),
            };
            if total == 1 {
                ctx.heartbeat(format!("{NAME}: uploading {} bytes", file.len()));
            } else {
                ctx.heartbeat(format!(
                    "{NAME}: uploading window {}/{total} ({} bytes)",
                    index + 1,
                    file.len()
                ));
            }
            let form = build_form(&blob, file, &mime, wire_model, &cfg)?;
            let body =
                post_transcription(client, form, Duration::from_secs(cfg.timeout_secs)).await?;
            let TranscriptionResponse {
                text,
                segments: window_segments,
                language: detected,
                duration,
            } = parse_transcription(&body)?;

            // The endpoint bills per second of audio, so report what it actually got.
            let mut usage = transcription_usage(duration, window.secs);
            ProviderCosts::global().apply(NAME, model, &mut usage);
            ctx.record_usage(usage);

            if language.is_none() {
                language = detected.filter(|l| !l.trim().is_empty());
            }
            let window_text = match text {
                Some(t) if !t.trim().is_empty() => t.trim().to_string(),
                _ => join_segments(&window_segments),
            };
            if window_segments.is_empty() {
                // A window the endpoint returned no segments for still contributes its
                // text, spanning the window, in case another window has segments.
                if !window_text.is_empty() {
                    segments.push(TranscriptionSegment {
                        start: Some(window.start_secs),
                        end: window.secs.map(|secs| window.start_secs + secs),
                        text: Some(window_text.clone()),
                    });
                }
            } else {
                any_segments = true;
                // Timestamps are relative to the window: shift them onto the recording.
                segments.extend(window_segments.into_iter().map(|seg| TranscriptionSegment {
                    start: seg.start.map(|t| t + window.start_secs),
                    end: seg.end.map(|t| t + window.start_secs),
                    text: seg.text,
                }));
            }
            if !window_text.is_empty() {
                texts.push(window_text);
            }
            last_body = body;
        }
        if !any_segments {
            // No window had segments: the endpoint ignores `verbose_json`.
            segments.clear();
        }
        let language = language.or_else(|| cfg.language.clone());
        let full_text = texts.join(" ");
        if full_text.is_empty() && segments.is_empty() {
            return Err(PluginError::non_retryable(format!(
                "{NAME}: transcription response carries neither `text` nor `segments`: {}",
                truncate_chars(&last_body, 512)
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
    match (stem.is_empty(), blob.origin.is_some()) {
        // `sanitize_id("")` generates a fresh UUID, which is exactly the fallback we want.
        (true, false) => sanitize_id(""),
        (true, true) => blob.tag_id("audio"),
        (false, _) => blob.tag_id(sanitize_id(stem)),
    }
}

/// Assemble the `multipart/form-data` body (`file`, `model`, `response_format` and the
/// optional `language` / `prompt` / `temperature`).
fn build_form(
    blob: &Blob,
    file: Vec<u8>,
    mime: &str,
    model: &str,
    cfg: &WhisperTranscriberConfig,
) -> Result<reqwest::multipart::Form, PluginError> {
    let part = reqwest::multipart::Part::bytes(file)
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
fn transcription_usage(duration: Option<f64>, measured: Option<f64>) -> UsageUnits {
    let seconds = duration.filter(|d| d.is_finite() && *d >= 0.0).or(measured);
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
/// This is not an estimate: for PCM and IEEE-float WAV every sample frame has a fixed
/// size, so `audio bytes / (sample rate * frame size)` is the true length of the audio.
/// Returns `None` for anything that is not such a file — in particular for every
/// compressed format, where byte length says nothing reliable about duration.
///
/// The header is uploader-controlled, so only what a decoder plays from counts:
///
/// * the rate comes from the sample rate, channels and bits per sample, never from the
///   informational "average bytes per second" field, and a block align that disagrees
///   with them (or a zero in any of them) makes the file unmeasurable;
/// * the audio is every byte after the first `data` header, whatever length it declares,
///   since decoders read a stream-written file (declared 0 or short) to its end;
/// * the first `fmt ` and `data` chunks win; later duplicates are ignored.
fn wav_duration_secs(data: &[u8]) -> Option<f64> {
    let layout = wav_layout(data)?;
    Some((data.len() - layout.data_start) as f64 / layout.byte_rate as f64)
}

/// Where the format and the audio of an uncompressed RIFF/WAVE payload live, read
/// by the rules of [`wav_duration_secs`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct WavLayout {
    /// The body of the first `fmt ` chunk.
    fmt: std::ops::Range<usize>,
    /// Bytes per second of audio: sample rate times frame size.
    byte_rate: u64,
    /// Bytes per sample frame (one sample of every channel).
    frame: usize,
    /// The first byte after the first `data` header. The audio runs to the end.
    data_start: usize,
}

fn wav_layout(data: &[u8]) -> Option<WavLayout> {
    /// Uncompressed formats whose frame size is fixed.
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

    // The `fmt ` body, the byte rate and the frame size.
    let mut format: Option<(std::ops::Range<usize>, u64, u64)> = None;
    let mut data_start: Option<usize> = None;
    let mut pos = 12usize;
    while pos + 8 <= data.len() && (format.is_none() || data_start.is_none()) {
        let id = &data[pos..pos + 4];
        let declared = le_u32(&data[pos + 4..])? as usize;
        let body = &data[pos + 8..];
        // A stream-written WAV can declare a length past what is actually there;
        // clamp so a truncated upload still yields the length of the bytes we hold.
        let len = declared.min(body.len());
        match id {
            b"fmt " if format.is_none() => {
                if len < 16 {
                    return None;
                }
                let tag: u16 = le_u16(body)?;
                let uncompressed = match tag {
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
                let channels = u64::from(le_u16(&body[2..])?);
                let sample_rate = u64::from(le_u32(&body[4..])?);
                let block_align = u64::from(le_u16(&body[12..])?);
                // For EXTENSIBLE this is the container size, which is what frames hold.
                let bits = u64::from(le_u16(&body[14..])?);
                let frame = channels * bits.div_ceil(8);
                if sample_rate == 0 || frame == 0 || block_align != frame {
                    return None;
                }
                format = Some((pos + 8..pos + 8 + len, sample_rate * frame, frame));
            }
            b"data" if data_start.is_none() => data_start = Some(pos + 8),
            _ => {}
        }
        // Chunks are word-aligned: an odd length is followed by a pad byte.
        pos += 8 + len + (len & 1);
    }

    let ((fmt, byte_rate, frame), data_start) = (format?, data_start?);
    Some(WavLayout {
        fmt,
        byte_rate,
        frame: usize::try_from(frame).ok()?,
        data_start,
    })
}

// ---------------------------------------------------------------------------
// Windows
// ---------------------------------------------------------------------------

/// One request's worth of audio.
#[derive(Debug, Clone, PartialEq)]
struct Window {
    /// The bytes of the blob this window carries: all of them when the blob is not
    /// split, else a run of whole frames of the WAV's audio.
    bytes: std::ops::Range<usize>,
    /// Where the window starts in the whole recording, in seconds.
    start_secs: f64,
    /// Its length in seconds, when the payload is an uncompressed WAV.
    secs: Option<f64>,
}

/// Cut an uncompressed WAV into windows that each fit in `max_bytes` once
/// [`wav_window`] gives them their own header.
///
/// Windows hold whole frames and follow one another with no gap or overlap, so their
/// transcripts put back together read as the whole recording (a word straddling a
/// cut can be split between two windows). `None` when `data` is not an uncompressed
/// WAV, or when `max_bytes` cannot hold a header and one frame.
fn wav_windows(data: &[u8], max_bytes: usize) -> Option<(WavLayout, Vec<Window>)> {
    let layout = wav_layout(data)?;
    // Every window is a RIFF file, whose lengths are 32-bit.
    let limit = max_bytes.min(u32::MAX as usize);
    let per_window = limit.checked_sub(wav_header_len(&layout))? / layout.frame * layout.frame;
    if per_window == 0 {
        return None;
    }
    // A trailing partial frame cannot be played, so it is dropped.
    let audio = (data.len() - layout.data_start) / layout.frame * layout.frame;
    let byte_rate = layout.byte_rate as f64;
    let windows: Vec<Window> = (0..audio)
        .step_by(per_window)
        .map(|offset| {
            let len = per_window.min(audio - offset);
            let start = layout.data_start + offset;
            Window {
                bytes: start..start + len,
                start_secs: offset as f64 / byte_rate,
                secs: Some(len as f64 / byte_rate),
            }
        })
        .collect();
    if windows.is_empty() {
        return None;
    }
    Some((layout, windows))
}

/// Size of the header [`wav_window`] writes: `RIFF`/`WAVE`, the source's `fmt `
/// chunk (padded to a word) and the `data` header.
fn wav_header_len(layout: &WavLayout) -> usize {
    let fmt = layout.fmt.len();
    12 + 8 + fmt + (fmt & 1) + 8
}

/// A standalone WAV file holding `audio`: the source's `fmt ` chunk verbatim, and
/// `RIFF` and `data` lengths that match what it holds.
fn wav_window(data: &[u8], layout: &WavLayout, audio: &[u8]) -> Vec<u8> {
    let fmt = &data[layout.fmt.clone()];
    let header = wav_header_len(layout);
    let mut out = Vec::with_capacity(header + audio.len());
    out.extend_from_slice(b"RIFF");
    // Both lengths fit: `wav_windows` keeps a window within `u32::MAX` bytes.
    out.extend_from_slice(&((header - 8 + audio.len()) as u32).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&(fmt.len() as u32).to_le_bytes());
    out.extend_from_slice(fmt);
    if fmt.len() & 1 == 1 {
        out.push(0);
    }
    out.extend_from_slice(b"data");
    out.extend_from_slice(&(audio.len() as u32).to_le_bytes());
    out.extend_from_slice(audio);
    out
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

    #[test]
    fn a_long_upload_path_keeps_segment_ids_apart() {
        // No origin: an archive member whose path is longer than an id can be. The
        // `_t<index>` suffix must survive, or every segment overwrites the previous one.
        let name = format!("{}/{}.mp3", "d".repeat(300), "f".repeat(300));
        let blob = Blob::new(vec![], "audio/mpeg", Some(name));
        let base = base_id(&blob);
        let first = Document::with_id(format!("{base}_t0"), "").id;
        let second = Document::with_id(format!("{base}_t1"), "").id;
        assert!(first.len() <= 511 && second.len() <= 511);
        assert!(first.ends_with("_t0"), "{first}");
        assert_ne!(first, second);
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
        // 137.25 s at 100 micro-USD per second (whisper-1 is the default model).
        assert_eq!(usage.cost_micro_usd, 13_725);
        assert_eq!(usage.unpriced_calls, 0);
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

    #[test]
    fn a_forged_byte_rate_does_not_shrink_the_duration() {
        // The average-bytes-per-second field is informational: a decoder plays the
        // samples from the sample rate, channels and bits per sample, so billing must too.
        let mut forged = wav_bytes(3.5);
        forged[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_eq!(wav_duration_secs(&forged), Some(3.5));
        // A block align that disagrees with channels * bytes per sample is not a file
        // we can measure: the seconds stay visibly unknown rather than being guessed.
        let mut odd_align = wav_bytes(1.0);
        odd_align[32..34].copy_from_slice(&64u16.to_le_bytes());
        assert_eq!(wav_duration_secs(&odd_align), None);
        // Zero sample rate, channels or bits cannot describe playable audio.
        for (at, len) in [(24usize, 4usize), (22, 2), (34, 2)] {
            let mut zeroed = wav_bytes(1.0);
            zeroed[at..at + len].fill(0);
            assert_eq!(wav_duration_secs(&zeroed), None, "zeroed bytes {at}..");
        }
    }

    #[test]
    fn a_forged_data_length_does_not_shrink_the_duration() {
        // Decoders read a `data` chunk declared as 0 (or any short length) to the end
        // of a stream-written file, so the bytes actually held are what is billed.
        let mut zero = wav_bytes(2.0);
        zero[40..44].copy_from_slice(&0u32.to_le_bytes());
        assert_eq!(wav_duration_secs(&zero), Some(2.0));
        let mut short = wav_bytes(2.0);
        short[40..44].copy_from_slice(&2u32.to_le_bytes());
        assert_eq!(wav_duration_secs(&short), Some(2.0));
    }

    #[test]
    fn only_the_first_fmt_chunk_counts() {
        // A second `fmt ` chunk after the audio cannot replace the one a decoder used.
        let mut wav = wav_bytes(1.0);
        wav.extend_from_slice(b"fmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes()); // PCM
        wav.extend_from_slice(&8u16.to_le_bytes()); // 8 channels
        wav.extend_from_slice(&192_000u32.to_le_bytes());
        wav.extend_from_slice(&(192_000u32 * 32).to_le_bytes());
        wav.extend_from_slice(&32u16.to_le_bytes());
        wav.extend_from_slice(&32u16.to_le_bytes());
        // The trailing chunk's 24 bytes count as audio (everything after the `data`
        // header does), but the rate is still the first chunk's.
        let secs = wav_duration_secs(&wav).unwrap();
        assert!((secs - 1.0).abs() < 0.01, "got {secs}");
    }

    #[tokio::test]
    async fn a_forged_wav_header_is_billed_for_its_real_length() {
        let mut wav = wav_bytes(3.5);
        wav[28..32].copy_from_slice(&u32::MAX.to_le_bytes());
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
    }

    /// The WAV file carried in a multipart body (from `RIFF` to the end of the chunk
    /// its header declares).
    fn uploaded_wav(body: &[u8]) -> &[u8] {
        let at = body
            .windows(4)
            .position(|w| w == b"RIFF")
            .expect("a RIFF file in the body");
        let size = u32::from_le_bytes(body[at + 4..at + 8].try_into().unwrap()) as usize;
        &body[at..at + 8 + size]
    }

    #[tokio::test]
    async fn a_wav_over_max_bytes_is_sent_in_windows_and_reads_as_one_transcript() {
        // The default 25 MiB holds 819.2 s of 16 kHz mono 16-bit audio: what
        // `video_audio_extractor` emits for a 13 min 40 s video.
        let server = MockServer::start().await;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(move |_: &wiremock::Request| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                ResponseTemplate::new(200)
                    .set_body_json(serde_json::json!({ "text": format!(" part {n} ") }))
            })
            .expect(2)
            .mount(&server)
            .await;

        let wav = wav_bytes(820.0);
        assert!(wav.len() > DEFAULT_MAX_BYTES);
        let ctx = ActivityContext::noop();
        let docs = plugin(&server)
            .execute(
                &ctx,
                PluginInput::Bytes(Blob::new(wav, "audio/wav", Some("talk.wav".into()))),
                serde_json::json!({}),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "talk");
        assert_eq!(docs[0].content, "part 0 part 1");
        assert!(docs[0].meta.chunk_index.is_none());

        let usage = ctx.usage();
        assert!(
            (usage.audio_seconds - 820.0).abs() < 1e-9,
            "got {}",
            usage.audio_seconds
        );
        assert_eq!(usage.external_requests, 2);

        // Each window is a well-formed WAV within the limit, cut on a frame boundary,
        // and together they hold every sample.
        let reqs = server.received_requests().await.unwrap();
        let mut total = 0.0;
        for req in &reqs {
            let window = uploaded_wav(&req.body);
            assert!(window.len() <= DEFAULT_MAX_BYTES, "{} bytes", window.len());
            let declared = u32::from_le_bytes(window[40..44].try_into().unwrap()) as usize;
            assert_eq!(declared, window.len() - 44);
            assert_eq!(declared % 2, 0);
            total += wav_duration_secs(window).unwrap();
        }
        assert!((total - 820.0).abs() < 1e-9, "windows hold {total}s");
    }

    #[tokio::test]
    async fn segments_from_every_window_are_offset_and_numbered_as_one() {
        let server = MockServer::start().await;
        let calls = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = calls.clone();
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(move |_: &wiremock::Request| {
                let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let body = match n {
                    // An endpoint may return no segments for one window: its text
                    // still comes back, spanning the window.
                    1 => serde_json::json!({ "text": "middle", "language": "en" }),
                    _ => serde_json::json!({
                        "text": format!("w{n}"),
                        "language": "en",
                        "duration": if n == 2 { 0.5 } else { 1.0 },
                        "segments": [
                            { "start": 0.0, "end": 0.25, "text": format!(" w{n}a") },
                            { "start": 0.25, "end": 0.5, "text": format!(" w{n}b") }
                        ]
                    }),
                };
                ResponseTemplate::new(200).set_body_json(body)
            })
            .expect(3)
            .mount(&server)
            .await;

        // 1 s windows (32,000 bytes of audio plus the 44-byte header) over 2.5 s.
        let ctx = ActivityContext::noop();
        let docs = plugin(&server)
            .execute(
                &ctx,
                PluginInput::Bytes(Blob::new(
                    wav_bytes(2.5),
                    "audio/wav",
                    Some("call.wav".into()),
                )),
                serde_json::json!({ "segment_documents": true, "max_bytes": 32_044 }),
            )
            .await
            .unwrap()
            .into_documents()
            .unwrap();

        let got: Vec<(String, String, f64, f64, usize)> = docs
            .iter()
            .map(|d| {
                assert_eq!(d.meta.chunk_total, Some(5));
                assert_eq!(d.meta.parent_id.as_deref(), Some("call"));
                (
                    d.id.clone(),
                    d.content.clone(),
                    d.fields["start"].as_f64().unwrap(),
                    d.fields["end"].as_f64().unwrap(),
                    d.meta.chunk_index.unwrap(),
                )
            })
            .collect();
        let want = [
            ("call_t0", "w0a", 0.0, 0.25, 0),
            ("call_t1", "w0b", 0.25, 0.5, 1),
            ("call_t2", "middle", 1.0, 2.0, 2),
            ("call_t3", "w2a", 2.0, 2.25, 3),
            ("call_t4", "w2b", 2.25, 2.5, 4),
        ]
        .map(|(id, text, start, end, i)| (id.to_string(), text.to_string(), start, end, i));
        assert_eq!(got, want);

        let usage = ctx.usage();
        assert!(
            (usage.audio_seconds - 2.5).abs() < 1e-9,
            "got {}",
            usage.audio_seconds
        );
        assert_eq!(usage.external_requests, 3);
    }

    #[test]
    fn windows_find_the_audio_past_extra_chunks_and_keep_every_frame() {
        // Stereo 16-bit (4-byte frames) with a LIST chunk of odd length (and its pad
        // byte) between `fmt ` and `data`, and a trailing partial frame.
        let mut wav = wav_bytes(0.0);
        wav.truncate(36);
        wav[22..24].copy_from_slice(&2u16.to_le_bytes()); // channels
        wav[28..32].copy_from_slice(&64_000u32.to_le_bytes());
        wav[32..34].copy_from_slice(&4u16.to_le_bytes()); // block align
        wav.extend_from_slice(b"LIST");
        wav.extend_from_slice(&5u32.to_le_bytes());
        wav.extend_from_slice(b"INFO!\0");
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&0u32.to_le_bytes());
        let audio: Vec<u8> = (0..1_003u32).map(|i| (i % 251) as u8).collect();
        wav.extend_from_slice(&audio);

        let (layout, windows) = wav_windows(&wav, 44 + 400).unwrap();
        assert_eq!(layout.frame, 4);
        assert_eq!(windows.len(), 3);
        let mut joined = Vec::new();
        for (i, w) in windows.iter().enumerate() {
            let file = wav_window(&wav, &layout, &wav[w.bytes.clone()]);
            assert!(file.len() <= 44 + 400);
            assert_eq!(w.start_secs, (i * 400) as f64 / 64_000.0);
            let own = wav_layout(&file).unwrap();
            assert_eq!(own.byte_rate, 64_000);
            assert_eq!(own.data_start, 44);
            assert_eq!(wav_duration_secs(&file), w.secs);
            joined.extend_from_slice(&file[own.data_start..]);
        }
        // Every whole frame, in order; only the 3-byte partial frame is dropped.
        assert_eq!(joined, audio[..1_000]);
    }

    #[tokio::test]
    async fn a_wav_that_cannot_be_split_under_max_bytes_is_still_rejected() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200))
            .expect(0)
            .mount(&server)
            .await;
        // 45 bytes cannot hold the 44-byte header and one 2-byte frame.
        let err = plugin(&server)
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(wav_bytes(1.0), "audio/wav", None)),
                serde_json::json!({ "max_bytes": 45 }),
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("max_bytes")),
            "{err:?}"
        );
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

    #[tokio::test]
    async fn a_direct_call_to_the_vendor_drops_its_gateway_prefix() {
        for (host, base_path, prefixed, bare) in [
            ("api.openai.com", "/v1", "openai/whisper-1", "whisper-1"),
            (
                "api.groq.com",
                "/openai/v1",
                "groq/whisper-large-v3",
                "whisper-large-v3",
            ),
        ] {
            let server = MockServer::start().await;
            Mock::given(method("POST"))
                .and(path(format!("{base_path}/audio/transcriptions")))
                .respond_with(
                    ResponseTemplate::new(200).set_body_json(serde_json::json!({ "text": "hi" })),
                )
                .expect(1)
                .mount(&server)
                .await;
            let http = reqwest::Client::builder()
                .resolve(host, *server.address())
                .build()
                .unwrap();
            let base = format!("http://{host}:{}{base_path}", server.address().port());
            WhisperTranscriberPlugin::with_client(base, "test-key", "whisper-1", http)
                .execute(
                    &ActivityContext::noop(),
                    audio(),
                    serde_json::json!({ "model": prefixed }),
                )
                .await
                .unwrap();
            let reqs = server.received_requests().await.unwrap();
            assert_eq!(part_value(&reqs[0].body, "model"), bare, "{host}");
        }
    }

    #[tokio::test]
    async fn a_direct_call_elsewhere_keeps_the_id_as_written() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/audio/transcriptions"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "text": "hi" })),
            )
            .expect(1)
            .mount(&server)
            .await;
        plugin(&server)
            .execute(
                &ActivityContext::noop(),
                audio(),
                serde_json::json!({ "model": "openai/whisper-1" }),
            )
            .await
            .unwrap();
        let reqs = server.received_requests().await.unwrap();
        assert_eq!(part_value(&reqs[0].body, "model"), "openai/whisper-1");
    }
}
