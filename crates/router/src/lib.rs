//! # meili-ingest router
//!
//! Pure routing logic shared by the control plane (and its tests):
//!
//! * [`PipelineRouter`] — picks the pipeline to run for a MIME type / filename /
//!   tenant, honouring the precedence *tenant user > global user > builtin* and,
//!   within a tier, *filename pattern > MIME-only*.
//! * [`builtin_pipelines`] — the 15 built-in pipelines of SPEC §9.
//! * [`detect_mime`] — the MIME detection chain of SPEC §10 (magic bytes →
//!   extension → UTF-8 sniff → content-type hint → `application/octet-stream`).
//! * [`mime_to_default_index`] — the default index per MIME family (SPEC §11).
//! * [`plugin_task_queue`] — the Temporal task queue per plugin (SPEC §8.3).

use meili_ingest_plugin_sdk::{
    INDEXER_PLUGIN, PipelineDefinition, PipelineTrigger, RetryConfig, StepDefinition,
};
use serde::{Deserialize, Serialize};

/// Fallback MIME type when nothing is recognised.
pub const OCTET_STREAM: &str = "application/octet-stream";

/// Number of leading bytes inspected by the UTF-8 sniff.
const SNIFF_LEN: usize = 512;

// ---------------------------------------------------------------------------
// Well-known MIME types
// ---------------------------------------------------------------------------

/// Full MIME strings used by the built-in pipelines.
pub mod mime {
    /// PDF.
    pub const PDF: &str = "application/pdf";
    /// Word (OOXML).
    pub const DOCX: &str =
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document";
    /// Word 97-2003.
    pub const DOC: &str = "application/msword";
    /// Excel (OOXML).
    pub const XLSX: &str = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet";
    /// Excel 97-2003.
    pub const XLS: &str = "application/vnd.ms-excel";
    /// PowerPoint (OOXML).
    pub const PPTX: &str =
        "application/vnd.openxmlformats-officedocument.presentationml.presentation";
    /// PowerPoint 97-2003.
    pub const PPT: &str = "application/vnd.ms-powerpoint";
    /// HTML.
    pub const HTML: &str = "text/html";
    /// Plain text.
    pub const TEXT: &str = "text/plain";
    /// Markdown.
    pub const MARKDOWN: &str = "text/markdown";
    /// Markdown (legacy).
    pub const X_MARKDOWN: &str = "text/x-markdown";
    /// CSV.
    pub const CSV: &str = "text/csv";
    /// JSON.
    pub const JSON: &str = "application/json";
    /// Apache Parquet.
    pub const PARQUET: &str = "application/vnd.apache.parquet";
    /// Apache Avro object container file.
    pub const AVRO: &str = "application/vnd.apache.avro";
    /// MessagePack.
    pub const MSGPACK: &str = "application/vnd.msgpack";
    /// YAML.
    pub const YAML: &str = "application/yaml";
    /// Zip archive.
    pub const ZIP: &str = "application/zip";
    /// WAV audio, canonical spelling.
    pub const WAV: &str = "audio/wav";
    /// MP3 audio, canonical spelling.
    pub const MP3: &str = "audio/mpeg";
    /// MP4/M4A audio, canonical spelling.
    pub const M4A: &str = "audio/mp4";
    /// Ogg audio.
    pub const OGG: &str = "audio/ogg";
    /// FLAC audio.
    pub const FLAC: &str = "audio/flac";
    /// Matroska video.
    pub const MKV: &str = "video/x-matroska";
}

pub mod catalog;

/// Collapse the common spellings of a media type onto one canonical name.
///
/// Detection libraries, browsers and CLI tools disagree: `infer` reports a RIFF/WAVE
/// file as `audio/x-wav`, `mime_guess` maps `.m4a` to `audio/m4a`, and callers send
/// `audio/mp3`. Pipelines declare one spelling in `trigger.content_types`, so without
/// canonicalisation an ordinary `.wav` upload matches nothing and the gateway answers
/// 415. Everything downstream sees the canonical name.
pub fn canonical_mime(mime: &str) -> &str {
    match mime {
        "audio/x-wav" | "audio/wave" | "audio/vnd.wave" | "audio/x-pn-wav" | "audio/wav-x" => {
            mime::WAV
        }
        "audio/mp3" | "audio/x-mpeg" | "audio/mpeg3" | "audio/x-mp3" => mime::MP3,
        "audio/m4a" | "audio/x-m4a" | "audio/mp4a-latm" | "audio/aac" | "audio/x-aac" => mime::M4A,
        "audio/x-flac" => mime::FLAC,
        "audio/vorbis" | "audio/x-ogg" | "application/ogg" => mime::OGG,
        "video/x-quicktime" => "video/quicktime",
        "video/matroska" | "video/x-mkv" => mime::MKV,
        "image/jpg" => "image/jpeg",
        "text/x-csv" | "application/csv" => mime::CSV,
        "application/x-parquet" | "application/parquet" | "application/vnd.apache.parquet1" => {
            mime::PARQUET
        }
        "application/avro" | "avro/binary" | "application/x-avro" | "application/avro-binary" => {
            mime::AVRO
        }
        "application/x-msgpack" | "application/msgpack" | "application/x-messagepack" => {
            mime::MSGPACK
        }
        other => other,
    }
}

// ---------------------------------------------------------------------------
// Router
// ---------------------------------------------------------------------------

/// What the caller knows about the content to route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RouteRequest<'a> {
    /// Detected MIME type (parameters tolerated).
    pub mime: &'a str,
    /// Original filename, if any (matched against `trigger.filename_pattern`).
    pub filename: Option<&'a str>,
    /// Tenant id; tenant-scoped pipelines are only visible to their tenant.
    pub project_id: Option<&'a str>,
}

/// The pipeline selected by [`PipelineRouter::resolve`].
#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct RouteMatch<'a> {
    /// The selected pipeline.
    pub pipeline: &'a PipelineDefinition,
    /// `trigger.index_pattern` of the selected pipeline, if set.
    pub index_pattern: Option<&'a str>,
}

/// Resolves MIME type + filename + tenant into a pipeline.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PipelineRouter {
    pipelines: Vec<PipelineDefinition>,
}

/// Precedence tier of a pipeline (lower wins).
fn tier(p: &PipelineDefinition) -> u8 {
    if p.builtin {
        2
    } else if p.project_id.is_some() {
        0
    } else {
        1
    }
}

/// Whether a pipeline is visible to a tenant: global pipelines always are,
/// tenant-scoped ones only to their own tenant.
fn visible_to(p: &PipelineDefinition, project_id: Option<&str>) -> bool {
    match p.project_id.as_deref() {
        None => true,
        Some(owner) => Some(owner) == project_id,
    }
}

impl PipelineRouter {
    /// Build a router over a list of pipelines (built-ins and user-defined, any order).
    pub fn new(pipelines: Vec<PipelineDefinition>) -> Self {
        Self { pipelines }
    }

    /// All pipelines known to the router, regardless of tenant.
    pub fn pipelines(&self) -> &[PipelineDefinition] {
        &self.pipelines
    }

    /// Pick the pipeline whose trigger matches the request.
    ///
    /// Priority: tenant-scoped user pipeline > global user pipeline > builtin.
    /// Within a tier, a trigger with a matching `filename_pattern` beats a MIME-only
    /// trigger. Remaining ties go to the first pipeline in the list.
    pub fn resolve(&self, req: RouteRequest<'_>) -> Option<RouteMatch<'_>> {
        self.pipelines
            .iter()
            .enumerate()
            .filter(|(_, p)| visible_to(p, req.project_id))
            .filter(|(_, p)| p.trigger_matches(req.mime, req.filename))
            .min_by_key(|(i, p)| {
                let has_pattern = p
                    .trigger
                    .as_ref()
                    .and_then(|t| t.filename_pattern.as_ref())
                    .is_some();
                (tier(p), u8::from(!has_pattern), *i)
            })
            .map(|(_, p)| RouteMatch {
                pipeline: p,
                index_pattern: p.trigger.as_ref().and_then(|t| t.index_pattern.as_deref()),
            })
    }

    /// Look a pipeline up by uid. A tenant-scoped pipeline shadows a global one
    /// (or a builtin) with the same uid.
    pub fn by_uid(&self, uid: &str, project_id: Option<&str>) -> Option<&PipelineDefinition> {
        self.pipelines
            .iter()
            .enumerate()
            .filter(|(_, p)| p.uid == uid && visible_to(p, project_id))
            .min_by_key(|(i, p)| (tier(p), *i))
            .map(|(_, p)| p)
    }

    /// Every pipeline visible to a tenant (built-ins, global user pipelines and the
    /// tenant's own), in list order. When a tenant pipeline shadows a global one with
    /// the same uid, only the tenant's version is returned.
    pub fn all(&self, project_id: Option<&str>) -> Vec<&PipelineDefinition> {
        let visible: Vec<&PipelineDefinition> = self
            .pipelines
            .iter()
            .filter(|p| visible_to(p, project_id))
            .collect();
        visible
            .iter()
            .copied()
            .filter(|p| {
                p.project_id.is_some()
                    || !visible
                        .iter()
                        .any(|other| other.project_id.is_some() && other.uid == p.uid)
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Default index / task queue tables
// ---------------------------------------------------------------------------

/// Default Meilisearch index for a MIME type when none is given anywhere (SPEC §11).
pub fn mime_to_default_index(mime: &str) -> &'static str {
    let essence = mime_essence(mime);
    match essence.as_str() {
        m if m.starts_with("video/") => "videos",
        m if m.starts_with("audio/") => "audio",
        m if m.starts_with("image/") => "images",
        mime::HTML => "pages",
        mime::CSV | mime::PARQUET | mime::AVRO => "datasets",
        _ => "documents",
    }
}

/// Temporal task queue that runs a plugin (SPEC §8.3).
pub fn plugin_task_queue(plugin: &str) -> &'static str {
    match plugin {
        "whisper_transcriber" | "ocr" | "video_audio_extractor" => "workers-gpu",
        "llm_enricher" | "image_captioner" => "workers-llm",
        "s3_downloader" => "workers-io",
        _ => "workers-general",
    }
}

// ---------------------------------------------------------------------------
// MIME detection
// ---------------------------------------------------------------------------

/// Detect the MIME type of `data` (SPEC §10).
///
/// 1. Magic bytes (`infer`). A bare `application/zip` is refined with the
///    extension when it names an Office document.
/// 2. File extension of `filename` (`.md`/`.markdown` → `text/markdown`,
///    `.yaml`/`.yml` → `application/yaml`, the rest via `mime_guess`).
/// 3. UTF-8 sniff of the first 512 bytes → `text/plain`, upgraded to
///    `application/json` when the text starts with `{`/`[` and parses as JSON, or to
///    `text/html` when it starts with `<!doctype html` / `<html`.
/// 4. `content_type_hint`, only when it is a real type (not
///    `application/octet-stream`, not `multipart/*`).
/// 5. `application/octet-stream`.
///
/// The result is passed through [`canonical_mime`], so `audio/x-wav` becomes
/// `audio/wav` and `.m4a` becomes `audio/mp4` before any pipeline trigger is matched.
pub fn detect_mime(data: &[u8], filename: Option<&str>, content_type_hint: Option<&str>) -> String {
    canonical_mime(&detect_mime_raw(data, filename, content_type_hint)).to_owned()
}

/// [`detect_mime`] before alias canonicalisation. Exposed for tests that need to see
/// exactly what the detection chain produced.
fn detect_mime_raw(data: &[u8], filename: Option<&str>, content_type_hint: Option<&str>) -> String {
    let from_ext = filename.and_then(mime_from_extension);

    if let Some(magic) = magic_record_format(data) {
        return magic.to_owned();
    }

    if let Some(kind) = infer::get(data) {
        let magic = kind.mime_type();
        if magic == mime::ZIP
            && let Some(ext) = from_ext.as_deref().filter(|m| is_zip_based_office(m))
        {
            return ext.to_owned();
        }
        return magic.to_owned();
    }

    if let Some(ext) = from_ext {
        return ext;
    }

    if let Some(text) = sniff_utf8(data) {
        return classify_text(text, data).to_owned();
    }

    if let Some(hint) = content_type_hint.and_then(usable_hint) {
        return hint;
    }

    OCTET_STREAM.to_owned()
}

/// MIME from a filename's extension, with a few overrides on top of `mime_guess`.
fn mime_from_extension(filename: &str) -> Option<String> {
    let name = filename.rsplit(['/', '\\']).next().unwrap_or(filename);
    let (_, ext) = name.rsplit_once('.')?;
    let ext = ext.to_ascii_lowercase();
    if ext.is_empty() {
        return None;
    }
    let mapped = match ext.as_str() {
        "md" | "markdown" => Some(mime::MARKDOWN),
        "yaml" | "yml" => Some(mime::YAML),
        "json" => Some(mime::JSON),
        "csv" => Some(mime::CSV),
        "parquet" => Some(mime::PARQUET),
        "avro" => Some(mime::AVRO),
        "msgpack" | "mpk" => Some(mime::MSGPACK),
        "txt" | "text" | "log" => Some(mime::TEXT),
        "htm" | "html" => Some(mime::HTML),
        _ => None,
    };
    mapped
        .map(str::to_owned)
        .or_else(|| mime_guess::from_ext(&ext).first_raw().map(str::to_owned))
}

/// Binary record formats `infer` does not recognise, identified by their own markers.
///
/// Parquet brackets the file with `PAR1`, and an Avro object container file opens
/// with `Obj\x01`. MessagePack is deliberately absent: the format has no signature
/// of any kind, so it can only be recognised from the filename or an explicit
/// content type.
fn magic_record_format(data: &[u8]) -> Option<&'static str> {
    if data.len() >= 8 && data.starts_with(b"PAR1") && data.ends_with(b"PAR1") {
        return Some(mime::PARQUET);
    }
    if data.starts_with(b"Obj\x01") {
        return Some(mime::AVRO);
    }
    None
}

/// Office formats that are zip containers underneath.
fn is_zip_based_office(m: &str) -> bool {
    matches!(m, mime::DOCX | mime::XLSX | mime::PPTX)
        || m.starts_with("application/vnd.openxmlformats-officedocument.")
        || m.starts_with("application/vnd.oasis.opendocument.")
}

/// First 512 bytes as text when they look like UTF-8 text (no NUL / stray control chars).
fn sniff_utf8(data: &[u8]) -> Option<&str> {
    if data.is_empty() {
        return None;
    }
    let head = &data[..data.len().min(SNIFF_LEN)];
    let text = match std::str::from_utf8(head) {
        Ok(s) => s,
        // A multi-byte sequence may be cut at the 512-byte boundary: accept the valid prefix.
        Err(e) if e.error_len().is_none() && e.valid_up_to() > 0 => {
            std::str::from_utf8(&head[..e.valid_up_to()]).ok()?
        }
        Err(_) => return None,
    };
    let looks_binary = text
        .chars()
        .any(|c| c.is_control() && !matches!(c, '\t' | '\n' | '\r' | '\u{0c}'));
    if looks_binary { None } else { Some(text) }
}

/// Refine sniffed text into JSON / HTML / plain text.
fn classify_text(head: &str, full: &[u8]) -> &'static str {
    let trimmed = head.trim_start_matches(['\u{feff}', ' ', '\t', '\n', '\r']);
    if (trimmed.starts_with('{') || trimmed.starts_with('['))
        && serde_json::from_slice::<serde::de::IgnoredAny>(full).is_ok()
    {
        return mime::JSON;
    }
    let lower: String = trimmed
        .chars()
        .take(32)
        .collect::<String>()
        .to_ascii_lowercase();
    if lower.starts_with("<!doctype html") || lower.starts_with("<html") {
        return mime::HTML;
    }
    mime::TEXT
}

/// A content-type hint we are willing to trust as a last resort.
fn usable_hint(hint: &str) -> Option<String> {
    let essence = mime_essence(hint);
    if essence.is_empty()
        || essence == OCTET_STREAM
        || essence.starts_with("multipart/")
        || !essence.contains('/')
    {
        return None;
    }
    Some(essence)
}

/// `type/subtype; params` → lower-cased `type/subtype`.
fn mime_essence(mime: &str) -> String {
    mime.split(';')
        .next()
        .unwrap_or("")
        .trim()
        .to_ascii_lowercase()
}

// ---------------------------------------------------------------------------
// Built-in pipelines (SPEC §9)
// ---------------------------------------------------------------------------

/// Default chunker configuration used by every built-in pipeline with a chunk step.
pub fn default_chunker_config() -> serde_json::Value {
    serde_json::json!({ "strategy": "sentence", "chunk_size": 512, "overlap": 64 })
}

/// Default retry policy of built-in steps: 3 attempts, exponential backoff.
pub fn default_retry() -> RetryConfig {
    RetryConfig::default()
}

/// A built-in step with the default retry policy.
pub(crate) fn step(id: &str, plugin: &str) -> StepDefinition {
    let mut s = StepDefinition::new(id, plugin);
    s.retry = Some(default_retry());
    s
}

/// The `chunk` step with the default chunker config.
pub(crate) fn chunk_step() -> StepDefinition {
    step("chunk", "chunker").config(default_chunker_config())
}

/// The terminal `index` step.
///
/// Its time is spent waiting for Meilisearch tasks, and a busy instance processes
/// other tasks first (one at a time), so the 300s step default is too tight. The
/// indexer's own per-task budget (`INDEXER_TASK_TIMEOUT_SECS`) must stay below this.
pub(crate) fn index_step() -> StepDefinition {
    step("index", INDEXER_PLUGIN).timeout_secs(INDEX_STEP_TIMEOUT_SECS)
}

/// Step timeout of every built-in `index` step: 30 minutes.
pub const INDEX_STEP_TIMEOUT_SECS: u64 = 1800;

/// Assemble a built-in pipeline (`builtin.<suffix>`), sequential steps.
fn builtin(
    suffix: &str,
    name: &str,
    description: &str,
    content_types: &[&str],
    steps: Vec<StepDefinition>,
) -> PipelineDefinition {
    let mut p = PipelineDefinition {
        uid: format!("builtin.{suffix}"),
        name: name.to_owned(),
        description: Some(description.to_owned()),
        version: 1,
        trigger: Some(PipelineTrigger {
            content_types: content_types.iter().map(|s| (*s).to_owned()).collect(),
            filename_pattern: None,
            index_pattern: None,
        }),
        steps,
        builtin: true,
        project_id: None,
    };
    p.normalize();
    p
}

/// The 15 built-in pipelines of SPEC §9, in table order. Every one validates.
pub fn builtin_pipelines() -> Vec<PipelineDefinition> {
    vec![
        builtin(
            "pdf",
            "PDF",
            "Extract text per page, chunk, index.",
            &[mime::PDF],
            vec![step("extract", "pdf_extractor"), chunk_step(), index_step()],
        ),
        builtin(
            "word",
            "Word",
            "Extract text from Word documents, chunk, index.",
            &[mime::DOCX, mime::DOC],
            vec![
                step("extract", "docx_extractor"),
                chunk_step(),
                index_step(),
            ],
        ),
        builtin(
            "excel",
            "Excel",
            "One document per spreadsheet row, index.",
            &[mime::XLSX, mime::XLS],
            vec![step("extract", "xlsx_extractor"), index_step()],
        ),
        builtin(
            "powerpoint",
            "PowerPoint",
            "Extract slide text, index.",
            &[mime::PPTX],
            vec![step("extract", "pptx_extractor"), index_step()],
        ),
        builtin(
            "html",
            "HTML",
            "Extract readable text from HTML, chunk, index.",
            &[mime::HTML],
            vec![
                step("extract", "html_extractor"),
                chunk_step(),
                index_step(),
            ],
        ),
        builtin(
            "text",
            "Plain text",
            "Chunk plain text, index.",
            &[mime::TEXT],
            vec![chunk_step(), index_step()],
        ),
        builtin(
            "markdown",
            "Markdown",
            "Split Markdown on headings, index.",
            &[mime::MARKDOWN, mime::X_MARKDOWN],
            vec![step("extract", "markdown_extractor"), index_step()],
        ),
        builtin(
            "csv",
            "CSV",
            "One document per CSV row, index.",
            &[mime::CSV],
            vec![step("extract", "csv_parser"), index_step()],
        ),
        builtin(
            "json",
            "JSON",
            "Flatten JSON into documents, index.",
            &[mime::JSON],
            vec![step("extract", "json_flattener"), index_step()],
        ),
        builtin(
            "parquet",
            "Parquet",
            "One document per Parquet row, index.",
            &[mime::PARQUET],
            vec![step("extract", "parquet_parser"), index_step()],
        ),
        builtin(
            "avro",
            "Avro",
            "One document per Avro record, index.",
            &[mime::AVRO],
            vec![step("extract", "avro_parser"), index_step()],
        ),
        builtin(
            "msgpack",
            "MessagePack",
            "Decode MessagePack into documents, index.",
            &[mime::MSGPACK],
            vec![step("extract", "msgpack_parser"), index_step()],
        ),
        builtin(
            "image",
            "Image",
            "Caption the image with a vision model, index.",
            &["image/jpeg", "image/png", "image/webp", "image/gif"],
            vec![
                step("caption", "image_captioner").timeout_secs(600),
                index_step(),
            ],
        ),
        builtin(
            "audio",
            "Audio",
            "Transcribe audio with Whisper, index.",
            &[
                "audio/mpeg",
                "audio/wav",
                "audio/ogg",
                "audio/mp4",
                "audio/flac",
            ],
            vec![
                step("transcribe", "whisper_transcriber").timeout_secs(3600),
                index_step(),
            ],
        ),
        builtin(
            "video",
            "Video",
            "Extract the audio track, transcribe with Whisper, index.",
            &[
                "video/mp4",
                "video/quicktime",
                "video/webm",
                "video/x-matroska",
            ],
            vec![
                step("extract_audio", "video_audio_extractor").timeout_secs(1800),
                step("transcribe", "whisper_transcriber").timeout_secs(3600),
                index_step(),
            ],
        ),
    ]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use meili_ingest_plugin_sdk::Backoff;

    fn user_pipeline(
        uid: &str,
        project_id: Option<&str>,
        content_types: &[&str],
        pattern: Option<&str>,
    ) -> PipelineDefinition {
        PipelineDefinition {
            uid: uid.to_owned(),
            name: uid.to_owned(),
            description: None,
            version: 1,
            trigger: Some(PipelineTrigger {
                content_types: content_types.iter().map(|s| (*s).to_owned()).collect(),
                filename_pattern: pattern.map(str::to_owned),
                index_pattern: Some(format!("idx-{uid}")),
            }),
            steps: vec![StepDefinition::new("index", INDEXER_PLUGIN)],
            builtin: false,
            project_id: project_id.map(str::to_owned),
        }
    }

    // -- builtin table -------------------------------------------------------

    #[test]
    fn builtin_table_has_fifteen_valid_pipelines() {
        let all = builtin_pipelines();
        assert_eq!(all.len(), 15);
        let expected = [
            "builtin.pdf",
            "builtin.word",
            "builtin.excel",
            "builtin.powerpoint",
            "builtin.html",
            "builtin.text",
            "builtin.markdown",
            "builtin.csv",
            "builtin.json",
            "builtin.parquet",
            "builtin.avro",
            "builtin.msgpack",
            "builtin.image",
            "builtin.audio",
            "builtin.video",
        ];
        let uids: Vec<&str> = all.iter().map(|p| p.uid.as_str()).collect();
        assert_eq!(uids, expected);
        for p in &all {
            assert!(p.builtin, "{} must be builtin", p.uid);
            assert!(p.project_id.is_none(), "{} must be global", p.uid);
            assert_eq!(p.version, 1);
            assert!(!p.name.is_empty());
            let trigger = p
                .trigger
                .as_ref()
                .unwrap_or_else(|| panic!("{} has no trigger", p.uid));
            assert!(
                !trigger.content_types.is_empty(),
                "{} has no content types",
                p.uid
            );
            assert!(trigger.filename_pattern.is_none());
            let order = p
                .validate()
                .unwrap_or_else(|e| panic!("{} does not validate: {e}", p.uid));
            assert_eq!(order.len(), p.steps.len());
            let last = p.steps.last().unwrap();
            assert_eq!(last.id, "index");
            assert_eq!(last.plugin, INDEXER_PLUGIN);
            // Sequential: every non-root step depends on the previous one.
            for (i, s) in p.steps.iter().enumerate().skip(1) {
                assert_eq!(
                    s.depends_on,
                    vec![p.steps[i - 1].id.clone()],
                    "{}/{}",
                    p.uid,
                    s.id
                );
            }
            for s in &p.steps {
                let retry = s
                    .retry
                    .as_ref()
                    .unwrap_or_else(|| panic!("{}/{} has no retry", p.uid, s.id));
                assert_eq!(retry.max_attempts, 3);
                assert_eq!(retry.backoff, Backoff::Exponential);
                assert_eq!(s.effective_retry(), RetryConfig::default());
            }
        }
    }

    #[test]
    fn builtin_steps_follow_spec_table() {
        let all = builtin_pipelines();
        let plugins = |uid: &str| -> Vec<String> {
            all.iter()
                .find(|p| p.uid == uid)
                .unwrap()
                .steps
                .iter()
                .map(|s| s.plugin.clone())
                .collect()
        };
        assert_eq!(
            plugins("builtin.pdf"),
            ["pdf_extractor", "chunker", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.word"),
            ["docx_extractor", "chunker", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.excel"),
            ["xlsx_extractor", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.powerpoint"),
            ["pptx_extractor", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.html"),
            ["html_extractor", "chunker", "meili_indexer"]
        );
        assert_eq!(plugins("builtin.text"), ["chunker", "meili_indexer"]);
        assert_eq!(
            plugins("builtin.markdown"),
            ["markdown_extractor", "meili_indexer"]
        );
        assert_eq!(plugins("builtin.csv"), ["csv_parser", "meili_indexer"]);
        assert_eq!(plugins("builtin.json"), ["json_flattener", "meili_indexer"]);
        assert_eq!(
            plugins("builtin.parquet"),
            ["parquet_parser", "meili_indexer"]
        );
        assert_eq!(plugins("builtin.avro"), ["avro_parser", "meili_indexer"]);
        assert_eq!(
            plugins("builtin.msgpack"),
            ["msgpack_parser", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.image"),
            ["image_captioner", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.audio"),
            ["whisper_transcriber", "meili_indexer"]
        );
        assert_eq!(
            plugins("builtin.video"),
            [
                "video_audio_extractor",
                "whisper_transcriber",
                "meili_indexer"
            ]
        );

        let word = all.iter().find(|p| p.uid == "builtin.word").unwrap();
        assert_eq!(
            word.trigger.as_ref().unwrap().content_types,
            vec![
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                "application/msword"
            ]
        );
        let excel = all.iter().find(|p| p.uid == "builtin.excel").unwrap();
        assert_eq!(
            excel.trigger.as_ref().unwrap().content_types,
            vec![
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "application/vnd.ms-excel"
            ]
        );
        let ppt = all.iter().find(|p| p.uid == "builtin.powerpoint").unwrap();
        assert_eq!(
            ppt.trigger.as_ref().unwrap().content_types,
            vec!["application/vnd.openxmlformats-officedocument.presentationml.presentation"]
        );
    }

    #[test]
    fn builtin_chunk_steps_use_default_chunker_config() {
        for p in builtin_pipelines() {
            for s in p.steps.iter().filter(|s| s.plugin == "chunker") {
                assert_eq!(s.id, "chunk");
                assert_eq!(
                    s.config,
                    serde_json::json!({"strategy": "sentence", "chunk_size": 512, "overlap": 64})
                );
            }
        }
    }

    #[test]
    fn builtin_pipelines_route_every_spec_mime() {
        let router = PipelineRouter::new(builtin_pipelines());
        let table = [
            ("application/pdf", "builtin.pdf"),
            (mime::DOCX, "builtin.word"),
            ("application/msword", "builtin.word"),
            (mime::XLSX, "builtin.excel"),
            ("application/vnd.ms-excel", "builtin.excel"),
            (mime::PPTX, "builtin.powerpoint"),
            ("text/html", "builtin.html"),
            ("text/html; charset=utf-8", "builtin.html"),
            ("text/plain", "builtin.text"),
            ("text/markdown", "builtin.markdown"),
            ("text/x-markdown", "builtin.markdown"),
            ("text/csv", "builtin.csv"),
            ("application/json", "builtin.json"),
            ("image/jpeg", "builtin.image"),
            ("image/png", "builtin.image"),
            ("image/webp", "builtin.image"),
            ("image/gif", "builtin.image"),
            ("audio/mpeg", "builtin.audio"),
            ("audio/wav", "builtin.audio"),
            ("audio/ogg", "builtin.audio"),
            ("audio/mp4", "builtin.audio"),
            ("video/mp4", "builtin.video"),
            ("video/quicktime", "builtin.video"),
            ("video/webm", "builtin.video"),
        ];
        for (mime, uid) in table {
            let m = router
                .resolve(RouteRequest {
                    mime,
                    filename: None,
                    project_id: None,
                })
                .unwrap_or_else(|| panic!("no route for {mime}"));
            assert_eq!(m.pipeline.uid, uid, "{mime}");
            assert_eq!(m.index_pattern, None);
        }
        assert!(
            router
                .resolve(RouteRequest {
                    mime: "application/octet-stream",
                    filename: None,
                    project_id: None
                })
                .is_none()
        );
        assert!(
            router
                .resolve(RouteRequest {
                    mime: "application/zip",
                    filename: Some("a.zip"),
                    project_id: None
                })
                .is_none()
        );
    }

    // -- routing precedence -----------------------------------------------

    #[test]
    fn tenant_user_beats_global_user_beats_builtin() {
        let mut pipelines = builtin_pipelines();
        pipelines.push(user_pipeline(
            "global-pdf",
            None,
            &["application/pdf"],
            None,
        ));
        pipelines.push(user_pipeline(
            "tenant-pdf",
            Some("acme"),
            &["application/pdf"],
            None,
        ));
        pipelines.push(user_pipeline(
            "other-tenant-pdf",
            Some("globex"),
            &["application/pdf"],
            None,
        ));
        let router = PipelineRouter::new(pipelines);

        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: Some("x.pdf"),
                project_id: Some("acme"),
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "tenant-pdf");
        assert_eq!(m.index_pattern, Some("idx-tenant-pdf"));

        // Another tenant does not see acme's pipeline, but sees the global one.
        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: None,
                project_id: Some("initech"),
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "global-pdf");

        // No tenant at all: global user pipeline still beats builtin.
        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: None,
                project_id: None,
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "global-pdf");

        // A MIME nobody overrides falls through to the builtin.
        let m = router
            .resolve(RouteRequest {
                mime: "text/csv",
                filename: None,
                project_id: Some("acme"),
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "builtin.csv");
    }

    #[test]
    fn filename_pattern_beats_mime_only_within_a_tier() {
        let mut pipelines = builtin_pipelines();
        pipelines.push(user_pipeline("pdf-any", None, &["application/pdf"], None));
        pipelines.push(user_pipeline(
            "pdf-contracts",
            None,
            &["application/pdf"],
            Some("contract_*.pdf"),
        ));
        let router = PipelineRouter::new(pipelines);

        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: Some("contract_2024.pdf"),
                project_id: None,
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "pdf-contracts");

        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: Some("invoice.pdf"),
                project_id: None,
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "pdf-any");

        // Without a filename, pattern triggers cannot match.
        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: None,
                project_id: None,
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "pdf-any");
    }

    #[test]
    fn tier_beats_filename_pattern_specificity() {
        // A builtin-tier pattern match must not beat a global user MIME-only match.
        let mut pipelines = vec![user_pipeline("user-pdf", None, &["application/pdf"], None)];
        let mut builtin_with_pattern = builtin_pipelines().remove(0);
        builtin_with_pattern
            .trigger
            .as_mut()
            .unwrap()
            .filename_pattern = Some("*.pdf".into());
        pipelines.push(builtin_with_pattern);
        let router = PipelineRouter::new(pipelines);
        let m = router
            .resolve(RouteRequest {
                mime: "application/pdf",
                filename: Some("a.pdf"),
                project_id: None,
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "user-pdf");
    }

    #[test]
    fn ties_go_to_first_in_list_and_wildcards_work() {
        let pipelines = vec![
            user_pipeline("first", None, &["image/*"], None),
            user_pipeline("second", None, &["image/png"], None),
        ];
        let router = PipelineRouter::new(pipelines);
        let m = router
            .resolve(RouteRequest {
                mime: "image/png",
                filename: None,
                project_id: None,
            })
            .unwrap();
        assert_eq!(m.pipeline.uid, "first");
    }

    #[test]
    fn pipelines_without_trigger_never_route() {
        let mut p = user_pipeline("manual", None, &[], None);
        p.trigger = None;
        let router = PipelineRouter::new(vec![p]);
        assert!(
            router
                .resolve(RouteRequest {
                    mime: "application/pdf",
                    filename: Some("a.pdf"),
                    project_id: None
                })
                .is_none()
        );
        assert!(router.by_uid("manual", None).is_some());
    }

    #[test]
    fn by_uid_tenant_shadows_global() {
        let mut pipelines = builtin_pipelines();
        pipelines.push(user_pipeline("shared", None, &["text/plain"], None));
        pipelines.push(user_pipeline("shared", Some("acme"), &["text/plain"], None));
        let router = PipelineRouter::new(pipelines);

        assert_eq!(
            router
                .by_uid("shared", Some("acme"))
                .unwrap()
                .project_id
                .as_deref(),
            Some("acme")
        );
        assert_eq!(
            router.by_uid("shared", Some("globex")).unwrap().project_id,
            None
        );
        assert_eq!(router.by_uid("shared", None).unwrap().project_id, None);
        assert_eq!(
            router.by_uid("builtin.pdf", Some("acme")).unwrap().uid,
            "builtin.pdf"
        );
        assert!(router.by_uid("nope", None).is_none());

        // A tenant pipeline may even shadow a builtin uid.
        let mut pipelines = builtin_pipelines();
        pipelines.push(user_pipeline(
            "builtin.pdf",
            Some("acme"),
            &["application/pdf"],
            None,
        ));
        let router = PipelineRouter::new(pipelines);
        assert!(!router.by_uid("builtin.pdf", Some("acme")).unwrap().builtin);
        assert!(router.by_uid("builtin.pdf", None).unwrap().builtin);
    }

    #[test]
    fn all_lists_visible_pipelines_and_dedupes_shadowed_uids() {
        let mut pipelines = builtin_pipelines();
        pipelines.push(user_pipeline("shared", None, &["text/plain"], None));
        pipelines.push(user_pipeline("shared", Some("acme"), &["text/plain"], None));
        pipelines.push(user_pipeline(
            "globex-only",
            Some("globex"),
            &["text/plain"],
            None,
        ));
        let router = PipelineRouter::new(pipelines);

        let acme = router.all(Some("acme"));
        assert_eq!(acme.len(), 16);
        let shared: Vec<_> = acme.iter().filter(|p| p.uid == "shared").collect();
        assert_eq!(shared.len(), 1);
        assert_eq!(shared[0].project_id.as_deref(), Some("acme"));
        assert!(acme.iter().all(|p| p.uid != "globex-only"));

        let anon = router.all(None);
        assert_eq!(anon.len(), 16);
        assert!(
            anon.iter()
                .any(|p| p.uid == "shared" && p.project_id.is_none())
        );

        let globex = router.all(Some("globex"));
        assert_eq!(globex.len(), 17);
        assert_eq!(router.pipelines().len(), 18);
    }

    // -- mime_to_default_index / plugin_task_queue -------------------------

    #[test]
    fn default_index_table() {
        assert_eq!(mime_to_default_index("video/mp4"), "videos");
        assert_eq!(mime_to_default_index("video/webm"), "videos");
        assert_eq!(mime_to_default_index("audio/mpeg"), "audio");
        assert_eq!(mime_to_default_index("image/png"), "images");
        assert_eq!(mime_to_default_index("IMAGE/JPEG"), "images");
        assert_eq!(mime_to_default_index("text/html"), "pages");
        assert_eq!(mime_to_default_index("text/html; charset=utf-8"), "pages");
        assert_eq!(mime_to_default_index("text/csv"), "datasets");
        assert_eq!(
            mime_to_default_index("application/vnd.apache.parquet"),
            "datasets"
        );
        assert_eq!(
            mime_to_default_index("application/vnd.apache.avro"),
            "datasets"
        );
        assert_eq!(
            mime_to_default_index("application/vnd.msgpack"),
            "documents",
            "MessagePack is JSON-shaped, not tabular"
        );
        assert_eq!(mime_to_default_index("application/pdf"), "documents");
        assert_eq!(mime_to_default_index("text/plain"), "documents");
        assert_eq!(mime_to_default_index("application/json"), "documents");
        assert_eq!(mime_to_default_index(""), "documents");
    }

    #[test]
    fn task_queue_table() {
        assert_eq!(plugin_task_queue("whisper_transcriber"), "workers-gpu");
        assert_eq!(plugin_task_queue("ocr"), "workers-gpu");
        assert_eq!(plugin_task_queue("video_audio_extractor"), "workers-gpu");
        assert_eq!(plugin_task_queue("llm_enricher"), "workers-llm");
        assert_eq!(plugin_task_queue("image_captioner"), "workers-llm");
        assert_eq!(plugin_task_queue("s3_downloader"), "workers-io");
        assert_eq!(plugin_task_queue("pdf_extractor"), "workers-general");
        assert_eq!(plugin_task_queue("chunker"), "workers-general");
        assert_eq!(plugin_task_queue("meili_indexer"), "workers-general");
        assert_eq!(
            plugin_task_queue("some_community_plugin"),
            "workers-general"
        );
    }

    // -- detect_mime ----------------------------------------------------------

    const PDF_HEAD: &[u8] = b"%PDF-1.7\n%\xE2\xE3\xCF\xD3\n1 0 obj\n<< /Type /Catalog >>\nendobj\n";
    const PNG_HEAD: &[u8] = &[
        0x89, b'P', b'N', b'G', 0x0D, 0x0A, 0x1A, 0x0A, 0, 0, 0, 0x0D, b'I', b'H', b'D', b'R',
    ];
    const JPEG_HEAD: &[u8] = &[
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F', 0x00,
    ];
    /// Bytes that are neither a known signature nor valid UTF-8.
    const GARBAGE: &[u8] = &[
        0x00, 0xFF, 0xFE, 0x13, 0x37, 0xC0, 0x80, 0x01, 0x02, 0xFF, 0xFF, 0x00,
    ];

    /// Minimal zip local file header whose first entry is `word/document.xml`
    /// (what `infer` inspects to recognise OOXML containers).
    fn fake_zip(first_entry: &str) -> Vec<u8> {
        let name = first_entry.as_bytes();
        let content = b"<xml/>";
        let mut buf = Vec::new();
        buf.extend_from_slice(&[b'P', b'K', 0x03, 0x04]); // signature
        buf.extend_from_slice(&[0x14, 0x00]); // version needed
        buf.extend_from_slice(&[0x00, 0x00]); // flags
        buf.extend_from_slice(&[0x00, 0x00]); // method: stored
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // mod time/date
        buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]); // crc32
        buf.extend_from_slice(&(content.len() as u32).to_le_bytes()); // compressed size
        buf.extend_from_slice(&(content.len() as u32).to_le_bytes()); // uncompressed size
        buf.extend_from_slice(&(name.len() as u16).to_le_bytes()); // name length
        buf.extend_from_slice(&[0x00, 0x00]); // extra length
        assert_eq!(buf.len(), 0x1E);
        buf.extend_from_slice(name);
        buf.extend_from_slice(content);
        buf
    }

    #[test]
    fn magic_bytes_pdf_png_jpeg() {
        assert_eq!(detect_mime(PDF_HEAD, None, None), "application/pdf");
        assert_eq!(detect_mime(PNG_HEAD, None, None), "image/png");
        assert_eq!(detect_mime(JPEG_HEAD, None, None), "image/jpeg");
        // Magic bytes beat a misleading extension and a misleading hint.
        assert_eq!(
            detect_mime(PDF_HEAD, Some("report.txt"), Some("text/csv")),
            "application/pdf"
        );
        assert_eq!(detect_mime(PNG_HEAD, Some("photo.jpg"), None), "image/png");
    }

    #[test]
    fn magic_bytes_detect_ooxml_office_documents() {
        assert_eq!(
            detect_mime(&fake_zip("word/document.xml"), None, None),
            mime::DOCX
        );
        assert_eq!(
            detect_mime(&fake_zip("xl/workbook.xml"), None, None),
            mime::XLSX
        );
        assert_eq!(
            detect_mime(&fake_zip("ppt/presentation.xml"), None, None),
            mime::PPTX
        );
    }

    #[test]
    fn plain_zip_falls_back_to_office_extension() {
        // A zip whose first entry is not an Office directory: infer says application/zip.
        let zip = fake_zip("mimetype");
        assert_eq!(detect_mime(&zip, None, None), "application/zip");
        assert_eq!(detect_mime(&zip, Some("deck.pptx"), None), mime::PPTX);
        assert_eq!(detect_mime(&zip, Some("book.xlsx"), None), mime::XLSX);
        assert_eq!(detect_mime(&zip, Some("letter.docx"), None), mime::DOCX);
        // Non-office extensions do not override a genuine zip signature.
        assert_eq!(
            detect_mime(&zip, Some("archive.zip"), None),
            "application/zip"
        );
        assert_eq!(
            detect_mime(&zip, Some("weird.txt"), None),
            "application/zip"
        );
    }

    #[test]
    fn plain_utf8_text() {
        assert_eq!(
            detect_mime(b"Hello, world.\nSecond line.", None, None),
            "text/plain"
        );
        assert_eq!(
            detect_mime("Héllo wörld — ünïcödé ✓".as_bytes(), None, None),
            "text/plain"
        );
        // A multi-byte char cut at the sniff boundary is still text.
        let mut long = "a".repeat(SNIFF_LEN - 1);
        long.push('é');
        long.push_str(" more text");
        assert_eq!(detect_mime(long.as_bytes(), None, None), "text/plain");
    }

    #[test]
    fn json_sniff() {
        assert_eq!(
            detect_mime(br#"{"id": 1, "title": "x"}"#, None, None),
            "application/json"
        );
        assert_eq!(
            detect_mime(b"  \n[{\"id\":1},{\"id\":2}]", None, None),
            "application/json"
        );
        // Looks like JSON but does not parse: plain text.
        assert_eq!(detect_mime(b"{not json at all", None, None), "text/plain");
        // A JSON-ish string with a trailing newline is fine.
        assert_eq!(detect_mime(b"[1,2,3]\n", None, None), "application/json");
    }

    #[test]
    fn html_sniff() {
        assert_eq!(
            detect_mime(b"<!DOCTYPE html><html><body>x</body></html>", None, None),
            "text/html"
        );
        assert_eq!(
            detect_mime(b"\n  <html lang=\"en\"><head></head></html>", None, None),
            "text/html"
        );
        assert_eq!(detect_mime(b"<!doctype HTML>", None, None), "text/html");
        // Other XML-ish text stays plain.
        assert_eq!(
            detect_mime(b"<note><to>x</to></note>", None, None),
            "text/plain"
        );
    }

    #[test]
    fn binary_record_formats_are_detected() {
        // Parquet brackets the file with PAR1 and is found by magic bytes alone.
        let mut parquet = b"PAR1".to_vec();
        parquet.extend([0u8; 40]);
        parquet.extend(b"PAR1");
        assert_eq!(
            detect_mime(&parquet, None, None),
            "application/vnd.apache.parquet"
        );
        assert_eq!(
            detect_mime(&parquet, Some("rows.parquet"), None),
            "application/vnd.apache.parquet"
        );
        // A leading PAR1 with no trailing one is not a Parquet file.
        assert_ne!(
            detect_mime(b"PAR1 and then some text", None, None),
            "application/vnd.apache.parquet"
        );

        // Avro object container files open with Obj\x01.
        let mut avro = b"Obj\x01".to_vec();
        avro.extend([0u8; 24]);
        assert_eq!(
            detect_mime(&avro, None, None),
            "application/vnd.apache.avro"
        );

        // MessagePack has no signature, so only the extension or a hint finds it.
        let packed = [0x81u8, 0xa2, b'i', b'd', 0x01];
        assert_eq!(
            detect_mime(&packed, Some("data.msgpack"), None),
            "application/vnd.msgpack"
        );
        assert_eq!(
            detect_mime(&packed, Some("data.mpk"), None),
            "application/vnd.msgpack"
        );
        assert_eq!(
            detect_mime(GARBAGE, None, Some("application/x-msgpack")),
            "application/vnd.msgpack",
            "the alias spelling is canonicalised"
        );

        // Alias spellings collapse onto the canonical names.
        assert_eq!(canonical_mime("application/x-parquet"), mime::PARQUET);
        assert_eq!(canonical_mime("avro/binary"), mime::AVRO);
        assert_eq!(canonical_mime("application/msgpack"), mime::MSGPACK);
    }

    #[test]
    fn extension_fallback_for_text_formats() {
        let csv = b"id,name\n1,Alice\n2,Bob\n";
        assert_eq!(detect_mime(csv, Some("people.csv"), None), "text/csv");
        assert_eq!(
            detect_mime(csv, Some("/uploads/PEOPLE.CSV"), None),
            "text/csv"
        );
        let md = b"# Title\n\nSome *markdown*.\n";
        assert_eq!(detect_mime(md, Some("README.md"), None), "text/markdown");
        assert_eq!(
            detect_mime(md, Some("notes.markdown"), None),
            "text/markdown"
        );
        assert_eq!(
            detect_mime(b"key: value\n", Some("config.yaml"), None),
            "application/yaml"
        );
        assert_eq!(
            detect_mime(b"key: value\n", Some("config.yml"), None),
            "application/yaml"
        );
        assert_eq!(
            detect_mime(b"<p>hi</p>", Some("page.html"), None),
            "text/html"
        );
        assert_eq!(
            detect_mime(b"{\"a\":1}", Some("data.json"), None),
            "application/json"
        );
        assert_eq!(detect_mime(b"plain", Some("notes.txt"), None), "text/plain");
        // Extension beats the sniff: JSON content in a .txt file stays text/plain.
        assert_eq!(
            detect_mime(b"{\"a\":1}", Some("data.txt"), None),
            "text/plain"
        );
        // Unknown extension falls through to the sniff.
        assert_eq!(
            detect_mime(b"{\"a\":1}", Some("data.unknownext"), None),
            "application/json"
        );
        assert_eq!(detect_mime(md, Some("noext"), None), "text/plain");
        // Extension also applies to binary content without a signature.
        assert_eq!(
            detect_mime(GARBAGE, Some("blob.bin"), None),
            "application/octet-stream"
        );
        assert_eq!(
            detect_mime(GARBAGE, Some("legacy.doc"), None),
            "application/msword"
        );
    }

    #[test]
    fn binary_garbage_is_octet_stream() {
        assert_eq!(detect_mime(GARBAGE, None, None), "application/octet-stream");
        assert_eq!(detect_mime(&[], None, None), "application/octet-stream");
        // Valid UTF-8 but full of control characters: not text.
        assert_eq!(
            detect_mime(b"\x00\x01\x02\x03abc", None, None),
            "application/octet-stream"
        );
    }

    #[test]
    fn hint_is_used_only_as_last_resort() {
        // Nothing else knows: a real hint wins over octet-stream.
        assert_eq!(
            detect_mime(GARBAGE, None, Some("application/x-custom")),
            "application/x-custom"
        );
        assert_eq!(
            detect_mime(GARBAGE, None, Some("Audio/MPEG; rate=44100")),
            "audio/mpeg"
        );
        // Useless hints are ignored.
        assert_eq!(
            detect_mime(GARBAGE, None, Some("application/octet-stream")),
            "application/octet-stream"
        );
        assert_eq!(
            detect_mime(GARBAGE, None, Some("multipart/form-data; boundary=xyz")),
            "application/octet-stream"
        );
        assert_eq!(
            detect_mime(GARBAGE, None, Some("")),
            "application/octet-stream"
        );
        assert_eq!(
            detect_mime(GARBAGE, None, Some("garbage")),
            "application/octet-stream"
        );
        // Every earlier stage beats the hint.
        assert_eq!(
            detect_mime(PDF_HEAD, None, Some("text/csv")),
            "application/pdf"
        );
        assert_eq!(
            detect_mime(b"a,b\n1,2\n", Some("x.csv"), Some("application/json")),
            "text/csv"
        );
        assert_eq!(
            detect_mime(b"hello", None, Some("application/json")),
            "text/plain"
        );
        assert_eq!(
            detect_mime(GARBAGE, Some("x.pdf"), Some("text/csv")),
            "application/pdf"
        );
    }

    #[test]
    fn detect_then_route_end_to_end() {
        let router = PipelineRouter::new(builtin_pipelines());
        let cases: [(&[u8], Option<&str>, &str); 5] = [
            (PDF_HEAD, None, "builtin.pdf"),
            (b"id,name\n1,x\n", Some("a.csv"), "builtin.csv"),
            (b"# Hi\n", Some("a.md"), "builtin.markdown"),
            (b"{\"documents\":[]}", None, "builtin.json"),
            (b"just some prose", None, "builtin.text"),
        ];
        for (data, filename, uid) in cases {
            let mime = detect_mime(data, filename, None);
            let m = router
                .resolve(RouteRequest {
                    mime: &mime,
                    filename,
                    project_id: None,
                })
                .unwrap_or_else(|| panic!("no route for {mime}"));
            assert_eq!(m.pipeline.uid, uid);
        }
        let docx = fake_zip("word/document.xml");
        let mime = detect_mime(&docx, None, None);
        assert_eq!(
            router
                .resolve(RouteRequest {
                    mime: &mime,
                    filename: None,
                    project_id: None
                })
                .unwrap()
                .pipeline
                .uid,
            "builtin.word"
        );
    }

    #[test]
    fn route_types_serialize() {
        let router = PipelineRouter::new(builtin_pipelines());
        let m = router
            .resolve(RouteRequest {
                mime: "text/csv",
                filename: None,
                project_id: None,
            })
            .unwrap();
        let json = serde_json::to_value(m).unwrap();
        assert_eq!(json["pipeline"]["uid"], "builtin.csv");
        let round: PipelineRouter =
            serde_json::from_str(&serde_json::to_string(&router).unwrap()).unwrap();
        assert_eq!(round, router);
    }

    #[test]
    fn media_type_aliases_are_canonicalised() {
        // `infer` reports RIFF/WAVE as audio/x-wav and mime_guess maps .m4a to
        // audio/m4a; SPEC 9's builtin.audio trigger lists audio/wav and audio/mp4, so
        // without canonicalisation an ordinary upload would 415.
        assert_eq!(canonical_mime("audio/x-wav"), mime::WAV);
        assert_eq!(canonical_mime("audio/wave"), mime::WAV);
        assert_eq!(canonical_mime("audio/mp3"), mime::MP3);
        assert_eq!(canonical_mime("audio/m4a"), mime::M4A);
        assert_eq!(canonical_mime("audio/x-flac"), mime::FLAC);
        assert_eq!(canonical_mime("image/jpg"), "image/jpeg");
        // Anything already canonical is untouched.
        assert_eq!(canonical_mime(mime::PDF), mime::PDF);
        assert_eq!(canonical_mime("video/mp4"), "video/mp4");
    }

    #[test]
    fn a_real_wav_file_routes_to_the_audio_pipeline() {
        // Minimal RIFF/WAVE header, which is what `infer` matches on.
        let mut wav = Vec::new();
        wav.extend_from_slice(b"RIFF");
        wav.extend_from_slice(&36u32.to_le_bytes());
        wav.extend_from_slice(b"WAVEfmt ");
        wav.extend_from_slice(&16u32.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&1u16.to_le_bytes());
        wav.extend_from_slice(&16_000u32.to_le_bytes());
        wav.extend_from_slice(&32_000u32.to_le_bytes());
        wav.extend_from_slice(&2u16.to_le_bytes());
        wav.extend_from_slice(&16u16.to_le_bytes());
        wav.extend_from_slice(b"data");
        wav.extend_from_slice(&0u32.to_le_bytes());

        let detected = detect_mime(&wav, Some("clip.wav"), None);
        assert_eq!(
            detected,
            mime::WAV,
            "raw detection was {:?}",
            detect_mime_raw(&wav, Some("clip.wav"), None)
        );

        let router = PipelineRouter::new(builtin_pipelines());
        let m = router
            .resolve(RouteRequest {
                mime: &detected,
                filename: Some("clip.wav"),
                project_id: None,
            })
            .expect("a wav upload must route somewhere");
        assert_eq!(m.pipeline.uid, "builtin.audio");
    }

    #[test]
    fn an_m4a_file_routes_to_the_audio_pipeline() {
        let detected = detect_mime(b"\x00\x00", Some("voice.m4a"), None);
        assert_eq!(detected, mime::M4A);
        let router = PipelineRouter::new(builtin_pipelines());
        let m = router
            .resolve(RouteRequest {
                mime: &detected,
                filename: Some("voice.m4a"),
                project_id: None,
            })
            .expect("an m4a upload must route somewhere");
        assert_eq!(m.pipeline.uid, "builtin.audio");
    }
}
