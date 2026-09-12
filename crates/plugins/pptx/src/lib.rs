//! # `pptx_extractor`
//!
//! Built-in meili-ingest plugin that extracts the text of a PowerPoint `.pptx`
//! presentation. SPEC §18 (question 5) left this plugin open because there is no
//! obvious PPTX crate; it suggested LibreOffice headless or a gRPC container. Neither
//! is needed: a `.pptx` is an OOXML package — a plain zip archive of XML parts — so
//! this crate parses it directly with [`zip`](https://crates.io/crates/zip) and
//! [`quick-xml`](https://crates.io/crates/quick-xml). No external process, no GPU, no
//! network.
//!
//! ## The OOXML layout we parse
//!
//! ```text
//! presentation.pptx (zip)
//! ├── [Content_Types].xml
//! ├── ppt/
//! │   ├── presentation.xml
//! │   ├── slides/
//! │   │   ├── slide1.xml          ← one part per slide, N is an arbitrary part id
//! │   │   ├── slide2.xml
//! │   │   └── _rels/slide1.xml.rels
//! │   └── notesSlides/
//! │       └── notesSlide1.xml     ← speaker notes, numbered like the slides
//! └── docProps/…
//! ```
//!
//! Every other entry (layouts, masters, themes, media, rels) is ignored.
//!
//! Slide parts are ordered **numerically** by the `N` in `slide<N>.xml`, never
//! lexicographically — `slide10.xml` must come after `slide9.xml`.
//!
//! Inside a slide part the interesting shape is:
//!
//! ```xml
//! <p:sld>
//!   <p:cSld><p:spTree>
//!     <p:sp>                                  <!-- a shape -->
//!       <p:nvSpPr><p:nvPr>
//!         <p:ph type="title"/>                <!-- placeholder kind -->
//!       </p:nvPr></p:nvSpPr>
//!       <p:txBody>
//!         <a:p><a:r><a:t>Some text</a:t></a:r></a:p>   <!-- a paragraph -->
//!       </p:txBody>
//!     </p:sp>
//!   </p:spTree></p:cSld>
//! </p:sld>
//! ```
//!
//! * all visible text lives in `<a:t>` elements,
//! * `<a:p>` is a paragraph — one bullet, i.e. one output line,
//! * `<a:br>` is a soft line break inside a paragraph,
//! * `<p:ph type="…"/>` says what the shape is (`title`, `ctrTitle`, `body`,
//!   `sldNum`, `ftr`, `dt`, `sldImg`, …).
//!
//! Element names are matched on their **local name**, so any namespace prefix works
//! (`a:t` and `foo:t` parse identically). Entity and character references
//! (`&amp;`, `&#233;`) are decoded.
//!
//! ## Title heuristic
//!
//! The slide title is the text of the first shape carrying a
//! `<p:ph type="title"/>` or `<p:ph type="ctrTitle"/>` placeholder. Attribution is
//! reliable because we track shape nesting (`<p:sp>` … `</p:sp>`, including shapes
//! inside groups) while streaming, so a placeholder always belongs to the shape it is
//! nested in. When a slide has no title placeholder — blank layouts, decks built by
//! exporters that drop placeholder metadata — we fall back to the slide's first
//! non-empty line.
//!
//! Chrome placeholders (slide number, date, footer, and the notes page's slide
//! thumbnail) are dropped from the extracted text: they carry template artefacts
//! (`‹#›`) rather than content.

use std::collections::BTreeMap;
use std::io::{Cursor, Read};

use meili_ingest_plugin_sdk::prelude::*;
use quick_xml::events::{BytesRef, BytesStart, Event};
use quick_xml::{Reader, XmlVersion};
use serde::Deserialize;
use zip::ZipArchive;

/// Plugin name, as referenced by `steps[].plugin`.
pub const NAME: &str = "pptx_extractor";

/// MIME type of `.pptx` files.
const PPTX_MIME: &str = "application/vnd.openxmlformats-officedocument.presentationml.presentation";

/// Zip path prefix of a slide part (`ppt/slides/slide<N>.xml`).
const SLIDE_PREFIX: &str = "ppt/slides/slide";

/// Zip path prefix of a speaker-notes part (`ppt/notesSlides/notesSlide<N>.xml`).
const NOTES_PREFIX: &str = "ppt/notesSlides/notesSlide";

/// Suffix of both part kinds.
const XML_SUFFIX: &str = ".xml";

/// Number of slides between two heartbeats.
const HEARTBEAT_EVERY: usize = 10;

/// Placeholder kinds that hold template chrome rather than content.
const CHROME_PLACEHOLDERS: [&str; 4] = ["sldnum", "dt", "ftr", "sldimg"];

/// Placeholder kinds that mark the slide title.
const TITLE_PLACEHOLDERS: [&str; 2] = ["title", "ctrtitle"];

/// The PPTX extractor plugin. Stateless; construct with [`PptxExtractorPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct PptxExtractorPlugin;

impl PptxExtractorPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Step configuration for [`PptxExtractorPlugin`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    /// One document per slide (default) instead of one per deck.
    per_slide: bool,
    /// Extract speaker notes into `fields.notes`.
    include_notes: bool,
    /// Also append the speaker notes to `content`, after a blank line.
    notes_in_content: bool,
    /// Drop slides whose text *and* notes are empty.
    skip_empty_slides: bool,
    /// Stop after this many slides.
    max_slides: Option<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_slide: true,
            include_notes: true,
            notes_in_content: false,
            skip_empty_slides: true,
            max_slides: None,
        }
    }
}

fn parse_config(value: serde_json::Value) -> Result<Config, PluginError> {
    if value.is_null() {
        return Ok(Config::default());
    }
    serde_json::from_value(value).map_err(|e| PluginError::InvalidConfig(e.to_string()))
}

// ---------------------------------------------------------------------------
// Blob helpers (same shape as the other extractors)
// ---------------------------------------------------------------------------

/// Meilisearch-safe id derived from the blob's filename stem (a UUID when unknown).
fn base_id(blob: &Blob) -> String {
    let stem = blob
        .filename
        .as_deref()
        .and_then(|f| std::path::Path::new(f).file_stem())
        .and_then(|s| s.to_str())
        .unwrap_or("");
    sanitize_id(stem)
}

/// Provenance metadata shared by every document produced from `blob`.
fn base_meta(blob: &Blob) -> DocumentMeta {
    DocumentMeta {
        source: blob.filename.clone(),
        filename: blob.filename.clone(),
        mime: Some(blob.mime.clone()),
        ..DocumentMeta::default()
    }
}

// ---------------------------------------------------------------------------
// Zip part discovery
// ---------------------------------------------------------------------------

/// `("ppt/slides/slide12.xml", "ppt/slides/slide")` → `Some(12)`.
///
/// Returns `None` for anything else, in particular for the sibling
/// `ppt/slides/_rels/slide12.xml.rels` relationship parts.
fn part_number(name: &str, prefix: &str) -> Option<u32> {
    let digits = name.strip_prefix(prefix)?.strip_suffix(XML_SUFFIX)?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// Largest decompressed size accepted for a single XML part.
///
/// Uploads arrive from untrusted tenants and deflate reaches ratios of ~1000:1, so a
/// small `.pptx` can expand to gigabytes ("zip bomb") and exhaust a worker. Real slide
/// XML is a few hundred kilobytes; 64 MiB is far past any legitimate deck.
pub const MAX_PART_BYTES: u64 = 64 * 1024 * 1024;

/// Largest total decompressed size accepted across all parts of one deck.
pub const MAX_TOTAL_BYTES: u64 = 256 * 1024 * 1024;

/// Read one zip entry as text (lossy UTF-8; OOXML parts are always UTF-8).
///
/// Decompression is bounded twice: the entry's declared size is checked first (cheap,
/// and rejects an honest bomb before any work), then the read itself is capped, which
/// also catches a header that lies about the size. `budget` is the remaining
/// whole-deck allowance and is decremented by what was actually read.
fn read_part<R: Read + std::io::Seek>(
    archive: &mut ZipArchive<R>,
    name: &str,
    budget: &mut u64,
) -> Result<String, PluginError> {
    let mut entry = archive
        .by_name(name)
        .map_err(|e| PluginError::NonRetryable(format!("cannot read `{name}` from pptx: {e}")))?;

    let declared = entry.size();
    if declared > MAX_PART_BYTES {
        return Err(PluginError::NonRetryable(format!(
            "pptx part `{name}` declares {declared} bytes, above the {MAX_PART_BYTES} byte limit; \
             refusing to decompress (possible zip bomb)"
        )));
    }

    let cap = MAX_PART_BYTES.min(*budget);
    let mut buf = Vec::new();
    // `take(cap + 1)` lets us notice the overflow instead of silently truncating.
    entry
        .by_ref()
        .take(cap.saturating_add(1))
        .read_to_end(&mut buf)
        .map_err(|e| PluginError::NonRetryable(format!("cannot read `{name}` from pptx: {e}")))?;
    if buf.len() as u64 > cap {
        return Err(PluginError::NonRetryable(format!(
            "pptx part `{name}` decompresses past the {cap} byte limit for this deck; \
             refusing to continue (possible zip bomb)"
        )));
    }
    *budget -= buf.len() as u64;
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

// ---------------------------------------------------------------------------
// XML parsing
// ---------------------------------------------------------------------------

/// One `<p:sp>` shape (or the implicit root shape holding text that lives outside
/// any shape, e.g. a table inside a `<p:graphicFrame>`).
#[derive(Debug, Default)]
struct Shape {
    /// Lowercased `<p:ph type="…"/>` value when the shape is a placeholder.
    placeholder: Option<String>,
    /// One entry per `<a:p>` paragraph, trimmed, empty ones dropped.
    lines: Vec<String>,
}

impl Shape {
    fn is_chrome(&self) -> bool {
        self.placeholder
            .as_deref()
            .is_some_and(|p| CHROME_PLACEHOLDERS.contains(&p))
    }

    fn is_title(&self) -> bool {
        self.placeholder
            .as_deref()
            .is_some_and(|p| TITLE_PLACEHOLDERS.contains(&p))
    }
}

/// Value of the `type` attribute, matched on its local name so a prefixed
/// `p14:type` would work too. Lowercased for case-insensitive comparison.
fn placeholder_type(tag: &BytesStart<'_>) -> Option<String> {
    for attr in tag.attributes().flatten() {
        if attr.key.local_name().as_ref() == b"type" {
            return attr
                .normalized_value(XmlVersion::Explicit1_0)
                .ok()
                .map(|v| v.to_lowercase());
        }
    }
    None
}

/// Append a resolved `&entity;` / `&#char;` reference to `out`. Unknown entities are
/// dropped: a slide part cannot declare a DTD, so only the five predefined ones and
/// character references can legitimately appear.
fn push_reference(reference: &BytesRef<'_>, out: &mut String) {
    if let Ok(Some(c)) = reference.resolve_char_ref() {
        out.push(c);
        return;
    }
    let Ok(name) = reference.decode() else {
        return;
    };
    match name.as_ref() {
        "lt" => out.push('<'),
        "gt" => out.push('>'),
        "amp" => out.push('&'),
        "apos" => out.push('\''),
        "quot" => out.push('"'),
        _ => {}
    }
}

/// Stream one slide / notes part into shapes, in document order.
///
/// The parser is a flat event loop with a stack of shape indices, so shapes nested in
/// groups (`<p:grpSp>`) keep their own text and their own placeholder attribution.
/// Index 0 is an implicit root shape collecting text that belongs to no `<p:sp>`.
fn parse_part(xml: &str, part: &str) -> Result<Vec<Shape>, PluginError> {
    let mut reader = Reader::from_str(xml);
    let mut shapes: Vec<Shape> = vec![Shape::default()];
    let mut open: Vec<usize> = vec![0];
    // `<a:t>` can technically nest nothing, but a depth counter is cheaper than
    // asserting that and is robust against malformed input.
    let mut text_depth: usize = 0;
    let mut paragraph: Option<String> = None;

    let bad_xml = |e: quick_xml::Error| {
        PluginError::NonRetryable(format!("malformed XML in `{part}` of the pptx: {e}"))
    };

    loop {
        let event = reader.read_event().map_err(bad_xml)?;
        match event {
            Event::Start(ref tag) | Event::Empty(ref tag) => {
                let empty = matches!(event, Event::Empty(_));
                match tag.local_name().as_ref() {
                    b"sp" if !empty => {
                        shapes.push(Shape::default());
                        open.push(shapes.len() - 1);
                    }
                    b"ph" => {
                        if let Some(idx) = open.last().copied()
                            && let Some(shape) = shapes.get_mut(idx)
                            && shape.placeholder.is_none()
                        {
                            shape.placeholder = placeholder_type(tag);
                        }
                    }
                    b"p" if !empty => paragraph = Some(String::new()),
                    b"t" if !empty => text_depth += 1,
                    b"br" => {
                        if let Some(buf) = paragraph.as_mut() {
                            buf.push('\n');
                        }
                    }
                    _ => {}
                }
            }
            Event::End(ref tag) => match tag.local_name().as_ref() {
                b"sp" => {
                    if open.len() > 1 {
                        open.pop();
                    }
                }
                b"p" => {
                    if let Some(buf) = paragraph.take() {
                        let line = buf.trim();
                        if !line.is_empty()
                            && let Some(idx) = open.last().copied()
                            && let Some(shape) = shapes.get_mut(idx)
                        {
                            shape.lines.push(line.to_owned());
                        }
                    }
                }
                b"t" => text_depth = text_depth.saturating_sub(1),
                _ => {}
            },
            Event::Text(ref text) if text_depth > 0 => {
                if let Some(buf) = paragraph.as_mut() {
                    buf.push_str(&text.xml10_content().map_err(|e| {
                        PluginError::NonRetryable(format!("bad text in `{part}` of the pptx: {e}"))
                    })?);
                }
            }
            Event::CData(ref data) if text_depth > 0 => {
                if let Some(buf) = paragraph.as_mut() {
                    buf.push_str(&data.xml10_content().map_err(|e| {
                        PluginError::NonRetryable(format!("bad text in `{part}` of the pptx: {e}"))
                    })?);
                }
            }
            Event::GeneralRef(ref reference) if text_depth > 0 => {
                if let Some(buf) = paragraph.as_mut() {
                    push_reference(reference, buf);
                }
            }
            Event::Eof => break,
            _ => {}
        }
    }
    Ok(shapes)
}

/// The text of one slide, already stripped of template chrome.
#[derive(Debug, Default)]
struct SlideText {
    /// 1-based position of the slide in the deck.
    page: u32,
    /// Title placeholder text, falling back to the first non-empty line.
    title: Option<String>,
    /// Every line of the slide, bullets included, in reading order.
    lines: Vec<String>,
    /// Speaker notes, empty when absent or disabled.
    notes: String,
}

impl SlideText {
    fn from_shapes(page: u32, shapes: Vec<Shape>) -> Self {
        let mut title = None;
        let mut lines = Vec::new();
        for shape in shapes.iter().filter(|s| !s.is_chrome()) {
            if title.is_none() && shape.is_title() && !shape.lines.is_empty() {
                title = Some(shape.lines.join(" "));
            }
            lines.extend(shape.lines.iter().cloned());
        }
        // Fallback when the deck carries no title placeholder (blank layouts,
        // third-party exporters): the slide's first line reads as its title.
        if title.is_none() {
            title = lines.first().cloned();
        }
        Self {
            page,
            title,
            lines,
            notes: String::new(),
        }
    }

    fn body(&self) -> String {
        self.lines.join("\n")
    }

    /// `content` for this slide: body text plus, optionally, the notes.
    fn content(&self, notes_in_content: bool) -> String {
        let body = self.body();
        if !notes_in_content || self.notes.is_empty() {
            return body;
        }
        if body.is_empty() {
            return self.notes.clone();
        }
        format!("{body}\n\n{}", self.notes)
    }

    fn is_empty(&self) -> bool {
        self.lines.is_empty() && self.notes.is_empty()
    }
}

/// Notes text: every non-chrome shape of the notes page, one line per paragraph.
fn notes_text(shapes: Vec<Shape>) -> String {
    shapes
        .iter()
        .filter(|s| !s.is_chrome())
        .flat_map(|s| s.lines.iter().cloned())
        .collect::<Vec<_>>()
        .join("\n")
}

// ---------------------------------------------------------------------------
// Extraction
// ---------------------------------------------------------------------------

/// Whole synchronous parse: unzip, read the slide parts in numeric order, build the
/// documents. Runs inside [`run_blocking`] because zip inflation and XML parsing are
/// CPU-bound.
fn extract(ctx: &ActivityContext, blob: Blob, cfg: &Config) -> Result<Vec<Document>, PluginError> {
    let base = base_id(&blob);
    let meta = base_meta(&blob);

    let mut archive = ZipArchive::new(Cursor::new(blob.data)).map_err(|e| {
        PluginError::NonRetryable(format!("not a readable .pptx (OOXML zip) file: {e}"))
    })?;

    let mut slide_parts: Vec<(u32, String)> = archive
        .file_names()
        .filter_map(|n| part_number(n, SLIDE_PREFIX).map(|k| (k, n.to_owned())))
        .collect();
    // Numeric, NOT lexicographic: slide10.xml comes after slide9.xml.
    slide_parts.sort_by_key(|(number, _)| *number);
    if let Some(max) = cfg.max_slides {
        slide_parts.truncate(max as usize);
    }

    let notes_parts: BTreeMap<u32, String> = if cfg.include_notes {
        archive
            .file_names()
            .filter_map(|n| part_number(n, NOTES_PREFIX).map(|k| (k, n.to_owned())))
            .collect()
    } else {
        BTreeMap::new()
    };

    // Whole-deck decompression allowance, shared by slide and notes parts.
    let mut budget = MAX_TOTAL_BYTES;
    let mut slides = Vec::with_capacity(slide_parts.len());
    for (index, (number, part)) in slide_parts.iter().enumerate() {
        ctx.check_cancelled()?;
        if index > 0 && index.is_multiple_of(HEARTBEAT_EVERY) {
            ctx.heartbeat(format!("slide {}", index + 1));
        }
        let xml = read_part(&mut archive, part, &mut budget)?;
        // `meta.page` is the slide's position in the deck, not the part number: the
        // `N` in `slide<N>.xml` is an arbitrary part id that need not be contiguous.
        let mut slide = SlideText::from_shapes(index as u32 + 1, parse_part(&xml, part)?);
        // Notes are matched to slides by part number. The authoritative mapping lives
        // in `ppt/slides/_rels/slide<N>.xml.rels`, but PowerPoint and every exporter
        // we have seen number the notes part after its slide part.
        if let Some(notes_part) = notes_parts.get(number) {
            let notes_xml = read_part(&mut archive, notes_part, &mut budget)?;
            slide.notes = notes_text(parse_part(&notes_xml, notes_part)?);
        }
        slides.push(slide);
    }

    let docs = if cfg.per_slide {
        slides
            .iter()
            .filter(|s| !(cfg.skip_empty_slides && s.is_empty()))
            .map(|slide| {
                let mut doc = Document::with_id(
                    format!("{base}_s{}", slide.page),
                    slide.content(cfg.notes_in_content),
                );
                doc.title = slide.title.clone();
                doc.meta = meta.clone();
                doc.meta.page = Some(slide.page);
                doc.meta.section = slide.title.clone();
                if !slide.notes.is_empty() {
                    doc.fields.insert(
                        "notes".into(),
                        serde_json::Value::String(slide.notes.clone()),
                    );
                }
                doc
            })
            .collect()
    } else {
        // Whole-deck mode: slides joined by a blank line, all notes concatenated the
        // same way, the deck titled after its first titled slide.
        let content = join_non_empty(
            slides.iter().map(|s| s.content(cfg.notes_in_content)),
            "\n\n",
        );
        let notes = join_non_empty(slides.iter().map(|s| s.notes.clone()), "\n\n");
        if content.is_empty() && notes.is_empty() {
            Vec::new()
        } else {
            let mut doc = Document::with_id(base, content);
            doc.title = slides.iter().find_map(|s| s.title.clone());
            doc.meta = meta;
            if !notes.is_empty() {
                doc.fields
                    .insert("notes".into(), serde_json::Value::String(notes));
            }
            vec![doc]
        }
    };
    Ok(docs)
}

fn join_non_empty(parts: impl Iterator<Item = String>, sep: &str) -> String {
    parts
        .filter(|p| !p.is_empty())
        .collect::<Vec<_>>()
        .join(sep)
}

#[async_trait]
impl Plugin for PptxExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Extracts text from PowerPoint (.pptx) presentations by reading the OOXML \
                 slide parts directly: one document per slide (bullets on their own lines) \
                 with the title placeholder as title, plus speaker notes.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types([PPTX_MIME])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "per_slide": {
                        "type": "boolean",
                        "default": true,
                        "description": "One document per slide (`_meta.page` = slide number, id `<file>_s<n>`). When false, one document for the whole deck with slides joined by a blank line."
                    },
                    "include_notes": {
                        "type": "boolean",
                        "default": true,
                        "description": "Extract the speaker notes of each slide into `fields.notes`."
                    },
                    "notes_in_content": {
                        "type": "boolean",
                        "default": false,
                        "description": "Also append the speaker notes to `content`, after a blank line. Requires `include_notes`."
                    },
                    "skip_empty_slides": {
                        "type": "boolean",
                        "default": true,
                        "description": "Produce no document for slides whose text and notes are both empty."
                    },
                    "max_slides": {
                        "type": ["integer", "null"],
                        "minimum": 0,
                        "default": null,
                        "description": "Stop after this many slides. Unlimited when unset."
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
        let cfg = parse_config(config)?;
        let blob = input.into_bytes()?;

        let ctx = ctx.clone();
        let docs = run_blocking(move || extract(&ctx, blob, &cfg)).await??;

        tracing::debug!(plugin = NAME, documents = docs.len(), "extracted pptx");
        Ok(PluginOutput::Documents(docs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use zip::write::SimpleFileOptions;

    const CONTENT_TYPES: &str = concat!(
        r#"<?xml version="1.0" encoding="UTF-8"?>"#,
        r#"<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">"#,
        r#"<Default Extension="xml" ContentType="application/xml"/>"#,
        r#"</Types>"#
    );

    const SLD_NS: &str = concat!(
        r#" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main""#,
        r#" xmlns:p="http://schemas.openxmlformats.org/presentationml/2006/main""#
    );

    /// One `<a:p>` per non-empty line of `text`.
    fn paragraphs(text: &str) -> String {
        text.lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| format!("<a:p><a:r><a:t>{l}</a:t></a:r></a:p>"))
            .collect()
    }

    fn shape(placeholder: &str, text: &str) -> String {
        format!(
            concat!(
                r#"<p:sp><p:nvSpPr><p:cNvPr id="1" name="{0}"/><p:nvPr>"#,
                r#"<p:ph type="{0}"/></p:nvPr></p:nvSpPr>"#,
                "<p:txBody>{1}</p:txBody></p:sp>"
            ),
            placeholder,
            paragraphs(text)
        )
    }

    /// A slide part: an optional title placeholder plus a body placeholder.
    fn slide_xml(title: &str, body: &str) -> String {
        let mut shapes = String::new();
        if !title.is_empty() {
            shapes.push_str(&shape("title", title));
        }
        if !body.is_empty() {
            shapes.push_str(&shape("body", body));
        }
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><p:sld{SLD_NS}><p:cSld><p:spTree>{shapes}</p:spTree></p:cSld></p:sld>"#
        )
    }

    /// A notes part: the slide-image chrome placeholder plus the notes body.
    fn notes_xml(text: &str) -> String {
        let shapes = format!("{}{}", shape("sldImg", ""), shape("body", text));
        format!(
            r#"<?xml version="1.0" encoding="UTF-8"?><p:notes{SLD_NS}><p:cSld><p:spTree>{shapes}</p:spTree></p:cSld></p:notes>"#
        )
    }

    /// Zip up arbitrary `(part path, xml)` pairs into a minimal OOXML package.
    fn package(parts: &[(String, String)]) -> Vec<u8> {
        let mut writer = zip::ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default();
        writer.start_file("[Content_Types].xml", options).unwrap();
        writer.write_all(CONTENT_TYPES.as_bytes()).unwrap();
        for (name, xml) in parts {
            writer.start_file(name.clone(), options).unwrap();
            writer.write_all(xml.as_bytes()).unwrap();
        }
        writer.finish().unwrap().into_inner()
    }

    /// A deck of `(title, body)` slides. `body` may hold several lines (bullets).
    fn deck(slides: &[(&str, &str)]) -> Vec<u8> {
        let parts: Vec<(String, String)> = slides
            .iter()
            .enumerate()
            .map(|(i, (title, body))| {
                (
                    format!("{SLIDE_PREFIX}{}{XML_SUFFIX}", i + 1),
                    slide_xml(title, body),
                )
            })
            .collect();
        package(&parts)
    }

    /// A deck plus speaker notes, given as `(slide number, notes text)`.
    fn deck_with_notes(slides: &[(&str, &str)], notes: &[(u32, &str)]) -> Vec<u8> {
        let mut parts: Vec<(String, String)> = slides
            .iter()
            .enumerate()
            .map(|(i, (title, body))| {
                (
                    format!("{SLIDE_PREFIX}{}{XML_SUFFIX}", i + 1),
                    slide_xml(title, body),
                )
            })
            .collect();
        for (n, text) in notes {
            parts.push((format!("{NOTES_PREFIX}{n}{XML_SUFFIX}"), notes_xml(text)));
        }
        package(&parts)
    }

    fn input(bytes: Vec<u8>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            bytes,
            PPTX_MIME,
            Some("decks/Q3 all hands.pptx".into()),
        ))
    }

    async fn run(bytes: Vec<u8>, config: serde_json::Value) -> Vec<Document> {
        PptxExtractorPlugin::new()
            .execute(&ActivityContext::noop(), input(bytes), config)
            .await
            .unwrap()
            .into_documents()
            .unwrap()
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = PptxExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(m.content_types, vec![PPTX_MIME]);
        assert_eq!(m.config_schema["additionalProperties"], false);
        for (key, default) in [
            ("per_slide", serde_json::json!(true)),
            ("include_notes", serde_json::json!(true)),
            ("notes_in_content", serde_json::json!(false)),
            ("skip_empty_slides", serde_json::json!(true)),
            ("max_slides", serde_json::json!(null)),
        ] {
            assert_eq!(
                m.config_schema["properties"][key]["default"], default,
                "{key}"
            );
        }
    }

    #[tokio::test]
    async fn one_document_per_slide_with_pages_and_titles() {
        let docs = run(
            deck(&[
                ("Agenda", "Numbers\nRoadmap\nQ&amp;A"),
                ("Numbers", "Revenue up 12%"),
            ]),
            serde_json::json!({}),
        )
        .await;

        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, "Q3_all_hands_s1");
        assert_eq!(docs[0].title.as_deref(), Some("Agenda"));
        assert_eq!(docs[0].meta.page, Some(1));
        assert_eq!(docs[0].meta.section.as_deref(), Some("Agenda"));
        assert_eq!(
            docs[0].meta.filename.as_deref(),
            Some("decks/Q3 all hands.pptx")
        );
        assert_eq!(docs[0].meta.mime.as_deref(), Some(PPTX_MIME));
        // Bullets end up on their own lines, and `&amp;` is decoded.
        assert_eq!(docs[0].content, "Agenda\nNumbers\nRoadmap\nQ&A");
        assert_eq!(docs[1].id, "Q3_all_hands_s2");
        assert_eq!(docs[1].meta.page, Some(2));
        assert_eq!(docs[1].content, "Numbers\nRevenue up 12%");
    }

    #[tokio::test]
    async fn slides_are_ordered_numerically_not_lexicographically() {
        let slides: Vec<(String, String)> = (1..=12)
            .map(|n| (format!("Slide {n}"), format!("Body {n}")))
            .collect();
        let borrowed: Vec<(&str, &str)> = slides
            .iter()
            .map(|(t, b)| (t.as_str(), b.as_str()))
            .collect();

        let docs = run(deck(&borrowed), serde_json::json!({})).await;

        assert_eq!(docs.len(), 12);
        // Lexicographic sorting would put slide10/11/12 right after slide1.
        for (i, doc) in docs.iter().enumerate() {
            let n = i + 1;
            assert_eq!(doc.meta.page, Some(n as u32), "position {i}");
            assert_eq!(doc.id, format!("Q3_all_hands_s{n}"), "position {i}");
            assert_eq!(doc.title.as_deref(), Some(format!("Slide {n}").as_str()));
        }
        assert_eq!(docs[8].title.as_deref(), Some("Slide 9"));
        assert_eq!(docs[9].title.as_deref(), Some("Slide 10"));
    }

    #[tokio::test]
    async fn per_slide_false_joins_the_whole_deck() {
        let docs = run(
            deck(&[("One", "first"), ("Two", "second")]),
            serde_json::json!({"per_slide": false}),
        )
        .await;

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "Q3_all_hands");
        assert_eq!(docs[0].title.as_deref(), Some("One"));
        assert_eq!(docs[0].meta.page, None);
        assert_eq!(docs[0].content, "One\nfirst\n\nTwo\nsecond");
    }

    #[tokio::test]
    async fn speaker_notes_land_in_fields_and_optionally_in_content() {
        let bytes = deck_with_notes(
            &[("One", "first"), ("Two", "second")],
            &[(2, "Remember to demo\nthen pause")],
        );

        let docs = run(bytes.clone(), serde_json::json!({})).await;
        assert_eq!(docs.len(), 2);
        assert!(docs[0].fields.get("notes").is_none());
        assert_eq!(
            docs[1].fields.get("notes").and_then(|v| v.as_str()),
            Some("Remember to demo\nthen pause")
        );
        // Notes stay out of `content` by default.
        assert_eq!(docs[1].content, "Two\nsecond");

        let docs = run(bytes, serde_json::json!({"notes_in_content": true})).await;
        assert_eq!(
            docs[1].content,
            "Two\nsecond\n\nRemember to demo\nthen pause"
        );

        let docs = run(
            deck_with_notes(&[("One", "first")], &[(1, "hidden")]),
            serde_json::json!({"include_notes": false}),
        )
        .await;
        assert!(docs[0].fields.get("notes").is_none());
    }

    #[tokio::test]
    async fn empty_slides_are_skipped_unless_configured_otherwise() {
        let bytes = deck(&[("One", "first"), ("", ""), ("Three", "third")]);

        let docs = run(bytes.clone(), serde_json::json!({})).await;
        assert_eq!(docs.len(), 2);
        assert_eq!(
            docs.iter().map(|d| d.meta.page).collect::<Vec<_>>(),
            [Some(1), Some(3)]
        );

        let docs = run(bytes, serde_json::json!({"skip_empty_slides": false})).await;
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[1].meta.page, Some(2));
        assert_eq!(docs[1].content, "");
        assert_eq!(docs[1].title, None);
    }

    #[tokio::test]
    async fn max_slides_caps_extraction_and_empty_deck_yields_nothing() {
        let docs = run(
            deck(&[("One", "a"), ("Two", "b"), ("Three", "c")]),
            serde_json::json!({"max_slides": 2}),
        )
        .await;
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1].title.as_deref(), Some("Two"));

        // A package with no slide parts at all is not an error.
        assert!(run(package(&[]), serde_json::json!({})).await.is_empty());
        assert!(
            run(package(&[]), serde_json::json!({"per_slide": false}))
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn title_falls_back_to_the_first_line_without_a_placeholder() {
        // A slide whose only shape carries no `<p:ph>` at all.
        let xml = format!(
            concat!(
                r#"<?xml version="1.0" encoding="UTF-8"?><p:sld{0}><p:cSld><p:spTree>"#,
                r#"<p:sp><p:nvSpPr><p:cNvPr id="1" name="Free"/><p:nvPr/></p:nvSpPr>"#,
                r#"<p:txBody><a:p><a:r><a:t>Loose</a:t></a:r></a:p>"#,
                r#"<a:p><a:r><a:t>Second</a:t><a:br/><a:t>tail</a:t></a:r></a:p>"#,
                r#"</p:txBody></p:sp>"#,
                r#"</p:spTree></p:cSld></p:sld>"#
            ),
            SLD_NS
        );
        let bytes = package(&[(format!("{SLIDE_PREFIX}1{XML_SUFFIX}"), xml)]);

        let docs = run(bytes, serde_json::json!({})).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].title.as_deref(), Some("Loose"));
        assert_eq!(docs[0].content, "Loose\nSecond\ntail");
    }

    #[tokio::test]
    async fn non_zip_input_is_non_retryable() {
        let err = PptxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(b"PK\x03\x04 definitely not a zip".to_vec()),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn wrong_input_kind_is_invalid_input() {
        let err = PptxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unknown_or_ill_typed_config_is_invalid_config() {
        for config in [
            serde_json::json!({"per_slides": true}),
            serde_json::json!({"per_slide": "yes"}),
        ] {
            let err = PptxExtractorPlugin::new()
                .execute(
                    &ActivityContext::noop(),
                    input(deck(&[("One", "a")])),
                    config.clone(),
                )
                .await
                .unwrap_err();
            assert!(
                matches!(err, PluginError::InvalidConfig(_)),
                "{config}: got {err:?}"
            );
        }
    }

    #[tokio::test]
    async fn rejects_a_zip_bomb_instead_of_decompressing_it() {
        // A deck whose slide part declares (and delivers) far more than the per-part
        // limit must be refused: uploads come from untrusted tenants and deflate
        // compresses repetitive XML by ~1000:1.
        let oversized = (MAX_PART_BYTES + 1024) as usize;
        let mut payload = String::with_capacity(oversized + 128);
        payload.push_str(
            "<?xml version=\"1.0\"?><p:sld><p:cSld><p:spTree><p:sp><p:txBody><a:p><a:r><a:t>",
        );
        payload.push_str(&" ".repeat(oversized));
        payload.push_str("</a:t></a:r></a:p></p:txBody></p:sp></p:spTree></p:cSld></p:sld>");
        let bomb = package(&[("ppt/slides/slide1.xml".to_string(), payload)]);
        // The bomb is tiny on the wire but huge once decompressed.
        assert!(
            (bomb.len() as u64) < MAX_PART_BYTES / 10,
            "fixture should compress well, got {} bytes",
            bomb.len()
        );

        let err = PptxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Bytes(Blob::new(bomb, PPTX_MIME, Some("bomb.pptx".into()))),
                serde_json::json!({}),
            )
            .await
            .expect_err("a zip bomb must be rejected");
        match err {
            PluginError::NonRetryable(m) => {
                assert!(m.contains("zip bomb"), "unexpected message: {m}")
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
