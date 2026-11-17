//! # `pdf_extractor`
//!
//! Built-in meili-ingest plugin that turns a PDF into text [`Document`]s using the
//! [`pdf-extract`](https://crates.io/crates/pdf-extract) crate.
//!
//! * `per_page: true` (default) → one document per non-empty page, `meta.page` set
//!   to the 1-based page number and id `<filename_stem>_p<page>`.
//! * `per_page: false` → a single document with the whole text, id `<filename_stem>`.
//! * `max_pages` caps the number of pages that are read (in both modes): pages past
//!   the cap are never rendered.
//!
//! Extracted text is whitespace-normalised: runs of blanks collapse into one space,
//! lines are trimmed and at most one blank line separates blocks.
//!
//! Every page whose text is extracted is reported as one `UsageUnits::pages` unit,
//! blank pages included: extracting a page is the work, whether or not it yields a
//! document.

use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;

/// Plugin name, as referenced by `steps[].plugin`.
pub const NAME: &str = "pdf_extractor";

/// Number of pages between two heartbeats.
const HEARTBEAT_EVERY: u32 = 10;

/// The PDF extractor plugin. Stateless; construct with [`PdfExtractorPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct PdfExtractorPlugin;

impl PdfExtractorPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration for [`PdfExtractorPlugin`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    /// Emit one document per page (default) instead of one for the whole file.
    per_page: bool,
    /// Stop after this many pages.
    max_pages: Option<u32>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            per_page: true,
            max_pages: None,
        }
    }
}

/// Extract per-page text off the async runtime (`pdf-extract` is synchronous and
/// CPU-bound; running it inline would block a worker thread and stall heartbeats).
///
/// With `max_pages`, pages past the cap are never rendered.
async fn extract_pages(data: Vec<u8>, max_pages: Option<u32>) -> Result<Vec<String>, PluginError> {
    run_blocking(move || pages_text(&data, max_pages))
        .await?
        .map_err(pdf_error)
}

/// The text of the first `max_pages` pages (all pages when unset).
///
/// This is `pdf_extract::extract_text_from_mem_by_pages` with an early stop: that
/// function renders every page before returning, so a cap applied afterwards bounds
/// the output but not the work. Same behaviour otherwise: an encrypted document is
/// opened with the empty password, and reading stops quietly at the first page that
/// cannot be rendered, keeping the pages before it.
fn pages_text(
    data: &[u8],
    max_pages: Option<u32>,
) -> Result<Vec<String>, pdf_extract::OutputError> {
    let mut doc = pdf_extract::Document::load_mem(data)?;
    if doc.is_encrypted() {
        doc.decrypt("")
            .map_err(pdf_extract::OutputError::PdfError)?;
    }
    let mut pages = Vec::new();
    let mut page_num = 1;
    while max_pages.is_none_or(|max| page_num <= max) {
        let mut text = String::new();
        let rendered = {
            let mut output = pdf_extract::PlainTextOutput::new(&mut text);
            pdf_extract::output_doc_page(&doc, &mut output, page_num)
        };
        if rendered.is_err() {
            break;
        }
        pages.push(text);
        page_num += 1;
    }
    Ok(pages)
}

/// Extract the whole document's text off the async runtime, with the number of pages
/// it came from (see [`whole_text_and_pages`]).
async fn extract_whole(data: Vec<u8>) -> Result<(String, u64), PluginError> {
    run_blocking(move || whole_text_and_pages(&data))
        .await?
        .map_err(pdf_error)
}

/// The whole document's text and its number of pages, from one parse.
///
/// Does what `pdf_extract::extract_text_from_mem` does (load, decrypt with the empty
/// password when encrypted, write every page as plain text), keeping the parsed
/// document to read the page count from its page tree: whole-document text has no
/// page markers.
fn whole_text_and_pages(data: &[u8]) -> Result<(String, u64), pdf_extract::OutputError> {
    let mut doc = pdf_extract::Document::load_mem(data)?;
    if doc.is_encrypted() {
        doc.decrypt("")?;
    }
    let mut text = String::new();
    pdf_extract::output_doc(&doc, &mut pdf_extract::PlainTextOutput::new(&mut text))?;
    Ok((text, doc.get_pages().len() as u64))
}

fn parse_config(value: serde_json::Value) -> Result<Config, PluginError> {
    if value.is_null() {
        return Ok(Config::default());
    }
    serde_json::from_value(value).map_err(|e| PluginError::InvalidConfig(e.to_string()))
}

/// Meilisearch-safe id derived from the blob's filename stem (a UUID when unknown).
fn base_id(blob: &Blob) -> String {
    blob.id_stem("")
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

/// Collapse runs of blanks, trim every line and squeeze blank lines down to one.
fn normalize_whitespace(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut pending_blank = false;
    for raw_line in text.split(['\n', '\x0c']) {
        let line = raw_line.split_whitespace().collect::<Vec<_>>().join(" ");
        if line.is_empty() {
            pending_blank = !out.is_empty();
            continue;
        }
        if pending_blank {
            out.push_str("\n\n");
            pending_blank = false;
        } else if !out.is_empty() {
            out.push('\n');
        }
        out.push_str(&line);
    }
    out
}

fn pdf_error(e: pdf_extract::OutputError) -> PluginError {
    PluginError::NonRetryable(format!("failed to parse PDF: {e}"))
}

#[async_trait]
impl Plugin for PdfExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Extracts text from PDF files, one document per page by default, \
                 with the page number in `_meta.page`.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["application/pdf"])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "per_page": {
                        "type": "boolean",
                        "default": true,
                        "description": "Emit one document per page instead of one per file."
                    },
                    "max_pages": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "default": null,
                        "description": "Stop reading after this many pages."
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
        let base = base_id(&blob);
        let meta = base_meta(&blob);

        if cfg.per_page {
            let pages = extract_pages(blob.data, cfg.max_pages).await?;
            let mut docs = Vec::with_capacity(pages.len());
            // Pages whose text was extracted. Counted even when the page turns out to
            // be blank and produces no document: rendering it is the billable work.
            let mut pages_read = 0u64;
            for (idx, text) in pages.into_iter().enumerate() {
                ctx.check_cancelled()?;
                let page = u32::try_from(idx).unwrap_or(u32::MAX).saturating_add(1);
                if cfg.max_pages.is_some_and(|max| page > max) {
                    break;
                }
                pages_read += 1;
                if page.is_multiple_of(HEARTBEAT_EVERY) {
                    ctx.heartbeat(format!("page {page}"));
                }
                let text = normalize_whitespace(&text);
                if text.is_empty() {
                    continue;
                }
                let mut doc = Document::with_id(format!("{base}_p{page}"), text);
                doc.meta = meta.clone();
                doc.meta.page = Some(page);
                docs.push(doc);
            }
            ctx.record_usage(UsageUnits::pages(pages_read));
            tracing::debug!(plugin = NAME, pages = docs.len(), "extracted pdf pages");
            return Ok(PluginOutput::Documents(docs));
        }

        let text = match cfg.max_pages {
            // Whole-document mode with a page cap: read page by page and join.
            Some(max) => {
                let pages = extract_pages(blob.data, Some(max)).await?;
                let mut parts = Vec::new();
                let mut pages_read = 0u64;
                for (idx, page) in pages.into_iter().enumerate() {
                    ctx.check_cancelled()?;
                    if idx >= max as usize {
                        break;
                    }
                    pages_read += 1;
                    if (idx + 1).is_multiple_of(HEARTBEAT_EVERY as usize) {
                        ctx.heartbeat(format!("page {}", idx + 1));
                    }
                    let page = normalize_whitespace(&page);
                    if !page.is_empty() {
                        parts.push(page);
                    }
                }
                ctx.record_usage(UsageUnits::pages(pages_read));
                parts.join("\n\n")
            }
            None => {
                let (text, pages) = extract_whole(blob.data).await?;
                ctx.record_usage(UsageUnits::pages(pages));
                normalize_whitespace(&text)
            }
        };
        ctx.check_cancelled()?;
        if text.is_empty() {
            return Ok(PluginOutput::Documents(vec![]));
        }
        let mut doc = Document::with_id(base, text);
        doc.meta = meta;
        Ok(PluginOutput::Documents(vec![doc]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lopdf::content::{Content, Operation};
    use lopdf::{Document as PdfDocument, Object, Stream, dictionary};

    /// Build a small valid PDF with one page per entry of `pages`, each containing the text.
    fn make_pdf(pages: &[&str]) -> Vec<u8> {
        make_pdf_with(pages, usize::MAX)
    }

    /// [`make_pdf`], except that page `broken` (0-based) has no `MediaBox`, which makes
    /// `pdf-extract` panic when it renders that page.
    fn make_pdf_with(pages: &[&str], broken: usize) -> Vec<u8> {
        let mut doc = PdfDocument::with_version("1.5");
        let pages_id = doc.new_object_id();
        let font_id = doc.add_object(dictionary! {
            "Type" => "Font",
            "Subtype" => "Type1",
            "BaseFont" => "Helvetica",
        });
        let resources_id = doc.add_object(dictionary! {
            "Font" => dictionary! { "F1" => font_id },
        });
        let mut kids = Vec::new();
        for (index, text) in pages.iter().enumerate() {
            let content = Content {
                operations: vec![
                    Operation::new("BT", vec![]),
                    Operation::new("Tf", vec!["F1".into(), 24.into()]),
                    Operation::new("Td", vec![72.into(), 700.into()]),
                    Operation::new("Tj", vec![Object::string_literal(*text)]),
                    Operation::new("ET", vec![]),
                ],
            };
            let content_id = doc.add_object(Stream::new(dictionary! {}, content.encode().unwrap()));
            let mut page = dictionary! {
                "Type" => "Page",
                "Parent" => pages_id,
                "Contents" => content_id,
                "Resources" => resources_id,
            };
            if index != broken {
                page.set("MediaBox", vec![0.into(), 0.into(), 595.into(), 842.into()]);
            }
            let page_id = doc.add_object(page);
            kids.push(page_id.into());
        }
        let count = kids.len() as i64;
        doc.objects.insert(
            pages_id,
            Object::Dictionary(dictionary! {
                "Type" => "Pages",
                "Kids" => kids,
                "Count" => count,
            }),
        );
        let catalog_id = doc.add_object(dictionary! {
            "Type" => "Catalog",
            "Pages" => pages_id,
        });
        doc.trailer.set("Root", catalog_id);
        let mut out = Vec::new();
        doc.save_to(&mut out).unwrap();
        out
    }

    fn input(bytes: Vec<u8>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            bytes,
            "application/pdf",
            Some("Annual Report.pdf".into()),
        ))
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = PdfExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(m.content_types, vec!["application/pdf"]);
        assert!(m.config_schema["properties"]["per_page"].is_object());
        assert!(m.config_schema["properties"]["max_pages"].is_object());
    }

    #[tokio::test]
    async fn extracts_one_document_per_page_by_default() {
        let pdf = make_pdf(&["Hello first page", "Second page here"]);
        let out = PdfExtractorPlugin::new()
            .execute(&ActivityContext::noop(), input(pdf), serde_json::json!({}))
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].id, "Annual_Report_p1");
        assert_eq!(docs[0].meta.page, Some(1));
        assert_eq!(docs[0].meta.filename.as_deref(), Some("Annual Report.pdf"));
        assert_eq!(docs[0].meta.mime.as_deref(), Some("application/pdf"));
        assert!(
            docs[0].content.contains("Hello"),
            "content was {:?}",
            docs[0].content
        );
        assert!(
            docs[0].content.contains("first"),
            "content was {:?}",
            docs[0].content
        );
        assert_eq!(docs[1].id, "Annual_Report_p2");
        assert_eq!(docs[1].meta.page, Some(2));
        assert!(
            docs[1].content.contains("Second"),
            "content was {:?}",
            docs[1].content
        );
    }

    #[tokio::test]
    async fn whole_document_mode_and_max_pages() {
        let pdf = make_pdf(&["Alpha page", "Beta page", "Gamma page"]);
        let plugin = PdfExtractorPlugin::new();
        let ctx = ActivityContext::noop();

        let out = plugin
            .execute(
                &ctx,
                input(pdf.clone()),
                serde_json::json!({"per_page": false}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "Annual_Report");
        assert_eq!(docs[0].meta.page, None);
        assert!(docs[0].content.contains("Alpha") && docs[0].content.contains("Gamma"));

        let out = plugin
            .execute(
                &ctx,
                input(pdf.clone()),
                serde_json::json!({"max_pages": 2}),
            )
            .await
            .unwrap();
        assert_eq!(out.document_count(), 2);

        let out = plugin
            .execute(
                &ctx,
                input(pdf),
                serde_json::json!({"per_page": false, "max_pages": 1}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 1);
        assert!(docs[0].content.contains("Alpha"));
        assert!(!docs[0].content.contains("Beta"));
    }

    /// `max_pages` bounds the work, not just the output: pages past the cap are never
    /// rendered, so a page that would make `pdf-extract` fail is not reached.
    #[tokio::test]
    async fn pages_past_max_pages_are_not_read() {
        let pdf = make_pdf_with(&["Alpha page", "Beta page", "Gamma page"], 2);
        let plugin = PdfExtractorPlugin::new();

        let ctx = ActivityContext::noop();
        let out = plugin
            .execute(
                &ctx,
                input(pdf.clone()),
                serde_json::json!({"max_pages": 2}),
            )
            .await
            .unwrap();
        assert_eq!(out.document_count(), 2);
        assert_eq!(ctx.usage().pages, 2);

        let out = plugin
            .execute(
                &ActivityContext::noop(),
                input(pdf),
                serde_json::json!({"per_page": false, "max_pages": 2}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert!(docs[0].content.contains("Beta"), "{:?}", docs[0].content);
    }

    #[tokio::test]
    async fn records_one_page_unit_per_extracted_page() {
        let pdf = make_pdf(&["Alpha page", "Beta page", "Gamma page"]);
        let plugin = PdfExtractorPlugin::new();

        // Per page (the default): one unit per page of the real document.
        let ctx = ActivityContext::noop();
        let out = plugin
            .execute(&ctx, input(pdf.clone()), serde_json::json!({}))
            .await
            .unwrap();
        assert_eq!(out.document_count(), 3);
        assert_eq!(ctx.usage().pages, 3);
        assert_eq!(ctx.usage().llm_requests, 0);

        // Capped: only the pages actually read are billed.
        let ctx = ActivityContext::noop();
        plugin
            .execute(
                &ctx,
                input(pdf.clone()),
                serde_json::json!({"max_pages": 2}),
            )
            .await
            .unwrap();
        assert_eq!(ctx.usage().pages, 2);

        // Whole-document mode still knows how many pages it consumed.
        let ctx = ActivityContext::noop();
        plugin
            .execute(
                &ctx,
                input(pdf.clone()),
                serde_json::json!({"per_page": false}),
            )
            .await
            .unwrap();
        assert_eq!(ctx.usage().pages, 3);

        let ctx = ActivityContext::noop();
        plugin
            .execute(
                &ctx,
                input(pdf),
                serde_json::json!({"per_page": false, "max_pages": 1}),
            )
            .await
            .unwrap();
        assert_eq!(ctx.usage().pages, 1);
    }

    #[tokio::test]
    async fn a_failed_extraction_records_no_pages() {
        let ctx = ActivityContext::noop();
        PdfExtractorPlugin::new()
            .execute(
                &ctx,
                input(b"%PDF-1.4 this is not really a pdf".to_vec()),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(ctx.usage().is_empty());
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let pdf = make_pdf(&["x"]);
        let err = PdfExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(pdf.clone()),
                serde_json::json!({"per_page": "yes"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
        let err = PdfExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(pdf),
                serde_json::json!({"unknown_key": 1}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn wrong_input_kind_is_invalid_input() {
        let err = PdfExtractorPlugin::new()
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
    async fn corrupt_pdf_is_non_retryable() {
        let err = PdfExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(b"%PDF-1.4 this is not really a pdf".to_vec()),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "got {err:?}");
    }

    #[test]
    fn single_parse_matches_pdf_extract() {
        let pdf = make_pdf(&["First page", "Second page", "Third page"]);
        let (text, pages) = whole_text_and_pages(&pdf).unwrap();
        assert_eq!(text, pdf_extract::extract_text_from_mem(&pdf).unwrap());
        assert_eq!(pages, 3);
        assert!(whole_text_and_pages(b"%PDF-1.4 not a pdf").is_err());
    }

    #[test]
    fn whitespace_normalisation() {
        assert_eq!(
            normalize_whitespace("  a   b \n\n\n\n c\t d  \n"),
            "a b\n\nc d"
        );
        assert_eq!(normalize_whitespace("\n\n  \n"), "");
        assert_eq!(normalize_whitespace("x\x0cy"), "x\ny");
    }
}
