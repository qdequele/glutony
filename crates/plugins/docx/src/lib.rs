//! # `docx_extractor`
//!
//! Built-in meili-ingest plugin that extracts the text of a Word `.docx` file with
//! [`docx-rs`](https://crates.io/crates/docx-rs).
//!
//! Paragraphs are joined with a blank line (`\n\n`); tables are rendered as
//! tab-separated rows. By default one [`Document`] is produced per file. With
//! `split_on_headings: true` the body is cut into sections at every paragraph whose
//! style id starts with `Heading` (e.g. `Heading1`, `Heading2`); each section becomes
//! a document whose `title` and `_meta.section` carry the heading text.
//!
//! The document title comes from the core properties when present, falling back to
//! the first paragraph styled `Title`.

use docx_rs::{
    DocumentChild, Docx, InsertChild, Paragraph, ParagraphChild, RunChild, StructuredDataTagChild,
    Table, TableCellContent, TableChild, TableRowChild,
};
use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;

/// Plugin name, as referenced by `steps[].plugin`.
pub const NAME: &str = "docx_extractor";

/// MIME type of `.docx` files.
const DOCX_MIME: &str = "application/vnd.openxmlformats-officedocument.wordprocessingml.document";

/// Number of blocks between two heartbeats.
const HEARTBEAT_EVERY: usize = 50;

/// The DOCX extractor plugin. Stateless; construct with [`DocxExtractorPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct DocxExtractorPlugin;

impl DocxExtractorPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration for [`DocxExtractorPlugin`].
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    /// Split the body into one document per `Heading*` paragraph.
    split_on_headings: bool,
}

fn parse_config(value: serde_json::Value) -> Result<Config, PluginError> {
    if value.is_null() {
        return Ok(Config::default());
    }
    serde_json::from_value(value).map_err(|e| PluginError::InvalidConfig(e.to_string()))
}

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

/// One body-level block of the document, in reading order.
#[derive(Debug)]
enum Block {
    /// A paragraph with its style id (`Heading1`, `Title`, `Normal`, ...) and text.
    Paragraph { style: Option<String>, text: String },
    /// A table rendered as tab-separated rows.
    Table(String),
}

fn paragraph_children_text(children: &[ParagraphChild], out: &mut String) {
    for child in children {
        match child {
            ParagraphChild::Run(run) => run_text(&run.children, out),
            ParagraphChild::Hyperlink(link) => paragraph_children_text(&link.children, out),
            ParagraphChild::Insert(ins) => {
                for c in &ins.children {
                    if let InsertChild::Run(run) = c {
                        run_text(&run.children, out);
                    }
                }
            }
            ParagraphChild::StructuredDataTag(tag) => sdt_text(&tag.children, out),
            // Deletions, bookmarks, comments, page numbers: no visible body text.
            _ => {}
        }
    }
}

fn run_text(children: &[RunChild], out: &mut String) {
    for child in children {
        match child {
            RunChild::Text(t) => out.push_str(&t.text),
            RunChild::Tab(_) | RunChild::PTab(_) => out.push('\t'),
            RunChild::Break(_) | RunChild::CarriageReturn(_) => out.push('\n'),
            RunChild::Sym(sym) => out.push_str(&sym.char),
            _ => {}
        }
    }
}

fn sdt_text(children: &[StructuredDataTagChild], out: &mut String) {
    for child in children {
        match child {
            StructuredDataTagChild::Run(run) => run_text(&run.children, out),
            StructuredDataTagChild::Paragraph(p) => {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&paragraph_text(p));
            }
            StructuredDataTagChild::Table(t) => {
                if !out.is_empty() {
                    out.push('\n');
                }
                out.push_str(&table_text(t));
            }
            _ => {}
        }
    }
}

/// Visible text of a paragraph, trimmed.
fn paragraph_text(p: &Paragraph) -> String {
    let mut out = String::new();
    paragraph_children_text(&p.children, &mut out);
    out.trim().to_owned()
}

fn paragraph_style(p: &Paragraph) -> Option<String> {
    p.property.style.as_ref().map(|s| s.val.clone())
}

/// Render a table as tab-separated cells, one row per line. Empty rows are dropped.
fn table_text(table: &Table) -> String {
    let mut rows = Vec::new();
    for TableChild::TableRow(row) in &table.rows {
        let mut cells = Vec::new();
        for TableRowChild::TableCell(cell) in &row.cells {
            let mut parts = Vec::new();
            for content in &cell.children {
                match content {
                    TableCellContent::Paragraph(p) => {
                        let t = paragraph_text(p);
                        if !t.is_empty() {
                            parts.push(t);
                        }
                    }
                    TableCellContent::Table(nested) => {
                        let t = table_text(nested);
                        if !t.is_empty() {
                            parts.push(t);
                        }
                    }
                    TableCellContent::StructuredDataTag(tag) => {
                        let mut t = String::new();
                        sdt_text(&tag.children, &mut t);
                        if !t.trim().is_empty() {
                            parts.push(t.trim().to_owned());
                        }
                    }
                    TableCellContent::TableOfContents(_) => {}
                }
            }
            cells.push(parts.join(" "));
        }
        if cells.iter().any(|c| !c.is_empty()) {
            rows.push(cells.join("\t"));
        }
    }
    rows.join("\n")
}

/// Flatten the body into blocks.
fn collect_blocks(docx: &Docx) -> Vec<Block> {
    let mut blocks = Vec::new();
    for child in &docx.document.children {
        match child {
            DocumentChild::Paragraph(p) => {
                blocks.push(Block::Paragraph {
                    style: paragraph_style(p),
                    text: paragraph_text(p),
                });
            }
            DocumentChild::Table(t) => blocks.push(Block::Table(table_text(t))),
            DocumentChild::StructuredDataTag(tag) => {
                let mut text = String::new();
                sdt_text(&tag.children, &mut text);
                blocks.push(Block::Paragraph {
                    style: None,
                    text: text.trim().to_owned(),
                });
            }
            _ => {}
        }
    }
    blocks
}

/// Title from `docProps/core.xml` when the reader exposes it (best effort: the
/// struct's fields are private, so we go through its `Serialize` impl).
fn core_title(docx: &Docx) -> Option<String> {
    let value = serde_json::to_value(&docx.doc_props.core).ok()?;
    let title = value
        .get("config")
        .and_then(|c| c.get("title"))
        .or_else(|| value.get("title"))
        .and_then(serde_json::Value::as_str)?
        .trim();
    (!title.is_empty()).then(|| title.to_owned())
}

fn is_heading(style: Option<&str>) -> bool {
    style.is_some_and(|s| s.to_ascii_lowercase().starts_with("heading"))
}

fn is_title(style: Option<&str>) -> bool {
    style.is_some_and(|s| s.eq_ignore_ascii_case("title"))
}

/// A run of blocks that becomes one document.
#[derive(Debug, Default)]
struct Section {
    heading: Option<String>,
    parts: Vec<String>,
}

impl Section {
    fn push(&mut self, text: String) {
        if !text.is_empty() {
            self.parts.push(text);
        }
    }
    fn content(&self) -> String {
        self.parts.join("\n\n")
    }
}

#[async_trait]
impl Plugin for DocxExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Extracts text from Word (.docx) files: paragraphs joined by blank lines, \
                 tables as tab-separated rows, optionally split into sections on headings.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types([DOCX_MIME])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "split_on_headings": {
                        "type": "boolean",
                        "default": false,
                        "description": "Emit one document per section, cutting at paragraphs styled `Heading*`."
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

        let docx = docx_rs::read_docx(&blob.data)
            .map_err(|e| PluginError::NonRetryable(format!("failed to parse DOCX: {e}")))?;
        let blocks = collect_blocks(&docx);
        let mut title = core_title(&docx);

        let mut sections: Vec<Section> = vec![Section::default()];
        for (i, block) in blocks.into_iter().enumerate() {
            ctx.check_cancelled()?;
            if i > 0 && i.is_multiple_of(HEARTBEAT_EVERY) {
                ctx.heartbeat(format!("block {i}"));
            }
            match block {
                Block::Paragraph { style, text } => {
                    if text.is_empty() {
                        continue;
                    }
                    if title.is_none() && is_title(style.as_deref()) {
                        title = Some(text.clone());
                        continue;
                    }
                    if cfg.split_on_headings && is_heading(style.as_deref()) {
                        sections.push(Section {
                            heading: Some(text),
                            parts: Vec::new(),
                        });
                        continue;
                    }
                    if let Some(current) = sections.last_mut() {
                        current.push(text);
                    }
                }
                Block::Table(text) => {
                    if let Some(current) = sections.last_mut() {
                        current.push(text);
                    }
                }
            }
        }

        let docs: Vec<Document> = if cfg.split_on_headings {
            sections
                .iter()
                .filter(|s| !s.parts.is_empty())
                .enumerate()
                .map(|(i, s)| {
                    let mut doc = Document::with_id(format!("{base}_s{}", i + 1), s.content());
                    doc.title = s.heading.clone().or_else(|| title.clone());
                    doc.meta = meta.clone();
                    doc.meta.section = s.heading.clone();
                    doc
                })
                .collect()
        } else {
            let content = sections
                .iter()
                .map(Section::content)
                .filter(|c| !c.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
            if content.is_empty() {
                Vec::new()
            } else {
                let mut doc = Document::with_id(base, content);
                doc.title = title.clone();
                doc.meta = meta.clone();
                vec![doc]
            }
        };
        tracing::debug!(plugin = NAME, documents = docs.len(), "extracted docx");
        Ok(PluginOutput::Documents(docs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use docx_rs::{Docx, Paragraph, Run, Table, TableCell, TableRow};
    use std::io::Cursor;

    fn para(text: &str) -> Paragraph {
        Paragraph::new().add_run(Run::new().add_text(text))
    }

    fn cell(text: &str) -> TableCell {
        TableCell::new().add_paragraph(para(text))
    }

    /// Title, intro, two headed sections (the second containing a table).
    fn make_docx() -> Vec<u8> {
        let docx = Docx::new()
            .add_paragraph(para("Quarterly Review").style("Title"))
            .add_paragraph(para("Intro paragraph."))
            .add_paragraph(para("Sales").style("Heading1"))
            .add_paragraph(para("Sales went up."))
            .add_paragraph(Paragraph::new()) // empty paragraph must be ignored
            .add_paragraph(para("Costs").style("Heading1"))
            .add_paragraph(para("Costs went down."))
            .add_table(Table::new(vec![
                TableRow::new(vec![cell("Item"), cell("Amount")]),
                TableRow::new(vec![cell("Rent"), cell("100")]),
            ]));
        let mut cursor = Cursor::new(Vec::new());
        docx.build().pack(&mut cursor).unwrap();
        cursor.into_inner()
    }

    fn input(bytes: Vec<u8>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            bytes,
            DOCX_MIME,
            Some("reports/Q3 review.docx".into()),
        ))
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = DocxExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(m.content_types, vec![DOCX_MIME]);
        assert_eq!(
            m.config_schema["properties"]["split_on_headings"]["default"],
            false
        );
    }

    #[tokio::test]
    async fn extracts_single_document_with_tables() {
        let out = DocxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(make_docx()),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 1);
        let doc = &docs[0];
        assert_eq!(doc.id, "Q3_review");
        assert_eq!(doc.title.as_deref(), Some("Quarterly Review"));
        assert_eq!(doc.meta.filename.as_deref(), Some("reports/Q3 review.docx"));
        assert_eq!(doc.meta.mime.as_deref(), Some(DOCX_MIME));
        assert_eq!(
            doc.content,
            "Intro paragraph.\n\nSales\n\nSales went up.\n\nCosts\n\nCosts went down.\n\nItem\tAmount\nRent\t100"
        );
    }

    #[tokio::test]
    async fn splits_on_headings_when_configured() {
        let out = DocxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(make_docx()),
                serde_json::json!({"split_on_headings": true}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 3, "{docs:#?}");
        // Preamble keeps the document title, no section.
        assert_eq!(docs[0].id, "Q3_review_s1");
        assert_eq!(docs[0].title.as_deref(), Some("Quarterly Review"));
        assert_eq!(docs[0].meta.section, None);
        assert_eq!(docs[0].content, "Intro paragraph.");
        assert_eq!(docs[1].id, "Q3_review_s2");
        assert_eq!(docs[1].title.as_deref(), Some("Sales"));
        assert_eq!(docs[1].meta.section.as_deref(), Some("Sales"));
        assert_eq!(docs[1].content, "Sales went up.");
        assert_eq!(docs[2].title.as_deref(), Some("Costs"));
        assert_eq!(
            docs[2].content,
            "Costs went down.\n\nItem\tAmount\nRent\t100"
        );
    }

    #[tokio::test]
    async fn empty_document_yields_no_documents() {
        let mut cursor = Cursor::new(Vec::new());
        Docx::new().build().pack(&mut cursor).unwrap();
        let out = DocxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(cursor.into_inner()),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(out, PluginOutput::Documents(vec![]));
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let err = DocxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(make_docx()),
                serde_json::json!({"split_on_headings": "nope"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn wrong_input_kind_is_invalid_input() {
        let err = DocxExtractorPlugin::new()
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
    async fn corrupt_docx_is_non_retryable() {
        let err = DocxExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(b"PK\x03\x04 not a zip".to_vec()),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::NonRetryable(_)), "got {err:?}");
    }
}
