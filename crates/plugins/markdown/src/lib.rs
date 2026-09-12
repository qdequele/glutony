//! # `markdown_extractor`
//!
//! Built-in meili-ingest plugin that converts Markdown into plain-text
//! [`Document`]s with [`pulldown-cmark`](https://crates.io/crates/pulldown-cmark).
//!
//! * Formatting is dropped, link text is kept, code blocks are copied verbatim, list
//!   items are placed on their own lines and table cells are tab-separated.
//! * `split_on_headings: true` (default) cuts the file into sections at every heading
//!   whose level is `<= heading_level` (default `1`, i.e. `#`). Each section becomes a
//!   document whose `title` and `_meta.section` are the heading text; text before the
//!   first heading becomes a section of its own. With `split_on_headings: false` a
//!   single document is produced, titled after the first `#` heading.
//! * `front_matter: true` (default) parses a leading `---` YAML block into `fields`
//!   (a `title` key becomes the document title).

use meili_ingest_plugin_sdk::prelude::*;
use pulldown_cmark::{Event, HeadingLevel, Options, Parser, Tag, TagEnd};
use serde::Deserialize;

/// Plugin name, as referenced by `steps[].plugin`.
pub const NAME: &str = "markdown_extractor";

/// Number of finished sections between two heartbeats.
const HEARTBEAT_EVERY_SECTIONS: usize = 10;

/// Number of parser events between two cancellation checks.
const CANCEL_CHECK_EVERY_EVENTS: usize = 1_000;

/// The Markdown extractor plugin. Stateless; construct with [`MarkdownExtractorPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct MarkdownExtractorPlugin;

impl MarkdownExtractorPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration for [`MarkdownExtractorPlugin`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    /// Emit one document per heading section.
    split_on_headings: bool,
    /// Deepest heading level that starts a new section (1 = `#`, 2 = `##`, ...).
    heading_level: u8,
    /// Parse a leading YAML front matter block into `fields`.
    front_matter: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            split_on_headings: true,
            heading_level: 1,
            front_matter: true,
        }
    }
}

fn parse_config(value: serde_json::Value) -> Result<Config, PluginError> {
    let cfg: Config = if value.is_null() {
        Config::default()
    } else {
        serde_json::from_value(value).map_err(|e| PluginError::InvalidConfig(e.to_string()))?
    };
    if !(1..=6).contains(&cfg.heading_level) {
        return Err(PluginError::InvalidConfig(format!(
            "heading_level must be between 1 and 6, got {}",
            cfg.heading_level
        )));
    }
    Ok(cfg)
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

/// Split `---\n<yaml>\n---\n<body>` into `(yaml, body)`; `None` when there is no
/// well-formed leading front matter block.
fn split_front_matter(text: &str) -> Option<(&str, &str)> {
    let rest = text.strip_prefix("---")?;
    let rest = rest
        .strip_prefix("\r\n")
        .or_else(|| rest.strip_prefix('\n'))?;
    let mut offset = 0;
    for line in rest.split_inclusive('\n') {
        let delimiter = line.trim_end_matches(['\r', '\n']);
        if delimiter == "---" || delimiter == "..." {
            return Some((&rest[..offset], &rest[offset + line.len()..]));
        }
        offset += line.len();
    }
    None
}

/// Parse a YAML front matter block into a JSON object. Non-mapping or invalid YAML
/// yields `None` (the block is then treated as ordinary Markdown).
fn parse_front_matter(yaml: &str) -> Option<serde_json::Map<String, serde_json::Value>> {
    let value: serde_yaml::Value = match serde_yaml::from_str(yaml) {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(plugin = NAME, error = %e, "ignoring invalid YAML front matter");
            return None;
        }
    };
    match serde_json::to_value(value) {
        Ok(serde_json::Value::Object(map)) => Some(map),
        Ok(_) => None,
        Err(e) => {
            tracing::warn!(plugin = NAME, error = %e, "ignoring non-JSON-compatible front matter");
            None
        }
    }
}

/// One heading-delimited part of the file.
#[derive(Debug, Default)]
struct Section {
    heading: Option<String>,
    buf: String,
}

impl Section {
    /// Trim and squeeze runs of blank lines down to one.
    fn content(&self) -> String {
        let mut out = String::with_capacity(self.buf.len());
        let mut blank_pending = false;
        for line in self.buf.lines() {
            if line.trim().is_empty() {
                blank_pending = !out.is_empty();
                continue;
            }
            if blank_pending {
                out.push_str("\n\n");
                blank_pending = false;
            } else if !out.is_empty() {
                out.push('\n');
            }
            out.push_str(line.trim_end());
        }
        out
    }
}

/// Event-driven plain-text renderer.
struct Renderer<'c> {
    cfg: &'c Config,
    sections: Vec<Section>,
    /// Open heading: its level and the text collected so far.
    heading: Option<(HeadingLevel, String)>,
    /// Nesting stack of open lists: `None` for bullets, `Some(next_number)` for ordered.
    lists: Vec<Option<u64>>,
    /// A list-item marker was just written; the next paragraph continues on that line.
    pending_marker: bool,
    /// Index of the current cell in the current table row.
    cell_index: usize,
    /// First `#` heading seen, used as document title when not splitting.
    first_h1: Option<String>,
}

impl<'c> Renderer<'c> {
    fn new(cfg: &'c Config) -> Self {
        Self {
            cfg,
            sections: vec![Section::default()],
            heading: None,
            lists: Vec::new(),
            pending_marker: false,
            cell_index: 0,
            first_h1: None,
        }
    }

    fn buf(&mut self) -> &mut String {
        if self.sections.is_empty() {
            self.sections.push(Section::default());
        }
        let last = self.sections.len() - 1;
        &mut self.sections[last].buf
    }

    /// Append text to the open heading or the current section.
    fn text(&mut self, s: &str) {
        if let Some((_, h)) = &mut self.heading {
            h.push_str(s);
        } else {
            self.pending_marker = false;
            self.buf().push_str(s);
        }
    }

    /// Make sure the current section ends with a newline (or is empty).
    fn ensure_line(&mut self) {
        let buf = self.buf();
        if !buf.is_empty() && !buf.ends_with('\n') {
            buf.push('\n');
        }
    }

    /// Make sure the current section ends with a blank line (or is empty).
    fn ensure_blank(&mut self) {
        let buf = self.buf();
        if buf.is_empty() || buf.ends_with("\n\n") {
            return;
        }
        if buf.ends_with('\n') {
            buf.push('\n');
        } else {
            buf.push_str("\n\n");
        }
    }

    fn start_block(&mut self) {
        if self.pending_marker {
            self.pending_marker = false;
        } else {
            self.ensure_blank();
        }
    }

    fn end_heading(&mut self, level: HeadingLevel) {
        let Some((_, raw)) = self.heading.take() else {
            return;
        };
        let text = raw.split_whitespace().collect::<Vec<_>>().join(" ");
        if text.is_empty() {
            return;
        }
        if level == HeadingLevel::H1 && self.first_h1.is_none() {
            self.first_h1 = Some(text.clone());
        }
        if self.cfg.split_on_headings && (level as u8) <= self.cfg.heading_level {
            self.sections.push(Section {
                heading: Some(text),
                buf: String::new(),
            });
        } else {
            self.ensure_blank();
            self.buf().push_str(&text);
            self.buf().push_str("\n\n");
        }
    }

    fn start_item(&mut self) {
        self.ensure_line();
        let depth = self.lists.len().saturating_sub(1);
        let marker = match self.lists.last_mut() {
            Some(Some(n)) => {
                let m = format!("{n}. ");
                *n += 1;
                m
            }
            _ => "- ".to_owned(),
        };
        let buf = self.buf();
        for _ in 0..depth {
            buf.push_str("  ");
        }
        buf.push_str(&marker);
        self.pending_marker = true;
    }

    fn event(&mut self, event: Event<'_>) {
        match event {
            Event::Start(tag) => self.start(tag),
            Event::End(tag) => self.end(tag),
            Event::Text(t) | Event::Code(t) | Event::InlineMath(t) | Event::DisplayMath(t) => {
                self.text(&t)
            }
            Event::Html(_) | Event::InlineHtml(_) => {}
            Event::FootnoteReference(label) => self.text(&format!("[{label}]")),
            Event::SoftBreak => self.text(" "),
            Event::HardBreak => self.text("\n"),
            Event::Rule => self.ensure_blank(),
            Event::TaskListMarker(checked) => self.text(if checked { "[x] " } else { "[ ] " }),
        }
    }

    fn start(&mut self, tag: Tag<'_>) {
        match tag {
            Tag::Heading { level, .. } => self.heading = Some((level, String::new())),
            Tag::Paragraph => self.start_block(),
            Tag::CodeBlock(_) | Tag::BlockQuote(_) | Tag::HtmlBlock | Tag::Table(_) => {
                self.ensure_blank()
            }
            Tag::List(start) => {
                self.ensure_line();
                self.lists.push(start);
            }
            Tag::Item => self.start_item(),
            Tag::TableHead | Tag::TableRow => {
                self.cell_index = 0;
            }
            Tag::TableCell => {
                if self.cell_index > 0 {
                    self.buf().push('\t');
                }
                self.cell_index += 1;
            }
            Tag::FootnoteDefinition(label) => {
                self.ensure_line();
                let line = format!("[{label}]: ");
                self.buf().push_str(&line);
                self.pending_marker = true;
            }
            Tag::DefinitionList | Tag::DefinitionListTitle | Tag::DefinitionListDefinition => {
                self.ensure_line()
            }
            // Inline containers: keep their text, drop the markup.
            Tag::Emphasis
            | Tag::Strong
            | Tag::Strikethrough
            | Tag::Superscript
            | Tag::Subscript
            | Tag::Link { .. }
            | Tag::Image { .. }
            | Tag::MetadataBlock(_) => {}
        }
    }

    fn end(&mut self, tag: TagEnd) {
        match tag {
            TagEnd::Heading(level) => self.end_heading(level),
            TagEnd::Paragraph | TagEnd::CodeBlock | TagEnd::BlockQuote(_) | TagEnd::Table => {
                self.ensure_blank()
            }
            TagEnd::List(_) => {
                self.lists.pop();
                self.ensure_line();
                if self.lists.is_empty() {
                    self.ensure_blank();
                }
            }
            TagEnd::Item | TagEnd::TableHead | TagEnd::TableRow | TagEnd::FootnoteDefinition => {
                self.ensure_line()
            }
            TagEnd::DefinitionList
            | TagEnd::DefinitionListTitle
            | TagEnd::DefinitionListDefinition => self.ensure_line(),
            TagEnd::HtmlBlock
            | TagEnd::TableCell
            | TagEnd::Emphasis
            | TagEnd::Strong
            | TagEnd::Strikethrough
            | TagEnd::Superscript
            | TagEnd::Subscript
            | TagEnd::Link
            | TagEnd::Image
            | TagEnd::MetadataBlock(_) => {}
        }
    }
}

#[async_trait]
impl Plugin for MarkdownExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Converts Markdown to plain text, one document per heading section by \
                 default, with YAML front matter parsed into fields.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["text/markdown", "text/x-markdown"])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "split_on_headings": {
                        "type": "boolean",
                        "default": true,
                        "description": "Emit one document per heading section."
                    },
                    "heading_level": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 6,
                        "default": 1,
                        "description": "Deepest heading level that starts a new section (1 = `#`, 2 = `##`)."
                    },
                    "front_matter": {
                        "type": "boolean",
                        "default": true,
                        "description": "Parse a leading `---` YAML block into fields (and `title`)."
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

        let source = blob.text_lossy();
        let source = source.strip_prefix('\u{feff}').unwrap_or(&source);

        let (mut fields, body) = match split_front_matter(source).filter(|_| cfg.front_matter) {
            Some((yaml, body)) => match parse_front_matter(yaml) {
                Some(map) => (map, body),
                None => (serde_json::Map::new(), source),
            },
            None => (serde_json::Map::new(), source),
        };
        let fm_title = match fields.remove("title") {
            Some(serde_json::Value::String(s)) => {
                let s = s.trim().to_owned();
                (!s.is_empty()).then_some(s)
            }
            Some(other) => Some(other.to_string()),
            None => None,
        };

        let options = Options::ENABLE_TABLES
            | Options::ENABLE_FOOTNOTES
            | Options::ENABLE_STRIKETHROUGH
            | Options::ENABLE_TASKLISTS
            | Options::ENABLE_DEFINITION_LIST;
        let mut renderer = Renderer::new(&cfg);
        let mut sections_seen = 1;
        for (i, event) in Parser::new_ext(body, options).enumerate() {
            if i.is_multiple_of(CANCEL_CHECK_EVERY_EVENTS) {
                ctx.check_cancelled()?;
            }
            renderer.event(event);
            if renderer.sections.len() > sections_seen {
                sections_seen = renderer.sections.len();
                if sections_seen.is_multiple_of(HEARTBEAT_EVERY_SECTIONS) {
                    ctx.heartbeat(format!("section {sections_seen}"));
                }
            }
        }
        ctx.check_cancelled()?;

        let docs: Vec<Document> = if cfg.split_on_headings {
            renderer
                .sections
                .iter()
                .map(|s| (s.heading.clone(), s.content()))
                .filter(|(_, content)| !content.is_empty())
                .enumerate()
                .map(|(i, (heading, content))| {
                    let mut doc = Document::with_id(format!("{base}_s{}", i + 1), content);
                    doc.title = heading.clone().or_else(|| fm_title.clone());
                    doc.fields = fields.clone();
                    doc.meta = meta.clone();
                    doc.meta.section = heading;
                    doc
                })
                .collect()
        } else {
            let content = renderer
                .sections
                .iter()
                .map(Section::content)
                .filter(|c| !c.is_empty())
                .collect::<Vec<_>>()
                .join("\n\n");
            if content.is_empty() {
                Vec::new()
            } else {
                let mut doc = Document::with_id(base, content);
                doc.title = fm_title.clone().or_else(|| renderer.first_h1.clone());
                doc.fields = fields.clone();
                doc.meta = meta.clone();
                vec![doc]
            }
        };
        tracing::debug!(plugin = NAME, documents = docs.len(), "extracted markdown");
        Ok(PluginOutput::Documents(docs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const README: &str = r#"---
title: Getting Started
tags: [intro, setup]
version: 2
---
Welcome to the **project**. See [the docs](https://example.com/docs).

# Install

Run the following:

```sh
cargo install thing   # keep   spacing
```

- first step
- second step
  1. nested one
  2. nested two

## Requirements

Rust 1.92 or later.

# Usage

| Flag | Meaning |
| ---- | ------- |
| `-v` | verbose |

Line one
continued here.\
Hard break above.
"#;

    fn input(md: &str) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            md.as_bytes().to_vec(),
            "text/markdown",
            Some("guides/Getting Started.md".into()),
        ))
    }

    async fn run(md: &str, config: serde_json::Value) -> Vec<Document> {
        MarkdownExtractorPlugin::new()
            .execute(&ActivityContext::noop(), input(md), config)
            .await
            .unwrap()
            .into_documents()
            .unwrap()
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = MarkdownExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(m.content_types.iter().any(|c| c == "text/markdown"));
        assert_eq!(
            m.config_schema["properties"]["split_on_headings"]["default"],
            true
        );
        assert_eq!(m.config_schema["properties"]["heading_level"]["default"], 1);
        assert_eq!(
            m.config_schema["properties"]["front_matter"]["default"],
            true
        );
    }

    #[test]
    fn front_matter_splitting() {
        assert_eq!(
            split_front_matter("---\na: 1\n---\nbody"),
            Some(("a: 1\n", "body"))
        );
        assert_eq!(
            split_front_matter("---\r\na: 1\r\n---\r\nbody"),
            Some(("a: 1\r\n", "body"))
        );
        assert_eq!(split_front_matter("---\na: 1\n...\n"), Some(("a: 1\n", "")));
        assert_eq!(split_front_matter("--- not front matter\n---\n"), None);
        assert_eq!(split_front_matter("---\nnever closed\n"), None);
        assert_eq!(split_front_matter("no front matter"), None);
    }

    #[tokio::test]
    async fn splits_on_h1_with_front_matter_by_default() {
        let docs = run(README, serde_json::json!({})).await;
        assert_eq!(docs.len(), 3, "{docs:#?}");

        let preamble = &docs[0];
        assert_eq!(preamble.id, "Getting_Started_s1");
        assert_eq!(preamble.title.as_deref(), Some("Getting Started"));
        assert_eq!(preamble.meta.section, None);
        assert_eq!(preamble.content, "Welcome to the project. See the docs.");
        assert_eq!(
            preamble.fields["tags"],
            serde_json::json!(["intro", "setup"])
        );
        assert_eq!(preamble.fields["version"], 2);
        assert!(!preamble.fields.contains_key("title"));
        assert_eq!(
            preamble.meta.filename.as_deref(),
            Some("guides/Getting Started.md")
        );
        assert_eq!(preamble.meta.mime.as_deref(), Some("text/markdown"));

        let install = &docs[1];
        assert_eq!(install.id, "Getting_Started_s2");
        assert_eq!(install.title.as_deref(), Some("Install"));
        assert_eq!(install.meta.section.as_deref(), Some("Install"));
        assert_eq!(install.fields["version"], 2);
        assert_eq!(
            install.content,
            "Run the following:\n\ncargo install thing   # keep   spacing\n\n- first step\n- second step\n  1. nested one\n  2. nested two\n\nRequirements\n\nRust 1.92 or later."
        );

        let usage = &docs[2];
        assert_eq!(usage.title.as_deref(), Some("Usage"));
        assert_eq!(
            usage.content,
            "Flag\tMeaning\n-v\tverbose\n\nLine one continued here.\nHard break above."
        );
    }

    #[tokio::test]
    async fn config_controls_splitting_level_and_front_matter() {
        // No splitting: one document titled from the front matter.
        let docs = run(README, serde_json::json!({"split_on_headings": false})).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "Getting_Started");
        assert_eq!(docs[0].title.as_deref(), Some("Getting Started"));
        assert!(
            docs[0].content.starts_with(
                "Welcome to the project. See the docs.\n\nInstall\n\nRun the following:"
            )
        );
        assert!(docs[0].content.contains("\n\nRequirements\n\n"));
        assert!(docs[0].content.contains("\n\nUsage\n\n"));

        // Splitting down to `##`.
        let docs = run(README, serde_json::json!({"heading_level": 2})).await;
        let titles: Vec<_> = docs
            .iter()
            .map(|d| d.title.as_deref().unwrap_or(""))
            .collect();
        assert_eq!(
            titles,
            vec!["Getting Started", "Install", "Requirements", "Usage"]
        );
        assert_eq!(docs[2].content, "Rust 1.92 or later.");

        // Front matter disabled: no fields, title falls back to the first H1.
        let docs = run(
            README,
            serde_json::json!({"front_matter": false, "split_on_headings": false}),
        )
        .await;
        assert_eq!(docs.len(), 1);
        assert!(docs[0].fields.is_empty());
        assert_eq!(docs[0].title.as_deref(), Some("Install"));
        assert!(docs[0].content.contains("title: Getting Started"));
    }

    #[tokio::test]
    async fn empty_and_invalid_front_matter_are_handled() {
        assert!(run("", serde_json::json!({})).await.is_empty());
        assert!(run("\n\n   \n", serde_json::json!({})).await.is_empty());

        // Invalid YAML is kept as ordinary text rather than failing the step.
        let docs = run("---\n: : not yaml [\n---\n# T\nbody", serde_json::json!({})).await;
        assert!(
            docs.iter()
                .any(|d| d.title.as_deref() == Some("T") && d.content == "body")
        );
        assert!(docs.iter().all(|d| d.fields.is_empty()));
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let plugin = MarkdownExtractorPlugin::new();
        let err = plugin
            .execute(
                &ActivityContext::noop(),
                input(README),
                serde_json::json!({"heading_level": 9}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
        let err = plugin
            .execute(
                &ActivityContext::noop(),
                input(README),
                serde_json::json!({"front_matter": "maybe"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn wrong_input_kind_is_invalid_input() {
        let err = MarkdownExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Many(vec![]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "got {err:?}");
    }
}
