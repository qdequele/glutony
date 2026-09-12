//! # `html_extractor`
//!
//! Built-in meili-ingest plugin that turns an HTML page into one text [`Document`]
//! using [`scraper`](https://crates.io/crates/scraper).
//!
//! * `title` comes from `<title>`, falling back to the first `<h1>`.
//! * Boilerplate (`script, style, noscript, nav, footer, header, aside, template,
//!   svg`) is removed, then text is taken from `<main>` or `<article>` when present,
//!   else `<body>`, else the whole document.
//! * Block-level elements become separate lines; whitespace is collapsed.
//! * `fields.description` (from `<meta name="description">`) when
//!   `include_meta_description` (default true), `fields.links` (list of `href`s in
//!   the content root) when `extract_links` (default false).
//! * `_meta.language` from `<html lang>` or `<meta name="language">`.

use std::collections::HashSet;

use ego_tree::NodeRef;
use meili_ingest_plugin_sdk::prelude::*;
use scraper::{ElementRef, Html, Node, Selector};
use serde::Deserialize;

/// Plugin name, as referenced by `steps[].plugin`.
pub const NAME: &str = "html_extractor";

/// Elements dropped before text extraction.
const NOISE_SELECTOR: &str = "script, style, noscript, nav, footer, header, aside, template, svg";

/// Elements that start a new line in the extracted text.
const BLOCK_TAGS: &[&str] = &[
    "address",
    "article",
    "aside",
    "blockquote",
    "br",
    "dd",
    "details",
    "dialog",
    "div",
    "dl",
    "dt",
    "fieldset",
    "figcaption",
    "figure",
    "footer",
    "form",
    "h1",
    "h2",
    "h3",
    "h4",
    "h5",
    "h6",
    "header",
    "hgroup",
    "hr",
    "li",
    "main",
    "nav",
    "ol",
    "p",
    "pre",
    "section",
    "summary",
    "table",
    "tbody",
    "tfoot",
    "thead",
    "tr",
    "ul",
];

/// Elements separated from their siblings by a tab (table cells).
const CELL_TAGS: &[&str] = &["td", "th"];

/// The HTML extractor plugin. Stateless; construct with [`HtmlExtractorPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct HtmlExtractorPlugin;

impl HtmlExtractorPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration for [`HtmlExtractorPlugin`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    /// Collect the `href` of every `<a>` in the content root into `fields.links`.
    extract_links: bool,
    /// Copy `<meta name="description">` into `fields.description`.
    include_meta_description: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            extract_links: false,
            include_meta_description: true,
        }
    }
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

/// Provenance metadata for the document produced from `blob`.
fn base_meta(blob: &Blob) -> DocumentMeta {
    DocumentMeta {
        source: blob.filename.clone(),
        filename: blob.filename.clone(),
        mime: Some(blob.mime.clone()),
        ..DocumentMeta::default()
    }
}

/// Parse a CSS selector; the selectors used here are constants, so failure is a bug.
fn selector(css: &str) -> Result<Selector, PluginError> {
    Selector::parse(css)
        .map_err(|e| PluginError::NonRetryable(format!("invalid selector {css:?}: {e}")))
}

/// Collapse all whitespace runs into single spaces and trim.
fn collapse(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

/// Text of the first element matching `css`, collapsed; `None` when missing or blank.
fn first_text(doc: &Html, css: &str) -> Result<Option<String>, PluginError> {
    let sel = selector(css)?;
    Ok(doc
        .select(&sel)
        .map(|e| collapse(&e.text().collect::<String>()))
        .find(|t| !t.is_empty()))
}

/// `attr` of the first element matching `css`, trimmed; `None` when missing or blank.
fn first_attr(doc: &Html, css: &str, attr: &str) -> Result<Option<String>, PluginError> {
    let sel = selector(css)?;
    Ok(doc
        .select(&sel)
        .filter_map(|e| e.value().attr(attr))
        .map(|v| v.trim().to_owned())
        .find(|v| !v.is_empty()))
}

/// Detach every element matching [`NOISE_SELECTOR`] from the tree.
fn remove_noise(doc: &mut Html) -> Result<(), PluginError> {
    let sel = selector(NOISE_SELECTOR)?;
    let ids: Vec<_> = doc.select(&sel).map(|e| e.id()).collect();
    for id in ids {
        if let Some(mut node) = doc.tree.get_mut(id) {
            node.detach();
        }
    }
    Ok(())
}

/// Walk `node` in document order, appending text and line/cell separators to `out`.
fn append_text(node: NodeRef<'_, Node>, out: &mut String) {
    match node.value() {
        // Newlines inside a text node are plain whitespace in HTML; only block
        // elements start new lines.
        Node::Text(t) => out.push_str(&t.replace(['\n', '\r'], " ")),
        Node::Element(el) => {
            let name = el.name();
            let block = BLOCK_TAGS.contains(&name);
            let cell = CELL_TAGS.contains(&name);
            if block {
                out.push('\n');
            } else if cell {
                out.push('\t');
            }
            for child in node.children() {
                append_text(child, out);
            }
            if block {
                out.push('\n');
            }
        }
        // Comments, doctype, processing instructions and the document root itself.
        _ => {
            for child in node.children() {
                append_text(child, out);
            }
        }
    }
}

/// Extract readable text under `root`: one line per block, whitespace collapsed
/// within a line, blank lines dropped.
fn text_of(root: NodeRef<'_, Node>) -> String {
    let mut raw = String::new();
    append_text(root, &mut raw);
    raw.lines()
        .map(|line| {
            line.split('\t')
                .map(collapse)
                .filter(|cell| !cell.is_empty())
                .collect::<Vec<_>>()
                .join("\t")
        })
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Unique, non-empty `href`s of the `<a>` elements under `root`, in document order.
fn links_of(root: ElementRef<'_>) -> Result<Vec<String>, PluginError> {
    let sel = selector("a[href]")?;
    let mut seen = HashSet::new();
    let mut links = Vec::new();
    for a in root.select(&sel) {
        if let Some(href) = a
            .value()
            .attr("href")
            .map(str::trim)
            .filter(|h| !h.is_empty())
            && seen.insert(href.to_owned())
        {
            links.push(href.to_owned());
        }
    }
    Ok(links)
}

/// The element whose text is the page content: `<main>`, else `<article>`, else `<body>`.
fn content_root(doc: &Html) -> Result<Option<ElementRef<'_>>, PluginError> {
    for css in ["main", "article", "body"] {
        let sel = selector(css)?;
        if let Some(el) = doc.select(&sel).next() {
            return Ok(Some(el));
        }
    }
    Ok(None)
}

#[async_trait]
impl Plugin for HtmlExtractorPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Extracts the title and readable text of an HTML page (boilerplate such as \
                 scripts, styles, navigation and footers removed), plus optional meta \
                 description and links.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["text/html", "application/xhtml+xml"])
            .config_schema(serde_json::json!({
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "extract_links": {
                        "type": "boolean",
                        "default": false,
                        "description": "Collect the href of every link in the content into `links`."
                    },
                    "include_meta_description": {
                        "type": "boolean",
                        "default": true,
                        "description": "Copy <meta name=\"description\"> into `description`."
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
        let mut meta = base_meta(&blob);
        let source = blob.text_lossy();

        let mut doc = Html::parse_document(&source);
        ctx.check_cancelled()?;

        // Head-level information first: it may live inside elements we are about to drop.
        let title_tag = first_text(&doc, "title")?;
        let description = if cfg.include_meta_description {
            first_attr(&doc, r#"meta[name="description" i]"#, "content")?
        } else {
            None
        };
        let language = first_attr(&doc, "html[lang]", "lang")?
            .or(first_attr(&doc, r#"meta[name="language" i]"#, "content")?)
            .or(first_attr(
                &doc,
                r#"meta[http-equiv="content-language" i]"#,
                "content",
            )?);

        remove_noise(&mut doc)?;
        ctx.check_cancelled()?;

        let title = match title_tag {
            Some(t) => Some(t),
            None => first_text(&doc, "h1")?,
        };

        let (content, links) = match content_root(&doc)? {
            Some(root) => {
                let links = if cfg.extract_links {
                    links_of(root)?
                } else {
                    Vec::new()
                };
                (text_of(*root), links)
            }
            None => (text_of(doc.tree.root()), Vec::new()),
        };
        ctx.heartbeat("html parsed");

        if content.is_empty() && title.is_none() {
            return Ok(PluginOutput::Documents(vec![]));
        }

        meta.language = language;
        let mut document = Document::with_id(base, content);
        document.title = title;
        document.meta = meta;
        if let Some(d) = description {
            document
                .fields
                .insert("description".into(), serde_json::Value::String(d));
        }
        if cfg.extract_links {
            document
                .fields
                .insert("links".into(), serde_json::Value::from(links));
        }
        tracing::debug!(
            plugin = NAME,
            chars = document.content.len(),
            "extracted html"
        );
        Ok(PluginOutput::Documents(vec![document]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PAGE: &str = r#"<!doctype html>
<html lang="fr-FR">
<head>
  <title>  Guide   d'installation </title>
  <meta name="description" content="Comment installer le produit.">
  <style>body { color: red }</style>
  <script>console.log("noise")</script>
</head>
<body>
  <header><h1>Site header</h1><a href="/home">Home</a></header>
  <nav><a href="/a">A</a><a href="/b">B</a></nav>
  <main>
    <h1>Installation</h1>
    <p>Étape   <b>une</b> : télécharger
       l'archive.</p>
    <p>Étape deux : lancer <a href="https://example.com/run">le script</a>
       ou <a href="https://example.com/run">le même lien</a> et <a href="/docs">la doc</a>.</p>
    <ul><li>Premier</li><li>Second</li></ul>
    <table><tr><th>Clé</th><th>Valeur</th></tr><tr><td>a</td><td>1</td></tr></table>
    <script>alert(1)</script>
  </main>
  <aside>Sidebar noise</aside>
  <footer>Copyright</footer>
</body>
</html>"#;

    fn input(html: &str) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            html.as_bytes().to_vec(),
            "text/html",
            Some("docs/install guide.html".into()),
        ))
    }

    #[test]
    fn manifest_is_well_formed() {
        let m = HtmlExtractorPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert!(m.accepts_kind(InputKind::Bytes));
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(m.content_types.iter().any(|c| c == "text/html"));
        assert_eq!(
            m.config_schema["properties"]["extract_links"]["default"],
            false
        );
        assert_eq!(
            m.config_schema["properties"]["include_meta_description"]["default"],
            true
        );
    }

    #[tokio::test]
    async fn extracts_title_text_description_and_language() {
        let out = HtmlExtractorPlugin::new()
            .execute(&ActivityContext::noop(), input(PAGE), serde_json::json!({}))
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs.len(), 1);
        let doc = &docs[0];
        assert_eq!(doc.id, "install_guide");
        assert_eq!(doc.title.as_deref(), Some("Guide d'installation"));
        assert_eq!(doc.fields["description"], "Comment installer le produit.");
        assert!(!doc.fields.contains_key("links"));
        assert_eq!(doc.meta.language.as_deref(), Some("fr-FR"));
        assert_eq!(
            doc.meta.filename.as_deref(),
            Some("docs/install guide.html")
        );
        assert_eq!(doc.meta.mime.as_deref(), Some("text/html"));
        assert_eq!(
            doc.content,
            "Installation\nÉtape une : télécharger l'archive.\nÉtape deux : lancer le script ou le même lien et la doc.\nPremier\nSecond\nClé\tValeur\na\t1"
        );
        for noise in [
            "Site header",
            "Sidebar",
            "Copyright",
            "console.log",
            "alert",
            "color: red",
        ] {
            assert!(
                !doc.content.contains(noise),
                "{noise:?} leaked into {:?}",
                doc.content
            );
        }
    }

    #[tokio::test]
    async fn config_controls_links_and_description() {
        let out = HtmlExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(PAGE),
                serde_json::json!({"extract_links": true, "include_meta_description": false}),
            )
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        let doc = &docs[0];
        assert!(!doc.fields.contains_key("description"));
        assert_eq!(
            doc.fields["links"],
            serde_json::json!(["https://example.com/run", "/docs"])
        );
    }

    #[tokio::test]
    async fn falls_back_to_h1_and_body_without_main() {
        let html =
            "<html><body><h1>Only Heading</h1><div>Some <i>text</i></div><p></p></body></html>";
        let out = HtmlExtractorPlugin::new()
            .execute(&ActivityContext::noop(), input(html), serde_json::json!({}))
            .await
            .unwrap();
        let docs = out.into_documents().unwrap();
        assert_eq!(docs[0].title.as_deref(), Some("Only Heading"));
        assert_eq!(docs[0].content, "Only Heading\nSome text");
        assert_eq!(docs[0].meta.language, None);
        assert!(!docs[0].fields.contains_key("description"));
    }

    #[tokio::test]
    async fn empty_page_yields_no_documents() {
        let out = HtmlExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input("<html><body><script>x()</script></body></html>"),
                serde_json::json!({}),
            )
            .await
            .unwrap();
        assert_eq!(out, PluginOutput::Documents(vec![]));
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let err = HtmlExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                input(PAGE),
                serde_json::json!({"extract_links": 1}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn wrong_input_kind_is_invalid_input() {
        let err = HtmlExtractorPlugin::new()
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![Document::new("x")]),
                serde_json::json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "got {err:?}");
    }
}
