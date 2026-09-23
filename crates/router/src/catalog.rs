//! Curated catalog: authored copy describing every action and workflow the
//! system ships with.
//!
//! This is deliberately *not* part of `PluginManifest`. That type is implemented
//! by every third-party WASM and gRPC plugin author; the product's own showcase
//! copy has no business in it. Compiling the catalog in (rather than storing it)
//! means it is versioned with the code it describes and a test can assert every
//! entry names a plugin that actually exists.

use meili_ingest_plugin_sdk::{InputKind, OutputKind, PipelineDefinition};
use serde::{Deserialize, Serialize};

/// Every plugin name the catalog is expected to describe: the 18 compiled into
/// the worker plus the 2 provided by external gRPC containers. Kept here rather
/// than imported because `meili-ingest-control-plane` depends on this crate, not
/// the other way round; a test in Task 2 asserts the two lists agree.
pub const ALL_KNOWN_PLUGINS: &[&str] = &[
    "pdf_extractor",
    "docx_extractor",
    "xlsx_extractor",
    "pptx_extractor",
    "html_extractor",
    "markdown_extractor",
    "csv_parser",
    "json_flattener",
    "msgpack_parser",
    "avro_parser",
    "parquet_parser",
    "video_audio_extractor",
    "chunker",
    "document_script",
    "llm_enricher",
    "image_captioner",
    "whisper_transcriber",
    "ocr",
    "meili_indexer",
    "s3_downloader",
];

/// Where an action sits in the shape of a pipeline. Ordered left to right, so
/// the Actions grid doubles as a diagram of the mental model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionCategory {
    Fetch,
    Extract,
    Transform,
    Enrich,
    Index,
}

/// Authored showcase copy for one plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionEntry {
    /// Joins to `PluginManifest.name`.
    pub plugin: String,
    /// Display name, e.g. "PDF extractor".
    pub title: String,
    pub category: ActionCategory,
    /// One line, card-sized.
    pub summary: String,
    /// Two or three concrete things people use it for.
    pub use_cases: Vec<String>,
    /// A copyable YAML step block.
    pub example_step: String,
    /// Fallback shown when no worker has registered a manifest.
    pub accepts: Vec<InputKind>,
    /// Fallback shown when no worker has registered a manifest.
    pub produces: OutputKind,
}

/// What a workflow is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowCategory {
    Documents,
    Data,
    Media,
    Web,
}

/// Authored showcase copy for one workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEntry {
    /// `builtin.pdf`, or a curated template uid.
    pub uid: String,
    pub title: String,
    pub category: WorkflowCategory,
    pub summary: String,
    /// When you would reach for this one over its neighbours.
    pub when_to_use: String,
    /// `None` for `builtin.*`: the live `GET /pipelines` stays authoritative.
    /// `Some` for curated templates that are not deployed as pipelines.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<PipelineDefinition>,
}

/// The whole catalog, as `GET /catalog` returns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub actions: Vec<ActionEntry>,
    pub workflows: Vec<WorkflowEntry>,
}

/// Terse constructor for an action entry.
fn action(
    plugin: &str,
    title: &str,
    category: ActionCategory,
    summary: &str,
    use_cases: &[&str],
    example_step: &str,
    io: (&[InputKind], OutputKind),
) -> ActionEntry {
    ActionEntry {
        plugin: plugin.to_owned(),
        title: title.to_owned(),
        category,
        summary: summary.to_owned(),
        use_cases: use_cases.iter().map(|s| (*s).to_owned()).collect(),
        example_step: example_step.trim_start_matches('\n').to_owned(),
        accepts: io.0.to_vec(),
        produces: io.1,
    }
}

/// Every action the system can run, in pipeline order.
fn actions() -> Vec<ActionEntry> {
    use ActionCategory::*;
    use InputKind as I;
    vec![
        action(
            "s3_downloader",
            "S3 downloader",
            Fetch,
            "Stream an object out of S3-compatible storage without buffering it in the gateway.",
            &[
                "Ingest a bucket export too large to POST through the gateway",
                "Keep credentials in the worker rather than in the request",
            ],
            "\n- id: fetch\n  plugin: s3_downloader\n  config:\n    bucket: my-exports\n    key: dumps/latest.jsonl\n",
            (&[I::Ref, I::Empty], OutputKind::Bytes),
        ),
        action(
            "pdf_extractor",
            "PDF extractor",
            Extract,
            "Pull text out of a PDF, one document per page or one per file.",
            &[
                "Make a contract library searchable with page-level hits",
                "Index scientific papers while keeping page numbers",
            ],
            "\n- id: extract\n  plugin: pdf_extractor\n  config:\n    per_page: true\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "docx_extractor",
            "Word extractor",
            Extract,
            "Read text from Word documents, including tables and headers.",
            &[
                "Index a shared drive of .docx reports",
                "Search internal policy documents",
            ],
            "\n- id: extract\n  plugin: docx_extractor\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "xlsx_extractor",
            "Excel extractor",
            Extract,
            "Turn each spreadsheet row into its own document, with columns as fields.",
            &[
                "Make a product catalogue kept in Excel searchable",
                "Index an inventory export row by row",
            ],
            "\n- id: extract\n  plugin: xlsx_extractor\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "pptx_extractor",
            "PowerPoint extractor",
            Extract,
            "Extract the text of every slide, keeping slide order.",
            &[
                "Search a deck archive by what was actually said on a slide",
                "Index training material stored as presentations",
            ],
            "\n- id: extract\n  plugin: pptx_extractor\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "html_extractor",
            "HTML extractor",
            Extract,
            "Strip navigation and boilerplate, keeping the readable body of a page.",
            &[
                "Index a documentation site without its chrome",
                "Collect page links alongside the text",
            ],
            "\n- id: extract\n  plugin: html_extractor\n  config:\n    extract_links: true\n    include_meta_description: true\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "markdown_extractor",
            "Markdown extractor",
            Extract,
            "Split Markdown on its headings so each section becomes a document.",
            &[
                "Index a docs repository section by section",
                "Keep heading context on every search hit",
            ],
            "\n- id: extract\n  plugin: markdown_extractor\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "csv_parser",
            "CSV parser",
            Extract,
            "One document per row, with the delimiter sniffed when you do not say.",
            &[
                "Index a data export without converting it first",
                "Use an existing column as the document id",
            ],
            "\n- id: extract\n  plugin: csv_parser\n  config:\n    has_headers: true\n    id_column: sku\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "json_flattener",
            "JSON flattener",
            Extract,
            "Flatten nested JSON into the flat fields Meilisearch filters and facets on.",
            &[
                "Index an API dump with nested objects",
                "Make deep fields usable as filters",
            ],
            "\n- id: extract\n  plugin: json_flattener\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "msgpack_parser",
            "MessagePack parser",
            Extract,
            "Decode MessagePack records into documents.",
            &[
                "Ingest a compact binary export without a conversion step",
                "Index event dumps written by a MessagePack producer",
            ],
            "\n- id: extract\n  plugin: msgpack_parser\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "avro_parser",
            "Avro parser",
            Extract,
            "Read Avro container files, using the embedded schema for field names.",
            &[
                "Index a Kafka topic archived as Avro",
                "Ingest a data-lake export without a schema registry",
            ],
            "\n- id: extract\n  plugin: avro_parser\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "parquet_parser",
            "Parquet parser",
            Extract,
            "Read columnar Parquet files row by row into documents.",
            &[
                "Make an analytics export searchable",
                "Index a warehouse table dump directly",
            ],
            "\n- id: extract\n  plugin: parquet_parser\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "video_audio_extractor",
            "Video audio extractor",
            Extract,
            "Demux the audio track out of a video so it can be transcribed.",
            &[
                "Prepare recorded meetings for transcription",
                "Index a video library by what is said in it",
            ],
            "\n- id: audio\n  plugin: video_audio_extractor\n",
            (&[I::Bytes], OutputKind::Bytes),
        ),
        action(
            "chunker",
            "Chunker",
            Transform,
            "Split long text into overlapping chunks sized for retrieval.",
            &[
                "Keep long documents under an embedding model's context limit",
                "Return the relevant passage instead of a whole file",
            ],
            "\n- id: chunk\n  plugin: chunker\n  config:\n    strategy: sentence\n    chunk_size: 512\n    overlap: 64\n",
            (&[I::Documents], OutputKind::Documents),
        ),
        action(
            "document_script",
            "Document script",
            Transform,
            "Reshape every document with a sandboxed script: rename fields, drop them, compute new ones, or filter the document out.",
            &[
                "Rename incoming columns onto the field names your index already uses",
                "Compute a total, a margin or a slug from fields the source does not carry",
                "Drop internal columns and filter out empty records before indexing",
            ],
            "\n- id: shape\n  plugin: document_script\n  config:\n    script: |\n      doc.fields.price = doc.fields.prix;\n      doc.fields.remove(\"prix\");\n      doc.fields.total_ttc = doc.fields.price * doc.fields.qty * 1.2;\n      if doc.content.is_empty() { return false; }\n      true\n    on_error: fail\n",
            (&[I::Documents, I::Many], OutputKind::Documents),
        ),
        action(
            "llm_enricher",
            "LLM enricher",
            Enrich,
            "Call a language model per document to add titles, summaries, keywords or any JSON you prompt for.",
            &[
                "Generate summaries and keywords for search snippets",
                "Classify documents into facets you can filter on",
            ],
            "\n- id: enrich\n  plugin: llm_enricher\n  depends_on: [chunk]\n  fan_out: $.documents\n  config:\n    model: gpt-4o-mini\n    max_concurrent: 20\n    merge_strategy: merge\n",
            (&[I::Documents], OutputKind::Documents),
        ),
        action(
            "image_captioner",
            "Image captioner",
            Enrich,
            "Describe an image with a vision model so it can be found by its content.",
            &[
                "Make a photo library searchable in words",
                "Caption product images for retrieval",
            ],
            "\n- id: caption\n  plugin: image_captioner\n  config:\n    detail: auto\n    json: true\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "whisper_transcriber",
            "Whisper transcriber",
            Enrich,
            "Transcribe speech to text, with an optional language hint and vocabulary prompt.",
            &[
                "Search podcasts and recorded calls by what was said",
                "Index lecture audio with timestamps",
            ],
            "\n- id: transcribe\n  plugin: whisper_transcriber\n  config:\n    language: en\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "ocr",
            "OCR",
            Enrich,
            "Read text off scanned pages and images that carry no text layer.",
            &[
                "Index scanned contracts and invoices",
                "Recover text from image-only PDFs",
            ],
            "\n- id: ocr\n  plugin: ocr\n",
            (&[I::Bytes], OutputKind::Documents),
        ),
        action(
            "meili_indexer",
            "Meilisearch indexer",
            Index,
            "Write documents into the tenant's index. Every pipeline ends here.",
            &[
                "Upsert documents by id into a Meilisearch index",
                "Route content to a per-source index",
            ],
            "\n- id: index\n  plugin: meili_indexer\n",
            (&[I::Documents], OutputKind::Indexed),
        ),
    ]
}

/// Terse constructor for a built-in workflow entry. Built-ins never carry an
/// inline definition — `GET /pipelines` is authoritative for anything deployed.
fn builtin_workflow(
    suffix: &str,
    title: &str,
    category: WorkflowCategory,
    summary: &str,
    when_to_use: &str,
) -> WorkflowEntry {
    WorkflowEntry {
        uid: format!("builtin.{suffix}"),
        title: title.to_owned(),
        category,
        summary: summary.to_owned(),
        when_to_use: when_to_use.to_owned(),
        definition: None,
    }
}

/// Every workflow the system ships with, grouped by category.
fn workflows() -> Vec<WorkflowEntry> {
    use WorkflowCategory::*;
    vec![
        builtin_workflow(
            "pdf",
            "PDF",
            Documents,
            "Extract text per page, chunk it, index it.",
            "The default for any PDF. Page-level extraction keeps hits traceable to a page number.",
        ),
        builtin_workflow(
            "word",
            "Word",
            Documents,
            "Extract text from .doc and .docx, chunk it, index it.",
            "Word documents of any length; the chunk step keeps long reports retrievable.",
        ),
        builtin_workflow(
            "powerpoint",
            "PowerPoint",
            Documents,
            "Extract slide text and index it.",
            "Decks, where each slide is short enough that chunking would only add noise.",
        ),
        builtin_workflow(
            "markdown",
            "Markdown",
            Documents,
            "Split on headings and index each section.",
            "Documentation and READMEs, where headings are better boundaries than a fixed chunk size.",
        ),
        builtin_workflow(
            "text",
            "Plain text",
            Documents,
            "Chunk plain text and index it.",
            "Logs, transcripts and anything with no structure to exploit.",
        ),
        builtin_workflow(
            "excel",
            "Excel",
            Data,
            "One document per spreadsheet row.",
            "Tabular data where each row is a thing people search for, like a product or an order.",
        ),
        builtin_workflow(
            "csv",
            "CSV",
            Data,
            "One document per row, delimiter sniffed automatically.",
            "Exports from another system, when you would rather not convert the file first.",
        ),
        builtin_workflow(
            "json",
            "JSON",
            Data,
            "Flatten nested JSON and index it.",
            "API dumps, where nested fields need flattening before they can be filtered on.",
        ),
        builtin_workflow(
            "parquet",
            "Parquet",
            Data,
            "Read columnar Parquet rows and index them.",
            "Analytics and warehouse exports, without a conversion step.",
        ),
        builtin_workflow(
            "avro",
            "Avro",
            Data,
            "Read Avro container files using the embedded schema.",
            "Archived event streams, where the file carries its own schema.",
        ),
        builtin_workflow(
            "msgpack",
            "MessagePack",
            Data,
            "Decode MessagePack records and index them.",
            "Compact binary exports from a MessagePack producer.",
        ),
        builtin_workflow(
            "image",
            "Image",
            Media,
            "Caption the image with a vision model, then index the caption.",
            "Photo and product libraries you want to search with words.",
        ),
        builtin_workflow(
            "audio",
            "Audio",
            Media,
            "Transcribe speech, chunk the transcript, index it.",
            "Podcasts, calls and any recording where the words are the content.",
        ),
        builtin_workflow(
            "video",
            "Video",
            Media,
            "Demux the audio, transcribe it, chunk and index.",
            "Recorded meetings and video libraries, searched by what is said in them.",
        ),
        builtin_workflow(
            "html",
            "HTML",
            Web,
            "Strip boilerplate, chunk the readable body, index it.",
            "Crawled pages and documentation sites, without the navigation polluting results.",
        ),
        // The one curated template: a recipe nothing deploys, so it carries its
        // definition inline. It is what exercises the `Some(definition)` half of
        // `WorkflowEntry` and the clone path in Task 11.
        WorkflowEntry {
            uid: "pdf-with-enrichment".to_owned(),
            title: "PDF with LLM enrichment".to_owned(),
            category: Documents,
            summary: "Extract per page, chunk, enrich each chunk with an LLM, index.".to_owned(),
            when_to_use:
                "Contract and report libraries where generated summaries and keywords make search results readable."
                    .to_owned(),
            definition: Some(PipelineDefinition {
                uid: "pdf-with-enrichment".to_owned(),
                name: "PDF with LLM enrichment".to_owned(),
                description: Some("Extract, chunk, enrich, index.".to_owned()),
                version: 1,
                trigger: None,
                steps: vec![
                    crate::step("extract", "pdf_extractor"),
                    crate::chunk_step(),
                    crate::step("enrich", "llm_enricher")
                        .depends_on(["chunk"])
                        .fan_out("$.documents"),
                    crate::index_step(),
                ],
                builtin: false,
                project_id: None,
            }),
        },
    ]
}

/// The curated catalog served by `GET /catalog`.
pub fn catalog() -> Catalog {
    Catalog {
        actions: actions(),
        workflows: workflows(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_action_entry_names_a_known_plugin() {
        for entry in catalog().actions {
            assert!(
                ALL_KNOWN_PLUGINS.contains(&entry.plugin.as_str()),
                "catalog describes unknown plugin `{}`",
                entry.plugin
            );
        }
    }

    #[test]
    fn every_known_plugin_has_exactly_one_entry() {
        let actions = catalog().actions;
        for name in ALL_KNOWN_PLUGINS {
            let count = actions.iter().filter(|e| e.plugin == *name).count();
            assert_eq!(
                count, 1,
                "plugin `{name}` has {count} catalog entries, want 1"
            );
        }
    }

    #[test]
    fn every_action_entry_has_copy() {
        for entry in catalog().actions {
            assert!(!entry.title.is_empty(), "{} has no title", entry.plugin);
            assert!(!entry.summary.is_empty(), "{} has no summary", entry.plugin);
            assert!(
                entry.use_cases.len() >= 2,
                "{} has {} use cases, want at least 2",
                entry.plugin,
                entry.use_cases.len()
            );
            assert!(
                entry.example_step.contains(&entry.plugin),
                "{}'s example step does not name the plugin",
                entry.plugin
            );
        }
    }

    use crate::builtin_pipelines;

    #[test]
    fn every_builtin_pipeline_has_a_workflow_entry() {
        let workflows = catalog().workflows;
        for pipeline in builtin_pipelines() {
            let count = workflows.iter().filter(|w| w.uid == pipeline.uid).count();
            assert_eq!(
                count, 1,
                "built-in `{}` has {count} catalog entries, want 1",
                pipeline.uid
            );
        }
    }

    #[test]
    fn every_builtin_workflow_entry_matches_a_real_pipeline() {
        // The mirror of `every_builtin_pipeline_has_a_workflow_entry`. Without it a
        // `builtin.*` entry can outlive the pipeline it names, and its Clone link
        // 404s against `GET /pipelines/{uid}`.
        let uids: Vec<String> = builtin_pipelines().into_iter().map(|p| p.uid).collect();
        for entry in catalog().workflows {
            if let Some(suffix) = entry.uid.strip_prefix("builtin.") {
                assert!(
                    uids.contains(&entry.uid),
                    "catalog describes `builtin.{suffix}`, which is not a built-in pipeline"
                );
            }
        }
    }

    #[test]
    fn builtin_entries_carry_no_inline_definition() {
        // `GET /pipelines` stays authoritative for anything deployed; an inline
        // copy here would be a second source of truth that silently goes stale.
        for entry in catalog().workflows {
            if entry.uid.starts_with("builtin.") {
                assert!(
                    entry.definition.is_none(),
                    "{} duplicates a deployed definition",
                    entry.uid
                );
            }
        }
    }

    #[test]
    fn template_entries_carry_a_usable_definition() {
        for entry in catalog().workflows {
            if entry.uid.starts_with("builtin.") {
                continue;
            }
            let def = entry
                .definition
                .as_ref()
                .unwrap_or_else(|| panic!("template {} has no definition", entry.uid));
            assert_eq!(def.uid, entry.uid, "{} definition uid disagrees", entry.uid);
            assert!(!def.steps.is_empty(), "{} has no steps", entry.uid);
            for step in &def.steps {
                assert!(
                    ALL_KNOWN_PLUGINS.contains(&step.plugin.as_str()),
                    "template {} step `{}` names unknown plugin `{}`",
                    entry.uid,
                    step.id,
                    step.plugin
                );
            }
        }
    }

    #[test]
    fn every_workflow_entry_has_copy() {
        for entry in catalog().workflows {
            assert!(!entry.title.is_empty(), "{} has no title", entry.uid);
            assert!(!entry.summary.is_empty(), "{} has no summary", entry.uid);
            assert!(
                !entry.when_to_use.is_empty(),
                "{} has no when_to_use",
                entry.uid
            );
        }
    }
}
