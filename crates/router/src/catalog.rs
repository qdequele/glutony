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

/// Every plugin name the catalog is expected to describe: the 17 compiled into
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
    accepts: &[InputKind],
    produces: OutputKind,
) -> ActionEntry {
    ActionEntry {
        plugin: plugin.to_owned(),
        title: title.to_owned(),
        category,
        summary: summary.to_owned(),
        use_cases: use_cases.iter().map(|s| (*s).to_owned()).collect(),
        example_step: example_step.trim_start_matches('\n').to_owned(),
        accepts: accepts.to_vec(),
        produces,
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
            &[I::Ref, I::Empty],
            OutputKind::Bytes,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Bytes,
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
            &[I::Documents],
            OutputKind::Documents,
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
            &[I::Documents],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Bytes],
            OutputKind::Documents,
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
            &[I::Documents],
            OutputKind::Indexed,
        ),
    ]
}

/// Replaced by the real table in Task 2.
fn workflows() -> Vec<WorkflowEntry> {
    vec![]
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
}
