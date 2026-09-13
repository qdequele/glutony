//! # `parquet_parser` — Apache Parquet to flat documents
//!
//! Built-in `meili-ingest` plugin that turns a Parquet file into one
//! [`Document`] per row, with flat dotted-key `fields` produced by the shared
//! rules of [`json_flattener`](meili_ingest_plugin_json).
//!
//! Parquet is columnar and its schema lives in a footer at the *end* of the file,
//! so unlike CSV or NDJSON it cannot be parsed from a prefix — the whole payload
//! is needed, which is what `PluginInput::Bytes` already hands over.
//!
//! Two things follow from Parquet being self-describing:
//!
//! * **No type guessing.** Where `csv_parser` has to sniff whether `007` is a
//!   number, Parquet states it. Integers, floats, booleans, dates and timestamps
//!   arrive typed, and nested structs and lists map onto the same dotted keys as
//!   nested JSON.
//! * **Projection is cheap.** `columns` pushes the selection down into the reader,
//!   so unread columns are never decoded off disk.
//!
//! Rows are decoded a record batch at a time (`batch_size`), which is also the
//! heartbeat and cancellation granularity. Null cells are omitted from `fields`
//! rather than stored as JSON `null`, matching how `csv_parser` skips empty cells.
//!
//! Configuration (all keys optional):
//!
//! | key              | default | meaning |
//! |------------------|---------|---------|
//! | `id_field`       | `id`    | flattened key holding the document id; generated as `<stem>_r<n>` when absent |
//! | `content_fields` | all string leaves | flattened keys joined by a space into `content` (removed from `fields`) |
//! | `title_field`    | none    | flattened key mapped onto `title` (removed from `fields`) |
//! | `columns`        | all     | columns to read; pushed down as a projection |
//! | `flatten_arrays` | `false` | flatten arrays of objects into `a.0.b` keys instead of keeping them as JSON arrays |
//! | `max_depth`      | `8`     | nesting depth beyond which objects are kept as-is |
//! | `max_rows`       | unlimited | stop after this many rows |
//! | `batch_size`     | `1024`  | rows decoded per record batch |

use arrow_json::ArrayWriter;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use bytes::Bytes;
use meili_ingest_plugin_json::{FlattenConfig, documents_from_values, id_stem};
use meili_ingest_plugin_sdk::prelude::*;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde::Deserialize;
use serde_json::Value;

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "parquet_parser";

/// Every Parquet file starts and ends with this marker.
const MAGIC: &[u8; 4] = b"PAR1";

/// Default rows per decoded record batch.
const DEFAULT_BATCH_SIZE: usize = 1_024;

/// The Parquet parser plugin. Stateless; construct with [`ParquetParserPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct ParquetParserPlugin;

impl ParquetParserPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    id_field: String,
    content_fields: Option<Vec<String>>,
    title_field: Option<String>,
    columns: Option<Vec<String>>,
    flatten_arrays: bool,
    max_depth: usize,
    max_rows: Option<usize>,
    batch_size: usize,
}

impl Default for Config {
    fn default() -> Self {
        let f = FlattenConfig::default();
        Self {
            id_field: f.id_field,
            content_fields: f.content_fields,
            title_field: f.title_field,
            columns: None,
            flatten_arrays: f.flatten_arrays,
            max_depth: f.max_depth,
            max_rows: None,
            batch_size: DEFAULT_BATCH_SIZE,
        }
    }
}

impl Config {
    fn parse(value: Value) -> Result<Self, PluginError> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let cfg: Config = serde_json::from_value(value)
            .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))?;
        if cfg.max_rows == Some(0) {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `max_rows` must be greater than zero when set"
            )));
        }
        if cfg.batch_size == 0 {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `batch_size` must be greater than zero"
            )));
        }
        if cfg.columns.as_ref().is_some_and(|c| c.is_empty()) {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `columns` must not be empty when set"
            )));
        }
        Ok(cfg)
    }

    /// The subset handed to the shared flattener, validated by its own rules.
    fn flatten(&self) -> Result<FlattenConfig, PluginError> {
        FlattenConfig::parse(serde_json::json!({
            "id_field": self.id_field,
            "content_fields": self.content_fields,
            "title_field": self.title_field,
            "flatten_arrays": self.flatten_arrays,
            "max_depth": self.max_depth,
        }))
        .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))
    }
}

/// Decode a Parquet payload into one JSON object per row.
///
/// Arrow's own JSON writer does the per-cell type mapping, so the 40-odd Arrow
/// types (decimals, dictionaries, timestamps with zones, nested lists) are
/// rendered the canonical way instead of by a hand-written table here.
fn decode_payload(
    data: Vec<u8>,
    cfg: &Config,
    cancelled: &Arc<AtomicBool>,
) -> Result<Vec<Value>, PluginError> {
    if data.len() < MAGIC.len() * 2 || !data.starts_with(MAGIC) || !data.ends_with(MAGIC) {
        return Err(PluginError::InvalidInput(format!(
            "{NAME}: not a Parquet file (missing PAR1 marker)"
        )));
    }

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(data))
        .map_err(|e| PluginError::InvalidInput(format!("{NAME}: unreadable Parquet: {e}")))?
        .with_batch_size(cfg.batch_size);

    if let Some(columns) = &cfg.columns {
        let known: Vec<String> = builder
            .parquet_schema()
            .columns()
            .iter()
            .map(|c| c.path().string())
            .collect();
        // ProjectionMask silently ignores unknown names, which would quietly drop a
        // column the pipeline asked for. Fail on the typo instead.
        for name in columns {
            let matches = known
                .iter()
                .any(|k| k == name || k.starts_with(&format!("{name}.")));
            if !matches {
                return Err(PluginError::InvalidConfig(format!(
                    "{NAME}: `columns` refers to unknown column {name:?} (available: {known:?})"
                )));
            }
        }
        let mask =
            ProjectionMask::columns(builder.parquet_schema(), columns.iter().map(String::as_str));
        builder = builder.with_projection(mask);
    }

    let reader = builder
        .build()
        .map_err(|e| PluginError::InvalidInput(format!("{NAME}: unreadable Parquet: {e}")))?;

    let mut rows = Vec::new();
    for batch in reader {
        // A wide file can hold many row groups; a cancel should not have to wait
        // for the whole decode to finish.
        if cancelled.load(Ordering::Relaxed) {
            return Err(PluginError::Cancelled);
        }
        let batch =
            batch.map_err(|e| PluginError::InvalidInput(format!("{NAME}: bad row group: {e}")))?;
        let mut buf = Vec::new();
        let mut writer = ArrayWriter::new(&mut buf);
        writer
            .write(&batch)
            .and_then(|()| writer.finish())
            .map_err(|e| PluginError::NonRetryable(format!("{NAME}: cannot render batch: {e}")))?;
        let decoded: Vec<Value> = serde_json::from_slice(&buf)?;
        rows.extend(decoded);
        if cfg.max_rows.is_some_and(|max| rows.len() >= max) {
            break;
        }
    }
    if let Some(max) = cfg.max_rows {
        rows.truncate(max);
    }
    Ok(rows)
}

#[async_trait]
impl Plugin for ParquetParserPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Parses Apache Parquet files into one document per row. Types come from the \
                 file's own schema, and `columns` is pushed down as a read projection.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["application/vnd.apache.parquet", "application/x-parquet"])
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "id_field": {
                        "type": "string",
                        "default": "id",
                        "description": "Flattened key holding the document id. Generated as <filename stem>_r<row> when absent."
                    },
                    "content_fields": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                        "default": null,
                        "description": "Flattened keys joined by a space into `content` (removed from fields). Defaults to every string leaf."
                    },
                    "title_field": {
                        "type": ["string", "null"],
                        "default": null,
                        "description": "Flattened key mapped onto `title` (removed from fields)."
                    },
                    "columns": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                        "default": null,
                        "description": "Columns to read, pushed down as a projection. Defaults to every column."
                    },
                    "flatten_arrays": {
                        "type": "boolean",
                        "default": false,
                        "description": "Flatten arrays of objects into a.0.b keys. Arrays of scalars are always kept as arrays."
                    },
                    "max_depth": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 8,
                        "description": "Nesting depth beyond which objects are kept as JSON values."
                    },
                    "max_rows": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "default": null,
                        "description": "Stop after this many rows."
                    },
                    "batch_size": {
                        "type": "integer",
                        "minimum": 1,
                        "default": 1024,
                        "description": "Rows decoded per record batch."
                    }
                }
            }))
    }

    async fn execute(
        &self,
        ctx: &ActivityContext,
        input: PluginInput,
        config: Value,
    ) -> Result<PluginOutput, PluginError> {
        let cfg = Config::parse(config)?;
        let flatten = cfg.flatten()?;
        let blob = input.into_bytes()?;
        ctx.check_cancelled()?;

        let data = blob.data.clone();
        let decode_cfg = cfg.clone();
        let cancelled = ctx.cancellation_flag();
        let rows = run_blocking(move || decode_payload(data, &decode_cfg, &cancelled)).await??;
        ctx.heartbeat(format!("{NAME}: {} rows decoded", rows.len()));

        let stem = id_stem(blob.filename.as_deref(), "parquet");
        let meta = DocumentMeta {
            source: blob.filename.clone(),
            filename: blob.filename.clone(),
            mime: Some(blob.mime.clone()),
            ..Default::default()
        };
        let items = rows
            .into_iter()
            .enumerate()
            .map(|(i, v)| (v, format!("{stem}_r{}", i + 1)))
            .collect();

        Ok(PluginOutput::Documents(documents_from_values(
            ctx, items, &flatten, &meta, NAME,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field};
    use parquet::arrow::ArrowWriter;
    use serde_json::json;
    use std::sync::Arc;

    /// Serialize record batches into an in-memory Parquet file.
    fn parquet_file(batches: &[RecordBatch]) -> Vec<u8> {
        let mut out = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut out, batches[0].schema(), None).expect("writer");
        for batch in batches {
            writer.write(batch).expect("write");
        }
        writer.close().expect("close");
        out
    }

    /// A small typed table with a null in it.
    fn people() -> RecordBatch {
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            (
                "name",
                Arc::new(StringArray::from(vec!["Alice", "Bob"])) as ArrayRef,
            ),
            (
                "score",
                Arc::new(Float64Array::from(vec![Some(9.5), None])) as ArrayRef,
            ),
            (
                "active",
                Arc::new(BooleanArray::from(vec![true, false])) as ArrayRef,
            ),
        ])
        .expect("batch")
    }

    fn bytes(data: Vec<u8>, filename: Option<&str>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            data,
            "application/vnd.apache.parquet",
            filename.map(str::to_owned),
        ))
    }

    async fn run(data: Vec<u8>, filename: Option<&str>, cfg: Value) -> Vec<Document> {
        match ParquetParserPlugin::new()
            .execute(&ActivityContext::noop(), bytes(data, filename), cfg)
            .await
            .unwrap()
        {
            PluginOutput::Documents(d) => d,
            other => panic!("expected documents, got {other:?}"),
        }
    }

    #[test]
    fn manifest_is_correct() {
        let m = ParquetParserPlugin.manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Bytes]);
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(
            m.content_types
                .contains(&"application/vnd.apache.parquet".to_string())
        );
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in [
            "id_field",
            "content_fields",
            "title_field",
            "columns",
            "flatten_arrays",
            "max_depth",
            "max_rows",
            "batch_size",
        ] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
    }

    #[tokio::test]
    async fn rows_keep_the_types_declared_by_the_file() {
        let docs = run(parquet_file(&[people()]), Some("people.parquet"), json!({})).await;
        assert_eq!(docs.len(), 2);
        let a = &docs[0];
        assert_eq!(a.id, "1", "id comes from the id column");
        assert_eq!(a.fields["name"], json!("Alice"));
        assert_eq!(
            a.fields["score"],
            json!(9.5),
            "no type sniffing: the schema says double"
        );
        assert_eq!(a.fields["active"], json!(true));
        assert_eq!(a.content, "Alice", "content is every string leaf");
        assert_eq!(a.meta.filename.as_deref(), Some("people.parquet"));
        assert_eq!(
            a.meta.mime.as_deref(),
            Some("application/vnd.apache.parquet")
        );
        assert!(
            !docs[1].fields.contains_key("score"),
            "null cells are omitted, like empty CSV cells"
        );
    }

    #[tokio::test]
    async fn nested_structs_flatten_to_dotted_keys() {
        let address = StructArray::from(vec![
            (
                Arc::new(Field::new("city", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["Paris", "Lyon"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("zip", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["75001", "69001"])) as ArrayRef,
            ),
        ]);
        let batch = RecordBatch::try_from_iter(vec![
            ("id", Arc::new(Int64Array::from(vec![1, 2])) as ArrayRef),
            ("address", Arc::new(address) as ArrayRef),
        ])
        .expect("batch");

        let d = &run(parquet_file(&[batch]), None, json!({})).await[0];
        assert_eq!(d.fields["address.city"], json!("Paris"));
        assert_eq!(d.fields["address.zip"], json!("75001"));
    }

    #[tokio::test]
    async fn columns_are_pushed_down_as_a_projection() {
        let data = parquet_file(&[people()]);
        let docs = run(data.clone(), None, json!({"columns": ["id", "name"]})).await;
        let d = &docs[0];
        assert_eq!(d.fields["name"], json!("Alice"));
        assert!(
            !d.fields.contains_key("score"),
            "unselected column not read"
        );
        assert!(!d.fields.contains_key("active"));

        // A typo must fail loudly instead of silently returning nothing.
        let err = ParquetParserPlugin::new()
            .execute(
                &ActivityContext::noop(),
                bytes(data, None),
                json!({"columns": ["nope"]}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");
    }

    #[tokio::test]
    async fn max_rows_and_batch_size_and_generated_ids() {
        let batch = RecordBatch::try_from_iter(vec![(
            "v",
            Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])) as ArrayRef,
        )])
        .expect("batch");
        let data = parquet_file(&[batch]);

        let docs = run(data.clone(), Some("rows.parquet"), json!({})).await;
        assert_eq!(docs.len(), 5);
        assert_eq!(docs[0].id, "rows_r1", "generated ids are 1-based");
        assert_eq!(docs[4].id, "rows_r5");

        let docs = run(data.clone(), None, json!({"max_rows": 2})).await;
        assert_eq!(docs.len(), 2);

        // A batch size smaller than the file exercises the multi-batch path.
        let docs = run(data, None, json!({"batch_size": 2})).await;
        assert_eq!(docs.len(), 5);
    }

    #[tokio::test]
    async fn title_and_content_fields_are_honoured() {
        let batch = RecordBatch::try_from_iter(vec![
            ("sku", Arc::new(StringArray::from(vec!["A 1"])) as ArrayRef),
            (
                "headline",
                Arc::new(StringArray::from(vec!["Hat"])) as ArrayRef,
            ),
            (
                "body",
                Arc::new(StringArray::from(vec!["Warm hat"])) as ArrayRef,
            ),
        ])
        .expect("batch");
        let cfg = json!({
            "id_field": "sku",
            "title_field": "headline",
            "content_fields": ["body"]
        });
        let d = &run(parquet_file(&[batch]), None, cfg).await[0];
        assert_eq!(d.id, "A_1", "id is sanitized");
        assert_eq!(d.title.as_deref(), Some("Hat"));
        assert_eq!(d.content, "Warm hat");
    }

    #[tokio::test]
    async fn invalid_input_and_config_are_rejected() {
        let plugin = ParquetParserPlugin::new();
        let ctx = ActivityContext::noop();
        let good = parquet_file(&[people()]);

        for bad in [Vec::new(), b"PAR".to_vec(), b"not a parquet file".to_vec()] {
            let err = plugin
                .execute(&ctx, bytes(bad, None), json!({}))
                .await
                .unwrap_err();
            assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");
        }

        // Right markers, garbage in between.
        let mut corrupt = b"PAR1".to_vec();
        corrupt.extend(vec![0u8; 32]);
        corrupt.extend(b"PAR1");
        let err = plugin
            .execute(&ctx, bytes(corrupt, None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");

        for bad in [
            json!({"bogus": true}),
            json!({"max_rows": 0}),
            json!({"batch_size": 0}),
            json!({"columns": []}),
            json!({"max_depth": 0}),
        ] {
            let err = plugin
                .execute(&ctx, bytes(good.clone(), None), bad.clone())
                .await
                .unwrap_err();
            assert!(matches!(err, PluginError::InvalidConfig(_)), "{bad}: {err}");
        }

        let err = plugin
            .execute(&ctx, PluginInput::Documents(vec![]), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");
    }

    #[tokio::test]
    async fn honours_cancellation() {
        let ctx = ActivityContext::noop();
        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = ParquetParserPlugin::new()
            .execute(&ctx, bytes(parquet_file(&[people()]), None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled), "{err}");
    }
}
