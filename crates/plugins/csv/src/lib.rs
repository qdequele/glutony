//! # `csv_parser` — CSV / TSV to documents
//!
//! Built-in `meili-ingest` plugin that turns a CSV payload into one
//! [`Document`] per row. Column headers become keys of `Document::fields`
//! (with cells parsed into typed JSON when the whole cell is an integer,
//! float or boolean), and `content` is the space-joined text of the
//! configured content columns.
//!
//! Configuration (all keys optional):
//!
//! | key               | default            | meaning |
//! |-------------------|--------------------|---------|
//! | `delimiter`       | sniffed            | single ASCII character; sniffed among `,` `;` `\t` `\|` from the first line |
//! | `has_headers`     | `true`             | first row is the header; otherwise columns are named `col_1`, `col_2`, ... |
//! | `id_column`       | none               | column whose value becomes the document id (sanitized) |
//! | `content_columns` | all columns        | columns joined by a space into `content` |
//! | `max_rows`        | unlimited          | stop after this many data rows |
//! | `trim`            | `true`             | trim whitespace around headers and cells |

use csv::{ReaderBuilder, StringRecord, Trim};
use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "csv_parser";

/// Rows between two heartbeats.
const HEARTBEAT_EVERY: usize = 100;

/// Delimiters tried by the sniffer, in order of preference on ties.
const CANDIDATE_DELIMITERS: [u8; 4] = *b",;\t|";

/// The CSV parser plugin. Stateless; construct with [`CsvParserPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct CsvParserPlugin;

impl CsvParserPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    delimiter: Option<char>,
    has_headers: bool,
    id_column: Option<String>,
    content_columns: Option<Vec<String>>,
    max_rows: Option<usize>,
    trim: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            delimiter: None,
            has_headers: true,
            id_column: None,
            content_columns: None,
            max_rows: None,
            trim: true,
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
        if let Some(d) = cfg.delimiter
            && !d.is_ascii()
        {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `delimiter` must be a single ASCII character, got {d:?}"
            )));
        }
        if cfg.max_rows == Some(0) {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `max_rows` must be greater than zero when set"
            )));
        }
        Ok(cfg)
    }
}

/// Pick the delimiter by counting candidate characters on the first line.
/// Falls back to `,` when nothing matches.
fn sniff_delimiter(data: &[u8]) -> u8 {
    let first_line = data.split(|b| *b == b'\n').next().unwrap_or(data);
    let mut best = b',';
    let mut best_count = 0usize;
    for cand in CANDIDATE_DELIMITERS {
        let count = first_line.iter().filter(|b| **b == cand).count();
        if count > best_count {
            best = cand;
            best_count = count;
        }
    }
    best
}

/// Parse a cell into typed JSON: integer, float, boolean, or string.
/// Leading-zero numerics (`007`, zip codes) stay strings.
fn typed_cell(cell: &str) -> Value {
    let looks_like_leading_zero = {
        let digits = cell.strip_prefix(['-', '+']).unwrap_or(cell);
        digits.len() > 1 && digits.starts_with('0') && !digits.starts_with("0.")
    };
    if !looks_like_leading_zero {
        if let Ok(i) = cell.parse::<i64>() {
            return Value::from(i);
        }
        if let Ok(u) = cell.parse::<u64>() {
            return Value::from(u);
        }
        if let Ok(f) = cell.parse::<f64>()
            && f.is_finite()
            && let Some(n) = serde_json::Number::from_f64(f)
        {
            return Value::Number(n);
        }
    }
    match cell.to_ascii_lowercase().as_str() {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        _ => Value::String(cell.to_owned()),
    }
}

/// Filename stem used to build generated ids (`<stem>_r<n>`).
fn id_stem(filename: Option<&str>) -> String {
    let stem = filename
        .map(|f| f.rsplit(['/', '\\']).next().unwrap_or(f))
        .map(|f| f.rsplit_once('.').map(|(s, _)| s).unwrap_or(f))
        .filter(|s| !s.is_empty())
        .unwrap_or("csv");
    sanitize_id(stem)
}

fn column_name(headers: &[String], idx: usize) -> String {
    headers
        .get(idx)
        .cloned()
        .unwrap_or_else(|| format!("col_{}", idx + 1))
}

/// Resolve a configured column name to its index, or fail with `InvalidConfig`.
fn column_index(headers: &[String], name: &str, key: &str) -> Result<usize, PluginError> {
    headers.iter().position(|h| h == name).ok_or_else(|| {
        PluginError::InvalidConfig(format!(
            "{NAME}: `{key}` refers to unknown column {name:?} (available: {headers:?})"
        ))
    })
}

#[async_trait]
impl Plugin for CsvParserPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Parses CSV/TSV files into one document per row. Headers become typed fields, \
                 `content` is the space-joined text of the content columns.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["text/csv", "text/tab-separated-values"])
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "delimiter": {
                        "type": ["string", "null"],
                        "minLength": 1,
                        "maxLength": 1,
                        "default": null,
                        "description": "Field delimiter (single ASCII char). Sniffed among , ; tab | when omitted."
                    },
                    "has_headers": {
                        "type": "boolean",
                        "default": true,
                        "description": "Whether the first row holds column names. Otherwise columns are col_1, col_2, ..."
                    },
                    "id_column": {
                        "type": ["string", "null"],
                        "default": null,
                        "description": "Column whose value becomes the document id (sanitized). Defaults to <filename stem>_r<row>."
                    },
                    "content_columns": {
                        "type": ["array", "null"],
                        "items": {"type": "string"},
                        "default": null,
                        "description": "Columns joined by a space into `content`. Defaults to all columns."
                    },
                    "max_rows": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "default": null,
                        "description": "Stop after this many data rows."
                    },
                    "trim": {
                        "type": "boolean",
                        "default": true,
                        "description": "Trim whitespace around headers and cells."
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
        let blob = input.into_bytes()?;

        let delimiter = match cfg.delimiter {
            Some(c) => c as u8,
            None => sniff_delimiter(&blob.data),
        };
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(cfg.has_headers)
            .flexible(true)
            .trim(if cfg.trim { Trim::All } else { Trim::None })
            .from_reader(blob.data.as_slice());

        let headers: Vec<String> = if cfg.has_headers {
            reader
                .headers()
                .map_err(|e| PluginError::InvalidInput(format!("{NAME}: invalid CSV header: {e}")))?
                .iter()
                .enumerate()
                .map(|(i, h)| {
                    if h.is_empty() {
                        format!("col_{}", i + 1)
                    } else {
                        h.to_owned()
                    }
                })
                .collect()
        } else {
            Vec::new()
        };

        // Resolve configured columns up front so typos fail fast.
        let id_idx = match &cfg.id_column {
            Some(name) if cfg.has_headers => Some(column_index(&headers, name, "id_column")?),
            Some(name) => Some(parse_positional(name, "id_column")?),
            None => None,
        };
        let content_idx: Option<Vec<usize>> = match &cfg.content_columns {
            Some(cols) if cfg.has_headers => Some(
                cols.iter()
                    .map(|c| column_index(&headers, c, "content_columns"))
                    .collect::<Result<_, _>>()?,
            ),
            Some(cols) => Some(
                cols.iter()
                    .map(|c| parse_positional(c, "content_columns"))
                    .collect::<Result<_, _>>()?,
            ),
            None => None,
        };

        let stem = id_stem(blob.filename.as_deref());
        let meta = DocumentMeta {
            source: blob.filename.clone(),
            filename: blob.filename.clone(),
            mime: Some(blob.mime.clone()),
            ..Default::default()
        };

        let mut docs = Vec::new();
        let mut record = StringRecord::new();
        let mut row: usize = 0;
        loop {
            ctx.check_cancelled()?;
            if cfg.max_rows.is_some_and(|max| row >= max) {
                break;
            }
            let more = reader.read_record(&mut record).map_err(|e| {
                PluginError::InvalidInput(format!("{NAME}: invalid CSV at row {}: {e}", row + 1))
            })?;
            if !more {
                break;
            }
            row += 1;
            if row.is_multiple_of(HEARTBEAT_EVERY) {
                ctx.heartbeat(format!("{NAME}: {row} rows"));
            }

            let mut fields = Map::new();
            let mut content_parts: Vec<&str> = Vec::new();
            for (i, cell) in record.iter().enumerate() {
                if cell.is_empty() {
                    continue;
                }
                fields.insert(column_name(&headers, i), typed_cell(cell));
                let include = match &content_idx {
                    Some(idx) => idx.contains(&i),
                    None => true,
                };
                if include {
                    content_parts.push(cell);
                }
            }
            if fields.is_empty() {
                // Blank line: nothing to index.
                continue;
            }

            let id = id_idx
                .and_then(|i| record.get(i))
                .filter(|v| !v.is_empty())
                .map(sanitize_id)
                .unwrap_or_else(|| format!("{stem}_r{row}"));
            fields.insert("_row".into(), Value::from(row));

            docs.push(Document {
                id,
                title: None,
                content: content_parts.join(" "),
                fields,
                meta: meta.clone(),
            });
        }

        Ok(PluginOutput::Documents(docs))
    }
}

/// Without headers, columns are addressed as `col_<n>` (1-based) in the config.
fn parse_positional(name: &str, key: &str) -> Result<usize, PluginError> {
    name.strip_prefix("col_")
        .and_then(|n| n.parse::<usize>().ok())
        .filter(|n| *n >= 1)
        .map(|n| n - 1)
        .ok_or_else(|| {
            PluginError::InvalidConfig(format!(
                "{NAME}: with `has_headers: false`, `{key}` must use positional names like `col_1`, got {name:?}"
            ))
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bytes(data: &str, filename: Option<&str>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            data.as_bytes().to_vec(),
            "text/csv",
            filename.map(str::to_owned),
        ))
    }

    async fn run(data: &str, filename: Option<&str>, cfg: Value) -> Vec<Document> {
        let out = CsvParserPlugin::new()
            .execute(&ActivityContext::noop(), bytes(data, filename), cfg)
            .await
            .unwrap();
        match out {
            PluginOutput::Documents(d) => d,
            other => panic!("expected documents, got {other:?}"),
        }
    }

    #[test]
    fn manifest_is_correct() {
        let m = CsvParserPlugin.manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Bytes]);
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(m.content_types.contains(&"text/csv".to_string()));
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in [
            "delimiter",
            "has_headers",
            "id_column",
            "content_columns",
            "max_rows",
            "trim",
        ] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
    }

    #[tokio::test]
    async fn parses_rows_with_typed_fields_and_generated_ids() {
        let csv = "name,age,score,active\nAlice,30,9.5,true\nBob,,7,false\n";
        let docs = run(csv, Some("people.csv"), json!({})).await;
        assert_eq!(docs.len(), 2);
        let a = &docs[0];
        assert_eq!(a.id, "people_r1");
        assert_eq!(a.fields["name"], json!("Alice"));
        assert_eq!(a.fields["age"], json!(30));
        assert_eq!(a.fields["score"], json!(9.5));
        assert_eq!(a.fields["active"], json!(true));
        assert_eq!(a.fields["_row"], json!(1));
        assert_eq!(a.content, "Alice 30 9.5 true");
        assert_eq!(a.meta.filename.as_deref(), Some("people.csv"));
        assert_eq!(a.meta.mime.as_deref(), Some("text/csv"));
        let b = &docs[1];
        assert_eq!(b.id, "people_r2");
        assert!(!b.fields.contains_key("age"), "empty cells are skipped");
        assert_eq!(b.content, "Bob 7 false");
    }

    #[tokio::test]
    async fn sniffs_semicolon_and_tab_delimiters() {
        let docs = run("a;b\n1;2\n", None, json!({})).await;
        assert_eq!(docs[0].fields["a"], json!(1));
        assert_eq!(docs[0].fields["b"], json!(2));
        assert_eq!(docs[0].id, "csv_r1");

        let docs = run("a\tb\nx\ty\n", None, json!({})).await;
        assert_eq!(docs[0].fields["b"], json!("y"));
    }

    #[tokio::test]
    async fn id_column_and_content_columns() {
        let csv = "sku,title,desc\nA 1,Hat,Warm hat\nB-2,Shoe,Nice shoe\n";
        let cfg = json!({"id_column": "sku", "content_columns": ["title", "desc"]});
        let docs = run(csv, Some("catalog.csv"), cfg).await;
        assert_eq!(docs[0].id, "A_1", "id is sanitized");
        assert_eq!(docs[1].id, "B-2");
        assert_eq!(docs[0].content, "Hat Warm hat");
        assert_eq!(docs[0].fields["sku"], json!("A 1"));
    }

    #[tokio::test]
    async fn no_headers_uses_positional_columns() {
        let docs = run("1,x\n2,y\n", None, json!({"has_headers": false})).await;
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].fields["col_1"], json!(1));
        assert_eq!(docs[0].fields["col_2"], json!("x"));
    }

    #[tokio::test]
    async fn max_rows_and_flexible_rows() {
        let csv = "a,b\n1,2,3\n4\n5,6\n";
        let docs = run(csv, None, json!({"max_rows": 2})).await;
        assert_eq!(docs.len(), 2);
        assert_eq!(
            docs[0].fields["col_3"],
            json!(3),
            "extra cells get positional names"
        );
        assert!(
            !docs[1].fields.contains_key("b"),
            "short rows just lack the field"
        );
    }

    #[tokio::test]
    async fn leading_zeros_stay_strings_and_trim_is_configurable() {
        let docs = run("zip, v\n 01234 , 7 \n", None, json!({})).await;
        assert_eq!(docs[0].fields["zip"], json!("01234"));
        assert_eq!(docs[0].fields["v"], json!(7));
        let docs = run("zip, v\n 01234 , 7 \n", None, json!({"trim": false})).await;
        assert_eq!(docs[0].fields[" v"], json!(" 7 "));
    }

    #[tokio::test]
    async fn invalid_config_is_rejected() {
        let plugin = CsvParserPlugin::new();
        let ctx = ActivityContext::noop();
        let err = plugin
            .execute(&ctx, bytes("a,b\n1,2\n", None), json!({"delimiter": "é"}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");
        let err = plugin
            .execute(
                &ctx,
                bytes("a,b\n1,2\n", None),
                json!({"id_column": "nope"}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");
        let err = plugin
            .execute(&ctx, bytes("a,b\n1,2\n", None), json!({"unknown_key": 1}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");
    }

    #[tokio::test]
    async fn rejects_non_bytes_input_and_honours_cancellation() {
        let plugin = CsvParserPlugin::new();
        let err = plugin
            .execute(
                &ActivityContext::noop(),
                PluginInput::Documents(vec![]),
                json!({}),
            )
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)));

        let ctx = ActivityContext::noop();
        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = plugin
            .execute(&ctx, bytes("a\n1\n", None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled));
    }

    #[tokio::test]
    async fn empty_file_yields_no_documents() {
        let docs = run("a,b\n", None, json!({})).await;
        assert!(docs.is_empty());
    }
}
