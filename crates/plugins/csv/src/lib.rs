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
//! | `delimiter`       | sniffed            | single ASCII character; sniffed among `,` `;` `\t` `\|` as the one that splits the first records into the most consistent number of fields (quoted fields ignored) |
//! | `has_headers`     | `true`             | first row is the header; otherwise columns are named `col_1`, `col_2`, ... |
//! | `id_column`       | none               | column whose value becomes the document id (sanitized) |
//! | `content_columns` | all columns        | columns joined by a space into `content` |
//! | `max_rows`        | unlimited          | stop after this many data rows |
//! | `trim`            | `true`             | trim whitespace around headers and cells |
//! | `encoding`        | detected           | character encoding of the file, as a WHATWG label (`windows-1252`, `iso-8859-15`, `shift_jis`, `utf-16le`, …) |
//!
//! Without `encoding`, the bytes are decoded by the first rule that applies: a
//! byte order mark (UTF-8, UTF-16LE, UTF-16BE); UTF-8 when the whole file is valid
//! UTF-8; the charset the server declared ([`Blob::charset`]) unless it is UTF-8,
//! which the bytes just contradicted; else windows-1252, which maps every byte, so
//! an Excel export in a Western European locale reads right and no file fails on a
//! stray byte. A forced `encoding` replaces bytes it cannot map with U+FFFD.

use std::borrow::Cow;
use std::cmp::Reverse;

use encoding_rs::Encoding;

use csv::{ByteRecord, ReaderBuilder, StringRecord, Trim};
use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "csv_parser";

/// Rows between two heartbeats.
const HEARTBEAT_EVERY: usize = 100;

/// Delimiters tried by the sniffer, in order of preference on ties.
const CANDIDATE_DELIMITERS: [u8; 4] = *b",;\t|";

/// At most this many bytes from the start of the payload are sampled to sniff
/// the delimiter.
const SNIFF_BYTES: usize = 64 * 1024;

/// At most this many records are sampled to sniff the delimiter.
const SNIFF_RECORDS: usize = 100;

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
    encoding: Option<String>,
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
            encoding: None,
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
        cfg.forced_encoding()?;
        Ok(cfg)
    }

    /// The encoding `encoding` names, when set.
    fn forced_encoding(&self) -> Result<Option<&'static Encoding>, PluginError> {
        self.encoding
            .as_deref()
            .map(|label| {
                Encoding::for_label(label.trim().as_bytes()).ok_or_else(|| {
                    PluginError::InvalidConfig(format!(
                        "{NAME}: `encoding` {label:?} is not a known character encoding \
                         (try utf-8, windows-1252, iso-8859-15, shift_jis, utf-16le)"
                    ))
                })
            })
            .transpose()
    }
}

/// Encoding of a file that is not valid UTF-8 and declares nothing usable.
const FALLBACK_ENCODING: &Encoding = encoding_rs::WINDOWS_1252;

/// The file's bytes as UTF-8, borrowed when they already are. See the module docs
/// for the detection order; `forced` (the `encoding` option) skips it.
fn decode_text<'a>(
    data: &'a [u8],
    declared: Option<&str>,
    forced: Option<&'static Encoding>,
) -> Cow<'a, [u8]> {
    let utf8 = |text: Cow<'a, str>| match text {
        Cow::Borrowed(t) => Cow::Borrowed(t.as_bytes()),
        Cow::Owned(t) => Cow::Owned(t.into_bytes()),
    };
    if let Some(encoding) = forced {
        return utf8(encoding.decode_with_bom_removal(data).0);
    }
    if let Some((encoding, bom_len)) = Encoding::for_bom(data) {
        return utf8(encoding.decode_without_bom_handling(&data[bom_len..]).0);
    }
    if std::str::from_utf8(data).is_ok() {
        return Cow::Borrowed(data);
    }
    let encoding = declared
        .and_then(|label| Encoding::for_label(label.as_bytes()))
        .filter(|e| *e != encoding_rs::UTF_8)
        .unwrap_or(FALLBACK_ENCODING);
    utf8(encoding.decode_without_bom_handling(data).0)
}

/// How a candidate delimiter splits the sample. Compared field by field: the
/// higher score wins.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct DelimiterScore {
    /// Records with the modal field count, as a share of all sampled records
    /// (in thousandths, so scores compare exactly).
    consistency: usize,
    /// The modal field count itself.
    fields: usize,
    /// Fields that start with a space, fewer is better: a delimiter followed by a
    /// space is more likely prose punctuation (`Address, City`) than a separator.
    leading_spaces: Reverse<usize>,
}

/// Score `delimiter` over the sample, parsing it with the same quoting rules as
/// the real reader so delimiters inside quoted fields do not count. `None` when it
/// never yields more than one field per record.
fn score_delimiter(sample: &[u8], truncated: bool, delimiter: u8) -> Option<DelimiterScore> {
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(false)
        .flexible(true)
        .from_reader(sample);
    let mut records: Vec<(usize, usize)> = Vec::new();
    let mut record = ByteRecord::new();
    while records.len() < SNIFF_RECORDS {
        match reader.read_byte_record(&mut record) {
            Ok(true) => {
                let leading = record.iter().skip(1).filter(|f| f.first() == Some(&b' '));
                records.push((record.len(), leading.count()));
            }
            Ok(false) | Err(_) => break,
        }
    }
    // A sample cut short ends mid-record; that record says nothing reliable.
    if truncated && records.len() > 1 && reader.is_done() {
        records.pop();
    }
    if records.is_empty() {
        return None;
    }

    let mut counts: Vec<(usize, usize)> = Vec::new(); // (field count, records)
    for (len, _) in &records {
        match counts.iter_mut().find(|(n, _)| n == len) {
            Some((_, seen)) => *seen += 1,
            None => counts.push((*len, 1)),
        }
    }
    let (fields, seen) = counts
        .into_iter()
        .max_by_key(|&(n, seen)| (seen, n))
        .expect("at least one record");
    if fields < 2 {
        return None;
    }
    let leading: usize = records.iter().map(|(_, l)| l).sum();
    Some(DelimiterScore {
        consistency: seen * 1000 / records.len(),
        fields,
        leading_spaces: Reverse(leading),
    })
}

/// Pick the delimiter that splits the first records (up to [`SNIFF_RECORDS`] or
/// [`SNIFF_BYTES`]) into the most consistent number of fields, ignoring delimiters
/// inside quoted fields. Ties go to more fields, then to the candidate less often
/// followed by a space, then to the order of [`CANDIDATE_DELIMITERS`]. Falls back
/// to `,` when no candidate yields more than one field.
fn sniff_delimiter(data: &[u8]) -> u8 {
    let truncated = data.len() > SNIFF_BYTES;
    let sample = &data[..data.len().min(SNIFF_BYTES)];
    let mut best = (b',', None);
    for cand in CANDIDATE_DELIMITERS {
        let score = score_delimiter(sample, truncated, cand);
        if score > best.1 {
            best = (cand, score);
        }
    }
    best.0
}

/// Parse a cell into typed JSON: integer, float, boolean, or string.
///
/// Cells that look like identifiers rather than quantities keep their text, since
/// turning them into numbers would change them: leading-zero numerics (`007`, zip
/// codes), a leading `+` (`+33612345678`), integers too wide for 64 bits (an f64
/// would round their digits) and exponent notation (`1E5`, a SKU as often as a
/// number).
fn typed_cell(cell: &str) -> Value {
    let unsigned = cell.strip_prefix('-').unwrap_or(cell);
    let looks_like_leading_zero =
        unsigned.len() > 1 && unsigned.starts_with('0') && !unsigned.starts_with("0.");
    let looks_like_identifier = looks_like_leading_zero
        || cell.starts_with('+')
        || cell.contains(['e', 'E'])
        // An integer neither i64 nor u64 holds stays text rather than an f64.
        || (!unsigned.is_empty()
            && unsigned.bytes().all(|b| b.is_ascii_digit())
            && cell.parse::<i64>().is_err()
            && cell.parse::<u64>().is_err());
    if !looks_like_identifier {
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
fn id_stem(blob: &Blob) -> String {
    blob.id_stem("csv")
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
                        "description": "Field delimiter (single ASCII char). When omitted, sniffed among , ; tab | as the one that splits the first records into the most consistent number of fields, ignoring quoted fields."
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
                    },
                    "encoding": {
                        "type": ["string", "null"],
                        "default": null,
                        "description": "Character encoding of the file, as a WHATWG label (windows-1252, iso-8859-15, shift_jis, utf-16le, ...). When omitted: a byte order mark, else UTF-8 when the file is valid UTF-8, else the charset the server declared, else windows-1252."
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
        // Parsing is CPU-bound: keep it off the async runtime.
        let ctx = ctx.clone();
        let docs = run_blocking(move || parse_csv(&ctx, &blob, &cfg)).await??;
        Ok(PluginOutput::Documents(docs))
    }
}

/// Parse `blob` into one document per row. Synchronous and CPU-bound: run it with
/// [`run_blocking`]. It polls cancellation per row, since a blocking task cannot
/// be aborted from outside.
fn parse_csv(
    ctx: &ActivityContext,
    blob: &Blob,
    cfg: &Config,
) -> Result<Vec<Document>, PluginError> {
    let data = decode_text(&blob.data, blob.charset.as_deref(), cfg.forced_encoding()?);
    let delimiter = match cfg.delimiter {
        Some(c) => c as u8,
        None => sniff_delimiter(&data),
    };
    let mut reader = ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(cfg.has_headers)
        .flexible(true)
        .trim(if cfg.trim { Trim::All } else { Trim::None })
        .from_reader(&*data);

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

    let stem = id_stem(blob);
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

    Ok(docs)
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
            "encoding",
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
    async fn sniffing_ignores_delimiters_inside_quoted_fields() {
        let docs = run("\"Last, First\";age\n\"Doe, John\";42\n", None, json!({})).await;
        assert_eq!(docs[0].fields["Last, First"], json!("Doe, John"));
        assert_eq!(docs[0].fields["age"], json!(42));
    }

    #[tokio::test]
    async fn sniffing_prefers_the_delimiter_not_used_as_prose_punctuation() {
        let docs = run(
            "Name;Address, City\nBob;12 Main St, Paris\n",
            None,
            json!({}),
        )
        .await;
        assert_eq!(docs[0].fields["Name"], json!("Bob"));
        assert_eq!(docs[0].fields["Address, City"], json!("12 Main St, Paris"));
    }

    #[tokio::test]
    async fn sniffing_prefers_the_delimiter_with_a_consistent_field_count() {
        // The header has as many commas as tabs, but only tabs split every row
        // into the same number of fields.
        let tsv = "title\tsummary,short,long\tyear\nDune\tSand,worms,spice\t1965\nEmma\tMatchmaking\t1815\n";
        let docs = run(tsv, None, json!({})).await;
        assert_eq!(docs.len(), 2);
        assert_eq!(
            docs[0].fields["summary,short,long"],
            json!("Sand,worms,spice")
        );
        assert_eq!(docs[1].fields["year"], json!(1815));

        // A comma file whose free-text column sometimes holds a semicolon.
        let docs = run("id,tags\n1,a;b\n2,c\n3,d\n", None, json!({})).await;
        assert_eq!(docs[0].fields["tags"], json!("a;b"));
    }

    #[test]
    fn sniffing_falls_back_to_comma_for_single_column_and_empty_input() {
        assert_eq!(sniff_delimiter(b""), b',');
        assert_eq!(sniff_delimiter(b"single\ncolumn\n"), b',');
        assert_eq!(sniff_delimiter(b"a,b\n"), b',');
        assert_eq!(sniff_delimiter(b"a;b\n"), b';');
        assert_eq!(sniff_delimiter(b"a|b|c\n1|2|3"), b'|');
    }

    /// Parsing a large payload must not hold the runtime thread: on a
    /// single-threaded runtime, a ticker task keeps running while it parses.
    #[tokio::test(flavor = "current_thread")]
    async fn parsing_runs_off_the_async_runtime() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        let ticks = Arc::new(AtomicUsize::new(0));
        let ticker = tokio::spawn({
            let ticks = ticks.clone();
            async move {
                loop {
                    ticks.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                }
            }
        });
        let mut csv = String::from("id,name,score\n");
        for i in 0..20_000 {
            csv.push_str(&format!("{i},row {i},{i}.5\n"));
        }
        let docs = run(&csv, None, json!({})).await;
        ticker.abort();
        assert_eq!(docs.len(), 20_000);
        assert!(
            ticks.load(Ordering::Relaxed) > 0,
            "the runtime was blocked for the whole parse"
        );
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
    async fn identifier_like_cells_keep_their_text() {
        let docs = run(
            "phone,order_ref,sku,neg_ref,small_e\n+33612345678,123456789012345678901234,1E5,-123456789012345678901234,2e3\n",
            None,
            json!({}),
        )
        .await;
        let f = &docs[0].fields;
        assert_eq!(f["phone"], json!("+33612345678"), "a leading + is kept");
        assert_eq!(
            f["order_ref"],
            json!("123456789012345678901234"),
            "an integer too wide for 64 bits is not rounded through a float"
        );
        assert_eq!(f["neg_ref"], json!("-123456789012345678901234"));
        assert_eq!(f["sku"], json!("1E5"), "exponent notation is not expanded");
        assert_eq!(f["small_e"], json!("2e3"));
    }

    #[test]
    fn typed_cells_otherwise_parse_as_before() {
        assert_eq!(typed_cell("42"), json!(42));
        assert_eq!(typed_cell("-42"), json!(-42));
        assert_eq!(typed_cell("18446744073709551615"), json!(u64::MAX));
        assert_eq!(typed_cell("9.5"), json!(9.5));
        assert_eq!(typed_cell("-0.25"), json!(-0.25));
        assert_eq!(typed_cell("1.50"), json!(1.5));
        assert_eq!(typed_cell(".5"), json!(0.5));
        assert_eq!(typed_cell("0"), json!(0));
        assert_eq!(typed_cell("0.5"), json!(0.5));
        assert_eq!(typed_cell("007"), json!("007"));
        assert_eq!(typed_cell("TRUE"), json!(true));
        assert_eq!(typed_cell("false"), json!(false));
        assert_eq!(typed_cell("inf"), json!("inf"));
        assert_eq!(typed_cell("NaN"), json!("NaN"));
        assert_eq!(typed_cell("+"), json!("+"));
        assert_eq!(typed_cell("-"), json!("-"));
    }

    async fn run_raw(blob: Blob, cfg: Value) -> Result<Vec<Document>, PluginError> {
        match CsvParserPlugin::new()
            .execute(&ActivityContext::noop(), PluginInput::Bytes(blob), cfg)
            .await?
        {
            PluginOutput::Documents(d) => Ok(d),
            other => panic!("expected documents, got {other:?}"),
        }
    }

    fn raw(data: &[u8]) -> Blob {
        Blob::new(data.to_vec(), "text/csv", None)
    }

    #[tokio::test]
    async fn a_legacy_encoded_file_is_transcoded_instead_of_failing() {
        // Excel on Windows: windows-1252, `é` is the single byte 0xE9.
        let docs = run_raw(raw(b"name,city\nSoci\xE9t\xE9,Paris\n"), json!({}))
            .await
            .unwrap();
        assert_eq!(docs[0].fields["name"], json!("Société"));
        // In a header too: configured columns still resolve.
        let docs = run_raw(
            raw(b"soci\xE9t\xE9,v\nx,1\n"),
            json!({"id_column": "société", "content_columns": ["société"]}),
        )
        .await
        .unwrap();
        assert_eq!(docs[0].fields["société"], json!("x"));
        assert_eq!(docs[0].id, "x");
    }

    #[tokio::test]
    async fn the_declared_charset_and_byte_order_marks_are_honoured() {
        // Shift_JIS for `名前` (name) / `東京` (Tokyo), declared by the server.
        let (sjis, _, _) = encoding_rs::SHIFT_JIS.encode("名前,市\n東京,1\n");
        let docs = run_raw(raw(&sjis).with_charset(Some("Shift_JIS".into())), json!({}))
            .await
            .unwrap();
        assert_eq!(docs[0].fields["名前"], json!("東京"));

        // Excel's "Unicode Text": UTF-16LE with a byte order mark, tab-separated.
        let mut utf16 = vec![0xFF, 0xFE];
        for unit in "name\tcity\nZoë\tKöln\n".encode_utf16() {
            utf16.extend(unit.to_le_bytes());
        }
        let docs = run_raw(raw(&utf16), json!({})).await.unwrap();
        assert_eq!(docs[0].fields["name"], json!("Zoë"));
        assert_eq!(docs[0].fields["city"], json!("Köln"));

        // A UTF-8 byte order mark is not part of the first header.
        let docs = run_raw(raw("\u{FEFF}name\nAda\n".as_bytes()), json!({}))
            .await
            .unwrap();
        assert_eq!(docs[0].fields["name"], json!("Ada"));

        // Valid UTF-8 wins over a declaration it contradicts.
        let docs = run_raw(
            raw("name\nSociété\n".as_bytes()).with_charset(Some("windows-1252".into())),
            json!({}),
        )
        .await
        .unwrap();
        assert_eq!(docs[0].fields["name"], json!("Société"));
    }

    #[tokio::test]
    async fn the_encoding_option_forces_one() {
        // 0xA4 is `€` in ISO-8859-15 and `¤` in windows-1252.
        let data = b"price\n10\xA4\n";
        let docs = run_raw(raw(data), json!({"encoding": "iso-8859-15"}))
            .await
            .unwrap();
        assert_eq!(docs[0].fields["price"], json!("10€"));
        let docs = run_raw(raw(data), json!({})).await.unwrap();
        assert_eq!(
            docs[0].fields["price"],
            json!("10¤"),
            "windows-1252 fallback"
        );
        // Forcing UTF-8 on a stray byte replaces it rather than failing the file.
        let docs = run_raw(raw(data), json!({"encoding": "utf-8"}))
            .await
            .unwrap();
        assert_eq!(docs[0].fields["price"], json!("10\u{FFFD}"));

        let err = run_raw(raw(data), json!({"encoding": "klingon"}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");
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
