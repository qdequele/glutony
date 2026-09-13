//! # `avro_parser` — Avro object container files to flat documents
//!
//! Built-in `meili-ingest` plugin for Avro OCF payloads. An OCF carries its own
//! writer schema in the header, so nothing has to be configured to read one. Each
//! record becomes a [`Document`] whose `fields` are flat dotted keys, produced by
//! the shared rules of [`json_flattener`](meili_ingest_plugin_json).
//!
//! Logical types are resolved against the writer schema rather than leaking as raw
//! integers or byte arrays:
//!
//! | Avro | JSON |
//! |------|------|
//! | `null` branch of a union | the branch that was actually written, unwrapped |
//! | `bytes`, `fixed` | base64 string |
//! | `enum` | the symbol |
//! | `date` | `YYYY-MM-DD` |
//! | `time-millis`, `time-micros` | `HH:MM:SS.fff` |
//! | `timestamp-{millis,micros,nanos}` | RFC 3339 (UTC) |
//! | `local-timestamp-*` | `YYYY-MM-DDTHH:MM:SS.fff` (no offset) |
//! | `decimal`, `big-decimal` | string, scaled — a string so no precision is lost |
//! | `duration` | `{"months": m, "days": d, "millis": ms}` |
//! | `uuid` | string |
//!
//! Unions are unwrapped to the branch that was written, so a `["null","string"]`
//! field is just a string (or absent-valued `null`) rather than a tagged object.
//!
//! Configuration (all keys optional):
//!
//! | key              | default | meaning |
//! |------------------|---------|---------|
//! | `id_field`       | `id`    | flattened key holding the document id; generated as `<stem>_r<n>` when absent |
//! | `content_fields` | all string leaves | flattened keys joined by a space into `content` (removed from `fields`) |
//! | `title_field`    | none    | flattened key mapped onto `title` (removed from `fields`) |
//! | `flatten_arrays` | `false` | flatten arrays of objects into `a.0.b` keys instead of keeping them as JSON arrays |
//! | `max_depth`      | `8`     | nesting depth beyond which objects are kept as-is |
//! | `max_records`    | unlimited | stop after this many records |

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use apache_avro::schema::{Schema, UnionSchema};
use apache_avro::types::Value as Av;
use apache_avro::{Decimal as AvDecimal, Reader};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, NaiveTime};
use meili_ingest_plugin_json::{FlattenConfig, documents_from_values, id_stem};
use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "avro_parser";

/// Seconds in a day, for turning an Avro `date` into a calendar date.
const SECS_PER_DAY: i64 = 86_400;

/// The Avro parser plugin. Stateless; construct with [`AvroParserPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct AvroParserPlugin;

impl AvroParserPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// Step configuration. Mirrors `json_flattener` plus `max_records`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct Config {
    id_field: String,
    content_fields: Option<Vec<String>>,
    title_field: Option<String>,
    flatten_arrays: bool,
    max_depth: usize,
    max_records: Option<usize>,
}

impl Default for Config {
    fn default() -> Self {
        let f = FlattenConfig::default();
        Self {
            id_field: f.id_field,
            content_fields: f.content_fields,
            title_field: f.title_field,
            flatten_arrays: f.flatten_arrays,
            max_depth: f.max_depth,
            max_records: None,
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
        if cfg.max_records == Some(0) {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `max_records` must be greater than zero when set"
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

// ---------------------------------------------------------------------------
// Decimals
// ---------------------------------------------------------------------------

/// Render an unscaled integer with `scale` decimal places.
fn format_scaled(unscaled: i128, scale: usize) -> String {
    if scale == 0 {
        return unscaled.to_string();
    }
    let digits = unscaled.unsigned_abs().to_string();
    let body = if digits.len() <= scale {
        format!("0.{}{digits}", "0".repeat(scale - digits.len()))
    } else {
        let (int, frac) = digits.split_at(digits.len() - scale);
        format!("{int}.{frac}")
    };
    if unscaled < 0 {
        format!("-{body}")
    } else {
        body
    }
}

/// Avro stores a decimal as big-endian two's-complement bytes; the scale lives in
/// the schema, which is why [`convert`] threads the schema through. Values wider
/// than 128 bits are out of range and fall back to base64.
fn decimal_to_json(decimal: &AvDecimal, scale: usize) -> Value {
    let Ok(bytes) = Vec::<u8>::try_from(decimal) else {
        return Value::Null;
    };
    if bytes.is_empty() || bytes.len() > 16 {
        return Value::String(BASE64.encode(&bytes));
    }
    let negative = bytes[0] & 0x80 != 0;
    let mut buf = if negative { [0xFF; 16] } else { [0x00; 16] };
    buf[16 - bytes.len()..].copy_from_slice(&bytes);
    Value::String(format_scaled(i128::from_be_bytes(buf), scale))
}

// ---------------------------------------------------------------------------
// Schema-guided conversion
// ---------------------------------------------------------------------------

/// The schema a union branch was written with, when it can be resolved.
fn union_branch(union: Option<&UnionSchema>, index: u32) -> Option<&Schema> {
    union?.variants().get(index as usize)
}

/// `scale` of a decimal schema, defaulting to 0 when the schema is unavailable.
fn decimal_scale(schema: Option<&Schema>) -> usize {
    match schema {
        Some(Schema::Decimal(d)) => d.scale,
        _ => 0,
    }
}

fn as_union(schema: Option<&Schema>) -> Option<&UnionSchema> {
    match schema {
        Some(Schema::Union(u)) => Some(u),
        _ => None,
    }
}

fn time_string(secs: u32, nanos: u32) -> Value {
    NaiveTime::from_num_seconds_from_midnight_opt(secs, nanos)
        .map_or(Value::Null, |t| Value::String(t.to_string()))
}

/// Convert an Avro value to JSON, using `schema` (when it lines up) only to
/// resolve information the datum does not carry: decimal scale and union branches.
/// A schema that does not line up is ignored rather than fatal.
fn convert(value: Av, schema: Option<&Schema>) -> Value {
    match value {
        Av::Null => Value::Null,
        Av::Boolean(b) => Value::Bool(b),
        Av::Int(i) => Value::from(i),
        Av::Long(i) => Value::from(i),
        Av::Float(f) => serde_json::Number::from_f64(f as f64).map_or(Value::Null, Value::Number),
        Av::Double(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        Av::Bytes(b) | Av::Fixed(_, b) => Value::String(BASE64.encode(b)),
        Av::String(s) => Value::String(s),
        Av::Enum(_, symbol) => Value::String(symbol),
        Av::Union(idx, inner) => convert(*inner, union_branch(as_union(schema), idx)),
        Av::Array(items) => {
            let items_schema = match schema {
                Some(Schema::Array(a)) => Some(a.items.as_ref()),
                _ => None,
            };
            Value::Array(
                items
                    .into_iter()
                    .map(|v| convert(v, items_schema))
                    .collect(),
            )
        }
        Av::Map(entries) => {
            let values_schema = match schema {
                Some(Schema::Map(m)) => Some(m.types.as_ref()),
                _ => None,
            };
            let mut out = Map::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(k, convert(v, values_schema));
            }
            Value::Object(out)
        }
        Av::Record(fields) => {
            let record = match schema {
                Some(Schema::Record(r)) => Some(r),
                _ => None,
            };
            let mut out = Map::with_capacity(fields.len());
            for (name, v) in fields {
                let field_schema = record.and_then(|r| {
                    r.lookup
                        .get(&name)
                        .and_then(|i| r.fields.get(*i))
                        .map(|f| &f.schema)
                });
                out.insert(name, convert(v, field_schema));
            }
            Value::Object(out)
        }
        Av::Date(days) => DateTime::from_timestamp(days as i64 * SECS_PER_DAY, 0)
            .map_or(Value::Null, |dt| Value::String(dt.date_naive().to_string())),
        Av::Decimal(d) => decimal_to_json(&d, decimal_scale(schema)),
        Av::BigDecimal(d) => Value::String(d.to_string()),
        Av::TimeMillis(ms) => time_string((ms as u32) / 1_000, ((ms as u32) % 1_000) * 1_000_000),
        Av::TimeMicros(us) => {
            time_string((us / 1_000_000) as u32, ((us % 1_000_000) * 1_000) as u32)
        }
        Av::TimestampMillis(ms) => DateTime::from_timestamp_millis(ms)
            .map_or(Value::Null, |dt| Value::String(dt.to_rfc3339())),
        Av::TimestampMicros(us) => DateTime::from_timestamp_micros(us)
            .map_or(Value::Null, |dt| Value::String(dt.to_rfc3339())),
        Av::TimestampNanos(ns) => Value::String(DateTime::from_timestamp_nanos(ns).to_rfc3339()),
        // "Local" timestamps carry no zone, so they are rendered without an offset.
        Av::LocalTimestampMillis(ms) => DateTime::from_timestamp_millis(ms)
            .map_or(Value::Null, |dt| Value::String(dt.naive_utc().to_string())),
        Av::LocalTimestampMicros(us) => DateTime::from_timestamp_micros(us)
            .map_or(Value::Null, |dt| Value::String(dt.naive_utc().to_string())),
        Av::LocalTimestampNanos(ns) => {
            Value::String(DateTime::from_timestamp_nanos(ns).naive_utc().to_string())
        }
        Av::Duration(d) => {
            let months: u32 = d.months().into();
            let days: u32 = d.days().into();
            let millis: u32 = d.millis().into();
            serde_json::json!({"months": months, "days": days, "millis": millis})
        }
        Av::Uuid(u) => Value::String(u.to_string()),
    }
}

/// Read every record of an OCF payload, honouring `max_records`.
fn decode_payload(
    data: &[u8],
    max_records: Option<usize>,
    cancelled: &Arc<AtomicBool>,
) -> Result<Vec<Value>, PluginError> {
    let reader = Reader::new(data).map_err(|e| {
        PluginError::InvalidInput(format!("{NAME}: not an Avro container file: {e}"))
    })?;
    // Cloned because the reader borrows itself mutably while iterating.
    let schema = reader.writer_schema().clone();

    let mut out = Vec::new();
    for (i, record) in reader.enumerate() {
        if max_records.is_some_and(|max| out.len() >= max) {
            break;
        }
        if i.is_multiple_of(256) && cancelled.load(Ordering::Relaxed) {
            return Err(PluginError::Cancelled);
        }
        let record = record.map_err(|e| {
            PluginError::InvalidInput(format!("{NAME}: invalid Avro record {}: {e}", i + 1))
        })?;
        out.push(convert(record, Some(&schema)));
    }
    Ok(out)
}

#[async_trait]
impl Plugin for AvroParserPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Parses Avro object container files into one document per record, resolving \
                 logical types (dates, timestamps, decimals, uuid) against the writer schema.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["application/vnd.apache.avro", "avro/binary"])
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
                    "max_records": {
                        "type": ["integer", "null"],
                        "minimum": 1,
                        "default": null,
                        "description": "Stop after this many records."
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
        let max_records = cfg.max_records;
        let cancelled = ctx.cancellation_flag();
        let roots = run_blocking(move || decode_payload(&data, max_records, &cancelled)).await??;

        let stem = id_stem(blob.filename.as_deref(), "avro");
        let meta = DocumentMeta {
            source: blob.filename.clone(),
            filename: blob.filename.clone(),
            mime: Some(blob.mime.clone()),
            ..Default::default()
        };
        let items = roots
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
    use apache_avro::types::Record;
    use apache_avro::{Decimal, Writer};
    use serde_json::json;

    /// Build an object container file from a schema and a set of field/value rows.
    fn ocf(schema_json: &str, rows: Vec<Vec<(&str, Av)>>) -> Vec<u8> {
        let schema = Schema::parse_str(schema_json).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new()).expect("writer");
        for row in rows {
            let mut record = Record::new(writer.schema()).expect("record");
            for (field, value) in row {
                record.put(field, value);
            }
            writer.append_value(record).expect("append");
        }
        writer.into_inner().expect("finish")
    }

    fn bytes(data: Vec<u8>, filename: Option<&str>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            data,
            "application/vnd.apache.avro",
            filename.map(str::to_owned),
        ))
    }

    async fn run(data: Vec<u8>, filename: Option<&str>, cfg: Value) -> Vec<Document> {
        match AvroParserPlugin::new()
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
        let m = AvroParserPlugin.manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Bytes]);
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(
            m.content_types
                .contains(&"application/vnd.apache.avro".to_string())
        );
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in [
            "id_field",
            "content_fields",
            "title_field",
            "flatten_arrays",
            "max_depth",
            "max_records",
        ] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
    }

    #[test]
    fn scaled_decimals_round_trip_through_their_schema() {
        assert_eq!(format_scaled(280, 2), "2.80");
        assert_eq!(format_scaled(-280, 2), "-2.80");
        assert_eq!(format_scaled(5, 3), "0.005");
        assert_eq!(format_scaled(-5, 3), "-0.005");
        assert_eq!(format_scaled(1234, 0), "1234");
    }

    #[tokio::test]
    async fn records_become_documents_with_unions_unwrapped() {
        let schema = r#"{
            "type": "record", "name": "User",
            "fields": [
                {"name": "id", "type": "long"},
                {"name": "name", "type": "string"},
                {"name": "nick", "type": ["null", "string"]},
                {"name": "score", "type": "double"},
                {"name": "active", "type": "boolean"}
            ]
        }"#;
        let data = ocf(
            schema,
            vec![
                vec![
                    ("id", Av::Long(7)),
                    ("name", Av::String("Alice".into())),
                    ("nick", Av::Union(1, Box::new(Av::String("ally".into())))),
                    ("score", Av::Double(9.5)),
                    ("active", Av::Boolean(true)),
                ],
                vec![
                    ("id", Av::Long(8)),
                    ("name", Av::String("Bob".into())),
                    ("nick", Av::Union(0, Box::new(Av::Null))),
                    ("score", Av::Double(7.0)),
                    ("active", Av::Boolean(false)),
                ],
            ],
        );
        let docs = run(data, Some("users.avro"), json!({})).await;
        assert_eq!(docs.len(), 2);
        let a = &docs[0];
        assert_eq!(a.id, "7");
        assert_eq!(a.fields["name"], json!("Alice"));
        assert_eq!(
            a.fields["nick"],
            json!("ally"),
            "a union is unwrapped to the branch written, not tagged"
        );
        assert_eq!(a.fields["score"], json!(9.5));
        assert_eq!(a.fields["active"], json!(true));
        assert_eq!(a.meta.filename.as_deref(), Some("users.avro"));
        assert_eq!(
            docs[1].fields["nick"],
            json!(null),
            "the null branch stays null"
        );
    }

    #[tokio::test]
    async fn logical_types_are_resolved_against_the_writer_schema() {
        let schema = r#"{
            "type": "record", "name": "Event",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                {"name": "at", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "clock", "type": {"type": "int", "logicalType": "time-millis"}},
                {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "kind", "type": {"type": "enum", "name": "Kind", "symbols": ["A", "B"]}},
                {"name": "raw", "type": "bytes"}
            ]
        }"#;
        let data = ocf(
            schema,
            vec![vec![
                ("id", Av::String("e1".into())),
                ("day", Av::Date(19_000)),
                ("at", Av::TimestampMillis(1_700_000_000_000)),
                ("clock", Av::TimeMillis(3_723_000)),
                ("price", Av::Decimal(Decimal::from(vec![1, 24]))),
                ("kind", Av::Enum(1, "B".into())),
                ("raw", Av::Bytes(vec![1, 2, 3])),
            ]],
        );
        let d = &run(data, None, json!({})).await[0];
        assert_eq!(d.fields["day"], json!("2022-01-08"));
        assert_eq!(d.fields["at"], json!("2023-11-14T22:13:20+00:00"));
        assert_eq!(d.fields["clock"], json!("01:02:03"));
        assert_eq!(
            d.fields["price"],
            json!("2.80"),
            "scale comes from the schema, not the datum"
        );
        assert_eq!(d.fields["kind"], json!("B"), "an enum is its symbol");
        assert_eq!(d.fields["raw"], json!("AQID"), "bytes are base64");
    }

    #[tokio::test]
    async fn nested_records_flatten_to_dotted_keys() {
        let schema = r#"{
            "type": "record", "name": "Doc",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "address", "type": {
                    "type": "record", "name": "Address",
                    "fields": [
                        {"name": "city", "type": "string"},
                        {"name": "zip", "type": "string"}
                    ]
                }},
                {"name": "tags", "type": {"type": "array", "items": "string"}}
            ]
        }"#;
        let data = ocf(
            schema,
            vec![vec![
                ("id", Av::String("d1".into())),
                (
                    "address",
                    Av::Record(vec![
                        ("city".into(), Av::String("Paris".into())),
                        ("zip".into(), Av::String("75001".into())),
                    ]),
                ),
                (
                    "tags",
                    Av::Array(vec![Av::String("a".into()), Av::String("b".into())]),
                ),
            ]],
        );
        let d = &run(data, None, json!({})).await[0];
        assert_eq!(d.id, "d1");
        assert_eq!(d.fields["address.city"], json!("Paris"));
        assert_eq!(d.fields["address.zip"], json!("75001"));
        assert_eq!(
            d.fields["tags"],
            json!(["a", "b"]),
            "scalar arrays stay arrays"
        );
    }

    #[tokio::test]
    async fn max_records_and_generated_ids() {
        let schema = r#"{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}"#;
        let rows: Vec<Vec<(&str, Av)>> = (0..5)
            .map(|i| vec![("v", Av::String(format!("v{i}")))])
            .collect();
        let data = ocf(schema, rows);

        let docs = run(data.clone(), Some("rows.avro"), json!({})).await;
        assert_eq!(docs.len(), 5);
        assert_eq!(docs[0].id, "rows_r1", "generated ids are 1-based");
        assert_eq!(docs[4].id, "rows_r5");

        let docs = run(data, None, json!({"max_records": 2})).await;
        assert_eq!(docs.len(), 2);
    }

    #[tokio::test]
    async fn invalid_input_and_config_are_rejected() {
        let plugin = AvroParserPlugin::new();
        let ctx = ActivityContext::noop();
        let schema = r#"{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}"#;
        let good = ocf(schema, vec![vec![("v", Av::String("x".into()))]]);

        for bad in [Vec::new(), b"not avro at all".to_vec()] {
            let err = plugin
                .execute(&ctx, bytes(bad, None), json!({}))
                .await
                .unwrap_err();
            assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");
        }

        for bad in [
            json!({"bogus": true}),
            json!({"max_records": 0}),
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
        let schema = r#"{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}"#;
        let data = ocf(schema, vec![vec![("v", Av::String("x".into()))]]);
        let ctx = ActivityContext::noop();
        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = AvroParserPlugin::new()
            .execute(&ctx, bytes(data, None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled), "{err}");
    }
}
