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
//! | `max_records`    | none    | stop after this many records (truncates) |
//!
//! Whatever the step says, the worker's [`DecodeLimits`] (`PARSER_MAX_ROWS`,
//! `PARSER_MAX_DECODED_MB`) bound what one file may decode: a file over either fails
//! the step as invalid input instead of being truncated. Each record is measured
//! from its encoded bytes against the writer schema before it is decoded, so the
//! step stops at the record that goes over without materialising it.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use std::str::FromStr;

use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::schema::{
    InnerDecimalSchema, NamesRef, NamespaceRef, ResolvedSchema, Schema, UnionSchema, UuidSchema,
};
use apache_avro::types::Value as Av;
use apache_avro::{Codec, Decimal as AvDecimal, Reader};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::{DateTime, NaiveTime};
use meili_ingest_plugin_json::limits::NODE_BYTES;
use meili_ingest_plugin_json::{
    DecodeBudget, DecodeLimits, FlattenConfig, documents_from_values, id_stem,
};
use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "avro_parser";

/// Seconds in a day, for turning an Avro `date` into a calendar date.
const SECS_PER_DAY: i64 = 86_400;

/// The Avro parser plugin. Construct with [`AvroParserPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct AvroParserPlugin {
    limits: DecodeLimits,
}

impl AvroParserPlugin {
    /// Create the plugin with the default [`DecodeLimits`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Create the plugin with the worker's [`DecodeLimits`].
    pub fn with_limits(limits: DecodeLimits) -> Self {
        Self { limits }
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
///
/// A named type used a second time appears in the writer schema as a
/// [`Schema::Ref`]; it is looked up in `names` (keyed by fully-qualified name,
/// qualified against `namespace`, the enclosing one) one level per call, so a
/// recursive type cannot loop. An unresolvable reference is ignored like any
/// other schema that does not line up.
fn convert(value: Av, schema: Option<&Schema>, names: &NamesRef, namespace: NamespaceRef) -> Value {
    if let Some(Schema::Ref { name }) = schema {
        let qualified = name.fully_qualified_name(namespace);
        let resolved = names.get(qualified.as_ref()).copied();
        return convert(value, resolved, names, qualified.namespace());
    }
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
        Av::Union(idx, inner) => convert(
            *inner,
            union_branch(as_union(schema), idx),
            names,
            namespace,
        ),
        Av::Array(items) => {
            let items_schema = match schema {
                Some(Schema::Array(a)) => Some(a.items.as_ref()),
                _ => None,
            };
            Value::Array(
                items
                    .into_iter()
                    .map(|v| convert(v, items_schema, names, namespace))
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
                out.insert(k, convert(v, values_schema, names, namespace));
            }
            Value::Object(out)
        }
        Av::Record(fields) => {
            let record = match schema {
                Some(Schema::Record(r)) => Some(r),
                _ => None,
            };
            // Field types are named relative to the record's own namespace.
            let record_name = record.map(|r| r.name.fully_qualified_name(namespace));
            let field_namespace = record_name.as_ref().map_or(namespace, |n| n.namespace());
            let mut out = Map::with_capacity(fields.len());
            for (name, v) in fields {
                let field_schema = record.and_then(|r| {
                    r.lookup
                        .get(&name)
                        .and_then(|i| r.fields.get(*i))
                        .map(|f| &f.schema)
                });
                out.insert(name, convert(v, field_schema, names, field_namespace));
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

// ---------------------------------------------------------------------------
// Measuring a datum before it is decoded
// ---------------------------------------------------------------------------

/// Value nodes walked between two cancellation checks while measuring.
const CANCEL_EVERY_NODES: u64 = 1 << 16;

/// Text allowed for a date, time or timestamp rendered as a string. The longest,
/// an RFC 3339 timestamp with a six-digit year and nanoseconds, is under 40.
const TEMPORAL_TEXT_BYTES: u64 = 64;

/// Text of a UUID rendered as a string.
const UUID_TEXT_BYTES: u64 = 36;

/// Memory an object key takes before its text, as
/// [`decoded_size`](meili_ingest_plugin_json::decoded_size) counts it.
const KEY_BYTES: u64 = std::mem::size_of::<String>() as u64;

/// Base64 length of `n` bytes.
fn base64_len(n: u64) -> u64 {
    n.div_ceil(3) * 4
}

/// Walks one datum's encoded bytes against the writer schema, the way
/// apache-avro's decoder reads them, without allocating anything, and adds up an
/// upper bound on the [`decoded_size`](meili_ingest_plugin_json::decoded_size) of
/// the JSON value [`convert`] makes of it.
/// apache-avro bounds each collection, not the record, so a few bytes can claim
/// millions of values (a `null` takes no bytes at all): every node is charged as
/// it is walked and the walk fails as soon as the datum cannot fit in `budget`,
/// before any of it is decoded.
struct Measure<'a> {
    names: &'a NamesRef<'a>,
    budget: &'a DecodeBudget,
    cancelled: &'a AtomicBool,
    /// 1-based index of the record, for the error messages.
    record: usize,
    nodes: u64,
    size: u64,
}

/// The error for record `record` (1-based) that cannot be read.
fn invalid_record(record: usize, why: &dyn std::fmt::Display) -> PluginError {
    PluginError::InvalidInput(format!("{NAME}: invalid Avro record {record}: {why}"))
}

impl Measure<'_> {
    fn invalid(&self, why: &str) -> PluginError {
        invalid_record(self.record, &why)
    }

    /// Charge one value node and `text` bytes of it, failing past the budget.
    fn charge(&mut self, text: u64) -> Result<(), PluginError> {
        self.nodes += 1;
        if self.nodes.is_multiple_of(CANCEL_EVERY_NODES) && self.cancelled.load(Ordering::Relaxed) {
            return Err(PluginError::Cancelled);
        }
        self.size = self.size.saturating_add(NODE_BYTES).saturating_add(text);
        self.budget.check_bytes(self.size)
    }

    /// Charge an object key of `len` bytes.
    fn charge_key(&mut self, len: u64) -> Result<(), PluginError> {
        self.size = self.size.saturating_add(KEY_BYTES).saturating_add(len);
        self.budget.check_bytes(self.size)
    }

    fn take<'d>(&self, data: &mut &'d [u8], n: usize) -> Result<&'d [u8], PluginError> {
        let bytes: &'d [u8] = data;
        let (head, rest) = bytes
            .split_at_checked(n)
            .ok_or_else(|| self.invalid("the record is truncated"))?;
        *data = rest;
        Ok(head)
    }

    /// A zigzag varint `long`, at most ten bytes as apache-avro reads it.
    fn long(&self, data: &mut &[u8]) -> Result<i64, PluginError> {
        read_long(data).ok_or_else(|| self.invalid("bad or truncated integer"))
    }

    /// A non-negative length.
    fn len(&self, data: &mut &[u8]) -> Result<usize, PluginError> {
        usize::try_from(self.long(data)?).map_err(|_| self.invalid("negative length"))
    }

    /// The `bytes`/`string` payload: its length, then that many bytes.
    fn bytes(&self, data: &mut &[u8]) -> Result<u64, PluginError> {
        let n = self.len(data)?;
        self.take(data, n)?;
        Ok(n as u64)
    }

    /// The items of an array or map, block by block: a count, then (when the count
    /// is negative) the block's size in bytes, then the items; a zero count ends it.
    fn blocks(
        &mut self,
        data: &mut &[u8],
        mut item: impl FnMut(&mut Self, &mut &[u8]) -> Result<(), PluginError>,
    ) -> Result<(), PluginError> {
        loop {
            let count = self.long(data)?;
            if count == 0 {
                return Ok(());
            }
            if count < 0 {
                self.long(data)?;
            }
            for _ in 0..count.unsigned_abs() {
                item(self, data)?;
            }
        }
    }

    fn walk(
        &mut self,
        schema: &Schema,
        namespace: NamespaceRef,
        data: &mut &[u8],
    ) -> Result<(), PluginError> {
        match schema {
            Schema::Null => self.charge(0),
            Schema::Boolean => match self.take(data, 1)?[0] {
                0 | 1 => self.charge(0),
                _ => Err(self.invalid("bad boolean")),
            },
            Schema::Int | Schema::Long => {
                self.long(data)?;
                self.charge(0)
            }
            Schema::Float => {
                self.take(data, 4)?;
                self.charge(0)
            }
            Schema::Double => {
                self.take(data, 8)?;
                self.charge(0)
            }
            Schema::Bytes => {
                let n = self.bytes(data)?;
                self.charge(base64_len(n))
            }
            Schema::String => {
                let n = self.bytes(data)?;
                self.charge(n)
            }
            Schema::Fixed(fixed) => {
                self.take(data, fixed.size)?;
                self.charge(base64_len(fixed.size as u64))
            }
            Schema::Enum(e) => {
                let symbol = usize::try_from(self.long(data)?)
                    .ok()
                    .and_then(|i| e.symbols.get(i))
                    .ok_or_else(|| self.invalid("enum index out of range"))?;
                self.charge(symbol.len() as u64)
            }
            Schema::Decimal(d) => {
                let n = match &d.inner {
                    InnerDecimalSchema::Bytes => self.bytes(data)?,
                    InnerDecimalSchema::Fixed(fixed) => {
                        self.take(data, fixed.size)?;
                        fixed.size as u64
                    }
                };
                // Base64 past 16 bytes; otherwise at most 39 digits, a sign, a dot
                // and the zeros the scale pads with.
                self.charge(base64_len(n).max(42 + d.scale as u64))
            }
            Schema::BigDecimal => {
                // The unscaled integer has under 3 digits per byte; the formatter
                // pads with at most a few dozen zeros, or writes an exponent.
                let n = self.bytes(data)?;
                self.charge(3 * n + 48)
            }
            Schema::Uuid(uuid) => {
                match uuid {
                    UuidSchema::String | UuidSchema::Bytes => {
                        self.bytes(data)?;
                    }
                    UuidSchema::Fixed(fixed) => {
                        self.take(data, fixed.size)?;
                    }
                }
                self.charge(UUID_TEXT_BYTES)
            }
            Schema::Date | Schema::TimeMillis => {
                self.long(data)?;
                self.charge(TEMPORAL_TEXT_BYTES)
            }
            Schema::TimeMicros
            | Schema::TimestampMillis
            | Schema::TimestampMicros
            | Schema::TimestampNanos
            | Schema::LocalTimestampMillis
            | Schema::LocalTimestampMicros
            | Schema::LocalTimestampNanos => {
                self.long(data)?;
                self.charge(TEMPORAL_TEXT_BYTES)
            }
            Schema::Duration(fixed) => {
                self.take(data, fixed.size)?;
                // {"months": m, "days": d, "millis": ms}
                self.charge(0)?;
                for key in ["months", "days", "millis"] {
                    self.charge_key(key.len() as u64)?;
                    self.charge(0)?;
                }
                Ok(())
            }
            Schema::Array(array) => {
                self.charge(0)?;
                self.blocks(data, |m, data| m.walk(&array.items, namespace, data))
            }
            Schema::Map(map) => {
                self.charge(0)?;
                self.blocks(data, |m, data| {
                    let key = m.bytes(data)?;
                    m.charge_key(key)?;
                    m.walk(&map.types, namespace, data)
                })
            }
            Schema::Union(union) => {
                let variant = usize::try_from(self.long(data)?)
                    .ok()
                    .and_then(|i| union.variants().get(i))
                    .ok_or_else(|| self.invalid("union index out of range"))?;
                self.walk(variant, namespace, data)
            }
            Schema::Record(record) => {
                self.charge(0)?;
                let name = record.name.fully_qualified_name(namespace);
                for field in &record.fields {
                    self.charge_key(field.name.len() as u64)?;
                    self.walk(&field.schema, name.namespace(), data)?;
                }
                Ok(())
            }
            Schema::Ref { name } => {
                let qualified = name.fully_qualified_name(namespace);
                let names = self.names;
                let resolved = names
                    .get(qualified.as_ref())
                    .copied()
                    .ok_or_else(|| self.invalid(&format!("unknown type {qualified}")))?;
                self.walk(resolved, qualified.namespace(), data)
            }
        }
    }
}

/// A zigzag varint `long` at the start of `data`, consumed: `None` when the bytes
/// end first or it runs past ten bytes.
fn read_long(data: &mut &[u8]) -> Option<i64> {
    let bytes: &[u8] = data;
    let mut z = 0u64;
    for (i, &byte) in bytes.iter().take(10).enumerate() {
        z |= u64::from(byte & 0x7f) << (7 * i);
        if byte & 0x80 == 0 {
            *data = &bytes[i + 1..];
            return Some(if z & 1 == 0 {
                (z >> 1) as i64
            } else {
                !(z >> 1) as i64
            });
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Container
// ---------------------------------------------------------------------------

/// Length of the sync marker that ends the header and every block.
const SYNC_LEN: usize = 16;

fn not_avro(e: impl std::fmt::Display) -> PluginError {
    PluginError::InvalidInput(format!("{NAME}: not an Avro container file: {e}"))
}

/// The codec named by the header's `avro.codec` (`null` when absent). `header`
/// starts after the magic bytes: a `map<bytes>` of metadata, block by block.
fn header_codec(mut header: &[u8]) -> Option<Codec> {
    let data = &mut header;
    let mut codec = None;
    loop {
        let count = read_long(data)?;
        if count == 0 {
            break;
        }
        if count < 0 {
            read_long(data)?;
        }
        for _ in 0..count.unsigned_abs() {
            let mut field = || {
                let n = usize::try_from(read_long(data)?).ok()?;
                let bytes: &[u8] = data;
                let (head, rest) = bytes.split_at_checked(n)?;
                *data = rest;
                Some(head)
            };
            let (key, value) = (field()?, field()?);
            if key == b"avro.codec" {
                codec = Some(value);
            }
        }
    }
    match codec {
        None => Some(Codec::Null),
        Some(name) => Codec::from_str(std::str::from_utf8(name).ok()?).ok(),
    }
}

/// Read every record of an OCF payload, honouring `max_records`. Each record is
/// measured from its encoded bytes ([`Measure`]) and counted against `limits`
/// before it is decoded, so a record that cannot fit is refused without being
/// materialised.
fn decode_payload(
    data: &[u8],
    max_records: Option<usize>,
    limits: DecodeLimits,
    cancelled: &Arc<AtomicBool>,
) -> Result<Vec<Value>, PluginError> {
    // apache-avro reads and validates the header (magic, schema, codec, marker),
    // and reads nothing past it: what it leaves is the data blocks.
    let mut blocks = data;
    let reader = Reader::new(&mut blocks).map_err(not_avro)?;
    let schema = reader.writer_schema().clone();
    drop(reader);
    let header_len = data.len() - blocks.len();
    let marker = &data[header_len - SYNC_LEN..header_len];
    let codec = header_codec(&data[4..header_len - SYNC_LEN])
        .ok_or_else(|| not_avro("unreadable codec"))?;

    // A named type used a second time is a `Schema::Ref`: resolve it by name.
    let resolved = ResolvedSchema::try_from(&schema).map_err(|e| {
        PluginError::InvalidInput(format!("{NAME}: invalid Avro writer schema: {e}"))
    })?;
    let names = resolved.get_names();
    let datum = GenericDatumReader::builder(&schema).build().map_err(|e| {
        PluginError::InvalidInput(format!("{NAME}: invalid Avro writer schema: {e}"))
    })?;

    let mut budget = DecodeBudget::new(limits, NAME);
    let mut out = Vec::new();
    let bad_block = |why: &dyn std::fmt::Display| {
        PluginError::InvalidInput(format!("{NAME}: invalid Avro block: {why}"))
    };
    // As apache-avro does, the data ends where the next block's count cannot be read.
    while let Some(count) = read_long(&mut blocks) {
        let count = u64::try_from(count).map_err(|_| bad_block(&"negative record count"))?;
        let size = read_long(&mut blocks)
            .and_then(|n| usize::try_from(n).ok())
            .ok_or_else(|| bad_block(&"bad block size"))?;
        let (body, rest) = blocks
            .split_at_checked(size)
            .ok_or_else(|| bad_block(&"the block is truncated"))?;
        let (sync, rest) = rest
            .split_at_checked(SYNC_LEN)
            .ok_or_else(|| bad_block(&"the block is truncated"))?;
        if sync != marker {
            return Err(bad_block(&"sync marker does not match the header"));
        }
        blocks = rest;
        let body: std::borrow::Cow<[u8]> = match codec {
            Codec::Null => body.into(),
            codec => {
                let mut buf = body.to_vec();
                codec.decompress(&mut buf).map_err(|e| bad_block(&e))?;
                buf.into()
            }
        };

        let mut records: &[u8] = &body;
        for _ in 0..count {
            if max_records.is_some_and(|max| out.len() >= max) {
                return Ok(out);
            }
            if out.len().is_multiple_of(256) && cancelled.load(Ordering::Relaxed) {
                return Err(PluginError::Cancelled);
            }
            let record = out.len() + 1;
            budget.add_rows(1)?;
            let mut walked = records;
            let mut measure = Measure {
                names,
                budget: &budget,
                cancelled,
                record,
                nodes: 0,
                size: 0,
            };
            measure.walk(&schema, None, &mut walked)?;
            let size = measure.size;
            budget.add_bytes(size)?;

            let mut decoded = records;
            let value = datum
                .read_value(&mut decoded)
                .map_err(|e| invalid_record(record, &e))?;
            if decoded.len() != walked.len() {
                // The measure and the decoder read the record differently: what was
                // measured is not what was decoded.
                return Err(invalid_record(
                    record,
                    &"the record's encoding is ambiguous",
                ));
            }
            if !records.is_empty() && decoded.len() == records.len() {
                // As apache-avro does: a record that reads no byte would never end.
                return Err(invalid_record(record, &"the record takes no bytes"));
            }
            records = decoded;
            out.push(convert(value, Some(&schema), names, None));
        }
        // As apache-avro does: an empty block ends the data.
        if count == 0 {
            break;
        }
    }
    // A count that does not end within ten bytes is an error; fewer bytes that end
    // mid-count are where apache-avro stops reading.
    if blocks.len() >= 10 && read_long(&mut blocks).is_none() {
        return Err(bad_block(&"bad record count"));
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
                        "description": "Stop after this many records. The worker's PARSER_MAX_ROWS still applies."
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
        let mut blob = input.into_bytes()?;
        ctx.check_cancelled()?;

        let stem = id_stem(&blob, "avro");
        let meta = DocumentMeta {
            source: blob.filename.clone(),
            filename: blob.filename.clone(),
            mime: Some(blob.mime.clone()),
            ..Default::default()
        };
        // Moved, not cloned: nothing below reads the bytes again.
        let data = std::mem::take(&mut blob.data);
        let max_records = cfg.max_records;
        let limits = self.limits;
        let ctx = ctx.clone();
        // Decoding and flattening are CPU-bound: keep them off the async runtime.
        let docs = run_blocking(move || {
            let roots = decode_payload(&data, max_records, limits, &ctx.cancellation_flag())?;
            drop(data);
            let items = roots
                .into_iter()
                .enumerate()
                .map(|(i, v)| (v, format!("{stem}_r{}", i + 1)))
                .collect();
            documents_from_values(&ctx, items, &flatten, &meta, NAME)
        })
        .await??;
        Ok(PluginOutput::Documents(docs))
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
        let m = AvroParserPlugin::new().manifest();
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

    /// A named type is defined once and then referenced by name; the decimal scale
    /// must come from the referenced definition at every use, including through a
    /// union branch and array items.
    async fn assert_named_decimal_reused(schema: &str) {
        let money = || {
            Av::Record(vec![(
                "amount".into(),
                Av::Decimal(Decimal::from(vec![1, 24])),
            )])
        };
        let data = ocf(
            schema,
            vec![vec![
                ("id", Av::String("o1".into())),
                ("price", money()),
                ("tax", money()),
                ("discount", Av::Union(1, Box::new(money()))),
                ("fees", Av::Array(vec![money(), money()])),
            ]],
        );
        let d = &run(data, None, json!({"flatten_arrays": true})).await[0];
        assert_eq!(d.fields["price.amount"], json!("2.80"), "inline definition");
        assert_eq!(d.fields["tax.amount"], json!("2.80"), "reference by name");
        assert_eq!(
            d.fields["discount.amount"],
            json!("2.80"),
            "reference through a union branch"
        );
        assert_eq!(
            d.fields["fees.0.amount"],
            json!("2.80"),
            "reference as array items"
        );
        assert_eq!(d.fields["fees.1.amount"], json!("2.80"));
    }

    #[tokio::test]
    async fn decimals_in_named_types_referenced_by_name_keep_their_scale() {
        assert_named_decimal_reused(
            r#"{
            "type": "record", "name": "Order",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "price", "type": {
                    "type": "record", "name": "Money",
                    "fields": [{"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}]
                }},
                {"name": "tax", "type": "Money"},
                {"name": "discount", "type": ["null", "Money"]},
                {"name": "fees", "type": {"type": "array", "items": "Money"}}
            ]
        }"#,
        )
        .await;
    }

    #[tokio::test]
    async fn decimals_in_namespaced_named_types_keep_their_scale() {
        assert_named_decimal_reused(
            r#"{
            "type": "record", "name": "Order", "namespace": "com.acme",
            "fields": [
                {"name": "id", "type": "string"},
                {"name": "price", "type": {
                    "type": "record", "name": "Money",
                    "fields": [{"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}}]
                }},
                {"name": "tax", "type": "Money"},
                {"name": "discount", "type": ["null", "com.acme.Money"]},
                {"name": "fees", "type": {"type": "array", "items": "Money"}}
            ]
        }"#,
        )
        .await;
    }

    #[tokio::test]
    async fn recursive_named_types_resolve_one_level_at_a_time() {
        let schema = r#"{
            "type": "record", "name": "Node",
            "fields": [
                {"name": "amount", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}},
                {"name": "next", "type": ["null", "Node"]}
            ]
        }"#;
        let amount = || Av::Decimal(Decimal::from(vec![1, 24]));
        let tail = Av::Record(vec![
            ("amount".into(), amount()),
            ("next".into(), Av::Union(0, Box::new(Av::Null))),
        ]);
        let data = ocf(
            schema,
            vec![vec![
                ("amount", amount()),
                ("next", Av::Union(1, Box::new(tail))),
            ]],
        );
        let d = &run(data, None, json!({})).await[0];
        assert_eq!(d.fields["amount"], json!("2.80"));
        assert_eq!(d.fields["next.amount"], json!("2.80"));
        assert_eq!(d.fields["next.next"], json!(null));
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
    async fn a_file_over_the_default_row_limit_fails_instead_of_filling_memory() {
        // `null` records take no bytes at all: a tiny file, a million and one rows.
        let schema = Schema::parse_str(r#""null""#).expect("schema");
        let mut writer = Writer::new(&schema, Vec::new()).expect("writer");
        for _ in 0..=meili_ingest_plugin_json::limits::DEFAULT_MAX_ROWS {
            writer.append_value(Av::Null).expect("append");
        }
        let data = writer.into_inner().expect("finish");
        assert!(data.len() < 64 * 1024, "{} bytes", data.len());
        let err = AvroParserPlugin::new()
            .execute(&ActivityContext::noop(), bytes(data, None), Value::Null)
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_ROWS")),
            "{err}"
        );
    }

    #[tokio::test]
    async fn the_worker_limits_bound_records_and_decoded_bytes() {
        let schema = r#"{"type":"record","name":"R","fields":[{"name":"v","type":"string"}]}"#;
        let rows: Vec<Vec<(&str, Av)>> = (0..5)
            .map(|i| vec![("v", Av::String(format!("v{i}")))])
            .collect();
        let data = ocf(schema, rows);
        let ctx = ActivityContext::noop();

        let rows_cap = AvroParserPlugin::with_limits(DecodeLimits {
            max_rows: 3,
            ..DecodeLimits::default()
        });
        for cfg in [json!({}), json!({"max_records": 4})] {
            let err = rows_cap
                .execute(&ctx, bytes(data.clone(), None), cfg.clone())
                .await
                .unwrap_err();
            assert!(
                matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_ROWS")),
                "{cfg}: {err}"
            );
        }
        let out = rows_cap
            .execute(&ctx, bytes(data.clone(), None), json!({"max_records": 3}))
            .await
            .unwrap();
        assert!(matches!(out, PluginOutput::Documents(d) if d.len() == 3));

        // One record whose single array holds many nulls: a few bytes on disk.
        let schema = r#"{"type":"record","name":"R","fields":[{"name":"v","type":{"type":"array","items":"null"}}]}"#;
        let data = ocf(schema, vec![vec![("v", Av::Array(vec![Av::Null; 1_000]))]]);
        let bytes_cap = AvroParserPlugin::with_limits(DecodeLimits {
            max_decoded_bytes: 500 * meili_ingest_plugin_json::limits::NODE_BYTES,
            ..DecodeLimits::default()
        });
        let err = bytes_cap
            .execute(&ctx, bytes(data, None), json!({}))
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_DECODED_MB")),
            "{err}"
        );
    }

    /// Counts the bytes each thread holds, so a test can see how much one decode
    /// allocated on its way to an error.
    mod meter {
        use std::alloc::{GlobalAlloc, Layout, System};
        use std::cell::Cell;

        pub struct Meter;

        thread_local! {
            static LIVE: Cell<isize> = const { Cell::new(0) };
            static PEAK: Cell<isize> = const { Cell::new(0) };
        }

        fn add(n: isize) {
            let _ = LIVE.try_with(|live| {
                let v = live.get() + n;
                live.set(v);
                let _ = PEAK.try_with(|peak| peak.set(peak.get().max(v)));
            });
        }

        // SAFETY: every call forwards to `System` unchanged; the counters only
        // read the layout sizes.
        unsafe impl GlobalAlloc for Meter {
            unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
                // SAFETY: same contract as the caller's.
                let p = unsafe { System.alloc(layout) };
                if !p.is_null() {
                    add(layout.size() as isize);
                }
                p
            }
            unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
                // SAFETY: same contract as the caller's.
                let p = unsafe { System.alloc_zeroed(layout) };
                if !p.is_null() {
                    add(layout.size() as isize);
                }
                p
            }
            unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
                // SAFETY: same contract as the caller's.
                unsafe { System.dealloc(ptr, layout) };
                add(-(layout.size() as isize));
            }
            unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
                // SAFETY: same contract as the caller's.
                let p = unsafe { System.realloc(ptr, layout, new_size) };
                if !p.is_null() {
                    add(new_size as isize - layout.size() as isize);
                }
                p
            }
        }

        /// Run `f` on this thread and return the most it held at once, over what
        /// was held before.
        pub fn peak_during<T>(f: impl FnOnce() -> T) -> (T, usize) {
            let base = LIVE.with(Cell::get);
            PEAK.with(|p| p.set(base));
            let out = f();
            let peak = PEAK.with(Cell::get) - base;
            (out, peak.max(0) as usize)
        }
    }

    #[global_allocator]
    static METER: meter::Meter = meter::Meter;

    /// Zigzag varint, as Avro writes an `int` or a `long`.
    fn zigzag(n: i64) -> Vec<u8> {
        let mut v = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        loop {
            if v < 0x80 {
                out.push(v as u8);
                return out;
            }
            out.push((v as u8 & 0x7f) | 0x80);
            v >>= 7;
        }
    }

    /// An uncompressed object container file holding `count` records whose
    /// encoded bytes are `records`, written by hand so that a test can state the
    /// counts a writer would refuse to build in memory.
    fn raw_ocf(schema_json: &str, count: i64, records: &[u8]) -> Vec<u8> {
        let sync = [7u8; 16];
        let mut out = b"Obj\x01".to_vec();
        out.extend(zigzag(2));
        for (key, value) in [
            ("avro.schema", schema_json.as_bytes()),
            ("avro.codec", b"null".as_slice()),
        ] {
            out.extend(zigzag(key.len() as i64));
            out.extend(key.as_bytes());
            out.extend(zigzag(value.len() as i64));
            out.extend(value);
        }
        out.extend(zigzag(0));
        out.extend(sync);
        out.extend(zigzag(count));
        out.extend(zigzag(records.len() as i64));
        out.extend(records);
        out.extend(sync);
        out
    }

    #[test]
    fn one_record_over_the_byte_limit_is_refused_before_it_is_decoded() {
        // One record of four arrays of a million nulls: a null takes no bytes, so
        // the file is a few hundred bytes, and 4M values once decoded.
        let schema = r#"{"type":"record","name":"R","fields":[
            {"name":"a","type":{"type":"array","items":"null"}},
            {"name":"b","type":{"type":"array","items":"null"}},
            {"name":"c","type":{"type":"array","items":"null"}},
            {"name":"d","type":{"type":"array","items":"null"}}
        ]}"#;
        let mut record = Vec::new();
        for _ in 0..4 {
            record.extend(zigzag(1_000_000));
            record.extend(zigzag(0));
        }
        let data = raw_ocf(schema, 1, &record);
        assert!(data.len() < 1024, "{} bytes", data.len());
        let limit = 1024 * 1024;
        let limits = DecodeLimits {
            max_decoded_bytes: limit,
            ..DecodeLimits::default()
        };
        let cancelled = Arc::new(AtomicBool::new(false));
        let (out, peak) = meter::peak_during(|| decode_payload(&data, None, limits, &cancelled));
        let err = out.expect_err("over the limit");
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_DECODED_MB")),
            "{err}"
        );
        assert!(peak < limit as usize, "held {peak} bytes before refusing");
    }

    /// A record of every type the writer schema can hold, and a value for it.
    fn every_type() -> (Schema, Av) {
        use apache_avro::{Days, Duration, Millis, Months, Uuid};
        let schema = Schema::parse_str(
            r#"{
            "type": "record", "name": "All", "namespace": "t",
            "fields": [
                {"name": "n", "type": "null"},
                {"name": "b", "type": "boolean"},
                {"name": "i", "type": "int"},
                {"name": "l", "type": "long"},
                {"name": "f", "type": "float"},
                {"name": "d", "type": "double"},
                {"name": "raw", "type": "bytes"},
                {"name": "s", "type": "string"},
                {"name": "fx", "type": {"type": "fixed", "name": "F4", "size": 4}},
                {"name": "e", "type": {"type": "enum", "name": "E", "symbols": ["A", "LONGER"]}},
                {"name": "dec", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 6}},
                {"name": "decf", "type": {"type": "fixed", "name": "DF", "size": 8, "logicalType": "decimal", "precision": 18, "scale": 4}},
                {"name": "u", "type": {"type": "string", "logicalType": "uuid"}},
                {"name": "day", "type": {"type": "int", "logicalType": "date"}},
                {"name": "tms", "type": {"type": "int", "logicalType": "time-millis"}},
                {"name": "tus", "type": {"type": "long", "logicalType": "time-micros"}},
                {"name": "ts1", "type": {"type": "long", "logicalType": "timestamp-millis"}},
                {"name": "ts2", "type": {"type": "long", "logicalType": "timestamp-micros"}},
                {"name": "ts3", "type": {"type": "long", "logicalType": "timestamp-nanos"}},
                {"name": "lt1", "type": {"type": "long", "logicalType": "local-timestamp-millis"}},
                {"name": "lt2", "type": {"type": "long", "logicalType": "local-timestamp-micros"}},
                {"name": "lt3", "type": {"type": "long", "logicalType": "local-timestamp-nanos"}},
                {"name": "dur", "type": {"type": "fixed", "name": "Dur", "size": 12, "logicalType": "duration"}},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
                {"name": "attrs", "type": {"type": "map", "values": ["null", "long"]}},
                {"name": "kids", "type": {"type": "array", "items": {
                    "type": "record", "name": "Kid",
                    "fields": [{"name": "name", "type": "string"}, {"name": "next", "type": ["null", "Kid"]}]
                }}},
                {"name": "again", "type": "t.F4"}
            ]
        }"#,
        )
        .expect("schema");
        let kid = |name: &str, next: Option<Av>| {
            Av::Record(vec![
                ("name".into(), Av::String(name.into())),
                (
                    "next".into(),
                    match next {
                        None => Av::Union(0, Box::new(Av::Null)),
                        Some(k) => Av::Union(1, Box::new(k)),
                    },
                ),
            ])
        };
        let value = Av::Record(vec![
            ("n".into(), Av::Null),
            ("b".into(), Av::Boolean(true)),
            ("i".into(), Av::Int(-7)),
            ("l".into(), Av::Long(1 << 40)),
            ("f".into(), Av::Float(1.5)),
            ("d".into(), Av::Double(-2.25)),
            ("raw".into(), Av::Bytes(vec![0, 1, 2, 3, 4])),
            ("s".into(), Av::String("héllo".into())),
            ("fx".into(), Av::Fixed(4, vec![9, 8, 7, 6])),
            ("e".into(), Av::Enum(1, "LONGER".into())),
            ("dec".into(), Av::Decimal(Decimal::from(vec![0xff, 0x01]))),
            (
                "decf".into(),
                Av::Decimal(Decimal::from(vec![0, 0, 0, 0, 0, 1, 2, 3])),
            ),
            ("u".into(), Av::Uuid(Uuid::from_u128(0x1234))),
            ("day".into(), Av::Date(19_000)),
            ("tms".into(), Av::TimeMillis(3_723_004)),
            ("tus".into(), Av::TimeMicros(3_723_000_005)),
            ("ts1".into(), Av::TimestampMillis(1_700_000_000_123)),
            ("ts2".into(), Av::TimestampMicros(-1)),
            ("ts3".into(), Av::TimestampNanos(i64::MAX)),
            ("lt1".into(), Av::LocalTimestampMillis(0)),
            (
                "lt2".into(),
                Av::LocalTimestampMicros(1_700_000_000_000_001),
            ),
            ("lt3".into(), Av::LocalTimestampNanos(i64::MIN)),
            (
                "dur".into(),
                Av::Duration(Duration::new(
                    Months::new(1),
                    Days::new(20),
                    Millis::new(300_000),
                )),
            ),
            (
                "tags".into(),
                Av::Array((0..40).map(|i| Av::String(format!("t{i}"))).collect()),
            ),
            (
                "attrs".into(),
                Av::Map(
                    (0..30)
                        .map(|i| {
                            let v = if i % 3 == 0 {
                                Av::Union(0, Box::new(Av::Null))
                            } else {
                                Av::Union(1, Box::new(Av::Long(i)))
                            };
                            (format!("k{i}"), v)
                        })
                        .collect(),
                ),
            ),
            (
                "kids".into(),
                Av::Array(vec![kid("a", Some(kid("b", None))), kid("c", None)]),
            ),
            ("again".into(), Av::Fixed(4, vec![1, 1, 1, 1])),
        ]);
        (schema, value)
    }

    #[test]
    fn every_avro_type_is_measured_above_its_decoded_size() {
        use apache_avro::writer::datum::GenericDatumWriter;
        let (schema, value) = every_type();
        let resolved = ResolvedSchema::try_from(&schema).unwrap();
        // Small array and map blocks, written with their byte size (negative count).
        let encoded = GenericDatumWriter::builder(&schema)
            .target_block_size(16)
            .build()
            .unwrap()
            .write_value_to_vec(value)
            .unwrap();
        let budget = DecodeBudget::new(DecodeLimits::default(), NAME);
        let cancelled = AtomicBool::new(false);
        let mut measure = Measure {
            names: resolved.get_names(),
            budget: &budget,
            cancelled: &cancelled,
            record: 1,
            nodes: 0,
            size: 0,
        };
        let mut walked = encoded.as_slice();
        measure.walk(&schema, None, &mut walked).unwrap();
        assert!(walked.is_empty(), "the walk reads the whole datum");

        let mut read = encoded.as_slice();
        let decoded = GenericDatumReader::builder(&schema)
            .build()
            .unwrap()
            .read_value(&mut read)
            .unwrap();
        let json = convert(decoded, Some(&schema), resolved.get_names(), None);
        let size = meili_ingest_plugin_json::decoded_size(&json);
        let bound = measure.size;
        assert!(bound >= size, "bound {bound} under the decoded {size}");
        assert!(
            bound < 4 * size,
            "bound {bound} far over the decoded {size}"
        );
    }

    #[test]
    fn compressed_files_in_several_blocks_read_as_apache_avro_reads_them() {
        let (schema, value) = every_type();
        for codec in [
            Codec::Null,
            Codec::Deflate(Default::default()),
            Codec::Snappy,
            Codec::Zstandard(Default::default()),
        ] {
            let mut writer = Writer::with_codec(&schema, Vec::new(), codec).unwrap();
            for _ in 0..3 {
                writer.append_value(value.clone()).unwrap();
                // One block per record.
                writer.flush().unwrap();
            }
            let data = writer.into_inner().unwrap();

            let reader = Reader::new(data.as_slice()).unwrap();
            let resolved = ResolvedSchema::try_from(&schema).unwrap();
            let expected: Vec<Value> = reader
                .map(|r| convert(r.unwrap(), Some(&schema), resolved.get_names(), None))
                .collect();
            let cancelled = Arc::new(AtomicBool::new(false));
            let got = decode_payload(&data, None, DecodeLimits::default(), &cancelled).unwrap();
            assert_eq!(got.len(), 3, "{codec:?}");
            assert_eq!(got, expected, "{codec:?}");
            let two = decode_payload(&data, Some(2), DecodeLimits::default(), &cancelled).unwrap();
            assert_eq!(two.len(), 2, "{codec:?}");

            // A block whose sync marker was overwritten is refused.
            let mut bad = data.clone();
            let last = bad.len() - 1;
            bad[last] ^= 0xff;
            let err = decode_payload(&bad, None, DecodeLimits::default(), &cancelled).unwrap_err();
            assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");
        }
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
