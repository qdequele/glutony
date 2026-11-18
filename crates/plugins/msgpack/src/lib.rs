//! # `msgpack_parser` — MessagePack to flat documents
//!
//! Built-in `meili-ingest` plugin for MessagePack payloads. MessagePack is JSON
//! with a binary encoding, so this plugin decodes the payload to JSON values and
//! then hands them to the shared flattening rules of
//! [`json_flattener`](meili_ingest_plugin_json) — same dotted keys, same
//! id/title/content selection, same wrapper-key lookup.
//!
//! A payload may hold one value or several concatenated ones (the NDJSON analog):
//! * a single value → the `json_flattener` selection rules apply (top-level array
//!   → one document per element, wrapper object → its array, otherwise one document);
//! * several concatenated values → one document per value.
//!
//! Type mapping beyond the obvious: `bin` becomes a base64 string, map keys that
//! are not strings are stringified, and the standard timestamp extension (type
//! `-1`) becomes an RFC 3339 string. Any other extension becomes a base64 string.
//!
//! Configuration (all keys optional):
//!
//! | key              | default | meaning |
//! |------------------|---------|---------|
//! | `id_field`       | `id`    | flattened key holding the document id; generated as `<stem>_<index>` when absent |
//! | `content_fields` | all string leaves | flattened keys joined by a space into `content` (removed from `fields`) |
//! | `title_field`    | none    | flattened key mapped onto `title` (removed from `fields`) |
//! | `flatten_arrays` | `false` | flatten arrays of objects into `a.0.b` keys instead of keeping them as JSON arrays |
//! | `max_depth`      | `8`     | nesting depth beyond which objects are kept as-is |
//! | `max_records`    | none    | stop after this many documents (truncates) |
//!
//! Whatever the step says, the worker's [`DecodeLimits`] (`PARSER_MAX_ROWS`,
//! `PARSER_MAX_DECODED_MB`) bound what one payload may decode: a payload over either
//! fails the step as invalid input instead of being truncated. Each value is
//! measured by walking its bytes before it is decoded, so a value too large to fit
//! is refused before any of it is allocated.

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::DateTime;
use std::sync::atomic::{AtomicBool, Ordering};

use meili_ingest_plugin_json::limits::NODE_BYTES;
use meili_ingest_plugin_json::{
    DecodeBudget, DecodeLimits, FlattenConfig, documents_from_values, id_stem, select_roots,
};
use meili_ingest_plugin_sdk::prelude::*;
use rmpv::Utf8String;
use rmpv::Value as Mp;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "msgpack_parser";

/// The MessagePack extension type reserved for timestamps.
const EXT_TIMESTAMP: i8 = -1;

/// The MessagePack parser plugin. Construct with [`MsgpackParserPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct MsgpackParserPlugin {
    limits: DecodeLimits,
}

impl MsgpackParserPlugin {
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

/// Decode the standard timestamp extension into an RFC 3339 string.
///
/// The spec allows three widths: 4 bytes (seconds), 8 bytes (30-bit nanoseconds
/// then 34-bit seconds packed big-endian) and 12 bytes (nanoseconds then a signed
/// 64-bit seconds). Anything else is not a timestamp.
fn decode_timestamp(data: &[u8]) -> Option<String> {
    let (secs, nanos) = match data.len() {
        4 => (u32::from_be_bytes(data.try_into().ok()?) as i64, 0),
        8 => {
            let packed = u64::from_be_bytes(data.try_into().ok()?);
            (
                ((packed & 0x0000_0003_ffff_ffff) as i64),
                (packed >> 34) as u32,
            )
        }
        12 => {
            let nanos = u32::from_be_bytes(data[..4].try_into().ok()?);
            let secs = i64::from_be_bytes(data[4..].try_into().ok()?);
            (secs, nanos)
        }
        _ => return None,
    };
    DateTime::from_timestamp(secs, nanos).map(|dt| dt.to_rfc3339())
}

/// MessagePack `str` is not required to be valid UTF-8, so decode leniently
/// rather than dropping the value.
fn utf8_string(s: Utf8String) -> String {
    match s.as_str() {
        Some(valid) => valid.to_owned(),
        None => String::from_utf8_lossy(s.as_bytes()).into_owned(),
    }
}

/// Render a map key as a string: strings stay as they are, everything else is
/// rendered the way it would read in JSON so no entry is silently dropped.
fn map_key(key: Mp) -> String {
    match key {
        Mp::String(s) => utf8_string(s),
        other => match convert(other) {
            Value::String(s) => s,
            v => v.to_string(),
        },
    }
}

/// Convert a decoded MessagePack value into its JSON equivalent.
fn convert(value: Mp) -> Value {
    match value {
        Mp::Nil => Value::Null,
        Mp::Boolean(b) => Value::Bool(b),
        Mp::Integer(i) => i
            .as_i64()
            .map(Value::from)
            .or_else(|| i.as_u64().map(Value::from))
            .or_else(|| {
                i.as_f64()
                    .and_then(serde_json::Number::from_f64)
                    .map(Value::Number)
            })
            .unwrap_or(Value::Null),
        Mp::F32(f) => serde_json::Number::from_f64(f as f64).map_or(Value::Null, Value::Number),
        Mp::F64(f) => serde_json::Number::from_f64(f).map_or(Value::Null, Value::Number),
        Mp::String(s) => Value::String(utf8_string(s)),
        Mp::Binary(b) => Value::String(BASE64.encode(b)),
        Mp::Array(items) => Value::Array(items.into_iter().map(convert).collect()),
        Mp::Map(entries) => {
            let mut out = Map::with_capacity(entries.len());
            for (k, v) in entries {
                out.insert(map_key(k), convert(v));
            }
            Value::Object(out)
        }
        Mp::Ext(tag, data) => {
            if tag == EXT_TIMESTAMP
                && let Some(ts) = decode_timestamp(&data)
            {
                Value::String(ts)
            } else {
                Value::String(BASE64.encode(data))
            }
        }
    }
}

/// Value nodes walked between two cancellation checks while measuring.
const CANCEL_EVERY_NODES: u64 = 1 << 16;

/// Base64 length of `n` bytes.
fn base64_len(n: u64) -> u64 {
    n.div_ceil(3) * 4
}

/// Estimated decoded size, in [`decoded_size`](meili_ingest_plugin_json::decoded_size)
/// units, of the first MessagePack value of `data`, found by walking its markers
/// without allocating anything. Fails as soon as the value cannot fit in `budget`,
/// so a huge value is refused before it is decoded. `Ok(None)` when the bytes end
/// before the value does, or hold a marker MessagePack never uses.
fn measure_value(
    data: &[u8],
    budget: &DecodeBudget,
    cancelled: &AtomicBool,
) -> Result<Option<u64>, PluginError> {
    let mut pos = 0usize;
    // Big-endian length of `width` bytes at `pos`.
    let read_len = |pos: &mut usize, width: usize| -> Option<u64> {
        let bytes = data.get(*pos..*pos + width)?;
        *pos += width;
        Some(bytes.iter().fold(0u64, |acc, b| (acc << 8) | u64::from(*b)))
    };
    let mut pending: u64 = 1;
    let mut size: u64 = 0;
    let mut nodes: u64 = 0;
    while pending > 0 {
        pending -= 1;
        nodes += 1;
        if nodes.is_multiple_of(CANCEL_EVERY_NODES) {
            if cancelled.load(Ordering::Relaxed) {
                return Err(PluginError::Cancelled);
            }
            budget.check_bytes(size)?;
        }
        let Some(&marker) = data.get(pos) else {
            return Ok(None);
        };
        pos += 1;
        size += NODE_BYTES;
        // Payload bytes to skip, and the decoded text they add.
        let (skip, text): (u64, u64) = match marker {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => (0, 0),
            0x80..=0x8f => {
                pending += 2 * u64::from(marker & 0x0f);
                (0, 0)
            }
            0x90..=0x9f => {
                pending += u64::from(marker & 0x0f);
                (0, 0)
            }
            0xa0..=0xbf => {
                let n = u64::from(marker & 0x1f);
                (n, n)
            }
            0xc4..=0xc6 => {
                let Some(n) = read_len(&mut pos, 1 << (marker - 0xc4)) else {
                    return Ok(None);
                };
                (n, base64_len(n))
            }
            0xc7..=0xc9 => {
                let Some(n) = read_len(&mut pos, 1 << (marker - 0xc7)) else {
                    return Ok(None);
                };
                // The type byte, then the data; rendered as base64 or a timestamp.
                (n + 1, base64_len(n).max(40))
            }
            0xca => (4, 0),
            0xcb => (8, 0),
            0xcc..=0xcf => (1 << (marker - 0xcc), 0),
            0xd0..=0xd3 => (1 << (marker - 0xd0), 0),
            0xd4..=0xd8 => {
                let n = 1u64 << (marker - 0xd4);
                (n + 1, base64_len(n).max(40))
            }
            0xd9..=0xdb => {
                let Some(n) = read_len(&mut pos, 1 << (marker - 0xd9)) else {
                    return Ok(None);
                };
                (n, n)
            }
            0xdc | 0xdd => {
                let Some(n) = read_len(&mut pos, if marker == 0xdc { 2 } else { 4 }) else {
                    return Ok(None);
                };
                pending += n;
                (0, 0)
            }
            0xde | 0xdf => {
                let Some(n) = read_len(&mut pos, if marker == 0xde { 2 } else { 4 }) else {
                    return Ok(None);
                };
                pending += 2 * n;
                (0, 0)
            }
            // Never used by MessagePack.
            0xc1 => return Ok(None),
        };
        let Some(end) = usize::try_from(skip).ok().and_then(|s| pos.checked_add(s)) else {
            return Ok(None);
        };
        if end > data.len() {
            return Ok(None);
        }
        pos = end;
        size += text;
        // Every pending item takes at least one more byte: a count past the bytes
        // left is a truncated value, not a reason to keep walking.
        if pending > (data.len() - pos) as u64 {
            return Ok(None);
        }
    }
    budget.check_bytes(size)?;
    Ok(Some(size))
}

/// Read the concatenated MessagePack values in the payload: all of them, or the
/// first `max_records` when set. Each value is measured against `limits` before it
/// is decoded, and the records produced are counted against them.
fn decode_payload(
    data: &[u8],
    max_records: Option<usize>,
    limits: DecodeLimits,
    cancelled: &AtomicBool,
) -> Result<Vec<Value>, PluginError> {
    if data.is_empty() {
        return Err(PluginError::InvalidInput(format!(
            "{NAME}: payload is empty"
        )));
    }
    let mut budget = DecodeBudget::new(limits, NAME);
    let mut rest = data;
    let mut values = Vec::new();
    while !rest.is_empty() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(PluginError::Cancelled);
        }
        if max_records.is_some_and(|max| values.len() >= max) {
            // More values follow, so the payload is a concatenation: one record each.
            break;
        }
        let (count, offset) = (values.len(), data.len() - rest.len());
        let invalid = |why: &dyn std::fmt::Display| {
            PluginError::InvalidInput(format!(
                "{NAME}: invalid MessagePack after {count} value(s) at byte {offset}: {why}"
            ))
        };
        budget.add_rows(1)?;
        let size = measure_value(rest, &budget, cancelled)?
            .ok_or_else(|| invalid(&"the value is truncated or uses a reserved marker"))?;
        budget.add_bytes(size)?;
        let value = rmpv::decode::read_value(&mut rest).map_err(|e| invalid(&e))?;
        values.push(convert(value));
    }
    // One value behaves like a JSON document: an array or a wrapper object holds
    // the records. Several concatenated values are already one record each.
    let mut roots = if values.len() == 1 && rest.is_empty() {
        select_roots(values.pop().expect("len checked"))
    } else {
        values
    };
    if let Some(max) = max_records {
        roots.truncate(max);
    }
    // The records a single value holds were counted as one row above.
    budget.check_rows(roots.len() as u64)?;
    Ok(roots)
}

#[async_trait]
impl Plugin for MsgpackParserPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Decodes MessagePack payloads (one value, or several concatenated) into \
                 documents with dotted-key fields, using the same rules as json_flattener.",
            )
            .accepts([InputKind::Bytes])
            .produces(OutputKind::Documents)
            .content_types(["application/vnd.msgpack", "application/x-msgpack"])
            .config_schema(serde_json::json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "id_field": {
                        "type": "string",
                        "default": "id",
                        "description": "Flattened key holding the document id. Generated as <filename stem>_<index> when absent."
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
                        "description": "Stop after this many documents. The worker's PARSER_MAX_ROWS still applies."
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

        let stem = id_stem(&blob, "msgpack");
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
                .map(|(i, v)| (v, format!("{stem}_{i}")))
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
    use serde_json::json;

    /// Encode a JSON value as MessagePack with string map keys.
    fn pack(value: &Value) -> Vec<u8> {
        rmp_serde::to_vec_named(value).expect("encode")
    }

    /// Encode a raw MessagePack value, for shapes JSON cannot express (bin, ext).
    fn pack_raw(value: &Mp) -> Vec<u8> {
        let mut out = Vec::new();
        rmpv::encode::write_value(&mut out, value).expect("encode");
        out
    }

    fn bytes(data: Vec<u8>, filename: Option<&str>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            data,
            "application/vnd.msgpack",
            filename.map(str::to_owned),
        ))
    }

    async fn run(data: Vec<u8>, filename: Option<&str>, cfg: Value) -> Vec<Document> {
        match MsgpackParserPlugin::new()
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
        let m = MsgpackParserPlugin::new().manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(m.accepts, vec![InputKind::Bytes]);
        assert_eq!(m.produces, OutputKind::Documents);
        assert!(
            m.content_types
                .contains(&"application/vnd.msgpack".to_string())
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

    #[tokio::test]
    async fn decodes_array_with_the_json_flattening_rules() {
        let payload = json!([
            {"id": 7, "name": "Alice", "address": {"city": "Paris"}, "tags": ["a", "b"]},
            {"name": "Bob"}
        ]);
        let docs = run(pack(&payload), Some("users.msgpack"), json!({})).await;
        assert_eq!(docs.len(), 2);
        let a = &docs[0];
        assert_eq!(a.id, "7");
        assert_eq!(a.fields["address.city"], json!("Paris"), "dotted keys");
        assert_eq!(a.fields["tags"], json!(["a", "b"]));
        assert_eq!(a.meta.filename.as_deref(), Some("users.msgpack"));
        assert_eq!(
            docs[1].id, "users_1",
            "missing id falls back to the stem and index"
        );
    }

    #[tokio::test]
    async fn wrapper_keys_apply_to_a_single_value() {
        let payload = json!({"total": 2, "hits": [{"id": "x"}, {"id": "y"}]});
        let docs = run(pack(&payload), None, json!({})).await;
        assert_eq!(
            docs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            vec!["x", "y"]
        );
    }

    #[tokio::test]
    async fn concatenated_values_are_one_document_each() {
        let mut data = pack(&json!({"id": 1, "t": "one"}));
        data.extend(pack(&json!({"id": 2, "t": "two"})));
        data.extend(pack(&json!({"id": 3, "t": "three"})));
        let docs = run(data.clone(), None, json!({})).await;
        assert_eq!(docs.len(), 3);
        assert_eq!(docs[2].id, "3");
        assert_eq!(docs[2].content, "three");

        let docs = run(data, None, json!({"max_records": 2})).await;
        assert_eq!(docs.len(), 2, "max_records truncates");
    }

    #[tokio::test]
    async fn binary_becomes_base64_and_non_string_keys_are_stringified() {
        let value = Mp::Map(vec![
            (Mp::String("id".into()), Mp::String("b1".into())),
            (Mp::String("blob".into()), Mp::Binary(vec![1, 2, 3])),
            (Mp::Integer(7.into()), Mp::String("by-int-key".into())),
            (Mp::Boolean(true), Mp::String("by-bool-key".into())),
        ]);
        let docs = run(pack_raw(&value), None, json!({})).await;
        let d = &docs[0];
        assert_eq!(d.id, "b1");
        assert_eq!(d.fields["blob"], json!("AQID"), "bin is base64");
        assert_eq!(d.fields["7"], json!("by-int-key"));
        assert_eq!(d.fields["true"], json!("by-bool-key"));
    }

    #[tokio::test]
    async fn timestamp_extension_becomes_rfc3339() {
        // 32-bit form: seconds since the epoch.
        let secs = Mp::Ext(EXT_TIMESTAMP, 1_700_000_000u32.to_be_bytes().to_vec());
        // 96-bit form: nanoseconds then a signed 64-bit seconds.
        let mut wide = 500_000_000u32.to_be_bytes().to_vec();
        wide.extend(1_700_000_000i64.to_be_bytes());
        let value = Mp::Map(vec![
            (Mp::String("id".into()), Mp::String("t1".into())),
            (Mp::String("at".into()), secs),
            (Mp::String("precise".into()), Mp::Ext(EXT_TIMESTAMP, wide)),
            (Mp::String("other".into()), Mp::Ext(42, vec![0xAA])),
        ]);
        let docs = run(pack_raw(&value), None, json!({})).await;
        let d = &docs[0];
        assert_eq!(d.fields["at"], json!("2023-11-14T22:13:20+00:00"));
        assert_eq!(d.fields["precise"], json!("2023-11-14T22:13:20.500+00:00"));
        assert_eq!(
            d.fields["other"],
            json!("qg=="),
            "non-timestamp extensions stay base64"
        );
    }

    #[tokio::test]
    async fn title_and_content_fields_are_honoured() {
        let payload = json!({"id": "doc 1", "headline": "Hello", "body": "World", "n": 3});
        let cfg = json!({"title_field": "headline", "content_fields": ["body", "n"]});
        let docs = run(pack(&payload), None, cfg).await;
        assert_eq!(docs[0].id, "doc_1", "id is sanitized");
        assert_eq!(docs[0].title.as_deref(), Some("Hello"));
        assert_eq!(docs[0].content, "World 3");
    }

    #[tokio::test]
    async fn invalid_input_and_config_are_rejected() {
        let plugin = MsgpackParserPlugin::new();
        let ctx = ActivityContext::noop();

        let err = plugin
            .execute(&ctx, bytes(Vec::new(), None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");

        // 0x91 opens a one-element array whose element never arrives.
        let err = plugin
            .execute(&ctx, bytes(vec![0x91], None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");

        // A truncated value must fail rather than yield a partial document.
        let mut truncated = pack(&json!({"id": 1, "name": "Alice"}));
        truncated.truncate(truncated.len() - 3);
        let err = plugin
            .execute(&ctx, bytes(truncated, None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");

        for bad in [
            json!({"bogus": true}),
            json!({"max_records": 0}),
            json!({"max_depth": 0}),
        ] {
            let err = plugin
                .execute(&ctx, bytes(pack(&json!({})), None), bad.clone())
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
    async fn concatenated_values_over_the_default_row_limit_fail() {
        // One `nil` byte per value.
        let rows = meili_ingest_plugin_json::limits::DEFAULT_MAX_ROWS as usize + 1;
        let err = MsgpackParserPlugin::new()
            .execute(
                &ActivityContext::noop(),
                bytes(vec![0xC0; rows], None),
                Value::Null,
            )
            .await
            .unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_ROWS")),
            "{err}"
        );
    }

    #[test]
    fn the_worker_limits_bound_records_and_decoded_bytes_before_decoding() {
        let never = AtomicBool::new(false);
        let rows_cap = DecodeLimits {
            max_rows: 3,
            ..DecodeLimits::default()
        };
        // Concatenated values, and the elements of a single array value.
        for data in [vec![0xC0; 4], pack(&json!([1, 2, 3, 4]))] {
            let err = decode_payload(&data, None, rows_cap, &never).unwrap_err();
            assert!(
                matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_ROWS")),
                "{err}"
            );
            // Truncating below the cap is the step's own choice; above it is not.
            assert_eq!(
                decode_payload(&data, Some(3), rows_cap, &never)
                    .unwrap()
                    .len(),
                3
            );
            assert!(decode_payload(&data, Some(4), rows_cap, &never).is_err());
        }
        // `max_records` reading only the first value still treats the payload as
        // a concatenation, as before.
        let mut two = pack(&json!([1, 2]));
        two.extend(pack(&json!([3])));
        assert_eq!(
            decode_payload(&two, Some(1), DecodeLimits::default(), &never).unwrap(),
            vec![json!([1, 2])]
        );

        // One map holding an array of a million nils: a ~1 MB value whose decode
        // would take ~32 MB. It is refused before rmpv allocates it.
        let mut data = vec![0x81, 0xA1, b'v', 0xDD];
        data.extend(1_000_000u32.to_be_bytes());
        data.extend(vec![0xC0; 1_000_000]);
        let bytes_cap = DecodeLimits {
            max_decoded_bytes: 1024 * 1024,
            ..DecodeLimits::default()
        };
        let err = decode_payload(&data, None, bytes_cap, &never).unwrap_err();
        assert!(
            matches!(&err, PluginError::InvalidInput(m) if m.contains("PARSER_MAX_DECODED_MB")),
            "{err}"
        );
        assert_eq!(
            decode_payload(&data, None, DecodeLimits::default(), &never)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn the_measured_size_matches_the_decoded_values() {
        let never = AtomicBool::new(false);
        let budget = DecodeBudget::new(DecodeLimits::default(), NAME);
        let value = json!({"id": 1, "name": "Alice", "tags": ["a", "b"], "n": null, "f": 1.5});
        let size = measure_value(&pack(&value), &budget, &never)
            .unwrap()
            .unwrap();
        let string = std::mem::size_of::<String>() as u64;
        // The measure counts keys as nodes; the decoded map as key strings.
        let decoded = meili_ingest_plugin_json::decoded_size(&value);
        assert!(
            size + 5 * string >= decoded && size <= decoded + 5 * NODE_BYTES,
            "{size} vs {decoded}"
        );
        // Truncated values and the reserved marker are not measured.
        assert_eq!(measure_value(&[0x92, 0xC0], &budget, &never).unwrap(), None);
        assert_eq!(measure_value(&[0xC1], &budget, &never).unwrap(), None);
        assert_eq!(
            measure_value(&[0xDD, 0xFF, 0xFF, 0xFF, 0xFF], &budget, &never).unwrap(),
            None,
            "a count past the bytes left is not walked"
        );
    }

    #[test]
    fn a_cancel_stops_the_decode_loop() {
        let cancelled = AtomicBool::new(true);
        let err =
            decode_payload(&[0xC0; 10], None, DecodeLimits::default(), &cancelled).unwrap_err();
        assert!(matches!(err, PluginError::Cancelled), "{err}");
    }

    #[tokio::test]
    async fn honours_cancellation() {
        let ctx = ActivityContext::noop();
        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = MsgpackParserPlugin::new()
            .execute(&ctx, bytes(pack(&json!([{"id": 1}])), None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled), "{err}");
    }
}
