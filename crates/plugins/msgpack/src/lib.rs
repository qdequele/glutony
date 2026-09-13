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
//! | `max_records`    | unlimited | stop after this many documents |

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use chrono::DateTime;
use meili_ingest_plugin_json::{FlattenConfig, documents_from_values, id_stem, select_roots};
use meili_ingest_plugin_sdk::prelude::*;
use rmpv::Utf8String;
use rmpv::Value as Mp;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "msgpack_parser";

/// The MessagePack extension type reserved for timestamps.
const EXT_TIMESTAMP: i8 = -1;

/// The MessagePack parser plugin. Stateless; construct with [`MsgpackParserPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct MsgpackParserPlugin;

impl MsgpackParserPlugin {
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

/// Read every concatenated MessagePack value in the payload.
fn decode_payload(data: &[u8]) -> Result<Vec<Value>, PluginError> {
    if data.is_empty() {
        return Err(PluginError::InvalidInput(format!(
            "{NAME}: payload is empty"
        )));
    }
    let mut rest = data;
    let mut values = Vec::new();
    while !rest.is_empty() {
        let value = rmpv::decode::read_value(&mut rest).map_err(|e| {
            PluginError::InvalidInput(format!(
                "{NAME}: invalid MessagePack after {} value(s) at byte {}: {e}",
                values.len(),
                data.len() - rest.len()
            ))
        })?;
        values.push(convert(value));
    }
    // One value behaves like a JSON document: an array or a wrapper object holds
    // the records. Several concatenated values are already one record each.
    Ok(match values.len() {
        1 => select_roots(values.pop().expect("len checked")),
        _ => values,
    })
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
                        "description": "Stop after this many documents."
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
        let mut roots = run_blocking(move || decode_payload(&data)).await??;
        if let Some(max) = cfg.max_records {
            roots.truncate(max);
        }

        let stem = id_stem(blob.filename.as_deref(), "msgpack");
        let meta = DocumentMeta {
            source: blob.filename.clone(),
            filename: blob.filename.clone(),
            mime: Some(blob.mime.clone()),
            ..Default::default()
        };
        let items = roots
            .into_iter()
            .enumerate()
            .map(|(i, v)| (v, format!("{stem}_{i}")))
            .collect();

        Ok(PluginOutput::Documents(documents_from_values(
            ctx, items, &flatten, &meta, NAME,
        )?))
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
        let m = MsgpackParserPlugin.manifest();
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
