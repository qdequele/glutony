//! # `json_flattener` — JSON / NDJSON to flat documents
//!
//! Built-in `meili-ingest` plugin that turns JSON payloads into
//! [`Document`]s whose `fields` are flat, dotted-key objects (`a.b.c`).
//!
//! Input can be raw bytes (a JSON value, or NDJSON with one value per line)
//! or documents from a previous step, whose `fields` are re-flattened.
//!
//! Document selection from a single JSON value:
//! * top-level array → one document per element;
//! * object with an array under `documents`, `items`, `data`, `results` or
//!   `hits` → one document per element of that array;
//! * anything else → one document.
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

use meili_ingest_plugin_sdk::prelude::*;
use serde::Deserialize;
use serde_json::{Map, Value};

/// Plugin name referenced by `steps[].plugin`.
pub const NAME: &str = "json_flattener";

/// Documents between two heartbeats.
const HEARTBEAT_EVERY: usize = 100;

/// Wrapper keys that may hold the document array, in lookup order.
const WRAPPER_KEYS: [&str; 5] = ["documents", "items", "data", "results", "hits"];

/// The JSON flattener plugin. Stateless; construct with [`JsonFlattenerPlugin::new`].
#[derive(Debug, Clone, Default)]
pub struct JsonFlattenerPlugin;

impl JsonFlattenerPlugin {
    /// Create the plugin.
    pub fn new() -> Self {
        Self
    }
}

/// How a decoded value is turned into a [`Document`].
///
/// Public because every format that decodes to JSON values — MessagePack, Avro,
/// Parquet — flattens by exactly these rules. Those plugins declare their own
/// serde config (so each keeps `deny_unknown_fields` and its own JSON Schema) and
/// build a `FlattenConfig` from it before calling [`documents_from_values`].
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FlattenConfig {
    /// Flattened key holding the document id.
    pub id_field: String,
    /// Flattened keys joined into `content`; every string leaf when `None`.
    pub content_fields: Option<Vec<String>>,
    /// Flattened key mapped onto `title`.
    pub title_field: Option<String>,
    /// Flatten arrays of objects into `a.0.b` keys.
    pub flatten_arrays: bool,
    /// Nesting depth beyond which objects are kept as JSON values.
    pub max_depth: usize,
}

impl Default for FlattenConfig {
    fn default() -> Self {
        Self {
            id_field: "id".into(),
            content_fields: None,
            title_field: None,
            flatten_arrays: false,
            max_depth: 8,
        }
    }
}

impl FlattenConfig {
    /// Validate a raw `config:` block. `null` yields the defaults.
    pub fn parse(value: Value) -> Result<Self, PluginError> {
        if value.is_null() {
            return Ok(Self::default());
        }
        let cfg: FlattenConfig = serde_json::from_value(value)
            .map_err(|e| PluginError::InvalidConfig(format!("{NAME}: {e}")))?;
        if cfg.id_field.is_empty() {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `id_field` must not be empty"
            )));
        }
        if cfg.max_depth == 0 {
            return Err(PluginError::InvalidConfig(format!(
                "{NAME}: `max_depth` must be at least 1"
            )));
        }
        Ok(cfg)
    }
}

/// Parse bytes as a single JSON value, falling back to NDJSON (one value per line).
fn parse_payload(data: &[u8]) -> Result<Vec<Value>, PluginError> {
    match serde_json::from_slice::<Value>(data) {
        Ok(v) => Ok(select_roots(v)),
        Err(single_err) => {
            let text = std::str::from_utf8(data).map_err(|e| {
                PluginError::InvalidInput(format!("{NAME}: payload is not UTF-8: {e}"))
            })?;
            let mut roots = Vec::new();
            for (i, line) in text.lines().enumerate() {
                if line.trim().is_empty() {
                    continue;
                }
                let v: Value = serde_json::from_str(line).map_err(|e| {
                    PluginError::InvalidInput(format!(
                        "{NAME}: payload is neither JSON ({single_err}) nor NDJSON (line {}: {e})",
                        i + 1
                    ))
                })?;
                roots.push(v);
            }
            if roots.is_empty() {
                return Err(PluginError::InvalidInput(format!(
                    "{NAME}: invalid JSON: {single_err}"
                )));
            }
            Ok(roots)
        }
    }
}

/// Apply the selection rules (array / wrapper object / single value).
///
/// Public so binary formats carrying a JSON-shaped payload reuse the same rule.
pub fn select_roots(value: Value) -> Vec<Value> {
    match value {
        Value::Array(items) => items,
        Value::Object(mut obj) => {
            for key in WRAPPER_KEYS {
                if matches!(obj.get(key), Some(Value::Array(_)))
                    && let Some(Value::Array(items)) = obj.remove(key)
                {
                    return items;
                }
            }
            vec![Value::Object(obj)]
        }
        other => vec![other],
    }
}

fn is_scalar(v: &Value) -> bool {
    !matches!(v, Value::Object(_) | Value::Array(_))
}

/// Recursively flatten `value` under `prefix` into `out`.
fn flatten_into(
    prefix: &str,
    value: Value,
    depth: usize,
    cfg: &FlattenConfig,
    out: &mut Map<String, Value>,
) {
    let key = |k: &str| {
        if prefix.is_empty() {
            k.to_owned()
        } else {
            format!("{prefix}.{k}")
        }
    };
    match value {
        Value::Object(obj) => {
            if depth >= cfg.max_depth || obj.is_empty() {
                out.insert(prefix.to_owned(), Value::Object(obj));
                return;
            }
            for (k, v) in obj {
                flatten_into(&key(&k), v, depth + 1, cfg, out);
            }
        }
        Value::Array(items) => {
            let all_scalars = items.iter().all(is_scalar);
            if all_scalars || !cfg.flatten_arrays || depth >= cfg.max_depth {
                out.insert(prefix.to_owned(), Value::Array(items));
            } else {
                for (i, v) in items.into_iter().enumerate() {
                    flatten_into(&key(&i.to_string()), v, depth + 1, cfg, out);
                }
            }
        }
        scalar => {
            out.insert(prefix.to_owned(), scalar);
        }
    }
}

/// Flatten one root value into a fields map. Non-object roots land under `value`.
fn flatten_root(root: Value, cfg: &FlattenConfig) -> Map<String, Value> {
    let mut out = Map::new();
    match root {
        Value::Object(obj) => {
            for (k, v) in obj {
                flatten_into(&k, v, 1, cfg, &mut out);
            }
        }
        other => flatten_into("value", other, 1, cfg, &mut out),
    }
    out
}

/// Textual form of a value used for `id`, `title` and `content`.
fn value_text(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::Array(items) if items.iter().all(is_scalar) => items
            .iter()
            .map(value_text)
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        other => other.to_string(),
    }
}

/// Collect every string leaf (including strings inside scalar arrays), in key order.
fn string_leaves(fields: &Map<String, Value>) -> Vec<String> {
    let mut out = Vec::new();
    for v in fields.values() {
        match v {
            Value::String(s) if !s.is_empty() => out.push(s.clone()),
            Value::Array(items) => out.extend(
                items
                    .iter()
                    .filter_map(|i| i.as_str())
                    .filter(|s| !s.is_empty())
                    .map(str::to_owned),
            ),
            _ => {}
        }
    }
    out
}

/// Fallbacks taken from the previous document when re-flattening `Documents` input.
#[derive(Debug, Default)]
struct Fallback {
    id: Option<String>,
    title: Option<String>,
    content: Option<String>,
    meta: DocumentMeta,
}

fn build_document(
    root: Value,
    cfg: &FlattenConfig,
    generated_id: String,
    fallback: Fallback,
) -> Document {
    let mut fields = flatten_root(root, cfg);

    let id = fields
        .remove(&cfg.id_field)
        .map(|v| value_text(&v))
        .filter(|s| !s.is_empty())
        .map(|s| sanitize_id(&s))
        .or(fallback.id)
        .unwrap_or(generated_id);

    let title = cfg
        .title_field
        .as_ref()
        .and_then(|t| fields.remove(t))
        .map(|v| value_text(&v))
        .filter(|s| !s.is_empty())
        .or(fallback.title);

    let content = match &cfg.content_fields {
        Some(keys) => keys
            .iter()
            .filter_map(|k| fields.remove(k))
            .map(|v| value_text(&v))
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        None => string_leaves(&fields).join(" "),
    };
    let content = if content.is_empty() {
        fallback.content.unwrap_or_default()
    } else {
        content
    };

    Document {
        id,
        title,
        content,
        fields,
        meta: fallback.meta,
    }
}

/// Build documents from `(root, generated id, fallback)` triples.
fn build_documents(
    ctx: &ActivityContext,
    items: Vec<(Value, String, Fallback)>,
    cfg: &FlattenConfig,
    label: &str,
) -> Result<Vec<Document>, PluginError> {
    let mut docs = Vec::with_capacity(items.len());
    for (i, (root, generated_id, fallback)) in items.into_iter().enumerate() {
        ctx.check_cancelled()?;
        if i > 0 && i.is_multiple_of(HEARTBEAT_EVERY) {
            ctx.heartbeat(format!("{label}: {i} documents"));
        }
        docs.push(build_document(root, cfg, generated_id, fallback));
    }
    Ok(docs)
}

/// Flatten already-decoded values into documents using the `json_flattener` rules.
///
/// This is the seam the binary-format plugins hang off: decode your payload to
/// [`Value`]s, pair each with the id to fall back on when it carries no `id_field`,
/// and this applies the same flattening, id/title/content selection and
/// cancellation/heartbeat behaviour as `json_flattener` itself. `label` prefixes
/// heartbeat messages so the calling plugin's name shows up in worker logs.
pub fn documents_from_values(
    ctx: &ActivityContext,
    items: Vec<(Value, String)>,
    cfg: &FlattenConfig,
    meta: &DocumentMeta,
    label: &str,
) -> Result<Vec<Document>, PluginError> {
    let items = items
        .into_iter()
        .map(|(root, generated_id)| {
            let fallback = Fallback {
                meta: meta.clone(),
                ..Default::default()
            };
            (root, generated_id, fallback)
        })
        .collect();
    build_documents(ctx, items, cfg, label)
}

/// Filename stem used to build generated ids (`<stem>_<index>`).
///
/// `fallback` names the format (`json`, `msgpack`, ...) and is used when there is
/// no filename to derive a stem from.
pub fn id_stem(filename: Option<&str>, fallback: &str) -> String {
    let stem = filename
        .map(|f| f.rsplit(['/', '\\']).next().unwrap_or(f))
        .map(|f| f.rsplit_once('.').map(|(s, _)| s).unwrap_or(f))
        .filter(|s| !s.is_empty())
        .unwrap_or(fallback);
    sanitize_id(stem)
}

#[async_trait]
impl Plugin for JsonFlattenerPlugin {
    fn manifest(&self) -> PluginManifest {
        PluginManifest::new(NAME, env!("CARGO_PKG_VERSION"))
            .description(
                "Flattens JSON or NDJSON payloads (or the fields of upstream documents) into \
                 documents with dotted-key fields; picks id/title/content from configurable keys.",
            )
            .accepts([InputKind::Bytes, InputKind::Documents, InputKind::Many])
            .produces(OutputKind::Documents)
            .content_types(["application/json", "application/x-ndjson"])
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
        let cfg = FlattenConfig::parse(config)?;

        // (root value, generated id, fallbacks)
        let items: Vec<(Value, String, Fallback)> = match input {
            PluginInput::Bytes(blob) => {
                let stem = id_stem(blob.filename.as_deref(), "json");
                let meta = DocumentMeta {
                    source: blob.filename.clone(),
                    filename: blob.filename.clone(),
                    mime: Some(blob.mime.clone()),
                    ..Default::default()
                };
                parse_payload(&blob.data)?
                    .into_iter()
                    .enumerate()
                    .map(|(i, v)| {
                        (
                            v,
                            format!("{stem}_{i}"),
                            Fallback {
                                meta: meta.clone(),
                                ..Default::default()
                            },
                        )
                    })
                    .collect()
            }
            other => other
                .into_documents()?
                .into_iter()
                .enumerate()
                .map(|(i, d)| {
                    let generated = format!("{}_{i}", d.id);
                    (
                        Value::Object(d.fields),
                        generated,
                        Fallback {
                            id: Some(d.id),
                            title: d.title,
                            content: Some(d.content),
                            meta: d.meta,
                        },
                    )
                })
                .collect(),
        };

        Ok(PluginOutput::Documents(build_documents(
            ctx, items, &cfg, NAME,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn bytes(data: &str, filename: Option<&str>) -> PluginInput {
        PluginInput::Bytes(Blob::new(
            data.as_bytes().to_vec(),
            "application/json",
            filename.map(str::to_owned),
        ))
    }

    async fn run(input: PluginInput, cfg: Value) -> Vec<Document> {
        match JsonFlattenerPlugin::new()
            .execute(&ActivityContext::noop(), input, cfg)
            .await
            .unwrap()
        {
            PluginOutput::Documents(d) => d,
            other => panic!("expected documents, got {other:?}"),
        }
    }

    #[test]
    fn manifest_is_correct() {
        let m = JsonFlattenerPlugin.manifest();
        assert_eq!(m.name, NAME);
        assert_eq!(
            m.accepts,
            vec![InputKind::Bytes, InputKind::Documents, InputKind::Many]
        );
        assert_eq!(m.produces, OutputKind::Documents);
        assert_eq!(
            m.content_types,
            vec!["application/json", "application/x-ndjson"]
        );
        let props = m.config_schema["properties"].as_object().unwrap();
        for key in [
            "id_field",
            "content_fields",
            "title_field",
            "flatten_arrays",
            "max_depth",
        ] {
            assert!(props.contains_key(key), "schema missing {key}");
        }
    }

    #[tokio::test]
    async fn top_level_array_is_flattened_with_dotted_keys() {
        let payload = r#"[
            {"id": 7, "name": "Alice", "address": {"city": "Paris", "geo": {"lat": 1.5}}, "tags": ["a", "b"]},
            {"name": "Bob"}
        ]"#;
        let docs = run(bytes(payload, Some("users.json")), json!({})).await;
        assert_eq!(docs.len(), 2);
        let a = &docs[0];
        assert_eq!(a.id, "7");
        assert!(!a.fields.contains_key("id"), "id is removed from fields");
        assert_eq!(a.fields["name"], json!("Alice"));
        assert_eq!(a.fields["address.city"], json!("Paris"));
        assert_eq!(a.fields["address.geo.lat"], json!(1.5));
        assert_eq!(
            a.fields["tags"],
            json!(["a", "b"]),
            "scalar arrays stay arrays"
        );
        let mut words: Vec<&str> = a.content.split(' ').collect();
        words.sort_unstable();
        assert_eq!(
            words,
            vec!["Alice", "Paris", "a", "b"],
            "content is every string leaf"
        );
        assert_eq!(a.meta.filename.as_deref(), Some("users.json"));
        assert_eq!(
            docs[1].id, "users_1",
            "missing id is generated from the stem and index"
        );
    }

    #[tokio::test]
    async fn wrapper_keys_and_ndjson() {
        let docs = run(
            bytes(r#"{"total": 2, "hits": [{"id": "x"}, {"id": "y"}]}"#, None),
            json!({}),
        )
        .await;
        assert_eq!(
            docs.iter().map(|d| d.id.as_str()).collect::<Vec<_>>(),
            vec!["x", "y"]
        );

        let docs = run(
            bytes(
                "{\"id\": 1, \"t\": \"one\"}\n\n{\"id\": 2, \"t\": \"two\"}\n",
                None,
            ),
            json!({}),
        )
        .await;
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1].id, "2");
        assert_eq!(docs[1].content, "two");

        let docs = run(bytes(r#"{"id": "solo", "v": 1}"#, None), json!({})).await;
        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].id, "solo");
    }

    #[tokio::test]
    async fn arrays_of_objects_kept_unless_flatten_arrays() {
        let payload = r#"{"id": 1, "items": [{"sku": "a"}, {"sku": "b"}]}"#;
        // `items` is a wrapper key at the top level, so nest it one level down.
        let payload = format!(r#"{{"id": 1, "order": {payload}}}"#);
        let docs = run(bytes(&payload, None), json!({})).await;
        assert_eq!(
            docs[0].fields["order.items"],
            json!([{"sku": "a"}, {"sku": "b"}])
        );
        assert_eq!(docs[0].fields["order.id"], json!(1));

        let docs = run(bytes(&payload, None), json!({"flatten_arrays": true})).await;
        assert_eq!(docs[0].fields["order.items.0.sku"], json!("a"));
        assert_eq!(docs[0].fields["order.items.1.sku"], json!("b"));
    }

    #[tokio::test]
    async fn title_and_content_fields_are_mapped_and_removed() {
        let payload =
            r#"{"id": "doc 1", "headline": "Hello", "body": "World", "extra": "kept", "n": 3}"#;
        let cfg = json!({"title_field": "headline", "content_fields": ["body", "n"]});
        let docs = run(bytes(payload, None), cfg).await;
        let d = &docs[0];
        assert_eq!(d.id, "doc_1", "id is sanitized");
        assert_eq!(d.title.as_deref(), Some("Hello"));
        assert_eq!(d.content, "World 3");
        assert_eq!(d.fields.len(), 1);
        assert_eq!(d.fields["extra"], json!("kept"));
    }

    #[tokio::test]
    async fn custom_id_field_can_be_nested() {
        let payload = r#"[{"meta": {"key": "K1"}, "v": "a"}]"#;
        let docs = run(bytes(payload, None), json!({"id_field": "meta.key"})).await;
        assert_eq!(docs[0].id, "K1");
        assert!(!docs[0].fields.contains_key("meta.key"));
    }

    #[tokio::test]
    async fn max_depth_keeps_deep_objects_intact() {
        let payload = r#"{"a": {"b": {"c": {"d": 1}}}}"#;
        let docs = run(bytes(payload, None), json!({"max_depth": 2})).await;
        assert_eq!(docs[0].fields["a.b"], json!({"c": {"d": 1}}));
    }

    #[tokio::test]
    async fn reflattens_upstream_documents() {
        let mut d = Document::with_id("parent", "old content");
        d.title = Some("T".into());
        d.meta.page = Some(3);
        d.fields
            .insert("nested".into(), json!({"x": {"y": "deep"}, "list": [1, 2]}));
        let docs = run(PluginInput::Documents(vec![d]), json!({})).await;
        let out = &docs[0];
        assert_eq!(out.id, "parent", "id is kept when no id field is present");
        assert_eq!(out.title.as_deref(), Some("T"));
        assert_eq!(out.fields["nested.x.y"], json!("deep"));
        assert_eq!(out.fields["nested.list"], json!([1, 2]));
        assert_eq!(out.content, "deep", "content is rebuilt from string leaves");
        assert_eq!(out.meta.page, Some(3), "meta is preserved");

        // Many input is flattened too, and an upstream `content` survives when nothing else is textual.
        let d = Document::with_id("p2", "keep me");
        let docs = run(
            PluginInput::Many(vec![PluginOutput::Documents(vec![d])]),
            json!({}),
        )
        .await;
        assert_eq!(docs[0].content, "keep me");
    }

    #[tokio::test]
    async fn scalar_roots_and_errors() {
        let docs = run(bytes(r#"["hello", 42]"#, Some("vals.json")), json!({})).await;
        assert_eq!(docs[0].id, "vals_0");
        assert_eq!(docs[0].fields["value"], json!("hello"));
        assert_eq!(docs[0].content, "hello");
        assert_eq!(docs[1].fields["value"], json!(42));

        let plugin = JsonFlattenerPlugin::new();
        let ctx = ActivityContext::noop();
        let err = plugin
            .execute(&ctx, bytes("{not json", None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidInput(_)), "{err}");
        let err = plugin
            .execute(&ctx, bytes("{}", None), json!({"max_depth": 0}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");
        let err = plugin
            .execute(&ctx, bytes("{}", None), json!({"bogus": true}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::InvalidConfig(_)), "{err}");

        ctx.cancellation_flag()
            .store(true, std::sync::atomic::Ordering::Relaxed);
        let err = plugin
            .execute(&ctx, bytes("[1]", None), json!({}))
            .await
            .unwrap_err();
        assert!(matches!(err, PluginError::Cancelled));
    }
}
