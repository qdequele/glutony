//! Document ↔ rhai map conversion, including the non-finite float guard.

use meili_ingest_plugin_sdk::{Document, PluginError, sanitize_id};
use rhai::{Dynamic, Map};
use serde_json::Value;

use crate::NAME;

/// Bind a document as the rhai map the script mutates.
///
/// Built key by key rather than through `serde`: `Document` skips empty `title` and
/// `fields` when it serialises, and a script that cannot count on `doc.fields`
/// existing is unusable.
pub(crate) fn to_scope_map(doc: &Document) -> Result<Map, PluginError> {
    let mut map = Map::new();
    map.insert("id".into(), Dynamic::from(doc.id.clone()));
    map.insert(
        "title".into(),
        match &doc.title {
            Some(t) => Dynamic::from(t.clone()),
            None => Dynamic::UNIT,
        },
    );
    map.insert("content".into(), Dynamic::from(doc.content.clone()));
    map.insert(
        "fields".into(),
        json_to_dynamic(&Value::Object(doc.fields.clone()))?,
    );
    map.insert(
        "meta".into(),
        json_to_dynamic(
            &serde_json::to_value(&doc.meta).map_err(|e| {
                PluginError::NonRetryable(format!("{NAME}: cannot expose meta: {e}"))
            })?,
        )?,
    );
    Ok(map)
}

/// Convert JSON into rhai values.
fn json_to_dynamic(value: &Value) -> Result<Dynamic, PluginError> {
    rhai::serde::to_dynamic(value).map_err(|e| {
        PluginError::NonRetryable(format!("{NAME}: cannot convert to script value: {e}"))
    })
}

/// Reject floats JSON cannot represent, before they are silently turned into `null`.
///
/// Rhai produces NaN and infinities from ordinary float arithmetic — `x / 0.0` does
/// not raise, unlike integer division — and `serde_json` maps every non-finite float
/// onto `null`. Left alone that writes a null field into the index and reports
/// success, so the conversion refuses instead and the step's `on_error` policy
/// decides what happens to the document.
fn reject_non_finite(value: &Dynamic, path: &str) -> Result<(), PluginError> {
    if value.is_float() {
        let f = value.as_float().unwrap_or(0.0);
        if !f.is_finite() {
            return Err(PluginError::NonRetryable(format!(
                "{NAME}: `{path}` is not a finite number ({f})"
            )));
        }
    } else if let Some(map) = value.read_lock::<Map>() {
        for (k, v) in map.iter() {
            reject_non_finite(v, &format!("{path}.{k}"))?;
        }
    } else if let Some(array) = value.read_lock::<rhai::Array>() {
        for (i, v) in array.iter().enumerate() {
            reject_non_finite(v, &format!("{path}[{i}]"))?;
        }
    }
    Ok(())
}

/// Convert rhai values back into JSON.
fn dynamic_to_json(value: &Dynamic) -> Result<Value, PluginError> {
    rhai::serde::from_dynamic(value).map_err(|e| {
        PluginError::InvalidInput(format!(
            "{NAME}: script produced a value that is not valid JSON: {e}"
        ))
    })
}

/// Read the mutated map back onto the document.
///
/// `meta` is deliberately not read back: it is provenance the pipeline owns, so the
/// script sees it but cannot rewrite it.
pub(crate) fn from_scope_map(map: &Map, doc: &mut Document) -> Result<(), PluginError> {
    for key in ["id", "title", "content", "fields"] {
        if let Some(v) = map.get(key) {
            reject_non_finite(v, &format!("doc.{key}"))?;
        }
    }
    if let Some(id) = map.get("id") {
        let id = match dynamic_to_json(id)? {
            Value::String(s) => s,
            other => other.to_string(),
        };
        doc.id = sanitize_id(&id);
    }
    doc.title = match map.get("title") {
        None => None,
        Some(t) if t.is_unit() => None,
        Some(t) => Some(match dynamic_to_json(t)? {
            Value::String(s) => s,
            other => other.to_string(),
        }),
    };
    if let Some(content) = map.get("content") {
        doc.content = match dynamic_to_json(content)? {
            Value::String(s) => s,
            Value::Null => String::new(),
            other => other.to_string(),
        };
    }
    doc.fields = match map.get("fields") {
        Some(f) => match dynamic_to_json(f)? {
            Value::Object(o) => o,
            other => {
                return Err(PluginError::InvalidInput(format!(
                    "{NAME}: `doc.fields` must stay a map, got {other}"
                )));
            }
        },
        None => Default::default(),
    };
    Ok(())
}
