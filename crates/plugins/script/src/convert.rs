//! Document ↔ rhai map conversion, including the non-finite float guard and the
//! restore of untouched integers above `i64::MAX`.

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
///
/// Every value is converted before any is written, so a failure leaves `doc` exactly
/// as it was: `on_error: keep` promises the document untouched.
pub(crate) fn from_scope_map(map: &Map, doc: &mut Document) -> Result<(), PluginError> {
    for key in ["id", "title", "content", "fields"] {
        if let Some(v) = map.get(key) {
            reject_non_finite(v, &format!("doc.{key}"))?;
        }
    }
    let id = match map.get("id") {
        Some(id) => Some(match dynamic_to_json(id)? {
            Value::String(s) => sanitize_id(&s),
            other => sanitize_id(&other.to_string()),
        }),
        None => None,
    };
    let title = match map.get("title") {
        None => None,
        Some(t) if t.is_unit() => None,
        Some(t) => Some(match dynamic_to_json(t)? {
            Value::String(s) => s,
            other => other.to_string(),
        }),
    };
    let content = match map.get("content") {
        Some(content) => Some(match dynamic_to_json(content)? {
            Value::String(s) => s,
            Value::Null => String::new(),
            other => other.to_string(),
        }),
        None => None,
    };
    let fields = match map.get("fields") {
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

    let mut fields = fields;
    for (key, value) in fields.iter_mut() {
        if let Some(original) = doc.fields.get(key) {
            restore_large_integers(value, original);
        }
    }

    if let Some(id) = id {
        doc.id = id;
    }
    doc.title = title;
    if let Some(content) = content {
        doc.content = content;
    }
    doc.fields = fields;
    Ok(())
}

/// Put back the integers above `i64::MAX` the script left alone.
///
/// Rhai integers are i64, so such a u64 reaches the script as a float and would be
/// written back as one (`18446744073709551615` becoming `1.8446744073709552e19`)
/// even when the script never touched it. Wherever the original held such an
/// integer at the same path and the result is the float it was converted to, the
/// original number is restored. A value the script moved to another key, or
/// computed with, stays the float rhai produced.
fn restore_large_integers(new: &mut Value, original: &Value) {
    match (new, original) {
        (Value::Number(n), Value::Number(o)) => {
            if let Some(big) = o.as_u64()
                && big > i64::MAX as u64
                && n.is_f64()
                && n.as_f64() == Some(big as f64)
            {
                *n = o.clone();
            }
        }
        (Value::Object(n), Value::Object(o)) => {
            for (key, value) in n.iter_mut() {
                if let Some(orig) = o.get(key) {
                    restore_large_integers(value, orig);
                }
            }
        }
        (Value::Array(n), Value::Array(o)) => {
            for (value, orig) in n.iter_mut().zip(o) {
                restore_large_integers(value, orig);
            }
        }
        _ => {}
    }
}
