//! Document helpers registered on every engine.
//!
//! They are the operations the builder offers, callable as methods on `doc` from Code
//! mode too (`doc.rename_field("prix", "price")`); `ops` compile to one call per
//! operation. One rule throughout: a **missing** field is a no-op, a field that is
//! **present with the wrong type** is an error, handled by the step's `on_error`.
//! `default_field` and the `field_is_*` / `field_equals` tests are where absence is
//! the point.

use rhai::{Array, Dynamic, Engine, EvalAltResult, Map};

pub(crate) type HelperResult<T> = Result<T, Box<EvalAltResult>>;

pub(crate) fn fail<T>(message: String) -> HelperResult<T> {
    Err(message.into())
}

/// Register every helper on `engine`.
pub(crate) fn register(engine: &mut Engine) {
    engine.register_fn("rename_field", rename_field);
    engine.register_fn("drop_fields", drop_fields);
    engine.register_fn("keep_fields", keep_fields);
    engine.register_fn("copy_field", copy_field);
    engine.register_fn("field_is_missing", field_is_missing);
    engine.register_fn("field_is_empty", field_is_empty);
    engine.register_fn("field_equals", field_equals);
}

/// Run `f` on `doc.fields`.
pub(crate) fn with_fields<T>(
    doc: &mut Map,
    f: impl FnOnce(&mut Map) -> HelperResult<T>,
) -> HelperResult<T> {
    let Some(fields) = doc.get_mut("fields") else {
        return fail("`doc.fields` is missing".into());
    };
    let Some(mut fields) = fields.write_lock::<Map>() else {
        return fail("`doc.fields` must be a map".into());
    };
    f(&mut fields)
}

/// The field names in a helper's list argument.
pub(crate) fn names(list: Array, helper: &str) -> HelperResult<Vec<String>> {
    list.into_iter()
        .map(|name| {
            name.into_string().or_else(|type_name| {
                fail(format!(
                    "{helper}: field names must be strings, got {type_name}"
                ))
            })
        })
        .collect()
}

/// Where the `field_is_*` and `field_equals` tests read `name`. `id`, `title` and
/// `content` are the document's own attributes — the ones every document has, and the
/// ones "drop empty documents" is about; every other name is a field. A title of `()`
/// counts as missing.
fn lookup(doc: &mut Map, name: &str) -> HelperResult<Option<Dynamic>> {
    match name {
        "id" | "title" | "content" => Ok(doc.get(name).filter(|v| !v.is_unit()).cloned()),
        _ => with_fields(doc, |fields| Ok(fields.get(name).cloned())),
    }
}

fn rename_field(doc: &mut Map, from: &str, to: &str) -> HelperResult<()> {
    with_fields(doc, |fields| {
        if let Some(value) = fields.remove(from) {
            fields.insert(to.into(), value);
        }
        Ok(())
    })
}

fn drop_fields(doc: &mut Map, list: Array) -> HelperResult<()> {
    let names = names(list, "drop_fields")?;
    with_fields(doc, |fields| {
        for name in &names {
            fields.remove(name.as_str());
        }
        Ok(())
    })
}

fn keep_fields(doc: &mut Map, list: Array) -> HelperResult<()> {
    let names = names(list, "keep_fields")?;
    with_fields(doc, |fields| {
        fields.retain(|key, _| names.iter().any(|name| name == key.as_str()));
        Ok(())
    })
}

fn copy_field(doc: &mut Map, from: &str, to: &str) -> HelperResult<()> {
    with_fields(doc, |fields| {
        if let Some(value) = fields.get(from).cloned() {
            fields.insert(to.into(), value);
        }
        Ok(())
    })
}

fn field_is_missing(doc: &mut Map, name: &str) -> HelperResult<bool> {
    Ok(lookup(doc, name)?.is_none())
}

fn field_is_empty(doc: &mut Map, name: &str) -> HelperResult<bool> {
    Ok(match lookup(doc, name)? {
        None => true,
        Some(v) if v.is_unit() => true,
        Some(v) if v.is_string() => v
            .into_string()
            .map(|s| s.trim().is_empty())
            .unwrap_or(false),
        Some(v) if v.is_array() => v.into_array().map(|a| a.is_empty()).unwrap_or(false),
        Some(v) if v.is_map() => v.read_lock::<Map>().is_some_and(|m| m.is_empty()),
        Some(_) => false,
    })
}

/// JSON equality, type-sensitive (`"1"` does not equal `1`). A missing field compares
/// as `null`.
fn field_equals(doc: &mut Map, name: &str, expected: Dynamic) -> HelperResult<bool> {
    let actual = lookup(doc, name)?.unwrap_or(Dynamic::UNIT);
    let actual: serde_json::Value = rhai::serde::from_dynamic(&actual)?;
    let expected: serde_json::Value = rhai::serde::from_dynamic(&expected)?;
    Ok(actual == expected)
}
