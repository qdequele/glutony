//! Document helpers registered on every engine.
//!
//! They are the operations the builder offers, callable as methods on `doc` from Code
//! mode too (`doc.rename_field("prix", "price")`); `ops` compile to one call per
//! operation. One rule throughout: a **missing** field is a no-op, a field that is
//! **present with the wrong type** is an error, handled by the step's `on_error`.
//! `default_field` and the `field_is_*` / `field_equals` tests are where absence is
//! the point.

use rhai::{Array, Dynamic, Engine, EvalAltResult, FLOAT, INT, Map};

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
    engine.register_fn("trim_fields", trim_fields);
    engine.register_fn("lowercase_fields", lowercase_fields);
    engine.register_fn("uppercase_fields", uppercase_fields);
    engine.register_fn("split_field", split_field);
    engine.register_fn("join_field", join_field);
    engine.register_fn("promote_field", promote_field);
    engine.register_fn("round_field", round_field);
    engine.register_fn("cast_field", cast_field);
    engine.register_fn("default_field", default_field);
    // rhai's own `round` takes one float; these add the decimals the docs promise.
    engine.register_fn("round", round_to);
    engine.register_fn("round", |x: INT, decimals: INT| -> HelperResult<INT> {
        round_to(0.0, decimals)?;
        Ok(x)
    });
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

/// Replace each listed string field with `f(value)`.
fn map_strings(doc: &mut Map, list: Array, helper: &str, f: impl Fn(&str) -> String) -> HelperResult<()> {
    let names = names(list, helper)?;
    with_fields(doc, |fields| {
        for name in &names {
            let Some(value) = fields.get_mut(name.as_str()) else { continue };
            let text = match value.clone().into_string() {
                Ok(text) => text,
                Err(type_name) => return fail(format!("{helper}: `{name}` is {type_name}, not a string")),
            };
            *value = Dynamic::from(f(&text));
        }
        Ok(())
    })
}

fn trim_fields(doc: &mut Map, list: Array) -> HelperResult<()> {
    map_strings(doc, list, "trim_fields", |s| s.trim().to_string())
}

fn lowercase_fields(doc: &mut Map, list: Array) -> HelperResult<()> {
    map_strings(doc, list, "lowercase_fields", str::to_lowercase)
}

fn uppercase_fields(doc: &mut Map, list: Array) -> HelperResult<()> {
    map_strings(doc, list, "uppercase_fields", str::to_uppercase)
}

fn split_field(doc: &mut Map, name: &str, separator: &str) -> HelperResult<()> {
    if separator.is_empty() {
        return fail("split_field: separator must not be empty".into());
    }
    with_fields(doc, |fields| {
        let Some(value) = fields.get_mut(name) else { return Ok(()) };
        let text = match value.clone().into_string() {
            Ok(text) => text,
            Err(type_name) => return fail(format!("split_field: `{name}` is {type_name}, not a string")),
        };
        *value = Dynamic::from_array(text.split(separator).map(|part| Dynamic::from(part.to_string())).collect());
        Ok(())
    })
}

fn join_field(doc: &mut Map, name: &str, separator: &str) -> HelperResult<()> {
    with_fields(doc, |fields| {
        let Some(value) = fields.get_mut(name) else { return Ok(()) };
        let items = match value.clone().into_array() {
            Ok(items) => items,
            Err(type_name) => return fail(format!("join_field: `{name}` is {type_name}, not an array")),
        };
        let parts = items
            .iter()
            .map(|item| {
                if item.is_string() || item.is_int() || item.is_float() || item.is_bool() {
                    Ok(item.to_string())
                } else {
                    fail(format!("join_field: `{name}` holds {}, which cannot be joined", item.type_name()))
                }
            })
            .collect::<HelperResult<Vec<_>>>()?;
        *value = Dynamic::from(parts.join(separator));
        Ok(())
    })
}

/// Move a string field into the document's `title` or `content`.
fn promote_field(doc: &mut Map, name: &str, target: &str) -> HelperResult<()> {
    if target != "title" && target != "content" {
        return fail(format!("promote_field: target must be \"title\" or \"content\", got {target:?}"));
    }
    let moved = with_fields(doc, |fields| {
        let shape = fields.get(name).map(|v| (v.is_string(), v.type_name()));
        match shape {
            None => Ok(None),
            Some((true, _)) => Ok(fields.remove(name)),
            Some((false, type_name)) => fail(format!("promote_field: `{name}` is {type_name}, not a string")),
        }
    })?;
    if let Some(value) = moved {
        doc.insert(target.into(), value);
    }
    Ok(())
}

pub(crate) fn round_to(x: FLOAT, decimals: INT) -> HelperResult<FLOAT> {
    if !(0..=15).contains(&decimals) {
        return fail(format!("round: decimals must be between 0 and 15, got {decimals}"));
    }
    let factor = 10f64.powi(decimals as i32);
    Ok((x * factor).round() / factor)
}

fn round_field(doc: &mut Map, name: &str, decimals: INT) -> HelperResult<()> {
    round_to(0.0, decimals)?; // validate decimals even when the value is an integer
    with_fields(doc, |fields| {
        let Some(value) = fields.get_mut(name) else { return Ok(()) };
        if value.is_int() {
            return Ok(());
        }
        let Ok(x) = value.as_float() else {
            return fail(format!("round_field: `{name}` is {}, not a number", value.type_name()));
        };
        *value = Dynamic::from_float(round_to(x, decimals)?);
        Ok(())
    })
}

fn cast_field(doc: &mut Map, name: &str, to: &str) -> HelperResult<()> {
    with_fields(doc, |fields| {
        let Some(value) = fields.get_mut(name) else { return Ok(()) };
        let cast = match to {
            "number" => cast_number(value),
            "string" => cast_string(value),
            "boolean" => cast_boolean(value),
            other => return fail(format!("cast_field: unknown target {other:?}")),
        };
        match cast {
            Ok(next) => {
                *value = next;
                Ok(())
            }
            Err(why) => fail(format!("cast_field: `{name}` {why}")),
        }
    })
}

fn cast_number(v: &Dynamic) -> Result<Dynamic, String> {
    if v.is_int() || v.is_float() {
        return Ok(v.clone());
    }
    if let Ok(b) = v.as_bool() {
        return Ok(Dynamic::from_int(INT::from(b)));
    }
    if v.is_string() {
        let text = v.clone().into_string().unwrap_or_default();
        let trimmed = text.trim();
        if let Ok(i) = trimmed.parse::<INT>() {
            return Ok(Dynamic::from_int(i));
        }
        if let Ok(f) = trimmed.parse::<FLOAT>()
            && f.is_finite()
        {
            return Ok(Dynamic::from_float(f));
        }
        return Err(format!("{text:?} is not a number"));
    }
    Err(format!("is {}, which cannot become a number", v.type_name()))
}

fn cast_string(v: &Dynamic) -> Result<Dynamic, String> {
    if v.is_string() {
        return Ok(v.clone());
    }
    if v.is_int() || v.is_float() || v.is_bool() {
        return Ok(Dynamic::from(v.to_string()));
    }
    Err(format!("is {}, which cannot become a string", v.type_name()))
}

fn cast_boolean(v: &Dynamic) -> Result<Dynamic, String> {
    if v.is_bool() {
        return Ok(v.clone());
    }
    if let Ok(i) = v.as_int() {
        return match i {
            0 => Ok(false.into()),
            1 => Ok(true.into()),
            _ => Err(format!("{i} is not 0 or 1")),
        };
    }
    if v.is_string() {
        let text = v.clone().into_string().unwrap_or_default();
        return match text.trim().to_ascii_lowercase().as_str() {
            "true" => Ok(true.into()),
            "false" => Ok(false.into()),
            _ => Err(format!("{text:?} is not true or false")),
        };
    }
    Err(format!("is {}, which cannot become a boolean", v.type_name()))
}

fn default_field(doc: &mut Map, name: &str, value: Dynamic) -> HelperResult<()> {
    with_fields(doc, |fields| {
        if fields.get(name).is_none_or(Dynamic::is_unit) {
            fields.insert(name.into(), value);
        }
        Ok(())
    })
}
