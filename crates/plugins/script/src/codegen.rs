//! `ops` → rhai source, one line per operation.
//!
//! Each operation calls a helper from [`crate::helpers`]. The only config data that
//! reaches the source goes through [`string_literal`] / [`value_literal`], so no field
//! name or value can be parsed as code. A `set` expression is the one place authored
//! code appears: it was compiled on its own by [`crate::ops::check_ops`], and it sits on
//! its own lines so a trailing `//` comment cannot swallow the closing parenthesis.

use std::fmt::Write as _;

use serde_json::{Number, Value};

use crate::ops::{CastTarget, DropIf, Op, Presence, PromoteTarget};

#[allow(dead_code)] // Task 7 wires this into `Config`; only tests call it for now.
pub(crate) fn generate(ops: &[Op]) -> String {
    let mut out = String::new();
    for op in ops {
        emit(&mut out, op);
    }
    out
}

fn emit(out: &mut String, op: &Op) {
    let s = string_literal;
    let line = match op {
        Op::Rename(ft) => format!("doc.rename_field({}, {});", s(&ft.from), s(&ft.to)),
        Op::Drop(names) => format!("doc.drop_fields({});", list(names)),
        Op::Keep(names) => format!("doc.keep_fields({});", list(names)),
        Op::Copy(ft) => format!("doc.copy_field({}, {});", s(&ft.from), s(&ft.to)),
        Op::Set(set) => format!("doc.fields[{}] = (\n{}\n);", s(&set.field), set.expr),
        Op::Filter(filter) => format!("if {} {{ return false; }}", condition(&filter.drop_if)),
        Op::Trim(names) => format!("doc.trim_fields({});", list(names)),
        Op::Lowercase(names) => format!("doc.lowercase_fields({});", list(names)),
        Op::Uppercase(names) => format!("doc.uppercase_fields({});", list(names)),
        Op::Split(sep) => format!("doc.split_field({}, {});", s(&sep.field), s(&sep.separator)),
        Op::Join(sep) => format!("doc.join_field({}, {});", s(&sep.field), s(&sep.separator)),
        Op::Promote(p) => {
            let target = match p.to {
                PromoteTarget::Title => "title",
                PromoteTarget::Content => "content",
            };
            format!("doc.promote_field({}, {});", s(&p.field), s(target))
        }
        Op::Round(r) => format!("doc.round_field({}, {});", s(&r.field), r.decimals),
        Op::Cast(c) => {
            let target = match c.to {
                CastTarget::Number => "number",
                CastTarget::String => "string",
                CastTarget::Boolean => "boolean",
            };
            format!("doc.cast_field({}, {});", s(&c.field), s(target))
        }
        Op::Default(d) => format!("doc.default_field({}, {});", s(&d.field), value_literal(&d.value)),
    };
    out.push_str(&line);
    out.push('\n');
}

fn condition(d: &DropIf) -> String {
    let field = string_literal(&d.field);
    if let Some(presence) = d.is {
        return match presence {
            Presence::Empty => format!("doc.field_is_empty({field})"),
            Presence::Missing => format!("doc.field_is_missing({field})"),
        };
    }
    if let Some(value) = &d.equals {
        return format!("doc.field_equals({field}, {})", value_literal(value));
    }
    if let Some(value) = &d.not_equals {
        return format!("!doc.field_equals({field}, {})", value_literal(value));
    }
    // Unreachable for validated ops: `check_ops` requires exactly one test.
    "false".into()
}

fn list(names: &[String]) -> String {
    let items: Vec<String> = names.iter().map(|n| string_literal(n)).collect();
    format!("[{}]", items.join(", "))
}

/// A double-quoted rhai string. Every character outside printable ASCII, and `"` and
/// `\`, is written as `\U` + 8 hex digits — rhai's only escape wide enough for any
/// Unicode scalar (it has no `\u{..}` form). Double-quoted strings do not interpolate.
#[allow(dead_code)] // Task 7 wires this into `Config`; only tests call it for now.
pub(crate) fn string_literal(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        if (' '..='~').contains(&c) && c != '"' && c != '\\' {
            out.push(c);
        } else {
            let _ = write!(out, "\\U{:08X}", c as u32);
        }
    }
    out.push('"');
    out
}

/// A rhai literal for a JSON value: `()`, booleans, integers, floats, strings, arrays
/// and `#{ "key": value }` maps.
#[allow(dead_code)] // Task 7 wires this into `Config`; only tests call it for now.
pub(crate) fn value_literal(v: &Value) -> String {
    match v {
        Value::Null => "()".into(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => number_literal(n),
        Value::String(s) => string_literal(s),
        Value::Array(items) => {
            let items: Vec<String> = items.iter().map(value_literal).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Object(map) => {
            let entries: Vec<String> = map
                .iter()
                .map(|(k, v)| format!("{}: {}", string_literal(k), value_literal(v)))
                .collect();
            format!("#{{{}}}", entries.join(", "))
        }
    }
}

fn number_literal(n: &Number) -> String {
    if let Some(i) = n.as_i64() {
        return i.to_string();
    }
    // Integers above i64::MAX are rejected by `check_ops`, so this is a float.
    let text = format!("{:?}", n.as_f64().unwrap_or(0.0));
    if text.contains('.') || !text.contains('e') { text } else { text.replacen('e', ".0e", 1) }
}
