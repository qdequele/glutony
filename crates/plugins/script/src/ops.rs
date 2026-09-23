//! The builder's operations: the declarative `ops` config and its validation.

use rhai::Engine;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::diagnostic::Diagnostic;

/// One operation. Serialized as a single-key map: `{ "rename": { "from": .., "to": .. } }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Op {
    Rename(FromTo),
    Drop(Vec<String>),
    Keep(Vec<String>),
    Copy(FromTo),
    Set(SetOp),
    Filter(FilterOp),
    Trim(Vec<String>),
    Lowercase(Vec<String>),
    Uppercase(Vec<String>),
    Split(SeparatorOp),
    Join(SeparatorOp),
    Promote(PromoteOp),
    Round(RoundOp),
    Cast(CastOp),
    Default(DefaultOp),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FromTo {
    pub from: String,
    pub to: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SetOp {
    pub field: String,
    pub expr: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FilterOp {
    pub drop_if: DropIf,
}

/// Exactly one of `is`, `equals`, `not_equals` is set (checked by [`check_ops`]).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DropIf {
    pub field: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub is: Option<Presence>,
    #[serde(default, deserialize_with = "present", skip_serializing_if = "Option::is_none")]
    pub equals: Option<Value>,
    #[serde(default, deserialize_with = "present", skip_serializing_if = "Option::is_none")]
    pub not_equals: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Presence {
    Empty,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeparatorOp {
    pub field: String,
    pub separator: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PromoteOp {
    pub field: String,
    pub to: PromoteTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum PromoteTarget {
    Title,
    Content,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RoundOp {
    pub field: String,
    pub decimals: u32,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CastOp {
    pub field: String,
    pub to: CastTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum CastTarget {
    Number,
    String,
    Boolean,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DefaultOp {
    pub field: String,
    pub value: Value,
}

/// Keep a present-but-`null` key distinguishable from an absent one.
fn present<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Value>, D::Error> {
    Value::deserialize(d).map(Some)
}

/// Parse and validate every operation, collecting all problems.
///
/// Each operation is deserialized on its own so a problem names the operation it is in,
/// and `set` expressions are compiled with `compile_expression`, which rejects
/// statements, `let` and loops outright.
#[allow(dead_code)] // Task 7 wires this into `Config`; only tests call it for now.
pub(crate) fn check_ops(raw: &[Value], engine: &Engine) -> Result<Vec<Op>, Vec<Diagnostic>> {
    let mut ops = Vec::with_capacity(raw.len());
    let mut diagnostics = Vec::new();
    for (i, value) in raw.iter().enumerate() {
        match serde_json::from_value::<Op>(value.clone()) {
            Ok(op) => {
                validate_op(i, &op, engine, &mut diagnostics);
                ops.push(op);
            }
            Err(e) => diagnostics.push(Diagnostic::at(format!("ops[{i}]"), e.to_string())),
        }
    }
    if diagnostics.is_empty() { Ok(ops) } else { Err(diagnostics) }
}

fn validate_op(i: usize, op: &Op, engine: &Engine, out: &mut Vec<Diagnostic>) {
    let at = |key: &str| format!("ops[{i}].{key}");
    match op {
        Op::Rename(ft) | Op::Copy(ft) => {
            require_name(out, at("from"), &ft.from);
            require_name(out, at("to"), &ft.to);
        }
        Op::Drop(names) | Op::Keep(names) | Op::Trim(names) | Op::Lowercase(names) | Op::Uppercase(names) => {
            require_names(out, format!("ops[{i}]"), names);
        }
        Op::Set(set) => {
            require_name(out, at("field"), &set.field);
            check_expr(out, at("expr"), &set.expr, engine);
        }
        Op::Filter(filter) => {
            let d = &filter.drop_if;
            require_name(out, at("drop_if.field"), &d.field);
            let tests = [d.is.is_some(), d.equals.is_some(), d.not_equals.is_some()];
            if tests.iter().filter(|set| **set).count() != 1 {
                out.push(Diagnostic::at(at("drop_if"), "set exactly one of `is`, `equals` or `not_equals`"));
            }
            for value in [&d.equals, &d.not_equals].into_iter().flatten() {
                check_literal(out, at("drop_if"), value);
            }
        }
        Op::Split(sep) | Op::Join(sep) => {
            require_name(out, at("field"), &sep.field);
            if sep.separator.is_empty() {
                out.push(Diagnostic::at(at("separator"), "separator must not be empty"));
            }
        }
        Op::Promote(p) => require_name(out, at("field"), &p.field),
        Op::Round(r) => {
            require_name(out, at("field"), &r.field);
            if r.decimals > 15 {
                out.push(Diagnostic::at(at("decimals"), "decimals must be between 0 and 15"));
            }
        }
        Op::Cast(c) => require_name(out, at("field"), &c.field),
        Op::Default(d) => {
            require_name(out, at("field"), &d.field);
            check_literal(out, at("value"), &d.value);
        }
    }
}

fn require_name(out: &mut Vec<Diagnostic>, path: String, name: &str) {
    if name.trim().is_empty() {
        out.push(Diagnostic::at(path, "field name must not be empty"));
    }
}

fn require_names(out: &mut Vec<Diagnostic>, path: String, names: &[String]) {
    if names.is_empty() {
        out.push(Diagnostic::at(path.clone(), "must name at least one field"));
    }
    for (j, name) in names.iter().enumerate() {
        require_name(out, format!("{path}[{j}]"), name);
    }
}

fn check_expr(out: &mut Vec<Diagnostic>, path: String, expr: &str, engine: &Engine) {
    if expr.trim().is_empty() {
        out.push(Diagnostic::at(path, "expression must not be empty"));
        return;
    }
    if let Err(e) = engine.compile_expression(expr) {
        out.push(Diagnostic::from_parse_error(path, &e));
    }
}

/// rhai integers are 64-bit signed; a JSON integer above `i64::MAX` cannot be a literal.
fn check_literal(out: &mut Vec<Diagnostic>, path: String, value: &Value) {
    match value {
        Value::Number(n) if n.is_u64() && n.as_i64().is_none() => {
            out.push(Diagnostic::at(path, format!("{n} is too large; integers must fit in 64 bits")));
        }
        Value::Array(items) => items.iter().for_each(|v| check_literal(out, path.clone(), v)),
        Value::Object(map) => map.values().for_each(|v| check_literal(out, path.clone(), v)),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runner::sandboxed_engine;
    use serde_json::json;

    fn check(raw: Value) -> Result<Vec<Op>, Vec<Diagnostic>> {
        check_ops(raw.as_array().unwrap(), &sandboxed_engine(100_000))
    }

    fn paths(diags: &[Diagnostic]) -> Vec<&str> {
        diags.iter().map(|d| d.path.as_str()).collect()
    }

    #[test]
    fn every_operation_parses() {
        let ops = check(json!([
            {"rename": {"from": "a", "to": "b"}},
            {"drop": ["a"]},
            {"keep": ["a"]},
            {"copy": {"from": "a", "to": "b"}},
            {"set": {"field": "t", "expr": "price * qty"}},
            {"filter": {"drop_if": {"field": "content", "is": "empty"}}},
            {"trim": ["a"]},
            {"lowercase": ["a"]},
            {"uppercase": ["a"]},
            {"split": {"field": "a", "separator": ","}},
            {"join": {"field": "a", "separator": ","}},
            {"promote": {"field": "a", "to": "title"}},
            {"round": {"field": "a", "decimals": 2}},
            {"cast": {"field": "a", "to": "number"}},
            {"default": {"field": "a", "value": "EUR"}}
        ]))
        .unwrap();
        assert_eq!(ops.len(), 15);
    }

    #[test]
    fn an_unknown_operation_names_its_index() {
        let diags = check(json!([{"drop": ["a"]}, {"explode": {}}])).unwrap_err();
        assert_eq!(paths(&diags), ["ops[1]"]);
        assert!(diags[0].message.contains("unknown variant"), "{}", diags[0].message);
    }

    #[test]
    fn an_unknown_key_inside_an_operation_is_rejected() {
        let diags = check(json!([{"rename": {"from": "a", "to": "b", "extra": 1}}])).unwrap_err();
        assert_eq!(paths(&diags), ["ops[0]"]);
    }

    #[test]
    fn equals_null_is_distinct_from_absent() {
        let ops = check(json!([{"filter": {"drop_if": {"field": "a", "equals": null}}}])).unwrap();
        let Op::Filter(f) = &ops[0] else { panic!() };
        assert_eq!(f.drop_if.equals, Some(Value::Null));
    }

    #[test]
    fn a_filter_needs_exactly_one_test() {
        let diags = check(json!([
            {"filter": {"drop_if": {"field": "a"}}},
            {"filter": {"drop_if": {"field": "a", "is": "empty", "equals": 1}}}
        ]))
        .unwrap_err();
        assert_eq!(paths(&diags), ["ops[0].drop_if", "ops[1].drop_if"]);
    }

    #[test]
    fn empty_names_lists_separators_and_bad_decimals_are_reported() {
        let diags = check(json!([
            {"rename": {"from": "", "to": "b"}},
            {"drop": []},
            {"trim": ["ok", " "]},
            {"split": {"field": "a", "separator": ""}},
            {"round": {"field": "a", "decimals": 16}}
        ]))
        .unwrap_err();
        assert_eq!(
            paths(&diags),
            ["ops[0].from", "ops[1]", "ops[2][1]", "ops[3].separator", "ops[4].decimals"]
        );
    }

    #[test]
    fn set_expressions_cannot_hold_statements() {
        let diags = check(json!([
            {"set": {"field": "t", "expr": "let x = 1"}},
            {"set": {"field": "t", "expr": "loop { }"}},
            {"set": {"field": "t", "expr": "a; b"}},
            {"set": {"field": "t", "expr": "price * qty"}}
        ]))
        .unwrap_err();
        assert_eq!(paths(&diags), ["ops[0].expr", "ops[1].expr", "ops[2].expr"]);
    }

    #[test]
    fn a_syntax_error_in_an_expression_has_a_position() {
        let diags = check(json!([{"set": {"field": "t", "expr": "1 +"}}])).unwrap_err();
        assert_eq!(diags[0].line, Some(1));
        assert!(diags[0].column.is_some());
    }

    #[test]
    fn an_integer_beyond_64_bits_is_reported() {
        let diags = check(json!([{"default": {"field": "a", "value": 18446744073709551615u64}}])).unwrap_err();
        assert_eq!(paths(&diags), ["ops[0].value"]);
    }
}
