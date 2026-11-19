//! Bare field names: a name that is not a variable resolves to `doc.fields[name]`.
//!
//! Installed on every engine, so `price * qty` works in `set` expressions and in Code
//! mode alike. A variable always wins: rhai consults the resolver *before* it searches
//! the scope (`rhai-1.26.1/src/eval/expr.rs`), so the resolver steps aside for any name
//! the scope already holds — otherwise a field would shadow the author's `let`.

use rhai::{Engine, Map};

#[allow(deprecated)] // `on_var` is marked volatile, not deprecated; rhai is pinned exactly.
pub(crate) fn install_field_resolver(engine: &mut Engine) {
    engine.on_var(|name, _index, context| {
        let scope = context.scope();
        if scope.contains(name) {
            return Ok(None);
        }
        let Some(doc) = scope.get("doc") else {
            return Ok(None);
        };
        let Some(doc) = doc.read_lock::<Map>() else {
            return Ok(None);
        };
        let Some(fields) = doc.get("fields").and_then(|f| f.read_lock::<Map>()) else {
            return Ok(None);
        };
        Ok(fields.get(name).cloned())
    });
}
