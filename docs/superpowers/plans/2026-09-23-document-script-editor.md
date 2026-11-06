# Document Script Editor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the `document_script` action authorable in the admin UI — a Builder backed by a declarative `ops` config, a Code editor for free-form rhai, a reference that doubles as the builder's palette, and line-precise compile diagnostics before save.

**Architecture:** The plugin learns `ops` (exactly one of `script`/`ops`), whose semantics live in Rust helper methods registered on the rhai engine (`doc.rename_field(..)`); `ops` compile to one readable line per operation calling those helpers, so both modes share one sandbox. A pure `check_config` compiles without executing; the control plane runs it inside the existing `/pipelines/validate` + `POST /pipelines` path and returns `invalid_step_config` diagnostics, which the gateway now forwards intact. The UI plugs a dedicated editor into `StepCard` through a registry keyed by plugin name.

**Tech Stack:** Rust (rhai 1.26.1, serde, axum, thiserror), Next.js App Router static export, TypeScript strict, shadcn/ui, CodeMirror 6, vitest.

**Spec:** `docs/superpowers/specs/2026-09-23-document-script-editor-design.md`

## Global Constraints

- rhai is pinned to exactly `=1.26.1`, features `["sync", "serde", "no_module", "no_custom_syntax", "no_time"]`.
- A step config holds **exactly one** of `script` / `ops`; both or neither is `InvalidConfig`.
- `script` is limited to **65536 bytes** (64 KiB), enforced in `Config::parse`.
- A missing field is a no-op; a present field of the wrong type is a per-document error handled by `on_error`.
- **A variable in scope always wins over a field** in bare-name resolution.
- Nothing before a job runs may *execute* a script — the pre-run check parses and compiles only.
- New error code is exactly `invalid_step_config`; 422 body `{ error, code, diagnostics: [{ step, path, message, line?, column? }] }`; `line`/`column` are 1-based.
- `/pipelines/validate` 200 body gains `generated_scripts: { <step id>: <rhai> }` for Builder-mode steps only.
- UI: TypeScript strict, never `any`; shadcn/ui components; lucide-react icons; `cn()` for class merging; co-locate under `ui/src/app/pipelines/_components/`.
- CodeMirror 6 packages only (`@codemirror/state`, `view`, `language`, `commands`, `lint`) — no React wrapper, no Monaco.
- Commits: conventional messages, **no `Co-Authored-By` lines** (user's global CLAUDE.md).

### Refinements to the spec discovered while planning

These were found by reading rhai's source and the codebase; the plan implements them and Task 8 folds them into the spec.

1. **Escaper syntax.** rhai has no `\u{..}` escape — only fixed-width `\xHH`, `\uHHHH`, `\UHHHHHHHH` (`rhai-1.26.1/src/tokenizer.rs:1510`). The escaper writes `\U` + 8 uppercase hex digits.
2. **`round(x, decimals)`.** rhai's built-in `round` takes one `FLOAT` (`math_basic.rs:257`); `round(x, 2)` compiles but fails at runtime. The engine registers `round(FLOAT, INT)` and `round(INT, INT)`.
3. **Operation semantics live in Rust helpers** registered as methods on `doc` (`doc.rename_field("a", "b")`). Generated code is one call per operation; the helpers also work in Code mode.
4. **`filter` addresses `id`, `title`, `content`** as the document's own attributes; every other name is a field. Without this the spec's own example (`drop_if: { field: content, is: empty }`) tests a field, not the document's text.
5. **`equals` / `not_equals`:** a missing field compares as `null`.
6. **Reference examples are executed**, not only compiled: each entry carries an input document and the expected output, and a Rust test runs both the snippet and the op on it. Compiling alone would not have caught (2).
7. **No `omit` prop on `SchemaForm`.** `StepCard` strips the editor's owned keys from both schema and config before calling it — this also covers `SchemaForm`'s raw-JSON fallback (no manifest yet), which an `omit` prop would miss.
8. **PR #3 doc bug:** `to_upper()` / `to_lower()` return a new string; only `trim`, `replace`, `pad`, `truncate`, `clear`, `make_upper`, `make_lower` edit in place. `docs/plugins/built-in.mdx` and the crate's module doc currently say otherwise.

## File Structure

**`crates/plugins/script/src/`** — split the current 890-line `lib.rs` by responsibility:

| File | Responsibility |
|---|---|
| `lib.rs` | `NAME`, `DocumentScriptPlugin`, manifest, `execute`, module wiring, public re-exports, execute-level tests |
| `config.rs` | `OnError`, `Source`, `Config::{check, parse}`, limits |
| `diagnostic.rs` | `Diagnostic` (public) |
| `ops.rs` | `Op` and its payload types, `check_ops` |
| `codegen.rs` | `generate`, `string_literal`, `value_literal` |
| `helpers.rs` | document helper methods + `round` overloads registered on the engine |
| `resolver.rs` | bare-field-name resolver |
| `runner.rs` | `sandboxed_engine`, `Runner` (deadline, run) |
| `convert.rs` | document ↔ rhai map, non-finite guard |
| `check.rs` | public `check_config`, `CheckResult` |
| `tests/reference.rs` | executes every `reference.json` example |

**Control plane / gateway:** `crates/control-plane/src/{error.rs,pipelines.rs}`, `crates/control-plane/tests/routes.rs`, `crates/gateway/src/{error.rs,state.rs,handlers/pipelines.rs}`, `docs/openapi.yaml`.

**UI** — `ui/src/lib/api/{types.ts,client.ts,diagnostics.ts}`, `ui/src/lib/schema-form/{fields.ts,schema-form.tsx}`, `ui/src/app/pipelines/_components/{step-card.tsx,pipeline-editor.tsx,config-editors.ts,config-editor-registry.tsx}`, and a new folder `ui/src/app/pipelines/_components/document-script/`:

| File | Responsibility |
|---|---|
| `ops.ts` | `Op` types, `OP_KINDS`, `opKind`, `newOp`, literal parse/format |
| `rows.ts` | read/write/add/move/remove/replace operations in a config |
| `mode.ts` | mode derivation and switch transitions |
| `diagnostics.ts` | route a diagnostic to a row, expression or code line; per-step check state |
| `generate-script.ts` | single-step validation that returns the generated script |
| `reference.json` / `reference.ts` | the reference data, types, filtering |
| `rhai-language.ts` | CodeMirror stream parser for rhai |
| `lint.ts` | 1-based line/column → document offsets |
| `script-code-editor.tsx` | CodeMirror component with an `insert` handle |
| `op-row.tsx` / `ops-builder.tsx` | Builder rows and the list with the Add menu |
| `script-reference.tsx` | the reference panel |
| `document-script-editor.tsx` | toggle, status, switch dialogs, sheet, expand |

---

## Phase 1 — Plugin crate

### Task 1: Split `lib.rs` into modules (pure refactor)

**Files:**
- Create: `crates/plugins/script/src/config.rs`, `convert.rs`, `runner.rs`
- Modify: `crates/plugins/script/src/lib.rs`

**Interfaces:**
- Produces: `crate::config::{Config, OnError}` (`pub(crate)`), `crate::runner::{Runner, Verdict}` (`pub(crate)`, `Runner.deadline` is `pub(crate)`), `crate::convert::{to_scope_map, from_scope_map}` (`pub(crate)`). No behaviour change.

- [ ] **Step 1: Confirm the suite is green before moving anything**

Run: `cargo test -p meili-ingest-plugin-script`
Expected: `test result: ok. 27 passed`

- [ ] **Step 2: Move items verbatim**

Move these items out of `lib.rs` **unchanged except for visibility** (`fn`/`struct`/`enum` → `pub(crate)`; struct fields used across modules → `pub(crate)`):

- `config.rs`: `OnError`, `Config`, `default_max_ops`, `default_timeout_ms`, `impl Config`. Header:
  ```rust
  //! Step configuration: parsing and validation of the `config:` block.

  use meili_ingest_plugin_sdk::PluginError;
  use serde::Deserialize;
  use serde_json::Value;

  use crate::NAME;
  ```
- `convert.rs`: `to_scope_map`, `json_to_dynamic`, `reject_non_finite`, `dynamic_to_json`, `from_scope_map`. Header:
  ```rust
  //! Document ↔ rhai map conversion, including the non-finite float guard.

  use meili_ingest_plugin_sdk::{Document, PluginError, sanitize_id};
  use rhai::{Dynamic, Map};
  use serde_json::Value;

  use crate::NAME;
  ```
- `runner.rs`: the constants `MAX_EXPR_DEPTH`, `MAX_CALL_LEVELS`, `MAX_STRING_SIZE`, `MAX_ARRAY_SIZE`, `MAX_MAP_SIZE`, `PROGRESS_EVERY`, then `Runner`, `impl Runner`, `Verdict`. Header:
  ```rust
  //! The sandboxed engine and the per-step runner.

  use std::sync::Arc;
  use std::sync::atomic::{AtomicU64, Ordering};
  use std::time::Instant;

  use meili_ingest_plugin_sdk::{Document, PluginError};
  use rhai::{AST, Dynamic, Engine, Map, Scope};

  use crate::NAME;
  use crate::config::Config;
  use crate::convert::{from_scope_map, to_scope_map};
  ```

- [ ] **Step 3: Wire the modules in `lib.rs`**

Replace the `use` block of `lib.rs` (keep the module doc comment above it) with:

```rust
mod config;
mod convert;
mod runner;

use meili_ingest_plugin_sdk::prelude::*;

use crate::config::{Config, OnError};
use crate::runner::{Runner, Verdict};
```

In the `#[cfg(test)] mod tests` block, add below `use super::*;`:

```rust
    use crate::config::Config;
    use crate::runner::Runner;
    use std::sync::atomic::Ordering;
```

- [ ] **Step 4: Verify nothing changed**

Run: `cargo test -p meili-ingest-plugin-script && cargo clippy -p meili-ingest-plugin-script --all-targets -- -D warnings`
Expected: `27 passed`, no clippy warnings.

- [ ] **Step 5: Commit**

```bash
git add crates/plugins/script/src
git commit -m "refactor(script): split the plugin into focused modules"
```

---

### Task 2: Pin rhai and resolve bare field names

**Files:**
- Create: `crates/plugins/script/src/resolver.rs`
- Modify: `Cargo.toml` (workspace `rhai` line), `crates/plugins/script/src/runner.rs`, `crates/plugins/script/src/lib.rs` (tests + `mod resolver;`)

**Interfaces:**
- Produces: `crate::runner::sandboxed_engine(max_ops: u64) -> rhai::Engine` (`pub(crate)`) — every sandbox limit + resolver (later tasks add helpers). `Runner::new` builds on it.

- [ ] **Step 1: Write the failing tests** (in `lib.rs` tests)

```rust
    #[tokio::test]
    async fn a_bare_name_reads_the_field() {
        let mut d = doc("a", "x");
        d.fields.insert("price".into(), json!(10));
        d.fields.insert("qty".into(), json!(3));

        let out = run(vec![d], script("doc.fields.total = price * qty;")).await;

        assert_eq!(out[0].fields.get("total"), Some(&json!(30)));
    }

    /// Rhai asks the resolver *before* it searches the scope, so without the guard a
    /// field named `price` would silently replace the author's `let price`.
    #[tokio::test]
    async fn a_variable_wins_over_a_field_of_the_same_name() {
        let mut d = doc("a", "x");
        d.fields.insert("price".into(), json!(10));

        let out = run(vec![d], script("let price = 5; doc.fields.out = price;")).await;

        assert_eq!(out[0].fields.get("out"), Some(&json!(5)));
    }

    #[tokio::test]
    async fn a_name_that_is_neither_variable_nor_field_is_an_error() {
        let err = try_run(vec![doc("a", "x")], script("doc.fields.x = nope;"))
            .await
            .unwrap_err();

        assert!(
            matches!(&err, PluginError::NonRetryable(m) if m.contains("nope")),
            "got {err:?}"
        );
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script bare_name variable_wins neither`
Expected: `a_bare_name_reads_the_field` FAILS ("Variable not found: price"); the other two pass already — they are guards for Step 3.

- [ ] **Step 3: Pin rhai**

In the root `Cargo.toml`, change the `rhai` line to:

```toml
rhai = { version = "=1.26.1", features = ["sync", "serde", "no_module", "no_custom_syntax", "no_time"] }
```

and extend the comment above it with: `# Pinned exactly: the field resolver relies on Engine::on_var, which rhai marks volatile.`

- [ ] **Step 4: Implement the resolver**

`crates/plugins/script/src/resolver.rs`:

```rust
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
```

In `runner.rs`, add `use crate::resolver::install_field_resolver;` and this function, then make `Runner::new` start from it (replace its `Engine::new()` + six `set_max_*` lines with `let mut engine = sandboxed_engine(cfg.max_ops);`):

```rust
/// An engine with every sandbox limit and the field resolver installed — everything
/// but the per-document deadline, which only a [`Runner`] needs. Also used to compile
/// `set` expressions and whole configs, so they are checked under the limits they run
/// under.
pub(crate) fn sandboxed_engine(max_ops: u64) -> Engine {
    let mut engine = Engine::new();
    engine.set_max_operations(max_ops);
    engine.set_max_expr_depths(MAX_EXPR_DEPTH, MAX_EXPR_DEPTH);
    engine.set_max_call_levels(MAX_CALL_LEVELS);
    engine.set_max_string_size(MAX_STRING_SIZE);
    engine.set_max_array_size(MAX_ARRAY_SIZE);
    engine.set_max_map_size(MAX_MAP_SIZE);
    install_field_resolver(&mut engine);
    engine
}
```

Add `mod resolver;` to `lib.rs`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p meili-ingest-plugin-script`
Expected: all pass (30).

- [ ] **Step 6: Mutation-check the guard**

Temporarily delete the `if scope.contains(name) { return Ok(None); }` block; run `cargo test -p meili-ingest-plugin-script variable_wins`.
Expected: FAIL (`out` is 10). Restore the block and re-run: PASS.

- [ ] **Step 7: Commit**

```bash
git add Cargo.toml Cargo.lock crates/plugins/script/src
git commit -m "feat(script): resolve bare field names, pin rhai to 1.26.1"
```

---

### Task 3: Field and filter helpers

**Files:**
- Create: `crates/plugins/script/src/helpers.rs`
- Modify: `crates/plugins/script/src/runner.rs` (register), `lib.rs` (`mod helpers;` + tests)

**Interfaces:**
- Produces (rhai methods on `doc`): `rename_field(from, to)`, `drop_fields([..])`, `keep_fields([..])`, `copy_field(from, to)`, `field_is_missing(name) -> bool`, `field_is_empty(name) -> bool`, `field_equals(name, value) -> bool`. Rust: `crate::helpers::register(&mut Engine)`.

- [ ] **Step 1: Write the failing tests** (in `lib.rs` tests)

```rust
    fn with_fields(fields: Value) -> Document {
        let mut d = doc("a", "x");
        d.fields = fields.as_object().unwrap().clone();
        d
    }

    fn fields(d: &Document) -> Value {
        Value::Object(d.fields.clone())
    }

    #[tokio::test]
    async fn rename_field_moves_and_overwrites() {
        let out = run(
            vec![with_fields(json!({"prix": 10, "price": 1}))],
            script(r#"doc.rename_field("prix", "price");"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"price": 10}));
    }

    #[tokio::test]
    async fn drop_keep_and_copy_fields() {
        let out = run(
            vec![with_fields(json!({"a": 1, "b": 2, "c": 3}))],
            script(r#"doc.drop_fields(["a"]); doc.keep_fields(["b", "zzz"]); doc.copy_field("b", "d");"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"b": 2, "d": 2}));
    }

    #[tokio::test]
    async fn field_helpers_ignore_a_missing_field() {
        let out = run(
            vec![with_fields(json!({"a": 1}))],
            script(r#"doc.rename_field("nope", "x"); doc.copy_field("nope", "y"); doc.drop_fields(["nope"]);"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"a": 1}));
    }

    #[tokio::test]
    async fn field_names_must_be_strings() {
        let err = try_run(vec![with_fields(json!({}))], script("doc.drop_fields([1]);"))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::NonRetryable(m) if m.contains("must be strings")), "got {err:?}");
    }

    #[tokio::test]
    async fn field_is_empty_covers_missing_null_blank_and_empty_collections() {
        let docs = vec![
            with_fields(json!({})),
            with_fields(json!({"v": null})),
            with_fields(json!({"v": "   "})),
            with_fields(json!({"v": []})),
            with_fields(json!({"v": {}})),
            with_fields(json!({"v": "x"})),
            with_fields(json!({"v": 0})),
        ];
        let out = run(docs, script(r#"if doc.field_is_empty("v") { return false; } true"#)).await;
        assert_eq!(out.len(), 2, "only \"x\" and 0 are non-empty");
    }

    #[tokio::test]
    async fn field_is_missing_distinguishes_null_from_absent() {
        let out = run(
            vec![with_fields(json!({})), with_fields(json!({"v": null}))],
            script(r#"if doc.field_is_missing("v") { return false; } true"#),
        )
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(fields(&out[0]), json!({"v": null}));
    }

    #[tokio::test]
    async fn field_equals_is_type_sensitive_and_treats_missing_as_null() {
        let docs = vec![
            with_fields(json!({"s": "1"})),
            with_fields(json!({"s": 1})),
            with_fields(json!({})),
        ];
        let out = run(docs, script(r#"if doc.field_equals("s", 1) { return false; } true"#)).await;
        assert_eq!(out.len(), 2, "only the integer 1 equals 1");

        let out = run(
            vec![with_fields(json!({})), with_fields(json!({"s": "x"}))],
            script(r#"if doc.field_equals("s", ()) { return false; } true"#),
        )
        .await;
        assert_eq!(out.len(), 1, "a missing field equals null");
    }

    #[tokio::test]
    async fn filter_helpers_read_id_title_and_content_from_the_document() {
        let mut d = with_fields(json!({"content": "a field, not the text"}));
        d.content = "   ".into();
        let out = run(vec![d], script(r#"if doc.field_is_empty("content") { return false; } true"#)).await;
        assert!(out.is_empty(), "`content` means the document's own text");

        let out = run(vec![doc("a", "x")], script(r#"if doc.field_is_missing("title") { return false; } true"#)).await;
        assert!(out.is_empty(), "a document without a title is missing one");
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script field`
Expected: FAIL with "Function not found: rename_field" (and similar).

- [ ] **Step 3: Implement**

`crates/plugins/script/src/helpers.rs`:

```rust
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
    f(&mut *fields)
}

/// The field names in a helper's list argument.
pub(crate) fn names(list: Array, helper: &str) -> HelperResult<Vec<String>> {
    list.into_iter()
        .map(|name| {
            name.into_string().or_else(|type_name| {
                fail(format!("{helper}: field names must be strings, got {type_name}"))
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
        Some(v) if v.is_string() => v.into_string().map(|s| s.trim().is_empty()).unwrap_or(false),
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
```

In `runner.rs` `sandboxed_engine`, after `install_field_resolver(&mut engine);` add `crate::helpers::register(&mut engine);`. Add `mod helpers;` to `lib.rs`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p meili-ingest-plugin-script`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add crates/plugins/script/src
git commit -m "feat(script): field and filter helpers on doc"
```

---

### Task 4: Text and number helpers, `round(x, decimals)`

**Files:**
- Modify: `crates/plugins/script/src/helpers.rs`, `lib.rs` (tests)

**Interfaces:**
- Produces (rhai methods on `doc`): `trim_fields([..])`, `lowercase_fields([..])`, `uppercase_fields([..])`, `split_field(name, sep)`, `join_field(name, sep)`, `promote_field(name, "title"|"content")`, `round_field(name, decimals)`, `cast_field(name, "number"|"string"|"boolean")`, `default_field(name, value)`; free functions `round(FLOAT, INT) -> FLOAT`, `round(INT, INT) -> INT`.

- [ ] **Step 1: Write the failing tests** (in `lib.rs` tests)

```rust
    #[tokio::test]
    async fn text_helpers_transform_listed_strings() {
        let out = run(
            vec![with_fields(json!({"a": "  Hi  ", "b": "MiXed", "c": "ab-1"}))],
            script(r#"doc.trim_fields(["a"]); doc.lowercase_fields(["b"]); doc.uppercase_fields(["c"]);"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"a": "Hi", "b": "mixed", "c": "AB-1"}));
    }

    #[tokio::test]
    async fn a_text_helper_on_a_non_string_is_an_error() {
        let err = try_run(vec![with_fields(json!({"n": 1}))], script(r#"doc.trim_fields(["n"]);"#))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::NonRetryable(m) if m.contains("not a string")), "got {err:?}");
    }

    #[tokio::test]
    async fn split_and_join_round_trip() {
        let out = run(
            vec![with_fields(json!({"tags": "a,b,c", "list": ["x", 2, true]}))],
            script(r#"doc.split_field("tags", ","); doc.join_field("list", " | ");"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"tags": ["a", "b", "c"], "list": "x | 2 | true"}));
    }

    #[tokio::test]
    async fn join_rejects_nested_collections() {
        let err = try_run(vec![with_fields(json!({"l": [{"k": 1}]}))], script(r#"doc.join_field("l", ",");"#))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::NonRetryable(m) if m.contains("cannot be joined")), "got {err:?}");
    }

    #[tokio::test]
    async fn promote_moves_a_field_into_title_or_content() {
        let out = run(
            vec![with_fields(json!({"name": "Widget", "body": "Text", "keep": 1}))],
            script(r#"doc.promote_field("name", "title"); doc.promote_field("body", "content");"#),
        )
        .await;
        assert_eq!(out[0].title.as_deref(), Some("Widget"));
        assert_eq!(out[0].content, "Text");
        assert_eq!(fields(&out[0]), json!({"keep": 1}));
    }

    #[tokio::test]
    async fn promote_rejects_a_non_string() {
        let err = try_run(vec![with_fields(json!({"n": 3}))], script(r#"doc.promote_field("n", "title");"#))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::NonRetryable(m) if m.contains("not a string")), "got {err:?}");
    }

    #[tokio::test]
    async fn round_field_rounds_floats_and_leaves_integers() {
        let out = run(
            vec![with_fields(json!({"f": 10.456, "i": 7}))],
            script(r#"doc.round_field("f", 2); doc.round_field("i", 2);"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"f": 10.46, "i": 7}));
    }

    #[tokio::test]
    async fn round_takes_decimals_in_expressions() {
        let out = run(
            vec![with_fields(json!({"price": 10, "qty": 3}))],
            script("doc.fields.t = round(price * qty * 1.2, 2); doc.fields.i = round(7, 2);"),
        )
        .await;
        assert_eq!(out[0].fields.get("t"), Some(&json!(36.0)));
        assert_eq!(out[0].fields.get("i"), Some(&json!(7)));
    }

    #[tokio::test]
    async fn round_rejects_decimals_out_of_range() {
        let err = try_run(vec![with_fields(json!({"f": 1.5}))], script(r#"doc.round_field("f", 16);"#))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::NonRetryable(m) if m.contains("between 0 and 15")), "got {err:?}");
    }

    #[tokio::test]
    async fn cast_field_follows_the_documented_table() {
        let out = run(
            vec![with_fields(json!({
                "i": "10", "f": " 2.5 ", "b": true, "t": "TRUE", "z": 0, "n": 3, "s": 1.5
            }))],
            script(r#"
                doc.cast_field("i", "number"); doc.cast_field("f", "number");
                doc.cast_field("b", "number"); doc.cast_field("t", "boolean");
                doc.cast_field("z", "boolean"); doc.cast_field("n", "string");
                doc.cast_field("s", "string");
            "#),
        )
        .await;
        assert_eq!(
            fields(&out[0]),
            json!({"i": 10, "f": 2.5, "b": 1, "t": true, "z": false, "n": "3", "s": "1.5"})
        );
    }

    #[tokio::test]
    async fn a_cast_that_cannot_convert_is_an_error() {
        for (value, to) in [(json!("abc"), "number"), (json!(2), "boolean"), (json!([1]), "string")] {
            let err = try_run(
                vec![with_fields(json!({ "v": value }))],
                script(&format!(r#"doc.cast_field("v", "{to}");"#)),
            )
            .await
            .unwrap_err();
            assert!(matches!(err, PluginError::NonRetryable(_)), "{value} -> {to}");
        }
    }

    #[tokio::test]
    async fn default_field_fills_missing_and_null_only() {
        let out = run(
            vec![with_fields(json!({"b": null, "c": "set"}))],
            script(r#"doc.default_field("a", "EUR"); doc.default_field("b", 1); doc.default_field("c", "no");"#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"a": "EUR", "b": 1, "c": "set"}));
    }

    #[tokio::test]
    async fn text_and_number_helpers_ignore_a_missing_field() {
        let out = run(
            vec![with_fields(json!({"a": 1}))],
            script(r#"
                doc.trim_fields(["nope"]); doc.lowercase_fields(["nope"]); doc.uppercase_fields(["nope"]);
                doc.split_field("nope", ","); doc.join_field("nope", ",");
                doc.promote_field("nope", "title"); doc.round_field("nope", 2);
                doc.cast_field("nope", "string");
            "#),
        )
        .await;
        assert_eq!(fields(&out[0]), json!({"a": 1}));
        assert_eq!(out[0].title, None);
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script helpers text round cast default promote split join`
Expected: FAIL with "Function not found".

- [ ] **Step 3: Implement**

Append to `helpers.rs` (and add `FLOAT, INT` to the `rhai` import):

```rust
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
```

Extend `register`:

```rust
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
```

- [ ] **Step 4: Run the tests**

Run: `cargo test -p meili-ingest-plugin-script`
Expected: all pass. If `if let … && …` (let-chains) is rejected by the toolchain, rewrite that one branch as a nested `if let`.

- [ ] **Step 5: Commit**

```bash
git add crates/plugins/script/src
git commit -m "feat(script): text and number helpers, round(x, decimals)"
```

---

### Task 5: The `ops` model and its validation

**Files:**
- Create: `crates/plugins/script/src/ops.rs`, `crates/plugins/script/src/diagnostic.rs`
- Modify: `lib.rs` (`mod ops; mod diagnostic; pub use diagnostic::Diagnostic;`)

**Interfaces:**
- Produces: `pub struct Diagnostic { pub path: String, pub message: String, pub line: Option<u32>, pub column: Option<u32> }` with `pub(crate) fn at(path, message)` and `pub(crate) fn from_parse_error(path, &rhai::ParseError)`; `pub(crate) enum Op` (15 variants) + payload structs; `pub(crate) fn check_ops(raw: &[serde_json::Value], engine: &Engine) -> Result<Vec<Op>, Vec<Diagnostic>>`.

- [ ] **Step 1: Write the failing tests** (bottom of `ops.rs`, inside `#[cfg(test)] mod tests`)

```rust
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
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script ops::`
Expected: compile error — `ops` module does not exist yet.

- [ ] **Step 3: Implement `diagnostic.rs`**

```rust
//! A problem in a step's config, pinned to where it is.

use serde::{Deserialize, Serialize};

/// One problem found by [`crate::check_config`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    /// `script`, `ops`, `ops[3]`, `ops[2].expr`, `ops[1][0]`, `config`, …
    pub path: String,
    pub message: String,
    /// 1-based, from rhai's parse position; `None` for structural errors.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<u32>,
}

impl Diagnostic {
    pub(crate) fn at(path: impl Into<String>, message: impl Into<String>) -> Self {
        Self { path: path.into(), message: message.into(), line: None, column: None }
    }

    pub(crate) fn from_parse_error(path: impl Into<String>, error: &rhai::ParseError) -> Self {
        let position = error.position();
        Self {
            path: path.into(),
            message: error.err_type().to_string(),
            line: position.line().map(|l| l as u32),
            column: position.position().map(|c| c as u32),
        }
    }
}
```

- [ ] **Step 4: Implement `ops.rs`**

```rust
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
```

Add to `lib.rs`: `mod diagnostic; mod ops;` and `pub use diagnostic::Diagnostic;`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p meili-ingest-plugin-script`
Expected: all pass. (`Op` is not used outside tests yet — if clippy flags dead code, add `#[allow(dead_code)]` on `check_ops` for this commit only; Task 7 uses it.)

- [ ] **Step 6: Commit**

```bash
git add crates/plugins/script/src
git commit -m "feat(script): the ops model and its validation"
```

---

### Task 6: Compile `ops` to rhai

**Files:**
- Create: `crates/plugins/script/src/codegen.rs`
- Modify: `lib.rs` (`mod codegen;` + tests)

**Interfaces:**
- Consumes: `Op` and payload types from Task 5.
- Produces: `pub(crate) fn generate(ops: &[Op]) -> String`, `pub(crate) fn string_literal(&str) -> String`, `pub(crate) fn value_literal(&serde_json::Value) -> String`.

- [ ] **Step 1: Write the failing tests** (in `lib.rs` tests — they run generated code as a `script`, which works before Task 7 wires `ops`)

```rust
    use crate::codegen::{generate, string_literal, value_literal};
    use crate::ops::Op;

    fn ops(value: Value) -> Vec<Op> {
        serde_json::from_value(value).unwrap()
    }

    #[test]
    fn each_operation_becomes_one_readable_line() {
        let program = generate(&ops(json!([
            {"rename": {"from": "prix", "to": "price"}},
            {"drop": ["a", "b"]},
            {"filter": {"drop_if": {"field": "content", "is": "empty"}}},
            {"filter": {"drop_if": {"field": "s", "not_equals": "live"}}},
            {"round": {"field": "p", "decimals": 2}}
        ])));
        assert_eq!(
            program,
            concat!(
                "doc.rename_field(\"prix\", \"price\");\n",
                "doc.drop_fields([\"a\", \"b\"]);\n",
                "if doc.field_is_empty(\"content\") { return false; }\n",
                "if !doc.field_equals(\"s\", \"live\") { return false; }\n",
                "doc.round_field(\"p\", 2);\n",
            )
        );
    }

    #[tokio::test]
    async fn adversarial_field_names_touch_only_their_own_key() {
        for name in [
            r#""]; loop {}; //"#,
            "a\"b",
            "back\\slash",
            "new\nline",
            "tab\tbed",
            "é日本",
            "${x}",
            "`tick`",
        ] {
            let mut d = with_fields(json!({ "other": 1 }));
            d.fields.insert(name.into(), json!("v"));
            let program = generate(&ops(json!([{ "rename": { "from": name, "to": "safe" } }])));

            let out = run(vec![d], json!({ "script": program })).await;

            assert_eq!(fields(&out[0]), json!({"other": 1, "safe": "v"}), "name {name:?}");
        }
    }

    #[test]
    fn string_literals_escape_everything_but_printable_ascii() {
        assert_eq!(string_literal("a\"b\\c\né"), r#""a\U00000022b\U0000005Cc\U0000000A\U000000E9""#);
    }

    #[tokio::test]
    async fn value_literals_round_trip() {
        for value in [
            json!(null),
            json!(true),
            json!(3),
            json!(-5),
            json!(2.5),
            json!(1e20),
            json!(1.5e-7),
            json!("é\n\"q\""),
            json!([1, "a", null]),
            json!({"k": [1, {"n": null}], "odd key": "x"}),
        ] {
            let program = format!("doc.fields.out = {};", value_literal(&value));
            let out = run(vec![with_fields(json!({}))], json!({ "script": program })).await;
            assert_eq!(out[0].fields.get("out"), Some(&value), "literal {}", value_literal(&value));
        }
    }

    #[tokio::test]
    async fn a_trailing_comment_in_a_set_expression_cannot_swallow_the_statement() {
        let program = generate(&ops(json!([
            {"set": {"field": "t", "expr": "price * 2 // double it"}},
            {"drop": ["price"]}
        ])));
        let out = run(vec![with_fields(json!({"price": 4}))], json!({ "script": program })).await;
        assert_eq!(fields(&out[0]), json!({"t": 8}));
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script literal adversarial readable trailing`
Expected: compile error — `codegen` does not exist.

- [ ] **Step 3: Implement**

`crates/plugins/script/src/codegen.rs`:

```rust
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
```

Add `mod codegen;` to `lib.rs`.

- [ ] **Step 4: Run the tests**

Run: `cargo test -p meili-ingest-plugin-script`
Expected: all pass. If `value_literals_round_trip` fails on `1.5e-7`, rhai does not accept a signed exponent — change `number_literal` to emit `format!("{}", f)` (plain decimal, no exponent) for floats and re-run.

- [ ] **Step 5: Mutation-check the escaper**

Temporarily change the `if` in `string_literal` to `if c != '"' {`; run `cargo test -p meili-ingest-plugin-script adversarial`.
Expected: FAIL. Restore; PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/plugins/script/src
git commit -m "feat(script): compile ops to readable rhai"
```

---

### Task 7: `script` or `ops` — exactly one, and run either

**Files:**
- Modify: `crates/plugins/script/src/config.rs`, `runner.rs`, `lib.rs` (tests)

**Interfaces:**
- Consumes: `check_ops`, `generate`, `Diagnostic`, `sandboxed_engine`.
- Produces: `pub(crate) enum Source { Script(String), Ops(Vec<Op>) }`; `Config { source, on_error, max_ops, timeout_ms }`; `pub(crate) const MAX_SCRIPT_BYTES: usize = 65536`; `pub(crate) fn Config::check(value: Value, engine: &Engine) -> Result<Config, Vec<Diagnostic>>` (collect-all); `Config::parse(value) -> Result<Config, PluginError>` (first problem). `Runner::new` runs `Source::Ops` through `generate`.

- [ ] **Step 1: Write the failing tests** (in `lib.rs` tests)

```rust
    #[tokio::test]
    async fn ops_run_end_to_end() {
        let docs = vec![
            with_fields(json!({"prix": 10, "qty": 3, "status": "live"})),
            with_fields(json!({"prix": 1, "qty": 1, "status": "draft"})),
        ];
        let out = run(
            docs,
            json!({"ops": [
                {"rename": {"from": "prix", "to": "price"}},
                {"set": {"field": "total", "expr": "price * qty"}},
                {"filter": {"drop_if": {"field": "status", "equals": "draft"}}}
            ]}),
        )
        .await;
        assert_eq!(out.len(), 1);
        assert_eq!(fields(&out[0]), json!({"price": 10, "qty": 3, "status": "live", "total": 30}));
    }

    #[tokio::test]
    async fn an_empty_ops_list_passes_documents_through() {
        let out = run(vec![with_fields(json!({"a": 1}))], json!({"ops": []})).await;
        assert_eq!(fields(&out[0]), json!({"a": 1}));
    }

    #[tokio::test]
    async fn script_and_ops_together_is_a_config_error() {
        let err = try_run(vec![doc("a", "x")], json!({"script": "1", "ops": []})).await.unwrap_err();
        assert!(matches!(&err, PluginError::InvalidConfig(m) if m.contains("exactly one")), "got {err:?}");
    }

    #[tokio::test]
    async fn a_script_over_64_kib_is_a_config_error() {
        let big = format!("// {}\n1", "x".repeat(65536));
        let err = try_run(vec![doc("a", "x")], json!({ "script": big })).await.unwrap_err();
        assert!(matches!(&err, PluginError::InvalidConfig(m) if m.contains("65536")), "got {err:?}");
    }

    #[tokio::test]
    async fn a_bad_operation_names_its_index() {
        let err = try_run(vec![doc("a", "x")], json!({"ops": [{"drop": ["a"]}, {"round": {"field": "p", "decimals": 99}}]}))
            .await
            .unwrap_err();
        assert!(matches!(&err, PluginError::InvalidConfig(m) if m.contains("ops[1].decimals")), "got {err:?}");
    }
```

The existing `a_missing_script_is_a_config_error` (config `{}`) must keep passing — it now means "neither key".

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script ops_run empty_ops together 64_kib bad_operation`
Expected: FAIL — `ops` is an unknown field.

- [ ] **Step 3: Rewrite `config.rs`**

Replace `Config`, its serde struct and `impl Config` with:

```rust
use rhai::Engine;

use crate::diagnostic::Diagnostic;
use crate::ops::{Op, check_ops};
use crate::runner::sandboxed_engine;

/// Largest `script` accepted, in bytes.
pub(crate) const MAX_SCRIPT_BYTES: usize = 64 * 1024;

/// What the step runs.
#[derive(Debug, Clone)]
pub(crate) enum Source {
    Script(String),
    Ops(Vec<Op>),
}

/// Step configuration, validated.
#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) source: Source,
    pub(crate) on_error: OnError,
    pub(crate) max_ops: u64,
    pub(crate) timeout_ms: u64,
}

/// The `config:` block as written.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    #[serde(default)]
    script: Option<String>,
    #[serde(default)]
    ops: Option<Vec<Value>>,
    #[serde(default)]
    on_error: OnError,
    #[serde(default = "default_max_ops")]
    max_ops: u64,
    #[serde(default = "default_timeout_ms")]
    timeout_ms: u64,
}

impl Config {
    /// Every problem with `value`, or the parsed config.
    pub(crate) fn check(value: Value, engine: &Engine) -> Result<Self, Vec<Diagnostic>> {
        let raw: RawConfig = serde_json::from_value(value)
            .map_err(|e| vec![Diagnostic::at("config", e.to_string())])?;
        let mut diagnostics = Vec::new();

        let source = match (raw.script, raw.ops) {
            (Some(_), Some(_)) => {
                diagnostics.push(Diagnostic::at("config", "set exactly one of `script` or `ops`, not both"));
                None
            }
            (None, None) => {
                diagnostics.push(Diagnostic::at("config", "set exactly one of `script` or `ops`"));
                None
            }
            (Some(script), None) => {
                if script.trim().is_empty() {
                    diagnostics.push(Diagnostic::at("script", "`script` must not be empty"));
                } else if script.len() > MAX_SCRIPT_BYTES {
                    diagnostics.push(Diagnostic::at(
                        "script",
                        format!("`script` is {} bytes; the limit is {MAX_SCRIPT_BYTES}", script.len()),
                    ));
                }
                Some(Source::Script(script))
            }
            (None, Some(raw_ops)) => match check_ops(&raw_ops, engine) {
                Ok(ops) => Some(Source::Ops(ops)),
                Err(found) => {
                    diagnostics.extend(found);
                    None
                }
            },
        };
        if raw.max_ops == 0 {
            diagnostics.push(Diagnostic::at("max_ops", "`max_ops` must be at least 1"));
        }
        if raw.timeout_ms == 0 {
            diagnostics.push(Diagnostic::at("timeout_ms", "`timeout_ms` must be at least 1"));
        }

        match source {
            Some(source) if diagnostics.is_empty() => Ok(Config {
                source,
                on_error: raw.on_error,
                max_ops: raw.max_ops,
                timeout_ms: raw.timeout_ms,
            }),
            _ => Err(diagnostics),
        }
    }

    /// Validate a raw `config:` block, failing on the first problem.
    pub(crate) fn parse(value: Value) -> Result<Self, PluginError> {
        let engine = sandboxed_engine(default_max_ops());
        Self::check(value, &engine).map_err(|diagnostics| {
            let first = &diagnostics[0];
            PluginError::InvalidConfig(format!("{NAME}: {}: {}", first.path, first.message))
        })
    }
}
```

Make `default_max_ops` `pub(crate)`.

- [ ] **Step 4: Run the right source in `Runner::new`**

In `runner.rs`, replace the `engine.compile(&cfg.script)` call with:

```rust
        let source = match &cfg.source {
            Source::Script(script) => script.clone(),
            Source::Ops(ops) => crate::codegen::generate(ops),
        };
        let ast = engine.compile(&source).map_err(|e| {
            PluginError::InvalidConfig(format!("{NAME}: script does not compile: {e}"))
        })?;
```

and add `use crate::config::Source;`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p meili-ingest-plugin-script && cargo clippy -p meili-ingest-plugin-script --all-targets -- -D warnings`
Expected: all pass, clean. Remove any `#[allow(dead_code)]` added in Task 5.

- [ ] **Step 6: Commit**

```bash
git add crates/plugins/script/src
git commit -m "feat(script): accept exactly one of script or ops"
```

---

### Task 8: `check_config`, the manifest, and the docs

**Files:**
- Create: `crates/plugins/script/src/check.rs`
- Modify: `lib.rs` (manifest, module doc, `pub use`), `docs/plugins/built-in.mdx`, `docs/superpowers/specs/2026-09-23-document-script-editor-design.md`

**Interfaces:**
- Produces (public): `pub fn check_config(config: &serde_json::Value) -> CheckResult`; `#[derive(Debug, Clone, Default, PartialEq)] pub struct CheckResult { pub diagnostics: Vec<Diagnostic>, pub generated_script: Option<String> }`. Consumed by the control plane (Task 9) and `tests/reference.rs` (Task 14).

- [ ] **Step 1: Write the failing tests** (bottom of `check.rs`)

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_valid_script_has_no_diagnostics() {
        assert_eq!(check_config(&json!({"script": "doc.fields.x = 1;"})), CheckResult::default());
    }

    #[test]
    fn a_syntax_error_reports_its_line_and_column() {
        let result = check_config(&json!({"script": "let a = 1;\ndoc.fields.x = ;"}));
        assert_eq!(result.diagnostics.len(), 1);
        let d = &result.diagnostics[0];
        assert_eq!(d.path, "script");
        assert_eq!(d.line, Some(2));
        assert!(d.column.is_some());
    }

    #[test]
    fn valid_ops_return_their_generated_program() {
        let result = check_config(&json!({"ops": [{"rename": {"from": "a", "to": "b"}}]}));
        assert!(result.diagnostics.is_empty());
        assert_eq!(result.generated_script.as_deref(), Some("doc.rename_field(\"a\", \"b\");\n"));
    }

    #[test]
    fn every_problem_is_collected() {
        let result = check_config(&json!({
            "ops": [{"set": {"field": "t", "expr": "1 +"}}, {"drop": []}],
            "max_ops": 0
        }));
        let paths: Vec<&str> = result.diagnostics.iter().map(|d| d.path.as_str()).collect();
        assert_eq!(paths, ["ops[0].expr", "ops[1]", "max_ops"]);
        assert_eq!(result.generated_script, None);
    }

    #[test]
    fn a_non_object_config_is_reported() {
        let result = check_config(&json!("nope"));
        assert_eq!(result.diagnostics[0].path, "config");
    }

    /// The check compiles only. An endless loop compiles fine and returns at once.
    #[test]
    fn the_check_never_executes_the_script() {
        let started = std::time::Instant::now();
        let result = check_config(&json!({"script": "loop { }"}));
        assert!(result.diagnostics.is_empty());
        assert!(started.elapsed() < std::time::Duration::from_secs(2));
    }
}
```

In `lib.rs`, replace the body of `manifest_is_correct`'s `required` assertion with:

```rust
        assert!(props.contains_key("ops"), "schema missing ops");
        assert!(m.config_schema.get("required").is_none(), "neither key is required on its own");
        assert_eq!(
            m.config_schema["oneOf"],
            json!([{"required": ["script"]}, {"required": ["ops"]}])
        );
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script check:: manifest_is_correct`
Expected: compile error (`check` module missing), then `manifest_is_correct` FAIL.

- [ ] **Step 3: Implement `check.rs`**

```rust
//! Compile-check a config without running anything.

use serde_json::Value;

use crate::codegen::generate;
use crate::config::{Config, Source, default_max_ops};
use crate::diagnostic::Diagnostic;
use crate::runner::sandboxed_engine;

/// What [`check_config`] found.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct CheckResult {
    /// Every problem, in config order. Empty when the config is valid.
    pub diagnostics: Vec<Diagnostic>,
    /// The program `ops` compile to; `None` in Code mode or when `ops` is invalid.
    pub generated_script: Option<String>,
}

/// Parse and compile a `document_script` config. **Never executes it.**
///
/// Runs every check `Config::parse` does, collecting all problems instead of stopping at
/// the first, then compiles the script or the program generated from `ops`.
pub fn check_config(config: &Value) -> CheckResult {
    if !config.is_object() {
        return CheckResult {
            diagnostics: vec![Diagnostic::at("config", "config must be an object")],
            generated_script: None,
        };
    }
    let engine = sandboxed_engine(default_max_ops());
    let cfg = match Config::check(config.clone(), &engine) {
        Ok(cfg) => cfg,
        Err(diagnostics) => return CheckResult { diagnostics, generated_script: None },
    };
    match &cfg.source {
        Source::Script(script) => match engine.compile(script) {
            Ok(_) => CheckResult::default(),
            Err(e) => CheckResult {
                diagnostics: vec![Diagnostic::from_parse_error("script", &e)],
                generated_script: None,
            },
        },
        Source::Ops(ops) => {
            let program = generate(ops);
            match engine.compile(&program) {
                Ok(_) => CheckResult { diagnostics: vec![], generated_script: Some(program) },
                Err(e) => CheckResult {
                    diagnostics: vec![Diagnostic::at("ops", format!("generated program does not compile: {e}"))],
                    generated_script: None,
                },
            }
        }
    }
}
```

`lib.rs`: add `mod check;` and `pub use check::{CheckResult, check_config};`.

- [ ] **Step 4: Update the manifest schema** (in `lib.rs` `manifest()`)

Remove `"required": ["script"],`; set `script`'s schema to add `"maxLength": 65536` and drop `"minLength"`; add after `script`:

```rust
                    "ops": {
                        "type": "array",
                        "description": "Builder operations, applied in order. Each item is a single-key object: rename, drop, keep, copy, set, filter, trim, lowercase, uppercase, split, join, promote, round, cast or default.",
                        "items": {
                            "type": "object",
                            "minProperties": 1,
                            "maxProperties": 1,
                            "propertyNames": {
                                "enum": ["rename", "drop", "keep", "copy", "set", "filter", "trim",
                                         "lowercase", "uppercase", "split", "join", "promote",
                                         "round", "cast", "default"]
                            }
                        }
                    },
```

and add at the top level of the schema object: `"oneOf": [{ "required": ["script"] }, { "required": ["ops"] }],`.

- [ ] **Step 5: Fix the module doc and `built-in.mdx`**

In `lib.rs`'s module doc, replace the paragraph starting "Rhai's string methods (`trim`, `to_upper`, `pad`, ...)" with:

```text
//! Some rhai string methods edit the string in place and return `()`: `trim`,
//! `replace`, `pad`, `truncate`, `clear`, `make_upper`, `make_lower`. So
//! `doc.content.trim() == ""` is always `false` — trim as its own statement, then test
//! `is_empty()`. `to_upper()` and `to_lower()` return a new string.
```

In `docs/plugins/built-in.mdx`, `document_script` section:
- Replace the `<Warning>` body with the same corrected list.
- Add after the bindings table:

````mdx
Instead of `script`, a step can hold `ops` — the list the pipeline editor's
**Builder** produces. Set exactly one of the two.

```yaml
config:
  ops:
    - rename: { from: prix, to: price }
    - drop: [raw_html, _internal_ref]
    - set: { field: total_ttc, expr: "round(price * qty * 1.2, 2)" }
    - filter: { drop_if: { field: content, is: empty } }
```

| Operation | Shape |
|---|---|
| `rename`, `copy` | `{ from, to }` — overwrites `to` if it exists |
| `drop`, `keep`, `trim`, `lowercase`, `uppercase` | `[field, ...]` |
| `set` | `{ field, expr }` — one expression; bare names read fields |
| `filter` | `{ drop_if: { field, is: empty \| missing } }`, or `equals` / `not_equals` a value |
| `split`, `join` | `{ field, separator }` |
| `promote` | `{ field, to: title \| content }` — moves the field |
| `round` | `{ field, decimals }` — 0 to 15; integers are left unchanged |
| `cast` | `{ field, to: number \| string \| boolean }` |
| `default` | `{ field, value }` — fills a missing or null field |

A **missing** field is a no-op; a field that is **present with the wrong type** is a
per-document error, handled by `on_error`. In `filter`, the names `id`, `title` and
`content` mean the document's own attributes; a missing field compares as `null`.

Every operation is also a method on `doc` in a script — `doc.rename_field("prix", "price")`,
`doc.drop_fields([..])`, `doc.field_is_empty("content")`, and so on — and a bare name
that is not a variable reads the field of that name (`price * qty`). A variable always
wins over a field. `round(x, decimals)` takes the number of decimals.

Broken scripts and operations are rejected when the pipeline is saved, with the line and
column of the problem.
````

- [ ] **Step 6: Fold the planning refinements into the spec**

In `docs/superpowers/specs/2026-09-23-document-script-editor-design.md`:
- In "Compiling `ops` to rhai", replace "`\u{XXXX}`" with "`\U` followed by 8 hex digits (rhai has no `\u{..}` escape)", and add: "Each operation compiles to one call to a helper method registered on the engine (`doc.rename_field(\"prix\", \"price\");`); the helpers implement the semantics in Rust and are callable from Code mode too."
- In the precise-semantics table, change the `filter … equals / not_equals` row to add "A missing field compares as `null`." and add a row: "`filter` | The names `id`, `title` and `content` mean the document's own attributes; every other name is a field."
- Add a bullet under "`set` expressions": "`round(x, decimals)` is registered on the engine; rhai's own `round` takes one float."
- In "Generic multi-line strings in `SchemaForm`"/"Plugging into the step form", replace the `omit` prop with: "`StepCard` strips the owned keys from both the schema and the config before calling `SchemaForm`, which also covers its raw-JSON fallback."
- In Testing, change "every `snippet` and every `op` passes `check_config`" to "every `snippet` and every `op` runs on the entry's example document and produces the expected output".

- [ ] **Step 7: Run everything and commit**

Run: `cargo test -p meili-ingest-plugin-script && cargo clippy -p meili-ingest-plugin-script --all-targets -- -D warnings`
Expected: all pass, clean.

```bash
git add crates/plugins/script/src docs/plugins/built-in.mdx docs/superpowers/specs/2026-09-23-document-script-editor-design.md
git commit -m "feat(script): check_config, ops in the manifest, corrected docs"
```

---

## Phase 2 — Control plane and gateway

### Task 9: Control plane rejects broken step configs

**Files:**
- Modify: `crates/control-plane/Cargo.toml`, `crates/control-plane/src/error.rs`, `crates/control-plane/src/pipelines.rs`, `crates/control-plane/tests/routes.rs`

**Interfaces:**
- Consumes: `meili_ingest_plugin_script::{check_config, Diagnostic, NAME}`.
- Produces: `CpError::StepConfig(Vec<StepDiagnostic>)` (422, `invalid_step_config`); `pub struct StepDiagnostic { pub step: String, #[serde(flatten)] pub diagnostic: Diagnostic }`; `prepare_definition(&mut def) -> Result<BTreeMap<String, String>, CpError>` (generated scripts by step id); `ValidationResult.generated_scripts: BTreeMap<String, String>`.

- [ ] **Step 1: Write the failing route tests** (append to `crates/control-plane/tests/routes.rs`)

```rust
fn script_step(id: &str, config: serde_json::Value) -> StepDefinition {
    let mut step = StepDefinition::new(id, "document_script");
    step.config = config;
    step
}

fn script_pipeline(steps: Vec<StepDefinition>) -> PipelineDefinition {
    let mut def = pipeline("scripts", &[]);
    def.steps = steps;
    def
}

#[tokio::test]
async fn validate_reports_every_broken_document_script_step() {
    let def = script_pipeline(vec![
        script_step("a", serde_json::json!({"script": "doc.fields.x = ;"})),
        script_step("b", serde_json::json!({"ops": [{"set": {"field": "t", "expr": "1 +"}}]})),
    ]);
    let (status, body) = call(app(state()), post_json("/pipelines/validate", serde_json::to_vec(&def).unwrap())).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = json(&body);
    assert_eq!(body["code"], "invalid_step_config");
    assert_eq!(body["error"], "2 steps have invalid configuration");
    let diagnostics = body["diagnostics"].as_array().unwrap();
    assert_eq!(diagnostics.len(), 2);
    assert_eq!(diagnostics[0]["step"], "a");
    assert_eq!(diagnostics[0]["path"], "script");
    assert_eq!(diagnostics[0]["line"], 1);
    assert_eq!(diagnostics[1]["step"], "b");
    assert_eq!(diagnostics[1]["path"], "ops[0].expr");
}

#[tokio::test]
async fn validate_returns_generated_scripts_for_builder_steps_only() {
    let def = script_pipeline(vec![
        script_step("shape", serde_json::json!({"ops": [{"rename": {"from": "a", "to": "b"}}]})),
        script_step("raw", serde_json::json!({"script": "1"})),
    ]);
    let (status, body) = call(app(state()), post_json("/pipelines/validate", serde_json::to_vec(&def).unwrap())).await;

    assert_eq!(status, StatusCode::OK);
    let body: serde_json::Value = json(&body);
    assert_eq!(body["generated_scripts"]["shape"], "doc.rename_field(\"a\", \"b\");\n");
    assert!(body["generated_scripts"].get("raw").is_none());
}

#[tokio::test]
async fn saving_a_broken_script_is_rejected_before_the_database() {
    let def = script_pipeline(vec![script_step("a", serde_json::json!({"script": "1 +"}))]);
    let (status, body) = call(app(state()), post_json("/pipelines", serde_json::to_vec(&def).unwrap())).await;

    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    let body: serde_json::Value = json(&body);
    assert_eq!(body["code"], "invalid_step_config");
    assert_eq!(body["error"], "step `a` has invalid configuration");
}
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-control-plane --test routes document_script generated_scripts broken_script`
Expected: FAIL — status 200 / 500 instead of 422 (no check yet).

- [ ] **Step 3: Add the dependency and the error variant**

`crates/control-plane/Cargo.toml` `[dependencies]`: add `meili-ingest-plugin-script.workspace = true`.

`error.rs`:

```rust
/// A step-config diagnostic, tagged with the step it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepDiagnostic {
    pub step: String,
    #[serde(flatten)]
    pub diagnostic: meili_ingest_plugin_script::Diagnostic,
}

fn step_config_summary(diagnostics: &[StepDiagnostic]) -> String {
    let mut steps: Vec<&str> = diagnostics.iter().map(|d| d.step.as_str()).collect();
    steps.sort_unstable();
    steps.dedup();
    match steps.as_slice() {
        [one] => format!("step `{one}` has invalid configuration"),
        many => format!("{} steps have invalid configuration", many.len()),
    }
}
```

Add to `CpError` (before `Db`):

```rust
    /// One or more steps carry a config their plugin rejects (422, `invalid_step_config`).
    #[error("{}", step_config_summary(.0))]
    StepConfig(Vec<StepDiagnostic>),
```

`status()`: add `CpError::StepConfig(_)` to the `UNPROCESSABLE_ENTITY` arm. `code()`: `CpError::StepConfig(_) => "invalid_step_config",`. Replace `into_response`'s final line with:

```rust
        let body = match &self {
            CpError::StepConfig(diagnostics) => serde_json::json!({
                "error": self.to_string(),
                "code": self.code(),
                "diagnostics": diagnostics,
            }),
            _ => serde_json::to_value(self.body()).unwrap_or_default(),
        };
        (self.status(), Json(body)).into_response()
```

Add a case to `status_and_code_mapping`: `(CpError::StepConfig(vec![]), StatusCode::UNPROCESSABLE_ENTITY, "invalid_step_config"),`.

- [ ] **Step 4: Check step configs in the shared path** (`pipelines.rs`)

```rust
use std::collections::BTreeMap;

use crate::error::StepDiagnostic;

/// Compile-check every `document_script` step, collecting every diagnostic across all
/// steps. Returns the generated program of each Builder-mode step.
fn check_step_configs(def: &PipelineDefinition) -> Result<BTreeMap<String, String>, CpError> {
    let mut generated = BTreeMap::new();
    let mut diagnostics = Vec::new();
    for step in def.steps.iter().filter(|s| s.plugin == meili_ingest_plugin_script::NAME) {
        let result = meili_ingest_plugin_script::check_config(&step.config);
        diagnostics.extend(
            result.diagnostics.into_iter().map(|diagnostic| StepDiagnostic { step: step.id.clone(), diagnostic }),
        );
        if let Some(script) = result.generated_script {
            generated.insert(step.id.clone(), script);
        }
    }
    if diagnostics.is_empty() { Ok(generated) } else { Err(CpError::StepConfig(diagnostics)) }
}
```

Change `prepare_definition` to return the map:

```rust
pub fn prepare_definition(def: &mut PipelineDefinition) -> Result<BTreeMap<String, String>, CpError> {
    // ...existing builtin check, normalize, validate...
    let generated = check_step_configs(def)?;
    def.builtin = false;
    Ok(generated)
}
```

`ValidationResult`: add

```rust
    /// The program each Builder-mode `document_script` step compiles to, by step id.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub generated_scripts: BTreeMap<String, String>,
```

In `validate_pipeline`: `let generated_scripts = prepare_definition(&mut def)?;` and add `generated_scripts,` to the returned `ValidationResult`. `create_pipeline` keeps `prepare_definition(&mut def)?;`.

- [ ] **Step 5: Run the tests**

Run: `cargo test -p meili-ingest-control-plane && cargo clippy -p meili-ingest-control-plane --all-targets -- -D warnings`
Expected: all pass (the DB-backed `tests/db.rs` may be skipped/ignored locally as before).

- [ ] **Step 6: Commit**

```bash
git add Cargo.lock crates/control-plane
git commit -m "feat(control-plane): reject broken document_script configs with diagnostics"
```

---

### Task 10: Gateway forwards diagnostics; OpenAPI

**Files:**
- Modify: `crates/gateway/src/error.rs`, `crates/gateway/src/state.rs`, `crates/gateway/src/handlers/pipelines.rs` (tests), `docs/openapi.yaml`

**Interfaces:**
- Produces: `GatewayError::InvalidStepConfig { message: String, diagnostics: serde_json::Value }` (422, `invalid_step_config`), rendered as `{ error, code, diagnostics }`.

- [ ] **Step 1: Write the failing tests** (in `handlers/pipelines.rs` tests module)

```rust
    #[tokio::test]
    async fn validate_forwards_step_config_diagnostics_unchanged() {
        let server = MockServer::start().await;
        let diagnostics = json!([{"step": "a", "path": "script", "line": 1, "column": 15, "message": "boom"}]);
        Mock::given(method("POST"))
            .and(path("/pipelines/validate"))
            .respond_with(ResponseTemplate::new(422).set_body_json(json!({
                "error": "step `a` has invalid configuration",
                "code": "invalid_step_config",
                "diagnostics": diagnostics,
            })))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"uid": "g", "steps": [{"id": "a", "plugin": "document_script"}]}).to_string();
        let req = Request::post("/pipelines/validate")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let json = json_body(resp).await;
        assert_eq!(json["code"], "invalid_step_config");
        assert_eq!(json["error"], "step `a` has invalid configuration");
        assert_eq!(json["diagnostics"], diagnostics);
    }

    #[tokio::test]
    async fn other_upstream_422s_still_map_to_invalid_pipeline() {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/pipelines/validate"))
            .respond_with(ResponseTemplate::new(422).set_body_json(json!({"error": "cycle", "code": "validation"})))
            .mount(&server)
            .await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let body = json!({"uid": "g", "steps": [{"id": "a", "plugin": "chunker"}]}).to_string();
        let req = Request::post("/pipelines/validate")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();

        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
        let json = json_body(resp).await;
        assert_eq!(json["code"], "invalid_pipeline");
        assert!(json.get("diagnostics").is_none());
    }
```

- [ ] **Step 2: Run to see them fail**

Run: `cargo test -p meili-ingest-gateway forwards_step_config other_upstream`
Expected: the first FAILS (`code` is `invalid_pipeline`, no `diagnostics`); the second passes (guard).

- [ ] **Step 3: Implement**

`state.rs` — `UpstreamErrorBody`:

```rust
struct UpstreamErrorBody {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    diagnostics: Option<serde_json::Value>,
}
```

In `error_from`, replace the `StatusCode::UNPROCESSABLE_ENTITY => GatewayError::Invalid(msg),` arm with:

```rust
            StatusCode::UNPROCESSABLE_ENTITY if parsed.code.as_deref() == Some("invalid_step_config") => {
                GatewayError::InvalidStepConfig {
                    message: msg,
                    diagnostics: parsed.diagnostics.unwrap_or_else(|| serde_json::Value::Array(vec![])),
                }
            }
            StatusCode::UNPROCESSABLE_ENTITY => GatewayError::Invalid(msg),
```

(`msg` is computed from `parsed.error`/`parsed.message` by moving those two fields; `code` and `diagnostics` remain available.)

`error.rs` — add the variant after `Invalid`:

```rust
    /// A step's config failed its plugin's check (422). Carries the control plane's
    /// per-step diagnostics through unchanged.
    #[error("{message}")]
    InvalidStepConfig { message: String, diagnostics: serde_json::Value },
```

`status()`: `GatewayError::InvalidStepConfig { .. } => StatusCode::UNPROCESSABLE_ENTITY,`. `code()`: `GatewayError::InvalidStepConfig { .. } => "invalid_step_config",`. In `into_response`, replace the body construction with:

```rust
        let body = match &self {
            GatewayError::InvalidStepConfig { diagnostics, .. } => serde_json::json!({
                "error": self.to_string(),
                "code": self.code(),
                "diagnostics": diagnostics,
            }),
            _ => serde_json::to_value(ErrorBody { error: self.to_string(), code: self.code().to_string() })
                .unwrap_or_default(),
        };
        (status, Json(body)).into_response()
```

- [ ] **Step 4: Document `/pipelines/validate`** (`docs/openapi.yaml`)

Insert before `  /pipelines/{uid}:`:

```yaml
  /pipelines/validate:
    post:
      tags: [Pipelines]
      operationId: validatePipeline
      summary: Validate a pipeline without saving it
      description: |
        Runs exactly the checks `POST /pipelines` runs — normalisation, DAG
        validation, unknown plugins, and a compile check of every
        `document_script` step — and persists nothing. The check parses and
        compiles scripts; it never executes them.
      parameters:
        - $ref: "#/components/parameters/XMeiliProjectId"
        - $ref: "#/components/parameters/XMeiliEnvoySecret"
      requestBody:
        required: true
        content:
          application/json:
            schema:
              $ref: "#/components/schemas/PipelineDefinition"
      responses:
        "200":
          description: The pipeline is valid.
          content:
            application/json:
              schema:
                type: object
                required: [valid, order, normalized]
                properties:
                  valid: { type: boolean, enum: [true] }
                  order:
                    type: array
                    items: { type: string }
                    description: Topological execution order of the steps.
                  normalized:
                    $ref: "#/components/schemas/PipelineDefinition"
                  generated_scripts:
                    type: object
                    additionalProperties: { type: string }
                    description: For each Builder-mode `document_script` step, by step id, the rhai program its `ops` compile to.
        "400":
          $ref: "#/components/responses/BadRequest"
        "422":
          description: |
            Invalid pipeline (`invalid_pipeline`), or one or more steps whose
            config their plugin rejects (`invalid_step_config`, with `diagnostics`).
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/StepConfigError"
              example:
                error: "step `shape` has invalid configuration"
                code: invalid_step_config
                diagnostics:
                  - { step: shape, path: "ops[2].expr", line: 1, column: 9, message: "Variable not found: qyt" }
        "502":
          $ref: "#/components/responses/UpstreamUnavailable"

```

Add under `components.schemas` (after `Error`):

```yaml
    StepConfigError:
      type: object
      required: [error, code]
      properties:
        error: { type: string }
        code: { type: string, example: invalid_step_config }
        diagnostics:
          type: array
          items:
            type: object
            required: [step, path, message]
            properties:
              step: { type: string, description: The step id. }
              path: { type: string, description: "Where in the step's config, e.g. `script`, `ops[2].expr`." }
              message: { type: string }
              line: { type: integer, minimum: 1 }
              column: { type: integer, minimum: 1 }
```

In `POST /pipelines`'s `"422"` description, append: "A step whose config its plugin rejects returns `invalid_step_config` with `diagnostics` (see `StepConfigError`)."

- [ ] **Step 5: Run the tests**

Run: `cargo test -p meili-ingest-gateway && cargo clippy -p meili-ingest-gateway --all-targets -- -D warnings`
Expected: all pass, clean.

- [ ] **Step 6: Commit**

```bash
git add crates/gateway docs/openapi.yaml
git commit -m "feat(gateway): forward step-config diagnostics; document /pipelines/validate"
```

---

## Phase 3 — UI

All UI commands run from the worktree root with `pnpm --dir ui …`.

### Task 11: API layer carries diagnostics and generated scripts

**Files:**
- Create: `ui/src/lib/api/diagnostics.ts`, `ui/src/lib/api/diagnostics.test.ts`
- Modify: `ui/src/lib/api/types.ts`, `ui/src/lib/api/client.ts`

**Interfaces:**
- Produces: `interface StepDiagnostic { step: string; path: string; message: string; line?: number; column?: number }`; `ValidationOutcome` = `{ kind: "ok"; order: string[]; generatedScripts: Record<string, string> } | { kind: "invalid"; error: string; code: string; diagnostics: StepDiagnostic[] } | { kind: "unsupported" }`; `parseDiagnostics(body: unknown): StepDiagnostic[]`, `parseGeneratedScripts(body: unknown): Record<string, string>`, `diagnosticsForStep(all: StepDiagnostic[], step: string): StepDiagnostic[]`; `ApiError.body: unknown`.

- [ ] **Step 1: Write the failing tests** (`ui/src/lib/api/diagnostics.test.ts`)

```ts
import { describe, expect, it } from "vitest";

import { diagnosticsForStep, parseDiagnostics, parseGeneratedScripts } from "./diagnostics";

describe("parseDiagnostics", () => {
  it("reads well-formed entries", () => {
    expect(
      parseDiagnostics({
        code: "invalid_step_config",
        diagnostics: [{ step: "a", path: "script", message: "boom", line: 2, column: 5 }],
      }),
    ).toEqual([{ step: "a", path: "script", message: "boom", line: 2, column: 5 }]);
  });

  it("drops malformed entries and non-positive positions", () => {
    expect(
      parseDiagnostics({
        diagnostics: [
          { step: "a", path: "ops[0]", message: "x", line: 0 },
          { step: 1, path: "script", message: "bad step" },
          "nope",
        ],
      }),
    ).toEqual([{ step: "a", path: "ops[0]", message: "x", line: undefined, column: undefined }]);
  });

  it("returns nothing for bodies without diagnostics", () => {
    expect(parseDiagnostics(undefined)).toEqual([]);
    expect(parseDiagnostics({ error: "x", code: "validation" })).toEqual([]);
  });
});

describe("parseGeneratedScripts", () => {
  it("keeps string entries only", () => {
    expect(parseGeneratedScripts({ generated_scripts: { a: "x;", b: 3 } })).toEqual({ a: "x;" });
    expect(parseGeneratedScripts({ valid: true })).toEqual({});
  });
});

describe("diagnosticsForStep", () => {
  it("filters by step id", () => {
    const all = [
      { step: "a", path: "script", message: "1" },
      { step: "b", path: "script", message: "2" },
    ];
    expect(diagnosticsForStep(all, "b")).toEqual([all[1]]);
  });
});
```

- [ ] **Step 2: Run to see them fail**

Run: `pnpm --dir ui test src/lib/api/diagnostics.test.ts`
Expected: FAIL — cannot resolve `./diagnostics`.

- [ ] **Step 3: Implement**

`types.ts` — add near `ApiErrorBody`:

```ts
/** One compile problem in a step's config, from an `invalid_step_config` error. */
export interface StepDiagnostic {
  /** The step id. */
  step: string;
  /** Where in the step's config: `script`, `ops[2].expr`, `config`, … */
  path: string;
  message: string;
  /** 1-based; absent for structural problems. */
  line?: number;
  column?: number;
}
```

Change `ValidatePipelineOk` and `ValidationOutcome`:

```ts
export interface ValidatePipelineOk {
  valid: true;
  /** Topological execution order of the steps. */
  order: string[];
  /** Builder-mode `document_script` steps: the rhai their `ops` compile to. */
  generated_scripts?: Record<string, string>;
}

export type ValidationOutcome =
  | { kind: "ok"; order: string[]; generatedScripts: Record<string, string> }
  | { kind: "invalid"; error: string; code: string; diagnostics: StepDiagnostic[] }
  | { kind: "unsupported" };
```

`ui/src/lib/api/diagnostics.ts`:

```ts
import type { StepDiagnostic } from "./types";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function position(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value > 0 ? value : undefined;
}

/**
 * Read `diagnostics` out of an `invalid_step_config` error body.
 *
 * Tolerant by design: the body crosses a network boundary, so a malformed entry is
 * dropped rather than failing the whole validation.
 */
export function parseDiagnostics(body: unknown): StepDiagnostic[] {
  if (!isRecord(body) || !Array.isArray(body.diagnostics)) return [];
  const out: StepDiagnostic[] = [];
  for (const entry of body.diagnostics) {
    if (!isRecord(entry)) continue;
    const { step, path, message } = entry;
    if (typeof step !== "string" || typeof path !== "string" || typeof message !== "string") {
      continue;
    }
    out.push({ step, path, message, line: position(entry.line), column: position(entry.column) });
  }
  return out;
}

/** Read `generated_scripts` out of a 200 validation body. */
export function parseGeneratedScripts(body: unknown): Record<string, string> {
  if (!isRecord(body) || !isRecord(body.generated_scripts)) return {};
  const out: Record<string, string> = {};
  for (const [step, script] of Object.entries(body.generated_scripts)) {
    if (typeof script === "string") out[step] = script;
  }
  return out;
}

/** The diagnostics that belong to one step. */
export function diagnosticsForStep(all: StepDiagnostic[], step: string): StepDiagnostic[] {
  return all.filter((diagnostic) => diagnostic.step === step);
}
```

`client.ts`:
- `ApiError`: add `readonly body: unknown;`, constructor `(status: number, code: string, message: string, body: unknown = undefined)` assigning `this.body = body;`.
- `toApiError`: pass `body` as the 4th argument in both `new ApiError(...)` calls.
- `validatePipeline`: import `{ parseDiagnostics, parseGeneratedScripts }` from `./diagnostics`, then

```ts
    return { kind: "ok", order: body.order ?? [], generatedScripts: parseGeneratedScripts(body) };
```

and

```ts
      return {
        kind: "invalid",
        error: error.message,
        code: error.code,
        diagnostics: parseDiagnostics(error.body),
      };
```

- [ ] **Step 4: Run tests and typecheck**

Run: `pnpm --dir ui test && pnpm --dir ui exec tsc --noEmit`
Expected: all pass, no type errors.

- [ ] **Step 5: Commit**

```bash
git add ui/src/lib/api
git commit -m "feat(ui): carry step diagnostics and generated scripts from validation"
```

---

### Task 12: Multi-line strings in `SchemaForm` (`x-widget`)

**Files:**
- Modify: `ui/src/lib/api/types.ts` (`JsonSchema`), `ui/src/lib/schema-form/fields.ts`, `ui/src/lib/schema-form/fields.test.ts`, `ui/src/lib/schema-form/schema-form.tsx`, `crates/plugins/llm-enricher/src/lib.rs`, `docs/plugins/authoring-builtin.mdx`, `docs/plugins/authoring-wasm.mdx`, `docs/plugins/authoring-grpc.mdx`

**Interfaces:**
- Produces: `FieldKind` gains `"text" | "code"`; `JsonSchema["x-widget"]?: string`.

- [ ] **Step 1: Write the failing tests** (append to `fields.test.ts`)

```ts
describe("x-widget", () => {
  it("maps textarea and code hints on strings", () => {
    expect(schemaToField("prompt", { type: "string", "x-widget": "textarea" }).kind).toBe("text");
    expect(schemaToField("script", { type: "string", "x-widget": "code" }).kind).toBe("code");
  });

  it("ignores unknown hints and hints on non-strings", () => {
    expect(schemaToField("a", { type: "string", "x-widget": "rainbow" }).kind).toBe("string");
    expect(schemaToField("n", { type: "integer", "x-widget": "textarea" }).kind).toBe("integer");
  });

  it("keeps multi-line text verbatim", () => {
    const field = schemaToField("prompt", { type: "string", "x-widget": "textarea" });
    expect(parseFieldInput(field, "line 1\nline 2")).toEqual({ ok: true, value: "line 1\nline 2" });
    expect(formatFieldValue(field, "a\nb")).toBe("a\nb");
  });
});
```

- [ ] **Step 2: Run to see them fail**

Run: `pnpm --dir ui test src/lib/schema-form/fields.test.ts`
Expected: FAIL — kind is `"string"`; type error on `"x-widget"`.

- [ ] **Step 3: Implement**

`types.ts` `JsonSchema`: add

```ts
  /**
   * Editor hint for `string` properties: `"textarea"` (multi-line text) or `"code"`
   * (monospace). Unknown values are ignored.
   */
  "x-widget"?: string;
```

`fields.ts`: extend the table in the module doc with `` | `{"type": "string", "x-widget": "textarea" \| "code"}` | `text` / `code` | ``; change `export type FieldKind = "string" | "enum" | "boolean" | "integer" | "number" | "json" | "text" | "code";` and the `case "string":` branch of `kindFor` to:

```ts
    case "string":
      if (schema["x-widget"] === "textarea") return "text";
      if (schema["x-widget"] === "code") return "code";
      return "string";
```

`schema-form.tsx` `TextControl`: render a `Textarea` for `json`, `text` and `code`:

```tsx
      {field.kind === "json" || field.kind === "text" || field.kind === "code" ? (
        <Textarea
          {...shared}
          rows={Math.min(field.kind === "text" ? 16 : 10, Math.max(3, text.split("\n").length))}
          spellCheck={field.kind === "text"}
          className={cn(field.kind !== "text" && "font-mono text-xs")}
          onChange={(event) => handle(event.target.value)}
        />
      ) : (
```

and in `SchemaForm`, widen the full-width cell: `field.kind === "json" || field.kind === "boolean" || field.kind === "text" || field.kind === "code"`.

`crates/plugins/llm-enricher/src/lib.rs`: change the `prompt` schema to `{ "type": "string", "x-widget": "textarea", "default": DEFAULT_PROMPT }`.

In each of the three authoring docs, add a section:

````mdx
## Editor hints

The pipeline editor renders a step's form from your `config_schema`. A `string`
property renders as a one-line input unless you add `"x-widget"`:

```json
"prompt": { "type": "string", "x-widget": "textarea" }
```

| Value | Renders as |
|---|---|
| `"textarea"` | multi-line text |
| `"code"` | multi-line monospace text |

Unknown values are ignored.
````

- [ ] **Step 4: Run tests**

Run: `pnpm --dir ui test && pnpm --dir ui exec tsc --noEmit && cargo test -p meili-ingest-plugin-llm-enricher`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ui/src/lib crates/plugins/llm-enricher docs/plugins
git commit -m "feat(ui): x-widget hint renders multi-line string fields"
```

---

### Task 13: Document-script editor models (pure)

**Files:**
- Create in `ui/src/app/pipelines/_components/document-script/`: `ops.ts`, `rows.ts`, `mode.ts`, `diagnostics.ts`, `generate-script.ts`, and `ops.test.ts`, `rows.test.ts`, `mode.test.ts`, `diagnostics.test.ts`, `generate-script.test.ts`

**Interfaces:**
- Produces:
  - `ops.ts`: `type Op`, `type OpKind`, `type OpGroup`, `OP_KINDS: readonly { kind: OpKind; group: OpGroup; label: string }[]`, `GROUP_LABELS: Record<OpGroup, string>`, `opKind(value: unknown): OpKind | undefined`, `newOp(kind: OpKind): Op`, `parseLiteral(text: string): JsonValue`, `formatLiteral(value: JsonValue | undefined): string`.
  - `rows.ts`: `readOps(config: JsonObject): JsonValue[]`, `writeOps(config, ops): JsonObject`, `addOp(ops, op: Op)`, `removeOp(ops, index)`, `moveOp(ops, index, direction: -1 | 1)`, `replaceOp(ops, index, op: JsonValue)`.
  - `mode.ts`: `type ScriptMode = "builder" | "code"`, `deriveMode(config)`, `needsInitialOps(config)`, `withInitialOps(config)`, `toBuilder(config)`, `toCode(config, script)`, `lineCount(script: unknown): number`.
  - `diagnostics.ts`: `type DiagnosticTarget`, `routeDiagnostic(d)`, `diagnosticsForOp(ds, index)`, `scriptDiagnostics(ds)`, `configDiagnostics(ds)`, `type ServerState = "idle" | "checking" | "ok" | "invalid"`, `stepCheckState(state, stepDiagnostics): "idle" | "checking" | "ok" | "invalid"`.
  - `generate-script.ts`: `SCRIPT_PLUGIN = "document_script"`, `singleStepPipeline(stepId, config): PipelineDefinition`, `generateScript(stepId, config): Promise<string | undefined>`.

- [ ] **Step 1: Write the failing tests**

`ops.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { formatLiteral, newOp, opKind, OP_KINDS, parseLiteral } from "./ops";

describe("opKind", () => {
  it("reads the single key of a known operation", () => {
    expect(opKind({ rename: { from: "a", to: "b" } })).toBe("rename");
  });
  it("rejects anything else", () => {
    expect(opKind({ rename: {}, drop: [] })).toBeUndefined();
    expect(opKind({ explode: {} })).toBeUndefined();
    expect(opKind(["drop"])).toBeUndefined();
    expect(opKind(null)).toBeUndefined();
  });
});

describe("newOp", () => {
  it("builds a blank operation of every kind", () => {
    for (const { kind } of OP_KINDS) expect(opKind(newOp(kind))).toBe(kind);
  });
});

describe("literals", () => {
  it("parses JSON when it can and falls back to a string", () => {
    expect(parseLiteral("12")).toBe(12);
    expect(parseLiteral("true")).toBe(true);
    expect(parseLiteral("null")).toBeNull();
    expect(parseLiteral('"12"')).toBe("12");
    expect(parseLiteral("draft")).toBe("draft");
  });
  it("formats strings bare unless they would parse as something else", () => {
    expect(formatLiteral("draft")).toBe("draft");
    expect(formatLiteral("12")).toBe('"12"');
    expect(formatLiteral(12)).toBe("12");
    expect(formatLiteral(undefined)).toBe("");
  });
});
```

`rows.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { addOp, moveOp, readOps, removeOp, replaceOp, writeOps } from "./rows";

const A = { drop: ["a"] };
const B = { keep: ["b"] };

describe("rows", () => {
  it("reads ops, tolerating a missing or malformed key", () => {
    expect(readOps({ ops: [A] })).toEqual([A]);
    expect(readOps({})).toEqual([]);
    expect(readOps({ ops: "nope" })).toEqual([]);
  });
  it("writes ops without touching other keys", () => {
    expect(writeOps({ on_error: "skip", ops: [] }, [A])).toEqual({ on_error: "skip", ops: [A] });
  });
  it("adds, removes, replaces and moves", () => {
    expect(addOp([A], B)).toEqual([A, B]);
    expect(removeOp([A, B], 0)).toEqual([B]);
    expect(replaceOp([A, B], 1, A)).toEqual([A, A]);
    expect(moveOp([A, B], 0, 1)).toEqual([B, A]);
    expect(moveOp([A, B], 0, -1)).toEqual([A, B]);
    expect(moveOp([A, B], 1, 1)).toEqual([A, B]);
  });
});
```

`mode.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { deriveMode, lineCount, needsInitialOps, toBuilder, toCode, withInitialOps } from "./mode";

describe("mode", () => {
  it("derives the mode from the config key", () => {
    expect(deriveMode({ ops: [] })).toBe("builder");
    expect(deriveMode({ script: "1" })).toBe("code");
    expect(deriveMode({})).toBe("builder");
    expect(deriveMode({ ops: [], script: "1" })).toBe("builder");
  });
  it("initialises a fresh step with an empty ops list", () => {
    expect(needsInitialOps({ on_error: "skip" })).toBe(true);
    expect(needsInitialOps({ script: "" })).toBe(false);
    expect(withInitialOps({ on_error: "skip" })).toEqual({ on_error: "skip", ops: [] });
  });
  it("switches to the builder by discarding the script", () => {
    expect(toBuilder({ script: "x", max_ops: 5 })).toEqual({ max_ops: 5, ops: [] });
  });
  it("switches to code with the given script and no ops", () => {
    expect(toCode({ ops: [{ drop: ["a"] }], max_ops: 5 }, "doc;")).toEqual({ max_ops: 5, script: "doc;" });
  });
  it("counts script lines", () => {
    expect(lineCount("a\nb\nc")).toBe(3);
    expect(lineCount("")).toBe(0);
    expect(lineCount(undefined)).toBe(0);
  });
});
```

`diagnostics.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { configDiagnostics, diagnosticsForOp, routeDiagnostic, scriptDiagnostics, stepCheckState } from "./diagnostics";

const d = (path: string, line?: number) => ({ step: "s", path, message: path, line });

describe("routeDiagnostic", () => {
  it("routes script, op and config paths", () => {
    expect(routeDiagnostic(d("script", 2))).toEqual({ kind: "script", line: 2, column: undefined });
    expect(routeDiagnostic(d("ops[3]"))).toEqual({ kind: "op", index: 3, key: undefined });
    expect(routeDiagnostic(d("ops[2].expr"))).toEqual({ kind: "op", index: 2, key: "expr" });
    expect(routeDiagnostic(d("ops[1][0]"))).toEqual({ kind: "op", index: 1, key: "[0]" });
    expect(routeDiagnostic(d("max_ops"))).toEqual({ kind: "config" });
  });
});

describe("selectors", () => {
  const all = [d("script", 1), d("ops[0].expr"), d("ops[1]"), d("config")];
  it("selects per target", () => {
    expect(scriptDiagnostics(all)).toEqual([all[0]]);
    expect(diagnosticsForOp(all, 1)).toEqual([all[2]]);
    expect(configDiagnostics(all)).toEqual([all[3]]);
  });
});

describe("stepCheckState", () => {
  it("is invalid only when this step has diagnostics", () => {
    expect(stepCheckState("invalid", [d("script")])).toBe("invalid");
    expect(stepCheckState("invalid", [])).toBe("ok");
    expect(stepCheckState("checking", [])).toBe("checking");
    expect(stepCheckState("idle", [])).toBe("idle");
  });
});
```

`generate-script.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { singleStepPipeline } from "./generate-script";

describe("singleStepPipeline", () => {
  it("isolates one step so other steps cannot block the switch", () => {
    expect(singleStepPipeline("shape", { ops: [] })).toEqual({
      uid: "document-script-switch",
      steps: [{ id: "shape", plugin: "document_script", config: { ops: [] } }],
    });
  });
});
```

- [ ] **Step 2: Run to see them fail**

Run: `pnpm --dir ui test src/app/pipelines/_components/document-script`
Expected: FAIL — modules not found.

- [ ] **Step 3: Implement**

`ops.ts`:

```ts
/**
 * The builder's operations, mirroring `crates/plugins/script/src/ops.rs`.
 *
 * Each operation is a single-key object — `{ rename: { from, to } }` — exactly as the
 * plugin deserializes it, so a row's value is written into `config.ops` unchanged.
 */
import type { JsonValue } from "@/lib/api/types";

export type Presence = "empty" | "missing";
export type PromoteTarget = "title" | "content";
export type CastTarget = "number" | "string" | "boolean";

export interface DropIf {
  field: string;
  is?: Presence;
  equals?: JsonValue;
  not_equals?: JsonValue;
}

export type Op =
  | { rename: { from: string; to: string } }
  | { drop: string[] }
  | { keep: string[] }
  | { copy: { from: string; to: string } }
  | { set: { field: string; expr: string } }
  | { filter: { drop_if: DropIf } }
  | { trim: string[] }
  | { lowercase: string[] }
  | { uppercase: string[] }
  | { split: { field: string; separator: string } }
  | { join: { field: string; separator: string } }
  | { promote: { field: string; to: PromoteTarget } }
  | { round: { field: string; decimals: number } }
  | { cast: { field: string; to: CastTarget } }
  | { default: { field: string; value: JsonValue } };

export type OpKind =
  | "rename" | "drop" | "keep" | "copy" | "set" | "filter" | "trim" | "lowercase"
  | "uppercase" | "split" | "join" | "promote" | "round" | "cast" | "default";

export type OpGroup = "fields" | "filter" | "text" | "numbers";

export interface OpKindInfo {
  kind: OpKind;
  group: OpGroup;
  label: string;
}

/** Every operation, in menu order. */
export const OP_KINDS: readonly OpKindInfo[] = [
  { kind: "rename", group: "fields", label: "Rename" },
  { kind: "drop", group: "fields", label: "Drop" },
  { kind: "keep", group: "fields", label: "Keep only" },
  { kind: "copy", group: "fields", label: "Copy" },
  { kind: "set", group: "fields", label: "Set" },
  { kind: "filter", group: "filter", label: "Filter" },
  { kind: "trim", group: "text", label: "Trim" },
  { kind: "lowercase", group: "text", label: "Lowercase" },
  { kind: "uppercase", group: "text", label: "Uppercase" },
  { kind: "split", group: "text", label: "Split" },
  { kind: "join", group: "text", label: "Join" },
  { kind: "promote", group: "text", label: "Promote" },
  { kind: "round", group: "numbers", label: "Round" },
  { kind: "cast", group: "numbers", label: "Cast" },
  { kind: "default", group: "numbers", label: "Default" },
];

export const GROUP_LABELS: Record<OpGroup, string> = {
  fields: "Fields",
  filter: "Filter",
  text: "Text",
  numbers: "Numbers",
};

/** The kind of an operation — its single key — or `undefined` for anything else. */
export function opKind(value: unknown): OpKind | undefined {
  if (typeof value !== "object" || value === null || Array.isArray(value)) return undefined;
  const keys = Object.keys(value);
  if (keys.length !== 1) return undefined;
  return OP_KINDS.find((info) => info.kind === keys[0])?.kind;
}

/** A blank operation of `kind`, as the Add menu inserts it. */
export function newOp(kind: OpKind): Op {
  switch (kind) {
    case "rename": return { rename: { from: "", to: "" } };
    case "drop": return { drop: [] };
    case "keep": return { keep: [] };
    case "copy": return { copy: { from: "", to: "" } };
    case "set": return { set: { field: "", expr: "" } };
    case "filter": return { filter: { drop_if: { field: "content", is: "empty" } } };
    case "trim": return { trim: [] };
    case "lowercase": return { lowercase: [] };
    case "uppercase": return { uppercase: [] };
    case "split": return { split: { field: "", separator: "," } };
    case "join": return { join: { field: "", separator: ", " } };
    case "promote": return { promote: { field: "", to: "title" } };
    case "round": return { round: { field: "", decimals: 2 } };
    case "cast": return { cast: { field: "", to: "number" } };
    case "default": return { default: { field: "", value: "" } };
  }
}

/** A value typed into a builder input: JSON when it parses, otherwise the text itself. */
export function parseLiteral(text: string): JsonValue {
  try {
    return JSON.parse(text) as JsonValue;
  } catch {
    return text;
  }
}

/** The text a builder input shows for a value; quotes a string only when it would parse as something else. */
export function formatLiteral(value: JsonValue | undefined): string {
  if (value === undefined) return "";
  if (typeof value === "string") {
    return typeof parseLiteral(value) === "string" ? value : JSON.stringify(value);
  }
  return JSON.stringify(value);
}
```

`rows.ts`:

```ts
import type { JsonObject, JsonValue } from "@/lib/api/types";

import type { Op } from "./ops";

/** The raw entries of `config.ops`; an unrecognised entry is kept so no data is lost. */
export function readOps(config: JsonObject): JsonValue[] {
  return Array.isArray(config.ops) ? config.ops : [];
}

export function writeOps(config: JsonObject, ops: JsonValue[]): JsonObject {
  return { ...config, ops };
}

export function addOp(ops: JsonValue[], op: Op): JsonValue[] {
  return [...ops, op as unknown as JsonValue];
}

export function removeOp(ops: JsonValue[], index: number): JsonValue[] {
  return ops.filter((_, i) => i !== index);
}

export function replaceOp(ops: JsonValue[], index: number, op: JsonValue): JsonValue[] {
  return ops.map((entry, i) => (i === index ? op : entry));
}

export function moveOp(ops: JsonValue[], index: number, direction: -1 | 1): JsonValue[] {
  const target = index + direction;
  if (target < 0 || target >= ops.length) return ops;
  const next = [...ops];
  [next[index], next[target]] = [next[target], next[index]];
  return next;
}
```

`mode.ts`:

```ts
import type { JsonObject } from "@/lib/api/types";

export type ScriptMode = "builder" | "code";

/** The mode is the config key: `ops` → Builder, `script` → Code, neither → Builder. */
export function deriveMode(config: JsonObject): ScriptMode {
  if (Array.isArray(config.ops)) return "builder";
  if (typeof config.script === "string") return "code";
  return "builder";
}

/** A freshly added step has neither key. */
export function needsInitialOps(config: JsonObject): boolean {
  return !("ops" in config) && !("script" in config);
}

export function withInitialOps(config: JsonObject): JsonObject {
  return { ...config, ops: [] };
}

/** Code → Builder: the script is discarded (the builder cannot read free-form code). */
export function toBuilder(config: JsonObject): JsonObject {
  const next = { ...config };
  delete next.script;
  next.ops = [];
  return next;
}

/** Builder → Code: seeded with the backend's generated script. */
export function toCode(config: JsonObject, script: string): JsonObject {
  const next = { ...config };
  delete next.ops;
  next.script = script;
  return next;
}

export function lineCount(script: unknown): number {
  return typeof script === "string" && script.length > 0 ? script.split("\n").length : 0;
}
```

`diagnostics.ts`:

```ts
import type { StepDiagnostic } from "@/lib/api/types";

export type DiagnosticTarget =
  | { kind: "script"; line?: number; column?: number }
  | { kind: "op"; index: number; key?: string }
  | { kind: "config" };

const OP_PATH = /^ops\[(\d+)\](.*)$/;

/** Where a diagnostic belongs in the editor. */
export function routeDiagnostic(
  diagnostic: Pick<StepDiagnostic, "path" | "line" | "column">,
): DiagnosticTarget {
  if (diagnostic.path === "script") {
    return { kind: "script", line: diagnostic.line, column: diagnostic.column };
  }
  const match = OP_PATH.exec(diagnostic.path);
  if (match) {
    const rest = match[2].replace(/^\./, "");
    return { kind: "op", index: Number(match[1]), key: rest.length > 0 ? rest : undefined };
  }
  return { kind: "config" };
}

export function diagnosticsForOp(all: StepDiagnostic[], index: number): StepDiagnostic[] {
  return all.filter((d) => {
    const target = routeDiagnostic(d);
    return target.kind === "op" && target.index === index;
  });
}

export function scriptDiagnostics(all: StepDiagnostic[]): StepDiagnostic[] {
  return all.filter((d) => routeDiagnostic(d).kind === "script");
}

export function configDiagnostics(all: StepDiagnostic[]): StepDiagnostic[] {
  return all.filter((d) => routeDiagnostic(d).kind === "config");
}

/** The pipeline-wide server check, as the editor sees it. */
export type ServerState = "idle" | "checking" | "ok" | "invalid";

/** A step is invalid only if *it* has diagnostics; another step's problem leaves it ok. */
export function stepCheckState(state: ServerState, stepDiagnostics: StepDiagnostic[]): ServerState {
  if (state === "invalid") return stepDiagnostics.length > 0 ? "invalid" : "ok";
  return state;
}
```

`generate-script.ts`:

```ts
import { validatePipeline } from "@/lib/api/client";
import type { JsonObject, PipelineDefinition } from "@/lib/api/types";

export const SCRIPT_PLUGIN = "document_script";

/** A one-step pipeline holding only this step, so other steps' problems cannot block the switch. */
export function singleStepPipeline(stepId: string, config: JsonObject): PipelineDefinition {
  return {
    uid: "document-script-switch",
    steps: [{ id: stepId, plugin: SCRIPT_PLUGIN, config }],
  };
}

/** The rhai the backend generates for this step's `ops`, or `undefined` if it cannot. */
export async function generateScript(stepId: string, config: JsonObject): Promise<string | undefined> {
  try {
    const outcome = await validatePipeline(singleStepPipeline(stepId, config));
    return outcome.kind === "ok" ? outcome.generatedScripts[stepId] : undefined;
  } catch {
    return undefined;
  }
}
```

- [ ] **Step 4: Run tests, lint, typecheck**

Run: `pnpm --dir ui test && pnpm --dir ui lint && pnpm --dir ui exec tsc --noEmit`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ui/src/app/pipelines/_components/document-script
git commit -m "feat(ui): document-script editor models"
```

---

### Task 14: The reference, with executed examples

**Files:**
- Create: `ui/src/app/pipelines/_components/document-script/reference.json`, `reference.ts`, `reference.test.ts`; `crates/plugins/script/tests/reference.rs`

**Interfaces:**
- Produces: `interface ReferenceEntry { id: string; group: OpGroup | "code"; title: string; summary: string; snippet: string; op: Op | null; example: ReferenceDoc; expect: ReferenceExpect }`, `REFERENCE: { bindings: Binding[]; gotchas: Gotcha[]; entries: ReferenceEntry[] }`, `REFERENCE_GROUPS: readonly (OpGroup | "code")[]`, `filterEntries(entries, query): ReferenceEntry[]`.

- [ ] **Step 1: Write the reference data** (`reference.json`)

```json
{
  "bindings": [
    { "name": "doc.id", "type": "string", "writable": true, "note": "Re-sanitised after the script runs, so it stays Meilisearch-safe." },
    { "name": "doc.title", "type": "string or ()", "writable": true, "note": "Set it to () to clear the title." },
    { "name": "doc.content", "type": "string", "writable": true, "note": "The main text." },
    { "name": "doc.fields", "type": "map", "writable": true, "note": "Structured attributes. Most operations act here." },
    { "name": "doc.meta", "type": "map", "writable": false, "note": "Provenance. Readable; writes are discarded." },
    { "name": "price", "type": "any", "writable": false, "note": "A bare name that is not a variable reads the field of that name." }
  ],
  "gotchas": [
    { "title": "Some string methods edit in place", "body": "trim(), replace(), pad(), truncate(), clear(), make_upper() and make_lower() change the string and return (). Write doc.content.trim(); on its own, then test doc.content.is_empty(). to_upper() and to_lower() return a new string." },
    { "title": "A variable beats a field", "body": "After let price = 5; the name price means 5, even if the document has a price field. Use doc.fields.price to reach the field." },
    { "title": "Set takes one expression", "body": "The builder's Set is a single expression: no let, no semicolons, no loops. Switch to Code for anything longer." },
    { "title": "round takes the decimals", "body": "Use round(x, 2). rhai's own round(x) accepts only floats." },
    { "title": "id, title and content in filters", "body": "In a filter, the names id, title and content mean the document's own attributes, not fields." }
  ],
  "entries": [
    { "id": "rename", "group": "fields", "title": "Rename a field", "summary": "Move a value to a new key, replacing that key if it exists.",
      "snippet": "doc.rename_field(\"prix\", \"price\");", "op": { "rename": { "from": "prix", "to": "price" } },
      "example": { "fields": { "prix": 10 } }, "expect": { "fields": { "price": 10 } } },
    { "id": "drop", "group": "fields", "title": "Drop fields", "summary": "Remove fields you don't want indexed.",
      "snippet": "doc.drop_fields([\"raw_html\", \"_ref\"]);", "op": { "drop": ["raw_html", "_ref"] },
      "example": { "fields": { "name": "A", "raw_html": "<p>A</p>", "_ref": 7 } }, "expect": { "fields": { "name": "A" } } },
    { "id": "keep", "group": "fields", "title": "Keep only some fields", "summary": "Remove every field not listed.",
      "snippet": "doc.keep_fields([\"sku\", \"name\"]);", "op": { "keep": ["sku", "name"] },
      "example": { "fields": { "sku": 1, "name": "A", "noise": true } }, "expect": { "fields": { "sku": 1, "name": "A" } } },
    { "id": "copy", "group": "fields", "title": "Copy a field", "summary": "Duplicate a value under a second key.",
      "snippet": "doc.copy_field(\"name\", \"name_raw\");", "op": { "copy": { "from": "name", "to": "name_raw" } },
      "example": { "fields": { "name": "A" } }, "expect": { "fields": { "name": "A", "name_raw": "A" } } },
    { "id": "set", "group": "fields", "title": "Compute a field", "summary": "Set a field from an expression; bare names read fields.",
      "snippet": "doc.fields[\"total\"] = price * qty;", "op": { "set": { "field": "total", "expr": "price * qty" } },
      "example": { "fields": { "price": 10, "qty": 3 } }, "expect": { "fields": { "price": 10, "qty": 3, "total": 30 } } },
    { "id": "filter-empty", "group": "filter", "title": "Drop empty documents", "summary": "Drop a document whose text (or a field) is empty.",
      "snippet": "if doc.field_is_empty(\"content\") { return false; }", "op": { "filter": { "drop_if": { "field": "content", "is": "empty" } } },
      "example": { "content": "   ", "fields": {} }, "expect": { "dropped": true } },
    { "id": "filter-equals", "group": "filter", "title": "Drop by value", "summary": "Drop a document when a field equals a value.",
      "snippet": "if doc.field_equals(\"status\", \"draft\") { return false; }", "op": { "filter": { "drop_if": { "field": "status", "equals": "draft" } } },
      "example": { "fields": { "status": "draft" } }, "expect": { "dropped": true } },
    { "id": "trim", "group": "text", "title": "Trim whitespace", "summary": "Remove leading and trailing whitespace.",
      "snippet": "doc.trim_fields([\"name\"]);", "op": { "trim": ["name"] },
      "example": { "fields": { "name": "  A  " } }, "expect": { "fields": { "name": "A" } } },
    { "id": "lowercase", "group": "text", "title": "Lowercase", "summary": "Lowercase text fields, for example emails.",
      "snippet": "doc.lowercase_fields([\"email\"]);", "op": { "lowercase": ["email"] },
      "example": { "fields": { "email": "A@B.CO" } }, "expect": { "fields": { "email": "a@b.co" } } },
    { "id": "uppercase", "group": "text", "title": "Uppercase", "summary": "Uppercase text fields, for example codes.",
      "snippet": "doc.uppercase_fields([\"sku\"]);", "op": { "uppercase": ["sku"] },
      "example": { "fields": { "sku": "ab-1" } }, "expect": { "fields": { "sku": "AB-1" } } },
    { "id": "split", "group": "text", "title": "Split into a list", "summary": "Turn \"a,b,c\" into a list — handy for facets.",
      "snippet": "doc.split_field(\"tags\", \",\");", "op": { "split": { "field": "tags", "separator": "," } },
      "example": { "fields": { "tags": "a,b,c" } }, "expect": { "fields": { "tags": ["a", "b", "c"] } } },
    { "id": "join", "group": "text", "title": "Join a list", "summary": "Turn a list into one string.",
      "snippet": "doc.join_field(\"tags\", \", \");", "op": { "join": { "field": "tags", "separator": ", " } },
      "example": { "fields": { "tags": ["a", "b"] } }, "expect": { "fields": { "tags": "a, b" } } },
    { "id": "promote", "group": "text", "title": "Promote to title", "summary": "Move a field into the document's title or content.",
      "snippet": "doc.promote_field(\"name\", \"title\");", "op": { "promote": { "field": "name", "to": "title" } },
      "example": { "fields": { "name": "Widget" } }, "expect": { "fields": {}, "title": "Widget" } },
    { "id": "round", "group": "numbers", "title": "Round a number", "summary": "Round to a number of decimals (0–15).",
      "snippet": "doc.round_field(\"price\", 2);", "op": { "round": { "field": "price", "decimals": 2 } },
      "example": { "fields": { "price": 10.456 } }, "expect": { "fields": { "price": 10.46 } } },
    { "id": "cast", "group": "numbers", "title": "Change a type", "summary": "Convert to number, string or boolean.",
      "snippet": "doc.cast_field(\"zip\", \"string\");", "op": { "cast": { "field": "zip", "to": "string" } },
      "example": { "fields": { "zip": 75001 } }, "expect": { "fields": { "zip": "75001" } } },
    { "id": "default", "group": "numbers", "title": "Default a value", "summary": "Fill a field that is missing or null.",
      "snippet": "doc.default_field(\"currency\", \"EUR\");", "op": { "default": { "field": "currency", "value": "EUR" } },
      "example": { "fields": {} }, "expect": { "fields": { "currency": "EUR" } } },
    { "id": "return-false", "group": "code", "title": "Drop the document", "summary": "Return false to drop the current document.",
      "snippet": "doc.content.trim();\nif doc.content.is_empty() { return false; }", "op": null,
      "example": { "content": "  ", "fields": {} }, "expect": { "dropped": true } },
    { "id": "round-expr", "group": "code", "title": "Compute with rounding", "summary": "round(x, decimals) inside any expression.",
      "snippet": "doc.fields.total_ttc = round(price * qty * 1.2, 2);", "op": null,
      "example": { "fields": { "price": 10, "qty": 3 } }, "expect": { "fields": { "price": 10, "qty": 3, "total_ttc": 36.0 } } }
  ]
}
```

- [ ] **Step 2: Write the failing tests**

`crates/plugins/script/tests/reference.rs`:

```rust
//! Every example in the editor's reference must run and produce what it claims — in
//! Code mode (`snippet`) and, where there is one, in Builder mode (`op`). Read at test
//! time, never with `include_str!`, so building the plugin never needs `ui/`.

use meili_ingest_plugin_script::DocumentScriptPlugin;
use meili_ingest_plugin_sdk::prelude::*;
use serde_json::{Value, json};

const REFERENCE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../../ui/src/app/pipelines/_components/document-script/reference.json"
);

async fn run(example: &Value, config: Value) -> Vec<Document> {
    let mut doc = Document::with_id("example", example["content"].as_str().unwrap_or(""));
    doc.fields = example["fields"].as_object().cloned().unwrap_or_default();
    match DocumentScriptPlugin::new()
        .execute(&ActivityContext::noop(), PluginInput::Documents(vec![doc]), config)
        .await
    {
        Ok(PluginOutput::Documents(docs)) => docs,
        Ok(other) => panic!("expected documents, got {other:?}"),
        Err(e) => panic!("{e}"),
    }
}

fn assert_expected(id: &str, mode: &str, out: &[Document], expect: &Value) {
    if expect["dropped"] == json!(true) {
        assert!(out.is_empty(), "{id} ({mode}): expected the document to be dropped");
        return;
    }
    assert_eq!(out.len(), 1, "{id} ({mode}): expected one document");
    assert_eq!(Value::Object(out[0].fields.clone()), expect["fields"], "{id} ({mode}): fields");
    if let Some(title) = expect.get("title") {
        assert_eq!(out[0].title.as_deref(), title.as_str(), "{id} ({mode}): title");
    }
}

#[tokio::test]
async fn every_reference_example_runs_as_documented() {
    let reference: Value = serde_json::from_str(&std::fs::read_to_string(REFERENCE).expect("reference.json"))
        .expect("reference.json is JSON");
    let entries = reference["entries"].as_array().expect("entries");
    assert!(!entries.is_empty());

    for entry in entries {
        let id = entry["id"].as_str().unwrap();
        let out = run(&entry["example"], json!({ "script": entry["snippet"] })).await;
        assert_expected(id, "snippet", &out, &entry["expect"]);

        if !entry["op"].is_null() {
            let out = run(&entry["example"], json!({ "ops": [entry["op"]] })).await;
            assert_expected(id, "op", &out, &entry["expect"]);
        }
    }
}

#[test]
fn every_builder_operation_has_a_reference_entry() {
    let reference: Value = serde_json::from_str(&std::fs::read_to_string(REFERENCE).unwrap()).unwrap();
    let kinds: Vec<String> = reference["entries"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|e| e["op"].as_object().and_then(|op| op.keys().next().cloned()))
        .collect();
    for kind in ["rename", "drop", "keep", "copy", "set", "filter", "trim", "lowercase",
                 "uppercase", "split", "join", "promote", "round", "cast", "default"] {
        assert!(kinds.iter().any(|k| k == kind), "no reference entry for `{kind}`");
    }
}
```

Add `tokio` is already a dev-dependency of the crate; add `meili-ingest-plugin-sdk` is a regular dependency — no manifest change.

`reference.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { opKind, OP_KINDS } from "./ops";
import { filterEntries, REFERENCE } from "./reference";

describe("reference", () => {
  it("has unique ids and complete copy", () => {
    const ids = REFERENCE.entries.map((entry) => entry.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const entry of REFERENCE.entries) {
      expect(entry.title.length).toBeGreaterThan(0);
      expect(entry.summary.length).toBeGreaterThan(0);
      expect(entry.snippet.length).toBeGreaterThan(0);
    }
  });

  it("covers every builder operation, in its own group", () => {
    for (const info of OP_KINDS) {
      const matches = REFERENCE.entries.filter((entry) => entry.op !== null && opKind(entry.op) === info.kind);
      expect(matches.length, info.kind).toBeGreaterThanOrEqual(1);
      for (const entry of matches) expect(entry.group).toBe(info.group);
    }
  });

  it("filters by title, summary or snippet, case-insensitively", () => {
    expect(filterEntries(REFERENCE.entries, "RENAME").map((e) => e.id)).toContain("rename");
    expect(filterEntries(REFERENCE.entries, "facets").map((e) => e.id)).toEqual(["split"]);
    expect(filterEntries(REFERENCE.entries, "")).toHaveLength(REFERENCE.entries.length);
  });
});
```

- [ ] **Step 3: Run to see them fail**

Run: `cargo test -p meili-ingest-plugin-script --test reference && pnpm --dir ui test reference`
Expected: Rust passes or fails on a real example mismatch (fix the **data** if an expectation is wrong — the helper semantics are pinned by Tasks 3–4); TS FAILS — `./reference` missing.

- [ ] **Step 4: Implement `reference.ts`**

```ts
import type { JsonValue } from "@/lib/api/types";

import data from "./reference.json";
import type { Op, OpGroup } from "./ops";

export type ReferenceGroup = OpGroup | "code";

export interface ReferenceDoc {
  content?: string;
  fields: Record<string, JsonValue>;
}

export interface ReferenceExpect {
  dropped?: boolean;
  fields?: Record<string, JsonValue>;
  title?: string;
}

export interface ReferenceEntry {
  id: string;
  group: ReferenceGroup;
  title: string;
  summary: string;
  snippet: string;
  /** `null` for code-only entries. */
  op: Op | null;
  example: ReferenceDoc;
  expect: ReferenceExpect;
}

export interface Binding {
  name: string;
  type: string;
  writable: boolean;
  note: string;
}

export interface Gotcha {
  title: string;
  body: string;
}

export interface Reference {
  bindings: Binding[];
  gotchas: Gotcha[];
  entries: ReferenceEntry[];
}

export const REFERENCE = data as Reference;

export const REFERENCE_GROUPS: readonly ReferenceGroup[] = ["fields", "filter", "text", "numbers", "code"];

export const REFERENCE_GROUP_LABELS: Record<ReferenceGroup, string> = {
  fields: "Fields",
  filter: "Filter",
  text: "Text",
  numbers: "Numbers",
  code: "Code only",
};

export function filterEntries(entries: ReferenceEntry[], query: string): ReferenceEntry[] {
  const needle = query.trim().toLowerCase();
  if (needle.length === 0) return entries;
  return entries.filter((entry) =>
    [entry.title, entry.summary, entry.snippet].some((text) => text.toLowerCase().includes(needle)),
  );
}
```

- [ ] **Step 5: Run everything**

Run: `cargo test -p meili-ingest-plugin-script && pnpm --dir ui test && pnpm --dir ui exec tsc --noEmit`
Expected: all pass.

- [ ] **Step 6: Commit**

```bash
git add ui/src/app/pipelines/_components/document-script crates/plugins/script/tests
git commit -m "feat(ui): reference data with examples executed by the plugin's tests"
```

---

### Task 15: CodeMirror code editor

**Files:**
- Modify: `ui/package.json`, `ui/pnpm-lock.yaml`
- Create in `document-script/`: `rhai-language.ts`, `rhai-language.test.ts`, `lint.ts`, `lint.test.ts`, `script-code-editor.tsx`

**Interfaces:**
- Produces: `rhai` (`StreamLanguage`), `rhaiParser`; `diagnosticRange(text, line?, column?): { from: number; to: number }`; `ScriptCodeEditor` component with props `{ value: string; onChange(next: string): void; diagnostics: StepDiagnostic[]; readOnly?: boolean; maxLines?: number; ariaLabel: string }` and ref handle `ScriptCodeEditorHandle { insert(text: string): void; focus(): void }`.

- [ ] **Step 1: Add the dependencies**

Run: `pnpm --dir ui add @codemirror/state @codemirror/view @codemirror/language @codemirror/commands @codemirror/lint`
Expected: five packages added to `dependencies`.

- [ ] **Step 2: Write the failing tests**

`lint.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import { diagnosticRange } from "./lint";

const TEXT = "let a = 1;\ndoc.fields.x = ;\nend";

describe("diagnosticRange", () => {
  it("points at the column on the given line", () => {
    // line 2 starts at offset 11; column 16 is the ';'
    expect(diagnosticRange(TEXT, 2, 16)).toEqual({ from: 26, to: 27 });
  });
  it("covers the whole line without a column", () => {
    expect(diagnosticRange(TEXT, 3, undefined)).toEqual({ from: 28, to: 31 });
  });
  it("clamps positions past the text", () => {
    expect(diagnosticRange(TEXT, 99, 99)).toEqual({ from: 31, to: 31 });
  });
  it("falls back to the first line without a line", () => {
    expect(diagnosticRange(TEXT, undefined, undefined)).toEqual({ from: 0, to: 10 });
  });
});
```

`rhai-language.test.ts`:

```ts
import { StringStream } from "@codemirror/language";
import { describe, expect, it } from "vitest";

import { rhaiParser } from "./rhai-language";

function tokens(line: string): [string, string | null][] {
  const state = rhaiParser.startState!(2);
  const stream = new StringStream(line, 4, 2);
  const out: [string, string | null][] = [];
  while (!stream.eol()) {
    const style = rhaiParser.token(stream, state);
    const text = stream.current();
    if (text.trim().length > 0) out.push([text, style]);
    stream.start = stream.pos;
  }
  return out;
}

describe("rhaiParser", () => {
  it("highlights keywords, doc, strings, numbers and comments", () => {
    expect(tokens('let x = doc.fields["a\\"b"] + 12.5; // note')).toEqual([
      ["let", "keyword"],
      ["x", "variableName"],
      ["=", null],
      ["doc", "variableName.special"],
      [".", null],
      ["fields", "variableName"],
      ["[", null],
      ['"a\\"b"', "string"],
      ["]", null],
      ["+", null],
      ["12.5", "number"],
      [";", null],
      ["// note", "comment"],
    ]);
  });
});
```

- [ ] **Step 3: Run to see them fail**

Run: `pnpm --dir ui test lint rhai-language`
Expected: FAIL — modules missing.

- [ ] **Step 4: Implement**

`lint.ts`:

```ts
export interface LintRange {
  from: number;
  to: number;
}

/** Map a 1-based line/column onto an offset range in `text`, clamped to the text. */
export function diagnosticRange(text: string, line: number | undefined, column: number | undefined): LintRange {
  const lines = text.split("\n");
  if (line !== undefined && line > lines.length) return { from: text.length, to: text.length };
  const index = Math.max(line ?? 1, 1) - 1;
  let start = 0;
  for (let i = 0; i < index; i++) start += lines[i].length + 1;
  const lineText = lines[index];
  if (column === undefined) return { from: start, to: start + lineText.length };
  const from = start + Math.min(Math.max(column - 1, 0), lineText.length);
  return { from, to: Math.min(from + 1, start + lineText.length) };
}
```

`rhai-language.ts`:

```ts
import { StreamLanguage, type StreamParser } from "@codemirror/language";

const KEYWORDS = new Set([
  "let", "const", "if", "else", "switch", "while", "loop", "for", "in", "do", "until",
  "break", "continue", "return", "throw", "try", "catch", "fn", "private", "true", "false", "this",
]);

interface RhaiState {
  inBlockComment: boolean;
}

/** A small stream tokenizer for rhai: enough for highlighting, not a parser. */
export const rhaiParser: StreamParser<RhaiState> = {
  name: "rhai",
  startState: () => ({ inBlockComment: false }),
  token(stream, state) {
    if (state.inBlockComment) {
      if (stream.skipTo("*/")) {
        stream.pos += 2;
        state.inBlockComment = false;
      } else {
        stream.skipToEnd();
      }
      return "comment";
    }
    if (stream.eatSpace()) return null;
    if (stream.match("//")) {
      stream.skipToEnd();
      return "comment";
    }
    if (stream.match("/*")) {
      state.inBlockComment = true;
      return "comment";
    }
    if (stream.match(/^"(?:[^"\\]|\\.)*"?/)) return "string";
    if (stream.match(/^`[^`]*`?/)) return "string";
    if (stream.match(/^\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][+-]?\d+)?/)) return "number";
    if (stream.match(/^[A-Za-z_][A-Za-z0-9_]*/)) {
      const word = stream.current();
      if (KEYWORDS.has(word)) return "keyword";
      if (word === "doc") return "variableName.special";
      return "variableName";
    }
    stream.next();
    return null;
  },
};

export const rhai = StreamLanguage.define(rhaiParser);
```

`script-code-editor.tsx`:

```tsx
"use client";

import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { bracketMatching, defaultHighlightStyle, indentOnInput, syntaxHighlighting } from "@codemirror/language";
import { lintGutter, setDiagnostics } from "@codemirror/lint";
import { Compartment, EditorState } from "@codemirror/state";
import { EditorView, keymap, lineNumbers } from "@codemirror/view";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";

import type { StepDiagnostic } from "@/lib/api/types";

import { diagnosticRange } from "./lint";
import { rhai } from "./rhai-language";

export interface ScriptCodeEditorHandle {
  insert(text: string): void;
  focus(): void;
}

export interface ScriptCodeEditorProps {
  value: string;
  onChange: (next: string) => void;
  /** Already filtered to `script` diagnostics. */
  diagnostics: StepDiagnostic[];
  readOnly?: boolean;
  /** Grow up to this many lines; `undefined` fills the parent's height. */
  maxLines?: number;
  ariaLabel: string;
}

function theme(maxLines: number | undefined) {
  return EditorView.theme({
    "&": {
      fontSize: "12px",
      height: maxLines === undefined ? "100%" : "auto",
      border: "1px solid var(--border)",
      borderRadius: "calc(var(--radius) - 2px)",
      backgroundColor: "var(--background)",
      color: "var(--foreground)",
    },
    "&.cm-focused": { outline: "2px solid var(--ring)", outlineOffset: "1px" },
    ".cm-content": { fontFamily: "var(--font-mono)" },
    ".cm-gutters": { backgroundColor: "var(--muted)", color: "var(--muted-foreground)", border: "none" },
    ".cm-scroller": maxLines === undefined ? { overflow: "auto" } : { maxHeight: `${maxLines * 1.45}em`, overflow: "auto" },
  });
}

/** The Code-mode editor. Mounted once; prop changes are pushed into the live view. */
export const ScriptCodeEditor = forwardRef<ScriptCodeEditorHandle, ScriptCodeEditorProps>(
  function ScriptCodeEditor({ value, onChange, diagnostics, readOnly = false, maxLines, ariaLabel }, ref) {
    const host = useRef<HTMLDivElement>(null);
    const view = useRef<EditorView | null>(null);
    const onChangeRef = useRef(onChange);
    const readOnlySlot = useRef(new Compartment());

    useEffect(() => {
      onChangeRef.current = onChange;
    }, [onChange]);

    useEffect(() => {
      if (!host.current) return;
      const editor = new EditorView({
        parent: host.current,
        state: EditorState.create({
          doc: value,
          extensions: [
            lineNumbers(),
            history(),
            indentOnInput(),
            bracketMatching(),
            syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
            keymap.of([...defaultKeymap, ...historyKeymap, indentWithTab]),
            rhai,
            lintGutter(),
            readOnlySlot.current.of(EditorState.readOnly.of(readOnly)),
            EditorView.contentAttributes.of({ "aria-label": ariaLabel }),
            EditorView.updateListener.of((update) => {
              if (update.docChanged) onChangeRef.current(update.state.doc.toString());
            }),
            theme(maxLines),
          ],
        }),
      });
      view.current = editor;
      return () => {
        editor.destroy();
        view.current = null;
      };
      // Mounted once on purpose: later prop changes are dispatched by the effects below.
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // An outside change (YAML pane, mode switch) replaces the document.
    useEffect(() => {
      const editor = view.current;
      if (!editor) return;
      const current = editor.state.doc.toString();
      if (current !== value) editor.dispatch({ changes: { from: 0, to: current.length, insert: value } });
    }, [value]);

    useEffect(() => {
      view.current?.dispatch({ effects: readOnlySlot.current.reconfigure(EditorState.readOnly.of(readOnly)) });
    }, [readOnly]);

    useEffect(() => {
      const editor = view.current;
      if (!editor) return;
      const text = editor.state.doc.toString();
      editor.dispatch(
        setDiagnostics(
          editor.state,
          diagnostics.map((d) => ({ ...diagnosticRange(text, d.line, d.column), severity: "error" as const, message: d.message })),
        ),
      );
    }, [diagnostics, value]);

    useImperativeHandle(ref, () => ({
      insert(text: string) {
        const editor = view.current;
        if (!editor) return;
        const { from } = editor.state.selection.main;
        const atLineStart = from === editor.state.doc.lineAt(from).from;
        editor.dispatch(editor.state.replaceSelection(`${atLineStart ? "" : "\n"}${text}\n`));
        editor.focus();
      },
      focus() {
        view.current?.focus();
      },
    }));

    return <div ref={host} className={maxLines === undefined ? "h-full min-h-0" : undefined} />;
  },
);
```

- [ ] **Step 5: Run tests, lint, typecheck**

Run: `pnpm --dir ui test && pnpm --dir ui lint && pnpm --dir ui exec tsc --noEmit`
Expected: all pass. If `lint.test.ts` disagrees with an expected offset, recount by hand against `TEXT` and fix the **test's arithmetic only if it is wrong**; the contract is 1-based line/column.

- [ ] **Step 6: Commit**

```bash
git add ui/package.json ui/pnpm-lock.yaml ui/src/app/pipelines/_components/document-script
git commit -m "feat(ui): CodeMirror rhai editor with server diagnostics"
```

---

### Task 16: Builder rows

**Files:**
- Create in `document-script/`: `op-row.tsx`, `ops-builder.tsx`

**Interfaces:**
- Consumes: `ops.ts`, `rows.ts`, `diagnostics.ts`, `MultiValueInput` (`../multi-value-input`).
- Produces: `OpsBuilder` props `{ idPrefix: string; ops: JsonValue[]; onChange(ops: JsonValue[]): void; diagnostics: StepDiagnostic[]; readOnly: boolean }`.

- [ ] **Step 1: Implement `op-row.tsx`**

```tsx
"use client";

import { ArrowDown, ArrowUp, X } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import type { JsonValue, StepDiagnostic } from "@/lib/api/types";
import { cn } from "@/lib/utils";

import { MultiValueInput } from "../multi-value-input";
import { routeDiagnostic } from "./diagnostics";
import { formatLiteral, opKind, OP_KINDS, parseLiteral, type DropIf, type OpKind } from "./ops";

type FilterTest = "empty" | "missing" | "equals" | "not_equals";

export interface OpRowProps {
  idPrefix: string;
  index: number;
  total: number;
  value: JsonValue;
  onChange: (next: JsonValue) => void;
  onMove: (direction: -1 | 1) => void;
  onRemove: () => void;
  diagnostics: StepDiagnostic[];
  readOnly: boolean;
}

function invalidKey(diagnostics: StepDiagnostic[], key: string): boolean {
  return diagnostics.some((d) => {
    const target = routeDiagnostic(d);
    return target.kind === "op" && target.key !== undefined && target.key.startsWith(key);
  });
}

function filterTest(dropIf: DropIf): FilterTest {
  if (dropIf.equals !== undefined) return "equals";
  if (dropIf.not_equals !== undefined) return "not_equals";
  return dropIf.is ?? "empty";
}

export function OpRow({ idPrefix, index, total, value, onChange, onMove, onRemove, diagnostics, readOnly }: OpRowProps) {
  const kind = opKind(value);
  const id = `${idPrefix}-op-${index}`;
  const label = kind ? OP_KINDS.find((info) => info.kind === kind)?.label : "Unrecognised";
  const body = kind ? (value as Record<OpKind, JsonValue>)[kind] : undefined;
  const put = (next: JsonValue) => kind && onChange({ [kind]: next });
  const patch = (fields: Record<string, JsonValue>) => put({ ...(body as Record<string, JsonValue>), ...fields });
  const obj = (body ?? {}) as Record<string, JsonValue>;
  const text = (key: string) => (typeof obj[key] === "string" ? (obj[key] as string) : "");
  const field = (key: string, placeholder: string, mono = false) => (
    <Input
      id={`${id}-${key}`}
      value={text(key)}
      placeholder={placeholder}
      disabled={readOnly}
      aria-invalid={invalidKey(diagnostics, key)}
      className={cn(mono && "font-mono text-xs")}
      onChange={(event) => patch({ [key]: event.target.value })}
    />
  );

  let controls: React.ReactNode;
  switch (kind) {
    case "rename":
    case "copy":
      controls = (
        <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
          {field("from", "from")}
          <span aria-hidden className="text-muted-foreground">→</span>
          {field("to", "to")}
        </div>
      );
      break;
    case "drop":
    case "keep":
    case "trim":
    case "lowercase":
    case "uppercase":
      controls = (
        <MultiValueInput
          id={`${id}-names`}
          values={Array.isArray(body) ? body.filter((n): n is string => typeof n === "string") : []}
          placeholder="field name, then Enter"
          disabled={readOnly}
          onChange={(names) => put(names)}
        />
      );
      break;
    case "set":
      controls = (
        <div className="grid grid-cols-[1fr_auto_2fr] items-center gap-2">
          {field("field", "field")}
          <span aria-hidden className="text-muted-foreground">=</span>
          {field("expr", "price * qty", true)}
        </div>
      );
      break;
    case "filter": {
      const dropIf = (obj.drop_if ?? { field: "" }) as unknown as DropIf;
      const test = filterTest(dropIf);
      const setDropIf = (next: DropIf) => put({ drop_if: next as unknown as JsonValue });
      controls = (
        <div className="grid grid-cols-[auto_1fr_auto_1fr] items-center gap-2">
          <span className="text-xs text-muted-foreground">drop if</span>
          <Input
            id={`${id}-field`}
            value={dropIf.field}
            placeholder="content"
            disabled={readOnly}
            aria-invalid={invalidKey(diagnostics, "drop_if")}
            onChange={(event) => setDropIf({ ...dropIf, field: event.target.value })}
          />
          <Select
            value={test}
            disabled={readOnly}
            onValueChange={(next: FilterTest) =>
              setDropIf(
                next === "empty" || next === "missing"
                  ? { field: dropIf.field, is: next }
                  : { field: dropIf.field, [next]: dropIf.equals ?? dropIf.not_equals ?? "" },
              )
            }
          >
            <SelectTrigger id={`${id}-test`} className="w-36"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="empty">is empty</SelectItem>
              <SelectItem value="missing">is missing</SelectItem>
              <SelectItem value="equals">equals</SelectItem>
              <SelectItem value="not_equals">does not equal</SelectItem>
            </SelectContent>
          </Select>
          {test === "equals" || test === "not_equals" ? (
            <Input
              id={`${id}-value`}
              value={formatLiteral(dropIf[test])}
              placeholder="draft"
              disabled={readOnly}
              onChange={(event) => setDropIf({ field: dropIf.field, [test]: parseLiteral(event.target.value) })}
            />
          ) : (
            <span />
          )}
        </div>
      );
      break;
    }
    case "split":
    case "join":
      controls = (
        <div className="grid grid-cols-[2fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">on</span>
          {field("separator", ",", true)}
        </div>
      );
      break;
    case "promote":
    case "cast": {
      const options = kind === "promote" ? ["title", "content"] : ["number", "string", "boolean"];
      controls = (
        <div className="grid grid-cols-[2fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">{kind === "promote" ? "into" : "to"}</span>
          <Select value={text("to")} disabled={readOnly} onValueChange={(next) => patch({ to: next })}>
            <SelectTrigger id={`${id}-to`}><SelectValue /></SelectTrigger>
            <SelectContent>
              {options.map((option) => <SelectItem key={option} value={option}>{option}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
      );
      break;
    }
    case "round":
      controls = (
        <div className="grid grid-cols-[2fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">decimals</span>
          <Input
            id={`${id}-decimals`}
            inputMode="numeric"
            value={String(typeof obj.decimals === "number" ? obj.decimals : "")}
            disabled={readOnly}
            aria-invalid={invalidKey(diagnostics, "decimals")}
            onChange={(event) => {
              const raw = event.target.value.trim();
              if (/^\d{1,2}$/.test(raw)) patch({ decimals: Number(raw) });
            }}
          />
        </div>
      );
      break;
    case "default":
      controls = (
        <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">=</span>
          <Input
            id={`${id}-value`}
            value={formatLiteral(obj.value)}
            placeholder="EUR"
            disabled={readOnly}
            onChange={(event) => patch({ value: parseLiteral(event.target.value) })}
          />
        </div>
      );
      break;
    default:
      controls = (
        <Textarea
          id={`${id}-raw`}
          className="font-mono text-xs"
          rows={3}
          disabled={readOnly}
          defaultValue={JSON.stringify(value, null, 2)}
          onBlur={(event) => {
            try {
              onChange(JSON.parse(event.target.value) as JsonValue);
            } catch {
              // Keep the text; the server check reports the operation as invalid.
            }
          }}
        />
      );
  }

  return (
    <div className={cn("rounded-md border p-2", diagnostics.length > 0 && "border-destructive")}>
      <div className="flex items-start gap-2">
        <Badge variant="secondary" className="mt-1.5 w-20 justify-center">{label}</Badge>
        <div className="min-w-0 flex-1">{controls}</div>
        {readOnly ? null : (
          <div className="flex shrink-0 items-center">
            <Button type="button" variant="ghost" size="icon" aria-label="Move up" disabled={index === 0} onClick={() => onMove(-1)}>
              <ArrowUp />
            </Button>
            <Button type="button" variant="ghost" size="icon" aria-label="Move down" disabled={index === total - 1} onClick={() => onMove(1)}>
              <ArrowDown />
            </Button>
            <Button type="button" variant="ghost" size="icon" aria-label="Remove operation" onClick={onRemove}>
              <X />
            </Button>
          </div>
        )}
      </div>
      {diagnostics.length > 0 ? (
        <ul className="mt-1.5 space-y-0.5 pl-22 text-xs text-destructive">
          {diagnostics.map((d, i) => <li key={i}>{d.message}</li>)}
        </ul>
      ) : null}
    </div>
  );
}
```

- [ ] **Step 2: Implement `ops-builder.tsx`**

```tsx
"use client";

import { Plus } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { JsonValue, StepDiagnostic } from "@/lib/api/types";

import { diagnosticsForOp } from "./diagnostics";
import { GROUP_LABELS, newOp, OP_KINDS, type OpGroup } from "./ops";
import { OpRow } from "./op-row";
import { addOp, moveOp, removeOp, replaceOp } from "./rows";

export interface OpsBuilderProps {
  idPrefix: string;
  ops: JsonValue[];
  onChange: (ops: JsonValue[]) => void;
  diagnostics: StepDiagnostic[];
  readOnly: boolean;
}

const GROUPS = Object.keys(GROUP_LABELS) as OpGroup[];

export function OpsBuilder({ idPrefix, ops, onChange, diagnostics, readOnly }: OpsBuilderProps) {
  return (
    <div className="space-y-2">
      {ops.length === 0 ? (
        <p className="rounded-md border border-dashed py-6 text-center text-xs text-muted-foreground">
          No operations yet — add one, or pick from the reference.
        </p>
      ) : (
        ops.map((op, index) => (
          <OpRow
            key={index}
            idPrefix={idPrefix}
            index={index}
            total={ops.length}
            value={op}
            onChange={(next) => onChange(replaceOp(ops, index, next))}
            onMove={(direction) => onChange(moveOp(ops, index, direction))}
            onRemove={() => onChange(removeOp(ops, index))}
            diagnostics={diagnosticsForOp(diagnostics, index)}
            readOnly={readOnly}
          />
        ))
      )}
      {readOnly ? null : (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button type="button" variant="outline" size="sm">
              <Plus /> Add operation
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            {GROUPS.map((group, i) => (
              <DropdownMenuGroup key={group}>
                {i > 0 ? <DropdownMenuSeparator /> : null}
                <DropdownMenuLabel>{GROUP_LABELS[group]}</DropdownMenuLabel>
                {OP_KINDS.filter((info) => info.group === group).map((info) => (
                  <DropdownMenuItem key={info.kind} onSelect={() => onChange(addOp(ops, newOp(info.kind)))}>
                    {info.label}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuGroup>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      )}
    </div>
  );
}
```

- [ ] **Step 3: Lint and typecheck**

Run: `pnpm --dir ui lint && pnpm --dir ui exec tsc --noEmit`
Expected: clean. Fix any class names not supported by Tailwind v4 (e.g. replace `pl-22` with `pl-[5.5rem]`).

- [ ] **Step 4: Commit**

```bash
git add ui/src/app/pipelines/_components/document-script
git commit -m "feat(ui): builder rows for document_script operations"
```

---

### Task 17: The editor shell, the registry, and wiring into `StepCard`

**Files:**
- Create: `ui/src/app/pipelines/_components/config-editors.ts`, `config-editors.test.ts`, `config-editor-registry.tsx`, `document-script/document-script-editor.tsx`
- Modify: `ui/src/app/pipelines/_components/step-card.tsx`, `ui/src/app/pipelines/_components/pipeline-editor.tsx`

**Interfaces:**
- Produces: `interface ConfigEditorProps { stepId: string; idPrefix: string; config: JsonObject; onChange(next: JsonObject): void; diagnostics: StepDiagnostic[]; serverState: ServerState; readOnly: boolean }`; `withoutOwnedKeys(schema, config, keys) => { schema; config }`; `mergeOwnedKeys(rest, full, keys): JsonObject`; `CONFIG_EDITORS: Record<string, { Editor: ComponentType<ConfigEditorProps>; ownedKeys: readonly string[] }>`. `StepCard` gains props `diagnostics: StepDiagnostic[]` and `serverState: ServerState`.

- [ ] **Step 1: Write the failing tests** (`config-editors.test.ts`)

```ts
import { describe, expect, it } from "vitest";

import type { JsonSchema } from "@/lib/api/types";

import { mergeOwnedKeys, withoutOwnedKeys } from "./config-editors";

const SCHEMA: JsonSchema = {
  type: "object",
  required: ["script"],
  properties: { script: { type: "string" }, ops: { type: "array" }, on_error: { type: "string" } },
};

describe("withoutOwnedKeys", () => {
  it("strips owned keys from the schema and the config", () => {
    const { schema, config } = withoutOwnedKeys(SCHEMA, { script: "x", on_error: "skip" }, ["script", "ops"]);
    expect(Object.keys(schema?.properties ?? {})).toEqual(["on_error"]);
    expect(schema?.required).toEqual([]);
    expect(config).toEqual({ on_error: "skip" });
  });
  it("passes an absent schema through", () => {
    expect(withoutOwnedKeys(undefined, { ops: [] }, ["ops"])).toEqual({ schema: undefined, config: {} });
  });
});

describe("mergeOwnedKeys", () => {
  it("puts the owned keys of the full config back", () => {
    expect(mergeOwnedKeys({ on_error: "keep" }, { script: "x", on_error: "skip" }, ["script", "ops"])).toEqual({
      on_error: "keep",
      script: "x",
    });
  });
});
```

- [ ] **Step 2: Run to see them fail**

Run: `pnpm --dir ui test config-editors`
Expected: FAIL — module missing.

- [ ] **Step 3: Implement `config-editors.ts`**

```ts
/**
 * Plugin-specific config editors: types and the pure helpers that let `StepCard`
 * render a dedicated editor for some keys and the generated `SchemaForm` for the rest.
 * The registry itself lives in `./config-editor-registry.tsx` so these stay importable
 * from node tests.
 */
import type { ComponentType } from "react";

import type { JsonObject, JsonSchema, StepDiagnostic } from "@/lib/api/types";

import type { ServerState } from "./document-script/diagnostics";

export interface ConfigEditorProps {
  stepId: string;
  idPrefix: string;
  config: JsonObject;
  onChange: (next: JsonObject) => void;
  /** This step's server diagnostics. */
  diagnostics: StepDiagnostic[];
  serverState: ServerState;
  readOnly: boolean;
}

export interface ConfigEditorEntry {
  Editor: ComponentType<ConfigEditorProps>;
  /** Keys the editor renders; `SchemaForm` never sees them. */
  ownedKeys: readonly string[];
}

/**
 * Remove `keys` from a schema and a config before handing them to `SchemaForm`.
 * Stripping both (rather than an `omit` prop) also covers `SchemaForm`'s raw-JSON
 * fallback when no manifest has been registered yet. `oneOf` is dropped: it describes
 * the owned keys and `SchemaForm` does not read it.
 */
export function withoutOwnedKeys(
  schema: JsonSchema | undefined,
  config: JsonObject,
  keys: readonly string[],
): { schema: JsonSchema | undefined; config: JsonObject } {
  const rest: JsonObject = {};
  for (const [key, value] of Object.entries(config)) if (!keys.includes(key)) rest[key] = value;
  if (!schema) return { schema: undefined, config: rest };
  const properties = Object.fromEntries(
    Object.entries(schema.properties ?? {}).filter(([key]) => !keys.includes(key)),
  );
  const { oneOf: _oneOf, ...base } = schema as JsonSchema & { oneOf?: unknown };
  void _oneOf;
  return {
    schema: { ...base, properties, required: (schema.required ?? []).filter((key) => !keys.includes(key)) },
    config: rest,
  };
}

/** Put the owned keys of `full` back onto the `SchemaForm`-edited `rest`. */
export function mergeOwnedKeys(rest: JsonObject, full: JsonObject, keys: readonly string[]): JsonObject {
  const merged: JsonObject = { ...rest };
  for (const key of keys) if (key in full) merged[key] = full[key];
  return merged;
}
```

- [ ] **Step 4: Implement `document-script-editor.tsx`**

```tsx
"use client";

import { BookOpen, Check, Code2, Loader2, Maximize2, Rows3 } from "lucide-react";
import { useEffect, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { cn } from "@/lib/utils";

import type { ConfigEditorProps } from "../config-editors";
import { configDiagnostics, scriptDiagnostics, stepCheckState } from "./diagnostics";
import { generateScript } from "./generate-script";
import { deriveMode, lineCount, needsInitialOps, toBuilder, toCode, withInitialOps, type ScriptMode } from "./mode";
import { OpsBuilder } from "./ops-builder";
import { addOp, readOps, writeOps } from "./rows";
import { ScriptCodeEditor, type ScriptCodeEditorHandle } from "./script-code-editor";
import type { ReferenceEntry } from "./reference";

type Confirm = "to-builder" | "to-code-empty" | null;

export function DocumentScriptEditor({
  stepId,
  idPrefix,
  config,
  onChange,
  diagnostics,
  serverState,
  readOnly,
}: ConfigEditorProps) {
  const mode = deriveMode(config);
  const [switching, setSwitching] = useState(false);
  const [confirm, setConfirm] = useState<Confirm>(null);
  const [expanded, setExpanded] = useState(false);
  const codeRef = useRef<ScriptCodeEditorHandle>(null);

  // A freshly added step has neither key: start it in the builder with no operations.
  useEffect(() => {
    if (!readOnly && needsInitialOps(config)) onChange(withInitialOps(config));
  }, [config, onChange, readOnly]);

  async function switchTo(next: ScriptMode) {
    if (next === mode || readOnly || switching) return;
    if (next === "builder") {
      setConfirm("to-builder");
      return;
    }
    setSwitching(true);
    try {
      const script = await generateScript(stepId, config);
      if (script === undefined) setConfirm("to-code-empty");
      else onChange(toCode(config, script));
    } finally {
      setSwitching(false);
    }
  }

  function applyEntry(entry: ReferenceEntry) {
    if (mode === "builder") {
      if (entry.op) onChange(writeOps(config, addOp(readOps(config), entry.op)));
    } else {
      codeRef.current?.insert(entry.snippet);
    }
  }

  const state = stepCheckState(serverState, diagnostics);
  const general = configDiagnostics(diagnostics);

  function body(fill: boolean) {
    return mode === "builder" ? (
      <OpsBuilder
        idPrefix={idPrefix}
        ops={readOps(config)}
        onChange={(ops) => onChange(writeOps(config, ops))}
        diagnostics={diagnostics}
        readOnly={readOnly}
      />
    ) : (
      <ScriptCodeEditor
        ref={codeRef}
        value={typeof config.script === "string" ? config.script : ""}
        onChange={(script) => onChange({ ...config, script })}
        diagnostics={scriptDiagnostics(diagnostics)}
        readOnly={readOnly}
        maxLines={fill ? undefined : 20}
        ariaLabel={`Script for step ${stepId}`}
      />
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <div role="group" aria-label="Authoring mode" className="inline-flex rounded-md border p-0.5">
          {(["builder", "code"] as const).map((option) => (
            <Button
              key={option}
              type="button"
              size="xs"
              variant={mode === option ? "secondary" : "ghost"}
              aria-pressed={mode === option}
              disabled={readOnly || switching}
              onClick={() => void switchTo(option)}
            >
              {option === "builder" ? <Rows3 /> : <Code2 />}
              {option === "builder" ? "Builder" : "Code"}
              {switching && option === "code" ? <Loader2 className="animate-spin" /> : null}
            </Button>
          ))}
        </div>
        <span
          className={cn(
            "text-xs",
            state === "invalid" ? "text-destructive" : "text-muted-foreground",
          )}
        >
          {state === "checking" ? "checking…" : null}
          {state === "ok" ? (
            <span className="inline-flex items-center gap-1"><Check className="size-3" /> valid</span>
          ) : null}
          {state === "invalid" ? `${diagnostics.length} ${diagnostics.length === 1 ? "error" : "errors"}` : null}
        </span>
        <div className="ml-auto flex items-center gap-1">
          <ReferenceButton mode={mode} onUse={applyEntry} />
          <Button type="button" variant="ghost" size="xs" onClick={() => setExpanded(true)}>
            <Maximize2 /> Expand
          </Button>
        </div>
      </div>

      {general.length > 0 ? (
        <ul className="space-y-0.5 text-xs text-destructive">
          {general.map((d, i) => <li key={i}>{d.message}</li>)}
        </ul>
      ) : null}

      {expanded ? (
        <p className="rounded-md border border-dashed py-4 text-center text-xs text-muted-foreground">
          Editing in the expanded view.
        </p>
      ) : (
        body(false)
      )}

      <ExpandedEditor open={expanded} onOpenChange={setExpanded} mode={mode} onUse={applyEntry}>
        {body(true)}
      </ExpandedEditor>

      <Dialog open={confirm !== null} onOpenChange={(open) => (open ? null : setConfirm(null))}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{confirm === "to-builder" ? "Discard the script?" : "Start with an empty script?"}</DialogTitle>
            <DialogDescription>
              {confirm === "to-builder"
                ? `This discards your script (${lineCount(config.script)} lines). The builder can't read free-form code.`
                : "The script for these operations couldn't be generated right now. Code mode would start empty and your operations would be discarded."}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button
              type="button"
              variant="destructive"
              onClick={() => {
                onChange(confirm === "to-builder" ? toBuilder(config) : toCode(config, ""));
                setConfirm(null);
              }}
            >
              Discard and switch
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
```

`ReferenceButton` and `ExpandedEditor` are defined in Task 18. For this task, add temporary local stand-ins at the bottom of the file so it compiles and renders:

```tsx
function ReferenceButton(_props: { mode: ScriptMode; onUse: (entry: ReferenceEntry) => void }) {
  return (
    <Button type="button" variant="ghost" size="xs" disabled>
      <BookOpen /> Reference
    </Button>
  );
}

function ExpandedEditor(props: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: ScriptMode;
  onUse: (entry: ReferenceEntry) => void;
  children: React.ReactNode;
}) {
  return (
    <Dialog open={props.open} onOpenChange={props.onOpenChange}>
      <DialogContent className="flex h-[90vh] max-w-[min(1200px,95vw)] flex-col sm:max-w-[min(1200px,95vw)]">
        <DialogHeader><DialogTitle>Document script</DialogTitle></DialogHeader>
        <div className="min-h-0 flex-1 overflow-auto">{props.children}</div>
      </DialogContent>
    </Dialog>
  );
}
```

- [ ] **Step 5: The registry and `StepCard`**

`config-editor-registry.tsx`:

```tsx
import type { ConfigEditorEntry } from "./config-editors";
import { DocumentScriptEditor } from "./document-script/document-script-editor";

/** Plugins whose config gets a dedicated editor instead of (part of) the generated form. */
export const CONFIG_EDITORS: Record<string, ConfigEditorEntry> = {
  document_script: { Editor: DocumentScriptEditor, ownedKeys: ["script", "ops"] },
};
```

`step-card.tsx`:
- Imports: `import type { StepDiagnostic } from "@/lib/api/types";`, `import { CONFIG_EDITORS } from "./config-editor-registry";`, `import { mergeOwnedKeys, withoutOwnedKeys } from "./config-editors";`, `import type { ServerState } from "./document-script/diagnostics";`.
- `StepCardProps`: add `diagnostics: StepDiagnostic[];` and `serverState: ServerState;`; destructure both.
- Before `return`, add:

```tsx
  const custom = CONFIG_EDITORS[step.plugin];
  const CustomEditor = custom?.Editor;
  const formPart = custom
    ? withoutOwnedKeys(manifest?.config_schema, step.config, custom.ownedKeys)
    : { schema: manifest?.config_schema, config: step.config };
```

- Replace the `config` `<div className="space-y-2">` block with:

```tsx
        <div className="space-y-2">
          <p className="text-xs font-medium">config</p>
          {CustomEditor ? (
            <CustomEditor
              stepId={step.id}
              idPrefix={`${idPrefix}-editor`}
              config={step.config}
              onChange={(next) => onPatch({ config: next })}
              diagnostics={diagnostics}
              serverState={serverState}
              readOnly={readOnly}
            />
          ) : null}
          <SchemaForm
            idPrefix={`${idPrefix}-config`}
            schema={formPart.schema}
            value={formPart.config}
            disabled={readOnly}
            onChange={(next: JsonObject) =>
              onPatch({ config: custom ? mergeOwnedKeys(next, step.config, custom.ownedKeys) : next })
            }
          />
        </div>
```

`pipeline-editor.tsx`:
- Imports: `import { diagnosticsForStep } from "@/lib/api/diagnostics";` and `import type { ServerState } from "./document-script/diagnostics";`.
- After `const serverCheck = …`, add:

```tsx
  const serverDiagnostics = serverCheck.data?.kind === "invalid" ? serverCheck.data.diagnostics : [];
  const serverState: ServerState = serverCheck.isFetching
    ? "checking"
    : serverCheck.data?.kind === "ok"
      ? "ok"
      : serverCheck.data?.kind === "invalid"
        ? "invalid"
        : "idle";
```

- On `<StepCard … />`, add `diagnostics={diagnosticsForStep(serverDiagnostics, step.id)}` and `serverState={serverState}`.

- [ ] **Step 6: Run tests, lint, typecheck, build**

Run: `pnpm --dir ui test && pnpm --dir ui lint && pnpm --dir ui exec tsc --noEmit && pnpm --dir ui build`
Expected: all pass; the static export builds. Fix lint complaints about unused `_props`/`_oneOf` by following the repo's ESLint config (prefix-ignore or `void`).

- [ ] **Step 7: Commit**

```bash
git add ui/src/app/pipelines/_components
git commit -m "feat(ui): document_script editor in the step card"
```

---

### Task 18: Reference sheet and expanded view

**Files:**
- Create: `ui/src/components/ui/sheet.tsx` (shadcn CLI), `ui/src/app/pipelines/_components/document-script/script-reference.tsx`
- Modify: `document-script/document-script-editor.tsx` (replace the two stand-ins)

**Interfaces:**
- Produces: `ScriptReference` props `{ mode: ScriptMode; onUse(entry: ReferenceEntry): void }`.

- [ ] **Step 1: Add the shadcn sheet**

Run: `cd ui && pnpm exec shadcn add sheet && cd ..`
Expected: `ui/src/components/ui/sheet.tsx` created exporting `Sheet`, `SheetContent`, `SheetDescription`, `SheetHeader`, `SheetTitle`, `SheetTrigger`.

- [ ] **Step 2: Implement `script-reference.tsx`**

```tsx
"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";

import type { ScriptMode } from "./mode";
import {
  filterEntries,
  REFERENCE,
  REFERENCE_GROUP_LABELS,
  REFERENCE_GROUPS,
  type ReferenceEntry,
} from "./reference";

export function ScriptReference({ mode, onUse }: { mode: ScriptMode; onUse: (entry: ReferenceEntry) => void }) {
  const [query, setQuery] = useState("");
  const entries = filterEntries(REFERENCE.entries, query);

  return (
    <div className="flex h-full min-h-0 flex-col gap-3">
      <Input
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Filter operations…"
        aria-label="Filter the reference"
      />
      <ScrollArea className="min-h-0 flex-1 pr-3">
        <div className="space-y-5 pb-4">
          {REFERENCE_GROUPS.map((group) => {
            const inGroup = entries.filter((entry) => entry.group === group);
            if (inGroup.length === 0) return null;
            return (
              <section key={group} className="space-y-2">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {REFERENCE_GROUP_LABELS[group]}
                </h3>
                {inGroup.map((entry) => {
                  const usable = mode === "code" || entry.op !== null;
                  return (
                    <article key={entry.id} className="space-y-1.5 rounded-md border p-2.5">
                      <div className="flex items-start justify-between gap-2">
                        <div>
                          <p className="text-sm font-medium">{entry.title}</p>
                          <p className="text-xs text-muted-foreground">{entry.summary}</p>
                        </div>
                        <Button
                          type="button"
                          size="xs"
                          variant="outline"
                          disabled={!usable}
                          title={usable ? undefined : "Code mode only"}
                          onClick={() => onUse(entry)}
                        >
                          {mode === "code" ? "Insert" : "Add"}
                        </Button>
                      </div>
                      <pre className="overflow-x-auto rounded bg-muted px-2 py-1.5 font-mono text-[11px]">
                        {entry.snippet}
                      </pre>
                    </article>
                  );
                })}
              </section>
            );
          })}

          <section className="space-y-2">
            <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Bindings</h3>
            <dl className="space-y-1.5 text-xs">
              {REFERENCE.bindings.map((binding) => (
                <div key={binding.name}>
                  <dt className="font-mono">
                    {binding.name} <span className="text-muted-foreground">· {binding.type}{binding.writable ? "" : " · read-only"}</span>
                  </dt>
                  <dd className="text-muted-foreground">{binding.note}</dd>
                </div>
              ))}
            </dl>
          </section>

          <section className="space-y-2">
            <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Gotchas</h3>
            {REFERENCE.gotchas.map((gotcha) => (
              <div key={gotcha.title} className="text-xs">
                <p className="font-medium">{gotcha.title}</p>
                <p className="text-muted-foreground">{gotcha.body}</p>
              </div>
            ))}
          </section>
        </div>
      </ScrollArea>
    </div>
  );
}
```

- [ ] **Step 3: Replace the stand-ins in `document-script-editor.tsx`**

Add imports:

```tsx
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { ScriptReference } from "./script-reference";
```

Replace the two stand-in functions with:

```tsx
function ReferenceButton({ mode, onUse }: { mode: ScriptMode; onUse: (entry: ReferenceEntry) => void }) {
  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button type="button" variant="ghost" size="xs">
          <BookOpen /> Reference
        </Button>
      </SheetTrigger>
      <SheetContent side="right" className="flex w-full flex-col sm:max-w-md">
        <SheetHeader>
          <SheetTitle>Reference</SheetTitle>
          <SheetDescription>
            {mode === "code" ? "Insert a snippet at the cursor." : "Add an operation to the builder."}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 px-4 pb-4">
          <ScriptReference mode={mode} onUse={onUse} />
        </div>
      </SheetContent>
    </Sheet>
  );
}

function ExpandedEditor({
  open,
  onOpenChange,
  mode,
  onUse,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: ScriptMode;
  onUse: (entry: ReferenceEntry) => void;
  children: React.ReactNode;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex h-[90vh] max-w-[min(1200px,95vw)] flex-col sm:max-w-[min(1200px,95vw)]">
        <DialogHeader>
          <DialogTitle>Document script</DialogTitle>
          <DialogDescription>{mode === "code" ? "Code mode" : "Builder mode"}</DialogDescription>
        </DialogHeader>
        <div className="grid min-h-0 flex-1 gap-4 md:grid-cols-[minmax(0,1fr)_22rem]">
          <div className="min-h-0 overflow-auto">{children}</div>
          <div className="min-h-0 border-t pt-4 md:border-t-0 md:border-l md:pt-0 md:pl-4">
            <ScriptReference mode={mode} onUse={onUse} />
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
```

- [ ] **Step 4: Run tests, lint, typecheck, build**

Run: `pnpm --dir ui test && pnpm --dir ui lint && pnpm --dir ui exec tsc --noEmit && pnpm --dir ui build`
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add ui/src/components/ui/sheet.tsx ui/src/app/pipelines/_components/document-script ui/package.json ui/pnpm-lock.yaml
git commit -m "feat(ui): reference sheet and expanded editor for document_script"
```

---

## Phase 4 — Verification

### Task 19: End-to-end verification

**Files:**
- Modify (only if a defect is found — each fix gets its own test first): any of the above.

- [ ] **Step 1: Full automated suite**

Run:
```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
pnpm --dir ui test
pnpm --dir ui lint
pnpm --dir ui exec tsc --noEmit
pnpm --dir ui build
```
Expected: every command passes. Record the counts.

- [ ] **Step 2: Run the stack**

Read `ui/next.config.ts` and `compose.yaml` to find how the UI reaches the API in dev (same-origin through the gateway with `NEXT_PUBLIC_BASE_PATH=/ui`, or a dev proxy). Start the backend with `docker compose watch` (OrbStack) and open the UI through the gateway's OrbStack URL (`gateway.<project>.orb.local/ui/pipelines/new/`) or the Next dev server via `preview_start`, whichever the config supports.

- [ ] **Step 3: Verify in the browser** (screenshot each)

1. New pipeline → add a `document_script` step → it starts in **Builder** with "No operations yet".
2. Add Rename, Set (`price * qty`), Filter (content is empty) → YAML pane shows `ops:` with the three single-key maps.
3. Type `price *` in Set → within ~1s the row turns red with the rhai message; status shows `1 error`.
4. Fix it → status `✓ valid`.
5. Switch to **Code** → the editor shows the generated `doc.rename_field(...)` lines; YAML shows `script:` and no `ops:`.
6. Type `doc.fields.x = ;` → a lint squiggle at the right line; status `1 error`.
7. Open **Reference** → filter "split" → **Insert** → the snippet lands at the cursor.
8. Switch to **Builder** → confirmation dialog shows the line count → confirm → empty builder.
9. **Expand** → dialog with editor + docked reference; **Add** from the reference appends a row.
10. Save a pipeline with a broken script → rejected with the diagnostic.
11. Add an `llm_enricher` step → `prompt` is a multi-line textarea that accepts newlines.
12. Toggle dark mode → editor and sheet use theme colours.

- [ ] **Step 4: Update the PR**

```bash
git push
```

Then update PR #3's description (or open a follow-up PR, per the user's preference) summarising the editor, the spec refinements, and the verification results.
