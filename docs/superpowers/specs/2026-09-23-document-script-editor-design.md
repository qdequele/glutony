# Document Script Editor — Design

**Status:** approved, pending implementation plan
**Date:** 2026-09-23
**Scope:** make the `document_script` action authorable in the admin UI — a visual
builder backed by a declarative `ops` config the plugin understands, a code editor
for free-form rhai, a reference that doubles as the builder's palette, and
line-precise compile diagnostics before save. Also fixes the single-line rendering of
every multi-line string field in the step form.

## Problem

`document_script` (PR #3) shipped with a backend and a marketplace card, but it cannot
be authored in the pipeline editor.

The step form is generated from each plugin's `config_schema` by `SchemaForm`
(`ui/src/lib/schema-form/`). A `"type": "string"` property renders as a single-line
`<Input>` (`schema-form.tsx:107`). You cannot type a newline into it, so the plugin's
only required field — a multi-line rhai script — is effectively uneditable in the
form. The raw YAML pane is the only way in. The same bug already affects
`llm_enricher`'s multi-line `prompt`.

Past the input itself, a script author gets no help: no syntax highlighting, no
reference for the `doc` bindings or rhai's sharp edges (string methods like `trim()`
edit in place and return `()`), and no feedback until a job runs — a typo costs a full
ingestion round-trip. And people who do not write code have no way to use the action
at all.

## Decisions

These resolve the design's forks. Where later sections appear to differ, this list wins.

1. **Two authoring modes, one per step.** A step is either in **Builder** mode or
   **Code** mode. There is no two-way sync: arbitrary rhai cannot be parsed back into
   builder rows, and a sync that silently degrades rows into opaque blocks is worse
   than an explicit choice.
2. **The mode is the config key.** A step's config holds exactly one of `ops`
   (Builder) or `script` (Code). Nothing else records the mode, so the UI reopens a
   step without parsing, and a YAML author sees the same two options.
3. **The backend understands `ops`.** The builder is not a UI-only feature that
   generates a script: `ops` is a declarative config the plugin validates, compiles to
   rhai, and runs. YAML and API authors get the builder's operations too.
4. **One execution path.** `ops` compile to rhai source in Rust and run through the
   existing `Runner`, so both modes share one sandbox, one set of budgets and one
   error policy.
5. **Switching Builder → Code keeps the work; Code → Builder clears it.** The first
   direction always succeeds because the builder generates code. The second cannot,
   so it asks for confirmation and starts the builder empty.
6. **The UI seeds Code mode from the backend's generated script**, never from a
   TypeScript re-implementation of the compiler, so the two cannot drift.
7. **Bare field names in expressions.** Inside `set` expressions — and, since the
   resolver is shared, in Code mode too — a bare name that is not a variable in scope
   resolves to the current document's field of that name. **A variable always wins
   over a field.**
8. **Compile check only, never execution, before a job runs.** No endpoint executes a
   script. The check parses and compiles.
9. **No new endpoint.** The check joins the existing `POST /pipelines/validate`, which
   shares its code path with `POST /pipelines` by design ("the two can never
   disagree"). Consequence: saving a pipeline with a broken script is rejected with a
   422, from any client.
10. **Approach A for the form.** `SchemaForm` gains one generic hint (`x-widget`) that
    fixes multi-line strings for every plugin, and `document_script` gets a dedicated
    editor through a registry keyed by plugin name.
11. **One reference file drives three surfaces:** the Reference sheet, Insert in Code
    mode, and Add (plus the Add-operation menu) in Builder mode. The reference *is* the
    visual helper.
12. **Documentation cannot rot.** A Rust test compiles every reference snippet and
    op template, so the docs cannot show code that does not run.
13. **CodeMirror 6, not Monaco.** ~150 KB against 2–3 MB, which matters because the UI
    is a static export embedded in the gateway binary.
14. **rhai is pinned to `=1.26.1`.** The field resolver uses `Engine::on_var`, which
    rhai marks volatile; a test pins the behaviour it relies on.

## Backend

### Config

```rust
/// Exactly one of `script` / `ops`.
struct Config {
    script: Option<String>,
    ops: Option<Vec<Op>>,
    on_error: OnError,   // unchanged
    max_ops: u64,        // unchanged
    timeout_ms: u64,     // unchanged
}
```

- Both or neither → `InvalidConfig`.
- `script` longer than **64 KiB** → `InvalidConfig`. The limit is enforced in
  `Config::parse`, so it holds at save time and at runtime.
- `ops: []` is valid (a no-op step); it is what a fresh Builder step saves.

### Operations

Each operation is a single-key map (serde's externally tagged enum):

| Group | Operation | Shape |
|---|---|---|
| Fields | `rename` | `{ from, to }` |
| | `drop` | `[field, ...]` |
| | `keep` | `[field, ...]` — drop every field not listed |
| | `copy` | `{ from, to }` |
| | `set` | `{ field, expr }` |
| Filter | `filter` | `{ drop_if: { field, is: empty \| missing } }` or `{ drop_if: { field, equals: value } }` or `{ drop_if: { field, not_equals: value } }` |
| Text | `trim` | `[field, ...]` |
| | `lowercase` | `[field, ...]` |
| | `uppercase` | `[field, ...]` |
| | `split` | `{ field, separator }` — string → array |
| | `join` | `{ field, separator }` — array → string |
| | `promote` | `{ field, to: title \| content }` |
| Numbers | `round` | `{ field, decimals }` |
| | `cast` | `{ field, to: number \| string \| boolean }` |
| | `default` | `{ field, value }` |

```yaml
config:
  ops:
    - rename: { from: prix, to: price }
    - drop: [raw_html, _internal_ref]
    - set: { field: total_ttc, expr: "round(price * qty * 1.2, 2)" }
    - filter: { drop_if: { field: content, is: empty } }
```

Operations apply in order, each seeing the result of the previous one.

**One rule for missing and mistyped fields:**

- A **missing** field is a no-op for every operation that reads one — there is
  nothing to rename, trim or round.
- A field that is **present but the wrong type** (`round` on a string, `split` on a
  number), or a `cast` that cannot convert (`"abc"` → number), is a per-document
  error handled by `on_error`.
- The two exceptions, where absence is the point: `default` fires when the field is
  missing **or** null, and `filter … is: missing | empty` tests for it. `empty` means
  missing, null, `""` after trimming, `[]` or `{}`.
- A `set` expression that references a missing field fails with rhai's
  "variable not found" — a per-document error.

**Precise semantics where two readings were possible:**

| Operation | Rule |
|---|---|
| `rename`, `copy` | Overwrite `to` if it already exists. |
| `keep` | Listed fields that are missing are ignored; every unlisted field is removed. |
| `promote` | **Moves**: the field is removed from `fields` (as `json_flattener`'s `title_field` does). Use `copy` first to keep both. A non-string value is an error. |
| `round` | Numbers only. Integers are left unchanged. `decimals` is 0–15. |
| `cast` → `number` | A string holding an integer literal becomes an integer; any other numeric string becomes a float; booleans become 0/1; anything else is an error. |
| `cast` → `boolean` | `"true"`/`"false"` (case-insensitive) and the numbers 0/1; anything else is an error. |
| `cast` → `string` | Numbers and booleans are formatted; arrays and maps are an error. |
| `filter … equals / not_equals` | JSON equality, type-sensitive: `"1"` does not equal `1`. A missing field compares as `null`. |
| `filter` | The names `id`, `title` and `content` mean the document's own attributes; every other name is a field. |
| every operation | An empty field name is `InvalidConfig`. |

`title`, `content` and `id` are addressed by `promote` and by Code mode, not by the
field operations, which act on `doc.fields` only.

### Compiling `ops` to rhai

`ops` compile to one rhai program that the existing `Runner` executes. Readability of
the generated source matters: it is what Code mode starts from after a switch.

**No config string ever reaches the source unescaped.** Field names and literal values
are emitted through one escaper that writes `"`, `\` and every character outside
printable ASCII as `\U` followed by 8 hex digits (rhai has no `\u{..}` escape), inside a
double-quoted rhai string. Field access is always the index form, `doc.fields["name"]`,
never dotted, so a name is never parsed as code. `set.expr` is the one place user code
appears, and it is compiled on its own first (below), then wrapped in parentheses. Each
operation compiles to one call to a helper method registered on the engine
(`doc.rename_field("prix", "price");`); the helpers implement the semantics in Rust and
are callable from Code mode too.

### `set` expressions and the field resolver

- `set.expr` is compiled with `Engine::compile_expression` in `Config::parse`, so it
  cannot contain statements, `let`, loops or function definitions. This is defence in
  depth on top of the operation and wall-clock budgets.
- Bare names resolve through `Engine::on_var`, installed on **every** `Runner`, in
  both modes. The resolver:
  1. returns `Ok(None)` if `context.scope().contains(name)` — **a variable in scope
     always wins**; rhai consults the resolver *before* the scope
     (`rhai-1.26.1/src/eval/expr.rs`), so this check is what prevents a field from
     shadowing a `let` binding;
  2. otherwise reads `doc` from the scope and returns `doc.fields[name]` if present;
  3. otherwise returns `Ok(None)`, so rhai raises "variable not found".
- Fields whose names are not identifiers are reached as `doc.fields["unit-price"]`.
- Consequence for Code mode: `doc.fields.total = price * qty;` now works there too.
- `round(x, decimals)` is registered on the engine; rhai's own `round` takes one float.

### `check_config`

```rust
/// Parse and compile a `document_script` config without running anything.
pub fn check_config(config: &serde_json::Value) -> CheckResult;

pub struct CheckResult {
    pub diagnostics: Vec<Diagnostic>,
    /// The program `ops` compile to; `None` in Code mode or when `ops` is invalid.
    pub generated_script: Option<String>,
}

pub struct Diagnostic {
    /// `script`, `ops`, `ops[3]`, `ops[2].expr`, …
    pub path: String,
    pub message: String,
    /// 1-based, from rhai's parse position; `None` for structural errors.
    pub line: Option<u32>,
    pub column: Option<u32>,
}
```

It performs every check `Config::parse` does, plus compiling the script or the
generated program, and collects **all** diagnostics rather than stopping at the first.
For an error inside `set.expr`, `line`/`column` are relative to the expression. The
plugin's `execute` keeps failing fast on the first problem, as today.

### Manifest

`config_schema` gains an `ops` property describing the operations, and replaces
`"required": ["script"]` with `"oneOf": [{ "required": ["script"] }, { "required": ["ops"] }]`.
Client-side draft validation (`ui/src/lib/pipeline/validate.ts`) checks input kinds
only, not `required`, so the change is safe there.

## API

### `POST /pipelines/validate` and `POST /pipelines`

The control plane links `meili-ingest-plugin-script` (≈1.5 MB for rhai) and, after the
existing normalisation and DAG checks, runs `check_config` on every `document_script`
step. Diagnostics from all steps are collected together.

**422 body** — additive over the existing `{ error, code }`:

```json
{
  "error": "2 steps have invalid configuration",
  "code": "invalid_step_config",
  "diagnostics": [
    { "step": "shape", "path": "ops[2].expr", "line": 1, "column": 9,
      "message": "Variable not found: qyt" }
  ]
}
```

A new `CpError::StepConfig(Vec<StepDiagnostic>)` renders this body. It applies to
both `/pipelines/validate` and `POST /pipelines`, since they share
`prepare_definition`.

**200 body of `/pipelines/validate`** — additive:

```json
{
  "valid": true,
  "order": ["extract", "shape", "index"],
  "normalized": { },
  "generated_scripts": { "shape": "doc.fields[\"price\"] = …" }
}
```

`generated_scripts` holds one entry per Builder-mode `document_script` step.

### Gateway: preserve `code` and `diagnostics`

The gateway currently re-wraps every upstream error: `error_from`
(`crates/gateway/src/state.rs:486`) keeps only the message, so a 422 becomes
`GatewayError::Invalid(message)` and `code` and any extra fields are dropped. This
design changes it:

- `UpstreamErrorBody` gains `code: Option<String>` and
  `diagnostics: Option<serde_json::Value>`.
- A new `GatewayError::InvalidStepConfig { message, diagnostics }` is produced when
  the upstream 422 carries `code: "invalid_step_config"`. Its response body is
  `{ error, code: "invalid_step_config", diagnostics }`.
- Every other upstream 422 keeps its current mapping to `GatewayError::Invalid`.
- The 200 body of `/pipelines/validate` is already forwarded as a raw
  `serde_json::Value`, so `generated_scripts` passes through unchanged.

### OpenAPI

`/pipelines/validate` is missing from `docs/openapi.yaml` today. It is added with its
200 body (including `generated_scripts`) and its 422 body (including `diagnostics`),
and `POST /pipelines` documents the new `invalid_step_config` 422.

## UI

### Plugging into the step form

- `ui/src/app/pipelines/_components/config-editors.ts` — a registry
  `Record<pluginName, { Editor: ComponentType<ConfigEditorProps>; ownedKeys: string[] }>`.
  `document_script` registers `DocumentScriptEditor` owning `["script", "ops"]`.
- `StepCard` renders the registered editor, then `SchemaForm`. `StepCard` strips the
  owned keys from both the schema and the config before calling `SchemaForm`, which
  also covers its raw-JSON fallback, so `on_error`, `max_ops` and `timeout_ms` keep
  their generated controls. Plugins without an entry render exactly as today.

### `DocumentScriptEditor`

Co-located under `ui/src/app/pipelines/_components/document-script/`.

```
┌ Step 2 · shape · document_script ───────────────┐
│ Mode: (● Builder) ( Code )     [📖] [⤢ Expand] │
│ 1  Rename  [prix    ] → [price   ]      ↑↓ ✕ │
│ 2  Set     [total   ] = [price * qty]   ↑↓ ✕ │
│ 3  Filter  drop if [content] [empty ▾]  ↑↓ ✕ │
│ [+ Add operation ▾]              ✓ valid    │
├─ On error [fail ▾]  Max ops [100000]  … ────┤
└──────────────────────────────────────────────┘
```

- **Header:** a Builder | Code toggle, a Reference button, an Expand button, and a
  status line — `✓ valid`, `N errors`, or `checking…`.
- **Mode derivation:** `ops` present → Builder; `script` present → Code; neither (a
  newly added step) → Builder, and the step's config is initialised to `{ ops: [] }`.
- **Builder:** one row per operation, each an op-specific inline form built from the
  existing `Input`, `Select` and `MultiValueInput` (for list-valued operations).
  `set.expr` is a monospace `Input`. Rows reorder with ↑/↓ buttons — no drag-and-drop
  dependency — and ✕ deletes. **+ Add operation** is a `DropdownMenu` grouped by
  Fields, Filter, Text and Numbers, generated from the reference file.
- **Code:** a CodeMirror 6 editor with a small rhai `StreamLanguage` (keywords,
  strings, numbers, comments, `doc`). It grows up to about 20 lines inline and fills
  the available height in Expand.
- **Diagnostics:** the editor reads `diagnostics` for its step from the existing
  debounced validation (`useValidatePipeline`, 600 ms). A path of `script` becomes a
  CodeMirror lint marker at `line`/`column`; `ops[i]` marks row `i`; `ops[i].expr`
  marks that row's expression input, with the message shown under it.
- **Expand:** a near-fullscreen `Dialog` holding the same editor, with the reference
  docked as a right-hand column.

### Switching modes

- **Code → Builder:** a confirmation `Dialog` — "This discards your script (N lines).
  The builder can't read free-form code." Confirming sets `{ ops: [] }` and removes
  `script`.
- **Builder → Code:** the toggle shows a spinner and runs one immediate
  `validatePipeline` on the current (not debounced) draft. On success it seeds the
  code editor with `generated_scripts[stepId]` and removes `ops`. If validation fails,
  errors, or reports `unsupported`, it falls back to the confirm-and-start-empty
  dialog above.

### Reference

`ui/src/app/pipelines/_components/document-script/reference.json`:

```json
{
  "bindings": [ { "name": "doc.fields", "type": "map", "writable": true, "note": "…" } ],
  "gotchas":  [ { "title": "String methods edit in place", "body": "…" } ],
  "entries": [
    {
      "id": "rename",
      "group": "fields",
      "title": "Rename a field",
      "summary": "Move a value to a new key and remove the old one.",
      "snippet": "doc.fields.price = doc.fields.prix;\ndoc.fields.remove(\"prix\");",
      "op": { "rename": { "from": "prix", "to": "price" } }
    }
  ]
}
```

- One entry per builder operation (15), plus code-only entries where they help (for
  example "Drop the document with `return false`").
- **Bindings** documents `doc.id`, `doc.title`, `doc.content`, `doc.fields`,
  `doc.meta` (read-only) and bare-name resolution.
- **Gotchas** covers in-place string methods, the variable-beats-field rule, and
  that `set` expressions cannot contain statements.
- Rendered by one `ScriptReference` component with a filter input, inside a shadcn
  `Sheet` (inline mode, opening from the right over the YAML pane) or as the docked
  column in Expand. Each entry shows **Insert** in Code mode (a CodeMirror transaction
  at the cursor) and **Add** in Builder mode (appends `op` as a new row).

### Generic multi-line strings in `SchemaForm`

- `fields.ts`: a `string` property with `"x-widget": "textarea"` maps to a new field
  kind `text` (auto-growing `Textarea`); `"x-widget": "code"` maps to `code`
  (monospace `Textarea`). Any other `x-widget` value is ignored and the property
  renders as a plain `string` — a third-party schema can never break the form.
- `llm_enricher`'s `prompt` gains `"x-widget": "textarea"`.
- `x-widget` is documented in `docs/plugins/authoring-builtin.mdx`,
  `authoring-wasm.mdx` and `authoring-grpc.mdx`.

### New dependencies

- `@codemirror/state`, `@codemirror/view`, `@codemirror/language`,
  `@codemirror/commands`, `@codemirror/lint` — mounted by a small `useCodeMirror`
  hook, with no React wrapper package.
- shadcn `sheet`, added through the shadcn CLI.

## Testing

**Rust — `meili-ingest-plugin-script`**

- Parsing: exactly one of `script`/`ops`; the 64 KiB limit; every operation's shape;
  unknown operation and unknown keys rejected.
- Behaviour of each operation, including the missing-vs-wrong-type rule and `filter`'s
  definition of empty.
- Code generation: generated programs compile; adversarial field names (`"]; loop {}; //`,
  quotes, backslashes, newlines, non-ASCII) touch only the named key.
- `set.expr` rejects statements, `let` and loops at parse time.
- Resolver: a bare name reads the field; a `let` variable of the same name wins over
  the field; a name that is neither raises "variable not found".
- `check_config`: collects all diagnostics; reports `line`/`column` for script errors
  and for errors inside `set.expr`; returns `generated_script` for valid `ops`.
- `reference.json` (read from `ui/` at test time, not with `include_str!`): every
  `snippet` and every `op` runs on the entry's example document and produces the
  expected output, and every builder operation has an entry.
- Mutation checks on the resolver's variable-wins guard and the escaper.

**Rust — control plane and gateway**

- `/pipelines/validate` and `POST /pipelines` return 422 `invalid_step_config` with
  diagnostics from every broken step; valid pipelines still validate and save.
- `generated_scripts` is present for Builder-mode steps and absent for Code-mode ones.
- The gateway forwards `code: "invalid_step_config"` and `diagnostics` unchanged, and
  keeps mapping other 422s to `invalid_pipeline`.

**UI — vitest, pure modules**

- Mode derivation, and both switch transitions (which keys are set and removed).
- The rows model: op templates, add, reorder, delete, round-trip to `ops` JSON.
- Routing a diagnostic path to a row, an expression input, or a code line.
- `x-widget` mapping in `fields.ts`, including unknown values.
- The shape of `reference.json`.

**In the browser, on the running stack**

Both modes; both switch directions, including the fallback; Insert and Add from the
reference; Expand; diagnostics appearing on the right row and line; a save rejected
with a broken script; `llm_enricher`'s prompt accepting newlines.

## Non-goals

- **Executing scripts before a job runs** — no "try it on a sample" preview. It was
  considered and declined; the check parses and compiles only.
- **Two-way sync** between Builder and Code.
- **Autocompletion** of field names (unknown statically) or rhai functions.
- **Drag-and-drop** reordering.
- **Syntax highlighting inside `set.expr` inputs** — they stay monospace `Input`s.
- **Executing `ops` in the browser** or any TypeScript compiler for `ops`.
