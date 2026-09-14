# Action & Workflow Marketplaces Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship two discovery surfaces in the admin UI — a browsable catalog of every action (plugin) the system can run, and one of every workflow (pipeline) that ships with it — backed by curated catalog data compiled into the gateway.

**Architecture:** A new `catalog` module in `meili-ingest-router` holds authored copy for all 19 known plugins and all 15 built-in pipelines, compiled into the binary. The gateway serves it at `GET /catalog` directly, with no control-plane hop and no database. The Next.js UI fetches it once, joins it against the live `GET /plugins` and `GET /pipelines`, and renders two tabbed grids plus detail pages. Cloning a workflow reuses the pipeline editor that already exists.

**Tech Stack:** Rust (axum, serde), Next.js 16 App Router with `output: "export"`, TypeScript strict, TanStack Query, Tailwind v4, shadcn/ui, lucide-react, Vitest.

**Spec:** `docs/superpowers/specs/2026-09-13-marketplaces-design.md`

## Global Constraints

- **Never use `any` in TypeScript.** Define proper interfaces. The repo's eslint config enforces this.
- **Wire types mirror Rust one-for-one, snake_case**, as the header of `ui/src/lib/api/types.ts` requires.
- **The UI is a static export** (`output: "export"`). No dynamic route segments — detail pages take query params and every URL goes through a feature-local `routes.ts` helper. Any component calling `useSearchParams` must be wrapped in `<Suspense>` or the build fails.
- **Every server read goes through a hook** in `ui/src/lib/api/hooks.ts`, never a bare `fetch`. Add cache keys to the `queryKeys` object, never inline string arrays.
- **Use `cn()`** for conditional class merging, never string concatenation.
- **Run `pnpm build` before `pnpm exec tsc --noEmit`.** Next 16 generates global types (`LayoutProps`, `PageProps`) into `.next/types` at build time; without a prior build, `tsc` reports a spurious `TS2304: Cannot find name 'LayoutProps'` in `ui/src/app/layout.tsx`. That file is unmodified by this plan — if you see that error, build first rather than reporting it as pre-existing breakage.
- **Two eslint warnings are pre-existing** in `ui/src/app/playground/_components/ingest-form.tsx` (`react-hooks/incompatible-library`). `pnpm lint` should report exactly `2 problems (0 errors, 2 warnings)` — anything more is yours.
- **Use existing shadcn primitives** from `ui/src/components/ui/` — `card`, `badge`, `button`, `input`, `tabs`, `tooltip`, `alert`, `skeleton`, `separator` are all vendored. Do not add new dependencies.
- **Co-locate components** in the feature's `_components/` directory.
- **Rust: run `cargo fmt` before every commit.** `rustfmt.toml` is at the repo root.
- **Commit messages end with a `Co-Authored-By:` trailer naming the model that wrote the commit**, e.g. `Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>` or `Co-Authored-By: Claude Haiku 4.5 <noreply@anthropic.com>`. Use your own model's name — never copy another model's.
- **Clippy must be clean on the crates you touch**, e.g. `cargo clippy -p meili-ingest-router -p meili-ingest-control-plane -p meili-ingest-gateway --all-targets -- -D warnings`. CI gates on clippy (`.github/workflows/ci.yml:38`), so a warning is a build failure, not a style note. Never silence one with `#[allow(...)]` — there is no precedent for it anywhere in `crates/`. Note: `--workspace` currently fails with 7 errors inside the third-party `prost-reflect` crate, for reasons that predate this branch (verified at merge base 3f99358). Ignore that, and never try to fix it as part of a task here.
- **Do not run `git checkout`, `git restore`, or `git stash` across the worktree.** Stage and commit only the files your task names. A blanket revert destroys the controller's uncommitted plan and ledger edits — this has already happened once.
- **Never add catalog copy to `PluginManifest`.** That type is implemented by third-party WASM and gRPC plugin authors; showcase copy belongs only in `catalog.rs`.

## File Structure

**Rust — created:**
- `crates/router/src/catalog.rs` — catalog types, the `catalog()` data function, and the invariant tests that keep copy from rotting against code. One responsibility: the authored description of what the system can do.
- `crates/gateway/src/handlers/catalog.rs` — the `GET /catalog` handler and its test.

**Rust — modified:**
- `crates/router/src/lib.rs` — add `pub mod catalog;` and re-export.
- `crates/gateway/src/handlers/mod.rs` — add `pub mod catalog;`.
- `crates/gateway/src/lib.rs:74` — register the route next to `/plugins`.
- `docs/openapi.yaml` — the path and its schemas.

**UI — created:**
- `ui/src/lib/catalog/merge.ts` — the pure catalog⨝manifest join. No React.
- `ui/src/lib/catalog/merge.test.ts`
- `ui/src/lib/catalog/neighbours.ts` — the pure `accepts`/`produces` kind algebra. No React.
- `ui/src/lib/catalog/neighbours.test.ts`
- `ui/src/app/marketplace/routes.ts` — the four href helpers.
- `ui/src/app/marketplace/layout.tsx` — shared header and the Actions|Workflows tab bar.
- `ui/src/app/marketplace/actions/page.tsx`
- `ui/src/app/marketplace/actions/detail/page.tsx`
- `ui/src/app/marketplace/workflows/page.tsx`
- `ui/src/app/marketplace/workflows/detail/page.tsx`
- `ui/src/app/marketplace/_components/action-card.tsx`
- `ui/src/app/marketplace/_components/workflow-card.tsx`
- `ui/src/app/marketplace/_components/catalog-filters.tsx` — search input + category chips, shared by both grids.
- `ui/src/app/marketplace/_components/config-schema-table.tsx`
- `ui/src/app/marketplace/_components/copy-block.tsx`
- `ui/src/app/marketplace/_components/trigger-line.tsx`

**UI — modified:**
- `ui/src/lib/api/types.ts` — catalog wire types.
- `ui/src/lib/api/client.ts` — `listCatalog`.
- `ui/src/lib/api/hooks.ts` — `useCatalog` + `queryKeys.catalog`.
- `ui/src/components/app-shell/nav.ts` — one `Marketplace` entry.
- `ui/src/app/pipelines/new/page.tsx` — catalog fallback for `?from=` (the only behavioural change to existing code).

---
### Task 1: Catalog types and action entries

**Files:**
- Create: `crates/router/src/catalog.rs`
- Modify: `crates/router/src/lib.rs` (add `pub mod catalog;` after the `mime` module block, around line 30)
- Test: inline `#[cfg(test)] mod tests` in `crates/router/src/catalog.rs`

**Interfaces:**
- Consumes: `meili_ingest_plugin_sdk::{InputKind, OutputKind, PipelineDefinition}`; `crate::builtin_pipelines()`; `crate::{IN_REPO_PLUGINS, EXTERNAL_PLUGINS}` are in `meili-ingest-control-plane`, **not** in router — this task defines its own `ALL_KNOWN_PLUGINS` list locally and Task 2 asserts the two agree.
- Produces: `catalog::{ActionCategory, ActionEntry, WorkflowCategory, WorkflowEntry, Catalog, catalog}`. Task 3 serves `catalog()`. Task 4 mirrors these types in TypeScript.

- [ ] **Step 1: Write the failing test**

Create `crates/router/src/catalog.rs` with only the test module for now:

```rust
//! Curated catalog: authored copy describing every action and workflow the
//! system ships with.
//!
//! This is deliberately *not* part of `PluginManifest`. That type is implemented
//! by every third-party WASM and gRPC plugin author; the product's own showcase
//! copy has no business in it. Compiling the catalog in (rather than storing it)
//! means it is versioned with the code it describes and a test can assert every
//! entry names a plugin that actually exists.

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_action_entry_names_a_known_plugin() {
        for entry in catalog().actions {
            assert!(
                ALL_KNOWN_PLUGINS.contains(&entry.plugin.as_str()),
                "catalog describes unknown plugin `{}`",
                entry.plugin
            );
        }
    }

    #[test]
    fn every_known_plugin_has_exactly_one_entry() {
        let actions = catalog().actions;
        for name in ALL_KNOWN_PLUGINS {
            let count = actions.iter().filter(|e| e.plugin == *name).count();
            assert_eq!(count, 1, "plugin `{name}` has {count} catalog entries, want 1");
        }
    }

    #[test]
    fn every_action_entry_has_copy() {
        for entry in catalog().actions {
            assert!(!entry.title.is_empty(), "{} has no title", entry.plugin);
            assert!(!entry.summary.is_empty(), "{} has no summary", entry.plugin);
            assert!(
                entry.use_cases.len() >= 2,
                "{} has {} use cases, want at least 2",
                entry.plugin,
                entry.use_cases.len()
            );
            assert!(
                entry.example_step.contains(&entry.plugin),
                "{}'s example step does not name the plugin",
                entry.plugin
            );
        }
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-router catalog`
Expected: FAIL — compile error, `cannot find function catalog in this scope` (and `ALL_KNOWN_PLUGINS` not found). The module is not yet declared either, so also add `pub mod catalog;` to `crates/router/src/lib.rs` before running, or the file is never compiled.

- [ ] **Step 3: Write minimal implementation**

Add above the test module in `crates/router/src/catalog.rs`:

```rust
use meili_ingest_plugin_sdk::{InputKind, OutputKind, PipelineDefinition};
use serde::{Deserialize, Serialize};

/// Every plugin name the catalog is expected to describe: the 17 compiled into
/// the worker plus the 2 provided by external gRPC containers. Kept here rather
/// than imported because `meili-ingest-control-plane` depends on this crate, not
/// the other way round; a test in Task 2 asserts the two lists agree.
pub const ALL_KNOWN_PLUGINS: &[&str] = &[
    "pdf_extractor",
    "docx_extractor",
    "xlsx_extractor",
    "pptx_extractor",
    "html_extractor",
    "markdown_extractor",
    "csv_parser",
    "json_flattener",
    "msgpack_parser",
    "avro_parser",
    "parquet_parser",
    "video_audio_extractor",
    "chunker",
    "llm_enricher",
    "image_captioner",
    "whisper_transcriber",
    "ocr",
    "meili_indexer",
    "s3_downloader",
];

/// Where an action sits in the shape of a pipeline. Ordered left to right, so
/// the Actions grid doubles as a diagram of the mental model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ActionCategory {
    Fetch,
    Extract,
    Transform,
    Enrich,
    Index,
}

/// Authored showcase copy for one plugin.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionEntry {
    /// Joins to `PluginManifest.name`.
    pub plugin: String,
    /// Display name, e.g. "PDF extractor".
    pub title: String,
    pub category: ActionCategory,
    /// One line, card-sized.
    pub summary: String,
    /// Two or three concrete things people use it for.
    pub use_cases: Vec<String>,
    /// A copyable YAML step block.
    pub example_step: String,
    /// Fallback shown when no worker has registered a manifest.
    pub accepts: Vec<InputKind>,
    /// Fallback shown when no worker has registered a manifest.
    pub produces: OutputKind,
}

/// What a workflow is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WorkflowCategory {
    Documents,
    Data,
    Media,
    Web,
}

/// Authored showcase copy for one workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowEntry {
    /// `builtin.pdf`, or a curated template uid.
    pub uid: String,
    pub title: String,
    pub category: WorkflowCategory,
    pub summary: String,
    /// When you would reach for this one over its neighbours.
    pub when_to_use: String,
    /// `None` for `builtin.*`: the live `GET /pipelines` stays authoritative.
    /// `Some` for curated templates that are not deployed as pipelines.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub definition: Option<PipelineDefinition>,
}

/// The whole catalog, as `GET /catalog` returns it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    pub actions: Vec<ActionEntry>,
    pub workflows: Vec<WorkflowEntry>,
}

/// Terse constructor for an action entry.
fn action(
    plugin: &str,
    title: &str,
    category: ActionCategory,
    summary: &str,
    use_cases: &[&str],
    example_step: &str,
    accepts: &[InputKind],
    produces: OutputKind,
) -> ActionEntry {
    ActionEntry {
        plugin: plugin.to_owned(),
        title: title.to_owned(),
        category,
        summary: summary.to_owned(),
        use_cases: use_cases.iter().map(|s| (*s).to_owned()).collect(),
        example_step: example_step.trim_start_matches('\n').to_owned(),
        accepts: accepts.to_vec(),
        produces,
    }
}

/// Every action the system can run, in pipeline order.
fn actions() -> Vec<ActionEntry> {
    use ActionCategory::*;
    use InputKind as I;
    vec![
        action(
            "s3_downloader",
            "S3 downloader",
            Fetch,
            "Stream an object out of S3-compatible storage without buffering it in the gateway.",
            &[
                "Ingest a bucket export too large to POST through the gateway",
                "Keep credentials in the worker rather than in the request",
            ],
            "\n- id: fetch\n  plugin: s3_downloader\n  config:\n    bucket: my-exports\n    key: dumps/latest.jsonl\n",
            &[I::Ref, I::Empty],
            OutputKind::Bytes,
        ),
        action(
            "pdf_extractor",
            "PDF extractor",
            Extract,
            "Pull text out of a PDF, one document per page or one per file.",
            &[
                "Make a contract library searchable with page-level hits",
                "Index scientific papers while keeping page numbers",
            ],
            "\n- id: extract\n  plugin: pdf_extractor\n  config:\n    per_page: true\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "docx_extractor",
            "Word extractor",
            Extract,
            "Read text from Word documents, including tables and headers.",
            &[
                "Index a shared drive of .docx reports",
                "Search internal policy documents",
            ],
            "\n- id: extract\n  plugin: docx_extractor\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "xlsx_extractor",
            "Excel extractor",
            Extract,
            "Turn each spreadsheet row into its own document, with columns as fields.",
            &[
                "Make a product catalogue kept in Excel searchable",
                "Index an inventory export row by row",
            ],
            "\n- id: extract\n  plugin: xlsx_extractor\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "pptx_extractor",
            "PowerPoint extractor",
            Extract,
            "Extract the text of every slide, keeping slide order.",
            &[
                "Search a deck archive by what was actually said on a slide",
                "Index training material stored as presentations",
            ],
            "\n- id: extract\n  plugin: pptx_extractor\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "html_extractor",
            "HTML extractor",
            Extract,
            "Strip navigation and boilerplate, keeping the readable body of a page.",
            &[
                "Index a documentation site without its chrome",
                "Collect page links alongside the text",
            ],
            "\n- id: extract\n  plugin: html_extractor\n  config:\n    extract_links: true\n    include_meta_description: true\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "markdown_extractor",
            "Markdown extractor",
            Extract,
            "Split Markdown on its headings so each section becomes a document.",
            &[
                "Index a docs repository section by section",
                "Keep heading context on every search hit",
            ],
            "\n- id: extract\n  plugin: markdown_extractor\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "csv_parser",
            "CSV parser",
            Extract,
            "One document per row, with the delimiter sniffed when you do not say.",
            &[
                "Index a data export without converting it first",
                "Use an existing column as the document id",
            ],
            "\n- id: extract\n  plugin: csv_parser\n  config:\n    has_headers: true\n    id_column: sku\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "json_flattener",
            "JSON flattener",
            Extract,
            "Flatten nested JSON into the flat fields Meilisearch filters and facets on.",
            &[
                "Index an API dump with nested objects",
                "Make deep fields usable as filters",
            ],
            "\n- id: extract\n  plugin: json_flattener\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "msgpack_parser",
            "MessagePack parser",
            Extract,
            "Decode MessagePack records into documents.",
            &[
                "Ingest a compact binary export without a conversion step",
                "Index event dumps written by a MessagePack producer",
            ],
            "\n- id: extract\n  plugin: msgpack_parser\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "avro_parser",
            "Avro parser",
            Extract,
            "Read Avro container files, using the embedded schema for field names.",
            &[
                "Index a Kafka topic archived as Avro",
                "Ingest a data-lake export without a schema registry",
            ],
            "\n- id: extract\n  plugin: avro_parser\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "parquet_parser",
            "Parquet parser",
            Extract,
            "Read columnar Parquet files row by row into documents.",
            &[
                "Make an analytics export searchable",
                "Index a warehouse table dump directly",
            ],
            "\n- id: extract\n  plugin: parquet_parser\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "video_audio_extractor",
            "Video audio extractor",
            Extract,
            "Demux the audio track out of a video so it can be transcribed.",
            &[
                "Prepare recorded meetings for transcription",
                "Index a video library by what is said in it",
            ],
            "\n- id: audio\n  plugin: video_audio_extractor\n",
            &[I::Bytes],
            OutputKind::Bytes,
        ),
        action(
            "chunker",
            "Chunker",
            Transform,
            "Split long text into overlapping chunks sized for retrieval.",
            &[
                "Keep long documents under an embedding model's context limit",
                "Return the relevant passage instead of a whole file",
            ],
            "\n- id: chunk\n  plugin: chunker\n  config:\n    strategy: sentence\n    chunk_size: 512\n    overlap: 64\n",
            &[I::Documents],
            OutputKind::Documents,
        ),
        action(
            "llm_enricher",
            "LLM enricher",
            Enrich,
            "Call a language model per document to add titles, summaries, keywords or any JSON you prompt for.",
            &[
                "Generate summaries and keywords for search snippets",
                "Classify documents into facets you can filter on",
            ],
            "\n- id: enrich\n  plugin: llm_enricher\n  depends_on: [chunk]\n  fan_out: $.documents\n  config:\n    model: gpt-4o-mini\n    max_concurrent: 20\n    merge_strategy: merge\n",
            &[I::Documents],
            OutputKind::Documents,
        ),
        action(
            "image_captioner",
            "Image captioner",
            Enrich,
            "Describe an image with a vision model so it can be found by its content.",
            &[
                "Make a photo library searchable in words",
                "Caption product images for retrieval",
            ],
            "\n- id: caption\n  plugin: image_captioner\n  config:\n    detail: auto\n    json: true\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "whisper_transcriber",
            "Whisper transcriber",
            Enrich,
            "Transcribe speech to text, with an optional language hint and vocabulary prompt.",
            &[
                "Search podcasts and recorded calls by what was said",
                "Index lecture audio with timestamps",
            ],
            "\n- id: transcribe\n  plugin: whisper_transcriber\n  config:\n    language: en\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "ocr",
            "OCR",
            Enrich,
            "Read text off scanned pages and images that carry no text layer.",
            &[
                "Index scanned contracts and invoices",
                "Recover text from image-only PDFs",
            ],
            "\n- id: ocr\n  plugin: ocr\n",
            &[I::Bytes],
            OutputKind::Documents,
        ),
        action(
            "meili_indexer",
            "Meilisearch indexer",
            Index,
            "Write documents into the tenant's index. Every pipeline ends here.",
            &[
                "Upsert documents by id into a Meilisearch index",
                "Route content to a per-source index",
            ],
            "\n- id: index\n  plugin: meili_indexer\n",
            &[I::Documents],
            OutputKind::Indexed,
        ),
    ]
}

/// The curated catalog served by `GET /catalog`.
pub fn catalog() -> Catalog {
    Catalog {
        actions: actions(),
        workflows: workflows(),
    }
}
```

Task 2 adds `workflows()`. To keep this task's tests runnable on their own, add a temporary stub directly below `actions()`:

```rust
/// Replaced by the real table in Task 2.
fn workflows() -> Vec<WorkflowEntry> {
    vec![]
}
```

And in `crates/router/src/lib.rs`, directly after the `use` block near line 18:

```rust
pub mod catalog;
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo fmt && cargo test -p meili-ingest-router catalog`
Expected: PASS — 3 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/router/src/catalog.rs crates/router/src/lib.rs
git commit -m "$(cat <<'MSG'
feat(catalog): action catalog types and entries

Authored copy for all 19 known plugins, compiled into the router crate
next to builtin_pipelines(). Tests assert the catalog and the plugin
list cannot drift apart.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

### Task 2: Workflow entries

**Files:**
- Modify: `crates/router/src/catalog.rs` (replace the `workflows()` stub from Task 1)
- Test: inline `#[cfg(test)] mod tests` in the same file

**Interfaces:**
- Consumes: `ActionCategory`, `WorkflowCategory`, `WorkflowEntry`, `ALL_KNOWN_PLUGINS` from Task 1; `crate::builtin_pipelines()`.
- Produces: a non-empty `catalog().workflows` covering all 15 built-in uids. Task 4 mirrors it in TypeScript; Task 10 renders it.

- [ ] **Step 1: Write the failing tests**

Append to the existing `mod tests` in `crates/router/src/catalog.rs`:

```rust
    use crate::builtin_pipelines;

    #[test]
    fn every_builtin_pipeline_has_a_workflow_entry() {
        let workflows = catalog().workflows;
        for pipeline in builtin_pipelines() {
            let count = workflows.iter().filter(|w| w.uid == pipeline.uid).count();
            assert_eq!(
                count, 1,
                "built-in `{}` has {count} catalog entries, want 1",
                pipeline.uid
            );
        }
    }

    #[test]
    fn builtin_entries_carry_no_inline_definition() {
        // `GET /pipelines` stays authoritative for anything deployed; an inline
        // copy here would be a second source of truth that silently goes stale.
        for entry in catalog().workflows {
            if entry.uid.starts_with("builtin.") {
                assert!(
                    entry.definition.is_none(),
                    "{} duplicates a deployed definition",
                    entry.uid
                );
            }
        }
    }

    #[test]
    fn template_entries_carry_a_usable_definition() {
        for entry in catalog().workflows {
            if entry.uid.starts_with("builtin.") {
                continue;
            }
            let def = entry
                .definition
                .as_ref()
                .unwrap_or_else(|| panic!("template {} has no definition", entry.uid));
            assert_eq!(def.uid, entry.uid, "{} definition uid disagrees", entry.uid);
            assert!(!def.steps.is_empty(), "{} has no steps", entry.uid);
            for step in &def.steps {
                assert!(
                    ALL_KNOWN_PLUGINS.contains(&step.plugin.as_str()),
                    "template {} step `{}` names unknown plugin `{}`",
                    entry.uid,
                    step.id,
                    step.plugin
                );
            }
        }
    }

    #[test]
    fn every_workflow_entry_has_copy() {
        for entry in catalog().workflows {
            assert!(!entry.title.is_empty(), "{} has no title", entry.uid);
            assert!(!entry.summary.is_empty(), "{} has no summary", entry.uid);
            assert!(!entry.when_to_use.is_empty(), "{} has no when_to_use", entry.uid);
        }
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test -p meili-ingest-router catalog`
Expected: FAIL — `every_builtin_pipeline_has_a_workflow_entry` panics with `built-in 'builtin.pdf' has 0 catalog entries, want 1`, because `workflows()` still returns an empty vec.

- [ ] **Step 3: Write the implementation**

Replace the `workflows()` stub in `crates/router/src/catalog.rs`:

```rust
/// Terse constructor for a built-in workflow entry. Built-ins never carry an
/// inline definition — `GET /pipelines` is authoritative for anything deployed.
fn builtin_workflow(
    suffix: &str,
    title: &str,
    category: WorkflowCategory,
    summary: &str,
    when_to_use: &str,
) -> WorkflowEntry {
    WorkflowEntry {
        uid: format!("builtin.{suffix}"),
        title: title.to_owned(),
        category,
        summary: summary.to_owned(),
        when_to_use: when_to_use.to_owned(),
        definition: None,
    }
}

/// Every workflow the system ships with, in built-in table order.
fn workflows() -> Vec<WorkflowEntry> {
    use WorkflowCategory::*;
    vec![
        builtin_workflow(
            "pdf",
            "PDF",
            Documents,
            "Extract text per page, chunk it, index it.",
            "The default for any PDF. Page-level extraction keeps hits traceable to a page number.",
        ),
        builtin_workflow(
            "word",
            "Word",
            Documents,
            "Extract text from .doc and .docx, chunk it, index it.",
            "Word documents of any length; the chunk step keeps long reports retrievable.",
        ),
        builtin_workflow(
            "powerpoint",
            "PowerPoint",
            Documents,
            "Extract slide text and index it.",
            "Decks, where each slide is short enough that chunking would only add noise.",
        ),
        builtin_workflow(
            "markdown",
            "Markdown",
            Documents,
            "Split on headings and index each section.",
            "Documentation and READMEs, where headings are better boundaries than a fixed chunk size.",
        ),
        builtin_workflow(
            "text",
            "Plain text",
            Documents,
            "Chunk plain text and index it.",
            "Logs, transcripts and anything with no structure to exploit.",
        ),
        builtin_workflow(
            "excel",
            "Excel",
            Data,
            "One document per spreadsheet row.",
            "Tabular data where each row is a thing people search for, like a product or an order.",
        ),
        builtin_workflow(
            "csv",
            "CSV",
            Data,
            "One document per row, delimiter sniffed automatically.",
            "Exports from another system, when you would rather not convert the file first.",
        ),
        builtin_workflow(
            "json",
            "JSON",
            Data,
            "Flatten nested JSON and index it.",
            "API dumps, where nested fields need flattening before they can be filtered on.",
        ),
        builtin_workflow(
            "parquet",
            "Parquet",
            Data,
            "Read columnar Parquet rows and index them.",
            "Analytics and warehouse exports, without a conversion step.",
        ),
        builtin_workflow(
            "avro",
            "Avro",
            Data,
            "Read Avro container files using the embedded schema.",
            "Archived event streams, where the file carries its own schema.",
        ),
        builtin_workflow(
            "msgpack",
            "MessagePack",
            Data,
            "Decode MessagePack records and index them.",
            "Compact binary exports from a MessagePack producer.",
        ),
        builtin_workflow(
            "image",
            "Image",
            Media,
            "Caption the image with a vision model, then index the caption.",
            "Photo and product libraries you want to search with words.",
        ),
        builtin_workflow(
            "audio",
            "Audio",
            Media,
            "Transcribe speech, chunk the transcript, index it.",
            "Podcasts, calls and any recording where the words are the content.",
        ),
        builtin_workflow(
            "video",
            "Video",
            Media,
            "Demux the audio, transcribe it, chunk and index.",
            "Recorded meetings and video libraries, searched by what is said in them.",
        ),
        builtin_workflow(
            "html",
            "HTML",
            Web,
            "Strip boilerplate, chunk the readable body, index it.",
            "Crawled pages and documentation sites, without the navigation polluting results.",
        ),
        // The one curated template: a recipe nothing deploys, so it carries its
        // definition inline. It is what exercises the `Some(definition)` half of
        // `WorkflowEntry` and the clone path in Task 11.
        WorkflowEntry {
            uid: "pdf-with-enrichment".to_owned(),
            title: "PDF with LLM enrichment".to_owned(),
            category: Documents,
            summary: "Extract per page, chunk, enrich each chunk with an LLM, index.".to_owned(),
            when_to_use:
                "Contract and report libraries where generated summaries and keywords make search results readable."
                    .to_owned(),
            definition: Some(PipelineDefinition {
                uid: "pdf-with-enrichment".to_owned(),
                name: "PDF with LLM enrichment".to_owned(),
                description: Some("Extract, chunk, enrich, index.".to_owned()),
                version: 1,
                trigger: None,
                steps: vec![
                    crate::step("extract", "pdf_extractor"),
                    crate::chunk_step(),
                    crate::step("enrich", "llm_enricher")
                        .depends_on(["chunk"])
                        .fan_out("$.documents"),
                    crate::index_step(),
                ],
                builtin: false,
                project_id: None,
            }),
        },
    ]
}
```

The template reuses the built-in step helpers, which are private today. In
`crates/router/src/lib.rs`, change the three signatures at lines 443, 450 and 455
from `fn` to `pub(crate) fn`:

```rust
pub(crate) fn step(id: &str, plugin: &str) -> StepDefinition {
pub(crate) fn chunk_step() -> StepDefinition {
pub(crate) fn index_step() -> StepDefinition {
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo fmt && cargo test -p meili-ingest-router catalog`
Expected: PASS — 7 tests. `template_entries_carry_a_usable_definition` now has
the `pdf-with-enrichment` entry to check rather than passing vacuously.

- [ ] **Step 5: Verify the catalog plugin list matches the control plane's**

Add to `crates/control-plane/src/builtin_pipelines.rs`, inside its existing `#[cfg(test)] mod tests` (create the module if the file has none):

```rust
    #[test]
    fn catalog_describes_every_known_plugin() {
        // The catalog keeps its own list because router cannot depend on this
        // crate. This test is the joint that stops the two drifting.
        let mut catalog_names = meili_ingest_router::catalog::ALL_KNOWN_PLUGINS.to_vec();
        let mut known = builtin_plugin_names().to_vec();
        catalog_names.sort_unstable();
        known.sort_unstable();
        assert_eq!(catalog_names, known);
    }
```

Run: `cargo test -p meili-ingest-control-plane catalog_describes`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add crates/router/src/catalog.rs crates/control-plane/src/builtin_pipelines.rs
git commit -m "$(cat <<'MSG'
feat(catalog): workflow entries for the 15 built-in pipelines

Built-ins carry no inline definition: GET /pipelines stays authoritative
for anything deployed. A control-plane test pins the catalog's plugin
list to builtin_plugin_names().

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---
### Task 3: `GET /catalog` endpoint

**Files:**
- Create: `crates/gateway/src/handlers/catalog.rs`
- Modify: `crates/gateway/src/handlers/mod.rs` (add `pub mod catalog;`)
- Modify: `crates/gateway/src/lib.rs:74` (register the route next to `/plugins`)
- Modify: `docs/openapi.yaml` (add the `/catalog` path and three schemas)
- Test: inline `#[cfg(test)] mod tests` in `crates/gateway/src/handlers/catalog.rs`

**Interfaces:**
- Consumes: `meili_ingest_router::catalog::{Catalog, catalog}` from Tasks 1–2.
- Produces: `GET /catalog` → `200 {"actions": [...], "workflows": [...]}`. Task 4 fetches it.

- [ ] **Step 1: Write the failing test**

Create `crates/gateway/src/handlers/catalog.rs`:

```rust
//! `GET /catalog` — the curated action and workflow catalog.
//!
//! Unlike `/plugins`, this is not proxied: the catalog is static data compiled
//! into the binary with no control plane or database behind it. Serving it here
//! removes a hop and a 502 path from data that cannot fail to load.

#[cfg(test)]
mod tests {
    use crate::test_support::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;
    use wiremock::MockServer;

    #[tokio::test]
    async fn catalog_is_served() {
        let server = MockServer::start().await;
        let (app, _) = test_app(&server, GatewayConfig::default()).await;
        let resp = app
            .oneshot(Request::get("/catalog").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let json = json_body(resp).await;
        assert!(!json["actions"].as_array().unwrap().is_empty());
        assert!(!json["workflows"].as_array().unwrap().is_empty());
        assert_eq!(json["actions"][0]["category"], "fetch");
    }

    /// The catalog has no control plane behind it, so it answers even when the
    /// control plane is unreachable — the reason it is not proxied.
    #[tokio::test]
    async fn catalog_survives_a_dead_control_plane() {
        let cfg = GatewayConfig {
            control_plane_url: "http://127.0.0.1:9".into(),
            ..GatewayConfig::default()
        };
        let (app, _) = test_app_with_url(cfg, "http://127.0.0.1:9").await;
        let resp = app
            .oneshot(Request::get("/catalog").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test -p meili-ingest-gateway catalog`
Expected: FAIL — the module is not declared, so it does not compile. Add `pub mod catalog;` to `crates/gateway/src/handlers/mod.rs` first; then it fails with `404 Not Found` because the route is unregistered.

- [ ] **Step 3: Write the implementation**

Add above the test module in `crates/gateway/src/handlers/catalog.rs`:

```rust
use axum::Json;
use meili_ingest_router::catalog::{Catalog, catalog};

/// `GET /catalog`. Takes no state and cannot fail — the one infallible endpoint
/// in the gateway, hence no `Result`.
pub async fn get_catalog() -> Json<Catalog> {
    Json(catalog())
}
```

In `crates/gateway/src/handlers/mod.rs`, alphabetically before the existing modules:

```rust
pub mod catalog;
```

In `crates/gateway/src/lib.rs`, immediately after the `/plugins` route at line 74:

```rust
        .route("/catalog", get(handlers::catalog::get_catalog))
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cargo fmt && cargo test -p meili-ingest-gateway catalog`
Expected: PASS — 2 tests.

- [ ] **Step 5: Document the endpoint**

In `docs/openapi.yaml`, add after the `/plugins` path block (which starts at line 427):

```yaml
  /catalog:
    get:
      summary: Curated action and workflow catalog
      description: >
        Authored descriptions of every action the system can run and every
        workflow it ships with. Static data compiled into the gateway: identical
        for every caller, and unaffected by which workers have registered.
      operationId: getCatalog
      responses:
        "200":
          description: The catalog.
          content:
            application/json:
              schema:
                $ref: "#/components/schemas/Catalog"
```

And under `components: schemas:`, alongside the existing schemas:

```yaml
    Catalog:
      type: object
      required: [actions, workflows]
      properties:
        actions:
          type: array
          items: { $ref: "#/components/schemas/ActionEntry" }
        workflows:
          type: array
          items: { $ref: "#/components/schemas/WorkflowEntry" }
    ActionEntry:
      type: object
      required: [plugin, title, category, summary, use_cases, example_step, accepts, produces]
      properties:
        plugin:
          type: string
          description: Joins to PluginManifest.name.
        title: { type: string }
        category:
          type: string
          enum: [fetch, extract, transform, enrich, index]
        summary: { type: string }
        use_cases:
          type: array
          items: { type: string }
        example_step:
          type: string
          description: A copyable YAML step block.
        accepts:
          type: array
          items: { $ref: "#/components/schemas/InputKind" }
        produces: { $ref: "#/components/schemas/OutputKind" }
    WorkflowEntry:
      type: object
      required: [uid, title, category, summary, when_to_use]
      properties:
        uid: { type: string }
        title: { type: string }
        category:
          type: string
          enum: [documents, data, media, web]
        summary: { type: string }
        when_to_use: { type: string }
        definition:
          allOf: [{ $ref: "#/components/schemas/PipelineDefinition" }]
          description: >
            Present only for curated templates that are not deployed as
            pipelines. Absent for builtin.* entries, whose definition comes
            from GET /pipelines.
```

If `InputKind` and `OutputKind` are not already named schemas in the file, inline them as `{ type: string, enum: [bytes, ref, documents, many, empty] }` and `{ type: string, enum: [bytes, ref, documents, many, indexed, empty] }` respectively.

- [ ] **Step 6: Commit**

```bash
git add crates/gateway/src/handlers/catalog.rs crates/gateway/src/handlers/mod.rs crates/gateway/src/lib.rs docs/openapi.yaml
git commit -m "$(cat <<'MSG'
feat(gateway): serve GET /catalog

Static data straight from the router crate — no control-plane hop, no
database, no failure path. A test pins that it answers with the control
plane down.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

### Task 4: UI wire types, client and hook

**Files:**
- Modify: `ui/src/lib/api/types.ts` (append a Catalog section before the "Jobs" section)
- Modify: `ui/src/lib/api/client.ts` (append a Catalog section after the Plugins section)
- Modify: `ui/src/lib/api/hooks.ts` (add `queryKeys.catalog` and `useCatalog`)

**Interfaces:**
- Consumes: `GET /catalog` from Task 3.
- Produces: `ActionCategory`, `ActionEntry`, `WorkflowCategory`, `WorkflowEntry`, `Catalog` types; `listCatalog(signal?)`; `useCatalog()`. Tasks 5–11 all consume these.

- [ ] **Step 1: Add the wire types**

In `ui/src/lib/api/types.ts`, insert before the `// Jobs` divider comment:

```ts
// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/** `ActionCategory` — where an action sits in the shape of a pipeline. */
export type ActionCategory = "fetch" | "extract" | "transform" | "enrich" | "index";

/** The five categories in pipeline order, which is the order the grid renders. */
export const ACTION_CATEGORIES: ActionCategory[] = [
  "fetch",
  "extract",
  "transform",
  "enrich",
  "index",
];

/** `WorkflowCategory` — what a workflow is for. */
export type WorkflowCategory = "documents" | "data" | "media" | "web";

/** The four workflow categories, in grid order. */
export const WORKFLOW_CATEGORIES: WorkflowCategory[] = ["documents", "data", "media", "web"];

/**
 * `ActionEntry` — authored copy for one plugin.
 *
 * Deliberately separate from `PluginManifest`: that type is implemented by
 * third-party WASM and gRPC plugin authors and carries no product copy.
 */
export interface ActionEntry {
  /** Joins to `PluginManifest.name`. */
  plugin: string;
  title: string;
  category: ActionCategory;
  summary: string;
  use_cases: string[];
  /** A copyable YAML step block. */
  example_step: string;
  /** Fallback for when no worker has registered a manifest. */
  accepts: InputKind[];
  /** Fallback for when no worker has registered a manifest. */
  produces: OutputKind;
}

/** `WorkflowEntry` — authored copy for one workflow. */
export interface WorkflowEntry {
  /** `builtin.pdf`, or a curated template uid. */
  uid: string;
  title: string;
  category: WorkflowCategory;
  summary: string;
  when_to_use: string;
  /**
   * Present only for curated templates that are not deployed as pipelines.
   * Absent for `builtin.*`, whose definition comes from `GET /pipelines`.
   */
  definition?: PipelineDefinition;
}

/** `Catalog` — the body of `GET /catalog`. */
export interface Catalog {
  actions: ActionEntry[];
  workflows: WorkflowEntry[];
}
```

- [ ] **Step 2: Add the client function**

In `ui/src/lib/api/client.ts`, add `Catalog` to the type import list at the top, then append after the Plugins section:

```ts
// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/** `GET /catalog` — curated action and workflow copy, compiled into the gateway. */
export function listCatalog(signal?: AbortSignal): Promise<Catalog> {
  return request<Catalog>("/catalog", { signal });
}
```

- [ ] **Step 3: Add the hook**

In `ui/src/lib/api/hooks.ts`: add `listCatalog` to the import from `./client`, add `Catalog` to the import from `./types`, add the cache key, and add the hook after `usePlugins`:

```ts
  catalog: ["catalog"] as const,
```

```ts
/**
 * The curated catalog. Compiled into the gateway, so it changes only on
 * redeploy — cached as hard as the plugin manifests.
 */
export function useCatalog(): UseQueryResult<Catalog, Error> {
  return useQuery({
    queryKey: queryKeys.catalog,
    queryFn: ({ signal }) => listCatalog(signal),
    staleTime: 5 * 60 * 1000,
  });
}
```

- [ ] **Step 4: Verify it type-checks and lints**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit`
Expected: no errors.

- [ ] **Step 5: Commit**

```bash
git add ui/src/lib/api/types.ts ui/src/lib/api/client.ts ui/src/lib/api/hooks.ts
git commit -m "$(cat <<'MSG'
feat(ui): catalog wire types, client and hook

Mirrors crates/router/src/catalog.rs one-for-one in snake_case, as the
header of types.ts requires.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

### Task 5: The pure helpers — merge and neighbours

**Files:**
- Create: `ui/src/lib/catalog/merge.ts`
- Create: `ui/src/lib/catalog/merge.test.ts`
- Create: `ui/src/lib/catalog/neighbours.ts`
- Create: `ui/src/lib/catalog/neighbours.test.ts`

**Interfaces:**
- Consumes: `ActionEntry`, `PluginManifest`, `InputKind`, `OutputKind` from Task 4.
- Produces: `MergedAction` interface; `mergeActions(entries, manifests)`; `isStubManifest(manifest)`; `neighboursOf(action, all)` returning `{ canFollow: MergedAction[]; canFeed: MergedAction[] }`. Tasks 7 and 8 consume both.

- [ ] **Step 1: Write the failing merge tests**

Create `ui/src/lib/catalog/merge.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import type { ActionEntry, PluginManifest } from "@/lib/api/types";
import { isStubManifest, mergeActions } from "./merge";

const pdf: ActionEntry = {
  plugin: "pdf_extractor",
  title: "PDF extractor",
  category: "extract",
  summary: "Pull text out of a PDF.",
  use_cases: ["a", "b"],
  example_step: "- id: extract\n  plugin: pdf_extractor\n",
  accepts: ["bytes"],
  produces: "documents",
};

/** What `static_manifests()` produces before any worker registers. */
const stub: PluginManifest = {
  name: "pdf_extractor",
  version: "0.1.0",
  description: "",
  accepts: [],
  produces: "documents",
  config_schema: { type: "object" },
  kind: "builtin",
};

const full: PluginManifest = {
  ...stub,
  description: "Extract text from PDF documents.",
  accepts: ["bytes"],
  config_schema: {
    type: "object",
    properties: { per_page: { type: "boolean", default: true } },
  },
};

describe("isStubManifest", () => {
  it("recognises the name-and-kind fallback", () => {
    expect(isStubManifest(stub)).toBe(true);
  });

  it("does not flag a manifest a worker published", () => {
    expect(isStubManifest(full)).toBe(false);
  });
});

describe("mergeActions", () => {
  it("reports not-registered when no manifest exists", () => {
    const [merged] = mergeActions([pdf], []);
    expect(merged.registered).toBe(false);
    expect(merged.manifest).toBeUndefined();
    // Catalog values stand in so the card is never blank.
    expect(merged.accepts).toEqual(["bytes"]);
    expect(merged.produces).toBe("documents");
  });

  it("reports not-registered for a stub, which carries no schema to show", () => {
    const [merged] = mergeActions([pdf], [stub]);
    expect(merged.registered).toBe(false);
    expect(merged.manifest).toBeUndefined();
  });

  it("prefers live manifest values over catalog fallbacks", () => {
    const [merged] = mergeActions(
      [{ ...pdf, accepts: ["documents"], produces: "bytes" }],
      [full],
    );
    expect(merged.registered).toBe(true);
    expect(merged.manifest).toBe(full);
    expect(merged.accepts).toEqual(["bytes"]);
    expect(merged.produces).toBe("documents");
  });

  it("keeps catalog order and ignores manifests with no catalog entry", () => {
    const chunker: ActionEntry = { ...pdf, plugin: "chunker", title: "Chunker" };
    const merged = mergeActions([pdf, chunker], [{ ...full, name: "unknown_plugin" }]);
    expect(merged.map((m) => m.entry.plugin)).toEqual(["pdf_extractor", "chunker"]);
  });
});
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd ui && pnpm test merge`
Expected: FAIL — `Failed to resolve import "./merge"`.

- [ ] **Step 3: Write the merge implementation**

Create `ui/src/lib/catalog/merge.ts`:

```ts
/**
 * Joining the curated catalog to the live plugin registry.
 *
 * The catalog says what the product can do; `GET /plugins` says what this
 * deployment actually registered. Cards show both, which is the whole point of
 * the layering — pure functions here, rendering in the components.
 */
import type {
  ActionEntry,
  InputKind,
  JsonSchema,
  OutputKind,
  PluginManifest,
} from "@/lib/api/types";

/** One catalog entry joined with whatever the registry reports for it. */
export interface MergedAction {
  entry: ActionEntry;
  /** The registered manifest, when a worker published a real one. */
  manifest?: PluginManifest;
  /** True when a worker published a manifest carrying real detail. */
  registered: boolean;
  /** Live manifest values win; catalog values are the fallback. */
  accepts: InputKind[];
  /** Live manifest value wins; the catalog value is the fallback. */
  produces: OutputKind;
}

function hasProperties(schema: JsonSchema | undefined): boolean {
  return Object.keys(schema?.properties ?? {}).length > 0;
}

/**
 * Whether this is the name-and-kind placeholder the control plane synthesises
 * before any worker registers (`static_manifests()` in
 * `crates/control-plane/src/plugins.rs`).
 *
 * It matters because a stub carries no `config_schema`, so treating it as
 * registered would promise the detail page a schema it cannot render.
 */
export function isStubManifest(manifest: PluginManifest): boolean {
  return (
    !manifest.description &&
    (manifest.accepts ?? []).length === 0 &&
    !hasProperties(manifest.config_schema)
  );
}

/**
 * Join catalog entries to manifests, preserving catalog order.
 *
 * A manifest with no catalog entry is dropped: it is a plugin nobody wrote copy
 * for, and an untitled card is worse than no card. The Rust test
 * `every_known_plugin_has_exactly_one_entry` is what keeps that set empty.
 */
export function mergeActions(
  entries: ActionEntry[],
  manifests: PluginManifest[],
): MergedAction[] {
  const byName = new Map(manifests.map((manifest) => [manifest.name, manifest]));
  return entries.map((entry) => {
    const found = byName.get(entry.plugin);
    const manifest = found && !isStubManifest(found) ? found : undefined;
    return {
      entry,
      manifest,
      registered: manifest !== undefined,
      accepts: manifest?.accepts?.length ? manifest.accepts : entry.accepts,
      produces: manifest?.produces ?? entry.produces,
    };
  });
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd ui && pnpm test merge`
Expected: PASS — 6 tests.

- [ ] **Step 5: Write the failing neighbours tests**

Create `ui/src/lib/catalog/neighbours.test.ts`:

```ts
import { describe, expect, it } from "vitest";

import type { ActionEntry } from "@/lib/api/types";
import { mergeActions } from "./merge";
import { neighboursOf } from "./neighbours";

function entry(
  plugin: string,
  accepts: ActionEntry["accepts"],
  produces: ActionEntry["produces"],
): ActionEntry {
  return {
    plugin,
    title: plugin,
    category: "extract",
    summary: "s",
    use_cases: ["a", "b"],
    example_step: `- id: x\n  plugin: ${plugin}\n`,
    accepts,
    produces,
  };
}

const all = mergeActions(
  [
    entry("pdf_extractor", ["bytes"], "documents"),
    entry("chunker", ["documents"], "documents"),
    entry("meili_indexer", ["documents"], "indexed"),
    entry("s3_downloader", ["ref", "empty"], "bytes"),
  ],
  [],
);

function plugins(actions: { entry: ActionEntry }[]): string[] {
  return actions.map((action) => action.entry.plugin);
}

describe("neighboursOf", () => {
  it("finds what can feed a plugin and what it can feed", () => {
    const chunker = all.find((a) => a.entry.plugin === "chunker")!;
    const { canFollow, canFeed } = neighboursOf(chunker, all);
    // Everything producing `documents`, minus chunker itself.
    expect(plugins(canFollow)).toEqual(["pdf_extractor"]);
    // Everything accepting `documents`, minus chunker itself.
    expect(plugins(canFeed)).toEqual(["meili_indexer"]);
  });

  it("gives a terminal plugin no downstream", () => {
    const indexer = all.find((a) => a.entry.plugin === "meili_indexer")!;
    const { canFeed } = neighboursOf(indexer, all);
    // Nothing accepts `indexed`: every pipeline ends at the indexer.
    expect(canFeed).toEqual([]);
  });

  it("gives a source plugin no upstream", () => {
    const downloader = all.find((a) => a.entry.plugin === "s3_downloader")!;
    const { canFollow } = neighboursOf(downloader, all);
    // Nothing in this set produces `ref` or `empty`.
    expect(canFollow).toEqual([]);
  });

  it("never lists a plugin as its own neighbour", () => {
    const chunker = all.find((a) => a.entry.plugin === "chunker")!;
    const { canFollow, canFeed } = neighboursOf(chunker, all);
    expect(plugins(canFollow)).not.toContain("chunker");
    expect(plugins(canFeed)).not.toContain("chunker");
  });
});
```

- [ ] **Step 6: Run tests to verify they fail**

Run: `cd ui && pnpm test neighbours`
Expected: FAIL — `Failed to resolve import "./neighbours"`.

- [ ] **Step 7: Write the neighbours implementation**

Create `ui/src/lib/catalog/neighbours.ts`:

```ts
/**
 * What can precede and what can follow an action.
 *
 * The DAG's only compatibility rule is that a step's `accepts` must contain the
 * previous step's `produces`. Surfacing that turns the catalog from a list into
 * a composition aid.
 */
import type { MergedAction } from "./merge";

/** The actions adjacent to one action in a pipeline. */
export interface Neighbours {
  /** Actions whose output this one accepts — they can run before it. */
  canFollow: MergedAction[];
  /** Actions that accept this one's output — they can run after it. */
  canFeed: MergedAction[];
}

/** Compute both neighbour sets, preserving the order of `all`. */
export function neighboursOf(action: MergedAction, all: MergedAction[]): Neighbours {
  const others = all.filter((other) => other.entry.plugin !== action.entry.plugin);
  return {
    canFollow: others.filter((other) =>
      action.accepts.some((kind) => kind === other.produces),
    ),
    canFeed: others.filter((other) =>
      other.accepts.some((kind) => kind === action.produces),
    ),
  };
}
```

Note the `kind === other.produces` comparison: `InputKind` and `OutputKind` are separate string unions that overlap on `bytes`, `ref`, `documents`, `many` and `empty`. Comparing them directly is what the DAG does, and TypeScript allows it because the unions intersect.

- [ ] **Step 8: Run tests to verify they pass**

Run: `cd ui && pnpm test neighbours`
Expected: PASS — 4 tests.

- [ ] **Step 9: Commit**

```bash
git add ui/src/lib/catalog/
git commit -m "$(cat <<'MSG'
feat(ui): pure catalog helpers, merge and neighbours

mergeActions joins curated copy to the live registry, treating the
control plane's name-only stub as unregistered so the detail page never
promises a config schema it cannot render.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---
### Task 6: Marketplace shell — routes, layout, nav

**Files:**
- Create: `ui/src/app/marketplace/routes.ts`
- Create: `ui/src/app/marketplace/layout.tsx`
- Create: `ui/src/app/marketplace/page.tsx` (redirect to the Actions tab)
- Create: `ui/src/app/marketplace/_components/catalog-filters.tsx`
- Modify: `ui/src/components/app-shell/nav.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `MARKETPLACE_ACTIONS_HREF`, `MARKETPLACE_WORKFLOWS_HREF`, `actionDetailHref(plugin)`, `workflowDetailHref(uid)`; the `<CatalogFilters>` component with props `{ query, onQueryChange, categories, active, onToggle, placeholder, label }`. Tasks 7 and 9 use the filters; Tasks 7–10 use the hrefs.

- [ ] **Step 1: Write the route helpers**

Create `ui/src/app/marketplace/routes.ts`:

```ts
/**
 * Marketplace URLs.
 *
 * Same constraint as the pipeline editor and the jobs detail page: the UI is a
 * static export (`output: "export"`), so a dynamic segment would need a
 * pre-rendered file per plugin and per workflow. Detail pages are static routes
 * reading a query param — see `src/app/pipelines/routes.ts` for the trade-off
 * spelled out in full.
 */
export const MARKETPLACE_HREF = "/marketplace";
export const MARKETPLACE_ACTIONS_HREF = "/marketplace/actions";
export const MARKETPLACE_WORKFLOWS_HREF = "/marketplace/workflows";

/** Detail view of one action. */
export function actionDetailHref(plugin: string): string {
  return `/marketplace/actions/detail/?plugin=${encodeURIComponent(plugin)}`;
}

/** Detail view of one workflow. */
export function workflowDetailHref(uid: string): string {
  return `/marketplace/workflows/detail/?uid=${encodeURIComponent(uid)}`;
}
```

- [ ] **Step 2: Add the nav entry**

In `ui/src/components/app-shell/nav.ts`, add `Store` to the lucide import and insert this entry **first** in `NAV_ITEMS`, before Pipelines:

```ts
  {
    href: "/marketplace",
    label: "Marketplace",
    icon: Store,
    description: "Browse every action and workflow the system ships with",
  },
```

The href is `/marketplace`, not `/marketplace/actions`: `isActive` in `sidebar.tsx` matches on `pathname.startsWith(`${href}/`)`, so the parent path keeps the entry highlighted on both tabs. Also update the file's doc comment, which currently says "The four screens of the admin UI" — it is five now.

- [ ] **Step 3: Write the layout with the tab bar**

Create `ui/src/app/marketplace/layout.tsx`:

```tsx
"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { PageHeader } from "@/components/common/page-header";
import { cn } from "@/lib/utils";
import { MARKETPLACE_ACTIONS_HREF, MARKETPLACE_WORKFLOWS_HREF } from "./routes";

const TABS = [
  { href: MARKETPLACE_ACTIONS_HREF, label: "Actions" },
  { href: MARKETPLACE_WORKFLOWS_HREF, label: "Workflows" },
];

/**
 * Shell for both marketplace surfaces.
 *
 * The tabs are links rather than a `<Tabs>` component: each surface is its own
 * route with its own URL, so browser history and deep links work. Detail pages
 * live under these paths and render without the tab bar highlighting either —
 * which is correct, they are neither tab.
 */
export default function MarketplaceLayout({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <>
      <PageHeader
        title="Marketplace"
        description="Everything this deployment can run, and everything it ships with."
      />
      <div className="flex gap-1 border-b px-4">
        {TABS.map((tab) => {
          const active = pathname === tab.href;
          return (
            <Link
              key={tab.href}
              href={tab.href}
              aria-current={active ? "page" : undefined}
              className={cn(
                "-mb-px border-b-2 px-3 py-2 text-sm transition-colors",
                active
                  ? "border-primary font-medium text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground",
              )}
            >
              {tab.label}
            </Link>
          );
        })}
      </div>
      {children}
    </>
  );
}
```

- [ ] **Step 4: Write the index redirect**

Create `ui/src/app/marketplace/page.tsx`:

```tsx
"use client";

import { useRouter } from "next/navigation";
import { useEffect } from "react";

import { MARKETPLACE_ACTIONS_HREF } from "./routes";

/**
 * `/marketplace` opens on the Actions tab. A client-side redirect is what a
 * static export can do — there is no server to answer with a 307. Same pattern
 * as `src/app/page.tsx`.
 */
export default function MarketplacePage() {
  const router = useRouter();
  useEffect(() => {
    router.replace(MARKETPLACE_ACTIONS_HREF);
  }, [router]);
  return null;
}
```

- [ ] **Step 5: Write the shared filter bar**

Create `ui/src/app/marketplace/_components/catalog-filters.tsx`:

```tsx
"use client";

import { Search } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

/**
 * Search box plus category chips, shared by both grids.
 *
 * Filtering is client-side over a list of at most a few dozen entries that
 * arrives in one cached request — no debounce, no server round trip.
 */
export function CatalogFilters<T extends string>({
  query,
  onQueryChange,
  categories,
  active,
  onToggle,
  placeholder,
  label,
}: {
  query: string;
  onQueryChange: (value: string) => void;
  categories: readonly T[];
  /** Selected categories; empty means "all". */
  active: readonly T[];
  onToggle: (category: T) => void;
  placeholder: string;
  label: string;
}) {
  return (
    <div className="flex flex-wrap items-center gap-3 border-b px-4 py-3">
      <div className="relative min-w-56 flex-1">
        <Search
          className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground"
          aria-hidden
        />
        <Input
          type="search"
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder={placeholder}
          aria-label={label}
          className="pl-8"
        />
      </div>
      <div className="flex flex-wrap gap-1.5">
        {categories.map((category) => {
          const selected = active.includes(category);
          return (
            <button key={category} type="button" onClick={() => onToggle(category)}>
              <Badge
                variant={selected ? "default" : "outline"}
                className={cn("cursor-pointer capitalize", !selected && "hover:bg-muted")}
              >
                {category}
              </Badge>
            </button>
          );
        })}
      </div>
    </div>
  );
}
```

- [ ] **Step 6: Verify it builds**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit && pnpm build`
Expected: no errors; the build emits `/marketplace` as a static route. Visiting `/marketplace` in `pnpm dev` redirects to `/marketplace/actions`, which 404s until Task 7 — that is expected at this point.

- [ ] **Step 7: Commit**

```bash
git add ui/src/app/marketplace/ ui/src/components/app-shell/nav.ts
git commit -m "$(cat <<'MSG'
feat(ui): marketplace shell, routes and nav entry

Nav points at /marketplace rather than a tab so the sidebar stays
highlighted on both surfaces.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

### Task 7: Actions grid

**Files:**
- Create: `ui/src/app/marketplace/actions/page.tsx`
- Create: `ui/src/app/marketplace/_components/action-card.tsx`

**Interfaces:**
- Consumes: `useCatalog`, `usePlugins`, `errorMessage` (Task 4); `mergeActions`, `MergedAction` (Task 5); `actionDetailHref` (Task 6); `CatalogFilters` (Task 6); `ACTION_CATEGORIES`, `ActionCategory` (Task 4).
- Produces: `<ActionCard action={merged} />`, consumed only here.

- [ ] **Step 1: Write the card**

Create `ui/src/app/marketplace/_components/action-card.tsx`:

```tsx
import Link from "next/link";
import { ArrowRight } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { MergedAction } from "@/lib/catalog/merge";
import { actionDetailHref } from "../routes";

/** One action in the grid: curated copy, with live availability from the registry. */
export function ActionCard({ action }: { action: MergedAction }) {
  const { entry, registered, accepts, produces } = action;

  return (
    <Card className="transition-colors hover:border-primary/40">
      <CardHeader>
        <CardTitle className="flex items-start justify-between gap-2">
          <Link href={actionDetailHref(entry.plugin)} className="hover:underline">
            {entry.title}
          </Link>
          <Badge variant={registered ? "secondary" : "outline"}>
            {registered ? "Registered" : "Not deployed here"}
          </Badge>
        </CardTitle>
        <p className="font-mono text-xs text-muted-foreground">{entry.plugin}</p>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-sm text-muted-foreground">{entry.summary}</p>
        <div className="flex flex-wrap items-center gap-1.5 text-xs">
          {accepts.length > 0 ? (
            accepts.map((kind) => (
              <Badge key={kind} variant="outline" className="font-mono">
                {kind}
              </Badge>
            ))
          ) : (
            <Badge variant="outline" className="font-mono">
              nothing
            </Badge>
          )}
          <ArrowRight className="size-3 text-muted-foreground" aria-label="produces" />
          <Badge variant="outline" className="font-mono">
            {produces}
          </Badge>
        </div>
      </CardContent>
    </Card>
  );
}
```

- [ ] **Step 2: Write the grid page**

Create `ui/src/app/marketplace/actions/page.tsx`:

```tsx
"use client";

import { useMemo, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePlugins } from "@/lib/api/hooks";
import { ACTION_CATEGORIES, type ActionCategory } from "@/lib/api/types";
import { mergeActions, type MergedAction } from "@/lib/catalog/merge";
import { ActionCard } from "../_components/action-card";
import { CatalogFilters } from "../_components/catalog-filters";

/** Everything a card can be searched by. */
function haystack(action: MergedAction): string {
  const { entry } = action;
  return [entry.title, entry.plugin, entry.summary, ...entry.use_cases]
    .join(" ")
    .toLowerCase();
}

export default function ActionsPage() {
  const catalog = useCatalog();
  // The registry is a second, independent read: the catalog renders with or
  // without it, so a slow or failing /plugins never blocks the grid.
  const plugins = usePlugins();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState<ActionCategory[]>([]);

  const merged = useMemo(
    () => mergeActions(catalog.data?.actions ?? [], plugins.data ?? []),
    [catalog.data, plugins.data],
  );

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return merged.filter((action) => {
      const byCategory =
        active.length === 0 || active.includes(action.entry.category);
      const byQuery = needle === "" || haystack(action).includes(needle);
      return byCategory && byQuery;
    });
  }, [merged, query, active]);

  function toggle(category: ActionCategory) {
    setActive((current) =>
      current.includes(category)
        ? current.filter((item) => item !== category)
        : [...current, category],
    );
  }

  if (catalog.isPending) {
    return (
      <div className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 9 }, (_, index) => (
          <Skeleton key={index} className="h-40 w-full" />
        ))}
      </div>
    );
  }

  if (catalog.error) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>Could not load the catalog</AlertTitle>
          <AlertDescription>{errorMessage(catalog.error)}</AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <>
      <CatalogFilters
        query={query}
        onQueryChange={setQuery}
        categories={ACTION_CATEGORIES}
        active={active}
        onToggle={toggle}
        placeholder="Search actions…"
        label="Search actions"
      />
      <div className="p-4">
        {visible.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No action matches that search.
          </p>
        ) : (
          <div className="space-y-8">
            {ACTION_CATEGORIES.map((category) => {
              const section = visible.filter(
                (action) => action.entry.category === category,
              );
              if (section.length === 0) return null;
              return (
                <section key={category}>
                  <h2 className="mb-3 text-sm font-semibold tracking-tight capitalize">
                    {category}
                    <span className="ml-2 font-normal text-muted-foreground">
                      {section.length}
                    </span>
                  </h2>
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {section.map((action) => (
                      <ActionCard key={action.entry.plugin} action={action} />
                    ))}
                  </div>
                </section>
              );
            })}
          </div>
        )}
      </div>
    </>
  );
}
```

- [ ] **Step 3: Verify it builds and renders**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit && pnpm build`
Expected: no errors.

Then, with a gateway running (`docker compose watch` at the repo root, or `cargo run -p meili-ingest-gateway`), run `pnpm dev` and open `http://localhost:3000/marketplace/actions`. Expected: five category sections in the order Fetch, Extract, Transform, Enrich, Index; 19 cards total; typing "pdf" narrows to one; clicking the Extract chip shows only that section.

- [ ] **Step 4: Commit**

```bash
git add ui/src/app/marketplace/actions/page.tsx ui/src/app/marketplace/_components/action-card.tsx
git commit -m "$(cat <<'MSG'
feat(ui): actions marketplace grid

Five category sections in pipeline order, so the grid doubles as a
diagram of the mental model. Catalog and registry are independent reads:
a failing /plugins degrades the availability badge, not the page.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---
### Task 8: Action detail page

**Files:**
- Create: `ui/src/app/marketplace/actions/detail/page.tsx`
- Create: `ui/src/app/marketplace/_components/config-schema-table.tsx`
- Create: `ui/src/app/marketplace/_components/copy-block.tsx`

**Interfaces:**
- Consumes: `useCatalog`, `usePlugins`, `usePipelines`, `errorMessage` (Task 4); `mergeActions` (Task 5); `neighboursOf` (Task 5); `actionDetailHref`, `MARKETPLACE_ACTIONS_HREF` (Task 6); `editPipelineHref` from `@/app/pipelines/routes`.
- Produces: `<ConfigSchemaTable schema={...} />` and `<CopyBlock text={...} label={...} />`, both reused by Task 10.

- [ ] **Step 1: Write the copy block**

Create `ui/src/app/marketplace/_components/copy-block.tsx`:

```tsx
"use client";

import { Check, Copy } from "lucide-react";
import { useState } from "react";
import { toast } from "sonner";

import { Button } from "@/components/ui/button";

/** A read-only code block with a copy button. */
export function CopyBlock({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard access is denied in some browsers and over plain HTTP; say so
      // rather than leaving the button looking broken.
      toast.error("Could not copy", { description: "Select the text and copy it manually." });
    }
  }

  return (
    <div className="relative">
      <pre className="overflow-x-auto rounded-md border bg-muted/40 p-3 text-xs">
        <code>{text}</code>
      </pre>
      <Button
        type="button"
        size="sm"
        variant="ghost"
        onClick={copy}
        aria-label={label}
        className="absolute top-1.5 right-1.5"
      >
        {copied ? <Check className="size-3.5" aria-hidden /> : <Copy className="size-3.5" aria-hidden />}
      </Button>
    </div>
  );
}
```

- [ ] **Step 2: Write the config schema table**

Create `ui/src/app/marketplace/_components/config-schema-table.tsx`:

```tsx
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { JsonSchema } from "@/lib/api/types";

/** `["integer", "null"]` → `integer | null`. */
function typeLabel(schema: JsonSchema): string {
  const type = schema.type;
  if (Array.isArray(type)) return type.join(" | ");
  return type ?? "any";
}

function defaultLabel(schema: JsonSchema): string {
  if (!("default" in schema) || schema.default === undefined) return "—";
  return JSON.stringify(schema.default);
}

/**
 * A plugin's `config:` block, read-only.
 *
 * Properties marked `readOnly` are injected by the workflow at run time — that
 * is how `meili_indexer` declares `host`, `api_key` and `index`, one of which is
 * a secret. They are shown, because knowing they exist matters, but labelled so
 * nobody tries to set them by hand.
 */
export function ConfigSchemaTable({ schema }: { schema: JsonSchema | undefined }) {
  const properties = Object.entries(schema?.properties ?? {});
  const required = new Set(schema?.required ?? []);

  if (properties.length === 0) {
    return (
      <p className="text-sm text-muted-foreground">
        This action takes no configuration.
      </p>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead>Key</TableHead>
            <TableHead>Type</TableHead>
            <TableHead>Default</TableHead>
            <TableHead>Description</TableHead>
          </TableRow>
        </TableHeader>
        <TableBody>
          {properties.map(([key, property]) => (
            <TableRow key={key}>
              <TableCell className="align-top font-mono text-xs">
                {key}
                {required.has(key) ? (
                  <Badge variant="outline" className="ml-1.5">
                    required
                  </Badge>
                ) : null}
                {property.readOnly ? (
                  <Badge variant="secondary" className="ml-1.5">
                    set by the system
                  </Badge>
                ) : null}
              </TableCell>
              <TableCell className="align-top font-mono text-xs">
                {typeLabel(property)}
              </TableCell>
              <TableCell className="align-top font-mono text-xs">
                {defaultLabel(property)}
              </TableCell>
              <TableCell className="align-top text-xs text-muted-foreground">
                {property.description ?? "—"}
                {property.enum ? (
                  <span className="mt-1 block font-mono">
                    one of {property.enum.map((value) => JSON.stringify(value)).join(", ")}
                  </span>
                ) : null}
              </TableCell>
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
}
```

- [ ] **Step 3: Write the detail page**

Create `ui/src/app/marketplace/actions/detail/page.tsx`:

```tsx
"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { ArrowLeft } from "lucide-react";
import { Suspense, useMemo } from "react";

import { editPipelineHref } from "@/app/pipelines/routes";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePipelines, usePlugins } from "@/lib/api/hooks";
import { mergeActions, type MergedAction } from "@/lib/catalog/merge";
import { neighboursOf } from "@/lib/catalog/neighbours";
import { ConfigSchemaTable } from "../../_components/config-schema-table";
import { CopyBlock } from "../../_components/copy-block";
import { MARKETPLACE_ACTIONS_HREF, actionDetailHref } from "../../routes";

function NeighbourList({ title, actions }: { title: string; actions: MergedAction[] }) {
  return (
    <div>
      <h3 className="mb-2 text-xs font-medium text-muted-foreground">{title}</h3>
      {actions.length === 0 ? (
        <p className="text-sm text-muted-foreground">Nothing.</p>
      ) : (
        <div className="flex flex-wrap gap-1.5">
          {actions.map((action) => (
            <Link key={action.entry.plugin} href={actionDetailHref(action.entry.plugin)}>
              <Badge variant="outline" className="cursor-pointer font-mono hover:bg-muted">
                {action.entry.plugin}
              </Badge>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

function ActionDetail() {
  const plugin = useSearchParams().get("plugin") ?? "";
  const catalog = useCatalog();
  const plugins = usePlugins();
  const pipelines = usePipelines();

  const merged = useMemo(
    () => mergeActions(catalog.data?.actions ?? [], plugins.data ?? []),
    [catalog.data, plugins.data],
  );
  const action = merged.find((item) => item.entry.plugin === plugin);
  const neighbours = useMemo(
    () => (action ? neighboursOf(action, merged) : { canFollow: [], canFeed: [] }),
    [action, merged],
  );
  const usedBy = useMemo(
    () =>
      (pipelines.data ?? []).filter((pipeline) =>
        pipeline.steps.some((step) => step.plugin === plugin),
      ),
    [pipelines.data, plugin],
  );

  if (catalog.isPending) {
    return (
      <div className="space-y-3 p-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-32 w-full" />
      </div>
    );
  }

  if (catalog.error) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>Could not load the catalog</AlertTitle>
          <AlertDescription>{errorMessage(catalog.error)}</AlertDescription>
        </Alert>
      </div>
    );
  }

  if (!action) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>No such action</AlertTitle>
          <AlertDescription>
            The catalog has no entry for “{plugin}”.
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  const { entry, manifest, registered, accepts, produces } = action;

  return (
    <div className="space-y-6 p-4">
      <div>
        <Button asChild size="sm" variant="ghost" className="-ml-2 mb-2">
          <Link href={MARKETPLACE_ACTIONS_HREF}>
            <ArrowLeft aria-hidden />
            All actions
          </Link>
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold tracking-tight">{entry.title}</h2>
          <Badge variant="outline" className="capitalize">
            {entry.category}
          </Badge>
          {manifest?.kind ? <Badge variant="outline">{manifest.kind}</Badge> : null}
          <Badge variant={registered ? "secondary" : "outline"}>
            {registered ? "Registered" : "Not deployed here"}
          </Badge>
        </div>
        <p className="mt-1 font-mono text-xs text-muted-foreground">{entry.plugin}</p>
        <p className="mt-3 max-w-2xl text-sm text-muted-foreground">{entry.summary}</p>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">What it is for</h3>
        <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
          {entry.use_cases.map((useCase) => (
            <li key={useCase}>{useCase}</li>
          ))}
        </ul>
      </div>

      <Separator />

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">Configuration</h3>
        {registered ? (
          <ConfigSchemaTable schema={manifest?.config_schema} />
        ) : (
          <p className="text-sm text-muted-foreground">
            No worker in this deployment has registered {entry.plugin}. Its
            configuration appears here once one does.
          </p>
        )}
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">Example step</h3>
        <CopyBlock text={entry.example_step} label={`Copy the ${entry.plugin} step`} />
      </div>

      <Separator />

      <div>
        <h3 className="mb-3 text-sm font-semibold tracking-tight">
          Fits in a pipeline
          <span className="ml-2 font-mono text-xs font-normal text-muted-foreground">
            {accepts.join(", ") || "nothing"} → {produces}
          </span>
        </h3>
        <div className="grid gap-4 sm:grid-cols-2">
          <NeighbourList title="Can run before it" actions={neighbours.canFollow} />
          <NeighbourList title="Can run after it" actions={neighbours.canFeed} />
        </div>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">Used by</h3>
        {usedBy.length === 0 ? (
          <p className="text-sm text-muted-foreground">No pipeline uses it yet.</p>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {usedBy.map((pipeline) => (
              <Link key={pipeline.uid} href={editPipelineHref(pipeline.uid)}>
                <Badge variant="outline" className="cursor-pointer font-mono hover:bg-muted">
                  {pipeline.uid}
                </Badge>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default function ActionDetailPage() {
  // `useSearchParams` needs a Suspense boundary to prerender into a static file.
  return (
    <Suspense fallback={<Skeleton className="m-4 h-64" />}>
      <ActionDetail />
    </Suspense>
  );
}
```

- [ ] **Step 4: Verify it builds and renders**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit && pnpm build`
Expected: no errors.

With a gateway and at least one worker running, open `/marketplace/actions/detail/?plugin=chunker`. Expected: the config table lists `strategy`, `chunk_size` and `overlap` with their defaults; "Can run before it" lists the extractors; "Can run after it" includes `meili_indexer`. Open `?plugin=meili_indexer`: `host` and `api_key` carry the "set by the system" badge, and "Can run after it" is empty. Open `?plugin=nope`: the "No such action" alert.

- [ ] **Step 5: Commit**

```bash
git add ui/src/app/marketplace/actions/detail/ ui/src/app/marketplace/_components/config-schema-table.tsx ui/src/app/marketplace/_components/copy-block.tsx
git commit -m "$(cat <<'MSG'
feat(ui): action detail with config schema and neighbours

Neighbours come from the accepts/produces algebra, which is what turns
the catalog from a list into a composition aid. readOnly properties are
labelled as system-injected rather than hidden.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

### Task 9: Workflows grid

**Files:**
- Create: `ui/src/app/marketplace/workflows/page.tsx`
- Create: `ui/src/app/marketplace/_components/workflow-card.tsx`
- Create: `ui/src/app/marketplace/_components/trigger-line.tsx`

**Interfaces:**
- Consumes: `useCatalog`, `usePipelines`, `errorMessage` (Task 4); `CatalogFilters`, `workflowDetailHref` (Task 6); `WORKFLOW_CATEGORIES`, `WorkflowCategory`, `WorkflowEntry`, `PipelineDefinition`, `PipelineTrigger` (Task 4); `newPipelineHref` from `@/app/pipelines/routes`.
- Produces: `<TriggerLine trigger={...} />` reused by Task 10; `definitionFor(entry, pipelines)` exported from `workflow-card.tsx` and reused by Task 10.

- [ ] **Step 1: Write the trigger line**

Create `ui/src/app/marketplace/_components/trigger-line.tsx`:

```tsx
import type { PipelineTrigger } from "@/lib/api/types";

/**
 * How a workflow starts, in one line.
 *
 * This is the whole of the trigger story: rather than a third marketplace for
 * what is currently two concepts, each workflow states its own trigger where it
 * is actually authored.
 */
export function TriggerLine({ trigger }: { trigger: PipelineTrigger | undefined }) {
  const types = trigger?.content_types ?? [];
  const pattern = trigger?.filename_pattern;

  if (types.length === 0 && !pattern) {
    return (
      <p className="text-xs text-muted-foreground">
        Explicit call only — <code className="font-mono">POST /ingest/pipeline/…</code>
      </p>
    );
  }

  return (
    <p className="text-xs text-muted-foreground">
      Runs automatically for{" "}
      {types.map((type, index) => (
        <span key={type}>
          {index > 0 ? ", " : ""}
          <code className="font-mono">{type}</code>
        </span>
      ))}
      {pattern ? (
        <>
          {types.length > 0 ? " " : ""}matching <code className="font-mono">{pattern}</code>
        </>
      ) : null}
    </p>
  );
}
```

- [ ] **Step 2: Write the card**

Create `ui/src/app/marketplace/_components/workflow-card.tsx`:

```tsx
import Link from "next/link";
import { ChevronRight, Copy } from "lucide-react";

import { newPipelineHref } from "@/app/pipelines/routes";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import type { PipelineDefinition, WorkflowEntry } from "@/lib/api/types";
import { workflowDetailHref } from "../routes";
import { TriggerLine } from "./trigger-line";

/**
 * The definition behind a catalog entry.
 *
 * `builtin.*` entries carry none: the deployed pipeline is authoritative, so it
 * is looked up in the live list. Curated templates carry theirs inline because
 * nothing has deployed them.
 */
export function definitionFor(
  entry: WorkflowEntry,
  pipelines: PipelineDefinition[],
): PipelineDefinition | undefined {
  return entry.definition ?? pipelines.find((pipeline) => pipeline.uid === entry.uid);
}

/** One workflow in the grid. */
export function WorkflowCard({
  entry,
  definition,
}: {
  entry: WorkflowEntry;
  definition: PipelineDefinition | undefined;
}) {
  const isBuiltin = entry.uid.startsWith("builtin.");

  return (
    <Card className="flex flex-col transition-colors hover:border-primary/40">
      <CardHeader>
        <CardTitle className="flex items-start justify-between gap-2">
          <Link href={workflowDetailHref(entry.uid)} className="hover:underline">
            {entry.title}
          </Link>
          <Badge variant={isBuiltin ? "secondary" : "outline"}>
            {isBuiltin ? "Built-in" : "Template"}
          </Badge>
        </CardTitle>
        <p className="font-mono text-xs text-muted-foreground">{entry.uid}</p>
      </CardHeader>
      <CardContent className="flex-1 space-y-3">
        <p className="text-sm text-muted-foreground">{entry.summary}</p>
        <TriggerLine trigger={definition?.trigger} />
        <div className="flex flex-wrap items-center gap-1">
          {(definition?.steps ?? []).map((step, index) => (
            <span key={step.id} className="flex items-center gap-1">
              {index > 0 ? (
                <ChevronRight className="size-3 text-muted-foreground" aria-hidden />
              ) : null}
              <Badge variant="outline" className="font-mono">
                {step.plugin}
              </Badge>
            </span>
          ))}
        </div>
      </CardContent>
      <CardFooter>
        <Button asChild size="sm" variant="outline">
          <Link href={newPipelineHref(entry.uid)}>
            <Copy aria-hidden />
            Clone into my pipelines
          </Link>
        </Button>
      </CardFooter>
    </Card>
  );
}
```

- [ ] **Step 3: Write the grid page**

Create `ui/src/app/marketplace/workflows/page.tsx`:

```tsx
"use client";

import { useMemo, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePipelines } from "@/lib/api/hooks";
import { WORKFLOW_CATEGORIES, type WorkflowCategory } from "@/lib/api/types";
import { CatalogFilters } from "../_components/catalog-filters";
import { WorkflowCard, definitionFor } from "../_components/workflow-card";

export default function WorkflowsPage() {
  const catalog = useCatalog();
  const pipelines = usePipelines();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState<WorkflowCategory[]>([]);

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return (catalog.data?.workflows ?? []).filter((entry) => {
      const byCategory = active.length === 0 || active.includes(entry.category);
      const haystack = [entry.title, entry.uid, entry.summary, entry.when_to_use]
        .join(" ")
        .toLowerCase();
      return byCategory && (needle === "" || haystack.includes(needle));
    });
  }, [catalog.data, query, active]);

  function toggle(category: WorkflowCategory) {
    setActive((current) =>
      current.includes(category)
        ? current.filter((item) => item !== category)
        : [...current, category],
    );
  }

  if (catalog.isPending) {
    return (
      <div className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 9 }, (_, index) => (
          <Skeleton key={index} className="h-52 w-full" />
        ))}
      </div>
    );
  }

  if (catalog.error) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>Could not load the catalog</AlertTitle>
          <AlertDescription>{errorMessage(catalog.error)}</AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <>
      <CatalogFilters
        query={query}
        onQueryChange={setQuery}
        categories={WORKFLOW_CATEGORIES}
        active={active}
        onToggle={toggle}
        placeholder="Search workflows…"
        label="Search workflows"
      />
      <div className="p-4">
        {visible.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No workflow matches that search.
          </p>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {visible.map((entry) => (
              <WorkflowCard
                key={entry.uid}
                entry={entry}
                definition={definitionFor(entry, pipelines.data ?? [])}
              />
            ))}
          </div>
        )}
      </div>
    </>
  );
}
```

- [ ] **Step 4: Verify it builds and renders**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit && pnpm build`
Expected: no errors.

Open `/marketplace/workflows`. Expected: 16 cards — the 15 built-ins plus the
`pdf-with-enrichment` template, which carries a "Template" badge rather than
"Built-in" — each with a step sequence and a trigger line ("Runs automatically for `application/pdf`" on the PDF card). Clicking "Clone into my pipelines" on the PDF card lands in the editor with the PDF steps and a `builtin.pdf`-derived uid.

- [ ] **Step 5: Commit**

```bash
git add ui/src/app/marketplace/workflows/page.tsx ui/src/app/marketplace/_components/workflow-card.tsx ui/src/app/marketplace/_components/trigger-line.tsx
git commit -m "$(cat <<'MSG'
feat(ui): workflows marketplace grid

Each card states its own trigger, which is the whole trigger story: no
separate surface for what is currently two concepts.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---
### Task 10: Workflow detail page

**Files:**
- Create: `ui/src/app/marketplace/workflows/detail/page.tsx`

**Interfaces:**
- Consumes: `useCatalog`, `usePipelines`, `errorMessage` (Task 4); `definitionFor` (Task 9); `TriggerLine` (Task 9); `CopyBlock` (Task 8); `actionDetailHref`, `MARKETPLACE_WORKFLOWS_HREF` (Task 6); `newPipelineHref` from `@/app/pipelines/routes`.
- Produces: nothing consumed elsewhere.

- [ ] **Step 1: Write the detail page**

Create `ui/src/app/marketplace/workflows/detail/page.tsx`:

```tsx
"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { ArrowLeft, ChevronRight, Copy } from "lucide-react";
import { Suspense } from "react";

import { newPipelineHref } from "@/app/pipelines/routes";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePipelines } from "@/lib/api/hooks";
import { CopyBlock } from "../../_components/copy-block";
import { TriggerLine } from "../../_components/trigger-line";
import { definitionFor } from "../../_components/workflow-card";
import { MARKETPLACE_WORKFLOWS_HREF, actionDetailHref } from "../../routes";

function WorkflowDetail() {
  const uid = useSearchParams().get("uid") ?? "";
  const catalog = useCatalog();
  const pipelines = usePipelines();

  if (catalog.isPending) {
    return (
      <div className="space-y-3 p-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-48 w-full" />
      </div>
    );
  }

  if (catalog.error) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>Could not load the catalog</AlertTitle>
          <AlertDescription>{errorMessage(catalog.error)}</AlertDescription>
        </Alert>
      </div>
    );
  }

  const entry = catalog.data.workflows.find((item) => item.uid === uid);
  if (!entry) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>No such workflow</AlertTitle>
          <AlertDescription>The catalog has no entry for “{uid}”.</AlertDescription>
        </Alert>
      </div>
    );
  }

  const definition = definitionFor(entry, pipelines.data ?? []);
  const isBuiltin = entry.uid.startsWith("builtin.");

  return (
    <div className="space-y-6 p-4">
      <div>
        <Button asChild size="sm" variant="ghost" className="-ml-2 mb-2">
          <Link href={MARKETPLACE_WORKFLOWS_HREF}>
            <ArrowLeft aria-hidden />
            All workflows
          </Link>
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold tracking-tight">{entry.title}</h2>
          <Badge variant="outline" className="capitalize">
            {entry.category}
          </Badge>
          <Badge variant={isBuiltin ? "secondary" : "outline"}>
            {isBuiltin ? "Built-in" : "Template"}
          </Badge>
        </div>
        <p className="mt-1 font-mono text-xs text-muted-foreground">{entry.uid}</p>
        <p className="mt-3 max-w-2xl text-sm text-muted-foreground">{entry.summary}</p>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">When to use it</h3>
        <p className="max-w-2xl text-sm text-muted-foreground">{entry.when_to_use}</p>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">How it starts</h3>
        <TriggerLine trigger={definition?.trigger} />
        {definition?.trigger?.index_pattern ? (
          <p className="mt-1 text-xs text-muted-foreground">
            Writes to <code className="font-mono">{definition.trigger.index_pattern}</code>
          </p>
        ) : null}
      </div>

      <Separator />

      <div>
        <h3 className="mb-3 text-sm font-semibold tracking-tight">Steps</h3>
        {!definition ? (
          // A builtin.* entry whose pipeline the gateway did not return: an
          // older gateway, or a list that failed to load.
          <p className="text-sm text-muted-foreground">
            This deployment did not return a definition for {entry.uid}.
          </p>
        ) : (
          <ol className="space-y-3">
            {definition.steps.map((step, index) => (
              <li key={step.id} className="rounded-md border p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-xs text-muted-foreground">{index + 1}</span>
                  <span className="font-mono text-sm">{step.id}</span>
                  <ChevronRight className="size-3 text-muted-foreground" aria-hidden />
                  <Link href={actionDetailHref(step.plugin)}>
                    <Badge variant="outline" className="cursor-pointer font-mono hover:bg-muted">
                      {step.plugin}
                    </Badge>
                  </Link>
                  {step.fan_out ? (
                    <Badge variant="outline" className="font-mono">
                      fan out {step.fan_out}
                    </Badge>
                  ) : null}
                  {step.depends_on?.length ? (
                    <span className="text-xs text-muted-foreground">
                      after {step.depends_on.join(", ")}
                    </span>
                  ) : null}
                </div>
                {step.config && Object.keys(step.config).length > 0 ? (
                  <pre className="mt-2 overflow-x-auto rounded bg-muted/40 p-2 text-xs">
                    <code>{JSON.stringify(step.config, null, 2)}</code>
                  </pre>
                ) : null}
              </li>
            ))}
          </ol>
        )}
      </div>

      {definition ? (
        <div>
          <h3 className="mb-2 text-sm font-semibold tracking-tight">Definition</h3>
          <CopyBlock
            text={JSON.stringify(definition, null, 2)}
            label={`Copy the ${entry.uid} definition`}
          />
        </div>
      ) : null}

      <Button asChild size="sm">
        <Link href={newPipelineHref(entry.uid)}>
          <Copy aria-hidden />
          Clone into my pipelines
        </Link>
      </Button>
    </div>
  );
}

export default function WorkflowDetailPage() {
  // `useSearchParams` needs a Suspense boundary to prerender into a static file.
  return (
    <Suspense fallback={<Skeleton className="m-4 h-64" />}>
      <WorkflowDetail />
    </Suspense>
  );
}
```

- [ ] **Step 2: Verify it builds and renders**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit && pnpm build`
Expected: no errors.

Open `/marketplace/workflows/detail/?uid=builtin.pdf`. Expected: three numbered steps (extract → chunk → index), the chunker's config shown as JSON, each plugin badge linking into the action detail, and the trigger reading "Runs automatically for `application/pdf`".

- [ ] **Step 3: Commit**

```bash
git add ui/src/app/marketplace/workflows/detail/
git commit -m "$(cat <<'MSG'
feat(ui): workflow detail page

Steps link into action detail, so the two marketplaces navigate into
each other rather than sitting side by side.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

### Task 11: Clone a catalog template

**Files:**
- Modify: `ui/src/app/pipelines/new/page.tsx`

**Interfaces:**
- Consumes: `useCatalog` (Task 4); the existing `usePipeline`, `cloneDraft`, `pipelineToDraft`, `emptyDraft`.
- Produces: nothing consumed elsewhere. This is the last task; it closes the loop from marketplace to editor.

**Why:** `/pipelines/new?from=<uid>` resolves the uid through `usePipeline`, which 404s for a curated template that was never deployed. Built-in clones already work; this adds the template case without changing them.

- [ ] **Step 1: Reproduce the failure**

There is no page-level test harness in this repo — the UI tests cover pure
modules only — so this is a manual reproduction against the `pdf-with-enrichment`
template added in Task 2.

With the gateway running, open `/marketplace/workflows` and click "Clone into my
pipelines" on the **PDF with LLM enrichment** card.

Expected: the editor shows "Could not load pdf-with-enrichment" — a 404 from
`GET /pipelines/pdf-with-enrichment`, because nothing ever deployed that uid.
That is the bug this task fixes. Clone from a `builtin.*` card in the same
session and confirm it still works: that is the behaviour that must not regress.

- [ ] **Step 2: Write the implementation**

In `ui/src/app/pipelines/new/page.tsx`, replace the body of `NewPipeline` with:

```tsx
function NewPipeline() {
  // `?from=<uid>` seeds the draft from an existing pipeline or a catalog
  // template (the "Clone" action).
  const cloneFrom = useSearchParams().get("from") ?? undefined;
  const catalog = useCatalog();

  // Curated templates carry their definition inline; a `builtin.*` uid does not,
  // and is fetched. Waiting for the catalog before enabling the fetch is what
  // stops a template flashing a 404 on the way through.
  const template = cloneFrom
    ? catalog.data?.workflows.find((workflow) => workflow.uid === cloneFrom)?.definition
    : undefined;
  const source = usePipeline(!catalog.isPending && !template ? cloneFrom : undefined);

  const definition = template ?? source.data;
  // A disabled query reports `isPending` forever, so the template case must be
  // excluded explicitly rather than relying on `source.isPending` alone.
  const loading = Boolean(cloneFrom) && (catalog.isPending || (!template && source.isPending));
  const failed = !template && source.error;

  if (loading) {
    return (
      <>
        <PageHeader title="New pipeline" description={`Cloning ${cloneFrom}…`} />
        <EditorSkeleton />
      </>
    );
  }
  if (cloneFrom && failed) {
    return (
      <>
        <PageHeader title="New pipeline" />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>Could not load {cloneFrom}</AlertTitle>
            <AlertDescription>{errorMessage(source.error)}</AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  const initialDraft = definition ? cloneDraft(pipelineToDraft(definition)) : emptyDraft();

  return <PipelineEditor key={cloneFrom ?? "blank"} mode="create" initialDraft={initialDraft} />;
}
```

Add `useCatalog` to the existing import from `@/lib/api/hooks`.

- [ ] **Step 3: Verify the fix**

Run: `cd ui && pnpm lint && pnpm exec tsc --noEmit && pnpm build`
Expected: no errors.

Then check all four paths in `pnpm dev`:
1. `/pipelines/new` — a blank editor, no network call for a clone source.
2. `/pipelines/new/?from=builtin.pdf` — the PDF steps, uid suffixed by `cloneDraft`. (Unchanged behaviour; this is the regression check.)
3. `/pipelines/new/?from=pdf-with-enrichment` — the template's four steps, including the `enrich` step with its fan-out. No 404.
4. `/pipelines/new/?from=nope` — the "Could not load nope" alert.

- [ ] **Step 4: Run the full suite**

Run from the repo root:

```bash
cargo fmt --check && cargo test --workspace
cd ui && pnpm lint && pnpm test && pnpm build
```

Expected: all green.

- [ ] **Step 5: Commit**

```bash
git add ui/src/app/pipelines/new/page.tsx
git commit -m "$(cat <<'MSG'
feat(ui): clone a catalog template into the editor

Templates carry their definition inline, so GET /pipelines/{uid} 404s for
them. The editor now prefers the cached catalog and only fetches once the
catalog has settled, which also avoids a 404 flashing on the way through.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>
MSG
)"
```

---

## Manual verification checklist

Run once at the end, with the gateway and at least one worker up (`docker compose watch`):

- [ ] `/marketplace` redirects to `/marketplace/actions`; the sidebar highlights Marketplace on both tabs.
- [ ] Actions grid: five sections in the order Fetch, Extract, Transform, Enrich, Index; 19 cards.
- [ ] Stop the workers and reload: every card still renders, all badged "Not deployed here". This is the fresh-deployment case the catalog exists for.
- [ ] `?plugin=meili_indexer`: `host` and `api_key` carry "set by the system"; "Can run after it" is empty.
- [ ] Workflows grid: 16 cards (15 built-in, 1 template), each with a step sequence and a trigger line.
- [ ] Clone the `pdf-with-enrichment` template: the editor opens with its four steps, no 404.
- [ ] Clone from a built-in card lands in the editor with the right steps and saves.
- [ ] Both detail pages survive a hard reload (the static-export `<Suspense>` path).
- [ ] Dark mode: check both grids and both detail pages via the theme toggle.
