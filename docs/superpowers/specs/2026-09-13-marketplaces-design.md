# Action & Workflow Marketplaces — Design

**Status:** approved, pending implementation plan
**Date:** 2026-09-13
**Scope:** two discovery surfaces in the admin UI — a catalog of every *action*
(plugin) the system can run, and a catalog of every *workflow* (pipeline) that ships
with it — backed by a curated catalog compiled into the gateway.

## Problem

Everything `meili-ingest` can do is invisible until you already know it exists.

The 17 in-repo plugins are reachable only through a `<Select>` inside the pipeline
editor (`ui/src/app/pipelines/_components/plugin-picker.tsx`). You cannot browse them,
compare them, read a plugin's config schema, or discover that `whisper_transcriber`
exists at all without opening the editor and scrolling a dropdown. The 15 built-in
pipelines (`crates/router/src/lib.rs`, `builtin_pipelines()`) render as undifferentiated
rows in the same table as user pipelines, with no indication of what each is *for*.

Worse, the live registry is not self-describing on a fresh deployment. `GET /plugins`
returns manifests that workers upsert at boot (`crates/control-plane/src/plugins.rs`,
`upsert_manifests`). Before any worker registers, the control plane falls back to
`static_manifests()` — **name and kind only**, with no description, no `accepts` /
`produces`, and no `config_schema`. A marketplace built purely on the live registry
would show seventeen near-empty cards to exactly the person who most needs to learn
what the product does.

This design adds the missing layer: authored catalog copy, compiled in, joined at
render time with whatever the live registry actually reports.

## Decisions

These resolve the design's forks. Where later sections appear to differ, this list wins.

1. **Layered, not either/or.** The marketplace is curated copy (the showcase) joined
   onto the live registry (the truth). A card shows the authored story *and* whether
   that plugin is registered in this deployment.
2. **No separate trigger marketplace.** Trigger kinds are thin today — MIME/filename
   auto-routing via `PipelineTrigger`, and explicit `POST /ingest/pipeline/:name`. They
   surface as one line on each workflow card, where they are actually authored. A
   third marketplace for two concepts would read as padding. Scheduled sources
   (`2026-09-13-scheduled-sources-design.md`) get a workflow-side surface when built.
3. **The catalog is compiled into the binary**, not stored in Postgres and not shipped
   as static files in the UI repo. It is versioned with the code it describes, it
   cannot drift per-deployment, and a Rust test can assert every entry names a real
   plugin.
4. **The gateway serves `/catalog` directly**, without proxying to the control plane.
   The catalog is pure static data with no DB behind it, and the gateway already
   depends on `meili-ingest-router` (`crates/gateway/Cargo.toml`). Proxying would add a
   hop and a 502 path for data that cannot fail to load.
5. **One endpoint, not two.** `GET /catalog` returns `{ actions, workflows }`. Both
   marketplace screens need it, it changes only when the binary changes, and splitting
   it would mean two hooks and two wire types for one cached blob.
6. **Catalog data lives in `meili-ingest-router`**, next to `builtin_pipelines()`, for
   the reason that module already gives: the gateway, the control plane and tests share
   one definition.
7. **`WorkflowEntry.definition` is optional.** `builtin.*` entries omit it and join to
   the live `GET /pipelines`, which stays authoritative for steps. Curated templates
   that are not deployed carry their definition inline. One type covers both halves of
   the layering without a union.
8. **Clone reuses the editor that exists.** `newPipelineHref(uid)` →
   `/pipelines/new/?from=<uid>` → `cloneDraft(pipelineToDraft(...))` already works
   (`ui/src/app/pipelines/new/page.tsx`). The only change is a catalog fallback for
   templates that `GET /pipelines/{uid}` does not know.
9. **One nav entry, two tabbed surfaces.** "Pipelines" already means *what I own*. A
   sibling entry called "Workflows" would be the same word twice. Marketplace means
   *what is possible*; Clone is the arrow between them.
10. **Query-param routes, not dynamic segments.** The UI is `output: "export"` — the
    constraint documented in `ui/src/app/pipelines/routes.ts`. Detail pages take
    `?plugin=` / `?uid=` and go through a `routes.ts` helper, as every other feature does.

### New dependencies

None. Rust reuses `serde` and the existing workspace crates; the UI reuses the shadcn
primitives already vendored (`card`, `badge`, `tabs`, `input`, `tooltip`).

## Data model

New module `crates/router/src/catalog.rs`, exported from the crate root alongside
`builtin_pipelines`.

```rust
/// Where a plugin sits in the shape of a pipeline. Ordered left to right, so the
/// Actions grid doubles as a diagram of the mental model.
pub enum ActionCategory { Fetch, Extract, Transform, Enrich, Index }

/// Authored showcase copy for one plugin. This is the product's own description,
/// deliberately not part of `PluginManifest`: that type is implemented by every
/// third-party WASM and gRPC plugin author and must not carry our marketing.
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
    /// Fallback for when no worker has registered a manifest.
    pub accepts: Vec<InputKind>,
    /// Fallback for when no worker has registered a manifest.
    pub produces: OutputKind,
}

pub enum WorkflowCategory { Documents, Data, Media, Web }

pub struct WorkflowEntry {
    /// `builtin.pdf`, or a template uid.
    pub uid: String,
    pub title: String,
    pub category: WorkflowCategory,
    pub summary: String,
    /// When you would reach for this one over its neighbours.
    pub when_to_use: String,
    /// `None` for `builtin.*`: the live `GET /pipelines` is authoritative.
    /// `Some` for curated templates that are not deployed as pipelines.
    pub definition: Option<PipelineDefinition>,
}

pub struct Catalog {
    pub actions: Vec<ActionEntry>,
    pub workflows: Vec<WorkflowEntry>,
}

pub fn catalog() -> Catalog;
```

All three structs derive `Serialize`/`Deserialize`; both enums serialize
`rename_all = "snake_case"`, matching every other wire enum in `plugin-sdk`.

### Category assignment

The five action categories cover all 19 known plugin names
(`IN_REPO_PLUGINS` + `EXTERNAL_PLUGINS`):

| Category | Plugins |
|---|---|
| Fetch | `s3_downloader` |
| Extract | `pdf_extractor`, `docx_extractor`, `xlsx_extractor`, `pptx_extractor`, `html_extractor`, `markdown_extractor`, `csv_parser`, `json_flattener`, `msgpack_parser`, `avro_parser`, `parquet_parser`, `video_audio_extractor` |
| Transform | `chunker` |
| Enrich | `llm_enricher`, `image_captioner`, `whisper_transcriber`, `ocr` |
| Index | `meili_indexer` |

The four workflow categories cover all 15 built-in uids exactly:

| Category | Built-in uids |
|---|---|
| Documents | `pdf`, `word`, `powerpoint`, `markdown`, `text` |
| Data | `excel`, `csv`, `json`, `parquet`, `avro`, `msgpack` |
| Media | `image`, `audio`, `video` |
| Web | `html` |

## API

`GET /catalog` → `200 application/json`, body `Catalog`. No auth beyond what the
gateway already applies, no tenant scoping: the catalog is identical for every caller.
Registered in `crates/gateway/src/lib.rs` next to `/plugins`:

```rust
.route("/catalog", get(handlers::catalog::get_catalog))
```

The handler (`crates/gateway/src/handlers/catalog.rs`) returns
`Json(meili_ingest_router::catalog::catalog())`. It takes no state and cannot fail, so
it has no `GatewayError` arm — the one endpoint in the gateway that is infallible.

`docs/openapi.yaml` gains the path and the three schemas.

## UI

### Routes

```
/marketplace/actions                       grid, five category sections
/marketplace/actions/detail/?plugin=…      one action
/marketplace/workflows                     grid
/marketplace/workflows/detail/?uid=…       one workflow
```

Files under `ui/src/app/marketplace/`, following the shape every other feature uses:
a `layout.tsx` holding the shared `PageHeader` and the Actions|Workflows tab bar, a
`routes.ts` with the four href helpers, per-route `page.tsx`, and `_components/`.
Detail pages wrap `useSearchParams` in `<Suspense>`, as `pipelines/new/page.tsx` does,
so they prerender into static files.

`NAV_ITEMS` (`ui/src/components/app-shell/nav.ts`) gains one entry: **Marketplace**,
icon `Store`, pointing at `/marketplace/actions`.

### Data flow

One new hook in `ui/src/lib/api/hooks.ts`:

```ts
export function useCatalog(): UseQueryResult<Catalog, Error>
```

keyed `queryKeys.catalog`, with the same hard `staleTime` as `usePlugins` — it changes
only on redeploy. `listCatalog` joins `client.ts`; `Catalog`, `ActionEntry`,
`WorkflowEntry` and the two category enums join `ui/src/lib/api/types.ts`, snake_case,
mirroring the Rust one-for-one as that file's header requires.

The merge is a pure function in `ui/src/lib/catalog/merge.ts`, unit-tested:

```ts
/** Catalog copy joined with whatever the live registry reports. */
export interface MergedAction {
  entry: ActionEntry;
  /** The registered manifest, when a worker published one. */
  manifest?: PluginManifest;
  /** True when a manifest with a real config schema is registered here. */
  registered: boolean;
  /** Live manifest values win; catalog values are the fallback. */
  accepts: InputKind[];
  produces: OutputKind;
}
```

`registered` is false both when the plugin is absent from `GET /plugins` and when it is
present only as a `static_manifests()` stub — the stub carries no `config_schema`, so
treating it as registered would promise a schema the detail page cannot render.

### Actions grid

A search `Input` filtering on title, plugin name, summary and use-cases, plus category
chips. Five sections in pipeline order. Each card: title, plugin name in mono, summary,
an `accepts → produces` pill pair, and an availability `Badge` — **Registered**, or
**Not deployed here** in the muted variant.

### Action detail

- Header: title, plugin name, category, and the manifest's `kind` (builtin/wasm/grpc)
  when known.
- Summary and use-cases.
- **Config schema**, read-only, from the live manifest: each property with its type,
  default, and description. Properties marked `readOnly` render as *set by the system*
  — that is how `meili_indexer` declares `host`, `api_key` and `index`, one of which is
  a secret. With no manifest registered, the section states that the schema appears
  once a worker registers, rather than rendering an empty table.
- **Compatible neighbours**, computed from the kind algebra in
  `ui/src/lib/catalog/neighbours.ts`: *can follow* = every action whose `produces` is in
  this one's `accepts`; *can feed* = every action whose `accepts` contains this one's
  `produces`. Both lists link onward, which is what makes the catalog compose rather
  than merely list.
- **Example step**, the `example_step` YAML in a copy-to-clipboard block.
- **Used by**, the built-in pipelines whose steps name this plugin, derived from
  `usePipelines()`.

### Workflows grid

Search and category chips, then cards: title, summary, the trigger line, step chips
naming the plugin sequence, and a Built-in / Template badge. Primary action:
**Clone into my pipelines**, a `<Link>` to `newPipelineHref(uid)`.

The trigger line is rendered from the entry's own `PipelineTrigger` and is the whole of
Decision 2: `content_types` present → "Runs automatically for `application/pdf`";
`filename_pattern` present → the glob alongside it; neither → "Explicit call only".
`index_pattern` shows on the detail page.

### Workflow detail

Ordered steps with each step's plugin, config, `depends_on` and `fan_out`; the full
trigger block including `index_pattern`; *when to use*; and Clone.

### The one change to existing code

`ui/src/app/pipelines/new/page.tsx` resolves `?from=` through `usePipeline`, which
404s for a curated template that was never deployed. It gains a catalog fallback:
prefer a `WorkflowEntry` with an inline `definition` from the already-cached catalog,
fall back to the live pipeline, and keep the existing error path for a uid that is
neither. Everything else in this design is additive.

## Testing

Following the split the repo already uses.

**Rust, in `catalog.rs`:**
- every `ActionEntry.plugin` satisfies `is_known_plugin` — the test that stops copy
  rotting when a plugin is renamed;
- every known plugin name has exactly one `ActionEntry` — the test that stops a new
  plugin shipping without catalog copy;
- every `WorkflowEntry` with a `builtin.` uid matches a uid in `builtin_pipelines()`,
  and carries `definition: None`;
- every `WorkflowEntry` with a non-`builtin.` uid carries `Some(definition)` whose
  steps name only known plugins.

**Rust, in `handlers/catalog.rs`:** `GET /catalog` returns 200 with both collections
non-empty, mirroring the `/plugins` handler test.

**Vitest:**
- `merge.test.ts` — catalog-only, catalog+full-manifest, and catalog+stub-manifest
  (the last must report `registered: false`);
- `neighbours.test.ts` — the kind algebra, including `meili_indexer` (produces
  `indexed`) having no downstream, and `empty` on both sides.

## Non-goals

- **Tenant-published templates.** The catalog is compiled in and identical for every
  caller. A publishable marketplace would need a Postgres table, CRUD routes and an
  auth model; nothing here forecloses it.
- **A trigger marketplace.** Deferred until scheduled sources exist (Decision 2).
- **Running from the marketplace.** No playground handoff and no curl snippets on
  workflow cards in v1; Clone is the single verb. Both are additive later.
- **Editing catalog copy at runtime.** It changes with a deploy, like the built-in
  pipelines it describes.
