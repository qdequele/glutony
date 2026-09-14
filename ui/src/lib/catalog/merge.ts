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
