/**
 * Searching the plugin picker.
 *
 * The picker lists the registered manifests, dressed with catalog copy where a
 * catalog entry exists. Everything here is pure so ranking is testable.
 */
import type {
  ActionCategory,
  ActionEntry,
  InputKind,
  PluginManifest,
} from "@/lib/api/types";
import { acceptsInput } from "./validate";

export interface PluginOption {
  manifest: PluginManifest;
  /** Catalog title, or the plugin name when nobody wrote copy for it. */
  title: string;
  summary: string;
  category?: ActionCategory;
  useCases: string[];
  /** `undefined` when there is nothing to check against. */
  compatible?: boolean;
}

export function buildPluginOptions(
  plugins: PluginManifest[],
  actions: ActionEntry[],
  required: InputKind | undefined,
): PluginOption[] {
  const byPlugin = new Map(actions.map((entry) => [entry.plugin, entry]));
  return plugins.map((manifest) => {
    const entry = byPlugin.get(manifest.name);
    return {
      manifest,
      title: entry?.title ?? manifest.name,
      summary: manifest.description || entry?.summary || "",
      category: entry?.category,
      useCases: entry?.use_cases ?? [],
      compatible: required === undefined ? undefined : acceptsInput(manifest, required),
    };
  });
}

function score(option: PluginOption, tokens: string[]): number {
  if (tokens.length === 0) return 1;
  const name = option.manifest.name.toLowerCase();
  const title = option.title.toLowerCase();
  const rest = [
    option.summary,
    option.category ?? "",
    option.manifest.kind ?? "",
    option.manifest.produces,
    ...(option.manifest.accepts ?? []),
    ...(option.manifest.content_types ?? []),
    ...option.useCases,
  ]
    .join(" ")
    .toLowerCase();

  let total = 0;
  for (const token of tokens) {
    if (name === token) total += 100;
    else if (name.startsWith(token)) total += 40;
    else if (name.includes(token)) total += 25;
    else if (title.includes(token)) total += 20;
    else if (rest.includes(token)) total += 5;
    else return 0; // every token must match somewhere
  }
  return total;
}

/**
 * Filter and rank options.
 *
 * With a query, best match first; without one, the input order. Either way
 * compatible plugins come before incompatible ones.
 */
export function searchPluginOptions(
  options: PluginOption[],
  query: string,
  filters: { category?: ActionCategory; compatibleOnly?: boolean } = {},
): PluginOption[] {
  const tokens = query.toLowerCase().split(/\s+/).filter(Boolean);
  return options
    .map((option, position) => ({ option, position, score: score(option, tokens) }))
    .filter(({ option, score }) => {
      if (score === 0) return false;
      if (filters.category && option.category !== filters.category) return false;
      if (filters.compatibleOnly && option.compatible === false) return false;
      return true;
    })
    .sort((a, b) => {
      const fitA = a.option.compatible === false ? 1 : 0;
      const fitB = b.option.compatible === false ? 1 : 0;
      return fitA - fitB || b.score - a.score || a.position - b.position;
    })
    .map(({ option }) => option);
}
