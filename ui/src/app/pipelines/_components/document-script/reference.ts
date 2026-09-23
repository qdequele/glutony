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
