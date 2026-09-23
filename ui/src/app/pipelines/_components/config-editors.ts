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
