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
