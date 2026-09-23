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
