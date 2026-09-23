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
  | "rename"
  | "drop"
  | "keep"
  | "copy"
  | "set"
  | "filter"
  | "trim"
  | "lowercase"
  | "uppercase"
  | "split"
  | "join"
  | "promote"
  | "round"
  | "cast"
  | "default";

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
    case "rename":
      return { rename: { from: "", to: "" } };
    case "drop":
      return { drop: [] };
    case "keep":
      return { keep: [] };
    case "copy":
      return { copy: { from: "", to: "" } };
    case "set":
      return { set: { field: "", expr: "" } };
    case "filter":
      return { filter: { drop_if: { field: "content", is: "empty" } } };
    case "trim":
      return { trim: [] };
    case "lowercase":
      return { lowercase: [] };
    case "uppercase":
      return { uppercase: [] };
    case "split":
      return { split: { field: "", separator: "," } };
    case "join":
      return { join: { field: "", separator: ", " } };
    case "promote":
      return { promote: { field: "", to: "title" } };
    case "round":
      return { round: { field: "", decimals: 2 } };
    case "cast":
      return { cast: { field: "", to: "number" } };
    case "default":
      return { default: { field: "", value: "" } };
    default: {
      const exhaustive: never = kind;
      throw new Error(`unhandled op kind: ${String(exhaustive)}`);
    }
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

/** What the round `decimals` input accepts while typing: an empty draft, or a whole number 0–15 to store. */
export type DecimalsDraft = { text: ""; value: undefined } | { text: string; value: number };

/** Parse typed `decimals` text; `undefined` means reject the keystroke. */
export function parseDecimals(raw: string): DecimalsDraft | undefined {
  const text = raw.trim();
  if (text === "") return { text: "", value: undefined };
  if (!/^\d{1,2}$/.test(text)) return undefined;
  const value = Number(text);
  return value <= 15 ? { text, value } : undefined;
}
