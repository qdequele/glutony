/**
 * JSON Schema → form field descriptors.
 *
 * Plugin manifests ship a `config_schema` (see `crates/plugins/<name>/src/lib.rs`)
 * and the step editor renders the `config:` block from it. This module holds
 * the pure mapping; `./schema-form.tsx` renders the descriptors.
 *
 * Supported shapes, and what they become:
 *
 * | schema                                   | field kind |
 * |------------------------------------------|------------|
 * | `{"type": "string"}`                     | `string`   |
 * | `{"type": "string", "enum": [...]}`      | `enum`     |
 * | `{"type": "boolean"}`                    | `boolean`  |
 * | `{"type": "integer"}`                    | `integer`  |
 * | `{"type": "number"}`                     | `number`   |
 * | `{"type": ["integer", "null"]}`          | `integer`, nullable |
 * | anything else (arrays, objects, `oneOf`) | `json`     |
 *
 * Nothing is ever dropped: a property whose shape is not understood becomes a
 * JSON textarea, and config keys the schema does not mention are reported by
 * `unknownConfigKeys` so the editor can surface them too.
 */
import type { JsonObject, JsonSchema, JsonValue } from "@/lib/api/types";

/** The control the editor renders for a property. */
export type FieldKind = "string" | "enum" | "boolean" | "integer" | "number" | "json";

/** One rendered property of a plugin `config:` block. */
export interface SchemaField {
  /** Property name, i.e. the config key. */
  name: string;
  /** Humanized `name`, used as the label. */
  label: string;
  /** Which control to render. */
  kind: FieldKind;
  /** `description` from the schema, shown as helper text. */
  description?: string;
  /** Listed in the schema's `required`. */
  required: boolean;
  /** The declared type union includes `"null"`. */
  nullable: boolean;
  /** The schema `default`, if any. */
  default?: JsonValue;
  /** Placeholder showing the default that applies when the field is left empty. */
  placeholder?: string;
  /** Allowed values, for `kind: "enum"`. */
  options?: string[];
  /** Inclusive bounds, for numeric kinds. */
  min?: number;
  /** Inclusive bounds, for numeric kinds. */
  max?: number;
  /**
   * The schema `format`, for string fields. The form renders a caller-supplied
   * widget for a format it was given one for (`meili-connection` → a Select of
   * the tenant's connections), and a plain input otherwise.
   */
  format?: string;
  /** The original property schema, kept for the JSON fallback. */
  schema: JsonSchema;
}

/** Split a `type` keyword into its non-null members plus a nullable flag. */
export function normalizeTypes(type: JsonSchema["type"]): {
  types: string[];
  nullable: boolean;
} {
  const declared = type === undefined ? [] : Array.isArray(type) ? type : [type];
  const nullable = declared.includes("null");
  return { types: declared.filter((t) => t !== "null"), nullable };
}

/** `max_input_chars` → `Max input chars`. */
export function humanizeName(name: string): string {
  const words = name.replace(/[_-]+/g, " ").replace(/([a-z0-9])([A-Z])/g, "$1 $2").trim();
  if (words.length === 0) return name;
  return words.charAt(0).toUpperCase() + words.slice(1);
}

function placeholderFor(hasDefault: boolean, value: JsonValue | undefined): string | undefined {
  if (!hasDefault) return undefined;
  if (value === null) return "null";
  if (typeof value === "string") return value.length === 0 ? '""' : value;
  if (typeof value === "boolean" || typeof value === "number") return String(value);
  return JSON.stringify(value);
}

function kindFor(types: string[], schema: JsonSchema): FieldKind {
  // A closed set of string choices is a Select, whatever else the schema says.
  if (Array.isArray(schema.enum) && schema.enum.length > 0) {
    return schema.enum.every((option) => typeof option === "string") ? "enum" : "json";
  }
  // Unions of two real types (or no type at all) are not renderable as one control.
  if (types.length !== 1) return "json";
  switch (types[0]) {
    case "string":
      return "string";
    case "boolean":
      return "boolean";
    case "integer":
      return "integer";
    case "number":
      return "number";
    default:
      // array, object, and anything unknown.
      return "json";
  }
}

/** Turn one property schema into a field descriptor. */
export function schemaToField(
  name: string,
  schema: JsonSchema,
  required: boolean = false,
): SchemaField {
  const { types, nullable } = normalizeTypes(schema.type);
  const kind = kindFor(types, schema);
  const hasDefault = Object.prototype.hasOwnProperty.call(schema, "default");

  const field: SchemaField = {
    name,
    label: humanizeName(name),
    kind,
    required,
    nullable,
    schema,
  };

  if (typeof schema.description === "string" && schema.description.length > 0) {
    field.description = schema.description;
  }
  if (hasDefault) {
    field.default = schema.default;
    const placeholder = placeholderFor(hasDefault, schema.default);
    if (placeholder !== undefined) field.placeholder = placeholder;
  }
  if (kind === "enum" && Array.isArray(schema.enum)) {
    field.options = schema.enum.filter((o): o is string => typeof o === "string");
  }
  if (kind === "integer" || kind === "number") {
    if (typeof schema.minimum === "number") field.min = schema.minimum;
    if (typeof schema.maximum === "number") field.max = schema.maximum;
  }
  if (kind === "string" && typeof schema.format === "string" && schema.format.length > 0) {
    field.format = schema.format;
  }

  return field;
}

/**
 * Turn a plugin `config_schema` into an ordered list of fields.
 *
 * Returns `[]` for a schema with no `properties` (`{"type": "object"}` is the
 * SDK default) — the caller then offers a raw JSON editor for the whole block.
 */
export function schemaToFields(schema: JsonSchema | undefined): SchemaField[] {
  const properties = schema?.properties;
  if (!properties || typeof properties !== "object") return [];
  const required = Array.isArray(schema?.required) ? schema.required : [];
  return Object.entries(properties)
    // `readOnly` properties are injected by the runtime, not authored: the indexer's
    // host, api_key and index come from the tenant context when the step runs.
    // Rendering them would ask the author to type credentials they must not know.
    .filter(([, property]) => property?.readOnly !== true)
    .map(([name, property]) => schemaToField(name, property ?? {}, required.includes(name)));
}

/**
 * Config keys that the schema does not describe. They must be shown (in the
 * raw JSON escape hatch) rather than silently dropped on the next save.
 */
export function unknownConfigKeys(
  schema: JsonSchema | undefined,
  config: JsonObject | undefined,
): string[] {
  if (!config) return [];
  const known = new Set(Object.keys(schema?.properties ?? {}));
  return Object.keys(config).filter((key) => !known.has(key));
}

/**
 * Parse what a text input holds into the JSON value the field should carry.
 *
 * An empty string clears the key (`undefined`) so the plugin's own default
 * applies, except on a nullable field, where it means an explicit `null`
 * when the schema defaults to `null`.
 */
export function parseFieldInput(
  field: SchemaField,
  raw: string,
): { ok: true; value: JsonValue | undefined } | { ok: false; error: string } {
  if (raw.trim().length === 0) {
    if (field.nullable && field.default === null) return { ok: true, value: null };
    return { ok: true, value: undefined };
  }
  switch (field.kind) {
    case "integer": {
      if (!/^-?\d+$/.test(raw.trim())) return { ok: false, error: "must be a whole number" };
      const value = Number(raw);
      if (field.min !== undefined && value < field.min) {
        return { ok: false, error: `must be >= ${field.min}` };
      }
      if (field.max !== undefined && value > field.max) {
        return { ok: false, error: `must be <= ${field.max}` };
      }
      return { ok: true, value };
    }
    case "number": {
      const value = Number(raw);
      if (!Number.isFinite(value)) return { ok: false, error: "must be a number" };
      if (field.min !== undefined && value < field.min) {
        return { ok: false, error: `must be >= ${field.min}` };
      }
      if (field.max !== undefined && value > field.max) {
        return { ok: false, error: `must be <= ${field.max}` };
      }
      return { ok: true, value };
    }
    case "json": {
      try {
        return { ok: true, value: JSON.parse(raw) as JsonValue };
      } catch (error) {
        return { ok: false, error: error instanceof Error ? error.message : "invalid JSON" };
      }
    }
    default:
      return { ok: true, value: raw };
  }
}

/** Render a stored config value back into the text an input should show. */
export function formatFieldValue(field: SchemaField, value: JsonValue | undefined): string {
  if (value === undefined) return "";
  if (value === null) return field.kind === "json" ? "null" : "";
  if (field.kind === "json") return JSON.stringify(value, null, 2);
  if (typeof value === "string") return value;
  return String(value);
}
