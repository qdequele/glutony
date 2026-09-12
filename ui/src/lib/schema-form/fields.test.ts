import { describe, expect, it } from "vitest";

import type { JsonSchema } from "@/lib/api/types";
import {
  formatFieldValue,
  humanizeName,
  normalizeTypes,
  parseFieldInput,
  schemaToField,
  schemaToFields,
  unknownConfigKeys,
} from "./fields";

/** The real `chunker` config schema, copied from crates/plugins/chunker. */
const CHUNKER_SCHEMA: JsonSchema = {
  $schema: "https://json-schema.org/draft/2020-12/schema",
  type: "object",
  additionalProperties: false,
  properties: {
    strategy: {
      type: "string",
      enum: ["sentence", "fixed", "paragraph"],
      default: "sentence",
      description: "How to find chunk boundaries.",
    },
    chunk_size: { type: "integer", minimum: 1, default: 512 },
    overlap: { type: "integer", minimum: 0, default: 64 },
    keep_parent_fields: { type: "boolean", default: true },
  },
};

describe("normalizeTypes", () => {
  it("reads a plain type", () => {
    expect(normalizeTypes("string")).toEqual({ types: ["string"], nullable: false });
  });

  it("splits a nullable union", () => {
    expect(normalizeTypes(["integer", "null"])).toEqual({ types: ["integer"], nullable: true });
  });

  it("treats a missing type as unknown", () => {
    expect(normalizeTypes(undefined)).toEqual({ types: [], nullable: false });
  });
});

describe("humanizeName", () => {
  it("turns snake_case into a label", () => {
    expect(humanizeName("max_input_chars")).toBe("Max input chars");
  });
});

describe("schemaToField", () => {
  it("maps string → Input", () => {
    const field = schemaToField("model", { type: "string", description: "Model name" });
    expect(field.kind).toBe("string");
    expect(field.description).toBe("Model name");
    expect(field.nullable).toBe(false);
  });

  it("maps string + enum → Select and keeps the options", () => {
    const field = schemaToField("merge_strategy", {
      type: "string",
      enum: ["merge", "replace"],
      default: "merge",
    });
    expect(field.kind).toBe("enum");
    expect(field.options).toEqual(["merge", "replace"]);
    expect(field.placeholder).toBe("merge");
  });

  it("maps boolean → Switch", () => {
    expect(schemaToField("per_page", { type: "boolean", default: true }).kind).toBe("boolean");
  });

  it("maps integer → numeric Input and carries the bounds", () => {
    const field = schemaToField("chunk_size", { type: "integer", minimum: 1, maximum: 4096 });
    expect(field.kind).toBe("integer");
    expect(field.min).toBe(1);
    expect(field.max).toBe(4096);
  });

  it("maps number → numeric Input", () => {
    const field = schemaToField("temperature", { type: "number", minimum: 0, maximum: 2 });
    expect(field.kind).toBe("number");
    expect(field.max).toBe(2);
  });

  it("maps a nullable integer to an optional field whose placeholder shows the default", () => {
    const field = schemaToField("max_pages", {
      type: ["integer", "null"],
      minimum: 1,
      default: null,
      description: "Stop reading after this many pages.",
    });
    expect(field.kind).toBe("integer");
    expect(field.nullable).toBe(true);
    expect(field.required).toBe(false);
    expect(field.placeholder).toBe("null");
  });

  it("falls back to a JSON textarea for an array", () => {
    const field = schemaToField("content_columns", {
      type: ["array", "null"],
      items: { type: "string" },
      default: null,
    });
    expect(field.kind).toBe("json");
  });

  it("falls back to a JSON textarea for an object", () => {
    expect(schemaToField("extra", { type: "object" }).kind).toBe("json");
  });

  it("falls back to a JSON textarea for a union of two real types", () => {
    expect(schemaToField("weird", { type: ["string", "integer"] }).kind).toBe("json");
  });

  it("falls back to a JSON textarea for a non-string enum", () => {
    expect(schemaToField("level", { enum: [1, 2, 3] }).kind).toBe("json");
  });

  it("falls back to a JSON textarea when no type is declared", () => {
    expect(schemaToField("mystery", { description: "?" }).kind).toBe("json");
  });

  it("marks required properties", () => {
    expect(schemaToField("host", { type: "string" }, true).required).toBe(true);
  });

  it("has no placeholder when the schema declares no default", () => {
    expect(schemaToField("language", { type: "string" }).placeholder).toBeUndefined();
  });
});

describe("schemaToFields", () => {
  it("keeps declaration order", () => {
    expect(schemaToFields(CHUNKER_SCHEMA).map((f) => f.name)).toEqual([
      "strategy",
      "chunk_size",
      "overlap",
      "keep_parent_fields",
    ]);
  });

  it("returns nothing for the SDK's default schema", () => {
    expect(schemaToFields({ type: "object" })).toEqual([]);
    expect(schemaToFields(undefined)).toEqual([]);
  });
});

describe("unknownConfigKeys", () => {
  it("reports config keys the schema does not describe", () => {
    expect(unknownConfigKeys(CHUNKER_SCHEMA, { chunk_size: 256, legacy_mode: true })).toEqual([
      "legacy_mode",
    ]);
  });

  it("reports every key when the schema has no properties", () => {
    expect(unknownConfigKeys({ type: "object" }, { a: 1 })).toEqual(["a"]);
  });
});

describe("parseFieldInput", () => {
  const integer = schemaToField("chunk_size", { type: "integer", minimum: 1, maximum: 10 });
  const nullableInteger = schemaToField("max_pages", { type: ["integer", "null"], default: null });
  const json = schemaToField("content_columns", { type: "array" });

  it("clears the key when a plain field is emptied", () => {
    expect(parseFieldInput(integer, "  ")).toEqual({ ok: true, value: undefined });
  });

  it("writes an explicit null when a null-defaulting field is emptied", () => {
    expect(parseFieldInput(nullableInteger, "")).toEqual({ ok: true, value: null });
  });

  it("rejects a non-integer", () => {
    expect(parseFieldInput(integer, "1.5").ok).toBe(false);
  });

  it("enforces the bounds", () => {
    expect(parseFieldInput(integer, "0")).toEqual({ ok: false, error: "must be >= 1" });
    expect(parseFieldInput(integer, "11")).toEqual({ ok: false, error: "must be <= 10" });
    expect(parseFieldInput(integer, "5")).toEqual({ ok: true, value: 5 });
  });

  it("parses the JSON fallback", () => {
    expect(parseFieldInput(json, '["a","b"]')).toEqual({ ok: true, value: ["a", "b"] });
    expect(parseFieldInput(json, "[oops").ok).toBe(false);
  });
});

describe("formatFieldValue", () => {
  const json = schemaToField("content_columns", { type: "array" });
  const text = schemaToField("model", { type: "string" });

  it("pretty-prints the JSON fallback", () => {
    expect(formatFieldValue(json, ["a"])).toBe('[\n  "a"\n]');
  });

  it("renders a missing value as an empty input", () => {
    expect(formatFieldValue(text, undefined)).toBe("");
  });

  it("skips injected readOnly properties so no editor renders a secret input", () => {
    // Shape of meili_indexer's schema: the tenant context is injected at run time.
    const fields = schemaToFields({
      type: "object",
      required: [],
      properties: {
        host: { type: "string", readOnly: true, description: "Injected" },
        api_key: { type: "string", readOnly: true, description: "Injected" },
        index: { type: "string", readOnly: true, description: "Injected" },
        primary_key: { type: "string", default: "id" },
        batch_size: { type: "integer", default: 1000 },
      },
    });
    expect(fields.map((f) => f.name)).toEqual(["primary_key", "batch_size"]);
  });
});
