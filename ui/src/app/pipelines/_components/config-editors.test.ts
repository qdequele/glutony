import { describe, expect, it } from "vitest";

import type { JsonSchema } from "@/lib/api/types";

import { mergeOwnedKeys, withoutOwnedKeys } from "./config-editors";

const SCHEMA: JsonSchema = {
  type: "object",
  required: ["script"],
  properties: { script: { type: "string" }, ops: { type: "array" }, on_error: { type: "string" } },
};

describe("withoutOwnedKeys", () => {
  it("strips owned keys from the schema and the config", () => {
    const { schema, config } = withoutOwnedKeys(SCHEMA, { script: "x", on_error: "skip" }, ["script", "ops"]);
    expect(Object.keys(schema?.properties ?? {})).toEqual(["on_error"]);
    expect(schema?.required).toEqual([]);
    expect(config).toEqual({ on_error: "skip" });
  });
  it("passes an absent schema through", () => {
    expect(withoutOwnedKeys(undefined, { ops: [] }, ["ops"])).toEqual({ schema: undefined, config: {} });
  });
});

describe("mergeOwnedKeys", () => {
  it("puts the owned keys of the full config back", () => {
    expect(mergeOwnedKeys({ on_error: "keep" }, { script: "x", on_error: "skip" }, ["script", "ops"])).toEqual({
      on_error: "keep",
      script: "x",
    });
  });
});
