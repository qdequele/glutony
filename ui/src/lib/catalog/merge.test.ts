import { describe, expect, it } from "vitest";

import type { ActionEntry, PluginManifest } from "@/lib/api/types";
import { isStubManifest, mergeActions } from "./merge";

const pdf: ActionEntry = {
  plugin: "pdf_extractor",
  title: "PDF extractor",
  category: "extract",
  summary: "Pull text out of a PDF.",
  use_cases: ["a", "b"],
  example_step: "- id: extract\n  plugin: pdf_extractor\n",
  accepts: ["bytes"],
  produces: "documents",
};

/** What `static_manifests()` produces before any worker registers. */
const stub: PluginManifest = {
  name: "pdf_extractor",
  version: "0.1.0",
  description: "",
  accepts: [],
  produces: "documents",
  config_schema: { type: "object" },
  kind: "builtin",
};

const full: PluginManifest = {
  ...stub,
  description: "Extract text from PDF documents.",
  accepts: ["bytes"],
  config_schema: {
    type: "object",
    properties: { per_page: { type: "boolean", default: true } },
  },
};

describe("isStubManifest", () => {
  it("recognises the name-and-kind fallback", () => {
    expect(isStubManifest(stub)).toBe(true);
  });

  it("does not flag a manifest a worker published", () => {
    expect(isStubManifest(full)).toBe(false);
  });
});

describe("mergeActions", () => {
  it("reports not-registered when no manifest exists", () => {
    const [merged] = mergeActions([pdf], []);
    expect(merged.registered).toBe(false);
    expect(merged.manifest).toBeUndefined();
    // Catalog values stand in so the card is never blank.
    expect(merged.accepts).toEqual(["bytes"]);
    expect(merged.produces).toBe("documents");
  });

  it("reports not-registered for a stub, which carries no schema to show", () => {
    const [merged] = mergeActions([pdf], [stub]);
    expect(merged.registered).toBe(false);
    expect(merged.manifest).toBeUndefined();
  });

  it("prefers live manifest values over catalog fallbacks", () => {
    const [merged] = mergeActions(
      [{ ...pdf, accepts: ["documents"], produces: "bytes" }],
      [full],
    );
    expect(merged.registered).toBe(true);
    expect(merged.manifest).toBe(full);
    expect(merged.accepts).toEqual(["bytes"]);
    expect(merged.produces).toBe("documents");
  });

  it("keeps catalog order and ignores manifests with no catalog entry", () => {
    const chunker: ActionEntry = { ...pdf, plugin: "chunker", title: "Chunker" };
    const merged = mergeActions([pdf, chunker], [{ ...full, name: "unknown_plugin" }]);
    expect(merged.map((m) => m.entry.plugin)).toEqual(["pdf_extractor", "chunker"]);
  });
});
