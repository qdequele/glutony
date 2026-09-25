import { describe, expect, it } from "vitest";

import type { ActionEntry, PluginManifest } from "@/lib/api/types";
import { buildPluginOptions, searchPluginOptions } from "./plugin-search";

const plugins: PluginManifest[] = [
  { name: "pdf_extractor", version: "1", accepts: ["bytes"], produces: "documents", content_types: ["application/pdf"] },
  { name: "chunker", version: "1", accepts: ["documents"], produces: "documents" },
  { name: "whisper", version: "1", accepts: ["bytes"], produces: "documents", description: "Transcribe audio" },
];

const actions: ActionEntry[] = [
  {
    plugin: "chunker",
    title: "Split into chunks",
    category: "transform",
    summary: "Cut long documents",
    use_cases: ["RAG"],
    example_step: "",
    accepts: ["documents"],
    produces: "documents",
  },
];

const names = (list: ReturnType<typeof searchPluginOptions>) => list.map((o) => o.manifest.name);

describe("buildPluginOptions", () => {
  it("dresses manifests with catalog copy and falls back to the name", () => {
    const options = buildPluginOptions(plugins, actions, undefined);
    expect(options[1].title).toBe("Split into chunks");
    expect(options[1].category).toBe("transform");
    expect(options[0].title).toBe("pdf_extractor");
    expect(options[0].compatible).toBeUndefined();
  });

  it("flags compatibility with the required input", () => {
    const options = buildPluginOptions(plugins, actions, "documents");
    expect(options.map((o) => o.compatible)).toEqual([false, true, false]);
  });
});

describe("searchPluginOptions", () => {
  const options = buildPluginOptions(plugins, actions, undefined);

  it("keeps input order without a query", () => {
    expect(names(searchPluginOptions(options, ""))).toEqual(["pdf_extractor", "chunker", "whisper"]);
  });

  it("matches name, title, description, content types and use cases", () => {
    expect(names(searchPluginOptions(options, "pdf"))).toEqual(["pdf_extractor"]);
    expect(names(searchPluginOptions(options, "chunks"))).toEqual(["chunker"]);
    expect(names(searchPluginOptions(options, "audio"))).toEqual(["whisper"]);
    expect(names(searchPluginOptions(options, "application/pdf"))).toEqual(["pdf_extractor"]);
    expect(names(searchPluginOptions(options, "rag"))).toEqual(["chunker"]);
  });

  it("requires every token to match", () => {
    expect(names(searchPluginOptions(options, "pdf audio"))).toEqual([]);
  });

  it("filters by category", () => {
    expect(names(searchPluginOptions(options, "", { category: "transform" }))).toEqual(["chunker"]);
  });

  it("sorts compatible plugins first and can hide the rest", () => {
    const typed = buildPluginOptions(plugins, actions, "documents");
    expect(names(searchPluginOptions(typed, ""))[0]).toBe("chunker");
    expect(names(searchPluginOptions(typed, "", { compatibleOnly: true }))).toEqual(["chunker"]);
  });
});
