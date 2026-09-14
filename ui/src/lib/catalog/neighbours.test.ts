import { describe, expect, it } from "vitest";

import type { ActionEntry } from "@/lib/api/types";
import { mergeActions } from "./merge";
import { neighboursOf } from "./neighbours";

function entry(
  plugin: string,
  accepts: ActionEntry["accepts"],
  produces: ActionEntry["produces"],
): ActionEntry {
  return {
    plugin,
    title: plugin,
    category: "extract",
    summary: "s",
    use_cases: ["a", "b"],
    example_step: `- id: x\n  plugin: ${plugin}\n`,
    accepts,
    produces,
  };
}

const all = mergeActions(
  [
    entry("pdf_extractor", ["bytes"], "documents"),
    entry("chunker", ["documents"], "documents"),
    entry("meili_indexer", ["documents"], "indexed"),
    entry("s3_downloader", ["ref", "empty"], "bytes"),
  ],
  [],
);

function plugins(actions: { entry: ActionEntry }[]): string[] {
  return actions.map((action) => action.entry.plugin);
}

describe("neighboursOf", () => {
  it("finds what can feed a plugin and what it can feed", () => {
    const chunker = all.find((a) => a.entry.plugin === "chunker")!;
    const { canFollow, canFeed } = neighboursOf(chunker, all);
    // Everything producing `documents`, minus chunker itself.
    expect(plugins(canFollow)).toEqual(["pdf_extractor"]);
    // Everything accepting `documents`, minus chunker itself.
    expect(plugins(canFeed)).toEqual(["meili_indexer"]);
  });

  it("gives a terminal plugin no downstream", () => {
    const indexer = all.find((a) => a.entry.plugin === "meili_indexer")!;
    const { canFeed } = neighboursOf(indexer, all);
    // Nothing accepts `indexed`: every pipeline ends at the indexer.
    expect(canFeed).toEqual([]);
  });

  it("gives a source plugin no upstream", () => {
    const downloader = all.find((a) => a.entry.plugin === "s3_downloader")!;
    const { canFollow } = neighboursOf(downloader, all);
    // Nothing in this set produces `ref` or `empty`.
    expect(canFollow).toEqual([]);
  });

  it("never lists a plugin as its own neighbour", () => {
    const chunker = all.find((a) => a.entry.plugin === "chunker")!;
    const { canFollow, canFeed } = neighboursOf(chunker, all);
    expect(plugins(canFollow)).not.toContain("chunker");
    expect(plugins(canFeed)).not.toContain("chunker");
  });
});
