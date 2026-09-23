import { describe, expect, it } from "vitest";

import { isArchived, pipelinePinsConnection, sourceListSearch } from "./sources";
import type { PipelineDefinition, StepDefinition } from "./types";

function pipeline(...steps: StepDefinition[]): PipelineDefinition {
  return { uid: "p", steps };
}

const parser: StepDefinition = { id: "parse", plugin: "json_parser" };
const pinned = (connection: string): StepDefinition => ({
  id: `index-${connection}`,
  plugin: "meili_indexer",
  config: { connection, index: "movies" },
});
const unpinned: StepDefinition = { id: "index", plugin: "meili_indexer", config: {} };

describe("pipelinePinsConnection", () => {
  it("is true when the only indexer names a connection", () => {
    expect(pipelinePinsConnection(pipeline(parser, pinned("prod")))).toBe(true);
  });

  it("needs every indexer to name one, like pins_destination() in Rust", () => {
    expect(pipelinePinsConnection(pipeline(pinned("a"), pinned("b")))).toBe(true);
    expect(pipelinePinsConnection(pipeline(pinned("a"), unpinned))).toBe(false);
  });

  it("is false for a pipeline with no indexer at all", () => {
    expect(pipelinePinsConnection(pipeline(parser))).toBe(false);
  });

  it("treats a blank or non-string connection as unset", () => {
    expect(pipelinePinsConnection(pipeline(pinned("   ")))).toBe(false);
    const numeric: StepDefinition = { id: "i", plugin: "meili_indexer", config: { connection: 1 } };
    expect(pipelinePinsConnection(pipeline(numeric))).toBe(false);
    const bare: StepDefinition = { id: "i", plugin: "meili_indexer" };
    expect(pipelinePinsConnection(pipeline(bare))).toBe(false);
  });
});

describe("isArchived", () => {
  it("reads archived_at", () => {
    expect(isArchived({ archived_at: "2026-09-20T10:00:00Z" })).toBe(true);
    expect(isArchived({})).toBe(false);
  });
});

describe("sourceListSearch", () => {
  it("asks for archived sources only when told to", () => {
    expect(sourceListSearch({ includeArchived: true })).toBe("?include_archived=true");
    expect(sourceListSearch({ includeArchived: false })).toBe("");
  });
});
