import { describe, expect, it } from "vitest";

import type { PluginManifest } from "@/lib/api/types";
import type { StepDraft } from "./draft";
import { effectiveDependencies, layoutPipeline, requiredInputAt } from "./graph";

function step(id: string, plugin: string, depends_on: string[] = [], extra: Partial<StepDraft> = {}): StepDraft {
  return { id, plugin, depends_on, config: {}, ...extra };
}

const plugins: PluginManifest[] = [
  { name: "pdf", version: "1", accepts: ["bytes"], produces: "documents" },
  { name: "chunker", version: "1", accepts: ["documents"], produces: "documents" },
  { name: "indexer", version: "1", accepts: ["documents", "many"], produces: "indexed" },
];

describe("effectiveDependencies", () => {
  it("chains steps without depends_on to the one above", () => {
    const steps = [step("a", "pdf"), step("b", "chunker"), step("c", "indexer")];
    expect(effectiveDependencies(steps)).toEqual([[], ["a"], ["b"]]);
  });

  it("keeps explicit dependencies", () => {
    const steps = [step("a", "pdf"), step("b", "chunker", ["a"]), step("c", "indexer", ["a", "b"])];
    expect(effectiveDependencies(steps)).toEqual([[], ["a"], ["a", "b"]]);
  });
});

describe("requiredInputAt", () => {
  const steps = [step("a", "pdf"), step("b", "chunker"), step("c", "indexer", ["a", "b"])];

  it("is unknown for the root step", () => {
    expect(requiredInputAt(steps, 0, plugins)).toBeUndefined();
  });

  it("follows what the upstream plugin produces", () => {
    expect(requiredInputAt(steps, 1, plugins)).toBe("documents");
  });

  it("is many when several steps merge", () => {
    expect(requiredInputAt(steps, 2, plugins)).toBe("many");
  });

  it("is unknown while the upstream plugin is unknown", () => {
    expect(requiredInputAt([step("a", "nope"), step("b", "chunker")], 1, plugins)).toBeUndefined();
  });
});

describe("layoutPipeline", () => {
  it("stacks a linear pipeline one step per row", () => {
    const layout = layoutPipeline([step("a", "pdf"), step("b", "chunker"), step("c", "indexer")]);
    expect(layout.nodes.map((node) => node.level)).toEqual([0, 1, 2]);
    expect(layout.edges).toEqual([
      { from: -1, to: 0 },
      { from: 0, to: 1 },
      { from: 1, to: 2 },
    ]);
    // Same row width, so every node is centred on the same x.
    expect(new Set(layout.nodes.map((node) => node.x)).size).toBe(1);
  });

  it("puts parallel branches on the same row and merges below them", () => {
    const layout = layoutPipeline([
      step("a", "pdf"),
      step("b", "chunker", ["a"]),
      step("c", "chunker", ["a"]),
      step("d", "indexer", ["b", "c"]),
    ]);
    expect(layout.nodes.map((node) => node.level)).toEqual([0, 1, 1, 2]);
    const [, b, c] = layout.nodes;
    expect(b.y).toBe(c.y);
    expect(b.x).toBeLessThan(c.x);
    expect(layout.edges.filter((edge) => edge.to === 3)).toHaveLength(2);
  });

  it("ignores forward and self dependencies instead of looping", () => {
    const layout = layoutPipeline([step("a", "pdf", ["b"]), step("b", "chunker", ["b"])]);
    expect(layout.nodes.map((node) => node.level)).toEqual([0, 0]);
  });

  it("handles an empty draft", () => {
    const layout = layoutPipeline([]);
    expect(layout.nodes).toEqual([]);
    expect(layout.height).toBeGreaterThan(0);
  });
});
