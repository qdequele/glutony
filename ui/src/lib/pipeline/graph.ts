/**
 * Laying a pipeline draft out as a graph of boxes.
 *
 * Pure geometry: which steps feed which, which row each step sits on, and the
 * pixel box of every node. The graph pane only draws what this returns.
 */
import type { InputKind, PluginManifest } from "@/lib/api/types";
import type { StepDraft } from "./draft";
import { outputToInput } from "./validate";

/**
 * The dependencies a step really runs with.
 *
 * Mirrors `PipelineDefinition::normalize` in the Rust SDK: a step with no
 * `depends_on` that is not the first one follows the step right above it.
 */
export function effectiveDependencies(steps: StepDraft[]): string[][] {
  return steps.map((step, index) => {
    if (step.depends_on.length > 0) return step.depends_on;
    return index > 0 ? [steps[index - 1].id] : [];
  });
}

/**
 * What a step at `index` will be handed, when that is knowable from the draft.
 *
 * `undefined` for the root step (it reads the ingest payload, whose kind
 * depends on the request), for `$.many` fan-outs (only known at runtime), and
 * while an upstream plugin is unknown.
 */
export function requiredInputAt(
  steps: StepDraft[],
  index: number,
  plugins: PluginManifest[],
): InputKind | undefined {
  const step = steps[index];
  if (!step) return undefined;
  const deps = effectiveDependencies(steps)[index];
  if (deps.length === 0) return undefined;
  if (deps.length > 1) return "many";
  if (step.fan_out === "$.documents") return "documents";
  if (step.fan_out === "$.many") return undefined;
  const upstream = steps.find((candidate) => candidate.id === deps[0]);
  const manifest = plugins.find((plugin) => plugin.name === upstream?.plugin);
  return manifest ? outputToInput(manifest.produces) : undefined;
}

export interface GraphNode {
  stepId: string;
  /** Position in `steps`, which is also the key of the step card. */
  index: number;
  /** Longest path from a root, in steps. Rows are drawn top to bottom. */
  level: number;
  x: number;
  y: number;
}

export interface GraphEdge {
  /** Index of the upstream step, or `-1` for the ingest payload. */
  from: number;
  to: number;
}

export interface GraphLayout {
  nodes: GraphNode[];
  edges: GraphEdge[];
  /** Box of the ingest-payload node every root hangs from. */
  source: { x: number; y: number };
  width: number;
  height: number;
}

export interface LayoutOptions {
  nodeWidth: number;
  nodeHeight: number;
  gapX: number;
  gapY: number;
  padding: number;
}

export const DEFAULT_LAYOUT: LayoutOptions = {
  nodeWidth: 168,
  nodeHeight: 68,
  gapX: 20,
  gapY: 44,
  padding: 16,
};

/**
 * Put every step on a row by its longest dependency path, then centre each row.
 *
 * Only backward edges are followed, so a draft with a forward dependency or a
 * cycle (both reported by the validator) still lays out instead of looping.
 */
export function layoutPipeline(
  steps: StepDraft[],
  options: LayoutOptions = DEFAULT_LAYOUT,
): GraphLayout {
  const { nodeWidth, nodeHeight, gapX, gapY, padding } = options;
  const deps = effectiveDependencies(steps);

  const firstIndex = new Map<string, number>();
  steps.forEach((step, index) => {
    if (!firstIndex.has(step.id)) firstIndex.set(step.id, index);
  });

  const levels: number[] = [];
  const edges: GraphEdge[] = [];
  steps.forEach((_, index) => {
    const upstream = deps[index]
      .map((id) => firstIndex.get(id))
      .filter((at): at is number => at !== undefined && at < index);
    if (upstream.length === 0) {
      levels.push(0);
      edges.push({ from: -1, to: index });
      return;
    }
    levels.push(1 + Math.max(...upstream.map((at) => levels[at])));
    for (const from of upstream) edges.push({ from, to: index });
  });

  const rows: number[][] = [];
  levels.forEach((level, index) => {
    (rows[level] ??= []).push(index);
  });

  const rowWidth = (count: number) => count * nodeWidth + Math.max(0, count - 1) * gapX;
  const widest = Math.max(nodeWidth, ...rows.map((row) => rowWidth(row.length)));
  const width = widest + padding * 2;

  // Row 0 is the ingest payload; steps start one row down.
  const top = (level: number) => padding + (level + 1) * (nodeHeight + gapY);

  const nodes: GraphNode[] = [];
  rows.forEach((row, level) => {
    const start = padding + (widest - rowWidth(row.length)) / 2;
    row.forEach((index, position) => {
      nodes.push({
        stepId: steps[index].id,
        index,
        level,
        x: start + position * (nodeWidth + gapX),
        y: top(level),
      });
    });
  });
  nodes.sort((a, b) => a.index - b.index);

  return {
    nodes,
    edges,
    source: { x: padding + (widest - nodeWidth) / 2, y: padding },
    width,
    height: top(rows.length) - gapY + padding,
  };
}
