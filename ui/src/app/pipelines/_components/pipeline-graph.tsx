"use client";

import { AlertTriangle, Inbox, Layers } from "lucide-react";
import { useMemo } from "react";

import { useCatalog } from "@/lib/api/hooks";
import type { PluginManifest } from "@/lib/api/types";
import type { PipelineDraft } from "@/lib/pipeline/draft";
import { DEFAULT_LAYOUT, layoutPipeline, type GraphEdge } from "@/lib/pipeline/graph";
import type { ValidationIssue } from "@/lib/pipeline/validate";
import { cn } from "@/lib/utils";

const { nodeWidth: W, nodeHeight: H, gapY } = DEFAULT_LAYOUT;

/**
 * The draft as boxes and arrows: the ingest payload on top, one box per step,
 * edges following the dependencies the worker will actually use. Clicking a
 * box hands its index back so the editor can bring that step card into view.
 */
export function PipelineGraph({
  draft,
  plugins,
  issues,
  selected,
  onSelect,
}: {
  draft: PipelineDraft;
  plugins: PluginManifest[];
  /** Issues keyed by step id, as `issuesByStep` returns them. */
  issues: Map<string, ValidationIssue[]>;
  selected?: number;
  onSelect: (index: number) => void;
}) {
  const catalog = useCatalog();
  const layout = useMemo(() => layoutPipeline(draft.steps), [draft.steps]);

  const manifestOf = (index: number) =>
    plugins.find((plugin) => plugin.name === draft.steps[index]?.plugin);
  const titleOf = (plugin: string) =>
    catalog.data?.actions.find((entry) => entry.plugin === plugin)?.title;

  const trigger = draft.trigger;
  const triggerLine =
    trigger.content_types.length > 0
      ? trigger.content_types.join(", ")
      : trigger.filename_pattern.trim() || "any content";

  if (draft.steps.length === 0) {
    return (
      <p className="m-3 rounded-md border border-dashed py-10 text-center text-sm text-muted-foreground">
        Add a step to see the pipeline.
      </p>
    );
  }

  function anchor(edge: GraphEdge) {
    const from =
      edge.from === -1 ? layout.source : layout.nodes[edge.from] ?? layout.source;
    const to = layout.nodes[edge.to];
    return { x1: from.x + W / 2, y1: from.y + H, x2: to.x + W / 2, y2: to.y };
  }

  function edgeLabel(edge: GraphEdge): string | undefined {
    const step = draft.steps[edge.to];
    if (step.fan_out === "$.documents") return "each document";
    if (step.fan_out === "$.many") return "each branch";
    if (edge.from === -1) return undefined;
    return manifestOf(edge.from)?.produces;
  }

  const mismatched = (index: number) =>
    (issues.get(draft.steps[index].id) ?? []).some(
      (issue) => issue.rule === "type_mismatch" || issue.rule === "fan_out_source",
    );

  return (
    <div className="h-full overflow-auto">
      <div
        className="relative mx-auto"
        style={{ width: layout.width, height: layout.height }}
        role="group"
        aria-label="Pipeline graph"
      >
        <svg
          className="pointer-events-none absolute inset-0 overflow-visible"
          width={layout.width}
          height={layout.height}
          aria-hidden
        >
          <defs>
            <marker id="graph-arrow" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L8,4 L0,8 z" className="fill-muted-foreground/70" />
            </marker>
            <marker id="graph-arrow-bad" viewBox="0 0 8 8" refX="7" refY="4" markerWidth="7" markerHeight="7" orient="auto">
              <path d="M0,0 L8,4 L0,8 z" className="fill-destructive" />
            </marker>
          </defs>
          {layout.edges.map((edge) => {
            const { x1, y1, x2, y2 } = anchor(edge);
            // An edge skipping rows drops straight down its source's column and
            // only bends in the last gap, so it runs beside the boxes it passes
            // instead of behind them.
            const turn = y2 - gapY;
            const d =
              turn > y1
                ? `M${x1},${y1} L${x1},${turn} C${x1},${turn + gapY / 2} ${x2},${y2 - gapY / 2} ${x2},${y2 - 1}`
                : `M${x1},${y1} C${x1},${y1 + gapY / 2} ${x2},${y2 - gapY / 2} ${x2},${y2 - 1}`;
            // Label sits in the gap right under the source, where it is never covered.
            const labelX = turn > y1 ? x1 : (x1 + x2) / 2;
            const labelY = y1 + gapY / 2;
            const bad = mismatched(edge.to);
            const label = edgeLabel(edge);
            const fanOut = draft.steps[edge.to].fan_out !== undefined;
            return (
              <g key={`${edge.from}-${edge.to}`}>
                <path
                  d={d}
                  fill="none"
                  strokeWidth={1.5}
                  strokeDasharray={fanOut ? "4 3" : undefined}
                  markerEnd={bad ? "url(#graph-arrow-bad)" : "url(#graph-arrow)"}
                  className={bad ? "stroke-destructive" : "stroke-muted-foreground/50"}
                />
                {label ? (
                  <text
                    x={labelX}
                    y={labelY}
                    dy="0.35em"
                    textAnchor="middle"
                    paintOrder="stroke"
                    strokeWidth={4}
                    className={cn(
                      "stroke-background font-mono text-[10px]",
                      bad ? "fill-destructive" : "fill-muted-foreground",
                    )}
                  >
                    {label}
                  </text>
                ) : null}
              </g>
            );
          })}
        </svg>

        <div
          className="absolute flex items-center gap-2.5 rounded-lg border border-dashed bg-muted/40 px-3"
          style={{ left: layout.source.x, top: layout.source.y, width: W, height: H }}
        >
          <Inbox className="size-4 shrink-0 text-muted-foreground" aria-hidden />
          <div className="min-w-0">
            <p className="text-xs font-medium">Ingest payload</p>
            <p className="truncate font-mono text-[11px] text-muted-foreground" title={triggerLine}>
              {triggerLine}
            </p>
          </div>
        </div>

        {layout.nodes.map((node) => {
          const step = draft.steps[node.index];
          const manifest = manifestOf(node.index);
          const stepIssues = issues.get(step.id) ?? [];
          const title = titleOf(step.plugin);
          return (
            <button
              key={node.index}
              type="button"
              onClick={() => onSelect(node.index)}
              title={stepIssues.map((issue) => issue.message).join("\n") || undefined}
              className={cn(
                "absolute flex flex-col justify-between rounded-lg border bg-card px-3 py-2 text-left shadow-xs transition-colors hover:border-ring",
                step.fan_out && "shadow-[3px_3px_0_-1px_var(--color-card),3px_3px_0_0_var(--color-border)]",
                stepIssues.length > 0 && "border-destructive/60",
                selected === node.index && "border-ring ring-2 ring-ring/40",
              )}
              style={{ left: node.x, top: node.y, width: W, height: H }}
            >
              <span className="flex min-w-0 items-center gap-1.5">
                <span className="text-[10px] tabular-nums text-muted-foreground">{node.index + 1}</span>
                <span className="truncate font-mono text-xs font-medium">{step.id || "unnamed"}</span>
                {stepIssues.length > 0 ? (
                  <AlertTriangle className="ml-auto size-3.5 shrink-0 text-destructive" aria-label="Has problems" />
                ) : step.fan_out ? (
                  <Layers className="ml-auto size-3.5 shrink-0 text-muted-foreground" aria-label="Fans out" />
                ) : null}
              </span>
              <span className="truncate text-[11px] text-muted-foreground">
                {title ?? (step.plugin || "no plugin")}
              </span>
              <span className="truncate font-mono text-[10px] text-muted-foreground/80">
                {manifest
                  ? `${(manifest.accepts ?? []).join("|") || "∅"} → ${manifest.produces}`
                  : step.plugin
                    ? "not registered"
                    : ""}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}
