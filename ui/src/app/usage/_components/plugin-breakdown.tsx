"use client";

/**
 * "What is actually costing us": one row per plugin, sortable on every column,
 * with each plugin's share of the range total next to the raw figure.
 *
 * Job-level rows never reach this table — see `../_lib/aggregate.ts`. A footer
 * row carries the column totals, so the table reconciles with the tiles above
 * without the reader having to add anything up.
 */
import { useMemo, useState } from "react";
import { ArrowDown, ArrowUp, ChevronsUpDown } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Table,
  TableBody,
  TableCell,
  TableFooter,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { cn } from "@/lib/utils";
import type { PluginUsage, PluginUsageMetric } from "../_lib/aggregate";
import { pluginTotal, sortPlugins } from "../_lib/aggregate";
import { formatCount, formatDuration, formatExact, formatMinutes, formatShare } from "../_lib/format";

type SortDirection = "asc" | "desc";

interface Column {
  metric: PluginUsageMetric;
  label: string;
  /** How the cell prints a value. */
  format: (value: number) => string;
  /** Whether the cell shows the plugin's share of the column total. */
  share: boolean;
}

const COLUMNS: Column[] = [
  { metric: "llmTokens", label: "LLM tokens", format: formatCount, share: true },
  { metric: "audioSeconds", label: "Audio", format: formatMinutes, share: true },
  { metric: "pages", label: "Pages", format: formatCount, share: true },
  { metric: "documentsOut", label: "Documents", format: formatCount, share: true },
  { metric: "durationMs", label: "Duration", format: formatDuration, share: true },
  { metric: "steps", label: "Steps", format: formatExact, share: false },
];

function SortButton({
  label,
  active,
  direction,
  onClick,
}: {
  label: string;
  active: boolean;
  direction: SortDirection;
  onClick: () => void;
}) {
  const Icon = !active ? ChevronsUpDown : direction === "desc" ? ArrowDown : ArrowUp;
  return (
    <Button
      variant="ghost"
      size="sm"
      onClick={onClick}
      aria-sort={active ? (direction === "asc" ? "ascending" : "descending") : "none"}
      className={cn(
        "-mr-2 h-7 w-full justify-end gap-1 px-2 font-medium",
        active ? "text-foreground" : "text-muted-foreground",
      )}
    >
      {label}
      <Icon className="size-3" aria-hidden />
    </Button>
  );
}

export function PluginBreakdown({ breakdown }: { breakdown: PluginUsage[] }) {
  const [sort, setSort] = useState<{ metric: PluginUsageMetric; direction: SortDirection }>({
    metric: "llmTokens",
    direction: "desc",
  });

  const rows = useMemo(
    () => sortPlugins(breakdown, sort.metric, sort.direction),
    [breakdown, sort],
  );
  const totals = useMemo(
    () =>
      Object.fromEntries(
        COLUMNS.map((column) => [column.metric, pluginTotal(breakdown, column.metric)]),
      ) as Record<PluginUsageMetric, number>,
    [breakdown],
  );

  function toggle(metric: PluginUsageMetric) {
    setSort((current) =>
      current.metric === metric
        ? { metric, direction: current.direction === "desc" ? "asc" : "desc" }
        : // A new column starts on "biggest first": nobody opens this table to
          // find the cheapest plugin.
          { metric, direction: "desc" },
    );
  }

  if (breakdown.length === 0) {
    return (
      <p className="rounded-md border border-dashed py-12 text-center text-sm text-muted-foreground">
        No plugin activity in this range.
      </p>
    );
  }

  return (
    <div className="overflow-hidden rounded-md border">
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[22%]">Plugin</TableHead>
            {COLUMNS.map((column) => (
              <TableHead key={column.metric} className="text-right">
                <SortButton
                  label={column.label}
                  active={sort.metric === column.metric}
                  direction={sort.direction}
                  onClick={() => toggle(column.metric)}
                />
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {rows.map((row) => (
            <TableRow key={row.plugin}>
              <TableCell className="font-mono text-xs">{row.plugin}</TableCell>
              {COLUMNS.map((column) => {
                const value = row[column.metric];
                return (
                  <TableCell key={column.metric} className="text-right tabular-nums">
                    <span className={cn(value === 0 && "text-muted-foreground")}>
                      {column.format(value)}
                    </span>
                    {column.share ? (
                      <span className="ml-1.5 text-xs text-muted-foreground">
                        {value === 0 ? "" : formatShare(value, totals[column.metric])}
                      </span>
                    ) : null}
                  </TableCell>
                );
              })}
            </TableRow>
          ))}
        </TableBody>
        <TableFooter>
          <TableRow>
            <TableCell className="text-xs text-muted-foreground">
              {rows.length} plugin{rows.length === 1 ? "" : "s"}
            </TableCell>
            {COLUMNS.map((column) => (
              <TableCell key={column.metric} className="text-right tabular-nums">
                {column.format(totals[column.metric])}
              </TableCell>
            ))}
          </TableRow>
        </TableFooter>
      </Table>
    </div>
  );
}
