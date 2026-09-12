"use client";

/**
 * The row of headline numbers at the top of the Usage screen.
 *
 * Every value is formatted for a human (`formatters` in `../_lib/format`) and
 * carries the raw figure in a tooltip, because the compact form is fine for
 * reading a trend and useless for reconciling an invoice.
 */
import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { AudioLines, Binary, CheckCheck, Database, FileText, HardDrive } from "lucide-react";

import { Card, CardContent } from "@/components/ui/card";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import type { UsageTotals } from "../_lib/aggregate";
import {
  formatBytes,
  formatCount,
  formatExact,
  formatMinutes,
  formatShare,
} from "../_lib/format";

interface Tile {
  label: string;
  icon: LucideIcon;
  value: string;
  /** Exact figure, shown on hover. */
  exact: string;
  /** Optional second line, e.g. the succeeded/failed split. */
  detail?: ReactNode;
}

function tilesFor(totals: UsageTotals): Tile[] {
  return [
    {
      label: "Documents indexed",
      icon: FileText,
      // The corpus that reached Meilisearch, taken from the job row's final-step
      // count. Distinct from `documentsOut`, which sums every stage and therefore
      // measures work done rather than documents stored.
      value: formatCount(totals.documentsIndexed),
      exact: `${formatExact(totals.documentsIndexed)} documents indexed · ${formatExact(
        totals.documentsOut,
      )} produced across all pipeline steps`,
    },
    {
      label: "Bytes processed",
      icon: HardDrive,
      value: formatBytes(totals.inputBytes),
      exact: `${formatExact(totals.inputBytes)} bytes fed into steps`,
    },
    {
      label: "Jobs",
      icon: CheckCheck,
      value: formatCount(totals.jobs),
      // The failure *rate* lives in the tooltip: the tile is only ~190px wide at
      // six across, and a truncated detail line is worse than a hidden one.
      exact: `${formatExact(totals.jobs)} jobs · ${formatShare(
        totals.jobsFailed,
        totals.jobs,
      )} failed`,
      detail: (
        <>
          <span className="text-foreground">{formatCount(totals.jobsSucceeded)}</span> ok ·{" "}
          <span className={cn(totals.jobsFailed > 0 && "text-destructive")}>
            {formatCount(totals.jobsFailed)} failed
          </span>
        </>
      ),
    },
    {
      label: "LLM tokens",
      icon: Binary,
      value: formatCount(totals.llmTokens),
      exact: `${formatExact(totals.llmTokens)} tokens over ${formatExact(
        totals.llmRequests,
      )} requests`,
      detail: (
        <>
          {formatCount(totals.llmInputTokens)} in · {formatCount(totals.llmOutputTokens)} out
        </>
      ),
    },
    {
      label: "Transcription",
      icon: AudioLines,
      value: formatMinutes(totals.audioSeconds),
      exact: `${formatExact(totals.audioSeconds)} seconds of audio`,
    },
    {
      label: "Pages extracted",
      icon: Database,
      value: formatCount(totals.pages),
      exact: `${formatExact(totals.pages)} pages · ${formatExact(totals.images)} images`,
    },
  ];
}

export function SummaryTiles({ totals }: { totals: UsageTotals }) {
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
      {tilesFor(totals).map((tile) => (
        <Card key={tile.label} size="sm">
          <CardContent className="space-y-1">
            <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <tile.icon className="size-3.5" aria-hidden />
              <span className="truncate">{tile.label}</span>
            </div>
            <Tooltip>
              <TooltipTrigger asChild>
                <p className="w-fit cursor-default text-2xl font-semibold tabular-nums tracking-tight">
                  {tile.value}
                </p>
              </TooltipTrigger>
              <TooltipContent>{tile.exact}</TooltipContent>
            </Tooltip>
            <p className="min-h-4 truncate text-[11px] text-muted-foreground">{tile.detail}</p>
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

/** Six placeholders in the same grid, so the layout does not jump on load. */
export function SummaryTilesSkeleton() {
  return (
    <div className="grid grid-cols-2 gap-3 md:grid-cols-3 xl:grid-cols-6">
      {Array.from({ length: 6 }, (_, index) => (
        <Skeleton key={index} className="h-[6.25rem] w-full rounded-xl" />
      ))}
    </div>
  );
}
