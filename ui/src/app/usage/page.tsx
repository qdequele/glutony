"use client";

/**
 * `/usage` — what a tenant consumed over a date range.
 *
 * Client-rendered like every other screen: the admin UI is a static export
 * embedded in the gateway binary, so there is no server to fetch on.
 *
 * The screen has three shapes and they are deliberately distinct:
 * - **not configured** (`501`) — no analytics backend on this deployment. The
 *   default for self-hosted, and an explanation rather than an error;
 * - **configured but empty** — nothing was ingested in the range;
 * - **data** — tiles, two charts, and the per-plugin cost table.
 *
 * How job-level and plugin-level rows are combined without double-counting is
 * documented, with the reasoning, at the top of `./_lib/aggregate.ts`.
 */
import { useMemo, useState } from "react";
import { RefreshCw } from "lucide-react";
import { toast } from "sonner";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { errorMessage } from "@/lib/api/hooks";
import { useUsage, type UsageDateRange } from "@/lib/api/usage";
import { PluginBreakdown } from "./_components/plugin-breakdown";
import { RangeControl } from "./_components/range-control";
import { SummaryTiles } from "./_components/summary-tiles";
import { CostChart, DocumentsChart } from "./_components/usage-charts";
import { NoUsageData, NotConfigured, UsageSkeleton } from "./_components/usage-states";
import {
  aggregateByPlugin,
  aggregateCostByDay,
  aggregateTotals,
  documentsByDay,
} from "./_lib/aggregate";
import {
  DEFAULT_PRESET,
  eachApiDay,
  rangeError,
  rangeForPreset,
  rangeLengthInDays,
  type RangePreset,
} from "./_lib/range";

export default function UsagePage() {
  const [preset, setPreset] = useState<RangePreset>(DEFAULT_PRESET);
  // `today` is captured once per mount rather than read on every render: a
  // `new Date()` in the render body would make the query key change identity
  // across midnight mid-session and refetch for no reason.
  const [today] = useState(() => new Date());
  const [range, setRange] = useState<UsageDateRange>(() =>
    rangeForPreset(DEFAULT_PRESET, today),
  );

  const invalid = rangeError(range) !== undefined;
  // Never send a half-typed custom date to the gateway — it answers 400 on
  // anything that is not a plain YYYY-MM-DD, and the last good range is a more
  // useful thing to keep on screen than an error.
  const [queried, setQueried] = useState<UsageDateRange>(range);
  if (!invalid && (queried.from !== range.from || queried.to !== range.to)) {
    setQueried(range);
  }

  const { data, error, isPending, isFetching, refetch } = useUsage(queried);

  const rows = data?.kind === "ready" ? data.rows : undefined;
  const days = useMemo(() => eachApiDay(queried), [queried]);
  const totals = useMemo(() => aggregateTotals(rows ?? []), [rows]);
  const breakdown = useMemo(() => aggregateByPlugin(rows ?? []), [rows]);
  const documents = useMemo(
    () => documentsByDay(rows ?? [], { days, topN: 5 }),
    [rows, days],
  );
  const cost = useMemo(() => aggregateCostByDay(rows ?? [], days), [rows, days]);

  function handlePreset(next: RangePreset) {
    setPreset(next);
    if (next !== "custom") setRange(rangeForPreset(next, today));
  }

  async function handleRefresh() {
    const result = await refetch();
    if (result.error) {
      toast.error("Could not refresh usage", { description: errorMessage(result.error) });
    }
  }

  return (
    <>
      <PageHeader
        title="Usage"
        description="Documents, bytes, tokens and seconds recorded for this tenant."
        actions={
          <>
            {data?.kind === "ready" && data.projectId ? (
              <Badge variant="outline" className="hidden font-mono text-[10px] lg:inline-flex">
                {data.projectId}
              </Badge>
            ) : null}
            <RangeControl
              preset={preset}
              range={range}
              onPresetChange={handlePreset}
              onRangeChange={setRange}
            />
            <Button
              variant="ghost"
              size="icon"
              className="size-7"
              aria-label="Refresh usage"
              disabled={isFetching}
              onClick={handleRefresh}
            >
              <RefreshCw className={isFetching ? "animate-spin" : undefined} aria-hidden />
            </Button>
          </>
        }
      />

      <div className="space-y-4 p-4">
        {isPending ? (
          <UsageSkeleton />
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load usage</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : data.kind === "not_configured" ? (
          <NotConfigured message={data.message} />
        ) : data.rows.length === 0 ? (
          <NoUsageData from={queried.from} to={queried.to} />
        ) : (
          <>
            <SummaryTiles totals={totals} />

            <div className="grid gap-4 lg:grid-cols-2">
              <DocumentsChart data={documents} />
              <CostChart data={cost} />
            </div>

            <section className="space-y-2">
              <div className="flex items-baseline justify-between gap-4">
                <h2 className="text-sm font-medium">Cost by plugin</h2>
                <p className="text-xs text-muted-foreground">
                  Share of the {rangeLengthInDays(queried)}-day range
                </p>
              </div>
              <PluginBreakdown breakdown={breakdown} />
            </section>
          </>
        )}

        <Footer />
      </div>
    </>
  );
}

/**
 * The reconciliation note. Someone will open Jobs and Usage side by side, get
 * different numbers, and assume one of them is wrong — so say why up front
 * instead of waiting for the bug report.
 */
function Footer() {
  return (
    <p className="border-t pt-3 text-xs leading-relaxed text-muted-foreground">
      Figures come from the hourly deduplicated billing rollup
      (<code className="font-mono">usage_daily_billing</code>), so they can lag by up to an hour
      and will not match the Jobs screen for work that just finished. Days are UTC calendar days,
      which can shift a job near midnight into the neighbouring day. Documents are summed over
      every step, so a pipeline that extracts, chunks and then indexes counts a document more
      than once. See <code className="font-mono">docs/concepts/usage.mdx</code>.
    </p>
  );
}
