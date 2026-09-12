"use client";

/**
 * The two time-series charts.
 *
 * Colours come from CSS variables (`./charts.module.css` for the series ramp,
 * `globals.css` for grid, axes and surfaces), never from literals, so both
 * charts follow the theme toggle. Recharts resolves `fill="var(--usage-1)"`
 * through the DOM, so the variables only have to be in scope on an ancestor —
 * which is what the `palette` class on the card is for.
 *
 * The tooltip is hand-rolled: the stock one paints an opaque white panel that
 * is unreadable in dark mode.
 */
import type { ReactNode } from "react";
import {
  Bar,
  BarChart,
  CartesianGrid,
  ComposedChart,
  Line,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import type { DailyCostPoint, DailyDocuments } from "../_lib/aggregate";
import { OTHER_SERIES } from "../_lib/aggregate";
import { formatCount, formatDayShort, formatExact, formatMinutes } from "../_lib/format";
import styles from "./charts.module.css";

/** The series ramp, in stacking order. Six is as many bands as stays readable. */
const SERIES_COLORS = [
  "var(--usage-1)",
  "var(--usage-2)",
  "var(--usage-3)",
  "var(--usage-4)",
  "var(--usage-5)",
  "var(--usage-6)",
] as const;

const AXIS_TICK = { fill: "var(--muted-foreground)", fontSize: 11 } as const;
const GRID_STROKE = "var(--border)";

// ---------------------------------------------------------------------------
// Shared chrome
// ---------------------------------------------------------------------------

function ChartCard({
  title,
  description,
  legend,
  children,
}: {
  title: string;
  description: string;
  legend: ReactNode;
  children: ReactNode;
}) {
  return (
    <Card className={cn(styles.palette, "min-w-0")}>
      <CardHeader>
        <CardTitle>{title}</CardTitle>
        <CardDescription>{description}</CardDescription>
      </CardHeader>
      <CardContent className="space-y-3">
        <div className="h-64 w-full">{children}</div>
        <div className="flex flex-wrap items-center gap-x-4 gap-y-1">{legend}</div>
      </CardContent>
    </Card>
  );
}

function LegendChip({ color, label }: { color: string; label: string }) {
  return (
    <span className="flex items-center gap-1.5 text-xs text-muted-foreground">
      <span
        aria-hidden
        className="size-2.5 shrink-0 rounded-[3px]"
        style={{ backgroundColor: color }}
      />
      <span className="truncate">{label}</span>
    </span>
  );
}

/** The tooltip surface, themed with the same tokens as a popover. */
function TooltipShell({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="rounded-md border bg-popover px-2.5 py-2 text-xs text-popover-foreground shadow-md">
      <p className="mb-1 font-medium">{label}</p>
      <div className="space-y-0.5">{children}</div>
    </div>
  );
}

function TooltipRow({
  color,
  label,
  value,
}: {
  color: string | undefined;
  label: string;
  value: string;
}) {
  return (
    <div className="flex items-center justify-between gap-4">
      <span className="flex items-center gap-1.5 text-muted-foreground">
        <span
          aria-hidden
          className="size-2 shrink-0 rounded-[2px]"
          style={{ backgroundColor: color ?? "var(--muted-foreground)" }}
        />
        {label}
      </span>
      <span className="font-mono tabular-nums">{value}</span>
    </div>
  );
}

/** Recharts hands tooltip values back loosely typed; coerce without `any`. */
function toNumber(value: unknown): number {
  if (typeof value === "number") return value;
  if (typeof value === "string") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

/** A plugin band's display name. */
function seriesLabel(plugin: string): string {
  return plugin === OTHER_SERIES ? "other plugins" : plugin;
}

// ---------------------------------------------------------------------------
// Documents per day
// ---------------------------------------------------------------------------

export function DocumentsChart({ data }: { data: DailyDocuments }) {
  const { points, series } = data;

  return (
    <ChartCard
      title="Documents per day"
      description="Documents each plugin emitted, stacked. Summed over every step of a pipeline."
      legend={series.map((band, index) => (
        <LegendChip
          key={band.key}
          color={SERIES_COLORS[index % SERIES_COLORS.length]}
          label={seriesLabel(band.plugin)}
        />
      ))}
    >
      {series.length === 0 ? (
        <EmptyChart message="No documents were produced in this range." />
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <BarChart data={points} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <CartesianGrid vertical={false} stroke={GRID_STROKE} />
            <XAxis
              dataKey="day"
              tickFormatter={formatDayShort}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={{ stroke: GRID_STROKE }}
              minTickGap={24}
            />
            <YAxis
              tickFormatter={formatCount}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={false}
              width={48}
            />
            <Tooltip
              cursor={{ fill: "var(--muted)", opacity: 0.5 }}
              content={({ active, payload, label }) => {
                if (active !== true || payload === undefined || payload.length === 0) return null;
                const total = payload.reduce((sum, entry) => sum + toNumber(entry.value), 0);
                return (
                  <TooltipShell label={formatDayShort(String(label))}>
                    {payload
                      .filter((entry) => toNumber(entry.value) > 0)
                      .map((entry) => (
                        <TooltipRow
                          key={String(entry.dataKey)}
                          color={entry.color}
                          label={String(entry.name)}
                          value={formatExact(toNumber(entry.value))}
                        />
                      ))}
                    <TooltipRow color={undefined} label="total" value={formatExact(total)} />
                  </TooltipShell>
                );
              }}
            />
            {series.map((band, index) => (
              <Bar
                key={band.key}
                dataKey={band.key}
                name={seriesLabel(band.plugin)}
                stackId="documents"
                fill={SERIES_COLORS[index % SERIES_COLORS.length]}
                radius={index === series.length - 1 ? [3, 3, 0, 0] : 0}
                isAnimationActive={false}
              />
            ))}
          </BarChart>
        </ResponsiveContainer>
      )}
    </ChartCard>
  );
}

// ---------------------------------------------------------------------------
// Cost units per day
// ---------------------------------------------------------------------------

/**
 * Tokens and audio seconds on the same chart but on **separate axes**: a day
 * can be six figures of tokens and two figures of audio seconds, and one shared
 * axis would flatten the audio line onto the baseline.
 */
export function CostChart({ data }: { data: DailyCostPoint[] }) {
  const hasTokens = data.some((point) => point.llmInputTokens + point.llmOutputTokens > 0);
  const hasAudio = data.some((point) => point.audioSeconds > 0);

  return (
    <ChartCard
      title="Cost units per day"
      description="LLM tokens on the left axis, transcribed audio on the right. Different magnitudes, different scales."
      legend={
        <>
          <LegendChip color="var(--usage-tokens-in)" label="input tokens" />
          <LegendChip color="var(--usage-tokens-out)" label="output tokens" />
          <LegendChip color="var(--usage-audio)" label="audio (minutes)" />
        </>
      }
    >
      {!hasTokens && !hasAudio ? (
        <EmptyChart message="No LLM or transcription usage in this range." />
      ) : (
        <ResponsiveContainer width="100%" height="100%">
          <ComposedChart data={data} margin={{ top: 4, right: 4, bottom: 0, left: 0 }}>
            <CartesianGrid vertical={false} stroke={GRID_STROKE} />
            <XAxis
              dataKey="day"
              tickFormatter={formatDayShort}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={{ stroke: GRID_STROKE }}
              minTickGap={24}
            />
            <YAxis
              yAxisId="tokens"
              tickFormatter={formatCount}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={false}
              width={48}
            />
            <YAxis
              yAxisId="audio"
              orientation="right"
              tickFormatter={(value: number) => formatCount(value / 60)}
              tick={AXIS_TICK}
              tickLine={false}
              axisLine={false}
              width={44}
            />
            <Tooltip
              cursor={{ fill: "var(--muted)", opacity: 0.5 }}
              content={({ active, payload, label }) => {
                if (active !== true || payload === undefined || payload.length === 0) return null;
                return (
                  <TooltipShell label={formatDayShort(String(label))}>
                    {payload.map((entry) => {
                      const value = toNumber(entry.value);
                      const isAudio = entry.dataKey === "audioSeconds";
                      return (
                        <TooltipRow
                          key={String(entry.dataKey)}
                          color={entry.color}
                          label={String(entry.name)}
                          value={isAudio ? formatMinutes(value) : formatExact(value)}
                        />
                      );
                    })}
                  </TooltipShell>
                );
              }}
            />
            <Bar
              yAxisId="tokens"
              dataKey="llmInputTokens"
              name="input tokens"
              stackId="tokens"
              fill="var(--usage-tokens-in)"
              isAnimationActive={false}
            />
            <Bar
              yAxisId="tokens"
              dataKey="llmOutputTokens"
              name="output tokens"
              stackId="tokens"
              fill="var(--usage-tokens-out)"
              radius={[3, 3, 0, 0]}
              isAnimationActive={false}
            />
            <Line
              yAxisId="audio"
              type="monotone"
              dataKey="audioSeconds"
              name="audio"
              stroke="var(--usage-audio)"
              strokeWidth={2}
              dot={false}
              activeDot={{ r: 3 }}
              isAnimationActive={false}
            />
          </ComposedChart>
        </ResponsiveContainer>
      )}
    </ChartCard>
  );
}

function EmptyChart({ message }: { message: string }) {
  return (
    <div className="flex h-full items-center justify-center rounded-md border border-dashed">
      <p className="text-xs text-muted-foreground">{message}</p>
    </div>
  );
}
