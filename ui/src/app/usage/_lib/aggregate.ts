/**
 * Turning `tenant_usage` rows into the numbers the Usage screen shows.
 *
 * ## The rule: job-level and plugin-level rows are read separately
 *
 * `GET /usage` returns one row per `(day, pipeline_uid, plugin)`. A row with
 * `plugin === ""` is job-level; a row with a plugin is step-level. Both families
 * carry the *same column names*, and that is the trap.
 *
 * Upstream (`crates/usage/src/lib.rs`) every finished job emits one raw event per
 * step **plus** one job event, and the job event repeats the whole job's cost
 * units as a per-job total. Summing steps and jobs together therefore counts
 * every token, second and page twice. The rollup
 * (`tinybird/pipes/usage_daily_billing.pipe`) already defends against that by
 * zeroing each family on the other's rows:
 *
 * | columns | filled from | non-zero on |
 * |---|---|---|
 * | `jobs`, `jobs_succeeded`, `jobs_failed` | `kind = 'job'` events | `plugin === ""` rows |
 * | `steps`, `documents_out`, `input_bytes`, `duration_ms` and every cost unit | `kind = 'step'` events | `plugin !== ""` rows |
 *
 * **We do not rely on that.** The gateway passes metric columns through untyped
 * so the pipe can grow a column without a release, which means the pipe can also
 * change what it puts where without this code being updated. So every function
 * below *partitions* the rows first and reads each column only from the family
 * that owns it: job counters from `plugin === ""` rows, work and cost counters
 * from `plugin !== ""` rows. A number can then never be counted twice, and the
 * worst a future pipe change can do is make a tile read low rather than double.
 *
 * ## One number this endpoint genuinely cannot give us
 *
 * `documents_out` is summed over *step* rows, so a pipeline whose extractor,
 * chunker and indexer each report documents counts the same document once per
 * stage. The job row holds the accurate "documents indexed" figure (the final
 * step's output) but the rollup zeroes it there, so it never reaches this
 * screen. The tile is therefore labelled "documents produced", not "documents
 * indexed": it is an upper bound on multi-stage pipelines.
 */
import { isJobRow, metricValue, type UsageRow } from "@/lib/api/usage";

/** Series name used for plugins outside the top N of a stacked chart. */
export const OTHER_SERIES = "other";

// ---------------------------------------------------------------------------
// Totals
// ---------------------------------------------------------------------------

/** Everything the summary tiles need, for one date range. */
export interface UsageTotals {
  /** Jobs that reached a terminal state. Job-level rows only. */
  jobs: number;
  /** Of those, the ones that succeeded. Job-level rows only. */
  jobsSucceeded: number;
  /** Of those, the ones that failed. Job-level rows only. */
  jobsFailed: number;
  /** Step attempts executed. Step-level rows only. */
  steps: number;
  /** Documents emitted across all steps — see the note in the module docs. */
  documentsOut: number;
  /** Bytes fed into steps. */
  inputBytes: number;
  /** Summed step wall time. */
  durationMs: number;
  /** LLM prompt tokens. */
  llmInputTokens: number;
  /** LLM completion tokens. */
  llmOutputTokens: number;
  /** Prompt + completion, which is how the tile quotes it. */
  llmTokens: number;
  /** Calls made to an LLM provider. */
  llmRequests: number;
  /** Audio submitted for transcription. */
  audioSeconds: number;
  /** Pages extracted from documents. */
  pages: number;
  /** Images processed. */
  images: number;
  /** Calls to any other external service. */
  externalRequests: number;
}

const ZERO_TOTALS: UsageTotals = {
  jobs: 0,
  jobsSucceeded: 0,
  jobsFailed: 0,
  steps: 0,
  documentsOut: 0,
  inputBytes: 0,
  durationMs: 0,
  llmInputTokens: 0,
  llmOutputTokens: 0,
  llmTokens: 0,
  llmRequests: 0,
  audioSeconds: 0,
  pages: 0,
  images: 0,
  externalRequests: 0,
};

/**
 * Sum a range into the summary tiles.
 *
 * Job counters come only from job-level rows and work/cost counters only from
 * plugin-level rows — the rule from the module docs. Rows the caller has
 * already filtered (by pipeline, say) are fine: this is a plain fold.
 */
export function aggregateTotals(rows: readonly UsageRow[]): UsageTotals {
  const totals: UsageTotals = { ...ZERO_TOTALS };

  for (const row of rows) {
    if (isJobRow(row)) {
      totals.jobs += metricValue(row, "jobs");
      totals.jobsSucceeded += metricValue(row, "jobs_succeeded");
      totals.jobsFailed += metricValue(row, "jobs_failed");
      continue;
    }
    totals.steps += metricValue(row, "steps");
    totals.documentsOut += metricValue(row, "documents_out");
    totals.inputBytes += metricValue(row, "input_bytes");
    totals.durationMs += metricValue(row, "duration_ms");
    totals.llmInputTokens += metricValue(row, "llm_input_tokens");
    totals.llmOutputTokens += metricValue(row, "llm_output_tokens");
    totals.llmRequests += metricValue(row, "llm_requests");
    totals.audioSeconds += metricValue(row, "audio_seconds");
    totals.pages += metricValue(row, "pages");
    totals.images += metricValue(row, "images");
    totals.externalRequests += metricValue(row, "external_requests");
  }

  totals.llmTokens = totals.llmInputTokens + totals.llmOutputTokens;
  return totals;
}

// ---------------------------------------------------------------------------
// Per-plugin breakdown
// ---------------------------------------------------------------------------

/** One plugin's consumption over the range. Built from step-level rows only. */
export interface PluginUsage {
  /** Plugin name. Never the empty string — job rows are not plugins. */
  plugin: string;
  /** Step attempts this plugin ran. */
  steps: number;
  /** Documents this plugin emitted. */
  documentsOut: number;
  /** Prompt + completion tokens. */
  llmTokens: number;
  /** Calls made to an LLM provider. */
  llmRequests: number;
  /** Audio seconds transcribed. */
  audioSeconds: number;
  /** Pages extracted. */
  pages: number;
  /** Images processed. */
  images: number;
  /** Summed wall time. */
  durationMs: number;
}

/** Every numeric column of {@link PluginUsage}; the sortable table columns. */
export type PluginUsageMetric = Exclude<keyof PluginUsage, "plugin">;

/**
 * Consumption per plugin, biggest token spender first.
 *
 * Job-level rows are skipped entirely: they have no plugin, and their counters
 * belong to the jobs tile, not to any row of this table.
 */
export function aggregateByPlugin(rows: readonly UsageRow[]): PluginUsage[] {
  const byPlugin = new Map<string, PluginUsage>();

  for (const row of rows) {
    if (isJobRow(row)) continue;
    let entry = byPlugin.get(row.plugin);
    if (entry === undefined) {
      entry = {
        plugin: row.plugin,
        steps: 0,
        documentsOut: 0,
        llmTokens: 0,
        llmRequests: 0,
        audioSeconds: 0,
        pages: 0,
        images: 0,
        durationMs: 0,
      };
      byPlugin.set(row.plugin, entry);
    }
    entry.steps += metricValue(row, "steps");
    entry.documentsOut += metricValue(row, "documents_out");
    entry.llmTokens += metricValue(row, "llm_input_tokens") + metricValue(row, "llm_output_tokens");
    entry.llmRequests += metricValue(row, "llm_requests");
    entry.audioSeconds += metricValue(row, "audio_seconds");
    entry.pages += metricValue(row, "pages");
    entry.images += metricValue(row, "images");
    entry.durationMs += metricValue(row, "duration_ms");
  }

  // Tokens first, then duration, then name: "what is costing us" is the
  // question this table exists to answer, and a stable tiebreak keeps the
  // order from jittering between refetches.
  return [...byPlugin.values()].sort(
    (a, b) =>
      b.llmTokens - a.llmTokens ||
      b.durationMs - a.durationMs ||
      a.plugin.localeCompare(b.plugin),
  );
}

/** Sum one column of a breakdown, for the "share of total" columns. */
export function pluginTotal(breakdown: readonly PluginUsage[], metric: PluginUsageMetric): number {
  return breakdown.reduce((sum, entry) => sum + entry[metric], 0);
}

/** Re-sort a breakdown by any numeric column. Ties fall back to the name. */
export function sortPlugins(
  breakdown: readonly PluginUsage[],
  metric: PluginUsageMetric,
  direction: "asc" | "desc",
): PluginUsage[] {
  const sign = direction === "asc" ? 1 : -1;
  return [...breakdown].sort(
    (a, b) => sign * (a[metric] - b[metric]) || a.plugin.localeCompare(b.plugin),
  );
}

// ---------------------------------------------------------------------------
// Daily series
// ---------------------------------------------------------------------------

/** One day of the cost chart. */
export interface DailyCostPoint {
  /** `YYYY-MM-DD`. */
  day: string;
  /** LLM prompt tokens that day. */
  llmInputTokens: number;
  /** LLM completion tokens that day. */
  llmOutputTokens: number;
  /** Audio seconds transcribed that day. Plotted on its own axis. */
  audioSeconds: number;
}

/**
 * Cost units per day, oldest first.
 *
 * Pass `days` (every day of the selected range) to get a point for days with no
 * activity too, so the chart shows a gap in ingestion as a gap and not as a
 * straight line between the two days that had traffic.
 */
export function aggregateCostByDay(
  rows: readonly UsageRow[],
  days?: readonly string[],
): DailyCostPoint[] {
  const byDay = new Map<string, DailyCostPoint>();
  const point = (day: string): DailyCostPoint => {
    let existing = byDay.get(day);
    if (existing === undefined) {
      existing = { day, llmInputTokens: 0, llmOutputTokens: 0, audioSeconds: 0 };
      byDay.set(day, existing);
    }
    return existing;
  };

  for (const day of days ?? []) point(day);

  for (const row of rows) {
    if (isJobRow(row)) continue;
    const entry = point(row.day);
    entry.llmInputTokens += metricValue(row, "llm_input_tokens");
    entry.llmOutputTokens += metricValue(row, "llm_output_tokens");
    entry.audioSeconds += metricValue(row, "audio_seconds");
  }

  return [...byDay.values()].sort((a, b) => a.day.localeCompare(b.day));
}

/**
 * Key a plugin's band takes on a chart point.
 *
 * Recharts addresses a stacked band by a flat `dataKey` on the datum, so the
 * per-plugin counts have to sit next to `day` and `total`. The `plugin:` prefix
 * keeps a plugin called `day` or `total` from overwriting them.
 */
export function seriesKey(plugin: string): string {
  return `plugin:${plugin}`;
}

/** One day of the documents chart: `day`, `total`, and one key per series. */
export interface DailyDocumentsPoint {
  /** `YYYY-MM-DD`. */
  day: string;
  /** Documents produced that day, all series summed. */
  total: number;
  /** Documents per plugin band, keyed by {@link seriesKey}. */
  [series: string]: number | string;
}

/** One band of the stacked documents chart. */
export interface DocumentSeries {
  /** `dataKey` to hand Recharts. */
  key: string;
  /** Plugin name, or {@link OTHER_SERIES} for the folded remainder. */
  plugin: string;
}

/** A stackable documents-per-day series set. */
export interface DailyDocuments {
  /** Points, oldest first. */
  points: DailyDocumentsPoint[];
  /**
   * Bands in stacking order: the biggest plugins first, then
   * {@link OTHER_SERIES} if anything was folded into it.
   */
  series: DocumentSeries[];
}

/**
 * Documents produced per day, split by plugin and capped at `topN` series.
 *
 * A tenant can easily run a dozen plugins; more than a handful of bands is a
 * chart nobody can read, so everything past `topN` (by total documents) is
 * folded into a single `other` band rather than dropped.
 */
export function documentsByDay(
  rows: readonly UsageRow[],
  options: { days?: readonly string[]; topN?: number } = {},
): DailyDocuments {
  const { days, topN = 5 } = options;

  const perPlugin = new Map<string, number>();
  for (const row of rows) {
    if (isJobRow(row)) continue;
    const documents = metricValue(row, "documents_out");
    if (documents === 0) continue;
    perPlugin.set(row.plugin, (perPlugin.get(row.plugin) ?? 0) + documents);
  }

  const ranked = [...perPlugin.entries()]
    .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
    .map(([plugin]) => plugin);
  const kept = new Set(ranked.slice(0, Math.max(0, topN)));
  const hasOther = ranked.length > kept.size;
  const series: DocumentSeries[] = [...kept, ...(hasOther ? [OTHER_SERIES] : [])].map(
    (plugin) => ({ key: seriesKey(plugin), plugin }),
  );

  const byDay = new Map<string, DailyDocumentsPoint>();
  const point = (day: string): DailyDocumentsPoint => {
    let existing = byDay.get(day);
    if (existing === undefined) {
      existing = { day, total: 0 };
      for (const band of series) existing[band.key] = 0;
      byDay.set(day, existing);
    }
    return existing;
  };

  for (const day of days ?? []) point(day);

  for (const row of rows) {
    if (isJobRow(row)) continue;
    const documents = metricValue(row, "documents_out");
    if (documents === 0) continue;
    const entry = point(row.day);
    const key = seriesKey(kept.has(row.plugin) ? row.plugin : OTHER_SERIES);
    const current = entry[key];
    entry[key] = (typeof current === "number" ? current : 0) + documents;
    entry.total += documents;
  }

  return {
    points: [...byDay.values()].sort((a, b) => a.day.localeCompare(b.day)),
    series,
  };
}
