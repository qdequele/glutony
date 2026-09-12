"use client";

/**
 * `GET /usage` — per-tenant consumption, for the Usage screen.
 *
 * Lives in its own file rather than in `client.ts` / `hooks.ts` so the usage
 * screen can evolve without touching the modules the other screens share. It
 * reuses `request` and `ApiError` from `./client`.
 *
 * Two things make this endpoint different from the rest of the API:
 *
 * 1. **501 is not a failure.** Usage analytics needs a Tinybird read token
 *    (`TINYBIRD_READ_TOKEN` on the gateway); without one the handler answers
 *    `501 {"code": "not_configured"}`. That is the default for self-hosted, so
 *    {@link fetchUsage} turns it into a `not_configured` result instead of a
 *    thrown error and the screen renders an explanatory state.
 * 2. **The metric columns are untyped on the wire.** The gateway flattens
 *    whatever numeric columns `tinybird/pipes/tenant_usage.pipe` returned, on
 *    purpose: adding a metric to the pipe must not require a gateway release.
 *    So {@link UsageRow} names the columns we know about but stays open, and
 *    every read goes through {@link metricValue}.
 */
import { useQuery, type UseQueryResult } from "@tanstack/react-query";

import { ApiError, request } from "./client";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/**
 * Metric columns `tenant_usage.pipe` returns today.
 *
 * Split into the two families the rollup fills from disjoint sets of raw
 * events — see `src/app/usage/_lib/aggregate.ts`, which is where the rule that
 * keeps them from double-counting lives.
 */
export const JOB_METRIC_KEYS = ["jobs", "jobs_succeeded", "jobs_failed"] as const;

/** Metric columns the rollup fills from `kind = 'step'` rows only. */
export const STEP_METRIC_KEYS = [
  "steps",
  "documents_out",
  "input_bytes",
  "duration_ms",
  "llm_input_tokens",
  "llm_output_tokens",
  "llm_requests",
  "audio_seconds",
  "pages",
  "images",
  "external_requests",
] as const;

/** Every metric column we know by name. Unknown extras are still tolerated. */
export const USAGE_METRIC_KEYS = [...JOB_METRIC_KEYS, ...STEP_METRIC_KEYS] as const;

/** A metric column filled from job-level (`plugin === ""`) rows. */
export type JobMetricKey = (typeof JOB_METRIC_KEYS)[number];
/** A metric column filled from step-level (`plugin !== ""`) rows. */
export type StepMetricKey = (typeof STEP_METRIC_KEYS)[number];
/** Any metric column we know by name. */
export type UsageMetricKey = JobMetricKey | StepMetricKey;

/**
 * What a metric column can hold on the wire. ClickHouse `UInt64` normally
 * arrives as a JSON number, but a large one can come back as a string, and a
 * column we do not know about can be anything at all.
 */
export type UsageColumnValue = number | string | boolean | null;

/**
 * One row of `tenant_usage`: a (day, pipeline_uid, plugin) triple plus the
 * metric columns.
 *
 * `plugin === ""` marks a job-level row. The index signature is what lets a
 * metric added to the Tinybird pipe flow through without a UI change.
 */
export interface UsageRow extends Partial<Record<UsageMetricKey, number>> {
  /** `YYYY-MM-DD`. */
  day: string;
  /** Pipeline the work ran through. */
  pipeline_uid: string;
  /** Plugin, empty string on job-level rows. */
  plugin: string;
  /** Metric columns, including ones this build does not know about. */
  [column: string]: UsageColumnValue | undefined;
}

/** 200 body of `GET /usage`. */
export interface UsageResponse {
  /** Tenant the rows belong to; empty string when self-hosted. */
  project_id: string;
  /** Daily rows, oldest first. */
  data: UsageRow[];
}

/**
 * The three honest outcomes of asking for usage.
 *
 * `not_configured` is a first-class state, not an error: it is what every
 * self-hosted deployment answers until a Tinybird read token is set.
 */
export type UsageResult =
  | { kind: "ready"; projectId: string; rows: UsageRow[] }
  | { kind: "not_configured"; message: string };

/** Inclusive `YYYY-MM-DD` day range, exactly as the endpoint takes it. */
export interface UsageDateRange {
  /** First day, inclusive. */
  from: string;
  /** Last day, inclusive. */
  to: string;
}

// ---------------------------------------------------------------------------
// Reading a metric off a row
// ---------------------------------------------------------------------------

/**
 * Read one metric column as a number, coercing what the wire actually sends.
 *
 * Returns `0` for a missing column, a non-numeric string, `null` or `NaN`:
 * a metric this build does not know about must never make a tile read `NaN`.
 */
export function metricValue(row: UsageRow, column: string): number {
  const value = row[column];
  if (typeof value === "number") return Number.isFinite(value) ? value : 0;
  if (typeof value === "string" && value.trim() !== "") {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : 0;
  }
  return 0;
}

/** True for a job-level row: the rollup puts `kind = 'job'` counters here. */
export function isJobRow(row: UsageRow): boolean {
  return row.plugin === "";
}

// ---------------------------------------------------------------------------
// Fetching
// ---------------------------------------------------------------------------

/**
 * `GET /usage?date_from=&date_to=`.
 *
 * A 501 `not_configured` is resolved, not thrown — see the module docs. Every
 * other non-2xx still throws {@link ApiError}.
 */
export async function fetchUsage(
  range: UsageDateRange,
  signal?: AbortSignal,
): Promise<UsageResult> {
  const query = new URLSearchParams({ date_from: range.from, date_to: range.to });
  try {
    const body = await request<UsageResponse>(`/usage?${query.toString()}`, { signal });
    return { kind: "ready", projectId: body.project_id ?? "", rows: body.data ?? [] };
  } catch (error) {
    if (error instanceof ApiError && (error.status === 501 || error.code === "not_configured")) {
      return { kind: "not_configured", message: error.message };
    }
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Query keys and hooks
// ---------------------------------------------------------------------------

/** Cache keys owned by the usage screen. Kept local, like `queryKeys` in `./hooks`. */
export const usageQueryKeys = {
  all: ["usage"] as const,
  range: (range: UsageDateRange) => ["usage", range.from, range.to] as const,
};

/**
 * Usage for one closed day range.
 *
 * The rollup behind the endpoint refreshes hourly, so there is no point
 * re-asking on every mount: `staleTime` matches that cadence loosely.
 */
export function useUsage(range: UsageDateRange): UseQueryResult<UsageResult, Error> {
  return useQuery({
    queryKey: usageQueryKeys.range(range),
    queryFn: ({ signal }) => fetchUsage(range, signal),
    staleTime: 5 * 60 * 1000,
  });
}
