/**
 * Date-range presets for the Usage screen.
 *
 * `GET /usage` takes two inclusive `YYYY-MM-DD` days, so everything here works
 * in whole calendar days rather than instants. Days are the browser's local
 * calendar days, which is what "today" means to the person reading the screen;
 * the rollup buckets on UTC days, so the very first and last day of a range can
 * straddle a boundary. That is called out in the footer of the page rather than
 * silently corrected — pretending to know the tenant's billing timezone would
 * be worse.
 */
import { addDays, format, isValid, parseISO, startOfMonth, subDays } from "date-fns";

import type { UsageDateRange } from "@/lib/api/usage";

/** The preset chosen in the range control. */
export type RangePreset = "7d" | "30d" | "month" | "custom";

/** A preset and its label, in the order the control lists them. */
export const RANGE_PRESETS: ReadonlyArray<{ value: RangePreset; label: string }> = [
  { value: "7d", label: "Last 7 days" },
  { value: "30d", label: "Last 30 days" },
  { value: "month", label: "This month" },
  { value: "custom", label: "Custom range" },
];

/** The preset the screen opens on. */
export const DEFAULT_PRESET: RangePreset = "30d";

/** Format a `Date` as the `YYYY-MM-DD` the endpoint expects, in local time. */
export function toApiDay(date: Date): string {
  return format(date, "yyyy-MM-dd");
}

/**
 * The `date_from` / `date_to` pair for a preset, relative to `today`.
 *
 * Both bounds are inclusive, so "last 7 days" spans `today - 6 … today` — seven
 * days including today, not eight. `"custom"` has no intrinsic range and falls
 * back to the 30-day window; the caller owns the custom bounds.
 */
export function rangeForPreset(preset: RangePreset, today: Date): UsageDateRange {
  const to = toApiDay(today);
  switch (preset) {
    case "7d":
      return { from: toApiDay(subDays(today, 6)), to };
    case "30d":
      return { from: toApiDay(subDays(today, 29)), to };
    case "month":
      // Month-to-date: the 1st of the current month through today. On the 1st
      // that is a single-day range, which is correct, not empty.
      return { from: toApiDay(startOfMonth(today)), to };
    case "custom":
      return { from: toApiDay(subDays(today, 29)), to };
  }
}

/** True for a plain `YYYY-MM-DD` that names a real calendar day. */
export function isApiDay(value: string): boolean {
  return /^\d{4}-\d{2}-\d{2}$/.test(value) && isValid(parseISO(value));
}

/**
 * Reject a range the endpoint would refuse or that cannot contain any data,
 * returning the reason to show under the inputs. `undefined` means valid.
 */
export function rangeError(range: UsageDateRange): string | undefined {
  if (!isApiDay(range.from)) return "Start date must be a valid YYYY-MM-DD date.";
  if (!isApiDay(range.to)) return "End date must be a valid YYYY-MM-DD date.";
  if (range.from > range.to) return "Start date must be on or before the end date.";
  return undefined;
}

/**
 * Every day of a closed range, oldest first.
 *
 * The charts use it to plot days with no ingestion as zero instead of joining
 * the two days that had traffic with a straight line. Capped at
 * `maxDays` so a hand-typed custom range of ten years cannot allocate an
 * unbounded array; past the cap the range is plotted from its rows alone.
 */
export function eachApiDay(range: UsageDateRange, maxDays = 400): string[] {
  if (rangeError(range) !== undefined) return [];
  const days: string[] = [];
  let cursor = parseISO(range.from);
  const last = parseISO(range.to);
  while (cursor.getTime() <= last.getTime() && days.length < maxDays) {
    days.push(toApiDay(cursor));
    cursor = addDays(cursor, 1);
  }
  return days;
}

/** Number of days a closed range covers, both bounds included. */
export function rangeLengthInDays(range: UsageDateRange): number {
  if (rangeError(range) !== undefined) return 0;
  const from = parseISO(range.from).getTime();
  const to = parseISO(range.to).getTime();
  return Math.round((to - from) / 86_400_000) + 1;
}
