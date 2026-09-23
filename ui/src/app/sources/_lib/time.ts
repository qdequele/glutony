/**
 * Time formatting for the sources screens. `now` is always a parameter so the
 * helpers stay pure and testable.
 */
import { differenceInMilliseconds, formatDistanceStrict } from "date-fns";

import { formatDuration } from "@/lib/api/jobs";

/** Parse an RFC 3339 string (or pass a `Date` through); `undefined` when unusable. */
export function toDate(value: string | Date | undefined | null): Date | undefined {
  if (value === undefined || value === null || value === "") return undefined;
  const date = value instanceof Date ? value : new Date(value);
  return Number.isNaN(date.getTime()) ? undefined : date;
}

/** Below this distance a timestamp reads as "now" rather than "in 0 seconds". */
const NOW_THRESHOLD_MS = 30_000;

/**
 * Relative time of a scheduled run: `"in 5 minutes"`, `"in 3 hours"`,
 * `"2 days ago"`. A next run that is already due (or a few seconds off) reads
 * `"due now"`; a missing or unparsable value reads as an em dash.
 */
export function formatNextRun(value: string | Date | undefined | null, now: Date): string {
  const date = toDate(value);
  if (!date) return "—";
  const delta = differenceInMilliseconds(date, now);
  if (Math.abs(delta) < NOW_THRESHOLD_MS) return "due now";
  return formatDistanceStrict(date, now, { addSuffix: true, roundingMethod: "round" });
}

/**
 * Relative time of something that already happened (a last run): `"5 minutes
 * ago"`. `"just now"` inside the threshold, an em dash when unknown.
 */
export function formatPast(value: string | Date | undefined | null, now: Date): string {
  const date = toDate(value);
  if (!date) return "—";
  if (Math.abs(differenceInMilliseconds(date, now)) < NOW_THRESHOLD_MS) return "just now";
  return formatDistanceStrict(date, now, { addSuffix: true, roundingMethod: "round" });
}

/** Wall time a run took. A run still in flight has no `finished_at` and reads "running". */
export function formatRunDuration(
  startedAt: string | undefined,
  finishedAt: string | undefined,
): string {
  const start = toDate(startedAt);
  if (!start) return "—";
  const end = toDate(finishedAt);
  if (!end) return "running";
  return formatDuration(Math.max(0, differenceInMilliseconds(end, start)));
}
