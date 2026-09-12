/**
 * Human formatting for the Usage screen.
 *
 * Every number on this screen is a raw counter from ClickHouse — bytes, tokens,
 * milliseconds, seconds of audio — and none of them are readable as-is. These
 * are pure so they can be unit tested; nothing here touches the DOM or a
 * locale the tests cannot pin down (`"en-US"` throughout, matching the rest of
 * the admin UI, which is English-only).
 */

/** Locale used for every number on the screen. The admin UI is English-only. */
const LOCALE = "en-US";

const compact = new Intl.NumberFormat(LOCALE, {
  notation: "compact",
  maximumFractionDigits: 1,
});

const exact = new Intl.NumberFormat(LOCALE, { maximumFractionDigits: 0 });

/**
 * A count, shortened once it stops being readable: `842`, `1.2K`, `3.4M`.
 *
 * Values below 1000 are printed in full — "976" is more useful than "1K" and
 * short enough to fit anywhere.
 */
export function formatCount(value: number): string {
  if (!Number.isFinite(value)) return "—";
  const rounded = Math.round(value);
  return Math.abs(rounded) < 1000 ? exact.format(rounded) : compact.format(rounded);
}

/** A count in full, with thousands separators. For table cells and tooltips. */
export function formatExact(value: number): string {
  if (!Number.isFinite(value)) return "—";
  return exact.format(Math.round(value));
}

const IEC_UNITS = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"] as const;

/**
 * Bytes in IEC units (1024-based): `0 B`, `512 B`, `1.5 KiB`, `2.3 GiB`.
 *
 * IEC rather than SI because the number came from counting bytes of a payload,
 * and every other tool an operator has open (`ls -h`, `du -h`) is 1024-based.
 */
export function formatBytes(value: number): string {
  if (!Number.isFinite(value) || value < 0) return "—";
  if (value < 1024) return `${Math.round(value)} B`;
  let size = value;
  let unit = 0;
  while (size >= 1024 && unit < IEC_UNITS.length - 1) {
    size /= 1024;
    unit += 1;
  }
  // One decimal below 10 (2.4 GiB), none above (340 MiB): three significant
  // figures is as much precision as a summary tile can justify.
  return `${size < 10 ? size.toFixed(1) : Math.round(size).toString()} ${IEC_UNITS[unit]}`;
}

/**
 * Audio seconds as minutes, which is how transcription is priced and quoted:
 * `0 min`, `0.5 min`, `12 min`, `1,240 min`.
 *
 * Deliberately not "20h 40m": the tile answers "how much did we transcribe",
 * and minutes are the billing unit.
 */
export function formatMinutes(seconds: number): string {
  if (!Number.isFinite(seconds) || seconds < 0) return "—";
  if (seconds === 0) return "0 min";
  const minutes = seconds / 60;
  if (minutes < 10) return `${minutes.toFixed(1)} min`;
  return `${exact.format(Math.round(minutes))} min`;
}

/**
 * A duration in milliseconds as something an operator reads at a glance:
 * `840 ms`, `4.2 s`, `3 m 12 s`, `5 h 2 m`.
 */
export function formatDuration(ms: number): string {
  if (!Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1000) return `${Math.round(ms)} ms`;
  const totalSeconds = ms / 1000;
  if (totalSeconds < 60) return `${totalSeconds.toFixed(1)} s`;
  const totalMinutes = Math.floor(totalSeconds / 60);
  const seconds = Math.round(totalSeconds - totalMinutes * 60);
  if (totalMinutes < 60) return `${totalMinutes} m ${seconds} s`;
  const hours = Math.floor(totalMinutes / 60);
  return `${exact.format(hours)} h ${totalMinutes - hours * 60} m`;
}

/**
 * A share of a total, as a percentage: `0%`, `0.4%`, `37%`, `100%`.
 *
 * `total <= 0` yields `—` rather than `0%`: no denominator is not the same
 * answer as no share.
 */
export function formatShare(value: number, total: number): string {
  if (!Number.isFinite(value) || !Number.isFinite(total) || total <= 0) return "—";
  const percent = (value / total) * 100;
  if (percent === 0) return "0%";
  if (percent < 1) return `${percent.toFixed(1)}%`;
  return `${Math.round(percent)}%`;
}

/** `2026-09-13` → `Sep 13`, for a chart axis. Passes anything else through. */
export function formatDayShort(day: string): string {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(day);
  if (!match) return day;
  const month = Number(match[2]);
  const date = Number(match[3]);
  const MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];
  if (month < 1 || month > 12) return day;
  return `${MONTHS[month - 1]} ${date}`;
}
