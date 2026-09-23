/**
 * Cron expressions, as far as the form needs to understand them.
 *
 * Temporal is the authority: it parses the expression when the schedule is
 * created and the gateway turns a rejection into a 422. This module only does
 * two cheap things on the client — check the five-field shape so an obvious
 * typo is caught before the round trip, and describe the common patterns in
 * words. Anything it does not recognise is described as the raw expression,
 * never guessed at.
 */

/** Names Temporal accepts for the `@` shorthands, and what they mean. */
const MACROS: Record<string, string> = {
  "@yearly": "Every year on January 1st at 00:00",
  "@annually": "Every year on January 1st at 00:00",
  "@monthly": "On the 1st of every month at 00:00",
  "@weekly": "Every Sunday at 00:00",
  "@daily": "Every day at 00:00",
  "@midnight": "Every day at 00:00",
  "@hourly": "Every hour at :00",
};

/** Field names in order, for error messages. */
export const CRON_FIELDS = ["minute", "hour", "day of month", "month", "day of week"] as const;

/** Characters a single cron field may contain (numbers, names, `* , - / ?`, `L W #`). */
const FIELD_PATTERN = /^[0-9A-Za-z*,\-/?#]+$/;

/**
 * Client-side shape check. Returns an error message, or `undefined` when the
 * expression looks like five cron fields (or a known `@` shorthand).
 */
export function validateCronShape(expression: string): string | undefined {
  const trimmed = expression.trim();
  if (trimmed.length === 0) return "Enter a cron expression.";
  if (trimmed.startsWith("@")) {
    return Object.hasOwn(MACROS, trimmed.toLowerCase())
      ? undefined
      : `Unknown shorthand ${trimmed}. Use @hourly, @daily, @weekly, @monthly or @yearly.`;
  }
  const fields = trimmed.split(/\s+/);
  if (fields.length !== CRON_FIELDS.length) {
    return `Expected 5 fields (${CRON_FIELDS.join(", ")}), got ${fields.length}.`;
  }
  const bad = fields.findIndex((field) => !FIELD_PATTERN.test(field));
  if (bad !== -1) return `The ${CRON_FIELDS[bad]} field "${fields[bad]}" is not valid.`;
  return undefined;
}

const DAY_NAMES = [
  "Sunday",
  "Monday",
  "Tuesday",
  "Wednesday",
  "Thursday",
  "Friday",
  "Saturday",
] as const;

const DAY_ALIASES: Record<string, number> = {
  SUN: 0,
  MON: 1,
  TUE: 2,
  WED: 3,
  THU: 4,
  FRI: 5,
  SAT: 6,
};

/** A plain non-negative integer field (`5`, `30`), or `undefined`. */
function integer(field: string, max: number): number | undefined {
  if (!/^\d{1,2}$/.test(field)) return undefined;
  const value = Number(field);
  return value <= max ? value : undefined;
}

/** `*` / `?`, which both mean "any". */
function isAny(field: string): boolean {
  return field === "*" || field === "?";
}

/** `*\/N` with N ≥ 1, or `undefined`. */
function step(field: string): number | undefined {
  const match = /^\*\/(\d{1,2})$/.exec(field);
  if (!match) return undefined;
  const value = Number(match[1]);
  return value >= 1 ? value : undefined;
}

function pad(value: number): string {
  return String(value).padStart(2, "0");
}

/** `1st`, `2nd`, `3rd`, `4th`, `11th`, `21st`, ... */
export function ordinal(value: number): string {
  const lastTwo = value % 100;
  if (lastTwo >= 11 && lastTwo <= 13) return `${value}th`;
  switch (value % 10) {
    case 1:
      return `${value}st`;
    case 2:
      return `${value}nd`;
    case 3:
      return `${value}rd`;
    default:
      return `${value}th`;
  }
}

function dayNumber(token: string): number | undefined {
  const upper = token.toUpperCase();
  if (upper in DAY_ALIASES) return DAY_ALIASES[upper];
  const value = integer(token, 7);
  if (value === undefined) return undefined;
  // 7 is Sunday too.
  return value % 7;
}

/**
 * The days a day-of-week field selects, in week order, or `undefined` when the
 * field is not a simple list/range of days.
 */
function daysOfWeek(field: string): number[] | undefined {
  const days = new Set<number>();
  for (const part of field.split(",")) {
    const range = part.split("-");
    if (range.length === 1) {
      const day = dayNumber(part);
      if (day === undefined) return undefined;
      days.add(day);
    } else if (range.length === 2) {
      const from = dayNumber(range[0]);
      // `5-7` must include Sunday, so read the upper bound before folding 7 onto 0.
      const to =
        range[1].toUpperCase() in DAY_ALIASES ? dayNumber(range[1]) : integer(range[1], 7);
      if (from === undefined || to === undefined || to < from) return undefined;
      for (let day = from; day <= to; day += 1) days.add(day % 7);
    } else {
      return undefined;
    }
  }
  // Monday-first, the way people read a week.
  return [...days].sort((a, b) => ((a + 6) % 7) - ((b + 6) % 7));
}

function describeDays(days: number[]): string {
  const key = days.join(",");
  if (key === "1,2,3,4,5") return "Every weekday (Monday to Friday)";
  if (key === "6,0") return "Every Saturday and Sunday";
  if (days.length === 7) return "Every day";
  const names = days.map((day) => DAY_NAMES[day]);
  if (names.length === 1) return `Every ${names[0]}`;
  return `Every ${names.slice(0, -1).join(", ")} and ${names[names.length - 1]}`;
}

/**
 * Describe a cron expression in words: `"30 8 * * 1"` → `"Every Monday at
 * 08:30"`. Falls back to the expression itself for anything it does not
 * recognise (ranges of hours, month restrictions, `L`, ...).
 */
export function describeCron(expression: string): string {
  const trimmed = expression.trim();
  if (trimmed.length === 0) return "";
  const macro = MACROS[trimmed.toLowerCase()];
  if (macro) return macro;

  const fields = trimmed.split(/\s+/);
  if (fields.length !== 5) return trimmed;
  const [minute, hour, dayOfMonth, month, dayOfWeek] = fields;
  // A month restriction is rare and hard to phrase well: leave it raw.
  if (!isAny(month)) return trimmed;

  const everyDay = isAny(dayOfMonth) && isAny(dayOfWeek);

  // Minute-level patterns: `* * * * *`, `*/15 * * * *`.
  if (isAny(hour) && everyDay) {
    if (isAny(minute)) return "Every minute";
    const everyMinutes = step(minute);
    if (everyMinutes !== undefined) {
      return everyMinutes === 1 ? "Every minute" : `Every ${everyMinutes} minutes`;
    }
  }

  const m = integer(minute, 59);
  if (m === undefined) return trimmed;

  // Hour-level patterns: `5 * * * *`, `0 */6 * * *`.
  if (everyDay) {
    if (isAny(hour)) return `Every hour at :${pad(m)}`;
    const everyHours = step(hour);
    if (everyHours !== undefined) {
      return everyHours === 1
        ? `Every hour at :${pad(m)}`
        : `Every ${everyHours} hours at :${pad(m)}`;
    }
  }

  // Everything below fires at fixed times of day: one or a few hours.
  const hours = hour.split(",").map((part) => integer(part, 23));
  if (hours.some((value) => value === undefined)) return trimmed;
  const times = (hours as number[]).map((h) => `${pad(h)}:${pad(m)}`);
  const at =
    times.length === 1
      ? times[0]
      : `${times.slice(0, -1).join(", ")} and ${times[times.length - 1]}`;

  if (everyDay) return `Every day at ${at}`;

  if (isAny(dayOfMonth)) {
    const days = daysOfWeek(dayOfWeek);
    if (!days || days.length === 0) return trimmed;
    return `${describeDays(days)} at ${at}`;
  }

  if (isAny(dayOfWeek)) {
    const day = integer(dayOfMonth, 31);
    if (day === undefined || day === 0) return trimmed;
    return `On the ${ordinal(day)} of every month at ${at}`;
  }

  return trimmed;
}
