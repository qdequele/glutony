/**
 * Source form values ↔ API bodies.
 *
 * The form holds plain strings (react-hook-form works best that way); this
 * module turns them into a `POST /sources` body, or into the smallest
 * `PATCH /sources/{uid}` body that expresses what changed. Both endpoints are
 * `deny_unknown_fields`, so nothing outside the documented keys is ever sent.
 */
import type {
  CreateSourceBody,
  SourceLocation,
  SourceView,
  UpdateSourceBody,
} from "@/lib/api/sources";
import {
  authBodyValue,
  authFormFromStored,
  headerRowsToRecord,
  type AuthChange,
  type AuthFormState,
  type HeaderRow,
} from "./auth";

/** HTTP methods the form offers for a URL location. */
export const SOURCE_METHODS = ["GET", "POST"] as const;

/** Default schedule for a new source: once a day, just after midnight. */
export const DEFAULT_CRON = "30 0 * * *";

/** The gateway's default timezone. */
export const DEFAULT_TIMEZONE = "UTC";

/** Everything the source form edits. */
export interface SourceFormValues {
  uid: string;
  name: string;
  description: string;
  pipeline: string;
  url: string;
  method: string;
  headers: HeaderRow[];
  cron: string;
  timezone: string;
  index: string;
  auth: AuthFormState;
}

/** A blank form, for `/sources/new`. */
export function emptySourceFormValues(): SourceFormValues {
  return {
    uid: "",
    name: "",
    description: "",
    pipeline: "",
    url: "",
    method: "GET",
    headers: [],
    cron: DEFAULT_CRON,
    timezone: DEFAULT_TIMEZONE,
    index: "",
    auth: authFormFromStored(undefined),
  };
}

/** The form for an existing source. Secret inputs start empty. */
export function sourceToFormValues(source: SourceView): SourceFormValues {
  return {
    uid: source.uid,
    // The gateway defaults the name to the uid; show it only when it says more.
    name: source.name === source.uid ? "" : source.name,
    description: source.description ?? "",
    pipeline: source.pipeline,
    url: source.location.url,
    method: (source.location.method ?? "GET").toUpperCase(),
    headers: Object.entries(source.location.headers ?? {}).map(([name, value]) => ({
      name,
      value,
    })),
    cron: source.cron,
    timezone: source.timezone,
    index: source.index ?? "",
    auth: authFormFromStored(source.auth),
  };
}

/** The `location` the form describes, with defaults left implicit. */
export function formLocation(values: SourceFormValues): SourceLocation {
  const location: SourceLocation = { kind: "url", url: values.url.trim() };
  const method = values.method.trim().toUpperCase();
  if (method && method !== "GET") location.method = method;
  const headers = headerRowsToRecord(values.headers);
  if (Object.keys(headers).length > 0) location.headers = headers;
  return location;
}

/** Normalize a stored location the same way, so the two compare equal when unchanged. */
function normalizeLocation(location: SourceLocation): SourceLocation {
  const normalized: SourceLocation = { kind: "url", url: location.url };
  const method = location.method?.toUpperCase();
  if (method && method !== "GET") normalized.method = method;
  if (location.headers && Object.keys(location.headers).length > 0) {
    normalized.headers = location.headers;
  }
  return normalized;
}

function sortedJson(value: SourceLocation): string {
  const headers = value.headers
    ? Object.fromEntries(Object.entries(value.headers).sort(([a], [b]) => a.localeCompare(b)))
    : undefined;
  return JSON.stringify({ url: value.url, method: value.method ?? "GET", headers });
}

/** Whether two locations are the same once defaults and header order are ignored. */
export function sameLocation(a: SourceLocation, b: SourceLocation): boolean {
  return sortedJson(normalizeLocation(a)) === sortedJson(normalizeLocation(b));
}

/** `POST /sources` body. `auth` must already be validated (not `invalid`). */
export function buildCreateBody(values: SourceFormValues, auth: AuthChange): CreateSourceBody {
  const body: CreateSourceBody = {
    uid: values.uid.trim(),
    pipeline: values.pipeline,
    location: formLocation(values),
    cron: values.cron.trim(),
    timezone: values.timezone || DEFAULT_TIMEZONE,
  };
  const name = values.name.trim();
  if (name) body.name = name;
  const description = values.description.trim();
  if (description) body.description = description;
  const index = values.index.trim();
  if (index) body.index = index;
  const credential = authBodyValue(auth);
  if (credential) body.auth = credential;
  return body;
}

/**
 * The smallest `PATCH /sources/{uid}` body for what changed. An empty object
 * means there is nothing to save.
 *
 * Two fields cannot be cleared through the API today (the control plane
 * `COALESCE`s them): the index override, and the name, which falls back to the
 * uid. A cleared index is therefore not sent — the form flags it instead — and
 * a cleared name is sent as the uid.
 */
export function buildPatchBody(
  stored: SourceView,
  values: SourceFormValues,
  auth: AuthChange,
): UpdateSourceBody {
  const body: UpdateSourceBody = {};

  const name = values.name.trim() || stored.uid;
  if (name !== stored.name) body.name = name;

  const description = values.description.trim();
  if (description !== (stored.description ?? "")) body.description = description;

  if (values.pipeline !== stored.pipeline) body.pipeline = values.pipeline;

  const location = formLocation(values);
  if (!sameLocation(location, stored.location)) body.location = location;

  const cron = values.cron.trim();
  if (cron !== stored.cron) body.cron = cron;

  const timezone = values.timezone || DEFAULT_TIMEZONE;
  if (timezone !== stored.timezone) body.timezone = timezone;

  const index = values.index.trim();
  if (index && index !== (stored.index ?? "")) body.index = index;

  const credential = authBodyValue(auth);
  if (credential !== undefined) body.auth = credential;

  return body;
}

/** True when the form would clear an index override, which the API cannot do yet. */
export function clearsIndex(stored: SourceView | undefined, values: SourceFormValues): boolean {
  return Boolean(stored?.index) && values.index.trim().length === 0;
}

/** Timezones offered first; any other IANA name can still be typed or kept. */
export const COMMON_TIMEZONES = [
  "UTC",
  "Europe/London",
  "Europe/Paris",
  "Europe/Berlin",
  "Europe/Madrid",
  "Europe/Amsterdam",
  "America/New_York",
  "America/Chicago",
  "America/Denver",
  "America/Los_Angeles",
  "America/Sao_Paulo",
  "Asia/Tokyo",
  "Asia/Singapore",
  "Asia/Kolkata",
  "Asia/Shanghai",
  "Australia/Sydney",
] as const;

/**
 * The options of the timezone Select: the common list, plus the browser's own
 * zone and the current value when they are not in it — a stored source must
 * never display an empty timezone just because its zone is uncommon.
 */
export function timezoneOptions(current: string, browserZone: string | undefined): string[] {
  const options: string[] = [...COMMON_TIMEZONES];
  for (const extra of [browserZone, current]) {
    if (extra && !options.includes(extra)) options.push(extra);
  }
  return options;
}

/** Tokens a URL may contain, for the help text under the input. */
export const URL_TEMPLATE_TOKENS = [
  { token: "{{ date:%Y-%m-%d }}", meaning: "the scheduled date, strftime-formatted" },
  {
    token: "{{ date-1d:%m_%d_%Y }}",
    meaning: "the same, offset by -1d, +2d, -3h, +30m, ...",
  },
  { token: "{{ timestamp }}", meaning: "Unix seconds of the scheduled time" },
] as const;
