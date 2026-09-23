/**
 * The source credential, as the form edits it.
 *
 * Secrets are write-only: the API returns `****` for every secret value and
 * never the value itself. So the form starts with empty secret inputs (showing
 * `****` as a placeholder when something is stored), and {@link authChange}
 * decides what a save means:
 *
 * - `keep`    — send no `auth` key at all; the stored credential is untouched;
 * - `clear`   — send `auth: null`;
 * - `replace` — send the full new credential (the API replaces, never merges).
 *
 * A partial edit that cannot be expressed without a secret the browser does not
 * know (renaming a header, changing the basic username) is `invalid`, with the
 * field to flag, rather than silently sending `****` back as a password.
 */
import type { FetchAuth, FetchAuthKind, RedactedFetchAuth } from "@/lib/api/sources";

/** What the kind Select can hold. `keep` exists only for an unreadable stored blob. */
export type AuthFormKind = "none" | FetchAuthKind | "keep";

/** One header row of the form. */
export interface HeaderRow {
  name: string;
  value: string;
}

/** The credential part of the source form. Secret inputs start empty. */
export interface AuthFormState {
  kind: AuthFormKind;
  token: string;
  username: string;
  password: string;
  headers: HeaderRow[];
}

/** Outcome of comparing the form against what is stored. */
export type AuthChange =
  | { kind: "keep" }
  | { kind: "clear" }
  | { kind: "replace"; auth: FetchAuth }
  | { kind: "invalid"; field: "token" | "username" | "password" | "headers"; message: string };

/** What the gateway uses as the masked value. */
export const MASK = "****";

/** Initial form state for a stored (redacted) credential, or none. */
export function authFormFromStored(stored: RedactedFetchAuth | undefined): AuthFormState {
  const base: AuthFormState = { kind: "none", token: "", username: "", password: "", headers: [] };
  if (!stored) return base;
  switch (stored.kind) {
    case "bearer":
      return { ...base, kind: "bearer" };
    case "basic":
      return { ...base, kind: "basic", username: stored.username };
    case "headers":
      return {
        ...base,
        kind: "headers",
        headers: Object.keys(stored.headers).map((name) => ({ name, value: "" })),
      };
    default:
      return { ...base, kind: "keep" };
  }
}

/** Trimmed, non-blank header rows as a record (last one wins on a duplicate name). */
export function headerRowsToRecord(rows: HeaderRow[]): Record<string, string> {
  const record: Record<string, string> = {};
  for (const row of rows) {
    const name = row.name.trim();
    if (name.length === 0) continue;
    record[name] = row.value;
  }
  return record;
}

/** Header rows the user actually filled in (a name, whatever the value). */
function namedRows(rows: HeaderRow[]): HeaderRow[] {
  return rows
    .map((row) => ({ name: row.name.trim(), value: row.value }))
    .filter((row) => row.name.length > 0);
}

function sameNames(a: string[], b: string[]): boolean {
  if (a.length !== b.length) return false;
  const left = [...a].sort();
  const right = [...b].sort();
  return left.every((name, index) => name === right[index]);
}

/** Build a full credential from the form, or say which field is missing. */
function fullCredential(form: AuthFormState): AuthChange {
  switch (form.kind) {
    case "bearer":
      if (form.token.length === 0) {
        return { kind: "invalid", field: "token", message: "Enter the token." };
      }
      return { kind: "replace", auth: { kind: "bearer", token: form.token } };
    case "basic":
      if (form.username.trim().length === 0) {
        return { kind: "invalid", field: "username", message: "Enter the username." };
      }
      if (form.password.length === 0) {
        return { kind: "invalid", field: "password", message: "Enter the password." };
      }
      return {
        kind: "replace",
        auth: { kind: "basic", username: form.username.trim(), password: form.password },
      };
    case "headers": {
      const rows = namedRows(form.headers);
      if (rows.length === 0) {
        return { kind: "invalid", field: "headers", message: "Add at least one header." };
      }
      const empty = rows.find((row) => row.value.length === 0);
      if (empty) {
        return {
          kind: "invalid",
          field: "headers",
          message: `Enter a value for ${empty.name}.`,
        };
      }
      return { kind: "replace", auth: { kind: "headers", headers: headerRowsToRecord(rows) } };
    }
    default:
      return { kind: "keep" };
  }
}

/**
 * What saving the form means for the credential. `stored` is the redacted
 * credential the API returned (`undefined` on create or when none is set).
 */
export function authChange(stored: RedactedFetchAuth | undefined, form: AuthFormState): AuthChange {
  if (form.kind === "keep") return { kind: "keep" };
  if (form.kind === "none") return stored ? { kind: "clear" } : { kind: "keep" };

  // New credential, or a different kind: every field must be filled in.
  if (!stored || stored.kind !== form.kind) return fullCredential(form);

  switch (stored.kind) {
    case "bearer":
      return form.token.length === 0 ? { kind: "keep" } : fullCredential(form);

    case "basic": {
      const usernameChanged = form.username.trim() !== stored.username;
      if (form.password.length > 0) return fullCredential(form);
      if (!usernameChanged) return { kind: "keep" };
      return {
        kind: "invalid",
        field: "password",
        message: "Re-enter the password to change the username.",
      };
    }

    case "headers": {
      const rows = namedRows(form.headers);
      const untouched =
        rows.every((row) => row.value.length === 0) &&
        sameNames(
          rows.map((row) => row.name),
          Object.keys(stored.headers),
        );
      if (untouched) return { kind: "keep" };
      const change = fullCredential(form);
      if (change.kind === "invalid" && rows.length > 0) {
        // The API replaces the whole set, so a value left blank cannot mean "keep".
        return {
          ...change,
          message: `${change.message} Changing the headers replaces all of them, so re-enter every value.`,
        };
      }
      return change;
    }

    default:
      return fullCredential(form);
  }
}

/**
 * The `auth` key of a request body: absent for `keep`, `null` for `clear`, the
 * credential for `replace`. Callers must reject `invalid` before building a body.
 */
export function authBodyValue(change: AuthChange): FetchAuth | null | undefined {
  switch (change.kind) {
    case "clear":
      return null;
    case "replace":
      return change.auth;
    default:
      return undefined;
  }
}

/** Placeholder for a secret input: `****` when a value is stored, empty otherwise. */
export function secretPlaceholder(
  stored: RedactedFetchAuth | undefined,
  kind: FetchAuthKind,
): string {
  return stored?.kind === kind ? MASK : "";
}
