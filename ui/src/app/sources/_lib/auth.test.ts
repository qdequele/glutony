import { describe, expect, it } from "vitest";

import type { RedactedFetchAuth } from "@/lib/api/sources";
import {
  authBodyValue,
  authChange,
  authFormFromStored,
  headerRowsToRecord,
  secretPlaceholder,
  type AuthFormState,
} from "./auth";

const bearer: RedactedFetchAuth = { kind: "bearer", token: "****" };
const basic: RedactedFetchAuth = { kind: "basic", username: "bot", password: "****" };
const headers: RedactedFetchAuth = {
  kind: "headers",
  headers: { "X-Api-Key": "****", "X-Tenant": "****" },
};

function form(patch: Partial<AuthFormState>): AuthFormState {
  return { kind: "none", token: "", username: "", password: "", headers: [], ...patch };
}

describe("authFormFromStored", () => {
  it("starts every secret input empty and keeps the non-secret parts", () => {
    expect(authFormFromStored(undefined).kind).toBe("none");
    expect(authFormFromStored(bearer)).toEqual(form({ kind: "bearer" }));
    expect(authFormFromStored(basic)).toEqual(form({ kind: "basic", username: "bot" }));
    expect(authFormFromStored(headers).headers).toEqual([
      { name: "X-Api-Key", value: "" },
      { name: "X-Tenant", value: "" },
    ]);
  });

  it("keeps an unreadable sealed credential rather than offering to clear it", () => {
    expect(authFormFromStored({ kind: "unknown", sealed: true }).kind).toBe("keep");
  });
});

describe("authChange: keep", () => {
  it("keeps when nothing is stored and nothing is chosen", () => {
    expect(authChange(undefined, form({}))).toEqual({ kind: "keep" });
  });

  it("keeps a stored credential whose form was left untouched", () => {
    expect(authChange(bearer, authFormFromStored(bearer))).toEqual({ kind: "keep" });
    expect(authChange(basic, authFormFromStored(basic))).toEqual({ kind: "keep" });
    expect(authChange(headers, authFormFromStored(headers))).toEqual({ kind: "keep" });
  });

  it("keeps an unknown blob", () => {
    expect(authChange({ kind: "unknown" }, form({ kind: "keep" }))).toEqual({ kind: "keep" });
  });
});

describe("authChange: clear", () => {
  it("clears when a stored credential is switched to none", () => {
    expect(authChange(bearer, form({ kind: "none" }))).toEqual({ kind: "clear" });
    expect(authChange(headers, form({ kind: "none" }))).toEqual({ kind: "clear" });
  });
});

describe("authChange: replace", () => {
  it("replaces a bearer token when a new one is typed", () => {
    expect(authChange(bearer, form({ kind: "bearer", token: "t2" }))).toEqual({
      kind: "replace",
      auth: { kind: "bearer", token: "t2" },
    });
  });

  it("replaces basic auth when a password is typed, username included", () => {
    expect(authChange(basic, form({ kind: "basic", username: " robot ", password: "p" }))).toEqual({
      kind: "replace",
      auth: { kind: "basic", username: "robot", password: "p" },
    });
  });

  it("replaces the whole header set when every value is re-entered", () => {
    const next = form({
      kind: "headers",
      headers: [
        { name: "X-Api-Key", value: "k" },
        { name: " ", value: "ignored" },
      ],
    });
    expect(authChange(headers, next)).toEqual({
      kind: "replace",
      auth: { kind: "headers", headers: { "X-Api-Key": "k" } },
    });
  });

  it("builds a new credential on create", () => {
    expect(authChange(undefined, form({ kind: "bearer", token: "t" }))).toEqual({
      kind: "replace",
      auth: { kind: "bearer", token: "t" },
    });
  });

  it("needs every field when the kind changes", () => {
    expect(authChange(bearer, form({ kind: "basic", username: "u", password: "p" }))).toEqual({
      kind: "replace",
      auth: { kind: "basic", username: "u", password: "p" },
    });
    expect(authChange(bearer, form({ kind: "basic", username: "u" }))).toMatchObject({
      kind: "invalid",
      field: "password",
    });
  });
});

describe("authChange: invalid", () => {
  it("refuses a new credential with a missing secret", () => {
    expect(authChange(undefined, form({ kind: "bearer" }))).toMatchObject({
      kind: "invalid",
      field: "token",
    });
    expect(authChange(undefined, form({ kind: "headers" }))).toMatchObject({
      kind: "invalid",
      field: "headers",
    });
  });

  it("refuses a username change without the password", () => {
    expect(authChange(basic, form({ kind: "basic", username: "other" }))).toMatchObject({
      kind: "invalid",
      field: "password",
    });
  });

  it("refuses a partial header edit: the API replaces the whole set", () => {
    const renamed = form({
      kind: "headers",
      headers: [
        { name: "X-Api-Key", value: "" },
        { name: "X-Other", value: "" },
      ],
    });
    const change = authChange(headers, renamed);
    expect(change).toMatchObject({ kind: "invalid", field: "headers" });
    expect(change.kind === "invalid" && change.message).toMatch(/re-enter every value/);
  });
});

describe("authBodyValue", () => {
  it("maps keep / clear / replace onto absent / null / object", () => {
    expect(authBodyValue({ kind: "keep" })).toBeUndefined();
    expect(authBodyValue({ kind: "clear" })).toBeNull();
    expect(authBodyValue({ kind: "replace", auth: { kind: "bearer", token: "t" } })).toEqual({
      kind: "bearer",
      token: "t",
    });
  });
});

describe("helpers", () => {
  it("shows **** only for the stored kind", () => {
    expect(secretPlaceholder(bearer, "bearer")).toBe("****");
    expect(secretPlaceholder(bearer, "basic")).toBe("");
    expect(secretPlaceholder(undefined, "bearer")).toBe("");
  });

  it("turns header rows into a record, dropping blank names", () => {
    expect(
      headerRowsToRecord([
        { name: " Accept ", value: "application/json" },
        { name: "", value: "x" },
      ]),
    ).toEqual({ Accept: "application/json" });
  });
});
