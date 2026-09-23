import { describe, expect, it } from "vitest";

import { hostError, isValidUid, uidError } from "./connections";

describe("isValidUid", () => {
  it("matches valid_uid on the gateway", () => {
    expect(isValidUid("prod-movies")).toBe(true);
    expect(isValidUid("a.b_c-9")).toBe(true);
    expect(isValidUid("")).toBe(false);
    expect(isValidUid("has space")).toBe(false);
    expect(isValidUid("slash/no")).toBe(false);
    expect(isValidUid("é")).toBe(false);
    expect(isValidUid("x".repeat(128))).toBe(true);
    expect(isValidUid("x".repeat(129))).toBe(false);
  });

  it("explains what is wrong", () => {
    expect(uidError("")).toBe("Enter a uid.");
    expect(uidError("ok")).toBeUndefined();
    expect(uidError("x".repeat(129))).toMatch(/128/);
    expect(uidError("no way")).toMatch(/letters, digits/);
  });
});

describe("hostError", () => {
  it("accepts absolute http(s) URLs", () => {
    expect(hostError("https://ms-1234.meilisearch.io")).toBeUndefined();
    expect(hostError(" http://localhost:7700/ ")).toBeUndefined();
  });

  it("rejects the rest", () => {
    expect(hostError("")).toMatch(/Enter/);
    expect(hostError("ms-1234.meilisearch.io")).toMatch(/absolute URL/);
    expect(hostError("ftp://example.com")).toMatch(/http\(s\)/);
  });
});
