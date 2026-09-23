import { describe, expect, it } from "vitest";

import { formatNextRun, formatPast, formatRunDuration, toDate } from "./time";

const now = new Date("2026-09-23T10:00:00Z");

describe("formatNextRun", () => {
  it("formats upcoming runs relative to now", () => {
    expect(formatNextRun("2026-09-23T10:05:00Z", now)).toBe("in 5 minutes");
    expect(formatNextRun("2026-09-23T13:00:00Z", now)).toBe("in 3 hours");
    expect(formatNextRun(new Date("2026-09-25T10:00:00Z"), now)).toBe("in 2 days");
  });

  it("formats a run in the past with a suffix", () => {
    expect(formatNextRun("2026-09-23T09:00:00Z", now)).toBe("1 hour ago");
  });

  it("reads 'due now' within a few seconds either way", () => {
    expect(formatNextRun("2026-09-23T10:00:10Z", now)).toBe("due now");
    expect(formatNextRun("2026-09-23T09:59:50Z", now)).toBe("due now");
  });

  it("renders missing and unparsable values as an em dash", () => {
    expect(formatNextRun(undefined, now)).toBe("—");
    expect(formatNextRun(null, now)).toBe("—");
    expect(formatNextRun("", now)).toBe("—");
    expect(formatNextRun("tomorrow-ish", now)).toBe("—");
  });
});

describe("formatPast", () => {
  it("formats last runs", () => {
    expect(formatPast("2026-09-23T09:55:00Z", now)).toBe("5 minutes ago");
    expect(formatPast("2026-09-23T09:59:55Z", now)).toBe("just now");
    expect(formatPast(undefined, now)).toBe("—");
  });
});

describe("formatRunDuration", () => {
  it("measures finished runs", () => {
    expect(formatRunDuration("2026-09-23T10:00:00Z", "2026-09-23T10:00:01.500Z")).toBe("1.50 s");
    expect(formatRunDuration("2026-09-23T10:00:00Z", "2026-09-23T10:01:35Z")).toBe("1m 35s");
  });

  it("marks a run without finished_at as running", () => {
    expect(formatRunDuration("2026-09-23T10:00:00Z", undefined)).toBe("running");
  });

  it("never renders a negative duration or a bad start", () => {
    expect(formatRunDuration("2026-09-23T10:00:05Z", "2026-09-23T10:00:00Z")).toBe("0 ms");
    expect(formatRunDuration("nope", "2026-09-23T10:00:00Z")).toBe("—");
  });
});

describe("toDate", () => {
  it("parses RFC 3339 and passes dates through", () => {
    expect(toDate("2026-09-23T10:00:00Z")?.toISOString()).toBe("2026-09-23T10:00:00.000Z");
    expect(toDate(now)).toBe(now);
    expect(toDate("garbage")).toBeUndefined();
  });
});
