import { describe, expect, it } from "vitest";

import {
  DEFAULT_PRESET,
  RANGE_PRESETS,
  eachApiDay,
  isApiDay,
  rangeError,
  rangeForPreset,
  rangeLengthInDays,
  toApiDay,
} from "./range";

/** A local-midnight `Date`, so the tests are not at the mercy of the runner's TZ. */
function localDay(year: number, month: number, day: number): Date {
  return new Date(year, month - 1, day, 12, 0, 0, 0);
}

describe("rangeForPreset", () => {
  const today = localDay(2026, 9, 13);

  it("makes both bounds inclusive: 7 days means 7, not 8", () => {
    expect(rangeForPreset("7d", today)).toEqual({ from: "2026-09-07", to: "2026-09-13" });
    expect(rangeLengthInDays(rangeForPreset("7d", today))).toBe(7);
  });

  it("defaults to the last 30 days", () => {
    expect(DEFAULT_PRESET).toBe("30d");
    expect(rangeForPreset("30d", today)).toEqual({ from: "2026-08-15", to: "2026-09-13" });
    expect(rangeLengthInDays(rangeForPreset("30d", today))).toBe(30);
  });

  it("reads 'this month' as month-to-date", () => {
    expect(rangeForPreset("month", today)).toEqual({ from: "2026-09-01", to: "2026-09-13" });
  });

  it("gives a one-day range on the first of the month, not an empty one", () => {
    const firstOfMarch = localDay(2026, 3, 1);
    expect(rangeForPreset("month", firstOfMarch)).toEqual({
      from: "2026-03-01",
      to: "2026-03-01",
    });
    expect(rangeLengthInDays(rangeForPreset("month", firstOfMarch))).toBe(1);
  });

  it("crosses a month boundary backwards", () => {
    const firstOfMarch = localDay(2026, 3, 1);
    // 2026 is not a leap year: 1 Mar - 29 days = 31 Jan.
    expect(rangeForPreset("30d", firstOfMarch).from).toBe("2026-01-31");
    expect(rangeForPreset("7d", firstOfMarch).from).toBe("2026-02-23");
  });

  it("crosses a leap day correctly", () => {
    const firstOfMarch2024 = localDay(2024, 3, 1);
    // 2024 is a leap year, so the same subtraction lands on 1 Feb.
    expect(rangeForPreset("30d", firstOfMarch2024).from).toBe("2024-02-01");
    expect(rangeForPreset("7d", firstOfMarch2024).from).toBe("2024-02-24");
  });

  it("crosses a year boundary", () => {
    const newYearsDay = localDay(2026, 1, 1);
    expect(rangeForPreset("30d", newYearsDay)).toEqual({ from: "2025-12-03", to: "2026-01-01" });
    expect(rangeForPreset("month", newYearsDay)).toEqual({
      from: "2026-01-01",
      to: "2026-01-01",
    });
  });

  it("falls back to the 30-day window for 'custom', which has no intrinsic range", () => {
    expect(rangeForPreset("custom", today)).toEqual(rangeForPreset("30d", today));
  });

  it("lists every preset in the control", () => {
    expect(RANGE_PRESETS.map((preset) => preset.value)).toEqual([
      "7d",
      "30d",
      "month",
      "custom",
    ]);
  });
});

describe("toApiDay", () => {
  it("emits the zero-padded YYYY-MM-DD the endpoint validates against", () => {
    expect(toApiDay(localDay(2026, 1, 5))).toBe("2026-01-05");
    expect(toApiDay(localDay(2026, 12, 31))).toBe("2026-12-31");
  });
});

describe("isApiDay", () => {
  it("accepts a real, zero-padded calendar day", () => {
    expect(isApiDay("2026-09-01")).toBe(true);
    expect(isApiDay("2024-02-29")).toBe(true);
  });

  it("rejects what the gateway would reject", () => {
    for (const bad of ["2026-9-1", "2026-09-011", "", "yyyy-mm-dd", "2026-13-01", "' OR 1=1"]) {
      expect(isApiDay(bad), bad).toBe(false);
    }
  });
});

describe("rangeError", () => {
  it("accepts a valid closed range, including a single day", () => {
    expect(rangeError({ from: "2026-09-01", to: "2026-09-30" })).toBeUndefined();
    expect(rangeError({ from: "2026-09-01", to: "2026-09-01" })).toBeUndefined();
  });

  it("names the bound that is wrong", () => {
    expect(rangeError({ from: "nope", to: "2026-09-30" })).toMatch(/Start date/);
    expect(rangeError({ from: "2026-09-01", to: "nope" })).toMatch(/End date/);
    expect(rangeError({ from: "2026-09-30", to: "2026-09-01" })).toMatch(/on or before/);
  });
});

describe("rangeLengthInDays", () => {
  it("counts both bounds", () => {
    expect(rangeLengthInDays({ from: "2026-09-01", to: "2026-09-01" })).toBe(1);
    expect(rangeLengthInDays({ from: "2026-09-01", to: "2026-09-30" })).toBe(30);
    expect(rangeLengthInDays({ from: "2026-01-01", to: "2026-12-31" })).toBe(365);
  });

  it("is zero for an invalid range", () => {
    expect(rangeLengthInDays({ from: "2026-09-30", to: "2026-09-01" })).toBe(0);
  });
});

describe("eachApiDay", () => {
  it("expands a range into every day it covers", () => {
    expect(eachApiDay({ from: "2026-02-27", to: "2026-03-02" })).toEqual([
      "2026-02-27",
      "2026-02-28",
      "2026-03-01",
      "2026-03-02",
    ]);
  });

  it("includes the leap day", () => {
    expect(eachApiDay({ from: "2024-02-28", to: "2024-03-01" })).toEqual([
      "2024-02-28",
      "2024-02-29",
      "2024-03-01",
    ]);
  });

  it("caps a hand-typed decade so the chart cannot allocate forever", () => {
    expect(eachApiDay({ from: "2016-01-01", to: "2026-01-01" })).toHaveLength(400);
  });

  it("is empty for an invalid range", () => {
    expect(eachApiDay({ from: "2026-09-30", to: "2026-09-01" })).toEqual([]);
    expect(eachApiDay({ from: "nope", to: "2026-09-01" })).toEqual([]);
  });
});
