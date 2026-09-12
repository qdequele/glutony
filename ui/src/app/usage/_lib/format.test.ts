import { describe, expect, it } from "vitest";

import {
  formatBytes,
  formatCount,
  formatDayShort,
  formatDuration,
  formatExact,
  formatMinutes,
  formatShare,
} from "./format";

describe("formatCount", () => {
  it("prints small counts in full and shortens the rest", () => {
    expect(formatCount(0)).toBe("0");
    expect(formatCount(7)).toBe("7");
    expect(formatCount(999)).toBe("999");
    expect(formatCount(1000)).toBe("1K");
    expect(formatCount(1240)).toBe("1.2K");
    expect(formatCount(3_400_000)).toBe("3.4M");
    expect(formatCount(2_000_000_000)).toBe("2B");
  });

  it("rounds rather than truncating, and survives a non-number", () => {
    expect(formatCount(999.6)).toBe("1K");
    expect(formatCount(Number.NaN)).toBe("—");
    expect(formatCount(Number.POSITIVE_INFINITY)).toBe("—");
  });
});

describe("formatExact", () => {
  it("groups thousands", () => {
    expect(formatExact(0)).toBe("0");
    expect(formatExact(1234567)).toBe("1,234,567");
    expect(formatExact(12.4)).toBe("12");
  });
});

describe("formatBytes", () => {
  it("uses IEC units, not SI", () => {
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(512)).toBe("512 B");
    expect(formatBytes(1023)).toBe("1023 B");
    expect(formatBytes(1024)).toBe("1.0 KiB");
    // 1000 bytes is NOT a kilobyte here.
    expect(formatBytes(1000)).toBe("1000 B");
    expect(formatBytes(1536)).toBe("1.5 KiB");
    expect(formatBytes(1024 * 1024)).toBe("1.0 MiB");
    expect(formatBytes(2.5 * 1024 ** 3)).toBe("2.5 GiB");
    expect(formatBytes(1024 ** 5)).toBe("1.0 PiB");
  });

  it("drops the decimal once the number is big enough to not need it", () => {
    expect(formatBytes(340 * 1024 * 1024)).toBe("340 MiB");
  });

  it("clamps at the largest unit it knows and rejects nonsense", () => {
    expect(formatBytes(5000 * 1024 ** 5)).toBe("5000 PiB");
    expect(formatBytes(-1)).toBe("—");
    expect(formatBytes(Number.NaN)).toBe("—");
  });
});

describe("formatMinutes", () => {
  it("reports audio seconds as minutes, the billing unit", () => {
    expect(formatMinutes(0)).toBe("0 min");
    expect(formatMinutes(30)).toBe("0.5 min");
    expect(formatMinutes(90)).toBe("1.5 min");
    expect(formatMinutes(600)).toBe("10 min");
    expect(formatMinutes(74_400)).toBe("1,240 min");
  });

  it("rejects nonsense", () => {
    expect(formatMinutes(-5)).toBe("—");
    expect(formatMinutes(Number.NaN)).toBe("—");
  });
});

describe("formatDuration", () => {
  it("picks a unit a human reads at a glance", () => {
    expect(formatDuration(0)).toBe("0 ms");
    expect(formatDuration(840)).toBe("840 ms");
    expect(formatDuration(4200)).toBe("4.2 s");
    expect(formatDuration(59_900)).toBe("59.9 s");
    expect(formatDuration(192_000)).toBe("3 m 12 s");
    expect(formatDuration(3600_000)).toBe("1 h 0 m");
    expect(formatDuration(18_120_000)).toBe("5 h 2 m");
  });

  it("rejects nonsense", () => {
    expect(formatDuration(-1)).toBe("—");
    expect(formatDuration(Number.NaN)).toBe("—");
  });
});

describe("formatShare", () => {
  it("reports a percentage of the total", () => {
    expect(formatShare(0, 100)).toBe("0%");
    expect(formatShare(37, 100)).toBe("37%");
    expect(formatShare(1, 3)).toBe("33%");
    expect(formatShare(100, 100)).toBe("100%");
  });

  it("keeps a decimal for slivers so they do not read as zero", () => {
    expect(formatShare(4, 1000)).toBe("0.4%");
    expect(formatShare(1, 10_000)).toBe("0.0%");
  });

  it("says nothing rather than 0% when there is no denominator", () => {
    expect(formatShare(0, 0)).toBe("—");
    expect(formatShare(5, -1)).toBe("—");
  });
});

describe("formatDayShort", () => {
  it("shortens an API day for a chart axis", () => {
    expect(formatDayShort("2026-09-13")).toBe("Sep 13");
    expect(formatDayShort("2026-01-01")).toBe("Jan 1");
    expect(formatDayShort("2026-12-31")).toBe("Dec 31");
  });

  it("passes anything that is not an API day straight through", () => {
    expect(formatDayShort("")).toBe("");
    expect(formatDayShort("2026-13-01")).toBe("2026-13-01");
    expect(formatDayShort("not a day")).toBe("not a day");
  });
});
