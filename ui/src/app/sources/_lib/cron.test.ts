import { describe, expect, it } from "vitest";

import { describeCron, ordinal, validateCronShape } from "./cron";

describe("describeCron", () => {
  it("describes every-N-minutes patterns", () => {
    expect(describeCron("* * * * *")).toBe("Every minute");
    expect(describeCron("*/1 * * * *")).toBe("Every minute");
    expect(describeCron("*/15 * * * *")).toBe("Every 15 minutes");
  });

  it("describes hourly patterns", () => {
    expect(describeCron("0 * * * *")).toBe("Every hour at :00");
    expect(describeCron("5 * * * *")).toBe("Every hour at :05");
    expect(describeCron("30 */6 * * *")).toBe("Every 6 hours at :30");
  });

  it("describes daily patterns", () => {
    expect(describeCron("30 0 * * *")).toBe("Every day at 00:30");
    expect(describeCron("0 8 * * *")).toBe("Every day at 08:00");
    expect(describeCron("15 6,18 * * *")).toBe("Every day at 06:15 and 18:15");
    expect(describeCron("0 8 ? * ?")).toBe("Every day at 08:00");
  });

  it("describes weekly patterns", () => {
    expect(describeCron("30 8 * * 1")).toBe("Every Monday at 08:30");
    expect(describeCron("0 9 * * MON")).toBe("Every Monday at 09:00");
    expect(describeCron("0 9 * * 0")).toBe("Every Sunday at 09:00");
    expect(describeCron("0 9 * * 7")).toBe("Every Sunday at 09:00");
    expect(describeCron("0 9 * * 1,3,5")).toBe("Every Monday, Wednesday and Friday at 09:00");
    expect(describeCron("0 9 * * 1-5")).toBe("Every weekday (Monday to Friday) at 09:00");
    expect(describeCron("0 9 * * MON-FRI")).toBe("Every weekday (Monday to Friday) at 09:00");
    expect(describeCron("0 10 * * 6,0")).toBe("Every Saturday and Sunday at 10:00");
    expect(describeCron("0 10 * * 5-7")).toBe("Every Friday, Saturday and Sunday at 10:00");
  });

  it("describes monthly patterns", () => {
    expect(describeCron("0 0 1 * *")).toBe("On the 1st of every month at 00:00");
    expect(describeCron("45 23 22 * *")).toBe("On the 22nd of every month at 23:45");
    expect(describeCron("0 3 13 * *")).toBe("On the 13th of every month at 03:00");
  });

  it("describes the @ shorthands", () => {
    expect(describeCron("@daily")).toBe("Every day at 00:00");
    expect(describeCron("@HOURLY")).toBe("Every hour at :00");
    expect(describeCron("@weekly")).toBe("Every Sunday at 00:00");
  });

  it("falls back to the raw expression for what it cannot phrase", () => {
    expect(describeCron("0 9 * 1 *")).toBe("0 9 * 1 *");
    expect(describeCron("0 9-17 * * *")).toBe("0 9-17 * * *");
    expect(describeCron("0 9 1 * 1")).toBe("0 9 1 * 1");
    expect(describeCron("0 9 L * *")).toBe("0 9 L * *");
    expect(describeCron("*/5 9 * * *")).toBe("*/5 9 * * *");
    expect(describeCron("0 99 * * *")).toBe("0 99 * * *");
    expect(describeCron("not a cron")).toBe("not a cron");
  });

  it("trims and survives an empty input", () => {
    expect(describeCron("  0 8 * * *  ")).toBe("Every day at 08:00");
    expect(describeCron("")).toBe("");
  });
});

describe("validateCronShape", () => {
  it("accepts five fields and the known shorthands", () => {
    expect(validateCronShape("30 0 * * *")).toBeUndefined();
    expect(validateCronShape("*/15 9-17 * * MON-FRI")).toBeUndefined();
    expect(validateCronShape("0 0 L * ?")).toBeUndefined();
    expect(validateCronShape("@daily")).toBeUndefined();
  });

  it("rejects the wrong number of fields with a readable message", () => {
    expect(validateCronShape("0 0 * *")).toMatch(/Expected 5 fields.*got 4/);
    expect(validateCronShape("0 0 0 * * * *")).toMatch(/got 7/);
  });

  it("rejects empty input, stray characters and unknown shorthands", () => {
    expect(validateCronShape("   ")).toBe("Enter a cron expression.");
    expect(validateCronShape("0 8 * * $")).toMatch(/day of week/);
    expect(validateCronShape("@sometimes")).toMatch(/Unknown shorthand/);
  });
});

describe("ordinal", () => {
  it("handles the teens", () => {
    expect([1, 2, 3, 4, 11, 12, 13, 21, 22, 23, 31].map(ordinal)).toEqual([
      "1st",
      "2nd",
      "3rd",
      "4th",
      "11th",
      "12th",
      "13th",
      "21st",
      "22nd",
      "23rd",
      "31st",
    ]);
  });
});
