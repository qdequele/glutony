import { describe, expect, it } from "vitest";

import { deriveMode, lineCount, needsInitialOps, toBuilder, toCode, withInitialOps } from "./mode";

describe("mode", () => {
  it("derives the mode from the config key", () => {
    expect(deriveMode({ ops: [] })).toBe("builder");
    expect(deriveMode({ script: "1" })).toBe("code");
    expect(deriveMode({})).toBe("builder");
    expect(deriveMode({ ops: [], script: "1" })).toBe("builder");
  });
  it("initialises a fresh step with an empty ops list", () => {
    expect(needsInitialOps({ on_error: "skip" })).toBe(true);
    expect(needsInitialOps({ script: "" })).toBe(false);
    expect(withInitialOps({ on_error: "skip" })).toEqual({ on_error: "skip", ops: [] });
  });
  it("switches to the builder by discarding the script", () => {
    expect(toBuilder({ script: "x", max_ops: 5 })).toEqual({ max_ops: 5, ops: [] });
  });
  it("switches to code with the given script and no ops", () => {
    expect(toCode({ ops: [{ drop: ["a"] }], max_ops: 5 }, "doc;")).toEqual({ max_ops: 5, script: "doc;" });
  });
  it("counts script lines", () => {
    expect(lineCount("a\nb\nc")).toBe(3);
    expect(lineCount("")).toBe(0);
    expect(lineCount(undefined)).toBe(0);
  });
});
