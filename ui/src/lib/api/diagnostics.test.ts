import { describe, expect, it } from "vitest";

import { diagnosticsForStep, parseDiagnostics, parseGeneratedScripts } from "./diagnostics";

describe("parseDiagnostics", () => {
  it("reads well-formed entries", () => {
    expect(
      parseDiagnostics({
        code: "invalid_step_config",
        diagnostics: [{ step: "a", path: "script", message: "boom", line: 2, column: 5 }],
      }),
    ).toEqual([{ step: "a", path: "script", message: "boom", line: 2, column: 5 }]);
  });

  it("drops malformed entries and non-positive positions", () => {
    expect(
      parseDiagnostics({
        diagnostics: [
          { step: "a", path: "ops[0]", message: "x", line: 0 },
          { step: 1, path: "script", message: "bad step" },
          "nope",
        ],
      }),
    ).toEqual([{ step: "a", path: "ops[0]", message: "x", line: undefined, column: undefined }]);
  });

  it("returns nothing for bodies without diagnostics", () => {
    expect(parseDiagnostics(undefined)).toEqual([]);
    expect(parseDiagnostics({ error: "x", code: "validation" })).toEqual([]);
  });
});

describe("parseGeneratedScripts", () => {
  it("keeps string entries only", () => {
    expect(parseGeneratedScripts({ generated_scripts: { a: "x;", b: 3 } })).toEqual({ a: "x;" });
    expect(parseGeneratedScripts({ valid: true })).toEqual({});
  });
});

describe("diagnosticsForStep", () => {
  it("filters by step id", () => {
    const all = [
      { step: "a", path: "script", message: "1" },
      { step: "b", path: "script", message: "2" },
    ];
    expect(diagnosticsForStep(all, "b")).toEqual([all[1]]);
  });
});
