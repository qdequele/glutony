import { describe, expect, it } from "vitest";

import { diagnosticRange } from "./lint";

const TEXT = "let a = 1;\ndoc.fields.x = ;\nend";

describe("diagnosticRange", () => {
  it("points at the column on the given line", () => {
    // line 2 starts at offset 11; column 16 is the ';'
    expect(diagnosticRange(TEXT, 2, 16)).toEqual({ from: 26, to: 27 });
  });
  it("covers the whole line without a column", () => {
    expect(diagnosticRange(TEXT, 3, undefined)).toEqual({ from: 28, to: 31 });
  });
  it("clamps positions past the text", () => {
    expect(diagnosticRange(TEXT, 99, 99)).toEqual({ from: 31, to: 31 });
  });
  it("falls back to the first line without a line", () => {
    expect(diagnosticRange(TEXT, undefined, undefined)).toEqual({ from: 0, to: 10 });
  });
});
