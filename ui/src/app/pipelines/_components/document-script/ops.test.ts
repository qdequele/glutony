import { describe, expect, it } from "vitest";

import { formatLiteral, newOp, opKind, OP_KINDS, parseDecimals, parseLiteral } from "./ops";

describe("opKind", () => {
  it("reads the single key of a known operation", () => {
    expect(opKind({ rename: { from: "a", to: "b" } })).toBe("rename");
  });
  it("rejects anything else", () => {
    expect(opKind({ rename: {}, drop: [] })).toBeUndefined();
    expect(opKind({ explode: {} })).toBeUndefined();
    expect(opKind(["drop"])).toBeUndefined();
    expect(opKind(null)).toBeUndefined();
  });
});

describe("newOp", () => {
  it("builds a blank operation of every kind", () => {
    for (const { kind } of OP_KINDS) expect(opKind(newOp(kind))).toBe(kind);
  });
});

describe("literals", () => {
  it("parses JSON when it can and falls back to a string", () => {
    expect(parseLiteral("12")).toBe(12);
    expect(parseLiteral("true")).toBe(true);
    expect(parseLiteral("null")).toBeNull();
    expect(parseLiteral('"12"')).toBe("12");
    expect(parseLiteral("draft")).toBe("draft");
  });
  it("formats strings bare unless they would parse as something else", () => {
    expect(formatLiteral("draft")).toBe("draft");
    expect(formatLiteral("12")).toBe('"12"');
    expect(formatLiteral(12)).toBe("12");
    expect(formatLiteral(undefined)).toBe("");
  });
});

describe("parseDecimals", () => {
  it("allows clearing the input without storing anything", () => {
    expect(parseDecimals("")).toEqual({ text: "", value: undefined });
    expect(parseDecimals("  ")).toEqual({ text: "", value: undefined });
  });
  it("stores whole numbers from 0 to 15", () => {
    expect(parseDecimals("0")).toEqual({ text: "0", value: 0 });
    expect(parseDecimals("15")).toEqual({ text: "15", value: 15 });
    expect(parseDecimals("07")).toEqual({ text: "07", value: 7 });
  });
  it("rejects anything else", () => {
    expect(parseDecimals("16")).toBeUndefined();
    expect(parseDecimals("100")).toBeUndefined();
    expect(parseDecimals("-1")).toBeUndefined();
    expect(parseDecimals("1.5")).toBeUndefined();
    expect(parseDecimals("x")).toBeUndefined();
  });
});
