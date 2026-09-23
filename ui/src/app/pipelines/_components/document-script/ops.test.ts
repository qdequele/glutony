import { describe, expect, it } from "vitest";

import { formatLiteral, newOp, opKind, OP_KINDS, parseLiteral } from "./ops";

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
