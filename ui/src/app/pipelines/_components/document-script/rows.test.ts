import { describe, expect, it } from "vitest";

import { addOp, moveOp, readOps, removeOp, replaceOp, writeOps } from "./rows";

const A = { drop: ["a"] };
const B = { keep: ["b"] };

describe("rows", () => {
  it("reads ops, tolerating a missing or malformed key", () => {
    expect(readOps({ ops: [A] })).toEqual([A]);
    expect(readOps({})).toEqual([]);
    expect(readOps({ ops: "nope" })).toEqual([]);
  });
  it("writes ops without touching other keys", () => {
    expect(writeOps({ on_error: "skip", ops: [] }, [A])).toEqual({ on_error: "skip", ops: [A] });
  });
  it("adds, removes, replaces and moves", () => {
    expect(addOp([A], B)).toEqual([A, B]);
    expect(removeOp([A, B], 0)).toEqual([B]);
    expect(replaceOp([A, B], 1, A)).toEqual([A, A]);
    expect(moveOp([A, B], 0, 1)).toEqual([B, A]);
    expect(moveOp([A, B], 0, -1)).toEqual([A, B]);
    expect(moveOp([A, B], 1, 1)).toEqual([A, B]);
  });
});
