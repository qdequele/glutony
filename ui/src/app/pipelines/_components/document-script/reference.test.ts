import { describe, expect, it } from "vitest";

import { opKind, OP_KINDS } from "./ops";
import { filterEntries, REFERENCE } from "./reference";

describe("reference", () => {
  it("has unique ids and complete copy", () => {
    const ids = REFERENCE.entries.map((entry) => entry.id);
    expect(new Set(ids).size).toBe(ids.length);
    for (const entry of REFERENCE.entries) {
      expect(entry.title.length).toBeGreaterThan(0);
      expect(entry.summary.length).toBeGreaterThan(0);
      expect(entry.snippet.length).toBeGreaterThan(0);
    }
  });

  it("covers every builder operation, in its own group", () => {
    for (const info of OP_KINDS) {
      const matches = REFERENCE.entries.filter((entry) => entry.op !== null && opKind(entry.op) === info.kind);
      expect(matches.length, info.kind).toBeGreaterThanOrEqual(1);
      for (const entry of matches) expect(entry.group).toBe(info.group);
    }
  });

  it("filters by title, summary or snippet, case-insensitively", () => {
    expect(filterEntries(REFERENCE.entries, "RENAME").map((e) => e.id)).toContain("rename");
    expect(filterEntries(REFERENCE.entries, "facets").map((e) => e.id)).toEqual(["split"]);
    expect(filterEntries(REFERENCE.entries, "")).toHaveLength(REFERENCE.entries.length);
  });
});
