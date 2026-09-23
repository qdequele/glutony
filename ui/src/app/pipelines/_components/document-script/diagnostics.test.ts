import { describe, expect, it } from "vitest";

import { configDiagnostics, diagnosticsForOp, routeDiagnostic, scriptDiagnostics, stepCheckState } from "./diagnostics";

const d = (path: string, line?: number) => ({ step: "s", path, message: path, line });

describe("routeDiagnostic", () => {
  it("routes script, op and config paths", () => {
    expect(routeDiagnostic(d("script", 2))).toEqual({ kind: "script", line: 2, column: undefined });
    expect(routeDiagnostic(d("ops[3]"))).toEqual({ kind: "op", index: 3, key: undefined });
    expect(routeDiagnostic(d("ops[2].expr"))).toEqual({ kind: "op", index: 2, key: "expr" });
    expect(routeDiagnostic(d("ops[1][0]"))).toEqual({ kind: "op", index: 1, key: "[0]" });
    expect(routeDiagnostic(d("max_ops"))).toEqual({ kind: "config" });
  });
});

describe("selectors", () => {
  const all = [d("script", 1), d("ops[0].expr"), d("ops[1]"), d("config")];
  it("selects per target", () => {
    expect(scriptDiagnostics(all)).toEqual([all[0]]);
    expect(diagnosticsForOp(all, 1)).toEqual([all[2]]);
    expect(configDiagnostics(all)).toEqual([all[3]]);
  });
});

describe("stepCheckState", () => {
  it("is invalid only when this step has diagnostics", () => {
    expect(stepCheckState("invalid", [d("script")])).toBe("invalid");
    expect(stepCheckState("invalid", [])).toBe("ok");
    expect(stepCheckState("checking", [])).toBe("checking");
    expect(stepCheckState("idle", [])).toBe("idle");
  });
});
