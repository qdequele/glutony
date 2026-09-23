import type { StepDiagnostic } from "@/lib/api/types";

export type DiagnosticTarget =
  | { kind: "script"; line?: number; column?: number }
  | { kind: "op"; index: number; key?: string }
  | { kind: "config" };

const OP_PATH = /^ops\[(\d+)\](.*)$/;

/** Where a diagnostic belongs in the editor. */
export function routeDiagnostic(
  diagnostic: Pick<StepDiagnostic, "path" | "line" | "column">,
): DiagnosticTarget {
  if (diagnostic.path === "script") {
    return { kind: "script", line: diagnostic.line, column: diagnostic.column };
  }
  const match = OP_PATH.exec(diagnostic.path);
  if (match) {
    const rest = match[2].replace(/^\./, "");
    return { kind: "op", index: Number(match[1]), key: rest.length > 0 ? rest : undefined };
  }
  return { kind: "config" };
}

export function diagnosticsForOp(all: StepDiagnostic[], index: number): StepDiagnostic[] {
  return all.filter((d) => {
    const target = routeDiagnostic(d);
    return target.kind === "op" && target.index === index;
  });
}

export function scriptDiagnostics(all: StepDiagnostic[]): StepDiagnostic[] {
  return all.filter((d) => routeDiagnostic(d).kind === "script");
}

export function configDiagnostics(all: StepDiagnostic[]): StepDiagnostic[] {
  return all.filter((d) => routeDiagnostic(d).kind === "config");
}

/** The pipeline-wide server check, as the editor sees it. */
export type ServerState = "idle" | "checking" | "ok" | "invalid";

/** A step is invalid only if *it* has diagnostics; another step's problem leaves it ok. */
export function stepCheckState(state: ServerState, stepDiagnostics: StepDiagnostic[]): ServerState {
  if (state === "invalid") return stepDiagnostics.length > 0 ? "invalid" : "ok";
  return state;
}
