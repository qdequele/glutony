import type { StepDiagnostic } from "./types";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function position(value: unknown): number | undefined {
  return typeof value === "number" && Number.isInteger(value) && value > 0 ? value : undefined;
}

/**
 * Read `diagnostics` out of an `invalid_step_config` error body.
 *
 * Tolerant by design: the body crosses a network boundary, so a malformed entry is
 * dropped rather than failing the whole validation.
 */
export function parseDiagnostics(body: unknown): StepDiagnostic[] {
  if (!isRecord(body) || !Array.isArray(body.diagnostics)) return [];
  const out: StepDiagnostic[] = [];
  for (const entry of body.diagnostics) {
    if (!isRecord(entry)) continue;
    const { step, path, message } = entry;
    if (typeof step !== "string" || typeof path !== "string" || typeof message !== "string") {
      continue;
    }
    out.push({ step, path, message, line: position(entry.line), column: position(entry.column) });
  }
  return out;
}

/** Read `generated_scripts` out of a 200 validation body. */
export function parseGeneratedScripts(body: unknown): Record<string, string> {
  if (!isRecord(body) || !isRecord(body.generated_scripts)) return {};
  const out: Record<string, string> = {};
  for (const [step, script] of Object.entries(body.generated_scripts)) {
    if (typeof script === "string") out[step] = script;
  }
  return out;
}

/** The diagnostics that belong to one step. */
export function diagnosticsForStep(all: StepDiagnostic[], step: string): StepDiagnostic[] {
  return all.filter((diagnostic) => diagnostic.step === step);
}
