import { validatePipeline } from "@/lib/api/client";
import type { JsonObject, PipelineDefinition } from "@/lib/api/types";

export const SCRIPT_PLUGIN = "document_script";

/** A one-step pipeline holding only this step, so other steps' problems cannot block the switch. */
export function singleStepPipeline(stepId: string, config: JsonObject): PipelineDefinition {
  return {
    uid: "document-script-switch",
    steps: [{ id: stepId, plugin: SCRIPT_PLUGIN, config }],
  };
}

/** The rhai the backend generates for this step's `ops`, or `undefined` if it cannot. */
export async function generateScript(stepId: string, config: JsonObject): Promise<string | undefined> {
  try {
    const outcome = await validatePipeline(singleStepPipeline(stepId, config));
    return outcome.kind === "ok" ? outcome.generatedScripts[stepId] : undefined;
  } catch {
    return undefined;
  }
}
