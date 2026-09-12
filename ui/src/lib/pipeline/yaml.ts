/**
 * Pipeline ⇄ YAML.
 *
 * The YAML pane is the source of truth for what gets saved: the form writes
 * the draft, `draftToYaml` renders it, and `yamlToDraft` reads it back. Both
 * directions go through the same normalisation, so a round trip is lossless
 * for everything the editor can express.
 *
 * Parsing never throws: a malformed document returns `{ ok: false }` with the
 * message to show, and the caller leaves the form untouched.
 */
import { parse, stringify } from "yaml";

import { FAN_OUT_PATHS } from "@/lib/api/types";
import {
  normalizeDraft,
  pipelineWireSchema,
  type PipelineDraft,
} from "./draft";

/** Result of reading the YAML pane. */
export type YamlParseResult =
  | { ok: true; draft: PipelineDraft }
  | { ok: false; error: string };

/**
 * Render a draft as YAML, in the same key order the docs and the built-in
 * pipeline files use. Empty optionals are omitted rather than written as
 * `null`, so the document stays close to what a human would type.
 */
export function draftToYaml(draft: PipelineDraft): string {
  const trigger: Record<string, unknown> = {};
  if (draft.trigger.content_types.length > 0) {
    trigger.content_types = draft.trigger.content_types;
  }
  if (draft.trigger.filename_pattern.trim()) {
    trigger.filename_pattern = draft.trigger.filename_pattern.trim();
  }
  if (draft.trigger.index_pattern.trim()) {
    trigger.index_pattern = draft.trigger.index_pattern.trim();
  }

  const document: Record<string, unknown> = { uid: draft.uid };
  if (draft.name.trim()) document.name = draft.name.trim();
  if (draft.description.trim()) document.description = draft.description.trim();
  if (Object.keys(trigger).length > 0) document.trigger = trigger;

  document.steps = draft.steps.map((step) => {
    const node: Record<string, unknown> = { id: step.id, plugin: step.plugin };
    if (step.depends_on.length > 0) node.depends_on = step.depends_on;
    if (step.fan_out) node.fan_out = step.fan_out;
    if (Object.keys(step.config).length > 0) node.config = step.config;
    if (step.timeout_secs !== undefined) node.timeout_secs = step.timeout_secs;
    if (step.retry) node.retry = step.retry;
    return node;
  });

  return stringify(document, { lineWidth: 0, indent: 2 });
}

/** Read the YAML pane back into a draft. Also accepts JSON, which is valid YAML. */
export function yamlToDraft(text: string): YamlParseResult {
  if (text.trim().length === 0) {
    return { ok: false, error: "the document is empty" };
  }

  let parsed: unknown;
  try {
    parsed = parse(text);
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "invalid YAML" };
  }
  if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
    return { ok: false, error: "expected a YAML mapping at the top level" };
  }

  const result = pipelineWireSchema.safeParse(parsed);
  if (!result.success) {
    const issue = result.error.issues[0];
    const where = issue?.path.length ? `${issue.path.join(".")}: ` : "";
    return { ok: false, error: `${where}${issue?.message ?? "does not match a pipeline"}` };
  }

  // Reject an unsupported fan_out here rather than letting normalisation drop it.
  for (const step of result.data.steps ?? []) {
    if (step.fan_out && !(FAN_OUT_PATHS as readonly string[]).includes(step.fan_out)) {
      return {
        ok: false,
        error: `steps.${step.id}.fan_out: ${JSON.stringify(
          step.fan_out,
        )} is not supported (use ${FAN_OUT_PATHS.map((p) => JSON.stringify(p)).join(", ")})`,
      };
    }
  }

  return { ok: true, draft: normalizeDraft(result.data) };
}
