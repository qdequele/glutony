/**
 * Client-side pipeline validation.
 *
 * These run on every keystroke and are reported inline on the offending step.
 * They mirror `PipelineDefinition::validate` in the Rust SDK, plus one rule the
 * SDK cannot express on its own: **type compatibility** between a step's plugin
 * and what the step before it produces. That is the bug class that broke
 * `builtin.text` — a pipeline that sends raw `bytes` into a plugin declaring
 * only `documents` is structurally perfect and fails at runtime.
 *
 * `POST /pipelines/validate` remains the authority; this is the fast feedback
 * loop, and it works when that endpoint is not deployed.
 */
import type { InputKind, OutputKind, PluginManifest } from "@/lib/api/types";
import { FAN_OUT_PATHS } from "@/lib/api/types";
import { UID_PATTERN, type PipelineDraft, type StepDraft } from "./draft";

/** Which part of a step card an issue belongs to. */
export type IssueField = "uid" | "steps" | "id" | "plugin" | "depends_on" | "fan_out";

/** One problem found in the draft. */
export interface ValidationIssue {
  /** Step the issue belongs to; absent for pipeline-level issues. */
  stepId?: string;
  /** Control to highlight. */
  field: IssueField;
  /** Machine-readable rule name, handy in tests. */
  rule:
    | "invalid_uid"
    | "empty_pipeline"
    | "duplicate_step"
    | "invalid_step_id"
    | "unknown_dependency"
    | "forward_dependency"
    | "cycle"
    | "fan_out_arity"
    | "fan_out_path"
    | "fan_out_source"
    | "unknown_plugin"
    | "type_mismatch";
  message: string;
}

/**
 * What a step receives, given what the step before it produced.
 *
 * Mirrors `impl From<PluginOutput> for PluginInput`: a terminal `indexed` and
 * an `empty` output both arrive as `empty`.
 */
export function outputToInput(produces: OutputKind): InputKind {
  switch (produces) {
    case "bytes":
      return "bytes";
    case "ref":
      return "ref";
    case "documents":
      return "documents";
    case "many":
      return "many";
    case "indexed":
    case "empty":
      return "empty";
  }
}

/**
 * Whether a plugin can be fed `required`.
 *
 * A `ref` is resolved into a `Blob` by the activity runner before the plugin
 * sees it, so a plugin that accepts `bytes` also accepts a `ref` upstream.
 */
export function acceptsInput(manifest: PluginManifest, required: InputKind): boolean {
  const accepts = manifest.accepts ?? [];
  if (accepts.includes(required)) return true;
  if (required === "ref" && accepts.includes("bytes")) return true;
  return false;
}

function findCycle(steps: StepDraft[]): string[] | undefined {
  const ids = new Set(steps.map((step) => step.id));
  const edges = new Map<string, string[]>();
  for (const step of steps) {
    edges.set(
      step.id,
      step.depends_on.filter((dep) => ids.has(dep)),
    );
  }

  const state = new Map<string, "visiting" | "done">();
  const stack: string[] = [];

  function visit(id: string): string[] | undefined {
    const seen = state.get(id);
    if (seen === "done") return undefined;
    if (seen === "visiting") return stack.slice(stack.indexOf(id));
    state.set(id, "visiting");
    stack.push(id);
    for (const dep of edges.get(id) ?? []) {
      const cycle = visit(dep);
      if (cycle) return cycle;
    }
    stack.pop();
    state.set(id, "done");
    return undefined;
  }

  for (const step of steps) {
    const cycle = visit(step.id);
    if (cycle) return cycle;
  }
  return undefined;
}

/**
 * Run every client-side rule over a draft.
 *
 * `plugins` is optional: while the manifests are still loading, pass
 * `undefined` and the plugin-dependent rules (unknown plugin, type
 * compatibility) are skipped rather than reported as failures.
 */
export function validateDraft(
  draft: PipelineDraft,
  plugins?: PluginManifest[],
): ValidationIssue[] {
  const issues: ValidationIssue[] = [];

  if (draft.uid.trim().length === 0) {
    issues.push({ field: "uid", rule: "invalid_uid", message: "uid is required" });
  } else if (!UID_PATTERN.test(draft.uid)) {
    issues.push({
      field: "uid",
      rule: "invalid_uid",
      message: "uid may only contain letters, digits, '.', '_' and '-'",
    });
  }

  if (draft.steps.length === 0) {
    issues.push({
      field: "steps",
      rule: "empty_pipeline",
      message: "a pipeline needs at least one step",
    });
    return issues;
  }

  // --- step ids ----------------------------------------------------------
  const seen = new Map<string, number>();
  draft.steps.forEach((step, index) => {
    if (step.id.trim().length === 0) {
      issues.push({
        stepId: step.id,
        field: "id",
        rule: "invalid_step_id",
        message: `step ${index + 1} has no id`,
      });
      return;
    }
    const first = seen.get(step.id);
    if (first !== undefined) {
      issues.push({
        stepId: step.id,
        field: "id",
        rule: "duplicate_step",
        message: `duplicate step id "${step.id}" (already used by step ${first + 1})`,
      });
    } else {
      seen.set(step.id, index);
    }
  });

  const positionOf = new Map<string, number>();
  draft.steps.forEach((step, index) => {
    if (!positionOf.has(step.id)) positionOf.set(step.id, index);
  });

  const byName = new Map((plugins ?? []).map((plugin) => [plugin.name, plugin]));

  draft.steps.forEach((step, index) => {
    // --- dependencies ----------------------------------------------------
    for (const dep of step.depends_on) {
      const at = positionOf.get(dep);
      if (at === undefined) {
        issues.push({
          stepId: step.id,
          field: "depends_on",
          rule: "unknown_dependency",
          message: `depends on unknown step "${dep}"`,
        });
      } else if (at >= index) {
        issues.push({
          stepId: step.id,
          field: "depends_on",
          rule: "forward_dependency",
          message:
            at === index
              ? `step "${step.id}" depends on itself`
              : `depends on "${dep}", which runs later — move it earlier in the list`,
        });
      }
    }

    // --- fan-out ---------------------------------------------------------
    if (step.fan_out) {
      if (!(FAN_OUT_PATHS as readonly string[]).includes(step.fan_out)) {
        issues.push({
          stepId: step.id,
          field: "fan_out",
          rule: "fan_out_path",
          message: `fan_out ${JSON.stringify(step.fan_out)} is not supported`,
        });
      }
      if (step.depends_on.length !== 1) {
        issues.push({
          stepId: step.id,
          field: "fan_out",
          rule: "fan_out_arity",
          message: `a fan-out step must depend on exactly one step (it depends on ${step.depends_on.length})`,
        });
      }
    }

    // --- plugin ----------------------------------------------------------
    if (plugins === undefined) return;
    const manifest = byName.get(step.plugin);
    if (!manifest) {
      issues.push({
        stepId: step.id,
        field: "plugin",
        rule: "unknown_plugin",
        message: `unknown plugin "${step.plugin}"`,
      });
      return;
    }

    // --- type compatibility ----------------------------------------------
    // Root steps read the ingest payload, whose kind depends on the request,
    // so there is nothing to compare them against.
    if (step.depends_on.length === 0) return;

    const upstream = step.depends_on
      .map((dep) => draft.steps.find((candidate) => candidate.id === dep))
      .filter((candidate): candidate is StepDraft => candidate !== undefined)
      .map((candidate) => byName.get(candidate.plugin))
      .filter((candidate): candidate is PluginManifest => candidate !== undefined);

    if (upstream.length !== step.depends_on.length) return; // an upstream plugin is unknown

    let required: InputKind;
    if (step.depends_on.length > 1) {
      // Several dependencies are merged into one `Many`.
      required = "many";
    } else if (step.fan_out === "$.documents") {
      const produces = upstream[0].produces;
      if (produces !== "documents" && produces !== "many") {
        issues.push({
          stepId: step.id,
          field: "fan_out",
          rule: "fan_out_source",
          message: `fan_out "$.documents" needs an upstream step producing documents, but "${step.depends_on[0]}" produces ${produces}`,
        });
        return;
      }
      required = "documents";
    } else if (step.fan_out === "$.many") {
      if (upstream[0].produces !== "many") {
        issues.push({
          stepId: step.id,
          field: "fan_out",
          rule: "fan_out_source",
          message: `fan_out "$.many" needs an upstream step producing many, but "${step.depends_on[0]}" produces ${upstream[0].produces}`,
        });
      }
      return; // the per-branch kind is only known at runtime
    } else {
      required = outputToInput(upstream[0].produces);
    }

    if (!acceptsInput(manifest, required)) {
      const accepts = manifest.accepts ?? [];
      issues.push({
        stepId: step.id,
        field: "plugin",
        rule: "type_mismatch",
        message: `"${step.depends_on.join('", "')}" hands this step ${required}, but ${
          manifest.name
        } accepts ${accepts.length > 0 ? accepts.join(", ") : "nothing"}`,
      });
    }
  });

  // --- cycles --------------------------------------------------------------
  const cycle = findCycle(draft.steps);
  if (cycle && cycle.length > 0) {
    for (const id of cycle) {
      issues.push({
        stepId: id,
        field: "depends_on",
        rule: "cycle",
        message: `cycle: ${[...cycle, cycle[0]].join(" → ")}`,
      });
    }
  }

  return issues;
}

/** Group issues by step id; pipeline-level issues land under `""`. */
export function issuesByStep(issues: ValidationIssue[]): Map<string, ValidationIssue[]> {
  const grouped = new Map<string, ValidationIssue[]>();
  for (const issue of issues) {
    const key = issue.stepId ?? "";
    const bucket = grouped.get(key);
    if (bucket) bucket.push(issue);
    else grouped.set(key, [issue]);
  }
  return grouped;
}
