"use client";

import { zodResolver } from "@hookform/resolvers/zod";
import { AlertTriangle, CheckCircle2, Copy, Loader2, Lock, Plus } from "lucide-react";
import Link from "next/link";
import { useRouter } from "next/navigation";
import { useCallback, useMemo } from "react";
import { useFieldArray, useForm } from "react-hook-form";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { useUpsertPipeline, usePlugins, useValidatePipeline } from "@/lib/api/hooks";
import type { PipelineDefinition, PluginManifest } from "@/lib/api/types";
import {
  draftToPipeline,
  pipelineDraftSchema,
  type PipelineDraft,
  type StepDraft,
} from "@/lib/pipeline/draft";
import { issuesByStep, outputToInput, validateDraft } from "@/lib/pipeline/validate";
import { useDebouncedValue } from "@/lib/use-debounced-value";
import { PIPELINES_HREF, newPipelineHref } from "../routes";
import { MetadataPanel } from "./metadata-panel";
import { StepCard } from "./step-card";
import { YamlPane } from "./yaml-pane";

/** Pick a plugin that can actually read what the previous step hands over. */
function suggestPlugin(
  previous: StepDraft | undefined,
  plugins: PluginManifest[],
): string {
  if (plugins.length === 0) return "";
  if (!previous) return plugins[0].name;
  const upstream = plugins.find((plugin) => plugin.name === previous.plugin);
  if (!upstream) return plugins[0].name;
  const required = outputToInput(upstream.produces);
  const match = plugins.find((plugin) => (plugin.accepts ?? []).includes(required));
  return (match ?? plugins[0]).name;
}

function nextStepId(steps: StepDraft[]): string {
  const taken = new Set(steps.map((step) => step.id));
  for (let index = steps.length + 1; ; index += 1) {
    const candidate = `step_${index}`;
    if (!taken.has(candidate)) return candidate;
  }
}

export interface PipelineEditorProps {
  /** `create` posts a new uid; `edit` freezes the uid of an existing pipeline. */
  mode: "create" | "edit";
  initialDraft: PipelineDraft;
  /** Server-owned metadata of the pipeline being edited. */
  stored?: Pick<PipelineDefinition, "version" | "builtin" | "project_id">;
}

export function PipelineEditor({ mode, initialDraft, stored }: PipelineEditorProps) {
  const router = useRouter();
  const plugins = usePlugins();
  const save = useUpsertPipeline();

  const form = useForm<PipelineDraft>({
    defaultValues: initialDraft,
    resolver: zodResolver(pipelineDraftSchema),
    mode: "onChange",
  });
  const steps = useFieldArray({ control: form.control, name: "steps" });
  const draft = form.watch();

  const readOnly = stored?.builtin === true;

  // --- validation ---------------------------------------------------------
  const issues = useMemo(
    () => validateDraft(draft, plugins.data),
    [draft, plugins.data],
  );
  const grouped = useMemo(() => issuesByStep(issues), [issues]);
  const pipelineIssues = grouped.get("") ?? [];

  // The authoritative check runs only once the draft is locally coherent, and
  // only after typing stops.
  const debounced = useDebouncedValue(draft, 600);
  const candidate = useMemo(() => {
    const localIssues = validateDraft(debounced, plugins.data);
    if (localIssues.length > 0) return undefined;
    return draftToPipeline(debounced);
  }, [debounced, plugins.data]);
  const serverCheck = useValidatePipeline(readOnly ? undefined : candidate);

  // --- edits --------------------------------------------------------------
  const patchDraft = useCallback(
    (patch: Partial<PipelineDraft>) => {
      for (const [key, value] of Object.entries(patch)) {
        form.setValue(key as keyof PipelineDraft, value as never, {
          shouldDirty: true,
          shouldValidate: true,
        });
      }
    },
    [form],
  );

  const patchStep = useCallback(
    (index: number, patch: Partial<StepDraft>) => {
      const current = form.getValues(`steps.${index}`);
      form.setValue(`steps.${index}`, { ...current, ...patch }, {
        shouldDirty: true,
        shouldValidate: true,
      });
    },
    [form],
  );

  const applyYaml = useCallback(
    (next: PipelineDraft) => {
      // `reset` is what keeps the step field array in sync with a wholesale
      // rewrite; a per-field `setValue` loop would not.
      form.reset(mode === "edit" ? { ...next, uid: initialDraft.uid } : next, {
        keepDefaultValues: true,
      });
    },
    [form, mode, initialDraft.uid],
  );

  function addStep() {
    const current = form.getValues("steps");
    steps.append({
      id: nextStepId(current),
      plugin: suggestPlugin(current.at(-1), plugins.data ?? []),
      depends_on: current.length > 0 ? [current[current.length - 1].id] : [],
      config: {},
    });
  }

  const blocking = issues.length > 0;

  async function onSubmit(values: PipelineDraft) {
    if (readOnly || blocking) return;
    try {
      const saved = await save.mutateAsync(draftToPipeline(values));
      router.push(PIPELINES_HREF);
      void saved;
    } catch {
      // Reported by the mutation's own toast.
    }
  }

  return (
    <form onSubmit={form.handleSubmit(onSubmit)} className="flex h-full min-h-0 flex-col">
      <PageHeader
        title={mode === "create" ? "New pipeline" : draft.uid || "Pipeline"}
        description={
          readOnly
            ? "Built-in pipeline — read-only. Clone it to make changes."
            : mode === "create"
              ? "Author a DAG of plugin steps, in the form or in YAML."
              : `version ${stored?.version ?? 1}${
                  stored?.project_id ? ` · project ${stored.project_id}` : ""
                }`
        }
        actions={
          <>
            {readOnly ? (
              <Badge variant="secondary" className="gap-1">
                <Lock aria-hidden /> built-in
              </Badge>
            ) : null}
            <Button asChild variant="ghost" size="sm">
              <Link href={PIPELINES_HREF}>Cancel</Link>
            </Button>
            {mode === "edit" ? (
              <Button asChild variant="outline" size="sm">
                <Link href={newPipelineHref(initialDraft.uid)}>
                  <Copy aria-hidden />
                  Clone
                </Link>
              </Button>
            ) : null}
            <Button
              type="submit"
              size="sm"
              disabled={readOnly || blocking || save.isPending}
              title={
                readOnly
                  ? "Built-in pipelines cannot be saved"
                  : blocking
                    ? "Fix the reported problems first"
                    : undefined
              }
            >
              {save.isPending ? <Loader2 className="animate-spin" aria-hidden /> : null}
              Save
            </Button>
          </>
        }
      />

      <div className="grid min-h-0 flex-1 grid-cols-1 lg:grid-cols-[minmax(0,1fr)_minmax(0,26rem)]">
        {/* Left: metadata and steps */}
        <div className="min-h-0 overflow-auto p-4">
          {readOnly ? (
            <Alert className="mb-4">
              <Lock aria-hidden />
              <AlertTitle>Read-only</AlertTitle>
              <AlertDescription>
                Built-in pipelines are compiled into the control plane. Clone this one to change it.
              </AlertDescription>
            </Alert>
          ) : null}

          <MetadataPanel
            draft={draft}
            onPatch={patchDraft}
            uidLocked={mode === "edit"}
            readOnly={readOnly}
            uidError={
              pipelineIssues.find((issue) => issue.field === "uid")?.message ??
              form.formState.errors.uid?.message
            }
          />

          <Separator className="my-4" />

          <div className="mb-2 flex items-center justify-between">
            <h2 className="text-sm font-medium">
              Steps
              <span className="ml-2 text-xs font-normal text-muted-foreground">
                run in listed order unless <span className="font-mono">depends_on</span> says
                otherwise
              </span>
            </h2>
            <Button type="button" variant="outline" size="sm" disabled={readOnly} onClick={addStep}>
              <Plus aria-hidden />
              Add step
            </Button>
          </div>

          {steps.fields.length === 0 ? (
            <p className="rounded-md border border-dashed py-10 text-center text-sm text-muted-foreground">
              No steps yet.
            </p>
          ) : (
            <div className="space-y-3">
              {steps.fields.map((field, index) => {
                const step = draft.steps[index];
                if (!step) return null;
                return (
                  <StepCard
                    key={field.id}
                    index={index}
                    total={steps.fields.length}
                    step={step}
                    earlierStepIds={draft.steps.slice(0, index).map((entry) => entry.id)}
                    plugins={plugins.data ?? []}
                    issues={grouped.get(step.id) ?? []}
                    readOnly={readOnly}
                    onPatch={(patch) => patchStep(index, patch)}
                    onRemove={() => steps.remove(index)}
                    onMove={(direction) => steps.move(index, index + direction)}
                  />
                );
              })}
            </div>
          )}

          {pipelineIssues.length > 0 ? (
            <Alert variant="destructive" className="mt-4">
              <AlertTriangle aria-hidden />
              <AlertTitle>Pipeline</AlertTitle>
              <AlertDescription>
                <ul className="list-disc pl-4">
                  {pipelineIssues.map((issue, index) => (
                    <li key={index}>{issue.message}</li>
                  ))}
                </ul>
              </AlertDescription>
            </Alert>
          ) : null}
        </div>

        {/* Right: YAML */}
        <div className="min-h-0 border-t lg:border-t-0 lg:border-l">
          <YamlPane draft={draft} onDraftChange={applyYaml} readOnly={readOnly} />
        </div>
      </div>

      <ValidationBar
        issueCount={issues.length}
        outcome={serverCheck.data}
        checking={serverCheck.isFetching}
        skipped={candidate === undefined}
      />
    </form>
  );
}

function ValidationBar({
  issueCount,
  outcome,
  checking,
  skipped,
}: {
  issueCount: number;
  outcome: ReturnType<typeof useValidatePipeline>["data"];
  checking: boolean;
  skipped: boolean;
}) {
  return (
    <div className="flex h-9 shrink-0 items-center gap-3 border-t px-3 text-xs">
      {issueCount > 0 ? (
        <span className="flex items-center gap-1.5 text-destructive">
          <AlertTriangle className="size-3.5" aria-hidden />
          {issueCount} problem{issueCount === 1 ? "" : "s"}
        </span>
      ) : (
        <span className="flex items-center gap-1.5 text-muted-foreground">
          <CheckCircle2 className="size-3.5" aria-hidden />
          No local problems
        </span>
      )}

      <span className="text-muted-foreground/50">·</span>

      {skipped ? (
        <span className="text-muted-foreground">Server check waits for a valid draft</span>
      ) : checking ? (
        <span className="flex items-center gap-1.5 text-muted-foreground">
          <Loader2 className="size-3.5 animate-spin" aria-hidden />
          Checking with the gateway…
        </span>
      ) : outcome?.kind === "ok" ? (
        <span className="text-muted-foreground">
          Gateway accepted it · order{" "}
          <span className="font-mono">{outcome.order.join(" → ")}</span>
        </span>
      ) : outcome?.kind === "invalid" ? (
        <span className="text-destructive">
          Gateway rejected it: {outcome.error} ({outcome.code})
        </span>
      ) : outcome?.kind === "unsupported" ? (
        <span className="text-muted-foreground">
          Gateway has no /pipelines/validate — local checks only
        </span>
      ) : (
        <span className="text-muted-foreground">Not checked yet</span>
      )}
    </div>
  );
}
