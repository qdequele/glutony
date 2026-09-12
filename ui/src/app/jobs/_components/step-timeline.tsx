"use client";

/**
 * The step timeline of a running or finished job.
 *
 * Shared on purpose: the Jobs detail page and the Playground answer the same
 * question ("what did each step do, and where did it stop?") and must not drift
 * apart, so the Playground imports this component rather than re-rendering
 * `progress.steps` its own way.
 */
import { Split } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Progress } from "@/components/ui/progress";
import { Skeleton } from "@/components/ui/skeleton";
import { formatBytes, formatDuration, progressPercent } from "@/lib/api/jobs";
import type { StepResult, WorkflowProgress } from "@/lib/api/types";
import { cn } from "@/lib/utils";
import { JobStatusBadge } from "./job-status-badge";

/** Billable units worth surfacing next to a step, when the plugin recorded any. */
function usageSummary(step: StepResult): string | undefined {
  const usage = step.usage;
  if (!usage) return undefined;
  const parts: string[] = [];
  if (usage.llm_requests > 0) {
    parts.push(
      `${usage.llm_requests} LLM call${usage.llm_requests === 1 ? "" : "s"}` +
        ` (${usage.llm_input_tokens}→${usage.llm_output_tokens} tok)`,
    );
  }
  if (usage.audio_seconds > 0) parts.push(`${usage.audio_seconds.toFixed(1)}s audio`);
  if (usage.pages > 0) parts.push(`${usage.pages} page${usage.pages === 1 ? "" : "s"}`);
  if (usage.images > 0) parts.push(`${usage.images} image${usage.images === 1 ? "" : "s"}`);
  if (usage.external_requests > 0) parts.push(`${usage.external_requests} external calls`);
  return parts.length > 0 ? parts.join(" · ") : undefined;
}

function StepRow({ step, isCurrent }: { step: StepResult; isCurrent: boolean }) {
  const usage = usageSummary(step);
  const branches = step.branches ?? 1;
  return (
    <li
      className={cn(
        "relative rounded-md border p-3",
        isCurrent && "border-primary/40 bg-primary/[0.03]",
        step.status === "failed" && "border-destructive/40 bg-destructive/[0.04]",
      )}
    >
      <div className="flex flex-wrap items-center gap-2">
        <span className="font-mono text-sm font-medium">{step.step_id}</span>
        <Badge variant="secondary" className="font-mono text-[10px]">
          {step.plugin}
        </Badge>
        {branches > 1 ? (
          <Badge variant="outline" className="gap-1 text-[10px]">
            <Split aria-hidden />
            {branches} branches
          </Badge>
        ) : null}
        <JobStatusBadge status={step.status} className="ml-auto" />
      </div>

      <dl className="mt-2 grid grid-cols-2 gap-x-4 gap-y-1 text-xs text-muted-foreground sm:grid-cols-3">
        <div className="flex gap-1.5">
          <dt>Duration</dt>
          <dd className="tabular-nums text-foreground">{formatDuration(step.duration_ms)}</dd>
        </div>
        <div className="flex gap-1.5">
          <dt>Documents</dt>
          <dd className="tabular-nums text-foreground">{step.document_count ?? 0}</dd>
        </div>
        <div className="flex gap-1.5">
          <dt>Input</dt>
          <dd className="tabular-nums text-foreground">{formatBytes(step.input_bytes)}</dd>
        </div>
      </dl>

      {usage ? <p className="mt-1 text-xs text-muted-foreground">{usage}</p> : null}

      {step.error ? (
        <p className="mt-2 rounded border border-destructive/30 bg-destructive/5 p-2 font-mono text-xs break-words text-destructive">
          {step.error}
        </p>
      ) : null}
    </li>
  );
}

export interface StepTimelineProps {
  /** `progress` from `GET /jobs/{id}`. `null` when Temporal no longer knows it. */
  progress: WorkflowProgress | null | undefined;
  /** True while the first response is in flight. */
  isPending?: boolean;
}

export function StepTimeline({ progress, isPending = false }: StepTimelineProps) {
  if (isPending) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-1 w-full" />
        {Array.from({ length: 3 }, (_, index) => (
          <Skeleton key={index} className="h-20 w-full" />
        ))}
      </div>
    );
  }

  if (!progress) {
    return (
      <p className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
        No step detail available. Temporal has forgotten this workflow, so only the cached summary
        above is left.
      </p>
    );
  }

  const steps = progress.steps ?? [];
  const percent = progressPercent(progress);

  return (
    <div className="space-y-3">
      <div className="space-y-1.5">
        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>
            {progress.completed_steps} / {progress.total_steps} step
            {progress.total_steps === 1 ? "" : "s"} completed
            {progress.current_step ? (
              <>
                {" · running "}
                <span className="font-mono text-foreground">{progress.current_step}</span>
              </>
            ) : null}
          </span>
          <span className="tabular-nums">{percent}%</span>
        </div>
        <Progress value={percent} />
      </div>

      {progress.cancel_requested ? (
        <Alert>
          <AlertTitle>Cancellation requested</AlertTitle>
          <AlertDescription>
            The workflow stops once the step in flight returns.
          </AlertDescription>
        </Alert>
      ) : null}

      {progress.error ? (
        <Alert variant="destructive">
          <AlertTitle>The job failed</AlertTitle>
          <AlertDescription className="font-mono text-xs break-words">
            {progress.error}
          </AlertDescription>
        </Alert>
      ) : null}

      {steps.length === 0 ? (
        <p className="rounded-md border border-dashed p-6 text-center text-sm text-muted-foreground">
          No step has reported yet.
        </p>
      ) : (
        <ol className="space-y-2">
          {steps.map((step, index) => (
            <StepRow
              key={`${step.step_id}-${index}`}
              step={step}
              isCurrent={step.step_id === progress.current_step}
            />
          ))}
        </ol>
      )}
    </div>
  );
}
