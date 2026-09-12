"use client";

import Link from "next/link";
import { ArrowRight, Ban, RotateCcw } from "lucide-react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { errorMessage } from "@/lib/api/hooks";
import { isCancellable, useJob } from "@/lib/api/jobs";
import type { IngestAccepted } from "@/lib/api/ingest";
import { JobStatusBadge } from "@/app/jobs/_components/job-status-badge";
import { StepTimeline } from "@/app/jobs/_components/step-timeline";
import { jobDetailHref } from "@/app/jobs/routes";

function Fact({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="truncate font-mono text-sm">{value}</dd>
    </div>
  );
}

export interface SubmissionResultProps {
  accepted: IngestAccepted;
  onCancelJob: (jobId: string) => void;
  onReset: () => void;
}

/**
 * What the gateway decided, then the job itself.
 *
 * The two facts at the top — which pipeline auto-routing picked and which index
 * it resolved — are the whole reason this screen exists; the timeline below is
 * the same component the Jobs detail page renders, polled by the same hook.
 */
export function SubmissionResult({ accepted, onCancelJob, onReset }: SubmissionResultProps) {
  const { data, isPending, error } = useJob(accepted.job_id);
  const status = data?.status;

  return (
    <div className="space-y-4">
      <dl className="grid grid-cols-2 gap-4 rounded-md border p-3 sm:grid-cols-4">
        <Fact label="Pipeline used" value={accepted.pipeline_used} />
        <Fact label="Target index" value={accepted.target_index} />
        <Fact label="Job" value={accepted.job_id.slice(0, 8)} />
        <div>
          <dt className="text-xs text-muted-foreground">Status</dt>
          <dd className="mt-0.5">
            {status ? (
              <JobStatusBadge status={status} />
            ) : (
              <span className="text-sm text-muted-foreground">…</span>
            )}
          </dd>
        </div>
      </dl>

      <div className="flex flex-wrap items-center gap-2">
        <Button asChild variant="outline" size="sm">
          <Link href={jobDetailHref(accepted.job_id)}>
            Open in Jobs
            <ArrowRight aria-hidden />
          </Link>
        </Button>
        {status && isCancellable(status) ? (
          <Button variant="destructive" size="sm" onClick={() => onCancelJob(accepted.job_id)}>
            <Ban aria-hidden />
            Cancel
          </Button>
        ) : null}
        <Button variant="ghost" size="sm" onClick={onReset}>
          <RotateCcw aria-hidden />
          Send something else
        </Button>
      </div>

      {error ? (
        <Alert variant="destructive">
          <AlertTitle>Could not follow the job</AlertTitle>
          <AlertDescription>{errorMessage(error)}</AlertDescription>
        </Alert>
      ) : null}

      {status === "succeeded" ? (
        <Alert>
          <AlertTitle>Indexed</AlertTitle>
          <AlertDescription>
            Every step finished. The documents are in{" "}
            <span className="font-mono">{data?.target_index ?? accepted.target_index}</span>.
          </AlertDescription>
        </Alert>
      ) : null}

      {status === "failed" ? (
        <Alert variant="destructive">
          <AlertTitle>
            Failed
            {(() => {
              const failed = data?.progress?.steps?.find((step) => step.status === "failed");
              return failed ? ` at "${failed.step_id}" (${failed.plugin})` : "";
            })()}
          </AlertTitle>
          <AlertDescription className="font-mono text-xs break-words">
            {data?.progress?.steps?.find((step) => step.status === "failed")?.error ??
              data?.error ??
              data?.progress?.error ??
              "No message reported."}
          </AlertDescription>
        </Alert>
      ) : null}

      <div>
        <h2 className="mb-2 text-sm font-semibold">Steps</h2>
        <StepTimeline progress={data?.progress} isPending={isPending} />
      </div>
    </div>
  );
}
