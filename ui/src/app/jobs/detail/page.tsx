"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense, useState } from "react";
import { ArrowLeft, Ban, RefreshCw } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage } from "@/lib/api/hooks";
import { isCancellable, useJob } from "@/lib/api/jobs";
import { cn } from "@/lib/utils";
import { CancelJobDialog } from "../_components/cancel-job-dialog";
import { JobStatusBadge } from "../_components/job-status-badge";
import { StepTimeline } from "../_components/step-timeline";
import { JOBS_HREF } from "../routes";

function Summary({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="truncate font-mono text-sm">{value || "—"}</dd>
    </div>
  );
}

function JobDetailView() {
  const jobId = useSearchParams().get("id") ?? "";
  const { data, isPending, error, isFetching, refetch } = useJob(jobId || undefined);
  const [cancelOpen, setCancelOpen] = useState(false);

  if (!jobId) {
    return (
      <>
        <PageHeader title="Job" />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>No job selected</AlertTitle>
            <AlertDescription>
              This page needs an <span className="font-mono">?id=</span> parameter.
              <Button asChild variant="link" size="sm">
                <Link href={JOBS_HREF}>Back to the list</Link>
              </Button>
            </AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title={jobId}
        description="Temporal is the source of truth; the cached row fills in once it forgets the workflow."
        actions={
          <>
            <Button asChild variant="ghost" size="sm">
              <Link href={JOBS_HREF}>
                <ArrowLeft aria-hidden />
                All jobs
              </Link>
            </Button>
            <Button
              variant="outline"
              size="sm"
              onClick={() => void refetch()}
              disabled={isFetching}
            >
              <RefreshCw aria-hidden className={cn(isFetching && "animate-spin")} />
              Refresh
            </Button>
            {data && isCancellable(data.status) ? (
              <Button variant="destructive" size="sm" onClick={() => setCancelOpen(true)}>
                <Ban aria-hidden />
                Cancel
              </Button>
            ) : null}
          </>
        }
      />

      <div className="space-y-4 p-4">
        {isPending ? (
          <>
            <Skeleton className="h-16 w-full" />
            <StepTimeline progress={undefined} isPending />
          </>
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load this job</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : (
          <>
            <dl className="grid grid-cols-2 gap-4 rounded-md border p-3 sm:grid-cols-4">
              <div>
                <dt className="text-xs text-muted-foreground">Status</dt>
                <dd className="mt-0.5">
                  <JobStatusBadge status={data.status} />
                </dd>
              </div>
              <Summary label="Pipeline" value={data.pipeline_used} />
              <Summary label="Target index" value={data.target_index} />
              <Summary label="Current step" value={data.current_step} />
            </dl>

            {data.error ? (
              <Alert variant="destructive">
                <AlertTitle>Failure</AlertTitle>
                <AlertDescription className="font-mono text-xs break-words">
                  {data.error}
                </AlertDescription>
              </Alert>
            ) : null}

            <div>
              <h2 className="mb-2 text-sm font-semibold">Steps</h2>
              <StepTimeline progress={data.progress} />
            </div>
          </>
        )}
      </div>

      <CancelJobDialog jobId={jobId} open={cancelOpen} onOpenChange={setCancelOpen} />
    </>
  );
}

export default function JobDetailPage() {
  return (
    <Suspense
      fallback={
        <div className="space-y-3 p-4">
          <Skeleton className="h-16 w-full" />
          <Skeleton className="h-40 w-full" />
        </div>
      }
    >
      <JobDetailView />
    </Suspense>
  );
}
