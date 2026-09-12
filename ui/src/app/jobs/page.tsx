"use client";

import { useState } from "react";
import { ChevronLeft, ChevronRight, RefreshCw } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage } from "@/lib/api/hooks";
import {
  DEFAULT_JOB_FILTERS,
  JOB_PAGE_SIZES,
  hasNextPage,
  jobListPollInterval,
  useJobs,
  type JobFilters,
} from "@/lib/api/jobs";
import { cn } from "@/lib/utils";
import { CancelJobDialog } from "./_components/cancel-job-dialog";
import { JobFiltersBar } from "./_components/job-filters";
import { JobsTable } from "./_components/jobs-table";

export default function JobsPage() {
  const [filters, setFilters] = useState<JobFilters>(DEFAULT_JOB_FILTERS);
  const [toCancel, setToCancel] = useState<string | undefined>(undefined);
  const { data, isPending, error, isFetching, refetch } = useJobs(filters);

  /** Any filter change goes back to the first page — offset 25 of a new filter is meaningless. */
  function patch(next: Partial<JobFilters>) {
    setFilters((current) => ({ ...current, ...next, offset: 0 }));
  }

  const jobs = data?.jobs ?? [];
  const live = jobListPollInterval(data) !== false;
  const page = Math.floor(filters.offset / filters.limit) + 1;
  const totalPages = data?.total ? Math.max(1, Math.ceil(data.total / filters.limit)) : undefined;
  const hasNext = hasNextPage(data);

  return (
    <>
      <PageHeader
        title="Jobs"
        description={
          live
            ? "Refreshing every few seconds while work is in flight."
            : "Everything on this page has settled; polling is off."
        }
        actions={
          <Button variant="outline" size="sm" onClick={() => void refetch()} disabled={isFetching}>
            <RefreshCw aria-hidden className={cn(isFetching && "animate-spin")} />
            Refresh
          </Button>
        }
      />

      <div className="space-y-3 p-4">
        <div className="flex flex-wrap items-center justify-between gap-2">
          <JobFiltersBar filters={filters} onChange={patch} />
          <Select
            value={String(filters.limit)}
            onValueChange={(value) => patch({ limit: Number(value) })}
          >
            <SelectTrigger size="sm" className="w-[120px]" aria-label="Rows per page">
              <SelectValue />
            </SelectTrigger>
            <SelectContent>
              {JOB_PAGE_SIZES.map((size) => (
                <SelectItem key={size} value={String(size)}>
                  {size} / page
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
        </div>

        {isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 6 }, (_, index) => (
              <Skeleton key={index} className="h-9 w-full" />
            ))}
          </div>
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load jobs</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : jobs.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            {filters.status || filters.pipeline_uid
              ? "No job matches these filters."
              : "No job yet. Send something through the Playground."}
          </p>
        ) : (
          <div className="overflow-hidden rounded-md border">
            <JobsTable jobs={jobs} onCancel={setToCancel} />
          </div>
        )}

        <div className="flex items-center justify-between text-xs text-muted-foreground">
          <span>
            Page {page}
            {totalPages ? ` of ${totalPages}` : ""}
            {data?.total ? ` · ${data.total} job${data.total === 1 ? "" : "s"}` : ""}
          </span>
          <div className="flex items-center gap-1">
            <Button
              variant="outline"
              size="sm"
              disabled={filters.offset === 0}
              onClick={() =>
                setFilters((current) => ({
                  ...current,
                  offset: Math.max(0, current.offset - current.limit),
                }))
              }
            >
              <ChevronLeft aria-hidden />
              Previous
            </Button>
            <Button
              variant="outline"
              size="sm"
              disabled={!hasNext}
              onClick={() =>
                setFilters((current) => ({ ...current, offset: current.offset + current.limit }))
              }
            >
              Next
              <ChevronRight aria-hidden />
            </Button>
          </div>
        </div>
      </div>

      <CancelJobDialog
        jobId={toCancel}
        open={toCancel !== undefined}
        onOpenChange={(open) => {
          if (!open) setToCancel(undefined);
        }}
      />
    </>
  );
}
