"use client";

import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { Suspense, useState, type ReactNode } from "react";
import { ArrowLeft, Pause, Pencil, Play, RefreshCw, Trash2, Zap } from "lucide-react";

import { editPipelineHref } from "@/app/pipelines/routes";
import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ApiError } from "@/lib/api/client";
import { errorMessage } from "@/lib/api/hooks";
import {
  isArchived,
  useSetSourcePaused,
  useSource,
  useSourceRuns,
  useTriggerSourceRun,
  type SourceView,
} from "@/lib/api/sources";
import { cn } from "@/lib/utils";
import { DeleteSourceDialog } from "../_components/delete-source-dialog";
import { NotEnabled } from "../_components/not-enabled";
import { RunsTable } from "../_components/runs-table";
import { RelativeTimestamp, ScheduleSummary } from "../_components/schedule-summary";
import { RunOutcomeBadge, SourceStateBadge } from "../_components/source-badges";
import { useNow } from "../_components/use-now";
import { SOURCES_HREF, editSourceHref } from "../routes";

function Summary({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="min-w-0">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="mt-0.5 min-w-0 text-sm">{children}</dd>
    </div>
  );
}

function credentialLabel(source: SourceView): string {
  switch (source.auth?.kind) {
    case undefined:
      return "none";
    case "bearer":
      return "bearer token ****";
    case "basic":
      return `basic ${source.auth.username} / ****`;
    case "headers":
      return `headers ${Object.keys(source.auth.headers).join(", ")}`;
    default:
      return "stored (unreadable)";
  }
}

function SourceSummary({ source, now }: { source: SourceView; now: Date }) {
  const archived = isArchived(source);
  const headers = Object.entries(source.location.headers ?? {});
  return (
    <dl className="grid grid-cols-2 gap-4 rounded-md border p-3 sm:grid-cols-4">
      <Summary label="Pipeline">
        {archived ? (
          <span className="font-mono text-muted-foreground line-through">{source.pipeline}</span>
        ) : (
          <Link href={editPipelineHref(source.pipeline)} className="font-mono hover:underline">
            {source.pipeline}
          </Link>
        )}
      </Summary>
      <Summary label="Schedule">
        <ScheduleSummary cron={source.cron} timezone={source.timezone} />
      </Summary>
      <Summary label="Next run">
        {archived ? (
          <span className="text-muted-foreground">never — archived</span>
        ) : source.paused ? (
          <span className="text-muted-foreground">paused</span>
        ) : (
          <RelativeTimestamp value={source.next_run_at} now={now} kind="next" />
        )}
      </Summary>
      <Summary label="Last run">
        <div className="flex flex-wrap items-center gap-1.5">
          <RunOutcomeBadge outcome={source.last_status} />
          {source.last_run_at ? (
            <span className="text-xs text-muted-foreground">
              <RelativeTimestamp value={source.last_run_at} now={now} kind="past" />
            </span>
          ) : null}
        </div>
      </Summary>
      <div className="col-span-2 sm:col-span-4">
        <Summary label="Location">
          <p className="font-mono text-xs break-all">
            <span className="text-muted-foreground">{source.location.method ?? "GET"} </span>
            {source.location.url}
          </p>
          {headers.length > 0 ? (
            <p className="mt-0.5 font-mono text-[11px] break-all text-muted-foreground">
              {headers.map(([name, value]) => `${name}: ${value}`).join(" · ")}
            </p>
          ) : null}
        </Summary>
      </div>
      <Summary label="Index">
        <span className="font-mono text-xs">{source.index ?? "from the pipeline"}</span>
      </Summary>
      <Summary label="Credential">
        <span className="font-mono text-xs">{credentialLabel(source)}</span>
      </Summary>
      {source.description ? (
        <div className="col-span-2">
          <Summary label="Description">{source.description}</Summary>
        </div>
      ) : null}
    </dl>
  );
}

function SourceDetailView() {
  const router = useRouter();
  const uid = useSearchParams().get("uid") ?? "";
  const source = useSource(uid || undefined);
  const runs = useSourceRuns(uid || undefined);
  const trigger = useTriggerSourceRun();
  const setPaused = useSetSourcePaused();
  const now = useNow();
  const [deleteOpen, setDeleteOpen] = useState(false);

  const back = (
    <Button asChild variant="ghost" size="sm">
      <Link href={SOURCES_HREF}>
        <ArrowLeft aria-hidden />
        All sources
      </Link>
    </Button>
  );

  if (!uid) {
    return (
      <>
        <PageHeader title="Source" actions={back} />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>No source selected</AlertTitle>
            <AlertDescription>
              This page needs a <span className="font-mono">?uid=</span> parameter.
            </AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  const data = source.data;
  const archived = data ? isArchived(data) : false;
  const refreshing = source.isFetching || runs.isFetching;
  const notConfigured = source.error instanceof ApiError && source.error.isNotConfigured;

  return (
    <>
      <PageHeader
        title={data?.name && data.name !== uid ? `${data.name} (${uid})` : uid}
        description={data?.description}
        actions={
          <>
            {back}
            <Button
              variant="outline"
              size="sm"
              disabled={refreshing}
              onClick={() => {
                void source.refetch();
                void runs.refetch();
              }}
            >
              <RefreshCw aria-hidden className={cn(refreshing && "animate-spin")} />
              Refresh
            </Button>
            {data ? (
              <>
                <Button
                  variant="outline"
                  size="sm"
                  disabled={archived || setPaused.isPending}
                  onClick={() => setPaused.mutate({ uid, paused: !data.paused })}
                >
                  {data.paused ? <Play aria-hidden /> : <Pause aria-hidden />}
                  {data.paused ? "Resume" : "Pause"}
                </Button>
                {archived ? (
                  <Button variant="outline" size="sm" disabled>
                    <Pencil aria-hidden />
                    Edit
                  </Button>
                ) : (
                  <Button asChild variant="outline" size="sm">
                    <Link href={editSourceHref(uid)}>
                      <Pencil aria-hidden />
                      Edit
                    </Link>
                  </Button>
                )}
                <Button
                  variant="outline"
                  size="sm"
                  className="text-destructive"
                  onClick={() => setDeleteOpen(true)}
                >
                  <Trash2 aria-hidden />
                  Delete
                </Button>
                <Button
                  size="sm"
                  disabled={archived || trigger.isPending}
                  onClick={() => trigger.mutate(uid)}
                >
                  <Zap aria-hidden />
                  {trigger.isPending ? "Triggering…" : "Run now"}
                </Button>
              </>
            ) : null}
          </>
        }
      />

      <div className="space-y-4 p-4">
        {source.isPending ? (
          <Skeleton className="h-40 w-full" />
        ) : notConfigured ? (
          <NotEnabled feature="Scheduled sources" />
        ) : source.error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load this source</AlertTitle>
            <AlertDescription>{errorMessage(source.error)}</AlertDescription>
          </Alert>
        ) : (
          <>
            <div className="flex items-center gap-2">
              <SourceStateBadge source={source.data} />
            </div>

            {archived ? (
              <Alert>
                <AlertTitle>Archived</AlertTitle>
                <AlertDescription>
                  Its pipeline <span className="font-mono">{source.data.pipeline}</span> was
                  deleted, so the schedule is gone and it will not run again. The history below is
                  kept.
                </AlertDescription>
              </Alert>
            ) : null}

            {source.data.last_status === "failed" && source.data.last_error ? (
              <Alert variant="destructive">
                <AlertTitle>Last run failed</AlertTitle>
                <AlertDescription className="font-mono text-xs break-words">
                  {source.data.last_error}
                </AlertDescription>
              </Alert>
            ) : null}

            <SourceSummary source={source.data} now={now} />

            <div>
              <h2 className="mb-2 text-sm font-semibold">Recent runs</h2>
              {runs.isPending ? (
                <div className="space-y-2">
                  {Array.from({ length: 3 }, (_, index) => (
                    <Skeleton key={index} className="h-9 w-full" />
                  ))}
                </div>
              ) : runs.error ? (
                <Alert variant="destructive">
                  <AlertTitle>Could not load the run history</AlertTitle>
                  <AlertDescription>{errorMessage(runs.error)}</AlertDescription>
                </Alert>
              ) : runs.data.length === 0 ? (
                <p className="rounded-md border py-10 text-center text-sm text-muted-foreground">
                  No run yet.{" "}
                  {archived ? null : "It runs on its schedule, or now with “Run now”."}
                </p>
              ) : (
                <div className="overflow-hidden rounded-md border">
                  <RunsTable runs={runs.data} now={now} />
                </div>
              )}
            </div>
          </>
        )}
      </div>

      <DeleteSourceDialog
        uid={uid}
        open={deleteOpen}
        onOpenChange={setDeleteOpen}
        onDeleted={() => router.push(SOURCES_HREF)}
      />
    </>
  );
}

export default function SourceDetailPage() {
  return (
    <Suspense
      fallback={
        <div className="space-y-3 p-4">
          <Skeleton className="h-40 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
      }
    >
      <SourceDetailView />
    </Suspense>
  );
}
