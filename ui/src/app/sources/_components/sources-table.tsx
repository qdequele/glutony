"use client";

import Link from "next/link";
import { useState } from "react";
import { History, MoreHorizontal, Pause, Pencil, Play, Trash2, Zap } from "lucide-react";

import { editPipelineHref } from "@/app/pipelines/routes";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import {
  isArchived,
  useSetSourcePaused,
  useSource,
  useTriggerSourceRun,
  type SourceView,
} from "@/lib/api/sources";
import { editSourceHref, sourceDetailHref } from "../routes";
import { DeleteSourceDialog } from "./delete-source-dialog";
import { ScheduleSummary, RelativeTimestamp } from "./schedule-summary";
import { RunOutcomeBadge, SourceStateBadge } from "./source-badges";
import { useNow } from "./use-now";

/**
 * `GET /sources` does not carry `next_run_at` (it comes from Temporal, one
 * describe per schedule), so an active row reads it from the single-source
 * endpoint. That also warms the cache the detail page opens on.
 */
function NextRunCell({ source, now }: { source: SourceView; now: Date }) {
  const inactive = isArchived(source) || source.paused;
  const detail = useSource(inactive || source.next_run_at ? undefined : source.uid);
  if (isArchived(source)) return <span className="text-muted-foreground">never</span>;
  if (source.paused) return <span className="text-muted-foreground">paused</span>;
  const next = source.next_run_at ?? detail.data?.next_run_at;
  if (!next && detail.isPending) return <span className="text-muted-foreground">…</span>;
  return <RelativeTimestamp value={next} now={now} kind="next" />;
}

function LastRunCell({ source, now }: { source: SourceView; now: Date }) {
  const badge = <RunOutcomeBadge outcome={source.last_status} />;
  return (
    <div className="flex flex-col items-start gap-0.5">
      {source.last_status === "failed" && source.last_error ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <span>{badge}</span>
          </TooltipTrigger>
          <TooltipContent className="max-w-sm break-words">{source.last_error}</TooltipContent>
        </Tooltip>
      ) : (
        badge
      )}
      {source.last_run_at ? (
        <span className="text-[11px] text-muted-foreground">
          <RelativeTimestamp value={source.last_run_at} now={now} kind="past" />
        </span>
      ) : null}
    </div>
  );
}

export function SourcesTable({ sources }: { sources: SourceView[] }) {
  const now = useNow();
  const [toDelete, setToDelete] = useState<string | undefined>(undefined);
  const trigger = useTriggerSourceRun();
  const setPaused = useSetSourcePaused();

  return (
    <>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[22%]">Source</TableHead>
            <TableHead className="w-[16%]">Pipeline</TableHead>
            <TableHead className="w-[24%]">Schedule</TableHead>
            <TableHead className="w-[13%]">Next run</TableHead>
            <TableHead className="w-[15%]">Last run</TableHead>
            <TableHead className="w-10" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {sources.map((source) => {
            const archived = isArchived(source);
            return (
              <TableRow
                key={`${source.project_id ?? ""}:${source.uid}`}
                className={archived ? "opacity-70" : undefined}
              >
                <TableCell>
                  <div className="flex min-w-0 flex-col gap-0.5">
                    <div className="flex items-center gap-1.5">
                      <Link
                        href={sourceDetailHref(source.uid)}
                        className="truncate font-mono text-xs hover:underline"
                      >
                        {source.uid}
                      </Link>
                      <SourceStateBadge source={source} />
                    </div>
                    {source.name && source.name !== source.uid ? (
                      <span className="truncate text-xs text-muted-foreground">{source.name}</span>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell className="truncate font-mono text-xs">
                  {archived ? (
                    <span className="text-muted-foreground line-through">{source.pipeline}</span>
                  ) : (
                    <Link href={editPipelineHref(source.pipeline)} className="hover:underline">
                      {source.pipeline}
                    </Link>
                  )}
                </TableCell>
                <TableCell>
                  <ScheduleSummary cron={source.cron} timezone={source.timezone} />
                </TableCell>
                <TableCell className="text-xs">
                  <NextRunCell source={source} now={now} />
                </TableCell>
                <TableCell className="text-xs">
                  <LastRunCell source={source} now={now} />
                </TableCell>
                <TableCell>
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="size-7"
                        aria-label={`Actions for ${source.uid}`}
                      >
                        <MoreHorizontal aria-hidden />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem asChild>
                        <Link href={sourceDetailHref(source.uid)}>
                          <History aria-hidden />
                          Runs
                        </Link>
                      </DropdownMenuItem>
                      <DropdownMenuItem
                        disabled={archived || trigger.isPending}
                        onSelect={() => trigger.mutate(source.uid)}
                      >
                        <Zap aria-hidden />
                        Run now
                      </DropdownMenuItem>
                      <DropdownMenuItem
                        disabled={archived || setPaused.isPending}
                        onSelect={() =>
                          setPaused.mutate({ uid: source.uid, paused: !source.paused })
                        }
                      >
                        {source.paused ? <Play aria-hidden /> : <Pause aria-hidden />}
                        {source.paused ? "Resume" : "Pause"}
                      </DropdownMenuItem>
                      {archived ? (
                        // An archived source has no schedule left to update: the
                        // only way forward is to delete it and create a new one.
                        <DropdownMenuItem disabled>
                          <Pencil aria-hidden />
                          Edit
                        </DropdownMenuItem>
                      ) : (
                        <DropdownMenuItem asChild>
                          <Link href={editSourceHref(source.uid)}>
                            <Pencil aria-hidden />
                            Edit
                          </Link>
                        </DropdownMenuItem>
                      )}
                      <DropdownMenuSeparator />
                      <DropdownMenuItem
                        variant="destructive"
                        onSelect={() => setToDelete(source.uid)}
                      >
                        <Trash2 aria-hidden />
                        Delete
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>

      <DeleteSourceDialog
        uid={toDelete}
        open={toDelete !== undefined}
        onOpenChange={(open) => {
          if (!open) setToDelete(undefined);
        }}
      />
    </>
  );
}
