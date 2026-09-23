"use client";

import Link from "next/link";

import { jobDetailHref } from "@/app/jobs/routes";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import type { RunRecord } from "@/lib/api/sources";
import { formatRunDuration } from "../_lib/time";
import { RelativeTimestamp } from "./schedule-summary";
import { RunOutcomeBadge } from "./source-badges";

/** Run history of one source, newest first, as the API returns it. */
export function RunsTable({ runs, now }: { runs: RunRecord[]; now: Date }) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-[14%]">Outcome</TableHead>
          <TableHead className="w-[16%]">Started</TableHead>
          <TableHead className="w-[16%]">Finished</TableHead>
          <TableHead className="w-[10%]">Duration</TableHead>
          <TableHead className="w-[8%] text-right">Items</TableHead>
          <TableHead className="w-[36%]">Jobs / error</TableHead>
        </TableRow>
      </TableHeader>
      <TableBody>
        {runs.map((run) => (
          <TableRow key={run.run_id}>
            <TableCell>
              <RunOutcomeBadge outcome={run.outcome} />
            </TableCell>
            <TableCell className="text-xs">
              <RelativeTimestamp value={run.started_at} now={now} kind="past" />
            </TableCell>
            <TableCell className="text-xs">
              <RelativeTimestamp value={run.finished_at} now={now} kind="past" />
            </TableCell>
            <TableCell className="text-xs tabular-nums text-muted-foreground">
              {formatRunDuration(run.started_at, run.finished_at)}
            </TableCell>
            <TableCell className="text-right text-xs tabular-nums">{run.items}</TableCell>
            <TableCell className="text-xs whitespace-normal">
              {run.error ? (
                <p className="font-mono break-words text-destructive">{run.error}</p>
              ) : null}
              {run.job_ids.length > 0 ? (
                <div className="flex flex-wrap gap-x-2 gap-y-0.5">
                  {run.job_ids.map((jobId) => (
                    <Link
                      key={jobId}
                      href={jobDetailHref(jobId)}
                      className="font-mono hover:underline"
                    >
                      {jobId.slice(0, 8)}
                    </Link>
                  ))}
                </div>
              ) : !run.error ? (
                <span className="text-muted-foreground">
                  {run.outcome === "unchanged" ? "Upstream unchanged — nothing ingested" : "—"}
                </span>
              ) : null}
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
