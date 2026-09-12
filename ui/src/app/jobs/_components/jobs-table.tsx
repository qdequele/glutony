"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { formatDistanceToNow } from "date-fns";
import { Ban, MoreHorizontal, SquareArrowOutUpRight } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
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
import { isCancellable, type JobRecord } from "@/lib/api/jobs";
import { jobDetailHref } from "../routes";
import { JobStatusBadge } from "./job-status-badge";

/** "2 minutes ago", with the absolute timestamp behind a tooltip. */
function RelativeTime({ iso }: { iso: string }) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) {
    return <span className="text-muted-foreground">—</span>;
  }
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="text-muted-foreground">
          {formatDistanceToNow(date, { addSuffix: true })}
        </span>
      </TooltipTrigger>
      <TooltipContent>{date.toLocaleString()}</TooltipContent>
    </Tooltip>
  );
}

export interface JobsTableProps {
  jobs: JobRecord[];
  /** Opens the cancel confirmation for this job. */
  onCancel: (jobId: string) => void;
}

export function JobsTable({ jobs, onCancel }: JobsTableProps) {
  const router = useRouter();

  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-[24%]">Job</TableHead>
          <TableHead className="w-[20%]">Pipeline</TableHead>
          <TableHead className="w-[18%]">Index</TableHead>
          <TableHead className="w-[14%]">Status</TableHead>
          <TableHead className="w-[14%]">Started</TableHead>
          <TableHead className="w-[10%]">Updated</TableHead>
          <TableHead className="w-10" />
        </TableRow>
      </TableHeader>
      <TableBody>
        {jobs.map((job) => (
          <TableRow
            key={job.job_id}
            // The whole row is a link target; the anchor below keeps it
            // keyboard-reachable and middle-clickable.
            className="cursor-pointer"
            onClick={() => router.push(jobDetailHref(job.job_id))}
          >
            <TableCell className="font-mono text-xs">
              <Link
                href={jobDetailHref(job.job_id)}
                className="hover:underline"
                onClick={(event) => event.stopPropagation()}
              >
                {job.job_id.slice(0, 8)}
              </Link>
              {job.current_step ? (
                <span className="ml-2 text-muted-foreground">→ {job.current_step}</span>
              ) : null}
            </TableCell>
            <TableCell className="font-mono text-xs">{job.pipeline_uid}</TableCell>
            <TableCell className="truncate font-mono text-xs">
              {job.index_name ?? <span className="text-muted-foreground">—</span>}
            </TableCell>
            <TableCell>
              <div className="flex items-center gap-1.5">
                <JobStatusBadge status={job.status} />
                {job.status === "failed" && job.error ? (
                  <Tooltip>
                    <TooltipTrigger asChild>
                      <Badge variant="ghost" className="text-[10px]">
                        why?
                      </Badge>
                    </TooltipTrigger>
                    <TooltipContent className="max-w-sm break-words">{job.error}</TooltipContent>
                  </Tooltip>
                ) : null}
              </div>
            </TableCell>
            <TableCell className="text-xs">
              <RelativeTime iso={job.started_at} />
            </TableCell>
            <TableCell className="text-xs">
              <RelativeTime iso={job.updated_at} />
            </TableCell>
            <TableCell onClick={(event) => event.stopPropagation()}>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="size-7"
                    aria-label={`Actions for job ${job.job_id}`}
                  >
                    <MoreHorizontal aria-hidden />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem asChild>
                    <Link href={jobDetailHref(job.job_id)}>
                      <SquareArrowOutUpRight aria-hidden />
                      Open detail
                    </Link>
                  </DropdownMenuItem>
                  <DropdownMenuItem
                    variant="destructive"
                    disabled={!isCancellable(job.status)}
                    onSelect={() => onCancel(job.job_id)}
                  >
                    <Ban aria-hidden />
                    Cancel
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
