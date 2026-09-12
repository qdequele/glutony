"use client";

import { X } from "lucide-react";

import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { JOB_STATUSES, type JobFilters } from "@/lib/api/jobs";
import { usePipelines } from "@/lib/api/hooks";

/** Sentinel for "no filter": Radix Select forbids an empty-string item value. */
const ANY = "__any__";

export interface JobFiltersBarProps {
  filters: JobFilters;
  /** Any change resets `offset`; the caller does that, not this component. */
  onChange: (patch: Partial<JobFilters>) => void;
}

export function JobFiltersBar({ filters, onChange }: JobFiltersBarProps) {
  const pipelines = usePipelines();
  const hasFilter = filters.status !== undefined || Boolean(filters.pipeline_uid);

  return (
    <div className="flex flex-wrap items-center gap-2">
      <Select
        value={filters.status ?? ANY}
        onValueChange={(value) =>
          onChange({ status: value === ANY ? undefined : (value as JobFilters["status"]) })
        }
      >
        <SelectTrigger size="sm" className="w-[150px]" aria-label="Filter by status">
          <SelectValue placeholder="Any status" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ANY}>Any status</SelectItem>
          {JOB_STATUSES.map((status) => (
            <SelectItem key={status} value={status} className="capitalize">
              {status}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select
        value={filters.pipeline_uid ?? ANY}
        onValueChange={(value) =>
          onChange({ pipeline_uid: value === ANY ? undefined : value })
        }
      >
        <SelectTrigger size="sm" className="w-[220px]" aria-label="Filter by pipeline">
          <SelectValue placeholder="Any pipeline" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ANY}>Any pipeline</SelectItem>
          {(pipelines.data ?? []).map((pipeline) => (
            <SelectItem key={pipeline.uid} value={pipeline.uid} className="font-mono text-xs">
              {pipeline.uid}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {hasFilter ? (
        <Button
          variant="ghost"
          size="sm"
          onClick={() => onChange({ status: undefined, pipeline_uid: undefined })}
        >
          <X aria-hidden />
          Clear
        </Button>
      ) : null}
    </div>
  );
}
