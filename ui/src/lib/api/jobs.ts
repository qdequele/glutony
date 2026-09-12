"use client";

/**
 * Jobs API: wire types, the `GET /jobs` query-string builder, the pure
 * formatting/polling helpers the Jobs screen needs, and the TanStack Query
 * bindings on top of them.
 *
 * Lives outside `client.ts` / `hooks.ts` on purpose: those two files are shared
 * by every screen, this one belongs to Jobs and to the Playground that follows a
 * job it just started.
 *
 * The shapes below mirror the Rust handlers, not `docs/openapi.yaml` (which is
 * behind): `crates/gateway/src/handlers/jobs.rs` for the responses,
 * `crates/control-plane/src/jobs.rs` for `JobRecord` and the list envelope, and
 * `crates/plugin-sdk/src/types.rs` for `WorkflowProgress` / `StepResult`.
 */
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseMutationResult,
  type UseQueryResult,
} from "@tanstack/react-query";
import { toast } from "sonner";

import { request } from "./client";
import { errorMessage } from "./hooks";
import type { JobStatus, WorkflowProgress } from "./types";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/**
 * One row of `GET /jobs` (`JobRecord` in the control plane).
 *
 * `started_at` / `updated_at` are RFC 3339 strings.
 */
export interface JobRecord {
  job_id: string;
  workflow_id: string;
  pipeline_uid: string;
  project_id?: string;
  index_name?: string;
  status: JobStatus;
  current_step?: string;
  error?: string;
  started_at: string;
  updated_at: string;
}

/** Envelope of `GET /jobs`. */
export interface JobListResponse {
  jobs: JobRecord[];
  limit: number;
  offset: number;
  /** Rows matching the filters ignoring paging, so "next" is exact. */
  total: number;
}

/** Whether another page exists after the one described by `page`. */
export function hasNextPage(page: JobListResponse | undefined): boolean {
  if (!page) return false;
  // Fall back to the full-page heuristic if an older gateway omits `total`.
  if (typeof page.total !== "number") return page.jobs.length === page.limit;
  return page.offset + page.jobs.length < page.total;
}

/**
 * `GET /jobs/{id}`.
 *
 * Everything but `job_id` and `status` is optional: the gateway falls back to
 * its cached row when Temporal no longer knows the workflow, and that fallback
 * carries no `progress` (the field is serialized as `null`).
 */
export interface JobDetail {
  job_id: string;
  status: JobStatus;
  current_step?: string | null;
  progress?: WorkflowProgress | null;
  pipeline_used?: string | null;
  target_index?: string | null;
  error?: string | null;
}

/** 202 body of `POST /jobs/{id}/cancel`. */
export interface CancelJobResponse {
  job_id: string;
  status: string;
}

/**
 * Filters of the Jobs list.
 *
 * There is deliberately no `project_id`: the gateway derives the tenant from
 * the request context and appends it itself, so sending one from the browser is
 * at best ignored and at worst misleading.
 */
export interface JobFilters {
  /** Only jobs in this status. `undefined` = every status. */
  status?: JobStatus;
  /** Only jobs started by this pipeline. */
  pipeline_uid?: string;
  /** Page size (the control plane clamps to 1..=200, default 50). */
  limit: number;
  /** Rows to skip. */
  offset: number;
}

/** Page sizes offered in the UI. */
export const JOB_PAGE_SIZES = [25, 50, 100] as const;

/** Default filter state: first page, every status, every pipeline. */
export const DEFAULT_JOB_FILTERS: JobFilters = { limit: 25, offset: 0 };

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in ./jobs.test.ts)
// ---------------------------------------------------------------------------

/** Every status a job can be in, in lifecycle order. */
export const JOB_STATUSES: readonly JobStatus[] = [
  "queued",
  "running",
  "succeeded",
  "failed",
  "cancelled",
] as const;

/** Mirrors `JobStatus::is_terminal()`: no further transition is possible. */
export function isTerminalStatus(status: JobStatus): boolean {
  return status === "succeeded" || status === "failed" || status === "cancelled";
}

/**
 * Query string for `GET /jobs`, leading `?` included (empty string when there
 * is nothing to send).
 *
 * Empty filters are dropped rather than sent blank, `offset=0` is left implicit,
 * and `project_id` is never emitted — see {@link JobFilters}.
 */
export function jobListSearch(filters: JobFilters): string {
  const params = new URLSearchParams();
  if (filters.status) params.set("status", filters.status);
  const pipeline = filters.pipeline_uid?.trim();
  if (pipeline) params.set("pipeline_uid", pipeline);
  params.set("limit", String(filters.limit));
  if (filters.offset > 0) params.set("offset", String(filters.offset));
  const query = params.toString();
  return query ? `?${query}` : "";
}

/** How often a screen with in-flight work re-reads the API. */
export const JOB_POLL_INTERVAL_MS = 3_000;

/**
 * Poll while something can still change, stop as soon as everything settled.
 *
 * Returning `false` is what tells TanStack Query to stop the timer, so a page
 * left open on a finished job costs nothing.
 */
export function pollIntervalFor(statuses: readonly JobStatus[]): number | false {
  return statuses.some((status) => !isTerminalStatus(status)) ? JOB_POLL_INTERVAL_MS : false;
}

/** `refetchInterval` for the Jobs list: driven by the statuses on screen. */
export function jobListPollInterval(page: JobListResponse | undefined): number | false {
  if (!page) return false;
  return pollIntervalFor(page.jobs.map((job) => job.status));
}

/** `refetchInterval` for a single job. */
export function jobDetailPollInterval(job: JobDetail | undefined): number | false {
  if (!job) return false;
  return pollIntervalFor([job.status]);
}

/** Human duration for a step. `undefined` renders as an em dash. */
export function formatDuration(ms: number | undefined): string {
  if (ms === undefined || !Number.isFinite(ms) || ms < 0) return "—";
  if (ms < 1_000) return `${Math.round(ms)} ms`;
  const seconds = ms / 1_000;
  if (seconds < 60) return `${seconds.toFixed(seconds < 10 ? 2 : 1)} s`;
  const minutes = Math.floor(seconds / 60);
  const rest = Math.round(seconds - minutes * 60);
  return rest === 0 ? `${minutes}m` : `${minutes}m ${rest}s`;
}

const BYTE_UNITS = ["B", "KB", "MB", "GB", "TB"] as const;

/** Human byte count (1024-based). `undefined` renders as an em dash. */
export function formatBytes(bytes: number | undefined): string {
  if (bytes === undefined || !Number.isFinite(bytes) || bytes < 0) return "—";
  if (bytes < 1024) return `${Math.round(bytes)} B`;
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < BYTE_UNITS.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${value.toFixed(value < 10 ? 1 : 0)} ${BYTE_UNITS[unit]}`;
}

/** `completed_steps / total_steps` as a 0..100 percentage. */
export function progressPercent(progress: WorkflowProgress | null | undefined): number {
  if (!progress || progress.total_steps <= 0) return 0;
  const ratio = progress.completed_steps / progress.total_steps;
  return Math.max(0, Math.min(100, Math.round(ratio * 100)));
}

/** Whether the UI should offer to cancel this job. */
export function isCancellable(status: JobStatus): boolean {
  return status === "queued" || status === "running";
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

/** `GET /jobs?status=&pipeline_uid=&limit=&offset=` — newest first. */
export function listJobs(filters: JobFilters, signal?: AbortSignal): Promise<JobListResponse> {
  return request<JobListResponse>(`/jobs${jobListSearch(filters)}`, { signal });
}

/** `GET /jobs/{id}` — Temporal-backed, with the cached row as a fallback. */
export function getJob(jobId: string, signal?: AbortSignal): Promise<JobDetail> {
  return request<JobDetail>(`/jobs/${encodeURIComponent(jobId)}`, { signal });
}

/** `POST /jobs/{id}/cancel` — 202, the workflow winds down asynchronously. */
export function cancelJob(jobId: string): Promise<CancelJobResponse> {
  return request<CancelJobResponse>(`/jobs/${encodeURIComponent(jobId)}/cancel`, {
    method: "POST",
  });
}

// ---------------------------------------------------------------------------
// Query bindings
// ---------------------------------------------------------------------------

/** Cache keys for everything job-shaped. */
export const jobQueryKeys = {
  all: ["jobs"] as const,
  list: (filters: JobFilters) => ["jobs", "list", filters] as const,
  detail: (jobId: string) => ["jobs", "detail", jobId] as const,
};

/** One page of jobs, polled while any row on it is still moving. */
export function useJobs(filters: JobFilters): UseQueryResult<JobListResponse, Error> {
  return useQuery({
    queryKey: jobQueryKeys.list(filters),
    queryFn: ({ signal }) => listJobs(filters, signal),
    refetchInterval: (query) => jobListPollInterval(query.state.data),
    placeholderData: (previous) => previous,
  });
}

/**
 * One job, polled until it reaches a terminal status.
 *
 * Disabled while `jobId` is empty, which is the `/jobs/detail/` page without a
 * `?id=` and the Playground before a submission.
 */
export function useJob(jobId: string | undefined): UseQueryResult<JobDetail, Error> {
  return useQuery({
    queryKey: jobQueryKeys.detail(jobId ?? ""),
    queryFn: ({ signal }) => getJob(jobId as string, signal),
    enabled: Boolean(jobId),
    refetchInterval: (query) => jobDetailPollInterval(query.state.data),
  });
}

/** Request cancellation, then refresh the job and the list it appears in. */
export function useCancelJob(): UseMutationResult<CancelJobResponse, Error, string> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (jobId: string) => cancelJob(jobId),
    onSuccess: async (_response, jobId) => {
      toast.success("Cancellation requested", {
        description: "The workflow stops after the step in flight.",
      });
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: jobQueryKeys.detail(jobId) }),
        queryClient.invalidateQueries({ queryKey: jobQueryKeys.all }),
      ]);
    },
    onError: (error) => {
      toast.error("Could not cancel the job", { description: errorMessage(error) });
    },
  });
}
