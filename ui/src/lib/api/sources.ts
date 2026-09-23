"use client";

/**
 * Sources API: cron-scheduled fetches that feed a pipeline.
 *
 * Mirrors `crates/gateway/src/sources.rs` (`SourceView`, `CreateSource`,
 * `UpdateSource`, `RunRecord`), `crates/source/src/model.rs` (`Location`,
 * `FetchAuth`, `RunOutcome`) and `crates/gateway/src/handlers/sources.rs`.
 *
 * Like connections, every route answers `501 not_configured` until the
 * deployment sets `SOURCE_SECRET_KEY`.
 */
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseMutationResult,
  type UseQueryResult,
} from "@tanstack/react-query";
import { toast } from "sonner";

import { isFormError, request } from "./client";
import { errorMessage } from "./hooks";
import { jobQueryKeys } from "./jobs";
import type { PipelineDefinition } from "./types";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/** `Location` — a tagged union; `url` is the only variant in v1. */
export interface UrlLocation {
  kind: "url";
  /** May contain `{{ date:FMT }}`, `{{ date-1d:FMT }}`, `{{ timestamp }}`. */
  url: string;
  /** `GET` when absent. */
  method?: string;
  /** Non-secret headers. Secrets belong in `auth`. */
  headers?: Record<string, string>;
}

/** Where a source fetches from. */
export type SourceLocation = UrlLocation;

/** `FetchAuth` as it is written: values in clear, sealed by the gateway. */
export type FetchAuth =
  | { kind: "bearer"; token: string }
  | { kind: "basic"; username: string; password: string }
  | { kind: "headers"; headers: Record<string, string> };

/** Every credential kind, in the order the form offers them. */
export const FETCH_AUTH_KINDS = ["bearer", "basic", "headers"] as const;
/** `FetchAuth["kind"]`. */
export type FetchAuthKind = (typeof FETCH_AUTH_KINDS)[number];

/**
 * `FetchAuth` as it is read back: every secret value is `"****"`, the basic
 * username and the header names are kept. `unknown` is what the gateway answers
 * when it can no longer open the sealed blob.
 */
export type RedactedFetchAuth =
  | { kind: "bearer"; token: string }
  | { kind: "basic"; username: string; password: string }
  | { kind: "headers"; headers: Record<string, string> }
  | { kind: "unknown"; sealed?: boolean };

/** `RunOutcome`. */
export type RunOutcome = "ingested" | "unchanged" | "failed";

/** `SourceView`. Timestamps are RFC 3339. */
export interface SourceView {
  uid: string;
  name: string;
  description?: string;
  project_id?: string;
  /** Pipeline uid fed on every tick. */
  pipeline: string;
  location: SourceLocation;
  cron: string;
  /** IANA timezone for the cron and the URL template. */
  timezone: string;
  paused: boolean;
  /** Index override. */
  index?: string;
  /** Redacted credential, when one is set. */
  auth?: RedactedFetchAuth;
  /** Set when the pipeline was deleted; archived sources never fire. */
  archived_at?: string;
  /** Next scheduled run — single-source reads only. */
  next_run_at?: string;
  last_run_at?: string;
  last_status?: RunOutcome;
  last_error?: string;
}

/** `POST /sources` body (`deny_unknown_fields`: send nothing else). */
export interface CreateSourceBody {
  uid: string;
  name?: string;
  description?: string;
  pipeline: string;
  location: SourceLocation;
  cron: string;
  timezone?: string;
  index?: string;
  auth?: FetchAuth;
}

/**
 * `PATCH /sources/{uid}` body. `auth` absent keeps the credential, `null`
 * clears it, an object replaces it.
 */
export interface UpdateSourceBody {
  name?: string;
  description?: string;
  pipeline?: string;
  location?: SourceLocation;
  cron?: string;
  timezone?: string;
  index?: string;
  auth?: FetchAuth | null;
}

/** One row of `GET /sources/{uid}/runs`, newest first. */
export interface RunRecord {
  run_id: string;
  source_id: string;
  started_at: string;
  finished_at?: string;
  outcome: RunOutcome;
  /** Items fetched (documents or files handed to the pipeline). */
  items: number;
  /** Jobs this run started; empty for `unchanged` and `failed`. */
  job_ids: string[];
  error?: string;
}

/** 202 body of `POST /sources/{uid}/run`. */
export interface TriggerRunResponse {
  status: string;
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in ./sources.test.ts)
// ---------------------------------------------------------------------------

/** The plugin whose `config.connection` pins a pipeline's destination. */
export const INDEXER_PLUGIN = "meili_indexer";

/**
 * Mirrors `PipelineDefinition::pins_destination()` on the Rust side: the
 * pipeline has at least one `meili_indexer` step and every one of them names a
 * non-blank `connection`. A scheduled run has no request to supply a
 * Meilisearch, so this is what the gateway requires of a source's pipeline.
 */
export function pipelinePinsConnection(pipeline: PipelineDefinition): boolean {
  const indexers = pipeline.steps.filter((step) => step.plugin === INDEXER_PLUGIN);
  if (indexers.length === 0) return false;
  return indexers.every((step) => {
    const connection = step.config?.connection;
    return typeof connection === "string" && connection.trim().length > 0;
  });
}

/** Whether "run now" and pause/unpause make sense for this source. */
export function isArchived(source: Pick<SourceView, "archived_at">): boolean {
  return Boolean(source.archived_at);
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

function sourcePath(uid: string): string {
  return `/sources/${encodeURIComponent(uid)}`;
}

/**
 * Query string for `GET /sources`, leading `?` included (empty when there is
 * nothing to send). The gateway omits archived sources unless asked.
 */
export function sourceListSearch(options: { includeArchived: boolean }): string {
  return options.includeArchived ? "?include_archived=true" : "";
}

/** `GET /sources[?include_archived=true]`. */
export function listSources(
  options: { includeArchived: boolean },
  signal?: AbortSignal,
): Promise<SourceView[]> {
  return request<SourceView[]>(`/sources${sourceListSearch(options)}`, { signal });
}

/** `GET /sources/{uid}` — includes `next_run_at`. */
export function getSource(uid: string, signal?: AbortSignal): Promise<SourceView> {
  return request<SourceView>(sourcePath(uid), { signal });
}

/** `POST /sources`. */
export function createSource(body: CreateSourceBody): Promise<SourceView> {
  return request<SourceView>("/sources", { method: "POST", json: body });
}

/** `PATCH /sources/{uid}`. */
export function updateSource(uid: string, body: UpdateSourceBody): Promise<SourceView> {
  return request<SourceView>(sourcePath(uid), { method: "PATCH", json: body });
}

/** `DELETE /sources/{uid}` — removes the schedule, then the row. */
export function deleteSource(uid: string): Promise<void> {
  return request<void>(sourcePath(uid), { method: "DELETE" });
}

/** `POST /sources/{uid}/pause` or `/unpause`. */
export function setSourcePaused(uid: string, paused: boolean): Promise<void> {
  return request<void>(`${sourcePath(uid)}/${paused ? "pause" : "unpause"}`, { method: "POST" });
}

/** `POST /sources/{uid}/run` — 202; 422 on an archived source. */
export function triggerSourceRun(uid: string): Promise<TriggerRunResponse> {
  return request<TriggerRunResponse>(`${sourcePath(uid)}/run`, { method: "POST" });
}

/** Default page of run history. */
export const SOURCE_RUNS_LIMIT = 20;

/** `GET /sources/{uid}/runs?limit=` — newest first. */
export function listSourceRuns(
  uid: string,
  limit: number = SOURCE_RUNS_LIMIT,
  signal?: AbortSignal,
): Promise<RunRecord[]> {
  const query = new URLSearchParams({ limit: String(limit) });
  return request<RunRecord[]>(`${sourcePath(uid)}/runs?${query.toString()}`, { signal });
}

// ---------------------------------------------------------------------------
// Query bindings
// ---------------------------------------------------------------------------

/** Cache keys for everything source-shaped. */
export const sourceQueryKeys = {
  all: ["sources"] as const,
  list: (includeArchived: boolean) => ["sources", "list", { includeArchived }] as const,
  detail: (uid: string) => ["sources", "detail", uid] as const,
  runs: (uid: string) => ["sources", "runs", uid] as const,
};

// ---------------------------------------------------------------------------
// Polling after "Run now"
// ---------------------------------------------------------------------------

/**
 * What the screens knew about a source when a run was triggered. `POST /run` is
 * asynchronous, so the run lands seconds later: the queries poll until what
 * they show differs from this, rather than refetching once, too early.
 */
export interface PendingRun {
  /** When the run was triggered (`Date.now()`). */
  since: number;
  /** Newest run id before the trigger, `null` when there was none. */
  lastRunId: string | null;
  /** The source's `last_run_at` before the trigger. */
  lastRunAt: string | null;
}

/** How often to poll while a triggered run has not landed. */
export const RUN_POLL_MS = 2_000;
/** Give up polling after this long; a slow run shows up on the next visit. */
export const RUN_POLL_MAX_MS = 120_000;

/** Kept outside `sourceQueryKeys.all` so invalidating sources never touches it. */
const pendingRunKey = (uid: string) => ["source-run-pending", uid] as const;

/**
 * Whether a query should keep polling: a run is pending, it has not shown up
 * yet (`landed` is false), and the window has not expired.
 */
export function shouldPollForRun(
  pending: PendingRun | undefined,
  landed: boolean,
  now: number,
): boolean {
  if (!pending || landed) return false;
  return now - pending.since <= RUN_POLL_MAX_MS;
}

/** A run is recorded when it finishes, so a new newest run id means it landed. */
export function runLandedInRuns(pending: PendingRun, runs: RunRecord[] | undefined): boolean {
  const newest = runs?.[0]?.run_id ?? null;
  return newest !== null && newest !== pending.lastRunId;
}

/** The source's `last_run_at` moved, so its detail or list row is up to date. */
export function runLandedInSource(
  pending: PendingRun,
  source: Pick<SourceView, "last_run_at"> | undefined,
): boolean {
  const current = source?.last_run_at ?? null;
  return current !== null && current !== pending.lastRunAt;
}

function pendingRun(
  queryClient: ReturnType<typeof useQueryClient>,
  uid: string,
): PendingRun | undefined {
  return queryClient.getQueryData<PendingRun>(pendingRunKey(uid));
}

/** Every source visible to the caller, archived ones only when asked. */
export function useSources(
  includeArchived: boolean = true,
): UseQueryResult<SourceView[], Error> {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: sourceQueryKeys.list(includeArchived),
    queryFn: ({ signal }) => listSources({ includeArchived }, signal),
    placeholderData: (previous) => previous,
    refetchInterval: (query) => {
      const now = Date.now();
      const waiting = (query.state.data ?? []).some((source) => {
        const pending = pendingRun(queryClient, source.uid);
        return shouldPollForRun(pending, pending ? runLandedInSource(pending, source) : true, now);
      });
      return waiting ? RUN_POLL_MS : false;
    },
  });
}

/** One source, with `next_run_at`. Disabled while `uid` is empty. */
export function useSource(uid: string | undefined): UseQueryResult<SourceView, Error> {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: sourceQueryKeys.detail(uid ?? ""),
    queryFn: ({ signal }) => getSource(uid as string, signal),
    enabled: Boolean(uid),
    refetchInterval: (query) => {
      const pending = uid ? pendingRun(queryClient, uid) : undefined;
      const landed = pending ? runLandedInSource(pending, query.state.data) : true;
      return shouldPollForRun(pending, landed, Date.now()) ? RUN_POLL_MS : false;
    },
  });
}

/** Recent runs of one source. Disabled while `uid` is empty. */
export function useSourceRuns(uid: string | undefined): UseQueryResult<RunRecord[], Error> {
  const queryClient = useQueryClient();
  return useQuery({
    queryKey: sourceQueryKeys.runs(uid ?? ""),
    queryFn: ({ signal }) => listSourceRuns(uid as string, SOURCE_RUNS_LIMIT, signal),
    enabled: Boolean(uid),
    refetchInterval: (query) => {
      const pending = uid ? pendingRun(queryClient, uid) : undefined;
      const landed = pending ? runLandedInRuns(pending, query.state.data) : true;
      return shouldPollForRun(pending, landed, Date.now()) ? RUN_POLL_MS : false;
    },
  });
}

/**
 * Refresh everything a change to one source can move: the list, its detail and
 * its runs. `sourceQueryKeys.all` is a prefix of all three.
 */
async function invalidateSources(
  queryClient: ReturnType<typeof useQueryClient>,
): Promise<void> {
  await queryClient.invalidateQueries({ queryKey: sourceQueryKeys.all });
}

/**
 * Create a source. A 422 (bad cron, unpinned pipeline, host outside the fetch
 * policy) is left to the form; anything else is a toast.
 */
export function useCreateSource(): UseMutationResult<SourceView, Error, CreateSourceBody> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateSourceBody) => createSource(body),
    onSuccess: async (saved) => {
      toast.success(`Source "${saved.uid}" created`, { description: saved.cron });
      await invalidateSources(queryClient);
    },
    onError: (error) => {
      if (isFormError(error)) return;
      toast.error("Could not create the source", { description: errorMessage(error) });
    },
  });
}

/** Update a source. Same failure contract as {@link useCreateSource}. */
export function useUpdateSource(): UseMutationResult<
  SourceView,
  Error,
  { uid: string; body: UpdateSourceBody }
> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ uid, body }) => updateSource(uid, body),
    onSuccess: async (saved) => {
      toast.success(`Source "${saved.uid}" saved`);
      await invalidateSources(queryClient);
    },
    onError: (error) => {
      if (isFormError(error)) return;
      toast.error("Could not save the source", { description: errorMessage(error) });
    },
  });
}

/** Delete a source. Drops its detail and runs, refreshes the list. */
export function useDeleteSource(): UseMutationResult<void, Error, string> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (uid: string) => deleteSource(uid),
    onSuccess: async (_result, uid) => {
      toast.success(`Source "${uid}" deleted`);
      queryClient.removeQueries({ queryKey: sourceQueryKeys.detail(uid) });
      queryClient.removeQueries({ queryKey: sourceQueryKeys.runs(uid) });
      await invalidateSources(queryClient);
    },
    onError: (error) => {
      toast.error("Could not delete the source", { description: errorMessage(error) });
    },
  });
}

/** Pause or resume a source's schedule. */
export function useSetSourcePaused(): UseMutationResult<
  void,
  Error,
  { uid: string; paused: boolean }
> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ uid, paused }) => setSourcePaused(uid, paused),
    onSuccess: async (_result, { uid, paused }) => {
      toast.success(paused ? `Source "${uid}" paused` : `Source "${uid}" resumed`);
      await invalidateSources(queryClient);
    },
    onError: (error, { paused }) => {
      toast.error(paused ? "Could not pause the source" : "Could not resume the source", {
        description: errorMessage(error),
      });
    },
  });
}

/**
 * Run a source now. The run is asynchronous (202): the trigger records a
 * {@link PendingRun}, so the source, its runs and the list poll until the run
 * lands instead of refetching once before it has finished.
 */
export function useTriggerSourceRun(): UseMutationResult<TriggerRunResponse, Error, string> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (uid: string) => triggerSourceRun(uid),
    onMutate: (uid) => {
      // Snapshot before the request, so what counts as "new" is what the
      // screens showed when the button was pressed.
      const runs = queryClient.getQueryData<RunRecord[]>(sourceQueryKeys.runs(uid));
      const detail = queryClient.getQueryData<SourceView>(sourceQueryKeys.detail(uid));
      const listed = queryClient
        .getQueriesData<SourceView[]>({ queryKey: ["sources", "list"] })
        .flatMap(([, rows]) => rows ?? [])
        .find((row) => row.uid === uid);
      const pending: PendingRun = {
        since: Date.now(),
        lastRunId: runs?.[0]?.run_id ?? null,
        lastRunAt: detail?.last_run_at ?? listed?.last_run_at ?? null,
      };
      queryClient.setQueryData(pendingRunKey(uid), pending);
    },
    onSuccess: async (_response, uid) => {
      toast.success(`Run of "${uid}" triggered`, {
        description: "It appears in the run history once the fetch finishes.",
      });
      await Promise.all([
        invalidateSources(queryClient),
        queryClient.invalidateQueries({ queryKey: jobQueryKeys.all }),
      ]);
    },
    onError: (error, uid) => {
      queryClient.removeQueries({ queryKey: pendingRunKey(uid) });
      toast.error("Could not trigger a run", { description: errorMessage(error) });
    },
  });
}
