"use client";

/**
 * Ingest API for the Playground: what to submit, how to submit it, and the pure
 * builder that decides between the two request shapes the gateway accepts.
 *
 * `POST /ingest` speaks either `multipart/form-data` (a `file` part, plus
 * optional `index` / `pipeline` fields) or `application/json`
 * (`{"documents":[…]}` / `{"url":"…"}`), and `POST /ingest/pipeline/{name}`
 * takes the same bodies while skipping MIME routing. `request()` in `./client`
 * only speaks JSON, so the upload path calls `fetch` directly here and rebuilds
 * the same `{error, code}` → `ApiError` translation.
 *
 * Shapes verified against `crates/gateway/src/handlers/ingest.rs`,
 * `handlers/pipeline.rs` and `crates/gateway/src/extract.rs`.
 */
import { useMutation, useQueryClient, type UseMutationResult } from "@tanstack/react-query";
import { toast } from "sonner";

import { API_BASE_URL, ApiError, request } from "./client";
import { errorMessage } from "./hooks";
import { jobQueryKeys } from "./jobs";
import type { ApiErrorBody, JsonObject } from "./types";

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/**
 * 202 body of every ingest route.
 *
 * `status` is the string `"queued"` on the wire; it is typed loosely because
 * the gateway builds it by hand rather than serializing a `JobStatus`.
 */
export interface IngestAccepted {
  job_id: string;
  /** uid of the pipeline that auto-routing picked, or the one that was forced. */
  pipeline_used: string;
  /** Fully resolved Meilisearch index the documents will land in. */
  target_index: string;
  status: string;
}

// ---------------------------------------------------------------------------
// Form state → request plan (pure, unit-tested in ./ingest.test.ts)
// ---------------------------------------------------------------------------

/** The three tabs of the Playground. */
export type IngestSourceKind = "file" | "url" | "documents";

/** What the user is submitting. */
export type IngestSource =
  | { kind: "file"; file: File }
  | { kind: "url"; url: string }
  | { kind: "documents"; documents: JsonObject[] };

/** Everything the Playground form holds once validated. */
export interface IngestFormState {
  source: IngestSource;
  /** Target index override. Empty/absent leaves the gateway's resolution alone. */
  index?: string;
  /** Force a pipeline uid instead of MIME/filename routing. */
  pipeline?: string;
}

/**
 * A request ready to be sent: which path, and which of the two body encodings.
 *
 * Splitting the decision out of the submit call is what makes it testable —
 * the awkward part of this API is that "same endpoint, two content types" is
 * decided purely by what the user picked in the UI.
 */
export type IngestPlan =
  | {
      encoding: "multipart";
      path: string;
      /** The `file` part. */
      file: File;
      /** Extra text fields sent alongside it. */
      fields: Record<string, string>;
    }
  | { encoding: "json"; path: string; body: JsonObject };

/**
 * Path for a submission: the explicit-pipeline route when a pipeline is forced,
 * `/ingest` otherwise. `?index=` beats every other way of naming the index, so
 * it is where the override goes in both cases.
 */
export function ingestPath(state: IngestFormState): string {
  const pipeline = state.pipeline?.trim();
  const index = state.index?.trim();
  const base = pipeline
    ? `/ingest/pipeline/${encodeURIComponent(pipeline)}`
    : "/ingest";
  return index ? `${base}?index=${encodeURIComponent(index)}` : base;
}

/**
 * Decide multipart vs JSON from the form state.
 *
 * A file always goes multipart (there is no other way to send bytes); a URL and
 * inline documents always go JSON. `index` and `pipeline` ride in the path, so
 * the multipart `fields` stay empty unless a caller adds to them — they are kept
 * in the plan because the gateway also accepts them as parts, and the Playground
 * shows the user exactly what was sent.
 */
export function buildIngestPlan(state: IngestFormState): IngestPlan {
  const path = ingestPath(state);
  if (state.source.kind === "file") {
    return { encoding: "multipart", path, file: state.source.file, fields: {} };
  }
  if (state.source.kind === "url") {
    return { encoding: "json", path, body: { url: state.source.url.trim() } };
  }
  return { encoding: "json", path, body: { documents: state.source.documents } };
}

/** Outcome of reading the JSON documents textarea. */
export type DocumentsParse =
  | { ok: true; documents: JsonObject[] }
  | { ok: false; error: string };

function isJsonObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Parse the "JSON documents" tab.
 *
 * The gateway accepts a bare array, a `{"documents":[…]}` envelope or a single
 * object; all three are normalized to an array here so the caller only ever
 * deals with one shape.
 */
export function parseDocumentsInput(text: string): DocumentsParse {
  const trimmed = text.trim();
  if (!trimmed) return { ok: false, error: "Paste at least one document." };

  let parsed: unknown;
  try {
    parsed = JSON.parse(trimmed);
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : "invalid JSON" };
  }

  const raw = isJsonObject(parsed) && "documents" in parsed ? parsed.documents : parsed;
  const list = Array.isArray(raw) ? raw : [raw];
  if (list.length === 0) return { ok: false, error: "`documents` must not be empty." };
  if (!list.every(isJsonObject)) {
    return { ok: false, error: "Every document must be a JSON object." };
  }
  return { ok: true, documents: list };
}

/**
 * Uploads above this stop travelling inline: the gateway stages them to the
 * blob store and hands the workers a `ContentRef`. Nothing breaks, it is just
 * slower — so the UI warns and does not block.
 */
export const LARGE_UPLOAD_BYTES = 50 * 1024 * 1024;

/** Whether to warn about a staged upload. */
export function isLargeUpload(bytes: number): boolean {
  return bytes > LARGE_UPLOAD_BYTES;
}

// ---------------------------------------------------------------------------
// Submission
// ---------------------------------------------------------------------------

function isApiErrorBody(value: unknown): value is ApiErrorBody {
  if (typeof value !== "object" || value === null) return false;
  const body = value as Record<string, unknown>;
  return typeof body.error === "string" && typeof body.code === "string";
}

/**
 * Same failure translation as `request()`, for the one call that cannot use it.
 * Kept byte-for-byte compatible so the Playground reports upload errors exactly
 * like every other screen reports its own.
 */
async function toApiError(response: Response): Promise<ApiError> {
  let body: unknown;
  try {
    body = await response.json();
  } catch {
    body = undefined;
  }
  if (isApiErrorBody(body)) {
    return new ApiError(response.status, body.code, body.error);
  }
  return new ApiError(
    response.status,
    `http_${response.status}`,
    `${response.status} ${response.statusText || "request failed"}`,
  );
}

/** Send a multipart plan with a bare `fetch` (the browser sets the boundary). */
async function postMultipart(
  plan: Extract<IngestPlan, { encoding: "multipart" }>,
  signal?: AbortSignal,
): Promise<IngestAccepted> {
  const form = new FormData();
  form.append("file", plan.file, plan.file.name);
  for (const [key, value] of Object.entries(plan.fields)) {
    form.append(key, value);
  }
  const response = await fetch(`${API_BASE_URL}${plan.path}`, {
    method: "POST",
    body: form,
    signal,
  });
  if (!response.ok) throw await toApiError(response);
  return (await response.json()) as IngestAccepted;
}

/** Execute a plan. Returns the 202 body: pipeline picked and index resolved. */
export function submitIngest(plan: IngestPlan, signal?: AbortSignal): Promise<IngestAccepted> {
  if (plan.encoding === "multipart") {
    return postMultipart(plan, signal);
  }
  return request<IngestAccepted>(plan.path, { method: "POST", json: plan.body, signal });
}

// ---------------------------------------------------------------------------
// Query bindings
// ---------------------------------------------------------------------------

/**
 * Cache keys for ingest.
 *
 * Submitting is a mutation and caches nothing of its own; the key that matters
 * is the jobs list it invalidates, which lives in `./jobs`.
 */
export const ingestQueryKeys = {
  all: ["ingest"] as const,
  submission: (path: string) => ["ingest", "submission", path] as const,
};

/**
 * Submit to the gateway and report what it decided.
 *
 * The success toast names the pipeline and the index because that is the whole
 * question the Playground exists to answer. The jobs list is invalidated so the
 * new job shows up on `/jobs` without a reload.
 */
export function useSubmitIngest(): UseMutationResult<IngestAccepted, Error, IngestFormState> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (state: IngestFormState) => submitIngest(buildIngestPlan(state)),
    onSuccess: async (accepted) => {
      toast.success(`Routed to ${accepted.pipeline_used}`, {
        description: `Indexing into "${accepted.target_index}".`,
      });
      await queryClient.invalidateQueries({ queryKey: jobQueryKeys.all });
    },
    onError: (error) => {
      toast.error("Ingestion was refused", { description: errorMessage(error) });
    },
  });
}
