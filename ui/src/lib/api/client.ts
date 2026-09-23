/**
 * Typed fetch wrapper around the meili-ingest gateway API.
 *
 * The UI is statically exported and embedded in the gateway binary, so the
 * default base URL is empty: every request is same-origin and no CORS is
 * involved. `NEXT_PUBLIC_API_BASE_URL` exists only for running `next dev`
 * against a gateway on another port.
 */
import type {
  ApiErrorBody,
  Catalog,
  PipelineDefinition,
  PluginManifest,
  ValidatePipelineOk,
  ValidationOutcome,
} from "./types";

/** Base URL for every request. Empty string = same origin (the embedded case). */
export const API_BASE_URL = process.env.NEXT_PUBLIC_API_BASE_URL ?? "";

/**
 * A non-2xx response from the gateway. `code` comes from the `{error, code}`
 * body when the gateway sent one, and falls back to `http_<status>`.
 */
export class ApiError extends Error {
  /** HTTP status code. */
  readonly status: number;
  /** Stable snake_case error code from the body. */
  readonly code: string;

  constructor(status: number, code: string, message: string) {
    super(message);
    this.name = "ApiError";
    this.status = status;
    this.code = code;
  }

  /** True when the endpoint itself is missing (not yet deployed). */
  get isNotFound(): boolean {
    return this.status === 404;
  }

  /** True when the gateway refused the write (built-in pipelines are read-only). */
  get isForbidden(): boolean {
    return this.status === 403;
  }

  /**
   * True when the feature is switched off on this deployment (501
   * `not_configured`) — for example sources and connections without a
   * `SOURCE_SECRET_KEY`. Screens render an explanatory state, not an error.
   */
  get isNotConfigured(): boolean {
    return this.status === 501 || this.code === "not_configured";
  }
}

/**
 * True for the failures a form should show inline rather than as a toast: a 422
 * (or 400) carries a readable message about a field — an unreachable host, a bad
 * key, a cron Temporal rejected.
 */
export function isFormError(error: unknown): error is ApiError {
  return error instanceof ApiError && (error.status === 422 || error.status === 400);
}

function isApiErrorBody(value: unknown): value is ApiErrorBody {
  if (typeof value !== "object" || value === null) return false;
  const body = value as Record<string, unknown>;
  return typeof body.error === "string" && typeof body.code === "string";
}

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

/** Options accepted by {@link request}. */
export interface RequestOptions {
  method?: "GET" | "POST" | "PATCH" | "DELETE";
  /** JSON body. Serialized and sent with `Content-Type: application/json`. */
  json?: unknown;
  signal?: AbortSignal;
}

/**
 * Typed `fetch` against the gateway.
 *
 * Exported so feature modules (jobs, usage, ...) can add their own endpoints in
 * their own files instead of all editing this one. Throws {@link ApiError} with the
 * gateway's `{error, code}` body when the response is not ok.
 */
export async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const { method = "GET", json, signal } = options;
  const response = await fetch(`${API_BASE_URL}${path}`, {
    method,
    signal,
    headers: json === undefined ? undefined : { "Content-Type": "application/json" },
    body: json === undefined ? undefined : JSON.stringify(json),
  });

  if (!response.ok) {
    throw await toApiError(response);
  }
  if (response.status === 204) {
    return undefined as T;
  }
  return (await response.json()) as T;
}

// ---------------------------------------------------------------------------
// Pipelines
// ---------------------------------------------------------------------------

/** `GET /pipelines` — built-ins, global user pipelines and the tenant's own. */
export function listPipelines(signal?: AbortSignal): Promise<PipelineDefinition[]> {
  return request<PipelineDefinition[]>("/pipelines", { signal });
}

/** `GET /pipelines/{uid}`. */
export function getPipeline(uid: string, signal?: AbortSignal): Promise<PipelineDefinition> {
  return request<PipelineDefinition>(`/pipelines/${encodeURIComponent(uid)}`, { signal });
}

/**
 * `POST /pipelines` — create or update. The gateway also accepts YAML, but the
 * editor always sends JSON: the YAML pane is the authoring surface, the parsed
 * definition is what travels.
 */
export function upsertPipeline(definition: PipelineDefinition): Promise<PipelineDefinition> {
  return request<PipelineDefinition>("/pipelines", { method: "POST", json: definition });
}

/** `DELETE /pipelines/{uid}` — 403 for `builtin.*`. */
export function deletePipeline(uid: string): Promise<void> {
  return request<void>(`/pipelines/${encodeURIComponent(uid)}`, { method: "DELETE" });
}

/**
 * `POST /pipelines/validate` — authoritative validation, no persistence.
 *
 * The endpoint is being added separately; when it is missing the gateway
 * answers 404 (unknown route) or 405, and we report `unsupported` so the
 * editor keeps working on client-side validation alone.
 */
export async function validatePipeline(
  definition: PipelineDefinition,
  signal?: AbortSignal,
): Promise<ValidationOutcome> {
  try {
    const body = await request<ValidatePipelineOk>("/pipelines/validate", {
      method: "POST",
      json: definition,
      signal,
    });
    return { kind: "ok", order: body.order ?? [] };
  } catch (error) {
    if (!(error instanceof ApiError)) throw error;
    if (error.status === 404 || error.status === 405 || error.status === 501) {
      return { kind: "unsupported" };
    }
    if (error.status === 422 || error.status === 400) {
      return { kind: "invalid", error: error.message, code: error.code };
    }
    throw error;
  }
}

// ---------------------------------------------------------------------------
// Plugins
// ---------------------------------------------------------------------------

/** `GET /plugins` — manifests published by the worker pools at boot. */
export function listPlugins(signal?: AbortSignal): Promise<PluginManifest[]> {
  return request<PluginManifest[]>("/plugins", { signal });
}

// ---------------------------------------------------------------------------
// Catalog
// ---------------------------------------------------------------------------

/** `GET /catalog` — curated action and workflow copy, compiled into the gateway. */
export function listCatalog(signal?: AbortSignal): Promise<Catalog> {
  return request<Catalog>("/catalog", { signal });
}
