"use client";

/**
 * Connections API: named Meilisearch destinations a pipeline's `meili_indexer`
 * step can pin with `config.connection`.
 *
 * Mirrors `crates/gateway/src/connections.rs` (`ConnectionView`,
 * `CreateConnection`, `UpdateConnection`) and
 * `crates/gateway/src/handlers/connections.rs`.
 *
 * Every route answers `501 not_configured` until the deployment sets
 * `SOURCE_SECRET_KEY` (the API key is sealed with it). Screens check
 * `ApiError.isNotConfigured` and render an explanatory state.
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

// ---------------------------------------------------------------------------
// Wire types
// ---------------------------------------------------------------------------

/** `ConnectionView` — the API key is always masked as `****`. */
export interface ConnectionView {
  uid: string;
  name: string;
  project_id?: string;
  host: string;
  /** Always `"****"`: the key never leaves the gateway. */
  api_key: string;
  /** Pipelines whose `meili_indexer` names this connection (single reads only). */
  used_by?: string[];
  /** RFC 3339. */
  created_at: string;
  /** RFC 3339. */
  updated_at: string;
}

/** `POST /connections` body. */
export interface CreateConnectionBody {
  uid: string;
  name?: string;
  host: string;
  api_key: string;
}

/** `PATCH /connections/{uid}` body. Omitting `api_key` keeps the stored key. */
export interface UpdateConnectionBody {
  name?: string;
  host?: string;
  api_key?: string;
}

// ---------------------------------------------------------------------------
// Pure helpers (unit-tested in ./connections.test.ts)
// ---------------------------------------------------------------------------

/** Longest uid the gateway accepts. */
export const MAX_UID_LENGTH = 128;

/**
 * Mirrors `valid_uid` in `crates/gateway/src/connections.rs`, which sources
 * reuse: 1-128 characters of `[a-zA-Z0-9._-]`.
 */
export function isValidUid(uid: string): boolean {
  return uid.length > 0 && uid.length <= MAX_UID_LENGTH && /^[A-Za-z0-9._-]+$/.test(uid);
}

/** Message for an invalid uid, or `undefined` when it is fine. */
export function uidError(uid: string): string | undefined {
  if (uid.length === 0) return "Enter a uid.";
  if (isValidUid(uid)) return undefined;
  if (uid.length > MAX_UID_LENGTH) return `At most ${MAX_UID_LENGTH} characters.`;
  return "Only letters, digits, dots, dashes and underscores.";
}

/** A Meilisearch host must be an absolute http(s) URL; the gateway normalizes the rest. */
export function hostError(host: string): string | undefined {
  const trimmed = host.trim();
  if (trimmed.length === 0) return "Enter the Meilisearch URL.";
  try {
    const url = new URL(trimmed);
    if (url.protocol !== "http:" && url.protocol !== "https:") return "Must be an http(s) URL.";
  } catch {
    return "Must be an absolute URL, e.g. https://ms-1234.meilisearch.io";
  }
  return undefined;
}

// ---------------------------------------------------------------------------
// API calls
// ---------------------------------------------------------------------------

/** `GET /connections`. */
export function listConnections(signal?: AbortSignal): Promise<ConnectionView[]> {
  return request<ConnectionView[]>("/connections", { signal });
}

/** `GET /connections/{uid}` — includes `used_by`. */
export function getConnection(uid: string, signal?: AbortSignal): Promise<ConnectionView> {
  return request<ConnectionView>(`/connections/${encodeURIComponent(uid)}`, { signal });
}

/** `POST /connections` — the gateway probes `/health` and the key before storing. */
export function createConnection(body: CreateConnectionBody): Promise<ConnectionView> {
  return request<ConnectionView>("/connections", { method: "POST", json: body });
}

/** `PATCH /connections/{uid}`. */
export function updateConnection(
  uid: string,
  body: UpdateConnectionBody,
): Promise<ConnectionView> {
  return request<ConnectionView>(`/connections/${encodeURIComponent(uid)}`, {
    method: "PATCH",
    json: body,
  });
}

/** `DELETE /connections/{uid}` — never blocked, even when pipelines still name it. */
export function deleteConnection(uid: string): Promise<void> {
  return request<void>(`/connections/${encodeURIComponent(uid)}`, { method: "DELETE" });
}

// ---------------------------------------------------------------------------
// Query bindings
// ---------------------------------------------------------------------------

/** Cache keys for everything connection-shaped. */
export const connectionQueryKeys = {
  all: ["connections"] as const,
  detail: (uid: string) => ["connections", "detail", uid] as const,
};

/** Every connection visible to the caller. */
export function useConnections(): UseQueryResult<ConnectionView[], Error> {
  return useQuery({
    queryKey: connectionQueryKeys.all,
    queryFn: ({ signal }) => listConnections(signal),
  });
}

/** One connection, with `used_by`. Disabled while `uid` is empty. */
export function useConnection(uid: string | undefined): UseQueryResult<ConnectionView, Error> {
  return useQuery({
    queryKey: connectionQueryKeys.detail(uid ?? ""),
    queryFn: ({ signal }) => getConnection(uid as string, signal),
    enabled: Boolean(uid),
  });
}

/**
 * Create a connection. A 422 from the health probe is left to the form, which
 * shows it next to the host and key; anything else is a toast.
 */
export function useCreateConnection(): UseMutationResult<
  ConnectionView,
  Error,
  CreateConnectionBody
> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (body: CreateConnectionBody) => createConnection(body),
    onSuccess: async (saved) => {
      toast.success(`Connection "${saved.uid}" created`, { description: saved.host });
      await queryClient.invalidateQueries({ queryKey: connectionQueryKeys.all });
    },
    onError: (error) => {
      if (isFormError(error)) return;
      toast.error("Could not create the connection", { description: errorMessage(error) });
    },
  });
}

/** Update a connection. Same failure contract as {@link useCreateConnection}. */
export function useUpdateConnection(): UseMutationResult<
  ConnectionView,
  Error,
  { uid: string; body: UpdateConnectionBody }
> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: ({ uid, body }) => updateConnection(uid, body),
    onSuccess: async (saved) => {
      toast.success(`Connection "${saved.uid}" saved`);
      await queryClient.invalidateQueries({ queryKey: connectionQueryKeys.all });
    },
    onError: (error) => {
      if (isFormError(error)) return;
      toast.error("Could not save the connection", { description: errorMessage(error) });
    },
  });
}

/** Delete a connection. Invalidates the list and drops the detail entry. */
export function useDeleteConnection(): UseMutationResult<void, Error, string> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (uid: string) => deleteConnection(uid),
    onSuccess: async (_result, uid) => {
      toast.success(`Connection "${uid}" deleted`);
      queryClient.removeQueries({ queryKey: connectionQueryKeys.detail(uid) });
      await queryClient.invalidateQueries({ queryKey: connectionQueryKeys.all });
    },
    onError: (error) => {
      toast.error("Could not delete the connection", { description: errorMessage(error) });
    },
  });
}
