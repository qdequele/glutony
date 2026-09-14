"use client";

/**
 * TanStack Query bindings for the gateway API.
 *
 * Conventions used across the whole admin UI:
 * - every server read goes through a hook here, never a bare `fetch`;
 * - every mutation invalidates the queries it can affect;
 * - every mutation reports through Sonner, on success *and* on failure.
 *
 * `queryKeys` is the single source of truth for cache keys — add to it rather
 * than inventing string arrays at call sites.
 */
import {
  useMutation,
  useQuery,
  useQueryClient,
  type UseMutationResult,
  type UseQueryResult,
} from "@tanstack/react-query";
import { toast } from "sonner";

import {
  ApiError,
  deletePipeline,
  getPipeline,
  listPipelines,
  listPlugins,
  listCatalog,
  upsertPipeline,
  validatePipeline,
} from "./client";
import type { Catalog, PipelineDefinition, PluginManifest, ValidationOutcome } from "./types";

/** Cache keys. Extend this object when you add an endpoint. */
export const queryKeys = {
  pipelines: ["pipelines"] as const,
  pipeline: (uid: string) => ["pipelines", uid] as const,
  plugins: ["plugins"] as const,
  catalog: ["catalog"] as const,
  pipelineValidation: (definition: unknown) => ["pipelines", "validate", definition] as const,
};

/** Human-readable message for anything thrown by the client. */
export function errorMessage(error: unknown): string {
  if (error instanceof ApiError) return error.message;
  if (error instanceof Error) return error.message;
  return "unexpected error";
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/** All pipelines visible to the caller. */
export function usePipelines(): UseQueryResult<PipelineDefinition[], Error> {
  return useQuery({
    queryKey: queryKeys.pipelines,
    queryFn: ({ signal }) => listPipelines(signal),
  });
}

/** Every registered plugin manifest. Rarely changes, so it is cached hard. */
export function usePlugins(): UseQueryResult<PluginManifest[], Error> {
  return useQuery({
    queryKey: queryKeys.plugins,
    queryFn: ({ signal }) => listPlugins(signal),
    staleTime: 5 * 60 * 1000,
  });
}

/**
 * The curated catalog. Compiled into the gateway, so it changes only on
 * redeploy — cached as hard as the plugin manifests.
 */
export function useCatalog(): UseQueryResult<Catalog, Error> {
  return useQuery({
    queryKey: queryKeys.catalog,
    queryFn: ({ signal }) => listCatalog(signal),
    staleTime: 5 * 60 * 1000,
  });
}

/** One pipeline by uid. Disabled while `uid` is empty (the "new pipeline" case). */
export function usePipeline(uid: string | undefined): UseQueryResult<PipelineDefinition, Error> {
  return useQuery({
    queryKey: queryKeys.pipeline(uid ?? ""),
    queryFn: ({ signal }) => getPipeline(uid as string, signal),
    enabled: Boolean(uid),
  });
}

/**
 * The tenant the gateway is answering for, if any.
 *
 * The API has no "current context" endpoint, so this is read off the pipelines
 * the gateway returned: behind Envoy, tenant-scoped pipelines carry the
 * `project_id`. Standalone deployments report nothing, which is correct.
 */
export function useProjectId(): string | undefined {
  const { data } = usePipelines();
  return data?.find((pipeline) => Boolean(pipeline.project_id))?.project_id;
}

/**
 * Debounced authoritative validation. `definition` should already be debounced
 * by the caller; pass `undefined` to skip (for example while the client-side
 * rules are already failing).
 */
export function useValidatePipeline(
  definition: PipelineDefinition | undefined,
): UseQueryResult<ValidationOutcome, Error> {
  return useQuery({
    queryKey: queryKeys.pipelineValidation(definition),
    queryFn: ({ signal }) => validatePipeline(definition as PipelineDefinition, signal),
    enabled: definition !== undefined,
    retry: false,
    staleTime: 30 * 1000,
  });
}

// ---------------------------------------------------------------------------
// Mutations
// ---------------------------------------------------------------------------

/** Create or update a pipeline. Invalidates the list and that pipeline. */
export function useUpsertPipeline(): UseMutationResult<
  PipelineDefinition,
  Error,
  PipelineDefinition
> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (definition: PipelineDefinition) => upsertPipeline(definition),
    onSuccess: async (saved) => {
      toast.success(`Pipeline "${saved.uid}" saved`, {
        description: `version ${saved.version ?? 1} · ${saved.steps.length} step${
          saved.steps.length === 1 ? "" : "s"
        }`,
      });
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: queryKeys.pipelines }),
        queryClient.invalidateQueries({ queryKey: queryKeys.pipeline(saved.uid) }),
      ]);
    },
    onError: (error) => {
      toast.error("Could not save the pipeline", { description: errorMessage(error) });
    },
  });
}

/** Delete a user pipeline. Invalidates the list and drops the detail entry. */
export function useDeletePipeline(): UseMutationResult<void, Error, string> {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: (uid: string) => deletePipeline(uid),
    onSuccess: async (_result, uid) => {
      toast.success(`Pipeline "${uid}" deleted`);
      queryClient.removeQueries({ queryKey: queryKeys.pipeline(uid) });
      await queryClient.invalidateQueries({ queryKey: queryKeys.pipelines });
    },
    onError: (error) => {
      toast.error("Could not delete the pipeline", { description: errorMessage(error) });
    },
  });
}
