"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { ArrowLeft } from "lucide-react";
import { Suspense, useMemo } from "react";

import { editPipelineHref } from "@/app/pipelines/routes";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePipelines, usePlugins } from "@/lib/api/hooks";
import { mergeActions, type MergedAction } from "@/lib/catalog/merge";
import { neighboursOf } from "@/lib/catalog/neighbours";
import { ConfigSchemaTable } from "../../_components/config-schema-table";
import { CopyBlock } from "../../_components/copy-block";
import { MARKETPLACE_ACTIONS_HREF, actionDetailHref } from "../../routes";

function NeighbourList({ title, actions }: { title: string; actions: MergedAction[] }) {
  return (
    <div>
      <h3 className="mb-2 text-xs font-medium text-muted-foreground">{title}</h3>
      {actions.length === 0 ? (
        <p className="text-sm text-muted-foreground">Nothing.</p>
      ) : (
        <div className="flex flex-wrap gap-1.5">
          {actions.map((action) => (
            <Link key={action.entry.plugin} href={actionDetailHref(action.entry.plugin)}>
              <Badge variant="outline" className="cursor-pointer font-mono hover:bg-muted">
                {action.entry.plugin}
              </Badge>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
}

function ActionDetail() {
  const plugin = useSearchParams().get("plugin") ?? "";
  const catalog = useCatalog();
  const plugins = usePlugins();
  const pipelines = usePipelines();

  const merged = useMemo(
    () => mergeActions(catalog.data?.actions ?? [], plugins.data ?? []),
    [catalog.data, plugins.data],
  );
  const action = merged.find((item) => item.entry.plugin === plugin);
  const neighbours = useMemo(
    () => (action ? neighboursOf(action, merged) : { canFollow: [], canFeed: [] }),
    [action, merged],
  );
  const usedBy = useMemo(
    () =>
      (pipelines.data ?? []).filter((pipeline) =>
        pipeline.steps.some((step) => step.plugin === plugin),
      ),
    [pipelines.data, plugin],
  );

  if (catalog.isPending) {
    return (
      <div className="space-y-3 p-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-32 w-full" />
      </div>
    );
  }

  if (catalog.error) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>Could not load the catalog</AlertTitle>
          <AlertDescription>{errorMessage(catalog.error)}</AlertDescription>
        </Alert>
      </div>
    );
  }

  if (!action) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>No such action</AlertTitle>
          <AlertDescription>
            The catalog has no entry for “{plugin}”.
          </AlertDescription>
        </Alert>
      </div>
    );
  }

  const { entry, manifest, registered, accepts, produces } = action;

  return (
    <div className="space-y-6 p-4">
      <div>
        <Button asChild size="sm" variant="ghost" className="-ml-2 mb-2">
          <Link href={MARKETPLACE_ACTIONS_HREF}>
            <ArrowLeft aria-hidden />
            All actions
          </Link>
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold tracking-tight">{entry.title}</h2>
          <Badge variant="outline" className="capitalize">
            {entry.category}
          </Badge>
          {manifest?.kind ? <Badge variant="outline">{manifest.kind}</Badge> : null}
          <Badge variant={registered ? "secondary" : "outline"}>
            {registered ? "Registered" : "Not deployed here"}
          </Badge>
        </div>
        <p className="mt-1 font-mono text-xs text-muted-foreground">{entry.plugin}</p>
        <p className="mt-3 max-w-2xl text-sm text-muted-foreground">{entry.summary}</p>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">What it is for</h3>
        <ul className="list-inside list-disc space-y-1 text-sm text-muted-foreground">
          {entry.use_cases.map((useCase) => (
            <li key={useCase}>{useCase}</li>
          ))}
        </ul>
      </div>

      <Separator />

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">Configuration</h3>
        {registered ? (
          <ConfigSchemaTable schema={manifest?.config_schema} />
        ) : (
          <p className="text-sm text-muted-foreground">
            No worker in this deployment has registered {entry.plugin}. Its
            configuration appears here once one does.
          </p>
        )}
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">Example step</h3>
        <CopyBlock text={entry.example_step} label={`Copy the ${entry.plugin} step`} />
      </div>

      <Separator />

      <div>
        <h3 className="mb-3 text-sm font-semibold tracking-tight">
          Fits in a pipeline
          <span className="ml-2 font-mono text-xs font-normal text-muted-foreground">
            {accepts.join(", ") || "nothing"} → {produces}
          </span>
        </h3>
        <div className="grid gap-4 sm:grid-cols-2">
          <NeighbourList title="Can run before it" actions={neighbours.canFollow} />
          <NeighbourList title="Can run after it" actions={neighbours.canFeed} />
        </div>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">Used by</h3>
        {usedBy.length === 0 ? (
          <p className="text-sm text-muted-foreground">No pipeline uses it yet.</p>
        ) : (
          <div className="flex flex-wrap gap-1.5">
            {usedBy.map((pipeline) => (
              <Link key={pipeline.uid} href={editPipelineHref(pipeline.uid)}>
                <Badge variant="outline" className="cursor-pointer font-mono hover:bg-muted">
                  {pipeline.uid}
                </Badge>
              </Link>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}

export default function ActionDetailPage() {
  // `useSearchParams` needs a Suspense boundary to prerender into a static file.
  return (
    <Suspense fallback={<Skeleton className="m-4 h-64" />}>
      <ActionDetail />
    </Suspense>
  );
}
