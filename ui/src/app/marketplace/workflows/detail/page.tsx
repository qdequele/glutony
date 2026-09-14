"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { ArrowLeft, ChevronRight, Copy } from "lucide-react";
import { Suspense } from "react";

import { newPipelineHref } from "@/app/pipelines/routes";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePipelines } from "@/lib/api/hooks";
import { CopyBlock } from "../../_components/copy-block";
import { TriggerLine } from "../../_components/trigger-line";
import { definitionFor } from "../../_components/workflow-card";
import { MARKETPLACE_WORKFLOWS_HREF, actionDetailHref } from "../../routes";

function WorkflowDetail() {
  const uid = useSearchParams().get("uid") ?? "";
  const catalog = useCatalog();
  const pipelines = usePipelines();

  if (catalog.isPending) {
    return (
      <div className="space-y-3 p-4">
        <Skeleton className="h-8 w-64" />
        <Skeleton className="h-48 w-full" />
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

  const entry = catalog.data.workflows.find((item) => item.uid === uid);
  if (!entry) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>No such workflow</AlertTitle>
          <AlertDescription>The catalog has no entry for “{uid}”.</AlertDescription>
        </Alert>
      </div>
    );
  }

  const definition = definitionFor(entry, pipelines.data ?? []);
  const isBuiltin = entry.uid.startsWith("builtin.");

  return (
    <div className="space-y-6 p-4">
      <div>
        <Button asChild size="sm" variant="ghost" className="-ml-2 mb-2">
          <Link href={MARKETPLACE_WORKFLOWS_HREF}>
            <ArrowLeft aria-hidden />
            All workflows
          </Link>
        </Button>
        <div className="flex flex-wrap items-center gap-2">
          <h2 className="text-lg font-semibold tracking-tight">{entry.title}</h2>
          <Badge variant="outline" className="capitalize">
            {entry.category}
          </Badge>
          <Badge variant={isBuiltin ? "secondary" : "outline"}>
            {isBuiltin ? "Built-in" : "Template"}
          </Badge>
        </div>
        <p className="mt-1 font-mono text-xs text-muted-foreground">{entry.uid}</p>
        <p className="mt-3 max-w-2xl text-sm text-muted-foreground">{entry.summary}</p>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">When to use it</h3>
        <p className="max-w-2xl text-sm text-muted-foreground">{entry.when_to_use}</p>
      </div>

      <div>
        <h3 className="mb-2 text-sm font-semibold tracking-tight">How it starts</h3>
        {!definition && pipelines.isPending ? (
          <Skeleton className="h-4 w-48" />
        ) : (
          <>
            <TriggerLine trigger={definition?.trigger} />
            {definition?.trigger?.index_pattern ? (
              <p className="mt-1 text-xs text-muted-foreground">
                Writes to <code className="font-mono">{definition.trigger.index_pattern}</code>
              </p>
            ) : null}
          </>
        )}
      </div>

      <Separator />

      <div>
        <h3 className="mb-3 text-sm font-semibold tracking-tight">Steps</h3>
        {!definition && pipelines.isPending ? (
          <Skeleton className="h-24 w-full" />
        ) : !definition ? (
          // A builtin.* entry whose pipeline the gateway did not return: an
          // older gateway, or a list that failed to load.
          <p className="text-sm text-muted-foreground">
            This deployment did not return a definition for {entry.uid}.
          </p>
        ) : (
          <ol className="space-y-3">
            {definition.steps.map((step, index) => (
              <li key={step.id} className="rounded-md border p-3">
                <div className="flex flex-wrap items-center gap-2">
                  <span className="text-xs text-muted-foreground">{index + 1}</span>
                  <span className="font-mono text-sm">{step.id}</span>
                  <ChevronRight className="size-3 text-muted-foreground" aria-hidden />
                  <Link href={actionDetailHref(step.plugin)}>
                    <Badge variant="outline" className="cursor-pointer font-mono hover:bg-muted">
                      {step.plugin}
                    </Badge>
                  </Link>
                  {step.fan_out ? (
                    <Badge variant="outline" className="font-mono">
                      fan out {step.fan_out}
                    </Badge>
                  ) : null}
                  {step.depends_on?.length ? (
                    <span className="text-xs text-muted-foreground">
                      after {step.depends_on.join(", ")}
                    </span>
                  ) : null}
                </div>
                {step.config && Object.keys(step.config).length > 0 ? (
                  <pre className="mt-2 overflow-x-auto rounded bg-muted/40 p-2 text-xs">
                    <code>{JSON.stringify(step.config, null, 2)}</code>
                  </pre>
                ) : null}
              </li>
            ))}
          </ol>
        )}
      </div>

      {definition ? (
        <div>
          <h3 className="mb-2 text-sm font-semibold tracking-tight">Definition</h3>
          <CopyBlock
            text={JSON.stringify(definition, null, 2)}
            label={`Copy the ${entry.uid} definition`}
          />
        </div>
      ) : null}

      <Button asChild size="sm">
        <Link href={newPipelineHref(entry.uid)}>
          <Copy aria-hidden />
          Clone into my pipelines
        </Link>
      </Button>
    </div>
  );
}

export default function WorkflowDetailPage() {
  // `useSearchParams` needs a Suspense boundary to prerender into a static file.
  return (
    <Suspense fallback={<Skeleton className="m-4 h-64" />}>
      <WorkflowDetail />
    </Suspense>
  );
}
