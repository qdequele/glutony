import Link from "next/link";
import { ChevronRight, Copy } from "lucide-react";

import { newPipelineHref } from "@/app/pipelines/routes";
import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardFooter, CardHeader, CardTitle } from "@/components/ui/card";
import type { PipelineDefinition, WorkflowEntry } from "@/lib/api/types";
import { workflowDetailHref } from "../routes";
import { TriggerLine } from "./trigger-line";

/**
 * The definition behind a catalog entry.
 *
 * `builtin.*` entries carry none: the deployed pipeline is authoritative, so it
 * is looked up in the live list. Curated templates carry theirs inline because
 * nothing has deployed them.
 */
export function definitionFor(
  entry: WorkflowEntry,
  pipelines: PipelineDefinition[],
): PipelineDefinition | undefined {
  return entry.definition ?? pipelines.find((pipeline) => pipeline.uid === entry.uid);
}

/** One workflow in the grid. */
export function WorkflowCard({
  entry,
  definition,
}: {
  entry: WorkflowEntry;
  definition: PipelineDefinition | undefined;
}) {
  const isBuiltin = entry.uid.startsWith("builtin.");

  return (
    <Card className="flex flex-col transition-colors hover:border-primary/40">
      <CardHeader>
        <CardTitle className="flex items-start justify-between gap-2">
          <Link href={workflowDetailHref(entry.uid)} className="hover:underline">
            {entry.title}
          </Link>
          <Badge variant={isBuiltin ? "secondary" : "outline"}>
            {isBuiltin ? "Built-in" : "Template"}
          </Badge>
        </CardTitle>
        <p className="font-mono text-xs text-muted-foreground">{entry.uid}</p>
      </CardHeader>
      <CardContent className="flex-1 space-y-3">
        <p className="text-sm text-muted-foreground">{entry.summary}</p>
        <TriggerLine trigger={definition?.trigger} />
        <div className="flex flex-wrap items-center gap-1">
          {(definition?.steps ?? []).map((step, index) => (
            <span key={step.id} className="flex items-center gap-1">
              {index > 0 ? (
                <ChevronRight className="size-3 text-muted-foreground" aria-hidden />
              ) : null}
              <Badge variant="outline" className="font-mono">
                {step.plugin}
              </Badge>
            </span>
          ))}
        </div>
      </CardContent>
      <CardFooter>
        <Button asChild size="sm" variant="outline">
          <Link href={newPipelineHref(entry.uid)}>
            <Copy aria-hidden />
            Clone into my pipelines
          </Link>
        </Button>
      </CardFooter>
    </Card>
  );
}
