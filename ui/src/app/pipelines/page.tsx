"use client";

import Link from "next/link";
import { Plus } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, usePipelines } from "@/lib/api/hooks";
import { PipelineTable } from "./_components/pipeline-table";
import { newPipelineHref } from "./routes";

export default function PipelinesPage() {
  const { data, isPending, error } = usePipelines();

  return (
    <>
      <PageHeader
        title="Pipelines"
        description="Built-in pipelines are read-only; clone one to start from it."
        actions={
          <Button asChild size="sm">
            <Link href={newPipelineHref()}>
              <Plus aria-hidden />
              New pipeline
            </Link>
          </Button>
        }
      />

      <div className="p-4">
        {isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 6 }, (_, index) => (
              <Skeleton key={index} className="h-9 w-full" />
            ))}
          </div>
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load pipelines</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : data.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No pipelines yet.
          </p>
        ) : (
          <div className="overflow-hidden rounded-md border">
            <PipelineTable pipelines={data} />
          </div>
        )}
      </div>
    </>
  );
}
