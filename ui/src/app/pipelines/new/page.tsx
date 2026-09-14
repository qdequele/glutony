"use client";

import { useSearchParams } from "next/navigation";
import { Suspense } from "react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { errorMessage, useCatalog, usePipeline } from "@/lib/api/hooks";
import { cloneDraft, emptyDraft, pipelineToDraft } from "@/lib/pipeline/draft";
import { EditorSkeleton } from "../_components/editor-skeleton";
import { PipelineEditor } from "../_components/pipeline-editor";

function NewPipeline() {
  // `?from=<uid>` seeds the draft from an existing pipeline or a catalog
  // template (the "Clone" action).
  const cloneFrom = useSearchParams().get("from") ?? undefined;
  const catalog = useCatalog();

  // Curated templates carry their definition inline; a `builtin.*` uid does not,
  // and is fetched. Waiting for the catalog before enabling the fetch is what
  // stops a template flashing a 404 on the way through.
  const template = cloneFrom
    ? catalog.data?.workflows.find((workflow) => workflow.uid === cloneFrom)?.definition
    : undefined;
  const source = usePipeline(!catalog.isPending && !template ? cloneFrom : undefined);

  const definition = template ?? source.data;
  // A disabled query reports `isPending` forever, so the template case must be
  // excluded explicitly rather than relying on `source.isPending` alone.
  const loading = Boolean(cloneFrom) && (catalog.isPending || (!template && source.isPending));
  const failed = !template && source.error;

  if (loading) {
    return (
      <>
        <PageHeader title="New pipeline" description={`Cloning ${cloneFrom}…`} />
        <EditorSkeleton />
      </>
    );
  }
  if (cloneFrom && failed) {
    return (
      <>
        <PageHeader title="New pipeline" />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>Could not load {cloneFrom}</AlertTitle>
            <AlertDescription>{errorMessage(source.error)}</AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  const initialDraft = definition ? cloneDraft(pipelineToDraft(definition)) : emptyDraft();

  return <PipelineEditor key={cloneFrom ?? "blank"} mode="create" initialDraft={initialDraft} />;
}

export default function NewPipelinePage() {
  // `useSearchParams` needs a Suspense boundary to prerender into a static file.
  return (
    <Suspense fallback={<EditorSkeleton />}>
      <NewPipeline />
    </Suspense>
  );
}
