"use client";

import { useSearchParams } from "next/navigation";
import { Suspense } from "react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { errorMessage, usePipeline } from "@/lib/api/hooks";
import { cloneDraft, emptyDraft, pipelineToDraft } from "@/lib/pipeline/draft";
import { EditorSkeleton } from "../_components/editor-skeleton";
import { PipelineEditor } from "../_components/pipeline-editor";

function NewPipeline() {
  // `?from=<uid>` seeds the draft from an existing pipeline (the "Clone" action).
  const cloneFrom = useSearchParams().get("from") ?? undefined;
  const source = usePipeline(cloneFrom);

  if (cloneFrom && source.isPending) {
    return (
      <>
        <PageHeader title="New pipeline" description={`Cloning ${cloneFrom}…`} />
        <EditorSkeleton />
      </>
    );
  }
  if (cloneFrom && source.error) {
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

  const initialDraft = source.data ? cloneDraft(pipelineToDraft(source.data)) : emptyDraft();

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
