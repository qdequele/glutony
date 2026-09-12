"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { errorMessage, usePipeline } from "@/lib/api/hooks";
import { pipelineToDraft } from "@/lib/pipeline/draft";
import { EditorSkeleton } from "../_components/editor-skeleton";
import { PipelineEditor } from "../_components/pipeline-editor";
import { PIPELINES_HREF } from "../routes";

function EditPipeline() {
  const uid = useSearchParams().get("uid") ?? "";
  const pipeline = usePipeline(uid || undefined);

  if (!uid) {
    return (
      <>
        <PageHeader title="Pipeline" />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>No pipeline selected</AlertTitle>
            <AlertDescription>
              This page needs a <span className="font-mono">?uid=</span> parameter.
              <Button asChild variant="link" size="sm">
                <Link href={PIPELINES_HREF}>Back to the list</Link>
              </Button>
            </AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  if (pipeline.isPending) {
    return (
      <>
        <PageHeader title={uid} />
        <EditorSkeleton />
      </>
    );
  }

  if (pipeline.error) {
    return (
      <>
        <PageHeader title={uid} />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>Could not load {uid}</AlertTitle>
            <AlertDescription>{errorMessage(pipeline.error)}</AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  return (
    <PipelineEditor
      key={uid}
      mode="edit"
      initialDraft={pipelineToDraft(pipeline.data)}
      stored={{
        version: pipeline.data.version,
        builtin: pipeline.data.builtin,
        project_id: pipeline.data.project_id,
      }}
    />
  );
}

export default function EditPipelinePage() {
  return (
    <Suspense fallback={<EditorSkeleton />}>
      <EditPipeline />
    </Suspense>
  );
}
