"use client";

import { useState } from "react";

import { PageHeader } from "@/components/common/page-header";
import { CancelJobDialog } from "@/app/jobs/_components/cancel-job-dialog";
import { useSubmitIngest, type IngestAccepted } from "@/lib/api/ingest";
import { IngestForm } from "./_components/ingest-form";
import { SubmissionResult } from "./_components/submission-result";

/**
 * The debugging surface for a pipeline you just authored: send one thing in,
 * see which pipeline claimed it, which index it resolved to, and how far each
 * step got.
 */
export default function PlaygroundPage() {
  const submit = useSubmitIngest();
  const [accepted, setAccepted] = useState<IngestAccepted | undefined>(undefined);
  const [toCancel, setToCancel] = useState<string | undefined>(undefined);

  return (
    <>
      <PageHeader
        title="Playground"
        description="Send content through a pipeline and watch it run, step by step."
      />

      <div className="grid gap-6 p-4 lg:grid-cols-2">
        <section className="space-y-3">
          <h2 className="text-sm font-semibold">Submit</h2>
          <IngestForm
            pending={submit.isPending}
            onSubmit={async (state) => {
              const result = await submit.mutateAsync(state);
              setAccepted(result);
              return result;
            }}
          />
        </section>

        <section className="space-y-3">
          <h2 className="text-sm font-semibold">Result</h2>
          {accepted ? (
            <SubmissionResult
              key={accepted.job_id}
              accepted={accepted}
              onCancelJob={setToCancel}
              onReset={() => setAccepted(undefined)}
            />
          ) : (
            <p className="rounded-md border border-dashed p-8 text-center text-sm text-muted-foreground">
              Nothing submitted yet. The pipeline that auto-routing picks and the index it resolves
              show up here.
            </p>
          )}
        </section>
      </div>

      <CancelJobDialog
        jobId={toCancel}
        open={toCancel !== undefined}
        onOpenChange={(open) => {
          if (!open) setToCancel(undefined);
        }}
      />
    </>
  );
}
