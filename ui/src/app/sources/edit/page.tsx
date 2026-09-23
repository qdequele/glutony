"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { Suspense } from "react";
import { ArrowLeft } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ApiError } from "@/lib/api/client";
import { errorMessage } from "@/lib/api/hooks";
import { isArchived, useSource } from "@/lib/api/sources";
import { NotEnabled } from "../_components/not-enabled";
import { SourceForm } from "../_components/source-form";
import { SOURCES_HREF, sourceDetailHref } from "../routes";

function FormSkeleton() {
  return (
    <div className="mx-auto max-w-3xl space-y-4 p-4">
      {Array.from({ length: 4 }, (_, index) => (
        <Skeleton key={index} className="h-32 w-full" />
      ))}
    </div>
  );
}

function EditSource() {
  const uid = useSearchParams().get("uid") ?? "";
  const source = useSource(uid || undefined);

  const back = (
    <Button asChild variant="ghost" size="sm">
      <Link href={uid ? sourceDetailHref(uid) : SOURCES_HREF}>
        <ArrowLeft aria-hidden />
        {uid ? "Back to the source" : "All sources"}
      </Link>
    </Button>
  );

  if (!uid) {
    return (
      <>
        <PageHeader title="Edit source" actions={back} />
        <div className="p-4">
          <Alert variant="destructive">
            <AlertTitle>No source selected</AlertTitle>
            <AlertDescription>
              This page needs a <span className="font-mono">?uid=</span> parameter.
            </AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  if (source.isPending) {
    return (
      <>
        <PageHeader title={`Edit ${uid}`} actions={back} />
        <FormSkeleton />
      </>
    );
  }

  if (source.error) {
    const notConfigured = source.error instanceof ApiError && source.error.isNotConfigured;
    return (
      <>
        <PageHeader title={`Edit ${uid}`} actions={back} />
        <div className="p-4">
          {notConfigured ? (
            <NotEnabled feature="Scheduled sources" />
          ) : (
            <Alert variant="destructive">
              <AlertTitle>Could not load {uid}</AlertTitle>
              <AlertDescription>{errorMessage(source.error)}</AlertDescription>
            </Alert>
          )}
        </div>
      </>
    );
  }

  if (isArchived(source.data)) {
    return (
      <>
        <PageHeader title={`Edit ${uid}`} actions={back} />
        <div className="p-4">
          <Alert>
            <AlertTitle>This source is archived</AlertTitle>
            <AlertDescription>
              Its pipeline <span className="font-mono">{source.data.pipeline}</span> was deleted,
              and its schedule with it. An archived source cannot be edited back to life: delete
              it and create a new one.
            </AlertDescription>
          </Alert>
        </div>
      </>
    );
  }

  return (
    <>
      <PageHeader
        title={`Edit ${uid}`}
        description="Secrets are write-only: leave them empty to keep what is stored."
        actions={back}
      />
      <div className="mx-auto max-w-3xl">
        <SourceForm key={uid} mode="edit" stored={source.data} />
      </div>
    </>
  );
}

export default function EditSourcePage() {
  // `useSearchParams` needs a Suspense boundary to prerender into a static file.
  return (
    <Suspense fallback={<FormSkeleton />}>
      <EditSource />
    </Suspense>
  );
}
