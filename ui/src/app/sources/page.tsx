"use client";

import Link from "next/link";
import { useState } from "react";
import { Plus } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Switch } from "@/components/ui/switch";
import { ApiError } from "@/lib/api/client";
import { errorMessage } from "@/lib/api/hooks";
import { useSources } from "@/lib/api/sources";
import { NotEnabled } from "./_components/not-enabled";
import { SourcesTable } from "./_components/sources-table";
import { NEW_SOURCE_HREF } from "./routes";

export default function SourcesPage() {
  const [includeArchived, setIncludeArchived] = useState(true);
  const { data, isPending, error } = useSources(includeArchived);
  const notConfigured = error instanceof ApiError && error.isNotConfigured;

  return (
    <>
      <PageHeader
        title="Sources"
        description="Fetch a URL on a cron schedule and feed it to a pipeline. Unchanged content is skipped."
        actions={
          notConfigured ? (
            <Button size="sm" disabled>
              <Plus aria-hidden />
              New source
            </Button>
          ) : (
            <Button asChild size="sm">
              <Link href={NEW_SOURCE_HREF}>
                <Plus aria-hidden />
                New source
              </Link>
            </Button>
          )
        }
      />

      <div className="space-y-3 p-4">
        {notConfigured ? null : (
          <div className="flex items-center justify-end gap-2">
            <Switch
              id="sources-archived"
              checked={includeArchived}
              onCheckedChange={setIncludeArchived}
            />
            <Label htmlFor="sources-archived" className="text-xs font-normal">
              Show archived
            </Label>
          </div>
        )}

        {isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 5 }, (_, index) => (
              <Skeleton key={index} className="h-11 w-full" />
            ))}
          </div>
        ) : notConfigured ? (
          <NotEnabled feature="Scheduled sources" />
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load sources</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : data.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            {includeArchived
              ? "No source yet. A source needs a pipeline whose meili_indexer step names a connection."
              : "No active source. Turn on “Show archived” to see the ones whose pipeline was deleted."}
          </p>
        ) : (
          <div className="overflow-hidden rounded-md border">
            <SourcesTable sources={data} />
          </div>
        )}
      </div>
    </>
  );
}
