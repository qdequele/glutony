"use client";

import { useState } from "react";
import { Plus } from "lucide-react";

import { NotEnabled } from "@/app/sources/_components/not-enabled";
import { PageHeader } from "@/components/common/page-header";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { ApiError } from "@/lib/api/client";
import { useConnections, type ConnectionView } from "@/lib/api/connections";
import { errorMessage } from "@/lib/api/hooks";
import { ConnectionDialog } from "./_components/connection-dialog";
import { ConnectionsTable } from "./_components/connections-table";
import { DeleteConnectionDialog } from "./_components/delete-connection-dialog";

export default function ConnectionsPage() {
  const { data, isPending, error } = useConnections();
  // The dialog's target outlives its closing animation: only `dialogOpen` flips.
  const [target, setTarget] = useState<ConnectionView | undefined>(undefined);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [toDelete, setToDelete] = useState<string | undefined>(undefined);
  const notConfigured = error instanceof ApiError && error.isNotConfigured;

  function openDialog(connection: ConnectionView | undefined) {
    setTarget(connection);
    setDialogOpen(true);
  }

  return (
    <>
      <PageHeader
        title="Connections"
        description="Named Meilisearch destinations. A pipeline's meili_indexer step pins one with config.connection."
        actions={
          <Button size="sm" disabled={notConfigured} onClick={() => openDialog(undefined)}>
            <Plus aria-hidden />
            New connection
          </Button>
        }
      />

      <div className="p-4">
        {isPending ? (
          <div className="space-y-2">
            {Array.from({ length: 4 }, (_, index) => (
              <Skeleton key={index} className="h-9 w-full" />
            ))}
          </div>
        ) : notConfigured ? (
          <NotEnabled feature="Connections" />
        ) : error ? (
          <Alert variant="destructive">
            <AlertTitle>Could not load connections</AlertTitle>
            <AlertDescription>{errorMessage(error)}</AlertDescription>
          </Alert>
        ) : data.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No connection yet. Create one to let pipelines — and the scheduled sources that run
            them — write to a Meilisearch of their own.
          </p>
        ) : (
          <div className="overflow-hidden rounded-md border">
            <ConnectionsTable connections={data} onEdit={openDialog} onDelete={setToDelete} />
          </div>
        )}
      </div>

      <ConnectionDialog
        // Remount per target so the form's resolver and defaults match it.
        key={target?.uid ?? "new"}
        connection={target}
        open={dialogOpen}
        onOpenChange={setDialogOpen}
      />
      <DeleteConnectionDialog
        uid={toDelete}
        open={toDelete !== undefined}
        onOpenChange={(open) => {
          if (!open) setToDelete(undefined);
        }}
      />
    </>
  );
}
