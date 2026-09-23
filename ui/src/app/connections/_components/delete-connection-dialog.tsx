"use client";

import Link from "next/link";
import { TriangleAlert } from "lucide-react";

import { editPipelineHref } from "@/app/pipelines/routes";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Skeleton } from "@/components/ui/skeleton";
import { useConnection, useDeleteConnection } from "@/lib/api/connections";
import { errorMessage } from "@/lib/api/hooks";

/**
 * Deleting a connection is never blocked. The single read carries `used_by`,
 * so the dialog fetches it and warns about the pipelines that will fail until
 * they are re-pointed.
 */
export function DeleteConnectionDialog({
  uid,
  open,
  onOpenChange,
}: {
  uid: string | undefined;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const detail = useConnection(open ? uid : undefined);
  const remove = useDeleteConnection();
  const usedBy = detail.data?.used_by ?? [];

  async function confirm() {
    if (!uid) return;
    try {
      await remove.mutateAsync(uid);
      onOpenChange(false);
    } catch {
      // The hook already reported it through a toast.
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Delete connection</DialogTitle>
          <DialogDescription>
            <span className="font-mono">{uid}</span> and its stored API key will be removed.
          </DialogDescription>
        </DialogHeader>

        {detail.isPending ? (
          <Skeleton className="h-12 w-full" />
        ) : detail.error ? (
          <p className="text-xs text-muted-foreground">
            Could not check which pipelines use it: {errorMessage(detail.error)}
          </p>
        ) : usedBy.length > 0 ? (
          <Alert variant="destructive">
            <TriangleAlert aria-hidden />
            <AlertTitle>
              {usedBy.length} pipeline{usedBy.length === 1 ? "" : "s"} still name
              {usedBy.length === 1 ? "s" : ""} this connection
            </AlertTitle>
            <AlertDescription>
              <p>They will fail at their indexing step until they are re-pointed.</p>
              <ul className="mt-1 flex flex-wrap gap-x-3 gap-y-1">
                {usedBy.map((pipeline) => (
                  <li key={pipeline}>
                    <Link
                      href={editPipelineHref(pipeline)}
                      className="font-mono text-xs underline underline-offset-2"
                    >
                      {pipeline}
                    </Link>
                  </li>
                ))}
              </ul>
            </AlertDescription>
          </Alert>
        ) : (
          <p className="text-xs text-muted-foreground">No pipeline names this connection.</p>
        )}

        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={remove.isPending}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={confirm} disabled={remove.isPending}>
            {remove.isPending ? "Deleting…" : "Delete"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
