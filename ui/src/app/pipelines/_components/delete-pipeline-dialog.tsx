"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useDeletePipeline } from "@/lib/api/hooks";

export function DeletePipelineDialog({
  uid,
  open,
  onOpenChange,
}: {
  uid: string | undefined;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const remove = useDeletePipeline();
  const [pending, setPending] = useState(false);

  async function confirm() {
    if (!uid) return;
    setPending(true);
    try {
      await remove.mutateAsync(uid);
      onOpenChange(false);
    } catch {
      // The hook already reported it through a toast.
    } finally {
      setPending(false);
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Delete pipeline</DialogTitle>
          <DialogDescription>
            <span className="font-mono">{uid}</span> will be removed. Jobs already running keep the
            snapshot they started with.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={pending}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={confirm} disabled={pending}>
            {pending ? "Deleting…" : "Delete"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
