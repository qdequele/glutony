"use client";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { useDeleteSource } from "@/lib/api/sources";

export function DeleteSourceDialog({
  uid,
  open,
  onOpenChange,
  onDeleted,
}: {
  uid: string | undefined;
  open: boolean;
  onOpenChange: (open: boolean) => void;
  /** Called after a successful delete, e.g. to leave a detail page. */
  onDeleted?: () => void;
}) {
  const remove = useDeleteSource();

  async function confirm() {
    if (!uid) return;
    try {
      await remove.mutateAsync(uid);
      onOpenChange(false);
      onDeleted?.();
    } catch {
      // The hook already reported it through a toast.
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <DialogHeader>
          <DialogTitle>Delete source</DialogTitle>
          <DialogDescription>
            <span className="font-mono">{uid}</span>, its schedule, its run history and its stored
            credential will be removed. Jobs it already started keep running, and their
            documents stay in the index.
          </DialogDescription>
        </DialogHeader>
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
