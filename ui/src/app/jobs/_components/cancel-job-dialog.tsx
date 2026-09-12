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
import { useCancelJob } from "@/lib/api/jobs";

/**
 * Confirmation for `POST /jobs/{id}/cancel`.
 *
 * The gateway answers 202 and the workflow winds down asynchronously, so the
 * hook invalidates rather than writing a status back: the next poll shows
 * `cancelled`.
 */
export function CancelJobDialog({
  jobId,
  open,
  onOpenChange,
}: {
  jobId: string | undefined;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}) {
  const cancel = useCancelJob();
  const [pending, setPending] = useState(false);

  async function confirm() {
    if (!jobId) return;
    setPending(true);
    try {
      await cancel.mutateAsync(jobId);
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
          <DialogTitle>Cancel this job?</DialogTitle>
          <DialogDescription>
            <span className="font-mono">{jobId}</span> stops after the step currently in flight.
            Documents already written to the index are not rolled back.
          </DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={pending}>
            Keep running
          </Button>
          <Button variant="destructive" onClick={confirm} disabled={pending}>
            {pending ? "Cancelling…" : "Cancel job"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
