import { Ban, CheckCircle2, CircleDashed, Loader2, XCircle } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { cn } from "@/lib/utils";
import type { JobStatus } from "@/lib/api/types";

/**
 * Colour and icon per `JobStatus`.
 *
 * The badge variants alone do not carry enough meaning here (there is no
 * "success" variant), so the two positive/neutral states get explicit tokens on
 * top of the `outline` variant while failure reuses `destructive`.
 */
const STYLES: Record<JobStatus, { className: string; icon: typeof CheckCircle2; spin?: boolean }> =
  {
    queued: {
      className: "border-border text-muted-foreground",
      icon: CircleDashed,
    },
    running: {
      className: "border-blue-500/40 bg-blue-500/10 text-blue-700 dark:text-blue-300",
      icon: Loader2,
      spin: true,
    },
    succeeded: {
      className: "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
      icon: CheckCircle2,
    },
    failed: {
      className: "border-destructive/40 bg-destructive/10 text-destructive",
      icon: XCircle,
    },
    cancelled: {
      className: "border-amber-500/40 bg-amber-500/10 text-amber-700 dark:text-amber-300",
      icon: Ban,
    },
  };

export function JobStatusBadge({
  status,
  className,
}: {
  status: JobStatus;
  className?: string;
}) {
  const { className: tone, icon: Icon, spin } = STYLES[status];
  return (
    <Badge variant="outline" className={cn("gap-1 capitalize", tone, className)}>
      <Icon aria-hidden className={cn(spin && "animate-spin")} />
      {status}
    </Badge>
  );
}
