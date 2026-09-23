import { Archive, CheckCircle2, CircleDashed, Equal, Pause, XCircle } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import type { RunOutcome, SourceView } from "@/lib/api/sources";
import { cn } from "@/lib/utils";

/** Colour and icon per run outcome, in the tokens the job status badge uses. */
const OUTCOME_STYLES: Record<RunOutcome, { className: string; icon: typeof CheckCircle2 }> = {
  ingested: {
    className: "border-emerald-500/40 bg-emerald-500/10 text-emerald-700 dark:text-emerald-300",
    icon: CheckCircle2,
  },
  unchanged: {
    className: "border-border text-muted-foreground",
    icon: Equal,
  },
  failed: {
    className: "border-destructive/40 bg-destructive/10 text-destructive",
    icon: XCircle,
  },
};

/** Outcome of one run, or of the source's last run. `undefined` = never ran. */
export function RunOutcomeBadge({
  outcome,
  className,
}: {
  outcome: RunOutcome | undefined;
  className?: string;
}) {
  if (!outcome) {
    return (
      <Badge variant="outline" className={cn("gap-1 text-muted-foreground", className)}>
        <CircleDashed aria-hidden />
        never run
      </Badge>
    );
  }
  const { className: tone, icon: Icon } = OUTCOME_STYLES[outcome];
  return (
    <Badge variant="outline" className={cn("gap-1 capitalize", tone, className)}>
      <Icon aria-hidden />
      {outcome}
    </Badge>
  );
}

/** Archived or paused, when the source is either. Nothing for an active one. */
export function SourceStateBadge({ source }: { source: Pick<SourceView, "archived_at" | "paused"> }) {
  if (source.archived_at) {
    return (
      <Badge variant="secondary" className="gap-1 text-[10px]">
        <Archive aria-hidden />
        archived
      </Badge>
    );
  }
  if (source.paused) {
    return (
      <Badge
        variant="outline"
        className="gap-1 border-amber-500/40 bg-amber-500/10 text-[10px] text-amber-700 dark:text-amber-300"
      >
        <Pause aria-hidden />
        paused
      </Badge>
    );
  }
  return null;
}
