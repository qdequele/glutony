import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
import { describeCron } from "../_lib/cron";
import { formatNextRun, formatPast, toDate } from "../_lib/time";

/** "Every day at 00:30" over "30 0 * * * · UTC". */
export function ScheduleSummary({
  cron,
  timezone,
  className,
}: {
  cron: string;
  timezone: string;
  className?: string;
}) {
  const description = describeCron(cron);
  return (
    <div className={cn("min-w-0", className)}>
      <div className="text-sm">{description}</div>
      {description !== cron.trim() ? (
        <div className="truncate font-mono text-[11px] text-muted-foreground">
          {cron} · {timezone}
        </div>
      ) : (
        <div className="truncate font-mono text-[11px] text-muted-foreground">{timezone}</div>
      )}
    </div>
  );
}

/** A relative timestamp with the absolute one behind a tooltip. */
export function RelativeTimestamp({
  value,
  now,
  kind,
}: {
  value: string | undefined;
  now: Date;
  /** `next` reads "in 5 minutes" / "due now"; `past` reads "5 minutes ago" / "just now". */
  kind: "next" | "past";
}) {
  const date = toDate(value);
  if (!date) return <span className="text-muted-foreground">—</span>;
  const label = kind === "next" ? formatNextRun(date, now) : formatPast(date, now);
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="whitespace-nowrap">{label}</span>
      </TooltipTrigger>
      <TooltipContent>{date.toLocaleString()}</TooltipContent>
    </Tooltip>
  );
}
