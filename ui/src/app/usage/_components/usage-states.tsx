"use client";

/**
 * The two states that are answers rather than failures.
 *
 * `NotConfigured` is what a self-hosted deployment sees by default: the gateway
 * answers `501 not_configured` until it has a Tinybird read token. Rendering it
 * as an error would tell operators something is broken when nothing is.
 *
 * `NoData` is the other one: analytics is wired up, the range just contains no
 * ingestion. Same shape, very different message — conflating them would send
 * someone hunting for a token they already set.
 */
import type { ReactNode } from "react";
import type { LucideIcon } from "lucide-react";
import { BarChart3, Settings2 } from "lucide-react";

import { Skeleton } from "@/components/ui/skeleton";
import { SummaryTilesSkeleton } from "./summary-tiles";

function EmptyState({
  icon: Icon,
  title,
  children,
}: {
  icon: LucideIcon;
  title: string;
  children: ReactNode;
}) {
  return (
    <div className="flex flex-col items-center gap-3 rounded-lg border border-dashed px-6 py-16 text-center">
      <Icon className="size-6 text-muted-foreground" aria-hidden />
      <h2 className="text-sm font-medium">{title}</h2>
      <div className="max-w-prose space-y-2 text-sm text-muted-foreground">{children}</div>
    </div>
  );
}

/** `501 not_configured`: usage analytics is off on this deployment. */
export function NotConfigured({ message }: { message?: string }) {
  return (
    <EmptyState icon={Settings2} title="Usage analytics is not enabled here">
      <p>
        This deployment has no analytics backend configured, so there is nothing to chart.
        That is the default for self-hosted installs — nothing is broken.
      </p>
      <p>
        To turn it on, give the gateway a Tinybird read token in{" "}
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">TINYBIRD_READ_TOKEN</code>{" "}
        and give the workers an append token in{" "}
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">TINYBIRD_TOKEN</code>;
        without the second one the workers never record usage in the first place. The pipes and
        data sources live in{" "}
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">tinybird/</code>.
      </p>
      <p>
        Full setup and the metering model are in{" "}
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-xs">
          docs/concepts/usage.mdx
        </code>
        .
      </p>
      {message ? <p className="text-xs">Gateway said: {message}</p> : null}
    </EmptyState>
  );
}

/** Configured, but the selected range contains no rows. */
export function NoUsageData({ from, to }: { from: string; to: string }) {
  return (
    <EmptyState icon={BarChart3} title="No usage recorded in this range">
      <p>
        Usage analytics is enabled, but nothing was ingested between{" "}
        <span className="font-mono text-xs">{from}</span> and{" "}
        <span className="font-mono text-xs">{to}</span>.
      </p>
      <p>
        Try a wider range. If a job did run in this window, remember the billing rollup refreshes
        hourly — very recent work has not landed yet.
      </p>
    </EmptyState>
  );
}

/** Loading skeleton for the whole screen. */
export function UsageSkeleton() {
  return (
    <div className="space-y-4">
      <SummaryTilesSkeleton />
      <div className="grid gap-4 lg:grid-cols-2">
        <Skeleton className="h-[22rem] w-full rounded-xl" />
        <Skeleton className="h-[22rem] w-full rounded-xl" />
      </div>
      <Skeleton className="h-64 w-full rounded-md" />
    </div>
  );
}
