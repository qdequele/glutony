import Link from "next/link";
import { ArrowRight } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import type { MergedAction } from "@/lib/catalog/merge";
import { actionDetailHref } from "../routes";

/** One action in the grid: curated copy, with live availability from the registry. */
export function ActionCard({ action }: { action: MergedAction }) {
  const { entry, registered, accepts, produces } = action;

  return (
    <Card className="transition-colors hover:border-primary/40">
      <CardHeader>
        <CardTitle className="flex flex-wrap items-start justify-between gap-x-2 gap-y-1">
          {/* `min-w-0` lets the title wrap instead of holding its min-content
              width, which would push the `shrink-0` badge past the card edge
              and get it clipped at narrow widths. */}
          <Link href={actionDetailHref(entry.plugin)} className="min-w-0 hover:underline">
            {entry.title}
          </Link>
          <Badge variant={registered ? "secondary" : "outline"}>
            {registered ? "Registered" : "Not deployed here"}
          </Badge>
        </CardTitle>
        <p className="font-mono text-xs text-muted-foreground">{entry.plugin}</p>
      </CardHeader>
      <CardContent className="space-y-3">
        <p className="text-sm text-muted-foreground">{entry.summary}</p>
        <div className="flex flex-wrap items-center gap-1.5 text-xs">
          {accepts.length > 0 ? (
            accepts.map((kind) => (
              <Badge key={kind} variant="outline" className="font-mono">
                {kind}
              </Badge>
            ))
          ) : (
            <Badge variant="outline" className="font-mono">
              nothing
            </Badge>
          )}
          <ArrowRight className="size-3 text-muted-foreground" aria-label="produces" />
          <Badge variant="outline" className="font-mono">
            {produces}
          </Badge>
        </div>
      </CardContent>
    </Card>
  );
}
