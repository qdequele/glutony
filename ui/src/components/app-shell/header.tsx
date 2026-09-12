"use client";

import Link from "next/link";

import { Badge } from "@/components/ui/badge";
import { useProjectId } from "@/lib/api/hooks";
import { ThemeToggle } from "./theme-toggle";

export function Header() {
  const projectId = useProjectId();

  return (
    <header className="flex h-12 shrink-0 items-center gap-3 border-b px-3">
      <Link href="/pipelines" className="flex items-baseline gap-2">
        <span className="text-sm font-semibold tracking-tight">meili-ingest</span>
        <span className="text-xs text-muted-foreground">admin</span>
      </Link>

      {projectId ? (
        <Badge variant="secondary" className="font-mono text-[11px]" title="Tenant project id">
          {projectId}
        </Badge>
      ) : null}

      <div className="ml-auto">
        <ThemeToggle />
      </div>
    </header>
  );
}
