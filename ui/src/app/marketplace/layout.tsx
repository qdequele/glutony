"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import type { ReactNode } from "react";

import { PageHeader } from "@/components/common/page-header";
import { cn } from "@/lib/utils";
import { MARKETPLACE_ACTIONS_HREF, MARKETPLACE_WORKFLOWS_HREF } from "./routes";

const TABS = [
  { href: MARKETPLACE_ACTIONS_HREF, label: "Actions" },
  { href: MARKETPLACE_WORKFLOWS_HREF, label: "Workflows" },
];

/** `usePathname()` keeps the trailing slash `next.config.ts` adds; hrefs do not carry one. */
function samePath(pathname: string, href: string): boolean {
  return pathname.replace(/\/$/, "") === href;
}

/**
 * Shell for both marketplace surfaces.
 *
 * The tabs are links rather than a `<Tabs>` component: each surface is its own
 * route with its own URL, so browser history and deep links work. Detail pages
 * live under these paths and render without the tab bar highlighting either —
 * which is correct, they are neither tab.
 */
export default function MarketplaceLayout({ children }: { children: ReactNode }) {
  const pathname = usePathname();

  return (
    <>
      <PageHeader
        title="Marketplace"
        description="Everything this deployment can run, and everything it ships with."
      />
      <div className="flex gap-1 border-b px-4">
        {TABS.map((tab) => {
          const active = samePath(pathname, tab.href);
          return (
            <Link
              key={tab.href}
              href={tab.href}
              aria-current={active ? "page" : undefined}
              className={cn(
                "-mb-px border-b-2 px-3 py-2 text-sm transition-colors",
                active
                  ? "border-primary font-medium text-foreground"
                  : "border-transparent text-muted-foreground hover:text-foreground",
              )}
            >
              {tab.label}
            </Link>
          );
        })}
      </div>
      {children}
    </>
  );
}
