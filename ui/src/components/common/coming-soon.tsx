import { Construction } from "lucide-react";

import { PageHeader } from "./page-header";

/**
 * Placeholder for a screen another agent owns. Keeps the sidebar navigable
 * until the real page lands; delete the file when you replace the route.
 */
export function ComingSoon({ title, description }: { title: string; description: string }) {
  return (
    <>
      <PageHeader title={title} description={description} />
      <div className="flex flex-col items-center justify-center gap-2 py-24 text-muted-foreground">
        <Construction className="size-6" aria-hidden />
        <p className="text-sm">Coming soon.</p>
      </div>
    </>
  );
}
