import type { ReactNode } from "react";

import { Header } from "./header";
import { Sidebar } from "./sidebar";

/** Persistent chrome: header on top, section nav on the left, page on the right. */
export function AppShell({ children }: { children: ReactNode }) {
  return (
    <div className="flex h-dvh flex-col">
      <Header />
      <div className="flex min-h-0 flex-1">
        <Sidebar />
        <main className="min-w-0 flex-1 overflow-auto">{children}</main>
      </div>
    </div>
  );
}
