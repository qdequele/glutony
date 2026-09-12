import type { Metadata } from "next";

import { AppShell } from "@/components/app-shell/app-shell";
import { Providers } from "@/lib/providers";
import "./globals.css";

export const metadata: Metadata = {
  title: "meili-ingest admin",
  description: "Author pipelines, follow jobs and read usage for meili-ingest.",
};

export default function RootLayout({ children }: LayoutProps<"/">) {
  return (
    <html lang="en" suppressHydrationWarning className="h-full antialiased">
      <body className="min-h-full">
        <Providers>
          <AppShell>{children}</AppShell>
        </Providers>
      </body>
    </html>
  );
}
