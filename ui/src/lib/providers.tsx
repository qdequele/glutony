"use client";

/**
 * Client-side providers for the whole app: TanStack Query (all server state),
 * next-themes (dark mode), Radix tooltips and the Sonner toaster.
 */
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "next-themes";
import { useState, type ReactNode } from "react";

import { Toaster } from "@/components/ui/sonner";
import { TooltipProvider } from "@/components/ui/tooltip";
import { ApiError } from "@/lib/api/client";

function makeQueryClient(): QueryClient {
  return new QueryClient({
    defaultOptions: {
      queries: {
        staleTime: 15 * 1000,
        refetchOnWindowFocus: false,
        // 4xx answers are the gateway telling us something true; only retry
        // transport failures and 5xx. A 501 `not_configured` is just as final.
        retry: (failureCount, error) => {
          if (error instanceof ApiError && (error.status < 500 || error.isNotConfigured)) {
            return false;
          }
          return failureCount < 2;
        },
      },
    },
  });
}

export function Providers({ children }: { children: ReactNode }) {
  const [queryClient] = useState(makeQueryClient);

  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider attribute="class" defaultTheme="system" enableSystem disableTransitionOnChange>
        <TooltipProvider delayDuration={200}>{children}</TooltipProvider>
        <Toaster richColors closeButton position="bottom-right" />
      </ThemeProvider>
    </QueryClientProvider>
  );
}
