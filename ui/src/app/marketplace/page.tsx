"use client";

import { useRouter } from "next/navigation";
import { useEffect } from "react";

import { MARKETPLACE_ACTIONS_HREF } from "./routes";

/**
 * `/marketplace` opens on the Actions tab. A client-side redirect is what a
 * static export can do — there is no server to answer with a 307. Same pattern
 * as `src/app/page.tsx`.
 */
export default function MarketplacePage() {
  const router = useRouter();
  useEffect(() => {
    router.replace(MARKETPLACE_ACTIONS_HREF);
  }, [router]);
  return null;
}
