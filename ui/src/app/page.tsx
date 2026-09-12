"use client";

import { useRouter } from "next/navigation";
import { useEffect } from "react";

/**
 * The admin UI opens on the pipeline list. A client-side redirect (rather than
 * `next/navigation`'s `redirect`) is what a static export can do: there is no
 * server to answer with a 307.
 */
export default function HomePage() {
  const router = useRouter();
  useEffect(() => {
    router.replace("/pipelines");
  }, [router]);
  return null;
}
