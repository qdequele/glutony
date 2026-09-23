"use client";

import { useEffect, useState } from "react";

/**
 * The current time, re-read every `intervalMs`, so relative timestamps ("in 5
 * minutes") keep moving on a page left open. Reading the clock in state rather
 * than during render keeps rendering pure.
 */
export function useNow(intervalMs: number = 30_000): Date {
  const [now, setNow] = useState(() => new Date());
  useEffect(() => {
    const id = window.setInterval(() => setNow(new Date()), intervalMs);
    return () => window.clearInterval(id);
  }, [intervalMs]);
  return now;
}
