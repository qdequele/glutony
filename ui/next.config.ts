import type { NextConfig } from "next";

/**
 * The UI is exported as plain static files (`ui/out`) and embedded in the
 * gateway binary, so it is served same-origin: no Node runtime, no CORS.
 *
 * - `output: "export"`   → static HTML/JS/CSS in `out/`, no server needed.
 * - `images.unoptimized` → the Next image optimizer needs a server.
 * - `trailingSlash`      → `/pipelines/` resolves to `pipelines/index.html`
 *                          on a dumb static file server.
 *
 * `NEXT_PUBLIC_BASE_PATH` exists because the page routes (`/pipelines`,
 * `/jobs`) collide with the gateway's own API routes of the same name. Build
 * with `NEXT_PUBLIC_BASE_PATH=/ui` to mount the whole UI under a prefix; the
 * API calls stay absolute (`/pipelines`, `/plugins`) and are unaffected,
 * because `basePath` only rewrites `next/link` hrefs and asset URLs.
 */
const basePath = process.env.NEXT_PUBLIC_BASE_PATH ?? "";

const nextConfig: NextConfig = {
  output: "export",
  trailingSlash: true,
  images: { unoptimized: true },
  ...(basePath ? { basePath } : {}),
};

export default nextConfig;
