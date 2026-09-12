/**
 * Editor URLs.
 *
 * The UI is a static export (`output: "export"`), so there is no server to
 * resolve `/pipelines/<uid>/` into a page: a dynamic segment would need a
 * pre-rendered file per pipeline, and pipelines are created at runtime. The
 * editor therefore lives at a static route and takes the uid in the query
 * string. Everything goes through these helpers, so switching to
 * `/pipelines/[uid]/` is a one-file change the day the gateway rewrites
 * unknown `/pipelines/*` paths to the editor's `index.html`.
 */
export const PIPELINES_HREF = "/pipelines";

/** Editor for an existing pipeline. */
export function editPipelineHref(uid: string): string {
  return `/pipelines/edit/?uid=${encodeURIComponent(uid)}`;
}

/** Editor for a new pipeline, optionally seeded from an existing one. */
export function newPipelineHref(cloneFrom?: string): string {
  return cloneFrom
    ? `/pipelines/new/?from=${encodeURIComponent(cloneFrom)}`
    : "/pipelines/new";
}
