/**
 * Sources URLs.
 *
 * Same constraint as the pipeline editor: the UI is a static export
 * (`output: "export"`), so `/sources/<uid>/` would need a pre-rendered file per
 * source. The detail and edit pages are static routes that read `?uid=` — see
 * `src/app/pipelines/routes.ts` for the trade-off spelled out.
 */
export const SOURCES_HREF = "/sources";

/** Form for a new source. */
export const NEW_SOURCE_HREF = "/sources/new";

/** Summary and run history of one source. */
export function sourceDetailHref(uid: string): string {
  return `/sources/detail/?uid=${encodeURIComponent(uid)}`;
}

/** Form for an existing source. */
export function editSourceHref(uid: string): string {
  return `/sources/edit/?uid=${encodeURIComponent(uid)}`;
}
