/**
 * Jobs URLs.
 *
 * Same constraint as the pipeline editor: the UI is a static export
 * (`output: "export"`), so `/jobs/<uuid>/` would need a pre-rendered file per
 * job. The detail page is therefore a static route that reads `?id=` — see
 * `src/app/pipelines/routes.ts` for the same trade-off spelled out.
 */
export const JOBS_HREF = "/jobs";

/** Detail view of one job. */
export function jobDetailHref(jobId: string): string {
  return `/jobs/detail/?id=${encodeURIComponent(jobId)}`;
}
