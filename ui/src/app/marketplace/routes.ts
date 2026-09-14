/**
 * Marketplace URLs.
 *
 * Same constraint as the pipeline editor and the jobs detail page: the UI is a
 * static export (`output: "export"`), so a dynamic segment would need a
 * pre-rendered file per plugin and per workflow. Detail pages are static routes
 * reading a query param — see `src/app/pipelines/routes.ts` for the trade-off
 * spelled out in full.
 */
export const MARKETPLACE_HREF = "/marketplace";
export const MARKETPLACE_ACTIONS_HREF = "/marketplace/actions";
export const MARKETPLACE_WORKFLOWS_HREF = "/marketplace/workflows";

/** Detail view of one action. */
export function actionDetailHref(plugin: string): string {
  return `/marketplace/actions/detail/?plugin=${encodeURIComponent(plugin)}`;
}

/** Detail view of one workflow. */
export function workflowDetailHref(uid: string): string {
  return `/marketplace/workflows/detail/?uid=${encodeURIComponent(uid)}`;
}
