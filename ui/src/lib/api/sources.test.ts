import { describe, expect, it } from "vitest";

import {
  isArchived,
  pipelinePinsConnection,
  RUN_POLL_MAX_MS,
  runLandedInRuns,
  runLandedInSource,
  shouldPollForRun,
  sourceListSearch,
  type PendingRun,
  type RunRecord,
} from "./sources";
import type { PipelineDefinition, StepDefinition } from "./types";

function pipeline(...steps: StepDefinition[]): PipelineDefinition {
  return { uid: "p", steps };
}

const parser: StepDefinition = { id: "parse", plugin: "json_parser" };
const pinned = (connection: string): StepDefinition => ({
  id: `index-${connection}`,
  plugin: "meili_indexer",
  config: { connection, index: "movies" },
});
const unpinned: StepDefinition = { id: "index", plugin: "meili_indexer", config: {} };

describe("pipelinePinsConnection", () => {
  it("is true when the only indexer names a connection", () => {
    expect(pipelinePinsConnection(pipeline(parser, pinned("prod")))).toBe(true);
  });

  it("needs every indexer to name one, like pins_destination() in Rust", () => {
    expect(pipelinePinsConnection(pipeline(pinned("a"), pinned("b")))).toBe(true);
    expect(pipelinePinsConnection(pipeline(pinned("a"), unpinned))).toBe(false);
  });

  it("is false for a pipeline with no indexer at all", () => {
    expect(pipelinePinsConnection(pipeline(parser))).toBe(false);
  });

  it("treats a blank or non-string connection as unset", () => {
    expect(pipelinePinsConnection(pipeline(pinned("   ")))).toBe(false);
    const numeric: StepDefinition = { id: "i", plugin: "meili_indexer", config: { connection: 1 } };
    expect(pipelinePinsConnection(pipeline(numeric))).toBe(false);
    const bare: StepDefinition = { id: "i", plugin: "meili_indexer" };
    expect(pipelinePinsConnection(pipeline(bare))).toBe(false);
  });
});

describe("isArchived", () => {
  it("reads archived_at", () => {
    expect(isArchived({ archived_at: "2026-09-20T10:00:00Z" })).toBe(true);
    expect(isArchived({})).toBe(false);
  });
});

describe("sourceListSearch", () => {
  it("asks for archived sources only when told to", () => {
    expect(sourceListSearch({ includeArchived: true })).toBe("?include_archived=true");
    expect(sourceListSearch({ includeArchived: false })).toBe("");
  });
});

describe("polling after Run now", () => {
  const pending: PendingRun = {
    since: 1_000,
    lastRunId: "run-1",
    lastRunAt: "2026-09-22T09:00:00Z",
  };
  const run = (run_id: string): RunRecord => ({
    run_id,
    source_id: "s",
    started_at: "2026-09-23T09:00:00Z",
    finished_at: "2026-09-23T09:00:01Z",
    outcome: "unchanged",
    items: 0,
    job_ids: [],
  });

  it("polls only while a run is pending, not landed, and inside the window", () => {
    expect(shouldPollForRun(undefined, false, 2_000)).toBe(false);
    expect(shouldPollForRun(pending, false, 2_000)).toBe(true);
    expect(shouldPollForRun(pending, true, 2_000)).toBe(false);
    expect(shouldPollForRun(pending, false, 1_000 + RUN_POLL_MAX_MS + 1)).toBe(false);
  });

  it("sees a run land as a new newest run id, whatever its outcome", () => {
    expect(runLandedInRuns(pending, [run("run-1")])).toBe(false);
    expect(runLandedInRuns(pending, [run("run-2"), run("run-1")])).toBe(true);
    expect(runLandedInRuns({ ...pending, lastRunId: null }, [])).toBe(false);
    expect(runLandedInRuns({ ...pending, lastRunId: null }, [run("run-1")])).toBe(true);
  });

  it("sees a run land on the source as a moved last_run_at", () => {
    expect(runLandedInSource(pending, { last_run_at: "2026-09-22T09:00:00Z" })).toBe(false);
    expect(runLandedInSource(pending, { last_run_at: "2026-09-23T09:00:00Z" })).toBe(true);
    expect(runLandedInSource({ ...pending, lastRunAt: null }, {})).toBe(false);
  });
});
