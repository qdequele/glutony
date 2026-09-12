import { describe, expect, it } from "vitest";

import type { JobStatus, WorkflowProgress } from "./types";
import {
  DEFAULT_JOB_FILTERS,
  JOB_POLL_INTERVAL_MS,
  formatBytes,
  formatDuration,
  isCancellable,
  isTerminalStatus,
  jobDetailPollInterval,
  jobListPollInterval,
  jobListSearch,
  pollIntervalFor,
  progressPercent,
  type JobDetail,
  type JobListResponse,
  type JobRecord,
} from "./jobs";

function job(status: JobStatus, jobId = "j1"): JobRecord {
  return {
    job_id: jobId,
    workflow_id: `ingest-${jobId}`,
    pipeline_uid: "builtin.pdf",
    status,
    started_at: "2026-09-13T10:00:00Z",
    updated_at: "2026-09-13T10:00:05Z",
  };
}

function page(...statuses: JobStatus[]): JobListResponse {
  return {
    jobs: statuses.map((status, index) => job(status, `j${index}`)),
    limit: 25,
    offset: 0,
  };
}

describe("terminal statuses", () => {
  it("matches JobStatus::is_terminal on the Rust side", () => {
    expect(isTerminalStatus("succeeded")).toBe(true);
    expect(isTerminalStatus("failed")).toBe(true);
    expect(isTerminalStatus("cancelled")).toBe(true);
    expect(isTerminalStatus("queued")).toBe(false);
    expect(isTerminalStatus("running")).toBe(false);
  });

  it("only offers Cancel while there is something to cancel", () => {
    expect(isCancellable("queued")).toBe(true);
    expect(isCancellable("running")).toBe(true);
    expect(isCancellable("succeeded")).toBe(false);
    expect(isCancellable("failed")).toBe(false);
    expect(isCancellable("cancelled")).toBe(false);
  });
});

describe("poll interval", () => {
  it("polls while at least one status is still moving", () => {
    expect(pollIntervalFor(["running"])).toBe(JOB_POLL_INTERVAL_MS);
    expect(pollIntervalFor(["queued"])).toBe(JOB_POLL_INTERVAL_MS);
    expect(pollIntervalFor(["succeeded", "failed", "queued"])).toBe(JOB_POLL_INTERVAL_MS);
  });

  it("stops once everything is terminal", () => {
    expect(pollIntervalFor(["succeeded"])).toBe(false);
    expect(pollIntervalFor(["succeeded", "failed", "cancelled"])).toBe(false);
  });

  it("stops on an empty page rather than spinning forever", () => {
    expect(pollIntervalFor([])).toBe(false);
    expect(jobListPollInterval(page())).toBe(false);
  });

  it("drives the list from the rows actually on screen", () => {
    expect(jobListPollInterval(page("succeeded", "running"))).toBe(JOB_POLL_INTERVAL_MS);
    expect(jobListPollInterval(page("succeeded", "cancelled"))).toBe(false);
  });

  it("drives the detail from the job's own status", () => {
    const running: JobDetail = { job_id: "j1", status: "running" };
    const done: JobDetail = { job_id: "j1", status: "succeeded" };
    expect(jobDetailPollInterval(running)).toBe(JOB_POLL_INTERVAL_MS);
    expect(jobDetailPollInterval(done)).toBe(false);
  });

  it("does not poll before the first response lands", () => {
    expect(jobListPollInterval(undefined)).toBe(false);
    expect(jobDetailPollInterval(undefined)).toBe(false);
  });
});

describe("the GET /jobs query string", () => {
  it("sends only limit on the default first page", () => {
    expect(jobListSearch(DEFAULT_JOB_FILTERS)).toBe("?limit=25");
  });

  it("adds the filters that are set", () => {
    expect(
      jobListSearch({ status: "failed", pipeline_uid: "builtin.pdf", limit: 50, offset: 100 }),
    ).toBe("?status=failed&pipeline_uid=builtin.pdf&limit=50&offset=100");
  });

  it("drops a blank pipeline filter instead of sending an empty value", () => {
    expect(jobListSearch({ pipeline_uid: "   ", limit: 25, offset: 0 })).toBe("?limit=25");
  });

  it("percent-encodes a pipeline uid", () => {
    expect(jobListSearch({ pipeline_uid: "my pipeline/v2", limit: 25, offset: 0 })).toBe(
      "?pipeline_uid=my+pipeline%2Fv2&limit=25",
    );
  });

  it("never sends project_id: the gateway scopes the tenant itself", () => {
    const search = jobListSearch({ status: "running", pipeline_uid: "p", limit: 25, offset: 25 });
    expect(search).not.toContain("project_id");
  });
});

describe("formatting", () => {
  it("formats durations", () => {
    expect(formatDuration(undefined)).toBe("—");
    expect(formatDuration(0)).toBe("0 ms");
    expect(formatDuration(940)).toBe("940 ms");
    expect(formatDuration(1_500)).toBe("1.50 s");
    expect(formatDuration(12_300)).toBe("12.3 s");
    expect(formatDuration(60_000)).toBe("1m");
    expect(formatDuration(95_000)).toBe("1m 35s");
  });

  it("formats byte counts", () => {
    expect(formatBytes(undefined)).toBe("—");
    expect(formatBytes(0)).toBe("0 B");
    expect(formatBytes(900)).toBe("900 B");
    expect(formatBytes(2048)).toBe("2.0 KB");
    expect(formatBytes(20 * 1024)).toBe("20 KB");
    expect(formatBytes(5 * 1024 * 1024)).toBe("5.0 MB");
  });

  it("refuses to render nonsense", () => {
    expect(formatDuration(Number.NaN)).toBe("—");
    expect(formatBytes(-1)).toBe("—");
  });
});

describe("progress percentage", () => {
  const progress = (completed: number, total: number): WorkflowProgress => ({
    status: "running",
    completed_steps: completed,
    total_steps: total,
  });

  it("maps completed/total onto 0..100", () => {
    expect(progressPercent(progress(0, 4))).toBe(0);
    expect(progressPercent(progress(1, 4))).toBe(25);
    expect(progressPercent(progress(4, 4))).toBe(100);
  });

  it("survives a missing or zero-step progress", () => {
    expect(progressPercent(undefined)).toBe(0);
    expect(progressPercent(null)).toBe(0);
    expect(progressPercent(progress(0, 0))).toBe(0);
  });
});
