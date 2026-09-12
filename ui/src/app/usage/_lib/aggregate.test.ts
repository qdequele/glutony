import { describe, expect, it } from "vitest";

import type { UsageRow } from "@/lib/api/usage";
import {
  OTHER_SERIES,
  aggregateByPlugin,
  aggregateCostByDay,
  aggregateTotals,
  documentsByDay,
  pluginTotal,
  seriesKey,
  sortPlugins,
} from "./aggregate";

/**
 * A step-level row: everything but the job counters, exactly as
 * `usage_daily_billing` fills it (job columns zeroed).
 */
function stepRow(over: Partial<UsageRow> & { plugin: string }): UsageRow {
  return {
    day: "2026-09-01",
    pipeline_uid: "pipe",
    jobs: 0,
    jobs_succeeded: 0,
    jobs_failed: 0,
    steps: 1,
    documents_out: 0,
    input_bytes: 0,
    duration_ms: 0,
    llm_input_tokens: 0,
    llm_output_tokens: 0,
    llm_requests: 0,
    audio_seconds: 0,
    pages: 0,
    images: 0,
    external_requests: 0,
    ...over,
  };
}

/** A job-level row: `plugin === ""`, only the job counters filled. */
function jobRow(over: Partial<UsageRow> = {}): UsageRow {
  return stepRow({
    plugin: "",
    steps: 0,
    jobs: 1,
    jobs_succeeded: 1,
    jobs_failed: 0,
    ...over,
  });
}

describe("aggregateTotals", () => {
  it("reads job counters from job rows and work counters from plugin rows", () => {
    const totals = aggregateTotals([
      jobRow({ jobs: 3, jobs_succeeded: 2, jobs_failed: 1 }),
      stepRow({
        plugin: "llm_enrich",
        steps: 3,
        documents_out: 12,
        input_bytes: 2048,
        duration_ms: 4500,
        llm_input_tokens: 900,
        llm_output_tokens: 100,
        llm_requests: 3,
      }),
      stepRow({ plugin: "whisper", steps: 1, audio_seconds: 120, duration_ms: 1500 }),
      stepRow({ plugin: "pdf_extract", steps: 2, pages: 44, images: 7, external_requests: 2 }),
    ]);

    expect(totals.jobs).toBe(3);
    expect(totals.jobsSucceeded).toBe(2);
    expect(totals.jobsFailed).toBe(1);
    expect(totals.steps).toBe(6);
    expect(totals.documentsOut).toBe(12);
    expect(totals.inputBytes).toBe(2048);
    expect(totals.durationMs).toBe(6000);
    expect(totals.llmInputTokens).toBe(900);
    expect(totals.llmOutputTokens).toBe(100);
    expect(totals.llmTokens).toBe(1000);
    expect(totals.llmRequests).toBe(3);
    expect(totals.audioSeconds).toBe(120);
    expect(totals.pages).toBe(44);
    expect(totals.images).toBe(7);
    expect(totals.externalRequests).toBe(2);
  });

  it("does not double-count when a job row repeats the job's cost units", () => {
    // This is the shape the raw `meili_ingest_usage` table has: the job event
    // carries the whole job's units as a per-job total. The billing rollup
    // zeroes them, but if it ever stopped doing so, summing both families
    // would double every number on the screen. It must not.
    const steps = [
      stepRow({ plugin: "llm_enrich", llm_input_tokens: 800, llm_output_tokens: 200 }),
      stepRow({ plugin: "whisper", audio_seconds: 60, documents_out: 4 }),
    ];
    const rollupShaped = aggregateTotals([jobRow(), ...steps]);
    const rawShaped = aggregateTotals([
      // job row echoing the sum of the steps, the way the raw events do
      jobRow({
        llm_input_tokens: 800,
        llm_output_tokens: 200,
        audio_seconds: 60,
        documents_out: 4,
        duration_ms: 9999,
        input_bytes: 123,
        steps: 2,
      }),
      ...steps,
    ]);

    expect(rawShaped).toEqual(rollupShaped);
    expect(rawShaped.llmTokens).toBe(1000);
    expect(rawShaped.audioSeconds).toBe(60);
    expect(rawShaped.documentsOut).toBe(4);
    expect(rawShaped.steps).toBe(2);
    expect(rawShaped.jobs).toBe(1);
  });

  it("does not credit plugin rows with job counters", () => {
    // Symmetric guard: a step row that somehow carried `jobs` must not inflate
    // the jobs tile.
    const totals = aggregateTotals([
      jobRow({ jobs: 1, jobs_succeeded: 1 }),
      stepRow({ plugin: "llm_enrich", jobs: 1, jobs_succeeded: 1, jobs_failed: 5 }),
    ]);

    expect(totals.jobs).toBe(1);
    expect(totals.jobsSucceeded).toBe(1);
    expect(totals.jobsFailed).toBe(0);
  });

  it("returns zeros for an empty range", () => {
    const totals = aggregateTotals([]);
    expect(totals.jobs).toBe(0);
    expect(totals.llmTokens).toBe(0);
    expect(totals.audioSeconds).toBe(0);
  });

  it("coerces stringified numbers and ignores columns it cannot read", () => {
    const totals = aggregateTotals([
      stepRow({ plugin: "llm_enrich", llm_input_tokens: "1500" as unknown as number }),
      stepRow({ plugin: "llm_enrich", llm_output_tokens: null as unknown as number }),
      stepRow({ plugin: "llm_enrich", audio_seconds: "not a number" as unknown as number }),
      // A column added to the Tinybird pipe that this build knows nothing about
      // must not break anything.
      stepRow({ plugin: "llm_enrich", gpu_seconds: 42 }),
    ]);

    expect(totals.llmTokens).toBe(1500);
    expect(totals.audioSeconds).toBe(0);
    expect(Number.isNaN(totals.llmTokens)).toBe(false);
  });
});

describe("aggregateByPlugin", () => {
  const rows = [
    jobRow({ jobs: 9, jobs_succeeded: 9 }),
    stepRow({ plugin: "llm_enrich", llm_input_tokens: 800, llm_output_tokens: 200, steps: 2 }),
    stepRow({
      plugin: "llm_enrich",
      day: "2026-09-02",
      llm_input_tokens: 100,
      llm_requests: 4,
      duration_ms: 500,
    }),
    stepRow({ plugin: "whisper", audio_seconds: 300, duration_ms: 20_000, documents_out: 3 }),
    stepRow({ plugin: "pdf_extract", pages: 120, images: 8, documents_out: 120 }),
  ];

  it("sums per plugin and never emits a row for job-level data", () => {
    const breakdown = aggregateByPlugin(rows);
    expect(breakdown.map((entry) => entry.plugin)).toEqual([
      "llm_enrich",
      "whisper",
      "pdf_extract",
    ]);
    expect(breakdown.some((entry) => entry.plugin === "")).toBe(false);

    const llm = breakdown[0];
    expect(llm.llmTokens).toBe(1100);
    expect(llm.llmRequests).toBe(4);
    expect(llm.steps).toBe(3);
    expect(llm.durationMs).toBe(500);
  });

  it("orders by tokens, then duration, then name", () => {
    const breakdown = aggregateByPlugin([
      stepRow({ plugin: "b", duration_ms: 10 }),
      stepRow({ plugin: "a", duration_ms: 10 }),
      stepRow({ plugin: "c", duration_ms: 50 }),
    ]);
    expect(breakdown.map((entry) => entry.plugin)).toEqual(["c", "a", "b"]);
  });

  it("totals a column across the breakdown for the share columns", () => {
    const breakdown = aggregateByPlugin(rows);
    expect(pluginTotal(breakdown, "llmTokens")).toBe(1100);
    expect(pluginTotal(breakdown, "documentsOut")).toBe(123);
    expect(pluginTotal(breakdown, "audioSeconds")).toBe(300);
    expect(pluginTotal([], "pages")).toBe(0);
  });

  it("agrees with the totals tiles on every shared column", () => {
    const breakdown = aggregateByPlugin(rows);
    const totals = aggregateTotals(rows);
    expect(pluginTotal(breakdown, "llmTokens")).toBe(totals.llmTokens);
    expect(pluginTotal(breakdown, "documentsOut")).toBe(totals.documentsOut);
    expect(pluginTotal(breakdown, "audioSeconds")).toBe(totals.audioSeconds);
    expect(pluginTotal(breakdown, "pages")).toBe(totals.pages);
    expect(pluginTotal(breakdown, "durationMs")).toBe(totals.durationMs);
  });

  it("sorts on any column in both directions", () => {
    const breakdown = aggregateByPlugin(rows);
    expect(sortPlugins(breakdown, "pages", "desc").map((e) => e.plugin)).toEqual([
      "pdf_extract",
      "llm_enrich",
      "whisper",
    ]);
    expect(sortPlugins(breakdown, "audioSeconds", "asc").map((e) => e.plugin)).toEqual([
      "llm_enrich",
      "pdf_extract",
      "whisper",
    ]);
    // The input is not mutated.
    expect(breakdown[0].plugin).toBe("llm_enrich");
  });
});

describe("aggregateCostByDay", () => {
  it("buckets cost units per day, oldest first, ignoring job rows", () => {
    const points = aggregateCostByDay([
      jobRow({ day: "2026-09-02", llm_input_tokens: 999 }),
      stepRow({ plugin: "llm_enrich", day: "2026-09-02", llm_input_tokens: 10 }),
      stepRow({ plugin: "llm_enrich", day: "2026-09-01", llm_output_tokens: 5 }),
      stepRow({ plugin: "whisper", day: "2026-09-02", audio_seconds: 30 }),
    ]);

    expect(points.map((p) => p.day)).toEqual(["2026-09-01", "2026-09-02"]);
    expect(points[1]).toEqual({
      day: "2026-09-02",
      llmInputTokens: 10,
      llmOutputTokens: 0,
      audioSeconds: 30,
    });
  });

  it("emits a zero point for days of the range with no activity", () => {
    const points = aggregateCostByDay(
      [stepRow({ plugin: "llm_enrich", day: "2026-09-02", llm_input_tokens: 10 })],
      ["2026-09-01", "2026-09-02", "2026-09-03"],
    );
    expect(points.map((p) => p.day)).toEqual(["2026-09-01", "2026-09-02", "2026-09-03"]);
    expect(points[0].llmInputTokens).toBe(0);
    expect(points[2].audioSeconds).toBe(0);
  });
});

describe("documentsByDay", () => {
  it("stacks documents by plugin and folds the tail into one band", () => {
    const rows = ["a", "b", "c", "d", "e", "f", "g"].map((plugin, index) =>
      stepRow({ plugin, documents_out: 100 - index }),
    );
    const { points, series } = documentsByDay(rows, { topN: 3 });

    expect(series.map((band) => band.plugin)).toEqual(["a", "b", "c", OTHER_SERIES]);
    expect(series[0].key).toBe(seriesKey("a"));
    expect(points).toHaveLength(1);
    expect(points[0][seriesKey("a")]).toBe(100);
    // d + e + f + g = 97 + 96 + 95 + 94
    expect(points[0][seriesKey(OTHER_SERIES)]).toBe(382);
    expect(points[0].total).toBe(100 + 99 + 98 + 382);
  });

  it("omits the other band when every plugin fits", () => {
    const { series } = documentsByDay(
      [stepRow({ plugin: "a", documents_out: 1 }), stepRow({ plugin: "b", documents_out: 2 })],
      { topN: 5 },
    );
    expect(series.map((band) => band.plugin)).toEqual(["b", "a"]);
  });

  it("ignores job rows, so documents are never counted twice", () => {
    const { points } = documentsByDay([
      jobRow({ day: "2026-09-01", documents_out: 50 }),
      stepRow({ plugin: "pdf_extract", day: "2026-09-01", documents_out: 50 }),
    ]);
    expect(points[0].total).toBe(50);
  });

  it("keeps every day of the range and stays sorted", () => {
    const { points } = documentsByDay(
      [stepRow({ plugin: "a", day: "2026-09-03", documents_out: 4 })],
      { days: ["2026-09-01", "2026-09-02", "2026-09-03"] },
    );
    expect(points.map((p) => p.day)).toEqual(["2026-09-01", "2026-09-02", "2026-09-03"]);
    expect(points[0].total).toBe(0);
    expect(points[0][seriesKey("a")]).toBe(0);
  });

  it("returns no bands when nothing produced a document", () => {
    const { points, series } = documentsByDay([stepRow({ plugin: "a", documents_out: 0 })]);
    expect(series).toEqual([]);
    expect(points).toEqual([]);
  });
});

describe("documents indexed vs produced", () => {
  it("bills the final-step count, not the sum across stages", () => {
    // A pdf -> chunk -> index pipeline touches the same document three times.
    // Summing step rows measures work done; only the job row knows how many
    // documents actually reached Meilisearch.
    const rows = [
      jobRow({ jobs: 1, jobs_succeeded: 1, documents_indexed: 12 }),
      stepRow({ plugin: "pdf_extractor", documents_out: 12 }),
      stepRow({ plugin: "chunker", documents_out: 40 }),
      stepRow({ plugin: "meili_indexer", documents_out: 40 }),
    ];
    const totals = aggregateTotals(rows);
    expect(totals.documentsIndexed).toBe(12);
    expect(totals.documentsOut).toBe(92);
  });

  it("does not read documents_indexed from step rows", () => {
    const rows = [stepRow({ plugin: "meili_indexer", documents_indexed: 999 })];
    expect(aggregateTotals(rows).documentsIndexed).toBe(0);
  });
});
