import { describe, expect, it } from "vitest";

import {
  LARGE_UPLOAD_BYTES,
  buildIngestPlan,
  ingestPath,
  isLargeUpload,
  parseDocumentsInput,
  type IngestFormState,
} from "./ingest";

/**
 * A stand-in for the browser `File` the drop zone produces. Only `name` and
 * `size` are read by the code under test, so a structural fake keeps the suite
 * runnable in the node environment vitest is configured with.
 */
function file(name: string, size = 12): File {
  return { name, size, type: "application/pdf" } as unknown as File;
}

const fileState = (extra: Partial<IngestFormState> = {}): IngestFormState => ({
  source: { kind: "file", file: file("report.pdf") },
  ...extra,
});

describe("the request path", () => {
  it("auto-routes through /ingest by default", () => {
    expect(ingestPath(fileState())).toBe("/ingest");
  });

  it("switches to the explicit-pipeline route when one is forced", () => {
    expect(ingestPath(fileState({ pipeline: "video-ingest" }))).toBe(
      "/ingest/pipeline/video-ingest",
    );
  });

  it("carries the index override in the query string", () => {
    expect(ingestPath(fileState({ index: "keynotes" }))).toBe("/ingest?index=keynotes");
    expect(ingestPath(fileState({ pipeline: "video-ingest", index: "keynotes" }))).toBe(
      "/ingest/pipeline/video-ingest?index=keynotes",
    );
  });

  it("encodes both, and ignores whitespace-only values", () => {
    expect(ingestPath(fileState({ pipeline: "a/b", index: "my index" }))).toBe(
      "/ingest/pipeline/a%2Fb?index=my%20index",
    );
    expect(ingestPath(fileState({ pipeline: "  ", index: "  " }))).toBe("/ingest");
  });
});

describe("multipart vs JSON", () => {
  it("sends a file as multipart", () => {
    const plan = buildIngestPlan(fileState({ index: "docs" }));
    expect(plan).toEqual({
      encoding: "multipart",
      path: "/ingest?index=docs",
      file: expect.objectContaining({ name: "report.pdf" }),
      fields: {},
    });
  });

  it("sends a URL as JSON", () => {
    const plan = buildIngestPlan({ source: { kind: "url", url: " https://x.dev/a.pdf " } });
    expect(plan).toEqual({
      encoding: "json",
      path: "/ingest",
      body: { url: "https://x.dev/a.pdf" },
    });
  });

  it("sends inline documents as JSON under a `documents` key", () => {
    const plan = buildIngestPlan({
      source: { kind: "documents", documents: [{ id: "1", content: "hi" }] },
      pipeline: "builtin.json",
    });
    expect(plan).toEqual({
      encoding: "json",
      path: "/ingest/pipeline/builtin.json",
      body: { documents: [{ id: "1", content: "hi" }] },
    });
  });
});

describe("parsing the JSON documents tab", () => {
  it("accepts a bare array", () => {
    expect(parseDocumentsInput('[{"id":"1"},{"id":"2"}]')).toEqual({
      ok: true,
      documents: [{ id: "1" }, { id: "2" }],
    });
  });

  it("accepts the {documents: [...]} envelope the API documents", () => {
    expect(parseDocumentsInput('{"documents":[{"id":"1"}]}')).toEqual({
      ok: true,
      documents: [{ id: "1" }],
    });
  });

  it("accepts a single object and wraps it", () => {
    expect(parseDocumentsInput('{"id":"1","content":"hi"}')).toEqual({
      ok: true,
      documents: [{ id: "1", content: "hi" }],
    });
  });

  it("rejects an empty input", () => {
    expect(parseDocumentsInput("   ")).toEqual({
      ok: false,
      error: "Paste at least one document.",
    });
  });

  it("rejects an empty array, like the gateway does", () => {
    expect(parseDocumentsInput("[]")).toMatchObject({ ok: false });
    expect(parseDocumentsInput('{"documents":[]}')).toMatchObject({ ok: false });
  });

  it("rejects non-object entries", () => {
    expect(parseDocumentsInput('["just a string"]')).toEqual({
      ok: false,
      error: "Every document must be a JSON object.",
    });
  });

  it("reports the syntax error rather than swallowing it", () => {
    const outcome = parseDocumentsInput("{nope}");
    expect(outcome.ok).toBe(false);
    if (!outcome.ok) expect(outcome.error).not.toHaveLength(0);
  });
});

describe("the large-upload warning", () => {
  it("warns strictly above the staging threshold", () => {
    expect(isLargeUpload(LARGE_UPLOAD_BYTES)).toBe(false);
    expect(isLargeUpload(LARGE_UPLOAD_BYTES + 1)).toBe(true);
    expect(isLargeUpload(1024)).toBe(false);
  });
});
