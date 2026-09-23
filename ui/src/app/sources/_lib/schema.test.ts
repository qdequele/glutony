import { describe, expect, it } from "vitest";

import type { SourceView } from "@/lib/api/sources";
import { emptySourceFormValues, sourceToFormValues, type SourceFormValues } from "./form";
import { fieldForServerError, sourceFormSchema } from "./schema";

const valid: SourceFormValues = {
  ...emptySourceFormValues(),
  uid: "tmdb",
  pipeline: "tmdb-movies",
  url: "https://files.tmdb.org/p/exports/movie_ids_{{ date-1d:%m_%d_%Y }}.json.gz",
};

const stored: SourceView = {
  uid: "tmdb",
  name: "tmdb",
  pipeline: "tmdb-movies",
  location: { kind: "url", url: valid.url },
  cron: "30 8 * * *",
  timezone: "UTC",
  paused: false,
  index: "movies",
  auth: { kind: "basic", username: "bot", password: "****" },
};

/** Paths of the issues the schema reports, as dotted strings. */
function issuePaths(mode: "create" | "edit", values: SourceFormValues, from?: SourceView) {
  const result = sourceFormSchema(mode, from).safeParse(values);
  return result.success ? [] : result.error.issues.map((issue) => issue.path.join("."));
}

describe("sourceFormSchema", () => {
  it("accepts a complete new source", () => {
    expect(issuePaths("create", valid)).toEqual([]);
  });

  it("flags the uid, pipeline, url and cron on create", () => {
    const paths = issuePaths("create", {
      ...valid,
      uid: "no spaces",
      pipeline: "",
      url: "ftp://x",
      cron: "0 0 * *",
    });
    expect(paths).toEqual(expect.arrayContaining(["uid", "pipeline", "url", "cron"]));
  });

  it("does not check the uid on edit (it is not editable)", () => {
    expect(issuePaths("edit", { ...sourceToFormValues(stored), uid: "" }, stored)).toEqual([]);
  });

  it("flags duplicate headers, case-insensitively", () => {
    const headers = [
      { name: "Accept", value: "a" },
      { name: "accept", value: "b" },
    ];
    expect(issuePaths("create", { ...valid, headers })).toContain("headers");
  });

  it("pins a credential problem on the right auth field", () => {
    const values = sourceToFormValues(stored);
    const renamed = { ...values, auth: { ...values.auth, username: "someone-else" } };
    expect(issuePaths("edit", renamed, stored)).toEqual(["auth.password"]);
  });

  it("refuses to clear an index override the API cannot clear", () => {
    expect(issuePaths("edit", { ...sourceToFormValues(stored), index: "" }, stored)).toEqual([
      "index",
    ]);
  });
});

describe("fieldForServerError", () => {
  it("maps gateway messages onto the field they are about", () => {
    expect(fieldForServerError("invalid cron string")).toBe("cron");
    expect(fieldForServerError('unknown timezone "Mars/Olympus"')).toBe("timezone");
    expect(
      fieldForServerError(
        'pipeline "p" cannot run on a schedule: its meili_indexer step names no Meilisearch `connection`',
      ),
    ).toBe("pipeline");
    expect(fieldForServerError('location "x" is not a URL: relative URL without a base')).toBe(
      "url",
    );
    expect(fieldForServerError("host 10.0.0.1 is not allowed by SOURCE_FETCH_HOSTS")).toBe("url");
  });

  it("returns undefined for a message about nothing it knows", () => {
    expect(fieldForServerError("something else went wrong")).toBeUndefined();
  });
});
