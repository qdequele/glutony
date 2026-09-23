import { describe, expect, it } from "vitest";

import type { SourceView } from "@/lib/api/sources";
import {
  buildCreateBody,
  buildPatchBody,
  clearsIndex,
  emptySourceFormValues,
  formLocation,
  sameLocation,
  sourceToFormValues,
  timezoneOptions,
  type SourceFormValues,
} from "./form";

const TMDB_URL =
  "https://files.tmdb.org/p/exports/movie_ids_{{ date-1d:%m_%d_%Y }}.json.gz";

const stored: SourceView = {
  uid: "tmdb",
  name: "TMDB daily export",
  pipeline: "tmdb-movies",
  location: { kind: "url", url: TMDB_URL, headers: { Accept: "application/gzip" } },
  cron: "30 8 * * *",
  timezone: "UTC",
  paused: false,
  index: "movies",
  auth: { kind: "bearer", token: "****" },
};

function values(patch: Partial<SourceFormValues>): SourceFormValues {
  return { ...emptySourceFormValues(), ...patch };
}

describe("buildCreateBody", () => {
  it("sends only the keys that are set", () => {
    const body = buildCreateBody(
      values({ uid: " tmdb ", pipeline: "tmdb-movies", url: ` ${TMDB_URL} ` }),
      { kind: "keep" },
    );
    expect(body).toEqual({
      uid: "tmdb",
      pipeline: "tmdb-movies",
      location: { kind: "url", url: TMDB_URL },
      cron: "30 0 * * *",
      timezone: "UTC",
    });
  });

  it("includes optional fields, a non-GET method, headers and auth", () => {
    const body = buildCreateBody(
      values({
        uid: "feed",
        name: "Feed",
        description: "Nightly",
        pipeline: "p",
        url: "https://example.com/feed.json",
        method: "post",
        headers: [
          { name: "Accept", value: "application/json" },
          { name: "", value: "dropped" },
        ],
        index: "feed",
        timezone: "Europe/Paris",
      }),
      { kind: "replace", auth: { kind: "bearer", token: "t" } },
    );
    expect(body).toEqual({
      uid: "feed",
      name: "Feed",
      description: "Nightly",
      pipeline: "p",
      location: {
        kind: "url",
        url: "https://example.com/feed.json",
        method: "POST",
        headers: { Accept: "application/json" },
      },
      cron: "30 0 * * *",
      timezone: "Europe/Paris",
      index: "feed",
      auth: { kind: "bearer", token: "t" },
    });
  });
});

describe("buildPatchBody", () => {
  it("is empty when nothing changed", () => {
    expect(buildPatchBody(stored, sourceToFormValues(stored), { kind: "keep" })).toEqual({});
  });

  it("sends only the changed fields", () => {
    const next = { ...sourceToFormValues(stored), cron: "0 9 * * 1", timezone: "Europe/Paris" };
    expect(buildPatchBody(stored, next, { kind: "keep" })).toEqual({
      cron: "0 9 * * 1",
      timezone: "Europe/Paris",
    });
  });

  it("sends the whole location when any part of it changed", () => {
    const next = { ...sourceToFormValues(stored), headers: [] };
    expect(buildPatchBody(stored, next, { kind: "keep" })).toEqual({
      location: { kind: "url", url: TMDB_URL },
    });
  });

  it("maps the auth change onto absent / null / object", () => {
    const form = sourceToFormValues(stored);
    expect(buildPatchBody(stored, form, { kind: "keep" })).not.toHaveProperty("auth");
    expect(buildPatchBody(stored, form, { kind: "clear" })).toEqual({ auth: null });
    expect(
      buildPatchBody(stored, form, { kind: "replace", auth: { kind: "bearer", token: "n" } }),
    ).toEqual({ auth: { kind: "bearer", token: "n" } });
  });

  it("resets a cleared name to the uid and clears a description with an empty string", () => {
    const withDescription: SourceView = { ...stored, description: "old" };
    const next = { ...sourceToFormValues(withDescription), name: "", description: "" };
    expect(buildPatchBody(withDescription, next, { kind: "keep" })).toEqual({
      name: "tmdb",
      description: "",
    });
  });

  it("never sends an empty index, which the API would store as-is", () => {
    const next = { ...sourceToFormValues(stored), index: "" };
    expect(buildPatchBody(stored, next, { kind: "keep" })).toEqual({});
    expect(clearsIndex(stored, next)).toBe(true);
    expect(clearsIndex(stored, sourceToFormValues(stored))).toBe(false);
    expect(clearsIndex(undefined, next)).toBe(false);
  });
});

describe("sourceToFormValues", () => {
  it("hides a name that is only the uid default", () => {
    expect(sourceToFormValues({ ...stored, name: "tmdb" }).name).toBe("");
    expect(sourceToFormValues(stored).name).toBe("TMDB daily export");
  });

  it("round-trips the location", () => {
    const form = sourceToFormValues(stored);
    expect(sameLocation(formLocation(form), stored.location)).toBe(true);
  });
});

describe("sameLocation", () => {
  it("ignores header order and an explicit GET", () => {
    expect(
      sameLocation(
        { kind: "url", url: "u", method: "GET", headers: { A: "1", B: "2" } },
        { kind: "url", url: "u", headers: { B: "2", A: "1" } },
      ),
    ).toBe(true);
    expect(sameLocation({ kind: "url", url: "u" }, { kind: "url", url: "u", headers: {} })).toBe(
      true,
    );
    expect(sameLocation({ kind: "url", url: "u" }, { kind: "url", url: "v" })).toBe(false);
  });
});

describe("timezoneOptions", () => {
  it("adds the browser zone and the current value when they are uncommon", () => {
    const options = timezoneOptions("Pacific/Auckland", "Africa/Lagos");
    expect(options[0]).toBe("UTC");
    expect(options).toContain("Africa/Lagos");
    expect(options).toContain("Pacific/Auckland");
  });

  it("does not duplicate a common zone", () => {
    const options = timezoneOptions("Europe/Paris", "Europe/Paris");
    expect(options.filter((zone) => zone === "Europe/Paris")).toHaveLength(1);
  });
});
