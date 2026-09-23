import { describe, expect, it } from "vitest";

import type { ConnectionView } from "@/lib/api/connections";
import {
  buildCreateConnectionBody,
  buildUpdateConnectionBody,
  connectionToFormValues,
} from "./form";

const stored: ConnectionView = {
  uid: "prod",
  name: "Production",
  host: "https://ms-1.meilisearch.io",
  api_key: "****",
  created_at: "2026-09-20T10:00:00Z",
  updated_at: "2026-09-20T10:00:00Z",
};

describe("buildCreateConnectionBody", () => {
  it("trims and omits an empty name", () => {
    expect(
      buildCreateConnectionBody({ uid: " prod ", name: " ", host: " https://h ", apiKey: "k" }),
    ).toEqual({ uid: "prod", host: "https://h", api_key: "k" });
  });

  it("sends a name when given", () => {
    expect(
      buildCreateConnectionBody({ uid: "p", name: "Prod", host: "https://h", apiKey: "k" }).name,
    ).toBe("Prod");
  });
});

describe("buildUpdateConnectionBody", () => {
  it("is empty when nothing changed, and never sends the masked key back", () => {
    expect(buildUpdateConnectionBody(stored, connectionToFormValues(stored))).toEqual({});
  });

  it("sends a new key only when one was typed", () => {
    const values = { ...connectionToFormValues(stored), apiKey: "new-key" };
    expect(buildUpdateConnectionBody(stored, values)).toEqual({ api_key: "new-key" });
  });

  it("ignores a trailing slash on the host but sends a real change", () => {
    const same = { ...connectionToFormValues(stored), host: "https://ms-1.meilisearch.io/ " };
    expect(buildUpdateConnectionBody(stored, same)).toEqual({});
    const moved = { ...connectionToFormValues(stored), host: "https://ms-2.meilisearch.io" };
    expect(buildUpdateConnectionBody(stored, moved)).toEqual({
      host: "https://ms-2.meilisearch.io",
    });
  });

  it("resets a cleared name to the uid", () => {
    const values = { ...connectionToFormValues(stored), name: "" };
    expect(buildUpdateConnectionBody(stored, values)).toEqual({ name: "prod" });
    expect(connectionToFormValues({ ...stored, name: "prod" }).name).toBe("");
  });
});
