/**
 * The source form's zod schema. Built per form because two rules depend on
 * what is stored: the uid is only checked on create, and the credential diff
 * ({@link authChange}) needs the redacted credential to know what "left blank"
 * means.
 */
import { z } from "zod";

import { uidError } from "@/lib/api/connections";
import type { SourceView } from "@/lib/api/sources";
import { authChange } from "./auth";
import { validateCronShape } from "./cron";
import { clearsIndex, type SourceFormValues } from "./form";

const headerRow = z.object({ name: z.string(), value: z.string() });

/** Field paths a server-side 422 can be pinned to. */
export type SourceErrorField = "pipeline" | "cron" | "timezone" | "url" | "index" | "uid";

export function sourceFormSchema(mode: "create" | "edit", stored: SourceView | undefined) {
  return z
    .object({
      uid: z.string(),
      name: z.string(),
      description: z.string(),
      pipeline: z.string().min(1, "Pick the pipeline to run."),
      url: z.string(),
      method: z.string(),
      headers: z.array(headerRow),
      cron: z.string(),
      timezone: z.string().min(1, "Pick a timezone."),
      index: z.string(),
      auth: z.object({
        kind: z.enum(["none", "bearer", "basic", "headers", "keep"]),
        token: z.string(),
        username: z.string(),
        password: z.string(),
        headers: z.array(headerRow),
      }),
    })
    .superRefine((values: SourceFormValues, ctx) => {
      if (mode === "create") {
        const uid = uidError(values.uid.trim());
        if (uid) ctx.addIssue({ code: "custom", path: ["uid"], message: uid });
      }

      const url = values.url.trim();
      if (url.length === 0) {
        ctx.addIssue({ code: "custom", path: ["url"], message: "Enter the URL to fetch." });
      } else if (!/^https?:\/\//i.test(url)) {
        ctx.addIssue({ code: "custom", path: ["url"], message: "Must be an http(s) URL." });
      }

      const headerNames = values.headers.map((row) => row.name.trim()).filter(Boolean);
      if (new Set(headerNames.map((name) => name.toLowerCase())).size !== headerNames.length) {
        ctx.addIssue({ code: "custom", path: ["headers"], message: "A header is listed twice." });
      }

      const cron = validateCronShape(values.cron);
      if (cron) ctx.addIssue({ code: "custom", path: ["cron"], message: cron });

      if (clearsIndex(stored, values)) {
        ctx.addIssue({
          code: "custom",
          path: ["index"],
          message: "The API cannot remove an index override yet — set another index instead.",
        });
      }

      const auth = authChange(stored?.auth, values.auth);
      if (auth.kind === "invalid") {
        ctx.addIssue({ code: "custom", path: ["auth", auth.field], message: auth.message });
      }
    });
}

/**
 * Which field a gateway 422 is about, from its message, so the form can flag
 * it. `undefined` when the message names none of them (it is still shown).
 */
export function fieldForServerError(message: string): SourceErrorField | undefined {
  const text = message.toLowerCase();
  if (text.includes("cron")) return "cron";
  if (text.includes("timezone")) return "timezone";
  if (text.includes("pipeline") || text.includes("connection")) return "pipeline";
  if (
    text.includes("location") ||
    text.includes("url") ||
    text.includes("host") ||
    text.includes("template")
  ) {
    return "url";
  }
  if (text.includes("index")) return "index";
  if (text.includes("uid") || text.includes("already exists")) return "uid";
  return undefined;
}
