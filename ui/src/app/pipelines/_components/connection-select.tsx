"use client";

/**
 * Schema-form widget for `format: "meili-connection"` — the `connection` key of
 * a `meili_indexer` step. A Select of the tenant's connections instead of a
 * free-text input, with a "none" entry that removes the key so the step falls
 * back to the Meilisearch sent with the request.
 */
import Link from "next/link";

import { FieldDescription } from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectSeparator,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { CONNECTIONS_HREF } from "@/app/connections/routes";
import { ApiError } from "@/lib/api/client";
import { useConnections } from "@/lib/api/connections";
import { errorMessage } from "@/lib/api/hooks";
import type { SchemaWidgetProps, SchemaWidgets } from "@/lib/schema-form";

/** Sentinel for "no connection": Radix Select forbids an empty value. */
const NONE = "__none__";

export function ConnectionSelect({ value, onChange, disabled, inputId }: SchemaWidgetProps) {
  const connections = useConnections();
  const current = typeof value === "string" && value.trim().length > 0 ? value : undefined;
  const list = connections.data ?? [];
  // A pipeline can name a connection that was deleted since, or that this
  // tenant cannot see: keep it selectable rather than silently dropping it.
  const missing = current !== undefined && !list.some((entry) => entry.uid === current);
  const notConfigured = connections.error instanceof ApiError && connections.error.isNotConfigured;

  return (
    <>
      <Select
        value={current ?? NONE}
        disabled={disabled}
        onValueChange={(next) => onChange(next === NONE ? undefined : next)}
      >
        <SelectTrigger id={inputId} className="w-full">
          <SelectValue placeholder={connections.isPending ? "Loading connections…" : undefined} />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={NONE}>
            <span className="text-muted-foreground">None — use the request&rsquo;s Meilisearch</span>
          </SelectItem>
          {list.length > 0 || missing ? <SelectSeparator /> : null}
          {list.map((entry) => (
            <SelectItem key={entry.uid} value={entry.uid}>
              <span className="font-mono text-xs">{entry.uid}</span>
              <span className="truncate text-xs text-muted-foreground">{entry.host}</span>
            </SelectItem>
          ))}
          {missing ? (
            <SelectItem value={current}>
              <span className="font-mono text-xs">{current}</span>
              <span className="text-xs text-destructive">not found</span>
            </SelectItem>
          ) : null}
        </SelectContent>
      </Select>
      {notConfigured ? (
        <FieldDescription className="text-xs">
          Connections are not enabled on this deployment (no{" "}
          <span className="font-mono">SOURCE_SECRET_KEY</span>).
        </FieldDescription>
      ) : connections.error ? (
        <FieldDescription className="text-xs text-destructive">
          Could not load connections: {errorMessage(connections.error)}
        </FieldDescription>
      ) : connections.data && list.length === 0 ? (
        <FieldDescription className="text-xs">
          No connection yet.{" "}
          <Link href={CONNECTIONS_HREF} className="underline underline-offset-2">
            Create one
          </Link>{" "}
          to pin this step to a Meilisearch.
        </FieldDescription>
      ) : null}
    </>
  );
}

/** Widgets the step editor hands to every step's config form. */
export const STEP_CONFIG_WIDGETS: SchemaWidgets = {
  "meili-connection": ConnectionSelect,
};
