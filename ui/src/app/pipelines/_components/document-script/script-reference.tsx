"use client";

import { useState } from "react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";

import type { ScriptMode } from "./mode";
import {
  filterEntries,
  REFERENCE,
  REFERENCE_GROUP_LABELS,
  REFERENCE_GROUPS,
  type ReferenceEntry,
} from "./reference";

export function ScriptReference({
  mode,
  onUse,
  readOnly = false,
}: {
  mode: ScriptMode;
  onUse: (entry: ReferenceEntry) => void;
  readOnly?: boolean;
}) {
  const [query, setQuery] = useState("");
  const entries = filterEntries(REFERENCE.entries, query);

  return (
    <div className="flex h-full min-h-0 flex-col gap-3">
      <Input
        value={query}
        onChange={(event) => setQuery(event.target.value)}
        placeholder="Filter operations…"
        aria-label="Filter the reference"
      />
      <ScrollArea className="min-h-0 flex-1 pr-3 [&_[data-radix-scroll-area-viewport]>div]:!block">
        <div className="space-y-5 pb-4">
          {REFERENCE_GROUPS.map((group) => {
            const inGroup = entries.filter((entry) => entry.group === group);
            if (inGroup.length === 0) return null;
            return (
              <section key={group} className="space-y-2">
                <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                  {REFERENCE_GROUP_LABELS[group]}
                </h3>
                {inGroup.map((entry) => {
                  const usable = !readOnly && (mode === "code" || entry.op !== null);
                  return (
                    <article key={entry.id} className="space-y-1.5 rounded-md border p-2.5">
                      <div className="flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <p className="text-sm font-medium">{entry.title}</p>
                          <p className="text-xs text-muted-foreground">{entry.summary}</p>
                        </div>
                        <Button
                          type="button"
                          size="xs"
                          variant="outline"
                          disabled={!usable}
                          title={readOnly ? "Read-only step" : usable ? undefined : "Code mode only"}
                          onClick={() => onUse(entry)}
                          className="shrink-0"
                        >
                          {mode === "code" ? "Insert" : "Add"}
                        </Button>
                      </div>
                      <pre className="overflow-x-auto rounded bg-muted px-2 py-1.5 font-mono text-[11px] whitespace-pre-wrap break-words">
                        {entry.snippet}
                      </pre>
                    </article>
                  );
                })}
              </section>
            );
          })}

          <section className="space-y-2">
            <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Bindings</h3>
            <dl className="space-y-1.5 text-xs">
              {REFERENCE.bindings.map((binding) => (
                <div key={binding.name}>
                  <dt className="font-mono">
                    {binding.name} <span className="text-muted-foreground">· {binding.type}{binding.writable ? "" : " · read-only"}</span>
                  </dt>
                  <dd className="text-muted-foreground">{binding.note}</dd>
                </div>
              ))}
            </dl>
          </section>

          <section className="space-y-2">
            <h3 className="text-xs font-semibold uppercase tracking-wide text-muted-foreground">Gotchas</h3>
            {REFERENCE.gotchas.map((gotcha) => (
              <div key={gotcha.title} className="text-xs">
                <p className="font-medium">{gotcha.title}</p>
                <p className="text-muted-foreground">{gotcha.body}</p>
              </div>
            ))}
          </section>
        </div>
      </ScrollArea>
    </div>
  );
}
