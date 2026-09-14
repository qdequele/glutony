import type { PipelineTrigger } from "@/lib/api/types";

/**
 * How a workflow starts, in one line.
 *
 * This is the whole of the trigger story: rather than a third marketplace for
 * what is currently two concepts, each workflow states its own trigger where it
 * is actually authored.
 */
export function TriggerLine({ trigger }: { trigger: PipelineTrigger | undefined }) {
  const types = trigger?.content_types ?? [];
  const pattern = trigger?.filename_pattern;

  if (types.length === 0 && !pattern) {
    return (
      <p className="text-xs text-muted-foreground">
        Explicit call only — <code className="font-mono">POST /ingest/pipeline/…</code>
      </p>
    );
  }

  return (
    <p className="text-xs text-muted-foreground">
      Runs automatically for{" "}
      {types.length === 0 ? "files " : null}
      {types.map((type, index) => (
        <span key={type}>
          {index > 0 ? ", " : ""}
          <code className="font-mono">{type}</code>
        </span>
      ))}
      {pattern ? (
        <>
          {types.length > 0 ? " " : ""}matching <code className="font-mono">{pattern}</code>
        </>
      ) : null}
    </p>
  );
}
