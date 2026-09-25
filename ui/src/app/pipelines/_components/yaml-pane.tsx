"use client";

import { Check, Copy } from "lucide-react";
import { useEffect, useRef, useState, type ReactNode } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Tabs, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import type { PipelineDraft } from "@/lib/pipeline/draft";
import { draftToYaml, yamlToDraft } from "@/lib/pipeline/yaml";

type Mode = "graph" | "preview" | "edit";

/**
 * The graph and YAML views of the draft; YAML is also the second way to
 * author it.
 *
 * Preview always mirrors the form. In edit mode the text is local: every
 * keystroke is parsed, a valid document is pushed back into the form, and an
 * invalid one is reported without touching it — so a half-typed line never
 * wipes the pipeline. Edits made in the form meanwhile are folded back into
 * the text.
 */
export function YamlPane({
  draft,
  onDraftChange,
  readOnly,
  graph,
}: {
  draft: PipelineDraft;
  onDraftChange: (draft: PipelineDraft) => void;
  readOnly: boolean;
  /** Rendered under the Graph tab. */
  graph: ReactNode;
}) {
  const [mode, setMode] = useState<Mode>("graph");
  const rendered = draftToYaml(draft);
  const [text, setText] = useState(rendered);
  const [error, setError] = useState<string | undefined>(undefined);
  const [copied, setCopied] = useState(false);
  // The draft this pane last produced, so a change coming from the form is
  // told apart from the echo of our own edit.
  const emitted = useRef<PipelineDraft>(draft);

  useEffect(() => {
    if (JSON.stringify(draft) === JSON.stringify(emitted.current)) return;
    emitted.current = draft;
    setText(draftToYaml(draft));
    setError(undefined);
  }, [draft]);

  function handle(next: string) {
    setText(next);
    const parsed = yamlToDraft(next);
    if (!parsed.ok) {
      setError(parsed.error);
      return;
    }
    setError(undefined);
    emitted.current = parsed.draft;
    onDraftChange(parsed.draft);
  }

  async function copy() {
    try {
      await navigator.clipboard.writeText(mode === "edit" ? text : rendered);
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    } catch {
      // Clipboard permission denied; nothing useful to do.
    }
  }

  return (
    <div className="flex h-full min-h-0 flex-col">
      <div className="flex items-center gap-2 border-b px-3 py-2">
        <Tabs value={mode} onValueChange={(value) => setMode(value as Mode)}>
          <TabsList className="h-7">
            <TabsTrigger value="graph" className="text-xs">
              Graph
            </TabsTrigger>
            <TabsTrigger value="preview" className="text-xs">
              YAML
            </TabsTrigger>
            <TabsTrigger value="edit" className="text-xs" disabled={readOnly}>
              Edit
            </TabsTrigger>
          </TabsList>
        </Tabs>
        <p className="truncate text-xs text-muted-foreground">
          {mode === "edit"
            ? "Typing here rewrites the form."
            : mode === "graph"
              ? "Click a box to jump to its step."
              : "Mirrors the form."}
        </p>
        {mode === "graph" ? null : (
        <Button
          type="button"
          variant="ghost"
          size="icon"
          className="ml-auto size-7"
          aria-label="Copy YAML"
          onClick={copy}
        >
          {copied ? <Check aria-hidden /> : <Copy aria-hidden />}
        </Button>
        )}
      </div>

      {error ? (
        <Alert variant="destructive" className="m-3 mb-0 w-auto">
          <AlertTitle>YAML not applied</AlertTitle>
          <AlertDescription className="font-mono text-xs">{error}</AlertDescription>
        </Alert>
      ) : null}

      {mode === "graph" ? (
        <div className="min-h-0 flex-1 p-3">{graph}</div>
      ) : mode === "edit" ? (
        <Textarea
          value={text}
          spellCheck={false}
          aria-label="Pipeline YAML"
          aria-invalid={error !== undefined}
          className="m-3 min-h-0 flex-1 resize-none font-mono text-xs leading-relaxed"
          onChange={(event) => handle(event.target.value)}
        />
      ) : (
        <ScrollArea className="min-h-0 flex-1">
          <pre className="p-3 font-mono text-xs leading-relaxed whitespace-pre">{rendered}</pre>
        </ScrollArea>
      )}
    </div>
  );
}
