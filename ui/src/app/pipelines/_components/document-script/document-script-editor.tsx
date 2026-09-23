"use client";

import { BookOpen, Check, Code2, Loader2, Maximize2, Rows3 } from "lucide-react";
import type { ReactNode } from "react";
import { useEffect, useMemo, useRef, useState } from "react";

import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Sheet, SheetContent, SheetDescription, SheetHeader, SheetTitle, SheetTrigger } from "@/components/ui/sheet";
import { cn } from "@/lib/utils";

import type { ConfigEditorProps } from "../config-editors";
import { configDiagnostics, scriptDiagnostics, stepCheckState } from "./diagnostics";
import { generateScript } from "./generate-script";
import { deriveMode, lineCount, needsInitialOps, toBuilder, toCode, withInitialOps, type ScriptMode } from "./mode";
import { OpsBuilder } from "./ops-builder";
import type { ReferenceEntry } from "./reference";
import { addOp, readOps, writeOps } from "./rows";
import { ScriptCodeEditor, type ScriptCodeEditorHandle } from "./script-code-editor";
import { ScriptReference } from "./script-reference";

type Confirm = "to-builder" | "to-code-empty" | null;

export function DocumentScriptEditor({
  stepId,
  idPrefix,
  config,
  onChange,
  diagnostics,
  serverState,
  readOnly,
}: ConfigEditorProps) {
  const mode = deriveMode(config);
  const [switching, setSwitching] = useState(false);
  const [confirm, setConfirm] = useState<Confirm>(null);
  const [expanded, setExpanded] = useState(false);
  const codeRef = useRef<ScriptCodeEditorHandle>(null);

  // Always holds the latest config, so an in-flight `generateScript` request (below)
  // can tell whether the ops it was generated from are still current when it resolves.
  const latest = useRef(config);
  useEffect(() => {
    latest.current = config;
  }, [config]);

  // A freshly added step has neither key: start it in the builder with no operations.
  useEffect(() => {
    if (!readOnly && needsInitialOps(config)) onChange(withInitialOps(config));
  }, [config, onChange, readOnly]);

  async function switchTo(next: ScriptMode) {
    if (next === mode || readOnly || switching) return;
    if (next === "builder") {
      setConfirm("to-builder");
      return;
    }
    setSwitching(true);
    const requested = config.ops;
    try {
      const script = await generateScript(stepId, config);
      // The ops may have changed while the request was in flight (an edit to the
      // builder, or to another config field): a stale result must never overwrite
      // whatever the user has since typed.
      if (JSON.stringify(latest.current.ops) !== JSON.stringify(requested)) return;
      if (script === undefined) setConfirm("to-code-empty");
      else onChange(toCode(latest.current, script));
    } finally {
      setSwitching(false);
    }
  }

  function applyEntry(entry: ReferenceEntry) {
    if (readOnly) return;
    if (mode === "builder") {
      if (entry.op) onChange(writeOps(config, addOp(readOps(config), entry.op)));
    } else {
      codeRef.current?.insert(entry.snippet);
    }
  }

  const state = stepCheckState(serverState, diagnostics);
  const general = configDiagnostics(diagnostics);
  // Memoized so the code editor's diagnostics effect (which re-dispatches on every new
  // array identity) doesn't re-dispatch unchanged diagnostics on each keystroke.
  const scriptDiags = useMemo(() => scriptDiagnostics(diagnostics), [diagnostics]);

  function body(fill: boolean) {
    return mode === "builder" ? (
      <OpsBuilder
        idPrefix={idPrefix}
        ops={readOps(config)}
        onChange={(ops) => onChange(writeOps(config, ops))}
        diagnostics={diagnostics}
        readOnly={readOnly || switching}
      />
    ) : (
      <ScriptCodeEditor
        ref={codeRef}
        value={typeof config.script === "string" ? config.script : ""}
        onChange={(script) => onChange({ ...config, script })}
        diagnostics={scriptDiags}
        readOnly={readOnly}
        maxLines={fill ? undefined : 20}
        ariaLabel={`Script for step ${stepId}`}
      />
    );
  }

  return (
    <div className="space-y-2">
      <div className="flex flex-wrap items-center gap-2">
        <div role="group" aria-label="Authoring mode" className="inline-flex rounded-md border p-0.5">
          {(["builder", "code"] as const).map((option) => (
            <Button
              key={option}
              type="button"
              size="xs"
              variant={mode === option ? "secondary" : "ghost"}
              aria-pressed={mode === option}
              disabled={readOnly || switching}
              onClick={() => void switchTo(option)}
            >
              {option === "builder" ? <Rows3 /> : <Code2 />}
              {option === "builder" ? "Builder" : "Code"}
              {switching && option === "code" ? <Loader2 className="animate-spin" /> : null}
            </Button>
          ))}
        </div>
        <span
          className={cn(
            "text-xs",
            state === "invalid" ? "text-destructive" : "text-muted-foreground",
          )}
        >
          {state === "checking" ? "checking…" : null}
          {state === "ok" ? (
            <span className="inline-flex items-center gap-1"><Check className="size-3" /> valid</span>
          ) : null}
          {state === "invalid" ? `${diagnostics.length} ${diagnostics.length === 1 ? "error" : "errors"}` : null}
        </span>
        <div className="ml-auto flex items-center gap-1">
          <ReferenceButton mode={mode} onUse={applyEntry} readOnly={readOnly} />
          <Button type="button" variant="ghost" size="xs" onClick={() => setExpanded(true)}>
            <Maximize2 /> Expand
          </Button>
        </div>
      </div>

      {general.length > 0 ? (
        <ul className="space-y-0.5 text-xs text-destructive">
          {general.map((d, i) => <li key={i}>{d.message}</li>)}
        </ul>
      ) : null}

      {expanded ? (
        <p className="rounded-md border border-dashed py-4 text-center text-xs text-muted-foreground">
          Editing in the expanded view.
        </p>
      ) : (
        body(false)
      )}

      <ExpandedEditor open={expanded} onOpenChange={setExpanded} mode={mode} onUse={applyEntry} readOnly={readOnly}>
        {body(true)}
      </ExpandedEditor>

      <Dialog open={confirm !== null} onOpenChange={(open) => (open ? null : setConfirm(null))}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{confirm === "to-builder" ? "Discard the script?" : "Start with an empty script?"}</DialogTitle>
            <DialogDescription>
              {confirm === "to-builder"
                ? `This discards your script (${lineCount(config.script)} lines). The builder can't read free-form code.`
                : "The script for these operations couldn't be generated right now. Code mode would start empty and your operations would be discarded."}
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button type="button" variant="outline" onClick={() => setConfirm(null)}>Cancel</Button>
            <Button
              type="button"
              variant="destructive"
              onClick={() => {
                onChange(confirm === "to-builder" ? toBuilder(config) : toCode(config, ""));
                setConfirm(null);
              }}
            >
              Discard and switch
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}

function ReferenceButton({
  mode,
  onUse,
  readOnly,
}: {
  mode: ScriptMode;
  onUse: (entry: ReferenceEntry) => void;
  readOnly: boolean;
}) {
  return (
    <Sheet>
      <SheetTrigger asChild>
        <Button type="button" variant="ghost" size="xs">
          <BookOpen /> Reference
        </Button>
      </SheetTrigger>
      <SheetContent side="right" className="flex w-full flex-col sm:max-w-md">
        <SheetHeader>
          <SheetTitle>Reference</SheetTitle>
          <SheetDescription>
            {readOnly
              ? "Read-only pipeline — browse only."
              : mode === "code"
                ? "Insert a snippet at the cursor."
                : "Add an operation to the builder."}
          </SheetDescription>
        </SheetHeader>
        <div className="min-h-0 flex-1 px-4 pb-4">
          <ScriptReference mode={mode} onUse={onUse} readOnly={readOnly} />
        </div>
      </SheetContent>
    </Sheet>
  );
}

function ExpandedEditor({
  open,
  onOpenChange,
  mode,
  onUse,
  readOnly,
  children,
}: {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  mode: ScriptMode;
  onUse: (entry: ReferenceEntry) => void;
  readOnly: boolean;
  children: ReactNode;
}) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="flex h-[90vh] max-w-[min(1200px,95vw)] flex-col sm:max-w-[min(1200px,95vw)]">
        <DialogHeader>
          <DialogTitle>Document script</DialogTitle>
          <DialogDescription>{mode === "code" ? "Code mode" : "Builder mode"}</DialogDescription>
        </DialogHeader>
        <div className="grid min-h-0 flex-1 gap-4 md:grid-cols-[minmax(0,1fr)_22rem]">
          <div className="min-h-0 overflow-auto">{children}</div>
          <div className="min-h-0 border-t pt-4 md:border-t-0 md:border-l md:pt-0 md:pl-4">
            <ScriptReference mode={mode} onUse={onUse} readOnly={readOnly} />
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}
