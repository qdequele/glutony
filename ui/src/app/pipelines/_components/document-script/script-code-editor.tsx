"use client";

import { defaultKeymap, history, historyKeymap, indentWithTab } from "@codemirror/commands";
import { bracketMatching, defaultHighlightStyle, indentOnInput, syntaxHighlighting } from "@codemirror/language";
import { lintGutter, setDiagnostics } from "@codemirror/lint";
import { Compartment, EditorState } from "@codemirror/state";
import { EditorView, keymap, lineNumbers } from "@codemirror/view";
import { forwardRef, useEffect, useImperativeHandle, useRef } from "react";

import type { StepDiagnostic } from "@/lib/api/types";

import { diagnosticRange } from "./lint";
import { rhai } from "./rhai-language";

export interface ScriptCodeEditorHandle {
  insert(text: string): void;
  focus(): void;
}

export interface ScriptCodeEditorProps {
  value: string;
  onChange: (next: string) => void;
  /** Already filtered to `script` diagnostics. */
  diagnostics: StepDiagnostic[];
  readOnly?: boolean;
  /** Grow up to this many lines; `undefined` fills the parent's height. */
  maxLines?: number;
  ariaLabel: string;
}

function theme(maxLines: number | undefined) {
  return EditorView.theme({
    "&": {
      fontSize: "12px",
      height: maxLines === undefined ? "100%" : "auto",
      border: "1px solid var(--border)",
      borderRadius: "calc(var(--radius) - 2px)",
      backgroundColor: "var(--background)",
      color: "var(--foreground)",
    },
    "&.cm-focused": { outline: "2px solid var(--ring)", outlineOffset: "1px" },
    ".cm-content": { fontFamily: "var(--font-mono)" },
    ".cm-gutters": { backgroundColor: "var(--muted)", color: "var(--muted-foreground)", border: "none" },
    ".cm-scroller": maxLines === undefined ? { overflow: "auto" } : { maxHeight: `${maxLines * 1.45}em`, overflow: "auto" },
  });
}

/** The Code-mode editor. Mounted once; prop changes are pushed into the live view. */
export const ScriptCodeEditor = forwardRef<ScriptCodeEditorHandle, ScriptCodeEditorProps>(
  function ScriptCodeEditor({ value, onChange, diagnostics, readOnly = false, maxLines, ariaLabel }, ref) {
    const host = useRef<HTMLDivElement>(null);
    const view = useRef<EditorView | null>(null);
    const onChangeRef = useRef(onChange);
    const readOnlySlot = useRef(new Compartment());

    useEffect(() => {
      onChangeRef.current = onChange;
    }, [onChange]);

    useEffect(() => {
      if (!host.current) return;
      const editor = new EditorView({
        parent: host.current,
        state: EditorState.create({
          doc: value,
          extensions: [
            lineNumbers(),
            history(),
            indentOnInput(),
            bracketMatching(),
            syntaxHighlighting(defaultHighlightStyle, { fallback: true }),
            keymap.of([...defaultKeymap, ...historyKeymap, indentWithTab]),
            rhai,
            lintGutter(),
            readOnlySlot.current.of(EditorState.readOnly.of(readOnly)),
            EditorView.contentAttributes.of({ "aria-label": ariaLabel }),
            EditorView.updateListener.of((update) => {
              if (update.docChanged) onChangeRef.current(update.state.doc.toString());
            }),
            theme(maxLines),
          ],
        }),
      });
      view.current = editor;
      return () => {
        editor.destroy();
        view.current = null;
      };
      // Mounted once on purpose: later prop changes are dispatched by the effects below.
      // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    // An outside change (YAML pane, mode switch) replaces the document.
    useEffect(() => {
      const editor = view.current;
      if (!editor) return;
      const current = editor.state.doc.toString();
      if (current !== value) editor.dispatch({ changes: { from: 0, to: current.length, insert: value } });
    }, [value]);

    useEffect(() => {
      view.current?.dispatch({ effects: readOnlySlot.current.reconfigure(EditorState.readOnly.of(readOnly)) });
    }, [readOnly]);

    useEffect(() => {
      const editor = view.current;
      if (!editor) return;
      const text = editor.state.doc.toString();
      editor.dispatch(
        setDiagnostics(
          editor.state,
          diagnostics.map((d) => ({ ...diagnosticRange(text, d.line, d.column), severity: "error" as const, message: d.message })),
        ),
      );
    }, [diagnostics, value]);

    useImperativeHandle(ref, () => ({
      insert(text: string) {
        const editor = view.current;
        if (!editor) return;
        const { from } = editor.state.selection.main;
        const atLineStart = from === editor.state.doc.lineAt(from).from;
        editor.dispatch(editor.state.replaceSelection(`${atLineStart ? "" : "\n"}${text}\n`));
        editor.focus();
      },
      focus() {
        view.current?.focus();
      },
    }));

    return <div ref={host} className={maxLines === undefined ? "h-full min-h-0" : undefined} />;
  },
);
