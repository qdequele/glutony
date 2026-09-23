"use client";

import { ArrowDown, ArrowUp, X } from "lucide-react";
import { useState, type ReactNode } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import type { JsonValue, StepDiagnostic } from "@/lib/api/types";
import { cn } from "@/lib/utils";

import { MultiValueInput } from "../multi-value-input";
import { routeDiagnostic } from "./diagnostics";
import { formatLiteral, opKind, OP_KINDS, parseDecimals, parseLiteral, type DropIf, type OpKind } from "./ops";

type FilterTest = "empty" | "missing" | "equals" | "not_equals";

export interface OpRowProps {
  idPrefix: string;
  index: number;
  total: number;
  value: JsonValue;
  onChange: (next: JsonValue) => void;
  onMove: (direction: -1 | 1) => void;
  onRemove: () => void;
  diagnostics: StepDiagnostic[];
  readOnly: boolean;
}

function invalidKey(diagnostics: StepDiagnostic[], key: string): boolean {
  return diagnostics.some((d) => {
    const target = routeDiagnostic(d);
    return target.kind === "op" && target.key !== undefined && target.key.startsWith(key);
  });
}

function filterTest(dropIf: DropIf): FilterTest {
  if (dropIf.equals !== undefined) return "equals";
  if (dropIf.not_equals !== undefined) return "not_equals";
  return dropIf.is ?? "empty";
}

interface DecimalsInputProps {
  id: string;
  value: number | undefined;
  disabled: boolean;
  invalid: boolean;
  onCommit: (decimals: number) => void;
}

/**
 * The round `decimals` input. Keeps its own text so it can be emptied mid-edit; only a
 * whole number 0–15 reaches the op, and leaving the input empty restores the stored one.
 */
function DecimalsInput({ id, value, disabled, invalid, onCommit }: DecimalsInputProps) {
  const stored = value === undefined ? "" : String(value);
  const [text, setText] = useState(stored);
  const [synced, setSynced] = useState(stored);
  if (stored !== synced) {
    // The op changed from outside (undo, YAML pane): show the new value.
    setSynced(stored);
    setText(stored);
  }
  return (
    <Input
      id={id}
      inputMode="numeric"
      value={text}
      disabled={disabled}
      aria-invalid={invalid}
      onChange={(event) => {
        const draft = parseDecimals(event.target.value);
        if (!draft) return;
        setText(draft.text);
        if (draft.value !== undefined) {
          setSynced(String(draft.value));
          onCommit(draft.value);
        }
      }}
      onBlur={() => setText(stored)}
    />
  );
}

export function OpRow({ idPrefix, index, total, value, onChange, onMove, onRemove, diagnostics, readOnly }: OpRowProps) {
  const kind = opKind(value);
  const id = `${idPrefix}-op-${index}`;
  const label = kind ? OP_KINDS.find((info) => info.kind === kind)?.label : "Unrecognised";
  const body = kind ? (value as Record<OpKind, JsonValue>)[kind] : undefined;
  const put = (next: JsonValue) => kind && onChange({ [kind]: next });
  const patch = (fields: Record<string, JsonValue>) => put({ ...(body as Record<string, JsonValue>), ...fields });
  const obj = (body ?? {}) as Record<string, JsonValue>;
  const text = (key: string) => (typeof obj[key] === "string" ? (obj[key] as string) : "");
  const field = (key: string, placeholder: string, mono = false) => (
    <Input
      id={`${id}-${key}`}
      value={text(key)}
      placeholder={placeholder}
      disabled={readOnly}
      aria-invalid={invalidKey(diagnostics, key)}
      className={cn(mono && "font-mono text-xs")}
      onChange={(event) => patch({ [key]: event.target.value })}
    />
  );

  let controls: ReactNode;
  switch (kind) {
    case "rename":
    case "copy":
      controls = (
        <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
          {field("from", "from")}
          <span aria-hidden className="text-muted-foreground">→</span>
          {field("to", "to")}
        </div>
      );
      break;
    case "drop":
    case "keep":
    case "trim":
    case "lowercase":
    case "uppercase":
      controls = (
        <MultiValueInput
          id={`${id}-names`}
          values={Array.isArray(body) ? body.filter((n): n is string => typeof n === "string") : []}
          placeholder="field name, then Enter"
          disabled={readOnly}
          onChange={(names) => put(names)}
        />
      );
      break;
    case "set":
      controls = (
        <div className="grid grid-cols-[1fr_auto_2fr] items-center gap-2">
          {field("field", "field")}
          <span aria-hidden className="text-muted-foreground">=</span>
          {field("expr", "price * qty", true)}
        </div>
      );
      break;
    case "filter": {
      const dropIf = (obj.drop_if ?? { field: "" }) as unknown as DropIf;
      const test = filterTest(dropIf);
      const setDropIf = (next: DropIf) => put({ drop_if: next as unknown as JsonValue });
      controls = (
        <div className="grid grid-cols-[auto_1fr_auto_1fr] items-center gap-2">
          <span className="text-xs text-muted-foreground">drop if</span>
          <Input
            id={`${id}-field`}
            value={dropIf.field}
            placeholder="content"
            disabled={readOnly}
            aria-invalid={invalidKey(diagnostics, "drop_if")}
            onChange={(event) => setDropIf({ ...dropIf, field: event.target.value })}
          />
          <Select
            value={test}
            disabled={readOnly}
            onValueChange={(next: FilterTest) =>
              setDropIf(
                next === "empty" || next === "missing"
                  ? { field: dropIf.field, is: next }
                  : { field: dropIf.field, [next]: dropIf.equals ?? dropIf.not_equals ?? "" },
              )
            }
          >
            <SelectTrigger id={`${id}-test`} className="w-36"><SelectValue /></SelectTrigger>
            <SelectContent>
              <SelectItem value="empty">is empty</SelectItem>
              <SelectItem value="missing">is missing</SelectItem>
              <SelectItem value="equals">equals</SelectItem>
              <SelectItem value="not_equals">does not equal</SelectItem>
            </SelectContent>
          </Select>
          {test === "equals" || test === "not_equals" ? (
            <Input
              id={`${id}-value`}
              value={formatLiteral(dropIf[test])}
              placeholder="draft"
              disabled={readOnly}
              onChange={(event) => setDropIf({ field: dropIf.field, [test]: parseLiteral(event.target.value) })}
            />
          ) : (
            <span />
          )}
        </div>
      );
      break;
    }
    case "split":
    case "join":
      controls = (
        <div className="grid grid-cols-[2fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">on</span>
          {field("separator", ",", true)}
        </div>
      );
      break;
    case "promote":
    case "cast": {
      const options = kind === "promote" ? ["title", "content"] : ["number", "string", "boolean"];
      controls = (
        <div className="grid grid-cols-[2fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">{kind === "promote" ? "into" : "to"}</span>
          <Select value={text("to")} disabled={readOnly} onValueChange={(next) => patch({ to: next })}>
            <SelectTrigger id={`${id}-to`}><SelectValue /></SelectTrigger>
            <SelectContent>
              {options.map((option) => <SelectItem key={option} value={option}>{option}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
      );
      break;
    }
    case "round":
      controls = (
        <div className="grid grid-cols-[2fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">decimals</span>
          <DecimalsInput
            id={`${id}-decimals`}
            value={typeof obj.decimals === "number" ? obj.decimals : undefined}
            disabled={readOnly}
            invalid={invalidKey(diagnostics, "decimals")}
            onCommit={(decimals) => patch({ decimals })}
          />
        </div>
      );
      break;
    case "default":
      controls = (
        <div className="grid grid-cols-[1fr_auto_1fr] items-center gap-2">
          {field("field", "field")}
          <span className="text-xs text-muted-foreground">=</span>
          <Input
            id={`${id}-value`}
            value={formatLiteral(obj.value)}
            placeholder="EUR"
            disabled={readOnly}
            onChange={(event) => patch({ value: parseLiteral(event.target.value) })}
          />
        </div>
      );
      break;
    default:
      controls = (
        <Textarea
          key={JSON.stringify(value)}
          id={`${id}-raw`}
          className="font-mono text-xs"
          rows={3}
          disabled={readOnly}
          defaultValue={JSON.stringify(value, null, 2)}
          onBlur={(event) => {
            try {
              onChange(JSON.parse(event.target.value) as JsonValue);
            } catch {
              // Keep the text; the server check reports the operation as invalid.
            }
          }}
        />
      );
  }

  return (
    <div className={cn("rounded-md border p-2", diagnostics.length > 0 && "border-destructive")}>
      <div className="flex items-start gap-2">
        <Badge variant="secondary" className="mt-1.5 w-20 justify-center">{label}</Badge>
        <div className="min-w-0 flex-1">{controls}</div>
        {readOnly ? null : (
          <div className="flex shrink-0 items-center">
            <Button type="button" variant="ghost" size="icon" aria-label="Move up" disabled={index === 0} onClick={() => onMove(-1)}>
              <ArrowUp />
            </Button>
            <Button type="button" variant="ghost" size="icon" aria-label="Move down" disabled={index === total - 1} onClick={() => onMove(1)}>
              <ArrowDown />
            </Button>
            <Button type="button" variant="ghost" size="icon" aria-label="Remove operation" onClick={onRemove}>
              <X />
            </Button>
          </div>
        )}
      </div>
      {diagnostics.length > 0 ? (
        <ul className="mt-1.5 space-y-0.5 pl-[5.5rem] text-xs text-destructive">
          {diagnostics.map((d, i) => <li key={i}>{d.message}</li>)}
        </ul>
      ) : null}
    </div>
  );
}
