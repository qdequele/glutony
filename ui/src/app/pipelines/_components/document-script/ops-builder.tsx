"use client";

import { Plus } from "lucide-react";
import { useState } from "react";

import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuGroup,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import type { JsonValue, StepDiagnostic } from "@/lib/api/types";

import { diagnosticsForOp } from "./diagnostics";
import { GROUP_LABELS, newOp, OP_KINDS, type Op, type OpGroup } from "./ops";
import { OpRow } from "./op-row";
import { addOp, moveOp, removeOp, replaceOp } from "./rows";

export interface OpsBuilderProps {
  idPrefix: string;
  ops: JsonValue[];
  onChange: (ops: JsonValue[]) => void;
  diagnostics: StepDiagnostic[];
  readOnly: boolean;
}

const GROUPS = Object.keys(GROUP_LABELS) as OpGroup[];

let rowIdCounter = 0;
function nextRowId(): string {
  rowIdCounter += 1;
  return `row-${rowIdCounter}`;
}

export function OpsBuilder({ idPrefix, ops, onChange, diagnostics, readOnly }: OpsBuilderProps) {
  // A row's React key must stay attached to the *operation*, not to its array index — two
  // children (the raw-JSON textarea in OpRow, and MultiValueInput's draft) hold local state
  // that must not survive a reorder or delete landing a different op at the same index.
  const [rowIds, setRowIds] = useState<string[]>(() => ops.map(() => nextRowId()));

  // Adjust state during render (React's documented pattern for deriving state from props):
  // when the list was resized from outside these handlers (e.g. the YAML pane rewrote
  // config.ops), there is no id-to-op correspondence to preserve, so start fresh — and use the
  // fresh ids for this render's keys immediately, rather than the stale `rowIds`.
  let currentRowIds = rowIds;
  if (rowIds.length !== ops.length) {
    currentRowIds = ops.map(() => nextRowId());
    setRowIds(currentRowIds);
  }

  function handleAdd(op: Op) {
    setRowIds((ids) => [...ids, nextRowId()]);
    onChange(addOp(ops, op));
  }

  function handleRemove(index: number) {
    setRowIds((ids) => ids.filter((_, i) => i !== index));
    onChange(removeOp(ops, index));
  }

  function handleMove(index: number, direction: -1 | 1) {
    setRowIds((ids) => {
      const target = index + direction;
      if (target < 0 || target >= ids.length) return ids;
      const next = [...ids];
      [next[index], next[target]] = [next[target], next[index]];
      return next;
    });
    onChange(moveOp(ops, index, direction));
  }

  return (
    <div className="space-y-2">
      {ops.length === 0 ? (
        <p className="rounded-md border border-dashed py-6 text-center text-xs text-muted-foreground">
          No operations yet — add one, or pick from the reference.
        </p>
      ) : (
        ops.map((op, index) => (
          <OpRow
            key={currentRowIds[index]}
            idPrefix={idPrefix}
            index={index}
            total={ops.length}
            value={op}
            onChange={(next) => onChange(replaceOp(ops, index, next))}
            onMove={(direction) => handleMove(index, direction)}
            onRemove={() => handleRemove(index)}
            diagnostics={diagnosticsForOp(diagnostics, index)}
            readOnly={readOnly}
          />
        ))
      )}
      {readOnly ? null : (
        <DropdownMenu>
          <DropdownMenuTrigger asChild>
            <Button type="button" variant="outline" size="sm">
              <Plus /> Add operation
            </Button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start">
            {GROUPS.map((group, i) => (
              <DropdownMenuGroup key={group}>
                {i > 0 ? <DropdownMenuSeparator /> : null}
                <DropdownMenuLabel>{GROUP_LABELS[group]}</DropdownMenuLabel>
                {OP_KINDS.filter((info) => info.group === group).map((info) => (
                  <DropdownMenuItem key={info.kind} onSelect={() => handleAdd(newOp(info.kind))}>
                    {info.label}
                  </DropdownMenuItem>
                ))}
              </DropdownMenuGroup>
            ))}
          </DropdownMenuContent>
        </DropdownMenu>
      )}
    </div>
  );
}
