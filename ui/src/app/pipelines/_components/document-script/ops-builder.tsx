"use client";

import { Plus } from "lucide-react";

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
import { GROUP_LABELS, newOp, OP_KINDS, type OpGroup } from "./ops";
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

export function OpsBuilder({ idPrefix, ops, onChange, diagnostics, readOnly }: OpsBuilderProps) {
  return (
    <div className="space-y-2">
      {ops.length === 0 ? (
        <p className="rounded-md border border-dashed py-6 text-center text-xs text-muted-foreground">
          No operations yet — add one, or pick from the reference.
        </p>
      ) : (
        ops.map((op, index) => (
          <OpRow
            key={index}
            idPrefix={idPrefix}
            index={index}
            total={ops.length}
            value={op}
            onChange={(next) => onChange(replaceOp(ops, index, next))}
            onMove={(direction) => onChange(moveOp(ops, index, direction))}
            onRemove={() => onChange(removeOp(ops, index))}
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
                  <DropdownMenuItem key={info.kind} onSelect={() => onChange(addOp(ops, newOp(info.kind)))}>
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
