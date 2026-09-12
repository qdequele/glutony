"use client";

import Link from "next/link";
import { useState } from "react";
import { Copy, Lock, MoreHorizontal, Pencil, Trash2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import type { PipelineDefinition } from "@/lib/api/types";
import { editPipelineHref, newPipelineHref } from "../routes";
import { DeletePipelineDialog } from "./delete-pipeline-dialog";

function TriggerCell({ pipeline }: { pipeline: PipelineDefinition }) {
  const types = pipeline.trigger?.content_types ?? [];
  if (types.length === 0) {
    return <span className="text-muted-foreground">—</span>;
  }
  const shown = types.slice(0, 3);
  return (
    <div className="flex flex-wrap items-center gap-1">
      {shown.map((type) => (
        <Badge key={type} variant="outline" className="font-mono text-[11px]">
          {type}
        </Badge>
      ))}
      {types.length > shown.length ? (
        <Tooltip>
          <TooltipTrigger asChild>
            <Badge variant="ghost" className="text-[11px]">
              +{types.length - shown.length}
            </Badge>
          </TooltipTrigger>
          <TooltipContent>{types.slice(3).join(", ")}</TooltipContent>
        </Tooltip>
      ) : null}
    </div>
  );
}

export function PipelineTable({ pipelines }: { pipelines: PipelineDefinition[] }) {
  const [toDelete, setToDelete] = useState<string | undefined>(undefined);

  return (
    <>
      <Table>
        <TableHeader>
          <TableRow>
            <TableHead className="w-[26%]">uid</TableHead>
            <TableHead className="w-[22%]">Name</TableHead>
            <TableHead className="w-[28%]">Trigger</TableHead>
            <TableHead className="w-16 text-right">Steps</TableHead>
            <TableHead className="w-20 text-right">Version</TableHead>
            <TableHead className="w-10" />
          </TableRow>
        </TableHeader>
        <TableBody>
          {pipelines.map((pipeline) => {
            const builtin = pipeline.builtin === true;
            return (
              <TableRow key={`${pipeline.project_id ?? ""}:${pipeline.uid}`}>
                <TableCell className="font-mono text-xs">
                  <div className="flex items-center gap-1.5">
                    <Link href={editPipelineHref(pipeline.uid)} className="hover:underline">
                      {pipeline.uid}
                    </Link>
                    {builtin ? (
                      <Badge variant="secondary" className="gap-1 text-[10px]">
                        <Lock aria-hidden /> built-in
                      </Badge>
                    ) : null}
                    {pipeline.project_id ? (
                      <Badge variant="outline" className="text-[10px]">
                        {pipeline.project_id}
                      </Badge>
                    ) : null}
                  </div>
                </TableCell>
                <TableCell className="truncate text-sm">
                  {pipeline.name || <span className="text-muted-foreground">—</span>}
                </TableCell>
                <TableCell>
                  <TriggerCell pipeline={pipeline} />
                </TableCell>
                <TableCell className="text-right tabular-nums">{pipeline.steps.length}</TableCell>
                <TableCell className="text-right tabular-nums text-muted-foreground">
                  {pipeline.version ?? 1}
                </TableCell>
                <TableCell>
                  <DropdownMenu>
                    <DropdownMenuTrigger asChild>
                      <Button
                        variant="ghost"
                        size="icon"
                        className="size-7"
                        aria-label={`Actions for ${pipeline.uid}`}
                      >
                        <MoreHorizontal aria-hidden />
                      </Button>
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      <DropdownMenuItem asChild>
                        <Link href={editPipelineHref(pipeline.uid)}>
                          <Pencil aria-hidden />
                          {builtin ? "Open (read-only)" : "Edit"}
                        </Link>
                      </DropdownMenuItem>
                      <DropdownMenuItem asChild>
                        <Link href={newPipelineHref(pipeline.uid)}>
                          <Copy aria-hidden />
                          Clone
                        </Link>
                      </DropdownMenuItem>
                      <DropdownMenuItem
                        variant="destructive"
                        // Built-ins are read-only. The API answers 403; the UI
                        // does not rely on that alone.
                        disabled={builtin}
                        onSelect={() => setToDelete(pipeline.uid)}
                      >
                        <Trash2 aria-hidden />
                        Delete
                      </DropdownMenuItem>
                    </DropdownMenuContent>
                  </DropdownMenu>
                </TableCell>
              </TableRow>
            );
          })}
        </TableBody>
      </Table>

      <DeletePipelineDialog
        uid={toDelete}
        open={toDelete !== undefined}
        onOpenChange={(open) => {
          if (!open) setToDelete(undefined);
        }}
      />
    </>
  );
}
