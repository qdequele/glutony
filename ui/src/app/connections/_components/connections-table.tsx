"use client";

import { formatDistanceToNow } from "date-fns";
import { MoreHorizontal, Pencil, Trash2 } from "lucide-react";

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
import type { ConnectionView } from "@/lib/api/connections";

function Created({ iso }: { iso: string }) {
  const date = new Date(iso);
  if (Number.isNaN(date.getTime())) return <span className="text-muted-foreground">—</span>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <span className="text-muted-foreground">
          {formatDistanceToNow(date, { addSuffix: true })}
        </span>
      </TooltipTrigger>
      <TooltipContent>{date.toLocaleString()}</TooltipContent>
    </Tooltip>
  );
}

export interface ConnectionsTableProps {
  connections: ConnectionView[];
  onEdit: (connection: ConnectionView) => void;
  onDelete: (uid: string) => void;
}

export function ConnectionsTable({ connections, onEdit, onDelete }: ConnectionsTableProps) {
  return (
    <Table>
      <TableHeader>
        <TableRow>
          <TableHead className="w-[24%]">uid</TableHead>
          <TableHead className="w-[22%]">Name</TableHead>
          <TableHead className="w-[36%]">Host</TableHead>
          <TableHead className="w-[14%]">Created</TableHead>
          <TableHead className="w-10" />
        </TableRow>
      </TableHeader>
      <TableBody>
        {connections.map((connection) => (
          <TableRow key={`${connection.project_id ?? ""}:${connection.uid}`}>
            <TableCell className="font-mono text-xs">
              <div className="flex items-center gap-1.5">
                <button
                  type="button"
                  className="hover:underline"
                  onClick={() => onEdit(connection)}
                >
                  {connection.uid}
                </button>
                {connection.project_id ? (
                  <Badge variant="outline" className="text-[10px]">
                    {connection.project_id}
                  </Badge>
                ) : null}
              </div>
            </TableCell>
            <TableCell className="truncate text-sm">
              {connection.name && connection.name !== connection.uid ? (
                connection.name
              ) : (
                <span className="text-muted-foreground">—</span>
              )}
            </TableCell>
            <TableCell className="truncate font-mono text-xs">{connection.host}</TableCell>
            <TableCell className="text-xs">
              <Created iso={connection.created_at} />
            </TableCell>
            <TableCell>
              <DropdownMenu>
                <DropdownMenuTrigger asChild>
                  <Button
                    variant="ghost"
                    size="icon"
                    className="size-7"
                    aria-label={`Actions for ${connection.uid}`}
                  >
                    <MoreHorizontal aria-hidden />
                  </Button>
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">
                  <DropdownMenuItem onSelect={() => onEdit(connection)}>
                    <Pencil aria-hidden />
                    Edit
                  </DropdownMenuItem>
                  <DropdownMenuItem variant="destructive" onSelect={() => onDelete(connection.uid)}>
                    <Trash2 aria-hidden />
                    Delete
                  </DropdownMenuItem>
                </DropdownMenuContent>
              </DropdownMenu>
            </TableCell>
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}
