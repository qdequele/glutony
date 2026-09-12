"use client";

import { useRef, useState, type DragEvent } from "react";
import { FileUp, X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { formatBytes } from "@/lib/api/jobs";
import { LARGE_UPLOAD_BYTES, isLargeUpload } from "@/lib/api/ingest";
import { cn } from "@/lib/utils";

export interface DropZoneProps {
  file: File | undefined;
  onFile: (file: File | undefined) => void;
  disabled?: boolean;
}

/**
 * Drag-and-drop plus a plain file picker.
 *
 * One file at a time: `POST /ingest` starts exactly one job, and batches go to
 * `POST /ingest/batch`, which the Playground deliberately does not expose —
 * this screen is for debugging a single pipeline run.
 */
export function DropZone({ file, onFile, disabled = false }: DropZoneProps) {
  const input = useRef<HTMLInputElement>(null);
  const [over, setOver] = useState(false);

  function accept(event: DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setOver(false);
    if (disabled) return;
    const dropped = event.dataTransfer.files?.[0];
    if (dropped) onFile(dropped);
  }

  return (
    <div className="space-y-2">
      <div
        onDragOver={(event) => {
          event.preventDefault();
          if (!disabled) setOver(true);
        }}
        onDragLeave={() => setOver(false)}
        onDrop={accept}
        className={cn(
          "flex flex-col items-center justify-center gap-2 rounded-md border border-dashed px-4 py-10 text-center transition-colors",
          over && "border-primary bg-primary/5",
          disabled && "opacity-50",
        )}
      >
        <FileUp aria-hidden className="size-5 text-muted-foreground" />
        <p className="text-sm">
          Drop a file here, or{" "}
          <Button
            type="button"
            variant="link"
            size="sm"
            className="h-auto p-0"
            disabled={disabled}
            onClick={() => input.current?.click()}
          >
            browse
          </Button>
          .
        </p>
        <p className="text-xs text-muted-foreground">
          The MIME type and filename are what auto-routing matches on.
        </p>
        <input
          ref={input}
          type="file"
          className="hidden"
          disabled={disabled}
          onChange={(event) => onFile(event.target.files?.[0])}
        />
      </div>

      {file ? (
        <div className="flex items-center gap-2 rounded-md border px-3 py-2 text-sm">
          <span className="truncate font-mono text-xs">{file.name}</span>
          <span className="ml-auto shrink-0 tabular-nums text-muted-foreground">
            {formatBytes(file.size)}
          </span>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-6 shrink-0"
            aria-label="Remove the selected file"
            disabled={disabled}
            onClick={() => {
              onFile(undefined);
              if (input.current) input.current.value = "";
            }}
          >
            <X aria-hidden />
          </Button>
        </div>
      ) : null}

      {file && isLargeUpload(file.size) ? (
        <p className="text-xs text-amber-700 dark:text-amber-400">
          Over {formatBytes(LARGE_UPLOAD_BYTES)}: the gateway stages this upload to the blob store
          instead of sending it inline, so the first step starts a little later. It still works.
        </p>
      ) : null}
    </div>
  );
}
