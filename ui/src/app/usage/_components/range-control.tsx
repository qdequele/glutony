"use client";

/**
 * The date-range control: a preset picker, plus two day inputs once the preset
 * is "custom".
 *
 * Native `<input type="date">` rather than a calendar popover on purpose — the
 * value the endpoint wants *is* `YYYY-MM-DD`, so the native control needs no
 * parsing layer, and it keeps `react-day-picker` out of a bundle that ships
 * inside the gateway binary.
 */
import { CalendarRange } from "lucide-react";

import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { cn } from "@/lib/utils";
import type { UsageDateRange } from "@/lib/api/usage";
import { RANGE_PRESETS, rangeError, type RangePreset } from "../_lib/range";

export function RangeControl({
  preset,
  range,
  onPresetChange,
  onRangeChange,
  className,
}: {
  preset: RangePreset;
  range: UsageDateRange;
  onPresetChange: (preset: RangePreset) => void;
  onRangeChange: (range: UsageDateRange) => void;
  className?: string;
}) {
  const custom = preset === "custom";
  const error = rangeError(range);

  return (
    <div className={cn("flex flex-wrap items-center gap-2", className)}>
      <Select value={preset} onValueChange={(value) => onPresetChange(value as RangePreset)}>
        <SelectTrigger size="sm" aria-label="Date range preset" className="w-[9.5rem]">
          <CalendarRange aria-hidden />
          <SelectValue />
        </SelectTrigger>
        <SelectContent align="end">
          {RANGE_PRESETS.map((option) => (
            <SelectItem key={option.value} value={option.value}>
              {option.label}
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {custom ? (
        <div className="flex items-center gap-1.5">
          <Label htmlFor="usage-from" className="sr-only">
            Start date
          </Label>
          <Input
            id="usage-from"
            type="date"
            value={range.from}
            max={range.to}
            aria-invalid={error !== undefined}
            onChange={(event) => onRangeChange({ ...range, from: event.target.value })}
            className="h-7 w-[9.5rem] text-xs"
          />
          <span aria-hidden className="text-xs text-muted-foreground">
            →
          </span>
          <Label htmlFor="usage-to" className="sr-only">
            End date
          </Label>
          <Input
            id="usage-to"
            type="date"
            value={range.to}
            min={range.from}
            aria-invalid={error !== undefined}
            onChange={(event) => onRangeChange({ ...range, to: event.target.value })}
            className="h-7 w-[9.5rem] text-xs"
          />
        </div>
      ) : (
        // The resolved window is reassurance, not information the screen needs:
        // it is the first thing to drop when the header runs out of room, so the
        // page title keeps its description.
        <span className="hidden font-mono text-xs text-muted-foreground xl:inline">
          {range.from} → {range.to}
        </span>
      )}

      {error ? (
        <p role="alert" className="text-xs text-destructive">
          {error}
        </p>
      ) : null}
    </div>
  );
}
