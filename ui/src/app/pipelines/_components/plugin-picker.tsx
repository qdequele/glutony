"use client";

import { Check, ChevronsUpDown, Search } from "lucide-react";
import { useMemo, useRef, useState, type KeyboardEvent } from "react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetHeader,
  SheetTitle,
} from "@/components/ui/sheet";
import { Switch } from "@/components/ui/switch";
import { useCatalog } from "@/lib/api/hooks";
import type { ActionCategory, InputKind, PluginManifest } from "@/lib/api/types";
import { ACTION_CATEGORIES } from "@/lib/api/types";
import {
  buildPluginOptions,
  searchPluginOptions,
  type PluginOption,
} from "@/lib/pipeline/plugin-search";
import { cn } from "@/lib/utils";

/**
 * Plugin chooser: a trigger shaped like a select that opens a searchable sheet.
 *
 * The sheet has room for what a dropdown cannot show — catalog title, summary,
 * accepts → produces, and whether the plugin can read what the step upstream
 * hands over.
 */
export function PluginPicker({
  id,
  value,
  plugins,
  onChange,
  disabled,
  stepId,
  required,
}: {
  id: string;
  value: string;
  plugins: PluginManifest[];
  onChange: (name: string) => void;
  disabled?: boolean;
  /** Shown in the sheet header so it is clear which step is being edited. */
  stepId?: string;
  /** What this step will be handed, when known; drives the compatibility hints. */
  required?: InputKind;
}) {
  const [open, setOpen] = useState(false);
  const catalog = useCatalog();
  const current = plugins.find((plugin) => plugin.name === value);
  const title = catalog.data?.actions.find((entry) => entry.plugin === value)?.title;

  return (
    <>
      <Button
        id={id}
        type="button"
        variant="outline"
        disabled={disabled}
        aria-haspopup="dialog"
        aria-expanded={open}
        className="h-8 w-full min-w-0 justify-between gap-2 px-2.5 font-normal"
        onClick={() => setOpen(true)}
      >
        {value ? (
          <span className="flex min-w-0 items-baseline gap-2">
            <span className="truncate font-mono text-xs">{value}</span>
            {title && title !== value ? (
              <span className="hidden truncate text-xs text-muted-foreground xl:inline">{title}</span>
            ) : !current ? (
              <span className="truncate text-xs text-destructive">not registered</span>
            ) : null}
          </span>
        ) : (
          <span className="text-muted-foreground">Choose a plugin…</span>
        )}
        <ChevronsUpDown className="size-3.5 shrink-0 opacity-50" aria-hidden />
      </Button>

      <Sheet open={open} onOpenChange={setOpen}>
        <SheetContent className="w-full gap-0 sm:max-w-lg">
          {open ? (
            <PluginSheetBody
              value={value}
              plugins={plugins}
              actions={catalog.data?.actions ?? []}
              stepId={stepId}
              required={required}
              onPick={(name) => {
                onChange(name);
                setOpen(false);
              }}
            />
          ) : null}
        </SheetContent>
      </Sheet>
    </>
  );
}

function PluginSheetBody({
  value,
  plugins,
  actions,
  stepId,
  required,
  onPick,
}: {
  value: string;
  plugins: PluginManifest[];
  actions: Parameters<typeof buildPluginOptions>[1];
  stepId?: string;
  required?: InputKind;
  onPick: (name: string) => void;
}) {
  const [query, setQuery] = useState("");
  const [category, setCategory] = useState<ActionCategory | undefined>(undefined);
  const [compatibleOnly, setCompatibleOnly] = useState(false);
  const [active, setActive] = useState(0);
  const list = useRef<HTMLDivElement>(null);

  const options = useMemo(
    () => buildPluginOptions(plugins, actions, required),
    [plugins, actions, required],
  );
  const results = useMemo(
    () => searchPluginOptions(options, query, { category, compatibleOnly }),
    [options, query, category, compatibleOnly],
  );
  const categories = ACTION_CATEGORIES.filter((entry) =>
    options.some((option) => option.category === entry),
  );
  // A pipeline can reference a plugin no worker registered; say so rather than
  // pretending the current value is one of the options.
  const missing = value.length > 0 && !plugins.some((plugin) => plugin.name === value);
  const highlighted = Math.min(active, Math.max(0, results.length - 1));

  function move(delta: number) {
    if (results.length === 0) return;
    const next = (highlighted + delta + results.length) % results.length;
    setActive(next);
    list.current
      ?.querySelector<HTMLElement>(`[data-option-index="${next}"]`)
      ?.scrollIntoView({ block: "nearest" });
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "ArrowDown") {
      event.preventDefault();
      move(1);
    } else if (event.key === "ArrowUp") {
      event.preventDefault();
      move(-1);
    } else if (event.key === "Enter") {
      event.preventDefault();
      const choice = results[highlighted];
      if (choice) onPick(choice.manifest.name);
    }
  }

  return (
    <>
      <SheetHeader className="border-b pr-12">
        <SheetTitle>Choose a plugin</SheetTitle>
        <SheetDescription>
          {stepId ? (
            <>
              For step <span className="font-mono">{stepId}</span>
              {required ? (
                <>
                  , which receives <span className="font-mono">{required}</span>
                </>
              ) : (
                <>, which reads the ingest payload</>
              )}
              .
            </>
          ) : (
            "Pick what this step runs."
          )}
        </SheetDescription>
      </SheetHeader>

      <div className="space-y-3 border-b p-4">
        <div className="relative">
          <Search
            className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground"
            aria-hidden
          />
          <Input
            type="search"
            autoFocus
            value={query}
            placeholder="Search name, format, use case…"
            aria-label="Search plugins"
            aria-controls="plugin-options"
            className="pl-8"
            onChange={(event) => {
              setQuery(event.target.value);
              setActive(0);
            }}
            onKeyDown={onKeyDown}
          />
        </div>

        <div className="flex flex-wrap items-center gap-1.5">
          <CategoryChip selected={category === undefined} onClick={() => setCategory(undefined)}>
            all
          </CategoryChip>
          {categories.map((entry) => (
            <CategoryChip
              key={entry}
              selected={category === entry}
              onClick={() => {
                setCategory(category === entry ? undefined : entry);
                setActive(0);
              }}
            >
              {entry}
            </CategoryChip>
          ))}
          {required ? (
            <label className="ml-auto flex items-center gap-2 text-xs text-muted-foreground">
              <Switch
                size="sm"
                checked={compatibleOnly}
                onCheckedChange={(checked) => {
                  setCompatibleOnly(checked);
                  setActive(0);
                }}
              />
              Compatible only
            </label>
          ) : null}
        </div>
      </div>

      <ScrollArea className="min-h-0 flex-1">
        <div ref={list} id="plugin-options" role="listbox" aria-label="Plugins" className="space-y-1.5 p-3">
          {missing ? (
            <div className="rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs">
              <span className="font-mono">{value}</span>{" "}
              <span className="text-destructive">
                is not registered — no worker published this manifest.
              </span>
            </div>
          ) : null}
          {results.length === 0 ? (
            <p className="py-10 text-center text-sm text-muted-foreground">
              No plugin matches{query ? ` “${query}”` : ""}.
            </p>
          ) : (
            results.map((option, position) => (
              <PluginOptionRow
                key={option.manifest.name}
                option={option}
                index={position}
                selected={option.manifest.name === value}
                highlighted={position === highlighted}
                onHover={() => setActive(position)}
                onPick={() => onPick(option.manifest.name)}
              />
            ))
          )}
        </div>
      </ScrollArea>

      <div className="border-t px-4 py-2 text-[11px] text-muted-foreground">
        <kbd className="font-mono">↑</kbd> <kbd className="font-mono">↓</kbd> to move ·{" "}
        <kbd className="font-mono">Enter</kbd> to pick · <kbd className="font-mono">Esc</kbd> to
        close
      </div>
    </>
  );
}

function CategoryChip({
  selected,
  onClick,
  children,
}: {
  selected: boolean;
  onClick: () => void;
  children: string;
}) {
  return (
    <button type="button" aria-pressed={selected} onClick={onClick}>
      <Badge
        variant={selected ? "default" : "outline"}
        className={cn("cursor-pointer capitalize", !selected && "hover:bg-muted")}
      >
        {children}
      </Badge>
    </button>
  );
}

function PluginOptionRow({
  option,
  index,
  selected,
  highlighted,
  onHover,
  onPick,
}: {
  option: PluginOption;
  index: number;
  selected: boolean;
  highlighted: boolean;
  onHover: () => void;
  onPick: () => void;
}) {
  const { manifest } = option;
  const accepts = (manifest.accepts ?? []).join(", ") || "nothing";

  return (
    <button
      type="button"
      role="option"
      aria-selected={selected}
      data-option-index={index}
      onMouseMove={onHover}
      onClick={onPick}
      className={cn(
        "flex w-full items-start gap-3 rounded-md border px-3 py-2.5 text-left transition-colors",
        highlighted ? "border-ring bg-muted/60" : "hover:bg-muted/40",
        selected && "border-primary",
        option.compatible === false && "opacity-60",
      )}
    >
      <div className="min-w-0 flex-1 space-y-1">
        <div className="flex min-w-0 flex-wrap items-baseline gap-x-2">
          <span className="text-sm font-medium">{option.title}</span>
          {option.title !== manifest.name ? (
            <span className="font-mono text-xs text-muted-foreground">{manifest.name}</span>
          ) : null}
        </div>
        {option.summary ? (
          <p className="line-clamp-2 text-xs text-muted-foreground">{option.summary}</p>
        ) : null}
        <div className="flex flex-wrap items-center gap-1.5 pt-0.5">
          <span className="font-mono text-[11px] text-muted-foreground">
            {accepts} → {manifest.produces}
          </span>
          {option.category ? (
            <Badge variant="secondary" className="h-4 px-1.5 text-[10px] capitalize">
              {option.category}
            </Badge>
          ) : null}
          {manifest.kind && manifest.kind !== "builtin" ? (
            <Badge variant="outline" className="h-4 px-1.5 text-[10px]">
              {manifest.kind}
            </Badge>
          ) : null}
          {option.compatible === false ? (
            <Badge variant="outline" className="h-4 border-destructive/40 px-1.5 text-[10px] text-destructive">
              can&rsquo;t read this input
            </Badge>
          ) : null}
        </div>
      </div>
      {selected ? <Check className="mt-0.5 size-4 shrink-0 text-primary" aria-hidden /> : null}
    </button>
  );
}
