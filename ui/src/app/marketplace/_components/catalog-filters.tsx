"use client";

import { Search } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";

/**
 * Search box plus category chips, shared by both grids.
 *
 * Filtering is client-side over a list of at most a few dozen entries that
 * arrives in one cached request — no debounce, no server round trip.
 */
export function CatalogFilters<T extends string>({
  query,
  onQueryChange,
  categories,
  active,
  onToggle,
  placeholder,
  label,
}: {
  query: string;
  onQueryChange: (value: string) => void;
  categories: readonly T[];
  /** Selected categories; empty means "all". */
  active: readonly T[];
  onToggle: (category: T) => void;
  placeholder: string;
  label: string;
}) {
  return (
    <div className="flex flex-wrap items-center gap-3 border-b px-4 py-3">
      <div className="relative min-w-56 flex-1">
        <Search
          className="pointer-events-none absolute top-1/2 left-2.5 size-4 -translate-y-1/2 text-muted-foreground"
          aria-hidden
        />
        <Input
          type="search"
          value={query}
          onChange={(event) => onQueryChange(event.target.value)}
          placeholder={placeholder}
          aria-label={label}
          className="pl-8"
        />
      </div>
      <div className="flex flex-wrap gap-1.5">
        {categories.map((category) => {
          const selected = active.includes(category);
          return (
            <button key={category} type="button" onClick={() => onToggle(category)}>
              <Badge
                variant={selected ? "default" : "outline"}
                className={cn("cursor-pointer capitalize", !selected && "hover:bg-muted")}
              >
                {category}
              </Badge>
            </button>
          );
        })}
      </div>
    </div>
  );
}
