"use client";

import { useMemo, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePlugins } from "@/lib/api/hooks";
import { ACTION_CATEGORIES, type ActionCategory } from "@/lib/api/types";
import { mergeActions, type MergedAction } from "@/lib/catalog/merge";
import { ActionCard } from "../_components/action-card";
import { CatalogFilters } from "../_components/catalog-filters";

/** Everything a card can be searched by. */
function haystack(action: MergedAction): string {
  const { entry } = action;
  return [entry.title, entry.plugin, entry.summary, ...entry.use_cases]
    .join(" ")
    .toLowerCase();
}

export default function ActionsPage() {
  const catalog = useCatalog();
  // The registry is a second, independent read: the catalog renders with or
  // without it, so a slow or failing /plugins never blocks the grid.
  const plugins = usePlugins();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState<ActionCategory[]>([]);

  const merged = useMemo(
    () => mergeActions(catalog.data?.actions ?? [], plugins.data ?? []),
    [catalog.data, plugins.data],
  );

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return merged.filter((action) => {
      const byCategory =
        active.length === 0 || active.includes(action.entry.category);
      const byQuery = needle === "" || haystack(action).includes(needle);
      return byCategory && byQuery;
    });
  }, [merged, query, active]);

  function toggle(category: ActionCategory) {
    setActive((current) =>
      current.includes(category)
        ? current.filter((item) => item !== category)
        : [...current, category],
    );
  }

  if (catalog.isPending) {
    return (
      <div className="grid gap-3 p-4 sm:grid-cols-2 xl:grid-cols-3">
        {Array.from({ length: 9 }, (_, index) => (
          <Skeleton key={index} className="h-40 w-full" />
        ))}
      </div>
    );
  }

  if (catalog.error) {
    return (
      <div className="p-4">
        <Alert variant="destructive">
          <AlertTitle>Could not load the catalog</AlertTitle>
          <AlertDescription>{errorMessage(catalog.error)}</AlertDescription>
        </Alert>
      </div>
    );
  }

  return (
    <>
      <CatalogFilters
        query={query}
        onQueryChange={setQuery}
        categories={ACTION_CATEGORIES}
        active={active}
        onToggle={toggle}
        placeholder="Search actions…"
        label="Search actions"
      />
      <div className="p-4">
        {visible.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No action matches that search.
          </p>
        ) : (
          <div className="space-y-8">
            {ACTION_CATEGORIES.map((category) => {
              const section = visible.filter(
                (action) => action.entry.category === category,
              );
              if (section.length === 0) return null;
              return (
                <section key={category}>
                  <h2 className="mb-3 text-sm font-semibold tracking-tight capitalize">
                    {category}
                    <span className="ml-2 font-normal text-muted-foreground">
                      {section.length}
                    </span>
                    <span className="sr-only"> actions</span>
                  </h2>
                  <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
                    {section.map((action) => (
                      <ActionCard key={action.entry.plugin} action={action} />
                    ))}
                  </div>
                </section>
              );
            })}
          </div>
        )}
      </div>
    </>
  );
}
