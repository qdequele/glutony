"use client";

import { useMemo, useState } from "react";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Skeleton } from "@/components/ui/skeleton";
import { errorMessage, useCatalog, usePipelines } from "@/lib/api/hooks";
import { WORKFLOW_CATEGORIES, type WorkflowCategory } from "@/lib/api/types";
import { CatalogFilters } from "../_components/catalog-filters";
import { WorkflowCard, definitionFor } from "../_components/workflow-card";

export default function WorkflowsPage() {
  const catalog = useCatalog();
  const pipelines = usePipelines();
  const [query, setQuery] = useState("");
  const [active, setActive] = useState<WorkflowCategory[]>([]);

  const visible = useMemo(() => {
    const needle = query.trim().toLowerCase();
    return (catalog.data?.workflows ?? []).filter((entry) => {
      const byCategory = active.length === 0 || active.includes(entry.category);
      const haystack = [entry.title, entry.uid, entry.summary, entry.when_to_use]
        .join(" ")
        .toLowerCase();
      return byCategory && (needle === "" || haystack.includes(needle));
    });
  }, [catalog.data, query, active]);

  function toggle(category: WorkflowCategory) {
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
          <Skeleton key={index} className="h-52 w-full" />
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
        categories={WORKFLOW_CATEGORIES}
        active={active}
        onToggle={toggle}
        placeholder="Search workflows…"
        label="Search workflows"
      />
      <div className="p-4">
        {visible.length === 0 ? (
          <p className="py-16 text-center text-sm text-muted-foreground">
            No workflow matches that search.
          </p>
        ) : (
          <div className="grid gap-3 sm:grid-cols-2 xl:grid-cols-3">
            {visible.map((entry) => (
              <WorkflowCard
                key={entry.uid}
                entry={entry}
                definition={definitionFor(entry, pipelines.data ?? [])}
                definitionPending={pipelines.isPending && !entry.definition}
              />
            ))}
          </div>
        )}
      </div>
    </>
  );
}
