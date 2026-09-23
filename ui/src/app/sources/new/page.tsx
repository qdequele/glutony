"use client";

import Link from "next/link";
import { ArrowLeft } from "lucide-react";

import { PageHeader } from "@/components/common/page-header";
import { Button } from "@/components/ui/button";
import { SourceForm } from "../_components/source-form";
import { SOURCES_HREF } from "../routes";

export default function NewSourcePage() {
  return (
    <>
      <PageHeader
        title="New source"
        description="The pipeline must pin its Meilisearch with a connection: a scheduled run has no request to supply one."
        actions={
          <Button asChild variant="ghost" size="sm">
            <Link href={SOURCES_HREF}>
              <ArrowLeft aria-hidden />
              All sources
            </Link>
          </Button>
        }
      />
      <div className="mx-auto max-w-3xl">
        <SourceForm mode="create" />
      </div>
    </>
  );
}
