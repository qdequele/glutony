"use client";

import { useState } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { Play } from "lucide-react";
import { z } from "zod";

import { Button } from "@/components/ui/button";
import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Textarea } from "@/components/ui/textarea";
import { usePipelines } from "@/lib/api/hooks";
import {
  parseDocumentsInput,
  type IngestFormState,
  type IngestSourceKind,
} from "@/lib/api/ingest";
import { DropZone } from "./drop-zone";

/** Sentinel for "let the gateway route": Radix Select forbids an empty value. */
const AUTO = "__auto__";

/**
 * The three source tabs are validated conditionally, because only the active
 * one has to be filled in. The file itself is not part of the schema: a `File`
 * is not serializable form state and zod adds nothing over "is it there?", so
 * the drop zone owns it and the submit handler checks it.
 */
const schema = z
  .object({
    tab: z.enum(["file", "url", "documents"]),
    url: z.string(),
    documents: z.string(),
    index: z.string(),
    pipeline: z.string(),
  })
  .superRefine((values, ctx) => {
    if (values.tab === "url") {
      const url = values.url.trim();
      if (!url) {
        ctx.addIssue({ code: "custom", path: ["url"], message: "Enter a URL to fetch." });
      } else if (!/^https?:\/\//i.test(url)) {
        ctx.addIssue({ code: "custom", path: ["url"], message: "Must be an http(s) URL." });
      }
    }
    if (values.tab === "documents") {
      const parsed = parseDocumentsInput(values.documents);
      if (!parsed.ok) {
        ctx.addIssue({ code: "custom", path: ["documents"], message: parsed.error });
      }
    }
  });

type FormValues = z.infer<typeof schema>;

const DOCUMENTS_PLACEHOLDER = `[
  { "id": "1", "title": "Hello", "content": "…" }
]`;

export interface IngestFormProps {
  /** Resolves once the gateway answered; rejects when it refused. */
  onSubmit: (state: IngestFormState) => Promise<unknown>;
  pending: boolean;
}

export function IngestForm({ onSubmit, pending }: IngestFormProps) {
  const pipelines = usePipelines();
  const [file, setFile] = useState<File | undefined>(undefined);
  const [fileError, setFileError] = useState<string | undefined>(undefined);

  const form = useForm<FormValues>({
    resolver: zodResolver(schema),
    defaultValues: { tab: "file", url: "", documents: "", index: "", pipeline: "" },
  });

  const tab = form.watch("tab") as IngestSourceKind;

  async function submit(values: FormValues) {
    let source: IngestFormState["source"];
    if (values.tab === "file") {
      if (!file) {
        setFileError("Pick a file, or switch to the URL or JSON tab.");
        return;
      }
      source = { kind: "file", file };
    } else if (values.tab === "url") {
      source = { kind: "url", url: values.url };
    } else {
      const parsed = parseDocumentsInput(values.documents);
      // Unreachable: the resolver rejected the submission already.
      if (!parsed.ok) return;
      source = { kind: "documents", documents: parsed.documents };
    }
    setFileError(undefined);
    try {
      await onSubmit({
        source,
        index: values.index.trim() || undefined,
        pipeline: values.pipeline.trim() || undefined,
      });
    } catch {
      // The mutation already reported it through a toast.
    }
  }

  return (
    <form onSubmit={form.handleSubmit(submit)} className="space-y-4">
      <Tabs
        value={tab}
        onValueChange={(value) => {
          form.setValue("tab", value as IngestSourceKind);
          form.clearErrors();
          setFileError(undefined);
        }}
      >
        <TabsList>
          <TabsTrigger value="file">File</TabsTrigger>
          <TabsTrigger value="url">URL</TabsTrigger>
          <TabsTrigger value="documents">JSON documents</TabsTrigger>
        </TabsList>

        <TabsContent value="file" className="space-y-2">
          <DropZone
            file={file}
            disabled={pending}
            onFile={(next) => {
              setFile(next);
              setFileError(undefined);
            }}
          />
          {fileError ? <FieldError>{fileError}</FieldError> : null}
        </TabsContent>

        <TabsContent value="url">
          <Field className="gap-1.5">
            <FieldLabel htmlFor="ingest-url" className="text-xs font-medium">
              url
            </FieldLabel>
            <Input
              id="ingest-url"
              placeholder="https://example.com/report.pdf"
              className="font-mono"
              disabled={pending}
              aria-invalid={Boolean(form.formState.errors.url)}
              {...form.register("url")}
            />
            {form.formState.errors.url ? (
              <FieldError>{form.formState.errors.url.message}</FieldError>
            ) : null}
            <FieldDescription className="text-xs">
              The worker fetches it; routing uses the MIME type the server returns.
            </FieldDescription>
          </Field>
        </TabsContent>

        <TabsContent value="documents">
          <Field className="gap-1.5">
            <FieldLabel htmlFor="ingest-documents" className="text-xs font-medium">
              documents
            </FieldLabel>
            <Textarea
              id="ingest-documents"
              rows={8}
              placeholder={DOCUMENTS_PLACEHOLDER}
              className="font-mono text-xs"
              disabled={pending}
              aria-invalid={Boolean(form.formState.errors.documents)}
              {...form.register("documents")}
            />
            {form.formState.errors.documents ? (
              <FieldError>{form.formState.errors.documents.message}</FieldError>
            ) : null}
            <FieldDescription className="text-xs">
              An array, a single object, or the{" "}
              <span className="font-mono">{'{"documents": […]}'}</span> envelope.
            </FieldDescription>
          </Field>
        </TabsContent>
      </Tabs>

      <div className="grid gap-3 sm:grid-cols-2">
        <Field className="gap-1.5">
          <FieldLabel htmlFor="ingest-index" className="text-xs font-medium">
            index
          </FieldLabel>
          <Input
            id="ingest-index"
            placeholder="Leave empty to let the gateway resolve it"
            className="font-mono"
            disabled={pending}
            {...form.register("index")}
          />
          <FieldDescription className="text-xs">
            Sent as <span className="font-mono">?index=</span>, which beats the pipeline&rsquo;s
            own <span className="font-mono">index_pattern</span>.
          </FieldDescription>
        </Field>

        <Field className="gap-1.5">
          <FieldLabel htmlFor="ingest-pipeline" className="text-xs font-medium">
            pipeline
          </FieldLabel>
          <Select
            value={form.watch("pipeline") || AUTO}
            disabled={pending}
            onValueChange={(value) => form.setValue("pipeline", value === AUTO ? "" : value)}
          >
            <SelectTrigger id="ingest-pipeline" className="w-full">
              <SelectValue placeholder="Auto-route on MIME type" />
            </SelectTrigger>
            <SelectContent>
              <SelectItem value={AUTO}>Auto-route on MIME type</SelectItem>
              {(pipelines.data ?? []).map((pipeline) => (
                <SelectItem key={pipeline.uid} value={pipeline.uid} className="font-mono text-xs">
                  {pipeline.uid}
                </SelectItem>
              ))}
            </SelectContent>
          </Select>
          <FieldDescription className="text-xs">
            Forcing one calls{" "}
            <span className="font-mono">POST /ingest/pipeline/{"{uid}"}</span> and skips routing.
          </FieldDescription>
        </Field>
      </div>

      <Button type="submit" disabled={pending}>
        <Play aria-hidden />
        {pending ? "Submitting…" : "Ingest"}
      </Button>
    </form>
  );
}
