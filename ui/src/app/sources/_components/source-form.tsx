"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useMemo, type ReactNode } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { Controller, useForm, useWatch } from "react-hook-form";
import { TriangleAlert } from "lucide-react";
import { toast } from "sonner";

import { editPipelineHref } from "@/app/pipelines/routes";
import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import { isFormError } from "@/lib/api/client";
import { errorMessage, usePipeline, usePipelines } from "@/lib/api/hooks";
import {
  pipelinePinsConnection,
  useCreateSource,
  useUpdateSource,
  type SourceView,
} from "@/lib/api/sources";
import { authChange, secretPlaceholder } from "../_lib/auth";
import { describeCron, validateCronShape } from "../_lib/cron";
import {
  buildCreateBody,
  buildPatchBody,
  emptySourceFormValues,
  SOURCE_METHODS,
  sourceToFormValues,
  timezoneOptions,
  URL_TEMPLATE_TOKENS,
  type SourceFormValues,
} from "../_lib/form";
import { fieldForServerError, sourceFormSchema } from "../_lib/schema";
import { SOURCES_HREF, sourceDetailHref } from "../routes";
import { HeadersEditor } from "./headers-editor";

const AUTH_KIND_LABELS: Record<SourceFormValues["auth"]["kind"], string> = {
  none: "None",
  bearer: "Bearer token",
  basic: "Basic (username + password)",
  headers: "Secret headers",
  keep: "Keep the stored credential",
};

function Section({
  title,
  description,
  children,
}: {
  title: string;
  description?: string;
  children: ReactNode;
}) {
  return (
    <Card className="gap-3 py-3">
      <CardHeader className="px-4">
        <CardTitle className="text-sm">{title}</CardTitle>
        {description ? <CardDescription className="text-xs">{description}</CardDescription> : null}
      </CardHeader>
      <CardContent className="space-y-3 px-4">{children}</CardContent>
    </Card>
  );
}

/**
 * The message of an error on a whole field array. The resolver files it under
 * `.root` once rows are registered, and on the array itself while it is empty.
 */
function arrayError(
  error: { message?: string; root?: { message?: string } } | undefined,
): string | undefined {
  return error?.root?.message ?? error?.message;
}

/** The hint under the pipeline Select: sources need a pinned destination. */
function PipelineHint({ uid }: { uid: string }) {
  const pipeline = usePipeline(uid || undefined);
  if (!uid || !pipeline.data || pipelinePinsConnection(pipeline.data)) return null;
  const hasIndexer = pipeline.data.steps.some((step) => step.plugin === "meili_indexer");
  return (
    <Alert className="border-amber-500/40 text-amber-800 dark:text-amber-200">
      <TriangleAlert aria-hidden />
      <AlertTitle>This pipeline does not pin a Meilisearch</AlertTitle>
      <AlertDescription className="text-xs">
        <p>
          {hasIndexer
            ? "Not every meili_indexer step names a connection."
            : "It has no meili_indexer step."}{" "}
          A scheduled run has no request to supply a destination, so the gateway will refuse it.{" "}
          <Link
            href={editPipelineHref(uid)}
            className="font-medium underline underline-offset-2"
          >
            Edit the pipeline
          </Link>{" "}
          and pick a connection on its indexing step.
        </p>
      </AlertDescription>
    </Alert>
  );
}

export interface SourceFormProps {
  mode: "create" | "edit";
  /** The stored source, in edit mode. */
  stored?: SourceView;
}

export function SourceForm({ mode, stored }: SourceFormProps) {
  const router = useRouter();
  const pipelines = usePipelines();
  const create = useCreateSource();
  const update = useUpdateSource();

  const schema = useMemo(() => sourceFormSchema(mode, stored), [mode, stored]);
  const form = useForm<SourceFormValues>({
    resolver: zodResolver(schema),
    defaultValues: stored ? sourceToFormValues(stored) : emptySourceFormValues(),
  });
  const { control, register } = form;
  const { errors, isSubmitting } = form.formState;

  const cron = useWatch({ control, name: "cron" });
  const timezone = useWatch({ control, name: "timezone" });
  const pipelineUid = useWatch({ control, name: "pipeline" });
  const authKind = useWatch({ control, name: "auth.kind" });

  const cronDescription = validateCronShape(cron) === undefined ? describeCron(cron) : undefined;
  const zones = useMemo(
    () => timezoneOptions(timezone, Intl.DateTimeFormat().resolvedOptions().timeZone),
    [timezone],
  );
  const storedHeaderNames =
    stored?.auth?.kind === "headers" ? Object.keys(stored.auth.headers) : [];
  const authKinds: SourceFormValues["auth"]["kind"][] =
    stored?.auth?.kind === "unknown"
      ? ["keep", "none", "bearer", "basic", "headers"]
      : ["none", "bearer", "basic", "headers"];

  async function submit(values: SourceFormValues) {
    const auth = authChange(stored?.auth, values.auth);
    // Unreachable: the resolver already rejected an invalid credential.
    if (auth.kind === "invalid") return;
    try {
      if (mode === "create") {
        const saved = await create.mutateAsync(buildCreateBody(values, auth));
        router.push(sourceDetailHref(saved.uid));
        return;
      }
      if (!stored) return;
      const body = buildPatchBody(stored, values, auth);
      if (Object.keys(body).length === 0) {
        toast.info("Nothing to save");
        return;
      }
      await update.mutateAsync({ uid: stored.uid, body });
      router.push(sourceDetailHref(stored.uid));
    } catch (error) {
      if (!isFormError(error)) return; // Already a toast.
      // Pin the message on the field it is about (and focus it); fall back to
      // an alert above the form when it names none.
      const field = fieldForServerError(error.message);
      if (field && !(mode === "edit" && field === "uid")) {
        form.setError(field, { message: error.message }, { shouldFocus: true });
      } else {
        form.setError("root", { message: error.message });
      }
    }
  }

  const disabled = isSubmitting;

  return (
    <form onSubmit={form.handleSubmit(submit)} noValidate className="space-y-4 p-4">
      {errors.root ? (
        <Alert variant="destructive">
          <AlertTitle>The gateway refused this source</AlertTitle>
          <AlertDescription className="break-words">{errors.root.message}</AlertDescription>
        </Alert>
      ) : null}

      <Section title="Source">
        <div className="grid gap-3 sm:grid-cols-2">
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-uid" className="text-xs font-medium">
              uid
            </FieldLabel>
            <Input
              id="source-uid"
              className="font-mono read-only:bg-muted/50 read-only:text-muted-foreground"
              placeholder="tmdb-daily"
              // Read-only rather than disabled on edit: the uid is the handle, not a field.
              readOnly={mode === "edit"}
              disabled={disabled}
              aria-invalid={Boolean(errors.uid)}
              {...register("uid")}
            />
            {errors.uid ? <FieldError>{errors.uid.message}</FieldError> : null}
          </Field>
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-name" className="text-xs font-medium">
              name
            </FieldLabel>
            <Input
              id="source-name"
              placeholder={stored?.uid ?? "Defaults to the uid"}
              disabled={disabled}
              {...register("name")}
            />
          </Field>
        </div>
        <Field className="gap-1.5">
          <FieldLabel htmlFor="source-description" className="text-xs font-medium">
            description
          </FieldLabel>
          <Textarea
            id="source-description"
            rows={2}
            disabled={disabled}
            {...register("description")}
          />
        </Field>
      </Section>

      <Section
        title="Pipeline"
        description="Every run hands what it fetched to this pipeline."
      >
        <div className="grid gap-3 sm:grid-cols-2">
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-pipeline" className="text-xs font-medium">
              pipeline
            </FieldLabel>
            <Controller
              control={control}
              name="pipeline"
              render={({ field }) => (
                <Select
                  value={field.value || undefined}
                  disabled={disabled || pipelines.isPending}
                  onValueChange={field.onChange}
                >
                  <SelectTrigger
                    id="source-pipeline"
                    className="w-full"
                    aria-invalid={Boolean(errors.pipeline)}
                  >
                    <SelectValue
                      placeholder={pipelines.isPending ? "Loading pipelines…" : "Pick a pipeline"}
                    />
                  </SelectTrigger>
                  <SelectContent>
                    {(pipelines.data ?? []).map((pipeline) => (
                      <SelectItem key={pipeline.uid} value={pipeline.uid}>
                        <span className="font-mono text-xs">{pipeline.uid}</span>
                        {pipelinePinsConnection(pipeline) ? null : (
                          <span className="text-xs text-muted-foreground">no connection</span>
                        )}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            />
            {errors.pipeline ? <FieldError>{errors.pipeline.message}</FieldError> : null}
            {pipelines.error ? (
              <FieldError>Could not load pipelines: {errorMessage(pipelines.error)}</FieldError>
            ) : null}
          </Field>
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-index" className="text-xs font-medium">
              index
            </FieldLabel>
            <Input
              id="source-index"
              className="font-mono"
              placeholder="Resolved by the pipeline"
              disabled={disabled}
              aria-invalid={Boolean(errors.index)}
              {...register("index")}
            />
            {errors.index ? <FieldError>{errors.index.message}</FieldError> : null}
            <FieldDescription className="text-xs">
              Optional override of the target index.
            </FieldDescription>
          </Field>
        </div>
        <PipelineHint uid={pipelineUid} />
      </Section>

      <Section title="Location" description="Fetched on every tick. Unchanged content is skipped.">
        <Field className="gap-1.5">
          <FieldLabel htmlFor="source-url" className="text-xs font-medium">
            url
          </FieldLabel>
          <div className="flex gap-1.5">
            <Controller
              control={control}
              name="method"
              render={({ field }) => (
                <Select value={field.value} disabled={disabled} onValueChange={field.onChange}>
                  <SelectTrigger className="w-24 font-mono text-xs" aria-label="HTTP method">
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {SOURCE_METHODS.map((method) => (
                      <SelectItem key={method} value={method} className="font-mono text-xs">
                        {method}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            />
            <Input
              id="source-url"
              className="flex-1 font-mono text-xs"
              placeholder="https://files.tmdb.org/p/exports/movie_ids_{{ date-1d:%m_%d_%Y }}.json.gz"
              disabled={disabled}
              aria-invalid={Boolean(errors.url)}
              {...register("url")}
            />
          </div>
          {errors.url ? <FieldError>{errors.url.message}</FieldError> : null}
          <FieldDescription className="text-xs">
            Rendered against the scheduled time, in the source&rsquo;s timezone:
          </FieldDescription>
          <ul className="space-y-0.5 text-xs text-muted-foreground">
            {URL_TEMPLATE_TOKENS.map((entry) => (
              <li key={entry.token}>
                <span className="font-mono text-foreground">{entry.token}</span> — {entry.meaning}
              </li>
            ))}
          </ul>
        </Field>
        <Field className="gap-1.5">
          <FieldLabel className="text-xs font-medium">headers</FieldLabel>
          <HeadersEditor
            control={control}
            register={register}
            name="headers"
            idPrefix="source-header"
            disabled={disabled}
          />
          {arrayError(errors.headers) ? <FieldError>{arrayError(errors.headers)}</FieldError> : null}
          <FieldDescription className="text-xs">
            Not secret — stored and returned as written. Put API keys under Credential.
          </FieldDescription>
        </Field>
      </Section>

      <Section title="Schedule">
        <div className="grid gap-3 sm:grid-cols-2">
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-cron" className="text-xs font-medium">
              cron
            </FieldLabel>
            <Input
              id="source-cron"
              className="font-mono"
              placeholder="30 0 * * *"
              disabled={disabled}
              aria-invalid={Boolean(errors.cron)}
              {...register("cron")}
            />
            {errors.cron ? (
              <FieldError>{errors.cron.message}</FieldError>
            ) : cronDescription ? (
              <FieldDescription className="text-xs text-foreground">
                {cronDescription} ({timezone})
              </FieldDescription>
            ) : null}
            <FieldDescription className="text-xs">
              minute hour day-of-month month day-of-week. Temporal has the final say.
            </FieldDescription>
          </Field>
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-timezone" className="text-xs font-medium">
              timezone
            </FieldLabel>
            <Controller
              control={control}
              name="timezone"
              render={({ field }) => (
                <Select value={field.value} disabled={disabled} onValueChange={field.onChange}>
                  <SelectTrigger
                    id="source-timezone"
                    className="w-full"
                    aria-invalid={Boolean(errors.timezone)}
                  >
                    <SelectValue />
                  </SelectTrigger>
                  <SelectContent>
                    {zones.map((zone) => (
                      <SelectItem key={zone} value={zone}>
                        {zone}
                      </SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              )}
            />
            {errors.timezone ? <FieldError>{errors.timezone.message}</FieldError> : null}
            <FieldDescription className="text-xs">
              For the cron and the URL template.
            </FieldDescription>
          </Field>
        </div>
      </Section>

      <Section
        title="Credential"
        description="Write-only: stored encrypted, never shown again. Leave a secret empty to keep it."
      >
        <Field className="gap-1.5 sm:max-w-xs">
          <FieldLabel htmlFor="source-auth-kind" className="text-xs font-medium">
            auth
          </FieldLabel>
          <Controller
            control={control}
            name="auth.kind"
            render={({ field }) => (
              <Select value={field.value} disabled={disabled} onValueChange={field.onChange}>
                <SelectTrigger id="source-auth-kind" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {authKinds.map((kind) => (
                    <SelectItem key={kind} value={kind}>
                      {AUTH_KIND_LABELS[kind]}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            )}
          />
          {stored?.auth && authKind === "none" ? (
            <FieldDescription className="text-xs text-destructive">
              Saving removes the stored credential.
            </FieldDescription>
          ) : null}
        </Field>

        {authKind === "bearer" ? (
          <Field className="gap-1.5">
            <FieldLabel htmlFor="source-auth-token" className="text-xs font-medium">
              token
            </FieldLabel>
            <Input
              id="source-auth-token"
              type="password"
              autoComplete="new-password"
              className="font-mono"
              placeholder={secretPlaceholder(stored?.auth, "bearer") || "Sent as Authorization: Bearer …"}
              disabled={disabled}
              aria-invalid={Boolean(errors.auth?.token)}
              {...register("auth.token")}
            />
            {errors.auth?.token ? <FieldError>{errors.auth.token.message}</FieldError> : null}
          </Field>
        ) : null}

        {authKind === "basic" ? (
          <div className="grid gap-3 sm:grid-cols-2">
            <Field className="gap-1.5">
              <FieldLabel htmlFor="source-auth-username" className="text-xs font-medium">
                username
              </FieldLabel>
              <Input
                id="source-auth-username"
                autoComplete="off"
                disabled={disabled}
                aria-invalid={Boolean(errors.auth?.username)}
                {...register("auth.username")}
              />
              {errors.auth?.username ? (
                <FieldError>{errors.auth.username.message}</FieldError>
              ) : null}
            </Field>
            <Field className="gap-1.5">
              <FieldLabel htmlFor="source-auth-password" className="text-xs font-medium">
                password
              </FieldLabel>
              <Input
                id="source-auth-password"
                type="password"
                autoComplete="new-password"
                placeholder={secretPlaceholder(stored?.auth, "basic")}
                disabled={disabled}
                aria-invalid={Boolean(errors.auth?.password)}
                {...register("auth.password")}
              />
              {errors.auth?.password ? (
                <FieldError>{errors.auth.password.message}</FieldError>
              ) : null}
            </Field>
          </div>
        ) : null}

        {authKind === "headers" ? (
          <Field className="gap-1.5">
            <FieldLabel className="text-xs font-medium">secret headers</FieldLabel>
            <HeadersEditor
              control={control}
              register={register}
              name="auth.headers"
              secret
              storedNames={storedHeaderNames}
              idPrefix="source-auth-header"
              disabled={disabled}
            />
            {arrayError(errors.auth?.headers) ? (
              <FieldError>{arrayError(errors.auth?.headers)}</FieldError>
            ) : null}
            {storedHeaderNames.length > 0 ? (
              <FieldDescription className="text-xs">
                Changing any header replaces the whole set: re-enter every value.
              </FieldDescription>
            ) : null}
          </Field>
        ) : null}
      </Section>

      <div className="flex items-center justify-end gap-2">
        <Button type="button" variant="outline" asChild>
          <Link href={stored ? sourceDetailHref(stored.uid) : SOURCES_HREF}>Cancel</Link>
        </Button>
        <Button type="submit" disabled={disabled}>
          {disabled ? "Saving…" : mode === "create" ? "Create source" : "Save"}
        </Button>
      </div>
    </form>
  );
}
