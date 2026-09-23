"use client";

import { useEffect } from "react";
import { zodResolver } from "@hookform/resolvers/zod";
import { useForm } from "react-hook-form";
import { z } from "zod";

import { Alert, AlertDescription, AlertTitle } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";
import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { isFormError } from "@/lib/api/client";
import {
  hostError,
  uidError,
  useCreateConnection,
  useUpdateConnection,
  type ConnectionView,
} from "@/lib/api/connections";
import {
  buildCreateConnectionBody,
  buildUpdateConnectionBody,
  connectionToFormValues,
  emptyConnectionFormValues,
  type ConnectionFormValues,
} from "../_lib/form";

function schemaFor(mode: "create" | "edit") {
  return z
    .object({
      uid: z.string(),
      name: z.string(),
      host: z.string(),
      apiKey: z.string(),
    })
    .superRefine((values, ctx) => {
      if (mode === "create") {
        const uid = uidError(values.uid.trim());
        if (uid) ctx.addIssue({ code: "custom", path: ["uid"], message: uid });
        if (values.apiKey.length === 0) {
          ctx.addIssue({ code: "custom", path: ["apiKey"], message: "Enter the API key." });
        }
      }
      const host = hostError(values.host);
      if (host) ctx.addIssue({ code: "custom", path: ["host"], message: host });
    });
}

export interface ConnectionDialogProps {
  /** The connection being edited; `undefined` opens the dialog in create mode. */
  connection: ConnectionView | undefined;
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ConnectionDialog({ connection, open, onOpenChange }: ConnectionDialogProps) {
  const mode = connection ? "edit" : "create";
  const create = useCreateConnection();
  const update = useUpdateConnection();
  const form = useForm<ConnectionFormValues>({
    resolver: zodResolver(schemaFor(mode)),
    defaultValues: connection ? connectionToFormValues(connection) : emptyConnectionFormValues(),
  });
  const { errors, isSubmitting } = form.formState;

  // Reopening the dialog (or on another row) starts from what is stored.
  useEffect(() => {
    if (open) {
      form.reset(connection ? connectionToFormValues(connection) : emptyConnectionFormValues());
    }
  }, [open, connection, form]);

  async function submit(values: ConnectionFormValues) {
    try {
      if (connection) {
        const body = buildUpdateConnectionBody(connection, values);
        if (Object.keys(body).length > 0) {
          await update.mutateAsync({ uid: connection.uid, body });
        }
      } else {
        await create.mutateAsync(buildCreateConnectionBody(values));
      }
      onOpenChange(false);
    } catch (error) {
      // The gateway probed the host and the key: its message says which failed.
      if (isFormError(error)) form.setError("root", { message: error.message });
      // Anything else was already reported through a toast by the hook.
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <form onSubmit={form.handleSubmit(submit)} className="space-y-4" noValidate>
          <DialogHeader>
            <DialogTitle>{connection ? `Edit ${connection.uid}` : "New connection"}</DialogTitle>
            <DialogDescription>
              The gateway checks the host answers <span className="font-mono">/health</span> and
              that the key is accepted before saving.
            </DialogDescription>
          </DialogHeader>

          {errors.root ? (
            <Alert variant="destructive">
              <AlertTitle>The gateway refused this connection</AlertTitle>
              <AlertDescription className="break-words">{errors.root.message}</AlertDescription>
            </Alert>
          ) : null}

          <div className="grid gap-3 sm:grid-cols-2">
            <Field className="gap-1.5">
              <FieldLabel htmlFor="connection-uid" className="text-xs font-medium">
                uid
              </FieldLabel>
              <Input
                id="connection-uid"
                className="font-mono"
                placeholder="prod-movies"
                disabled={mode === "edit" || isSubmitting}
                aria-invalid={Boolean(errors.uid)}
                {...form.register("uid")}
              />
              {errors.uid ? <FieldError>{errors.uid.message}</FieldError> : null}
              <FieldDescription className="text-xs">
                What a <span className="font-mono">meili_indexer</span> step names.
              </FieldDescription>
            </Field>

            <Field className="gap-1.5">
              <FieldLabel htmlFor="connection-name" className="text-xs font-medium">
                name
              </FieldLabel>
              <Input
                id="connection-name"
                placeholder={connection?.uid ?? "Defaults to the uid"}
                disabled={isSubmitting}
                {...form.register("name")}
              />
            </Field>
          </div>

          <Field className="gap-1.5">
            <FieldLabel htmlFor="connection-host" className="text-xs font-medium">
              host
            </FieldLabel>
            <Input
              id="connection-host"
              className="font-mono"
              placeholder="https://ms-1234.meilisearch.io"
              disabled={isSubmitting}
              aria-invalid={Boolean(errors.host)}
              {...form.register("host")}
            />
            {errors.host ? <FieldError>{errors.host.message}</FieldError> : null}
          </Field>

          <Field className="gap-1.5">
            <FieldLabel htmlFor="connection-key" className="text-xs font-medium">
              api_key
            </FieldLabel>
            <Input
              id="connection-key"
              type="password"
              autoComplete="new-password"
              className="font-mono"
              placeholder={connection ? "****" : "Admin or indexing key"}
              disabled={isSubmitting}
              aria-invalid={Boolean(errors.apiKey)}
              {...form.register("apiKey")}
            />
            {errors.apiKey ? <FieldError>{errors.apiKey.message}</FieldError> : null}
            <FieldDescription className="text-xs">
              {connection
                ? "Write-only. Leave empty to keep the stored key."
                : "Stored encrypted; the API never returns it."}
            </FieldDescription>
          </Field>

          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => onOpenChange(false)}
              disabled={isSubmitting}
            >
              Cancel
            </Button>
            <Button type="submit" disabled={isSubmitting}>
              {isSubmitting ? "Checking…" : connection ? "Save" : "Create"}
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  );
}
