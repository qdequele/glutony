"use client";

import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Textarea } from "@/components/ui/textarea";
import type { PipelineDraft } from "@/lib/pipeline/draft";
import { MultiValueInput } from "./multi-value-input";

export interface MetadataPanelProps {
  draft: PipelineDraft;
  onPatch: (patch: Partial<PipelineDraft>) => void;
  /** Editing an existing pipeline: the uid is the storage key, so it is frozen. */
  uidLocked: boolean;
  readOnly: boolean;
  uidError?: string;
}

export function MetadataPanel({
  draft,
  onPatch,
  uidLocked,
  readOnly,
  uidError,
}: MetadataPanelProps) {
  return (
    <div className="space-y-3">
      <div className="grid gap-3 sm:grid-cols-2">
        <Field className="gap-1.5">
          <FieldLabel htmlFor="pipeline-uid" className="text-xs font-medium">
            uid
          </FieldLabel>
          <Input
            id="pipeline-uid"
            value={draft.uid}
            disabled={readOnly || uidLocked}
            placeholder="my-pdf-with-enrichment"
            aria-invalid={uidError !== undefined}
            className="font-mono"
            onChange={(event) => onPatch({ uid: event.target.value })}
          />
          {uidError ? <FieldError>{uidError}</FieldError> : null}
          <FieldDescription className="text-xs">
            {uidLocked
              ? "Frozen — saving under a different uid would create a second pipeline. Clone instead."
              : "Letters, digits, '.', '_' and '-'. builtin.* is reserved."}
          </FieldDescription>
        </Field>

        <Field className="gap-1.5">
          <FieldLabel htmlFor="pipeline-name" className="text-xs font-medium">
            name
          </FieldLabel>
          <Input
            id="pipeline-name"
            value={draft.name}
            disabled={readOnly}
            placeholder={draft.uid || "Display name"}
            onChange={(event) => onPatch({ name: event.target.value })}
          />
          <FieldDescription className="text-xs">Defaults to the uid.</FieldDescription>
        </Field>
      </div>

      <Field className="gap-1.5">
        <FieldLabel htmlFor="pipeline-description" className="text-xs font-medium">
          description
        </FieldLabel>
        <Textarea
          id="pipeline-description"
          value={draft.description}
          disabled={readOnly}
          rows={2}
          placeholder="What this pipeline is for."
          onChange={(event) => onPatch({ description: event.target.value })}
        />
      </Field>

      <div className="space-y-3 rounded-md border p-3">
        <p className="text-xs font-medium">trigger</p>
        <FieldDescription className="text-xs">
          When <span className="font-mono">POST /ingest</span> auto-routes to this pipeline. Leave
          empty to make it reachable only through{" "}
          <span className="font-mono">POST /ingest/pipeline/{"{uid}"}</span>.
        </FieldDescription>

        <Field className="gap-1.5">
          <FieldLabel htmlFor="trigger-content-types" className="text-xs font-medium">
            content_types
          </FieldLabel>
          <MultiValueInput
            id="trigger-content-types"
            values={draft.trigger.content_types}
            disabled={readOnly}
            placeholder="application/pdf, image/* …"
            onChange={(content_types) =>
              onPatch({ trigger: { ...draft.trigger, content_types } })
            }
          />
          <FieldDescription className="text-xs">
            Exact MIME types or <span className="font-mono">type/*</span> wildcards. Enter to add.
          </FieldDescription>
        </Field>

        <div className="grid gap-3 sm:grid-cols-2">
          <Field className="gap-1.5">
            <FieldLabel htmlFor="trigger-filename" className="text-xs font-medium">
              filename_pattern
            </FieldLabel>
            <Input
              id="trigger-filename"
              value={draft.trigger.filename_pattern}
              disabled={readOnly}
              placeholder="contract_*.pdf"
              className="font-mono"
              onChange={(event) =>
                onPatch({
                  trigger: { ...draft.trigger, filename_pattern: event.target.value },
                })
              }
            />
            <FieldDescription className="text-xs">
              Glob on the basename (<span className="font-mono">*</span>,{" "}
              <span className="font-mono">?</span>), case-insensitive.
            </FieldDescription>
          </Field>

          <Field className="gap-1.5">
            <FieldLabel htmlFor="trigger-index" className="text-xs font-medium">
              index_pattern
            </FieldLabel>
            <Input
              id="trigger-index"
              value={draft.trigger.index_pattern}
              disabled={readOnly}
              placeholder="contracts"
              className="font-mono"
              onChange={(event) =>
                onPatch({ trigger: { ...draft.trigger, index_pattern: event.target.value } })
              }
            />
            <FieldDescription className="text-xs">
              Overrides the header and <span className="font-mono">?index=</span>.
            </FieldDescription>
          </Field>
        </div>
      </div>
    </div>
  );
}
