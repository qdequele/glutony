"use client";

import { ArrowDown, ArrowUp, Trash2 } from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader } from "@/components/ui/card";
import { Field, FieldDescription, FieldLabel } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Separator } from "@/components/ui/separator";
import { Switch } from "@/components/ui/switch";
import type { Backoff, JsonObject, PluginManifest } from "@/lib/api/types";
import { FAN_OUT_PATHS } from "@/lib/api/types";
import { DEFAULT_RETRY, DEFAULT_TIMEOUT_SECS, type StepDraft } from "@/lib/pipeline/draft";
import type { IssueField, ValidationIssue } from "@/lib/pipeline/validate";
import { SchemaForm } from "@/lib/schema-form";
import { cn } from "@/lib/utils";
import { PluginPicker } from "./plugin-picker";

const NO_FAN_OUT = "__none__";

function fieldHasIssue(issues: ValidationIssue[], field: IssueField): boolean {
  return issues.some((issue) => issue.field === field);
}

export interface StepCardProps {
  index: number;
  total: number;
  step: StepDraft;
  /** Ids of the steps listed before this one — the only legal dependencies. */
  earlierStepIds: string[];
  plugins: PluginManifest[];
  issues: ValidationIssue[];
  readOnly: boolean;
  onPatch: (patch: Partial<StepDraft>) => void;
  onRemove: () => void;
  onMove: (direction: -1 | 1) => void;
}

export function StepCard({
  index,
  total,
  step,
  earlierStepIds,
  plugins,
  issues,
  readOnly,
  onPatch,
  onRemove,
  onMove,
}: StepCardProps) {
  const manifest = plugins.find((plugin) => plugin.name === step.plugin);
  const idPrefix = `step-${index}`;
  const retryEnabled = step.retry !== undefined;

  function toggleDependency(id: string) {
    const next = step.depends_on.includes(id)
      ? step.depends_on.filter((entry) => entry !== id)
      : [...step.depends_on, id];
    onPatch({ depends_on: next });
  }

  return (
    <Card className={cn("gap-0 py-0", issues.length > 0 && "border-destructive/50")}>
      <CardHeader className="flex flex-row items-center gap-2 border-b px-3 py-2 [.border-b]:pb-2">
        <Badge variant="ghost" className="tabular-nums text-muted-foreground">
          {index + 1}
        </Badge>

        <div className="min-w-0 flex-1">
          <Label htmlFor={`${idPrefix}-id`} className="sr-only">
            Step id
          </Label>
          <Input
            id={`${idPrefix}-id`}
            value={step.id}
            disabled={readOnly}
            placeholder="step id"
            aria-invalid={fieldHasIssue(issues, "id")}
            className="h-8 font-mono text-xs"
            onChange={(event) => onPatch({ id: event.target.value })}
          />
        </div>

        <div className="min-w-0 flex-[2]">
          <Label htmlFor={`${idPrefix}-plugin`} className="sr-only">
            Plugin
          </Label>
          <div aria-invalid={fieldHasIssue(issues, "plugin")}>
            <PluginPicker
              id={`${idPrefix}-plugin`}
              value={step.plugin}
              plugins={plugins}
              disabled={readOnly}
              onChange={(name) => onPatch({ plugin: name })}
            />
          </div>
        </div>

        <div className="flex items-center gap-0.5">
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7"
            aria-label="Move step up"
            disabled={readOnly || index === 0}
            onClick={() => onMove(-1)}
          >
            <ArrowUp aria-hidden />
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7"
            aria-label="Move step down"
            disabled={readOnly || index === total - 1}
            onClick={() => onMove(1)}
          >
            <ArrowDown aria-hidden />
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7 text-muted-foreground hover:text-destructive"
            aria-label="Remove step"
            disabled={readOnly}
            onClick={onRemove}
          >
            <Trash2 aria-hidden />
          </Button>
        </div>
      </CardHeader>

      <CardContent className="space-y-3 px-3 py-3">
        {issues.length > 0 ? (
          <ul className="space-y-1 rounded-md border border-destructive/40 bg-destructive/5 px-2.5 py-2 text-xs text-destructive">
            {issues.map((issue, position) => (
              <li key={`${issue.rule}-${position}`}>{issue.message}</li>
            ))}
          </ul>
        ) : null}

        {manifest ? (
          <p className="text-xs text-muted-foreground">
            {manifest.description ? `${manifest.description} ` : null}
            <span className="font-mono">
              accepts {(manifest.accepts ?? []).join(", ") || "nothing"} → produces{" "}
              {manifest.produces}
            </span>
          </p>
        ) : null}

        <Field className="gap-1.5">
          <FieldLabel className="text-xs font-medium">depends_on</FieldLabel>
          {earlierStepIds.length === 0 ? (
            <FieldDescription className="text-xs">
              First step — it reads the ingest payload.
            </FieldDescription>
          ) : (
            <>
              <div className="flex flex-wrap gap-1">
                {earlierStepIds.map((id) => {
                  const selected = step.depends_on.includes(id);
                  return (
                    <Button
                      key={id}
                      type="button"
                      size="sm"
                      variant={selected ? "secondary" : "outline"}
                      disabled={readOnly}
                      className="h-6 px-2 font-mono text-[11px]"
                      aria-pressed={selected}
                      onClick={() => toggleDependency(id)}
                    >
                      {id}
                    </Button>
                  );
                })}
              </div>
              {step.depends_on.length === 0 ? (
                <FieldDescription className="text-xs">
                  Nothing selected — the step follows the one right above it.
                </FieldDescription>
              ) : null}
            </>
          )}
        </Field>

        <div className="grid gap-3 sm:grid-cols-3">
          <Field className="gap-1.5">
            <FieldLabel htmlFor={`${idPrefix}-fanout`} className="text-xs font-medium">
              fan_out
            </FieldLabel>
            <Select
              value={step.fan_out ?? NO_FAN_OUT}
              disabled={readOnly}
              onValueChange={(value) =>
                onPatch({
                  fan_out:
                    value === NO_FAN_OUT ? undefined : (value as StepDraft["fan_out"]),
                })
              }
            >
              <SelectTrigger id={`${idPrefix}-fanout`} className="w-full">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value={NO_FAN_OUT}>none</SelectItem>
                {FAN_OUT_PATHS.map((path) => (
                  <SelectItem key={path} value={path} className="font-mono">
                    {path}
                  </SelectItem>
                ))}
              </SelectContent>
            </Select>
            <FieldDescription className="text-xs">One activity per match.</FieldDescription>
          </Field>

          <Field className="gap-1.5">
            <FieldLabel htmlFor={`${idPrefix}-timeout`} className="text-xs font-medium">
              timeout_secs
            </FieldLabel>
            <Input
              id={`${idPrefix}-timeout`}
              value={step.timeout_secs ?? ""}
              disabled={readOnly}
              inputMode="numeric"
              placeholder={String(DEFAULT_TIMEOUT_SECS)}
              onChange={(event) => {
                const raw = event.target.value.trim();
                if (raw.length === 0) {
                  onPatch({ timeout_secs: undefined });
                  return;
                }
                if (!/^\d+$/.test(raw)) return;
                onPatch({ timeout_secs: Number(raw) });
              }}
            />
            <FieldDescription className="text-xs">Start-to-close.</FieldDescription>
          </Field>

          <Field orientation="horizontal" className="items-start gap-2">
            <Switch
              id={`${idPrefix}-retry`}
              checked={retryEnabled}
              disabled={readOnly}
              onCheckedChange={(checked) =>
                onPatch({ retry: checked ? { ...DEFAULT_RETRY } : undefined })
              }
            />
            <FieldLabel htmlFor={`${idPrefix}-retry`} className="text-xs font-medium">
              Custom retry
              <FieldDescription className="text-xs font-normal">
                Default: {DEFAULT_RETRY.max_attempts} attempts, {DEFAULT_RETRY.backoff}.
              </FieldDescription>
            </FieldLabel>
          </Field>
        </div>

        {step.retry ? (
          <div className="grid gap-3 rounded-md border p-2.5 sm:grid-cols-3">
            <Field className="gap-1.5">
              <FieldLabel htmlFor={`${idPrefix}-attempts`} className="text-xs font-medium">
                max_attempts
              </FieldLabel>
              <Input
                id={`${idPrefix}-attempts`}
                value={step.retry.max_attempts}
                disabled={readOnly}
                inputMode="numeric"
                onChange={(event) => {
                  const raw = event.target.value.trim();
                  if (!/^\d+$/.test(raw)) return;
                  onPatch({ retry: { ...step.retry!, max_attempts: Number(raw) } });
                }}
              />
            </Field>
            <Field className="gap-1.5">
              <FieldLabel htmlFor={`${idPrefix}-backoff`} className="text-xs font-medium">
                backoff
              </FieldLabel>
              <Select
                value={step.retry.backoff}
                disabled={readOnly}
                onValueChange={(value) =>
                  onPatch({ retry: { ...step.retry!, backoff: value as Backoff } })
                }
              >
                <SelectTrigger id={`${idPrefix}-backoff`} className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="exponential">exponential</SelectItem>
                  <SelectItem value="linear">linear</SelectItem>
                  <SelectItem value="none">none</SelectItem>
                </SelectContent>
              </Select>
            </Field>
            <Field className="gap-1.5">
              <FieldLabel htmlFor={`${idPrefix}-interval`} className="text-xs font-medium">
                initial_interval_secs
              </FieldLabel>
              <Input
                id={`${idPrefix}-interval`}
                value={step.retry.initial_interval_secs}
                disabled={readOnly}
                inputMode="numeric"
                onChange={(event) => {
                  const raw = event.target.value.trim();
                  if (!/^\d+$/.test(raw)) return;
                  onPatch({ retry: { ...step.retry!, initial_interval_secs: Number(raw) } });
                }}
              />
            </Field>
          </div>
        ) : null}

        <Separator />

        <div className="space-y-2">
          <p className="text-xs font-medium">config</p>
          <SchemaForm
            idPrefix={`${idPrefix}-config`}
            schema={manifest?.config_schema}
            value={step.config}
            disabled={readOnly}
            onChange={(next: JsonObject) => onPatch({ config: next })}
          />
        </div>
      </CardContent>
    </Card>
  );
}
