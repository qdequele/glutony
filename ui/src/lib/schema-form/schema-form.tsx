"use client";

/**
 * Renders a plugin `config:` block from the plugin's `config_schema`.
 *
 * See `./fields.ts` for the schema → field mapping. Anything the mapping does
 * not understand — an unsupported shape, or a config key the schema never
 * mentions — is rendered as raw JSON rather than dropped.
 */
import { useEffect, useRef, useState, type ComponentType } from "react";

import { Input } from "@/components/ui/input";
import { Field, FieldDescription, FieldError, FieldLabel } from "@/components/ui/field";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Switch } from "@/components/ui/switch";
import { Textarea } from "@/components/ui/textarea";
import type { JsonObject, JsonSchema, JsonValue } from "@/lib/api/types";
import { cn } from "@/lib/utils";
import {
  formatFieldValue,
  parseFieldInput,
  schemaToFields,
  unknownConfigKeys,
  type SchemaField,
} from "./fields";

/** Sentinel for "leave the key out and let the plugin default apply". */
const UNSET = "__unset__";

function withKey(config: JsonObject, key: string, value: JsonValue | undefined): JsonObject {
  const next = { ...config };
  if (value === undefined) delete next[key];
  else next[key] = value;
  return next;
}

function sameJson(a: unknown, b: unknown): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

interface ControlProps {
  field: SchemaField;
  value: JsonValue | undefined;
  onChange: (value: JsonValue | undefined) => void;
  disabled?: boolean;
  inputId: string;
}

/**
 * Props of a custom widget. Same contract as the built-in controls: `value` is
 * the config key's current value, `onChange(undefined)` removes the key.
 */
export type SchemaWidgetProps = ControlProps;

/**
 * Custom controls keyed by JSON Schema `format`. A string property whose
 * `format` has an entry here renders that widget instead of a text input; the
 * label and description around it stay the form's own.
 */
export type SchemaWidgets = Partial<Record<string, ComponentType<SchemaWidgetProps>>>;

/**
 * Text-like control (string, integer, number and the JSON fallback).
 *
 * The typed text is local state: it has to survive intermediate values that do
 * not parse ("-", "1.", "{"). The parsed value is pushed up only when it is
 * valid, and the text resets when the value changes from the outside — which
 * is what happens when the YAML pane rewrites the draft.
 */
function TextControl({ field, value, onChange, disabled, inputId }: ControlProps) {
  const [text, setText] = useState(() => formatFieldValue(field, value));
  const [error, setError] = useState<string | undefined>(undefined);
  const emitted = useRef<JsonValue | undefined>(value);

  useEffect(() => {
    if (!sameJson(value, emitted.current)) {
      emitted.current = value;
      setText(formatFieldValue(field, value));
      setError(undefined);
    }
  }, [value, field]);

  function handle(next: string) {
    setText(next);
    const parsed = parseFieldInput(field, next);
    if (!parsed.ok) {
      setError(parsed.error);
      return;
    }
    setError(undefined);
    emitted.current = parsed.value;
    onChange(parsed.value);
  }

  const shared = {
    id: inputId,
    value: text,
    disabled,
    placeholder: field.placeholder,
    "aria-invalid": error !== undefined,
  };

  return (
    <>
      {field.kind === "json" ? (
        <Textarea
          {...shared}
          rows={Math.min(10, Math.max(3, text.split("\n").length))}
          spellCheck={false}
          className="font-mono text-xs"
          onChange={(event) => handle(event.target.value)}
        />
      ) : (
        <Input
          {...shared}
          inputMode={field.kind === "string" ? undefined : "decimal"}
          onChange={(event) => handle(event.target.value)}
        />
      )}
      {error ? <FieldError>{error}</FieldError> : null}
    </>
  );
}

function EnumControl({ field, value, onChange, disabled, inputId }: ControlProps) {
  const canUnset = !field.required && field.default !== undefined;
  const current = typeof value === "string" ? value : UNSET;

  return (
    <Select
      value={current}
      disabled={disabled}
      onValueChange={(next) => onChange(next === UNSET ? undefined : next)}
    >
      <SelectTrigger id={inputId} className="w-full">
        <SelectValue placeholder={field.placeholder ?? "Select…"} />
      </SelectTrigger>
      <SelectContent>
        {canUnset ? (
          <SelectItem value={UNSET}>
            <span className="text-muted-foreground">default ({String(field.default)})</span>
          </SelectItem>
        ) : null}
        {(field.options ?? []).map((option) => (
          <SelectItem key={option} value={option}>
            {option}
          </SelectItem>
        ))}
      </SelectContent>
    </Select>
  );
}

function BooleanControl({ field, value, onChange, disabled, inputId }: ControlProps) {
  const fallback = typeof field.default === "boolean" ? field.default : false;
  const checked = typeof value === "boolean" ? value : fallback;
  return (
    <Switch
      id={inputId}
      checked={checked}
      disabled={disabled}
      onCheckedChange={(next) => onChange(next)}
    />
  );
}

function SchemaFieldRow({
  field,
  value,
  onChange,
  disabled,
  idPrefix,
  widgets,
}: Omit<ControlProps, "inputId"> & { idPrefix: string; widgets?: SchemaWidgets }) {
  const inputId = `${idPrefix}-${field.name}`;
  const boolean = field.kind === "boolean";
  const Widget = field.format ? widgets?.[field.format] : undefined;

  return (
    <Field orientation={boolean ? "horizontal" : "vertical"} className="gap-1.5">
      <FieldLabel htmlFor={inputId} className="text-xs font-medium">
        <span className="font-mono">{field.name}</span>
        {field.required ? <span className="text-destructive">*</span> : null}
        {field.nullable ? (
          <span className="font-normal text-muted-foreground">nullable</span>
        ) : null}
      </FieldLabel>

      {Widget ? (
        <Widget
          field={field}
          value={value}
          onChange={onChange}
          disabled={disabled}
          inputId={inputId}
        />
      ) : boolean ? (
        <BooleanControl
          field={field}
          value={value}
          onChange={onChange}
          disabled={disabled}
          inputId={inputId}
        />
      ) : field.kind === "enum" ? (
        <EnumControl
          field={field}
          value={value}
          onChange={onChange}
          disabled={disabled}
          inputId={inputId}
        />
      ) : (
        <TextControl
          field={field}
          value={value}
          onChange={onChange}
          disabled={disabled}
          inputId={inputId}
        />
      )}

      {field.description ? (
        <FieldDescription className={cn("text-xs", boolean && "basis-full")}>
          {field.description}
        </FieldDescription>
      ) : null}
    </Field>
  );
}

/** Free-form JSON editor, used for the keys no field covers. */
function RawJsonBlock({
  label,
  description,
  value,
  onChange,
  disabled,
  inputId,
}: {
  label: string;
  description: string;
  value: JsonObject;
  onChange: (next: JsonObject) => void;
  disabled?: boolean;
  inputId: string;
}) {
  const [text, setText] = useState(() => JSON.stringify(value, null, 2));
  const [error, setError] = useState<string | undefined>(undefined);
  const emitted = useRef<JsonObject>(value);

  useEffect(() => {
    if (!sameJson(value, emitted.current)) {
      emitted.current = value;
      setText(JSON.stringify(value, null, 2));
      setError(undefined);
    }
  }, [value]);

  function handle(next: string) {
    setText(next);
    if (next.trim().length === 0) {
      setError(undefined);
      emitted.current = {};
      onChange({});
      return;
    }
    try {
      const parsed: unknown = JSON.parse(next);
      if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
        setError("expected a JSON object");
        return;
      }
      setError(undefined);
      emitted.current = parsed as JsonObject;
      onChange(parsed as JsonObject);
    } catch (parseError) {
      setError(parseError instanceof Error ? parseError.message : "invalid JSON");
    }
  }

  return (
    <Field className="gap-1.5">
      <FieldLabel htmlFor={inputId} className="text-xs font-medium">
        {label}
      </FieldLabel>
      <Textarea
        id={inputId}
        value={text}
        disabled={disabled}
        spellCheck={false}
        aria-invalid={error !== undefined}
        rows={Math.min(12, Math.max(3, text.split("\n").length))}
        className="font-mono text-xs"
        onChange={(event) => handle(event.target.value)}
      />
      {error ? <FieldError>{error}</FieldError> : null}
      <FieldDescription className="text-xs">{description}</FieldDescription>
    </Field>
  );
}

export interface SchemaFormProps {
  /** The plugin's `config_schema`. */
  schema: JsonSchema | undefined;
  /** Current `config:` block. */
  value: JsonObject;
  /** Called with the whole next config block. */
  onChange: (next: JsonObject) => void;
  /** Prefix for generated input ids; must be unique on the page. */
  idPrefix: string;
  disabled?: boolean;
  /** Custom controls for string properties, keyed by schema `format`. */
  widgets?: SchemaWidgets;
}

export function SchemaForm({
  schema,
  value,
  onChange,
  idPrefix,
  disabled,
  widgets,
}: SchemaFormProps) {
  const fields = schemaToFields(schema);
  const extraKeys = unknownConfigKeys(schema, value);

  if (fields.length === 0) {
    return (
      <RawJsonBlock
        inputId={`${idPrefix}-config`}
        label="Raw JSON"
        description={
          schema
            ? "This plugin publishes no config properties — edit the block as JSON."
            : "No manifest for this plugin — edit the block as JSON."
        }
        value={value}
        onChange={onChange}
        disabled={disabled}
      />
    );
  }

  const extra: JsonObject = Object.fromEntries(extraKeys.map((key) => [key, value[key]]));

  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {fields.map((field) => (
        <div
          key={field.name}
          className={field.kind === "json" || field.kind === "boolean" ? "sm:col-span-2" : undefined}
        >
          <SchemaFieldRow
            field={field}
            value={value[field.name]}
            onChange={(next) => onChange(withKey(value, field.name, next))}
            disabled={disabled}
            idPrefix={idPrefix}
            widgets={widgets}
          />
        </div>
      ))}

      {extraKeys.length > 0 ? (
        <div className="sm:col-span-2">
          <RawJsonBlock
            inputId={`${idPrefix}-extra`}
            label={`Unrecognised keys (${extraKeys.join(", ")})`}
            description="These keys are not in the plugin's schema. They are kept as written."
            value={extra}
            onChange={(next) => {
              const kept: JsonObject = {};
              for (const [key, entry] of Object.entries(value)) {
                if (!extraKeys.includes(key)) kept[key] = entry;
              }
              onChange({ ...kept, ...next });
            }}
            disabled={disabled}
          />
        </div>
      ) : null}
    </div>
  );
}
