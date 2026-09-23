"use client";

import { Plus, X } from "lucide-react";
import { useFieldArray, type Control, type UseFormRegister } from "react-hook-form";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import type { SourceFormValues } from "../_lib/form";

export interface HeadersEditorProps {
  control: Control<SourceFormValues>;
  register: UseFormRegister<SourceFormValues>;
  /** `headers` for the plain location headers, `auth.headers` for secret ones. */
  name: "headers" | "auth.headers";
  /** Secret rows render password inputs. */
  secret?: boolean;
  /** Header names whose value is stored (shown as `****` placeholders). */
  storedNames?: string[];
  disabled?: boolean;
  idPrefix: string;
}

/** Name/value rows, added and removed in place. */
export function HeadersEditor({
  control,
  register,
  name,
  secret = false,
  storedNames = [],
  disabled,
  idPrefix,
}: HeadersEditorProps) {
  const { fields, append, remove } = useFieldArray({ control, name });

  return (
    <div className="space-y-1.5">
      {fields.map((row, index) => (
        <div key={row.id} className="flex items-center gap-1.5">
          <Input
            aria-label={`Header ${index + 1} name`}
            id={`${idPrefix}-${index}-name`}
            placeholder={secret ? "X-Api-Key" : "Accept"}
            className="h-8 flex-1 font-mono text-xs"
            disabled={disabled}
            autoComplete="off"
            {...register(`${name}.${index}.name`)}
          />
          <Input
            aria-label={`Header ${index + 1} value`}
            id={`${idPrefix}-${index}-value`}
            type={secret ? "password" : "text"}
            placeholder={
              secret
                ? storedNames.includes(row.name)
                  ? "****"
                  : "secret value"
                : "application/json"
            }
            autoComplete={secret ? "new-password" : "off"}
            className="h-8 flex-[2] font-mono text-xs"
            disabled={disabled}
            {...register(`${name}.${index}.value`)}
          />
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7 text-muted-foreground hover:text-destructive"
            aria-label={`Remove header ${index + 1}`}
            disabled={disabled}
            onClick={() => remove(index)}
          >
            <X aria-hidden />
          </Button>
        </div>
      ))}
      <Button
        type="button"
        variant="outline"
        size="sm"
        className="h-7"
        disabled={disabled}
        onClick={() => append({ name: "", value: "" })}
      >
        <Plus aria-hidden />
        Add header
      </Button>
    </div>
  );
}
