"use client";

import { X } from "lucide-react";
import { useState, type KeyboardEvent } from "react";

import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";

/**
 * A list of short strings (MIME types, step ids) edited as chips.
 * Enter or a comma commits the current token; Backspace on an empty input
 * removes the last one.
 */
export function MultiValueInput({
  id,
  values,
  onChange,
  placeholder,
  disabled,
}: {
  id: string;
  values: string[];
  onChange: (next: string[]) => void;
  placeholder?: string;
  disabled?: boolean;
}) {
  const [draft, setDraft] = useState("");

  function commit(raw: string) {
    const value = raw.trim().replace(/,$/, "");
    if (value.length === 0) return;
    if (!values.includes(value)) onChange([...values, value]);
    setDraft("");
  }

  function onKeyDown(event: KeyboardEvent<HTMLInputElement>) {
    if (event.key === "Enter" || event.key === ",") {
      event.preventDefault();
      commit(draft);
      return;
    }
    if (event.key === "Backspace" && draft.length === 0 && values.length > 0) {
      onChange(values.slice(0, -1));
    }
  }

  return (
    <div className="space-y-1.5">
      <Input
        id={id}
        value={draft}
        disabled={disabled}
        placeholder={placeholder}
        onChange={(event) => setDraft(event.target.value)}
        onKeyDown={onKeyDown}
        onBlur={() => commit(draft)}
      />
      {values.length > 0 ? (
        <div className="flex flex-wrap gap-1">
          {values.map((value) => (
            <Badge key={value} variant="secondary" className="gap-1 font-mono text-[11px]">
              {value}
              {disabled ? null : (
                <button
                  type="button"
                  aria-label={`Remove ${value}`}
                  className="text-muted-foreground hover:text-foreground"
                  onClick={() => onChange(values.filter((entry) => entry !== value))}
                >
                  <X className="size-3" aria-hidden />
                </button>
              )}
            </Badge>
          ))}
        </div>
      ) : null}
    </div>
  );
}
