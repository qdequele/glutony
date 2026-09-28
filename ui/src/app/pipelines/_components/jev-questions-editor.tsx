"use client";

/**
 * Schema-form widget for `format: "jev-questions"` — the `questions` key of a
 * `jev_enricher` step. One row per question (key, type, instructions) with the
 * type's criteria below it: option → description pairs for a choice, an
 * ordered list of levels for a score, optional yes/no descriptions for a noul.
 *
 * Rows are local state because the config map cannot hold a half-typed row
 * (see `./jev-questions.ts`); the map is written back whenever it can be. A
 * value this editor cannot represent falls back to the form's JSON textarea
 * rather than being rewritten.
 */
import { Plus, Trash2, X } from "lucide-react";
import { useMemo, useState } from "react";

import { Alert, AlertDescription } from "@/components/ui/alert";
import { Button } from "@/components/ui/button";
import { FieldDescription, FieldError } from "@/components/ui/field";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Textarea } from "@/components/ui/textarea";
import type { JsonValue } from "@/lib/api/types";
import { TextControl, type SchemaWidgetProps } from "@/lib/schema-form";
import {
  MAX_CHOICE_OPTIONS,
  MAX_SCORE_LEVELS,
  MIN_SCORE_LEVELS,
  QUESTION_TYPES,
  addQuestion,
  changeType,
  newLevel,
  newOption,
  parseQuestions,
  serializeQuestions,
  validateQuestions,
  type QuestionDraft,
  type QuestionType,
} from "@/lib/pipeline/jev-questions";

const TYPE_LABELS: Record<QuestionType, string> = {
  noul: "Yes / no",
  choice: "Choice",
  score: "Score",
};

/** What lands in the document, per type. */
const OUTPUT_HINTS: Record<QuestionType, string> = {
  noul: "true or false (true at or above noul_threshold)",
  choice: "the key of the chosen option",
  score: "a number placing the document along the levels",
};

function sameJson(a: unknown, b: unknown): boolean {
  return JSON.stringify(a ?? null) === JSON.stringify(b ?? null);
}

export function JevQuestionsEditor({
  field,
  value,
  onChange,
  disabled,
  inputId,
}: SchemaWidgetProps) {
  const parsed = useMemo(() => parseQuestions(value), [value]);
  const [rows, setRows] = useState<QuestionDraft[]>(() => (parsed.ok ? parsed.questions : []));
  // The value `rows` mirrors: the last one written back, or the last one adopted.
  const [synced, setSynced] = useState<JsonValue | undefined>(value);

  // The YAML pane (or the JSON fallback) rewrote the value: start again from it.
  // Adjusted during render rather than in an effect, so there is no stale frame.
  if (!sameJson(value, synced)) {
    setSynced(value);
    if (parsed.ok) setRows(parsed.questions);
  }

  if (!parsed.ok) {
    return (
      <>
        <Alert>
          <AlertDescription className="text-xs">
            Showing raw JSON: {parsed.reason}.
          </AlertDescription>
        </Alert>
        <TextControl
          field={field}
          value={value}
          onChange={onChange}
          disabled={disabled}
          inputId={inputId}
        />
      </>
    );
  }

  const serialized = serializeQuestions(rows);
  const issues = validateQuestions(rows);

  function update(next: QuestionDraft[]) {
    setRows(next);
    const result = serializeQuestions(next);
    if (result.ok) {
      setSynced(result.value);
      onChange(result.value);
    }
  }

  function patch(id: string, change: (question: QuestionDraft) => QuestionDraft) {
    update(rows.map((question) => (question.id === id ? change(question) : question)));
  }

  return (
    <div className="space-y-2">
      {rows.map((question, index) => (
        <QuestionRow
          key={question.id}
          question={question}
          keyInputId={index === 0 ? inputId : undefined}
          errors={issues.byQuestion[question.id] ?? []}
          disabled={disabled}
          onPatch={(change) => patch(question.id, change)}
          onRemove={() => update(rows.filter((entry) => entry.id !== question.id))}
        />
      ))}

      {issues.form.map((message) => (
        <FieldError key={message}>{message}</FieldError>
      ))}
      {!serialized.ok ? (
        <FieldDescription className="text-xs text-destructive">
          Not applied yet: {serialized.reason}. Every question and option needs its own key.
        </FieldDescription>
      ) : null}

      {disabled ? null : (
        <Button
          id={rows.length === 0 ? inputId : undefined}
          type="button"
          variant="outline"
          size="sm"
          onClick={() => update(addQuestion(rows))}
        >
          <Plus aria-hidden />
          Add question
        </Button>
      )}
    </div>
  );
}

function QuestionRow({
  question,
  keyInputId,
  errors,
  disabled,
  onPatch,
  onRemove,
}: {
  question: QuestionDraft;
  keyInputId?: string;
  errors: string[];
  disabled?: boolean;
  onPatch: (change: (question: QuestionDraft) => QuestionDraft) => void;
  onRemove: () => void;
}) {
  const key = question.key.trim();
  const invalid = errors.length > 0;

  return (
    <div
      className="space-y-2 rounded-md border p-2.5 data-[invalid=true]:border-destructive/50"
      data-invalid={invalid}
    >
      <div className="flex items-center gap-1.5">
        <Input
          id={keyInputId}
          aria-label="Question key (output field)"
          aria-invalid={errors.some((message) => message.startsWith("Key"))}
          value={question.key}
          disabled={disabled}
          placeholder="field_name"
          className="h-8 min-w-0 flex-1 font-mono text-xs"
          onChange={(event) => {
            const next = event.target.value;
            onPatch((current) => ({ ...current, key: next }));
          }}
        />
        <Select
          value={question.type}
          disabled={disabled}
          onValueChange={(next) => onPatch((current) => changeType(current, next as QuestionType))}
        >
          <SelectTrigger aria-label="Question type" className="w-28 shrink-0 text-xs">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {QUESTION_TYPES.map((type) => (
              <SelectItem key={type} value={type} className="text-xs">
                {TYPE_LABELS[type]}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
        {disabled ? null : (
          <Button
            type="button"
            variant="ghost"
            size="icon"
            className="size-7 shrink-0 text-muted-foreground hover:text-destructive"
            aria-label={`Remove question ${key || "without a key"}`}
            onClick={onRemove}
          >
            <Trash2 aria-hidden />
          </Button>
        )}
      </div>

      <Textarea
        aria-label="Instructions"
        aria-invalid={errors.includes("Instructions are required.")}
        value={question.instructions}
        disabled={disabled}
        rows={2}
        placeholder={
          question.type === "noul"
            ? "Does this page describe a deprecated feature?"
            : question.type === "choice"
              ? "What is this page about?"
              : "How complete is this page?"
        }
        className="min-h-0 text-xs"
        onChange={(event) => {
          const next = event.target.value;
          onPatch((current) => ({ ...current, instructions: next }));
        }}
      />

      {question.type === "noul" ? (
        <NoulCriteria question={question} disabled={disabled} onPatch={onPatch} />
      ) : question.type === "choice" ? (
        <ChoiceCriteria question={question} disabled={disabled} onPatch={onPatch} />
      ) : (
        <ScoreCriteria question={question} disabled={disabled} onPatch={onPatch} />
      )}

      <FieldDescription className="text-xs">
        Writes <span className="font-mono">fields.{key || "…"}</span>: {OUTPUT_HINTS[question.type]}.
      </FieldDescription>
      {errors.map((message) => (
        <FieldError key={message}>{message}</FieldError>
      ))}
    </div>
  );
}

interface CriteriaProps {
  question: QuestionDraft;
  disabled?: boolean;
  onPatch: (change: (question: QuestionDraft) => QuestionDraft) => void;
}

function NoulCriteria({ question, disabled, onPatch }: CriteriaProps) {
  return (
    <div className="space-y-1.5">
      <p className="text-xs font-medium">
        Criteria <span className="font-normal text-muted-foreground">optional</span>
      </p>
      <div className="grid gap-1.5 sm:grid-cols-2">
        <Input
          aria-label='What "yes" means (optional)'
          value={question.yes}
          disabled={disabled}
          placeholder="Yes means…"
          className="h-8 text-xs"
          onChange={(event) => {
            const next = event.target.value;
            onPatch((current) => ({ ...current, yes: next }));
          }}
        />
        <Input
          aria-label='What "no" means (optional)'
          value={question.no}
          disabled={disabled}
          placeholder="No means…"
          className="h-8 text-xs"
          onChange={(event) => {
            const next = event.target.value;
            onPatch((current) => ({ ...current, no: next }));
          }}
        />
      </div>
    </div>
  );
}

function ChoiceCriteria({ question, disabled, onPatch }: CriteriaProps) {
  const full = question.options.length >= MAX_CHOICE_OPTIONS;

  function patchOption(id: string, change: { key?: string; description?: string }) {
    onPatch((current) => ({
      ...current,
      options: current.options.map((option) =>
        option.id === id ? { ...option, ...change } : option,
      ),
    }));
  }

  return (
    <div className="space-y-1.5">
      <p className="text-xs font-medium">
        Options{" "}
        <span className="font-normal tabular-nums text-muted-foreground">
          {question.options.length} / {MAX_CHOICE_OPTIONS}
        </span>
      </p>
      {question.options.map((option, index) => (
        <div key={option.id} className="flex items-center gap-1.5">
          <Input
            aria-label={`Option ${index + 1} key`}
            value={option.key}
            disabled={disabled}
            placeholder="option_key"
            className="h-8 w-2/5 min-w-0 font-mono text-xs"
            onChange={(event) => patchOption(option.id, { key: event.target.value })}
          />
          <Input
            aria-label={`Option ${index + 1} description`}
            value={option.description}
            disabled={disabled}
            placeholder="What this option covers"
            className="h-8 min-w-0 flex-1 text-xs"
            onChange={(event) => patchOption(option.id, { description: event.target.value })}
          />
          {disabled ? null : (
            <Button
              type="button"
              variant="ghost"
              size="icon"
              className="size-7 shrink-0 text-muted-foreground"
              aria-label={`Remove option ${option.key || index + 1}`}
              onClick={() =>
                onPatch((current) => ({
                  ...current,
                  options: current.options.filter((entry) => entry.id !== option.id),
                }))
              }
            >
              <X aria-hidden />
            </Button>
          )}
        </div>
      ))}
      {disabled ? null : (
        <Button
          type="button"
          variant="ghost"
          size="xs"
          disabled={full}
          onClick={() =>
            onPatch((current) => ({ ...current, options: [...current.options, newOption()] }))
          }
        >
          <Plus aria-hidden />
          Add option
        </Button>
      )}
    </div>
  );
}

function ScoreCriteria({ question, disabled, onPatch }: CriteriaProps) {
  const full = question.levels.length >= MAX_SCORE_LEVELS;

  return (
    <div className="space-y-1.5">
      <p className="text-xs font-medium">
        Levels{" "}
        <span className="font-normal text-muted-foreground">
          lowest first, {MIN_SCORE_LEVELS}–{MAX_SCORE_LEVELS}
        </span>
      </p>
      <ol className="space-y-1.5">
        {question.levels.map((level, index) => (
          <li key={level.id} className="flex items-center gap-1.5">
            <span className="w-5 shrink-0 text-right text-xs tabular-nums text-muted-foreground">
              {index + 1}.
            </span>
            <Input
              aria-label={`Level ${index + 1}`}
              value={level.text}
              disabled={disabled}
              placeholder={
                index === 0
                  ? "Lowest, e.g. stub"
                  : index === question.levels.length - 1
                    ? "Highest, e.g. complete"
                    : "e.g. partial"
              }
              className="h-8 min-w-0 flex-1 text-xs"
              onChange={(event) => {
                const next = event.target.value;
                onPatch((current) => ({
                  ...current,
                  levels: current.levels.map((entry) =>
                    entry.id === level.id ? { ...entry, text: next } : entry,
                  ),
                }));
              }}
            />
            {disabled ? null : (
              <Button
                type="button"
                variant="ghost"
                size="icon"
                className="size-7 shrink-0 text-muted-foreground"
                aria-label={`Remove level ${index + 1}`}
                onClick={() =>
                  onPatch((current) => ({
                    ...current,
                    levels: current.levels.filter((entry) => entry.id !== level.id),
                  }))
                }
              >
                <X aria-hidden />
              </Button>
            )}
          </li>
        ))}
      </ol>
      {disabled ? null : (
        <Button
          type="button"
          variant="ghost"
          size="xs"
          disabled={full}
          onClick={() =>
            onPatch((current) => ({ ...current, levels: [...current.levels, newLevel()] }))
          }
        >
          <Plus aria-hidden />
          Add level
        </Button>
      )}
    </div>
  );
}
