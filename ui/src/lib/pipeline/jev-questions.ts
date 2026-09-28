/**
 * The `questions` block of a `jev_enricher` step, as editable rows.
 *
 * The config stores questions as a map keyed by the output field name, which
 * cannot hold what an author has on screen mid-edit: a blank key, two rows
 * with the same key, a choice option not yet named. The editor therefore works
 * on `QuestionDraft[]` and only writes the map back when `serializeQuestions`
 * can represent it. Limit checks (option and level counts, blank instructions)
 * are separate, in `validateQuestions`: those drafts still serialize, so the
 * YAML pane stays in sync while the author fixes them.
 *
 * Mirrors `crates/plugins/jev-enricher/src/lib.rs` (`Question`, `validate`).
 */
import type { JsonObject, JsonValue } from "@/lib/api/types";

export type QuestionType = "noul" | "choice" | "score";

export const QUESTION_TYPES: readonly QuestionType[] = ["noul", "choice", "score"];

/** Most options Jev accepts in one choice question. */
export const MAX_CHOICE_OPTIONS = 255;
/** Fewest and most levels Jev accepts in one score rubric. */
export const MIN_SCORE_LEVELS = 2;
export const MAX_SCORE_LEVELS = 10;

export interface ChoiceOption {
  id: string;
  key: string;
  description: string;
}

export interface ScoreLevel {
  id: string;
  text: string;
}

/**
 * One question row. Every type's fields are kept whatever the current `type`,
 * so switching a question to another type and back loses nothing; only the
 * current type's fields are written out.
 */
export interface QuestionDraft {
  /** Stable React key; not part of the config. */
  id: string;
  /** Map key, and the output field name. */
  key: string;
  type: QuestionType;
  instructions: string;
  /** `noul` criteria: what "yes" and "no" mean. Both blank means no criteria. */
  yes: string;
  no: string;
  /** `choice` criteria. */
  options: ChoiceOption[];
  /** `score` criteria, lowest first. */
  levels: ScoreLevel[];
}

let lastId = 0;
/** A process-unique id for a row, option or level. */
export function newId(): string {
  lastId += 1;
  return `jq${lastId}`;
}

export function newOption(key = ""): ChoiceOption {
  return { id: newId(), key, description: "" };
}

export function newLevel(text = ""): ScoreLevel {
  return { id: newId(), text };
}

function isObject(value: unknown): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

type Parsed = { ok: true; questions: QuestionDraft[] } | { ok: false; reason: string };

/**
 * Read a config value into rows. Refuses anything the rows cannot carry
 * (unknown keys, unknown types, wrongly shaped criteria) so the caller can
 * fall back to raw JSON instead of silently dropping it.
 */
export function parseQuestions(value: JsonValue | undefined): Parsed {
  if (value === undefined || value === null) return { ok: true, questions: [] };
  if (!isObject(value)) return { ok: false, reason: "questions must be an object" };

  const questions: QuestionDraft[] = [];
  for (const [key, raw] of Object.entries(value)) {
    if (!isObject(raw)) return { ok: false, reason: `"${key}" is not an object` };
    const extra = Object.keys(raw).filter((k) => !["type", "instructions", "criteria"].includes(k));
    if (extra.length > 0) {
      return { ok: false, reason: `"${key}" has keys this editor does not know: ${extra.join(", ")}` };
    }
    const type = raw.type;
    if (typeof type !== "string" || !(QUESTION_TYPES as readonly string[]).includes(type)) {
      return { ok: false, reason: `"${key}" has an unknown type: ${JSON.stringify(type)}` };
    }
    const instructions = raw.instructions ?? "";
    if (typeof instructions !== "string") {
      return { ok: false, reason: `"${key}".instructions is not a string` };
    }

    const draft: QuestionDraft = {
      id: newId(),
      key,
      type: type as QuestionType,
      instructions,
      yes: "",
      no: "",
      options: [],
      levels: [],
    };
    const criteria = raw.criteria;

    if (draft.type === "noul" && criteria !== undefined && criteria !== null) {
      if (!isObject(criteria)) return { ok: false, reason: `"${key}".criteria is not an object` };
      const unknown = Object.keys(criteria).filter((k) => k !== "true" && k !== "false");
      const { true: yes = "", false: no = "" } = criteria;
      if (unknown.length > 0 || typeof yes !== "string" || typeof no !== "string") {
        return { ok: false, reason: `"${key}".criteria must be { "true": text, "false": text }` };
      }
      draft.yes = yes;
      draft.no = no;
    }

    if (draft.type === "choice") {
      if (criteria !== undefined && !isObject(criteria)) {
        return { ok: false, reason: `"${key}".criteria must map each option to a description` };
      }
      for (const [option, description] of Object.entries(criteria ?? {})) {
        if (typeof description !== "string") {
          return { ok: false, reason: `"${key}".criteria.${option} is not a string` };
        }
        draft.options.push({ id: newId(), key: option, description });
      }
    }

    if (draft.type === "score") {
      if (criteria !== undefined && !Array.isArray(criteria)) {
        return { ok: false, reason: `"${key}".criteria must list the levels` };
      }
      for (const level of criteria ?? []) {
        if (typeof level !== "string") {
          return { ok: false, reason: `"${key}".criteria has a level that is not a string` };
        }
        draft.levels.push(newLevel(level));
      }
    }

    questions.push(draft);
  }
  return { ok: true, questions };
}

type Serialized = { ok: true; value: JsonObject } | { ok: false; reason: string };

/**
 * Rows → the config map. Fails only when the map cannot hold the rows: a
 * blank or repeated question key, or a blank or repeated option key.
 */
export function serializeQuestions(questions: QuestionDraft[]): Serialized {
  const value: JsonObject = {};
  for (const question of questions) {
    const key = question.key.trim();
    if (key.length === 0) return { ok: false, reason: "a question has no key" };
    if (Object.prototype.hasOwnProperty.call(value, key)) {
      return { ok: false, reason: `question key "${key}" is used twice` };
    }

    const out: JsonObject = { type: question.type, instructions: question.instructions };
    switch (question.type) {
      case "noul":
        if (question.yes.length > 0 || question.no.length > 0) {
          out.criteria = { true: question.yes, false: question.no };
        }
        break;
      case "choice": {
        const criteria: JsonObject = {};
        for (const option of question.options) {
          const optionKey = option.key.trim();
          if (optionKey.length === 0) return { ok: false, reason: `"${key}" has an unnamed option` };
          if (Object.prototype.hasOwnProperty.call(criteria, optionKey)) {
            return { ok: false, reason: `"${key}" lists option "${optionKey}" twice` };
          }
          criteria[optionKey] = option.description;
        }
        out.criteria = criteria;
        break;
      }
      case "score":
        out.criteria = question.levels.map((level) => level.text);
        break;
    }
    value[key] = out;
  }
  return { ok: true, value };
}

function nextQuestionKey(questions: QuestionDraft[]): string {
  const taken = new Set(questions.map((q) => q.key.trim()));
  let n = 1;
  while (taken.has(`question_${n}`)) n += 1;
  return `question_${n}`;
}

/** Append a yes/no question with a fresh key, which serializes as is. */
export function addQuestion(questions: QuestionDraft[]): QuestionDraft[] {
  return [
    ...questions,
    {
      id: newId(),
      key: nextQuestionKey(questions),
      type: "noul",
      instructions: "",
      yes: "",
      no: "",
      options: [],
      levels: [],
    },
  ];
}

/**
 * Switch a question's type. A choice with no options yet gets two named ones
 * and a score with no levels gets three blank ones, so the author starts from
 * something that serializes and only has to fill it in.
 */
export function changeType(question: QuestionDraft, type: QuestionType): QuestionDraft {
  const next = { ...question, type };
  if (type === "choice" && next.options.length === 0) {
    next.options = [newOption("option_1"), newOption("option_2")];
  }
  if (type === "score" && next.levels.length === 0) {
    next.levels = [newLevel(), newLevel(), newLevel()];
  }
  return next;
}

export interface QuestionIssues {
  /** Problems with the block as a whole. */
  form: string[];
  /** Problems of one row, keyed by `QuestionDraft.id`. Rows without issues are absent. */
  byQuestion: Record<string, string[]>;
}

/** Everything the plugin would reject at run time, worded for the editor. */
export function validateQuestions(questions: QuestionDraft[]): QuestionIssues {
  const form = questions.length === 0 ? ["Add at least one question."] : [];
  const byQuestion: Record<string, string[]> = {};
  const add = (id: string, message: string) => {
    (byQuestion[id] ??= []).push(message);
  };

  const keyCounts = new Map<string, number>();
  for (const question of questions) {
    const key = question.key.trim();
    if (key.length > 0) keyCounts.set(key, (keyCounts.get(key) ?? 0) + 1);
  }

  for (const question of questions) {
    const key = question.key.trim();
    if (key.length === 0) add(question.id, "Key is required.");
    else if ((keyCounts.get(key) ?? 0) > 1) add(question.id, `Key "${key}" is used twice.`);
    if (question.instructions.trim().length === 0) add(question.id, "Instructions are required.");

    switch (question.type) {
      case "noul":
        if ((question.yes.trim().length > 0) !== (question.no.trim().length > 0)) {
          add(question.id, 'Describe both "yes" and "no", or neither.');
        }
        break;
      case "choice": {
        const count = question.options.length;
        if (count < 2 || count > MAX_CHOICE_OPTIONS) {
          add(question.id, `A choice needs 2 to ${MAX_CHOICE_OPTIONS} options.`);
        }
        const seen = new Set<string>();
        const repeated = new Set<string>();
        for (const option of question.options) {
          const optionKey = option.key.trim();
          if (seen.has(optionKey)) repeated.add(optionKey);
          seen.add(optionKey);
        }
        if (seen.has("")) add(question.id, "Every option needs a key.");
        for (const optionKey of repeated) {
          if (optionKey.length > 0) add(question.id, `Option "${optionKey}" is listed twice.`);
        }
        break;
      }
      case "score": {
        const count = question.levels.length;
        if (count < MIN_SCORE_LEVELS || count > MAX_SCORE_LEVELS) {
          add(question.id, `A score needs ${MIN_SCORE_LEVELS} to ${MAX_SCORE_LEVELS} levels.`);
        }
        if (question.levels.some((level) => level.text.trim().length === 0)) {
          add(question.id, "Every level needs a label.");
        }
        break;
      }
    }
  }
  return { form, byQuestion };
}

/**
 * The problems of a `questions` config value, as messages prefixed with where
 * they are (`questions.category: …`). Used by the draft validator, so a block
 * the plugin would reject blocks Save rather than failing the step at run time.
 */
export function questionsConfigIssues(name: string, value: JsonValue | undefined): string[] {
  const parsed = parseQuestions(value);
  if (!parsed.ok) return [`${name}: ${parsed.reason}`];
  const { form, byQuestion } = validateQuestions(parsed.questions);
  return [
    ...form.map((message) => `${name}: ${message}`),
    ...parsed.questions.flatMap((question) =>
      (byQuestion[question.id] ?? []).map(
        (message) => `${name}.${question.key.trim() || "(no key)"}: ${message}`,
      ),
    ),
  ];
}
