import { describe, expect, it } from "vitest";

import {
  MAX_CHOICE_OPTIONS,
  MAX_SCORE_LEVELS,
  addQuestion,
  changeType,
  parseQuestions,
  serializeQuestions,
  validateQuestions,
  type QuestionDraft,
} from "./jev-questions";

const sample = {
  category: {
    type: "choice",
    instructions: "What is this page about?",
    criteria: { billing: "Invoices", api: "API reference" },
  },
  is_outdated: {
    type: "noul",
    instructions: "Is this deprecated?",
    criteria: { true: "Deprecated", false: "Current" },
  },
  quality: {
    type: "score",
    instructions: "How complete is it?",
    criteria: ["stub", "partial", "complete"],
  },
  plain: { type: "noul", instructions: "Is it in English?" },
};

function parsed(value: unknown): QuestionDraft[] {
  const result = parseQuestions(value as never);
  if (!result.ok) throw new Error(result.reason);
  return result.questions;
}

describe("parseQuestions", () => {
  it("reads every question type, in key order", () => {
    const questions = parsed(sample);
    expect(questions.map((q) => [q.key, q.type])).toEqual([
      ["category", "choice"],
      ["is_outdated", "noul"],
      ["quality", "score"],
      ["plain", "noul"],
    ]);
    expect(questions[0].options.map((o) => [o.key, o.description])).toEqual([
      ["billing", "Invoices"],
      ["api", "API reference"],
    ]);
    expect(questions[1].yes).toBe("Deprecated");
    expect(questions[1].no).toBe("Current");
    expect(questions[2].levels.map((l) => l.text)).toEqual(["stub", "partial", "complete"]);
    expect(questions[3].yes).toBe("");
  });

  it("treats a missing value as no questions yet", () => {
    expect(parsed(undefined)).toEqual([]);
    expect(parsed({})).toEqual([]);
  });

  it("gives every row, option and level a distinct id", () => {
    const questions = parsed(sample);
    const ids = questions.flatMap((q) => [
      q.id,
      ...q.options.map((o) => o.id),
      ...q.levels.map((l) => l.id),
    ]);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("refuses shapes it would lose data on, so the JSON fallback keeps them", () => {
    for (const [label, value] of [
      ["not an object", ["a"]],
      ["question not an object", { q: "yes?" }],
      ["unknown type", { q: { type: "rank", instructions: "x" } }],
      ["unknown key", { q: { type: "noul", instructions: "x", weight: 2 } }],
      ["choice criteria is a list", { q: { type: "choice", instructions: "x", criteria: ["a"] } }],
      ["score criteria is a map", { q: { type: "score", instructions: "x", criteria: { a: "b" } } }],
      ["non-string level", { q: { type: "score", instructions: "x", criteria: [1, 2] } }],
      ["noul criteria is a string", { q: { type: "noul", instructions: "x", criteria: "yes" } }],
    ] as const) {
      const result = parseQuestions(value as never);
      expect(result.ok, label).toBe(false);
    }
  });
});

describe("serializeQuestions", () => {
  it("round-trips what it parsed", () => {
    const result = serializeQuestions(parsed(sample));
    expect(result).toEqual({ ok: true, value: sample });
  });

  it("writes noul criteria only when one side is described", () => {
    const [question] = parsed({ q: { type: "noul", instructions: "x" } });
    expect(serializeQuestions([question])).toEqual({
      ok: true,
      value: { q: { type: "noul", instructions: "x" } },
    });
    expect(serializeQuestions([{ ...question, yes: "Yes it is" }])).toEqual({
      ok: true,
      value: { q: { type: "noul", instructions: "x", criteria: { true: "Yes it is", false: "" } } },
    });
  });

  it("only writes the fields of the current type", () => {
    const [choice] = parsed({ q: sample.category });
    const asScore = changeType(choice, "score");
    const result = serializeQuestions([asScore]);
    expect(result.ok && Object.keys((result.value as Record<string, object>).q)).toEqual([
      "type",
      "instructions",
      "criteria",
    ]);
    expect(result.ok && Array.isArray((result.value.q as Record<string, unknown>).criteria)).toBe(
      true,
    );
  });

  it("cannot write empty or duplicate keys, which a map cannot hold", () => {
    const questions = parsed(sample);
    expect(serializeQuestions([{ ...questions[0], key: " " }]).ok).toBe(false);
    expect(serializeQuestions([questions[0], { ...questions[1], key: "category" }]).ok).toBe(false);

    const choice = questions[0];
    const dupOption = { ...choice, options: [choice.options[0], { ...choice.options[1], key: "billing" }] };
    expect(serializeQuestions([dupOption]).ok).toBe(false);
    const blankOption = { ...choice, options: [{ ...choice.options[0], key: "" }, choice.options[1]] };
    expect(serializeQuestions([blankOption]).ok).toBe(false);
  });

  it("ignores option problems on a question that is no longer a choice", () => {
    const choice = parsed({ q: sample.category })[0];
    const broken = { ...choice, options: [{ ...choice.options[0], key: "" }] };
    expect(serializeQuestions([changeType(broken, "noul")]).ok).toBe(true);
  });

  it("trims keys", () => {
    const [question] = parsed({ q: sample.plain });
    const result = serializeQuestions([{ ...question, key: "  lang  " }]);
    expect(result.ok && Object.keys(result.value)).toEqual(["lang"]);
  });
});

describe("addQuestion and changeType", () => {
  it("adds a yes/no question with a fresh key that serializes right away", () => {
    const first = addQuestion([]);
    const second = addQuestion(first);
    expect(second.map((q) => [q.key, q.type])).toEqual([
      ["question_1", "noul"],
      ["question_2", "noul"],
    ]);
    expect(serializeQuestions(second).ok).toBe(true);
  });

  it("skips keys that are already taken", () => {
    const questions = addQuestion(parsed({ question_1: sample.plain }));
    expect(questions.map((q) => q.key)).toEqual(["question_1", "question_2"]);
  });

  it("seeds a choice with two options and a score with three levels", () => {
    const [question] = addQuestion([]);
    const choice = changeType(question, "choice");
    expect(choice.options.map((o) => o.key)).toEqual(["option_1", "option_2"]);
    expect(serializeQuestions([choice]).ok).toBe(true);
    expect(changeType(question, "score").levels).toHaveLength(3);
  });

  it("keeps what was typed when switching back and forth", () => {
    const choice = parsed({ q: sample.category })[0];
    const back = changeType(changeType(choice, "score"), "choice");
    expect(back.options.map((o) => o.key)).toEqual(["billing", "api"]);
  });
});

describe("validateQuestions", () => {
  function issuesOf(value: unknown) {
    return validateQuestions(parsed(value));
  }

  it("accepts a valid set", () => {
    expect(validateQuestions(parsed(sample))).toEqual({ form: [], byQuestion: {} });
  });

  it("asks for at least one question", () => {
    expect(validateQuestions([]).form).toEqual(["Add at least one question."]);
  });

  it("reports blank and duplicate keys and blank instructions per question", () => {
    const questions = parsed(sample);
    const result = validateQuestions([
      { ...questions[0], key: "" },
      { ...questions[1], key: "quality" },
      { ...questions[2], instructions: "  " },
    ]);
    expect(result.byQuestion[questions[0].id]).toContain("Key is required.");
    expect(result.byQuestion[questions[1].id]).toContain('Key "quality" is used twice.');
    expect(result.byQuestion[questions[2].id]).toContain('Key "quality" is used twice.');
    expect(result.byQuestion[questions[2].id]).toContain("Instructions are required.");
  });

  it("enforces Jev's option and level limits", () => {
    const tooFewOptions = issuesOf({ q: { type: "choice", instructions: "x", criteria: { a: "" } } });
    expect(Object.values(tooFewOptions.byQuestion).flat()).toContain(
      `A choice needs 2 to ${MAX_CHOICE_OPTIONS} options.`,
    );
    const many = Object.fromEntries(
      Array.from({ length: MAX_CHOICE_OPTIONS + 1 }, (_, i) => [`o${i}`, ""]),
    );
    expect(
      Object.values(issuesOf({ q: { type: "choice", instructions: "x", criteria: many } }).byQuestion)
        .flat(),
    ).toContain(`A choice needs 2 to ${MAX_CHOICE_OPTIONS} options.`);

    const tooManyLevels = issuesOf({
      q: { type: "score", instructions: "x", criteria: Array(MAX_SCORE_LEVELS + 1).fill("l") },
    });
    expect(Object.values(tooManyLevels.byQuestion).flat()).toContain(
      `A score needs 2 to ${MAX_SCORE_LEVELS} levels.`,
    );
    const blankLevel = issuesOf({ q: { type: "score", instructions: "x", criteria: ["low", " "] } });
    expect(Object.values(blankLevel.byQuestion).flat()).toContain("Every level needs a label.");
  });

  it("reports blank and duplicate option keys", () => {
    const [choice] = parsed({ q: sample.category });
    const result = validateQuestions([
      { ...choice, options: [{ ...choice.options[0], key: "" }, { ...choice.options[1], key: "api" }] },
    ]);
    expect(result.byQuestion[choice.id]).toContain("Every option needs a key.");

    const dup = validateQuestions([
      { ...choice, options: [choice.options[0], { ...choice.options[1], key: "billing" }] },
    ]);
    expect(dup.byQuestion[choice.id]).toContain('Option "billing" is listed twice.');
  });

  it("needs both sides of a noul description once one is written", () => {
    const [question] = parsed({ q: sample.plain });
    expect(validateQuestions([{ ...question, yes: "Yes" }]).byQuestion[question.id]).toContain(
      'Describe both "yes" and "no", or neither.',
    );
  });
});
