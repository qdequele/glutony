import { describe, expect, it } from "vitest";

import type { PluginManifest } from "@/lib/api/types";
import { emptyDraft, type PipelineDraft, type StepDraft } from "./draft";
import { acceptsInput, issuesByStep, outputToInput, validateDraft } from "./validate";

/** Manifests copied from the real built-in plugins. */
const PLUGINS: PluginManifest[] = [
  {
    name: "pdf_extractor",
    version: "0.1.0",
    accepts: ["bytes"],
    produces: "documents",
  },
  {
    name: "chunker",
    version: "0.1.0",
    accepts: ["documents", "many", "bytes"],
    produces: "documents",
  },
  {
    name: "llm_enricher",
    version: "0.1.0",
    accepts: ["documents", "many"],
    produces: "documents",
  },
  {
    name: "meili_indexer",
    version: "0.1.0",
    accepts: ["documents", "many", "empty"],
    produces: "indexed",
  },
  {
    name: "video_audio_extractor",
    version: "0.1.0",
    accepts: ["bytes"],
    produces: "bytes",
  },
  {
    name: "audio_transcriber",
    version: "0.1.0",
    accepts: ["bytes"],
    produces: "documents",
  },
];

function step(id: string, plugin: string, extra: Partial<StepDraft> = {}): StepDraft {
  return { id, plugin, depends_on: [], config: {}, ...extra };
}

function draft(steps: StepDraft[], uid = "test"): PipelineDraft {
  return { ...emptyDraft(), uid, steps };
}

const rules = (issues: ReturnType<typeof validateDraft>) => issues.map((issue) => issue.rule);

describe("a valid pipeline", () => {
  it("reports nothing for the canonical PDF pipeline", () => {
    const pipeline = draft([
      step("extract", "pdf_extractor"),
      step("chunk", "chunker", { depends_on: ["extract"] }),
      step("enrich", "llm_enricher", { depends_on: ["chunk"], fan_out: "$.documents" }),
      step("index", "meili_indexer", { depends_on: ["enrich"] }),
    ]);
    expect(validateDraft(pipeline, PLUGINS)).toEqual([]);
  });

  it("reports nothing for builtin.text, which feeds raw bytes into the chunker", () => {
    const pipeline = draft(
      [
        step("chunk", "chunker"),
        step("index", "meili_indexer", { depends_on: ["chunk"] }),
      ],
      "builtin.text",
    );
    expect(validateDraft(pipeline, PLUGINS)).toEqual([]);
  });
});

describe("rule: uid", () => {
  it("rejects an empty uid", () => {
    expect(rules(validateDraft(draft([step("a", "chunker")], ""), PLUGINS))).toContain(
      "invalid_uid",
    );
  });

  it("rejects characters the control plane forbids", () => {
    expect(rules(validateDraft(draft([step("a", "chunker")], "my pipeline!"), PLUGINS))).toContain(
      "invalid_uid",
    );
  });
});

describe("rule: empty pipeline", () => {
  it("rejects a pipeline with no steps", () => {
    expect(rules(validateDraft(draft([]), PLUGINS))).toEqual(["empty_pipeline"]);
  });
});

describe("rule: duplicate step ids", () => {
  it("flags the second occurrence", () => {
    const issues = validateDraft(
      draft([step("a", "chunker"), step("a", "meili_indexer", { depends_on: ["a"] })]),
      PLUGINS,
    );
    const duplicate = issues.find((issue) => issue.rule === "duplicate_step");
    expect(duplicate).toBeDefined();
    expect(duplicate?.stepId).toBe("a");
    expect(duplicate?.field).toBe("id");
  });

  it("flags an empty step id", () => {
    expect(rules(validateDraft(draft([step("", "chunker")]), PLUGINS))).toContain(
      "invalid_step_id",
    );
  });
});

describe("rule: depends_on references an unknown step", () => {
  it("flags the missing dependency on the offending step", () => {
    const issues = validateDraft(
      draft([step("a", "chunker"), step("b", "meili_indexer", { depends_on: ["nope"] })]),
      PLUGINS,
    );
    const issue = issues.find((i) => i.rule === "unknown_dependency");
    expect(issue?.stepId).toBe("b");
    expect(issue?.message).toContain("nope");
  });
});

describe("rule: depends_on references a later step", () => {
  it("flags a forward reference", () => {
    const issues = validateDraft(
      draft([
        step("a", "chunker", { depends_on: ["b"] }),
        step("b", "chunker"),
      ]),
      PLUGINS,
    );
    const issue = issues.find((i) => i.rule === "forward_dependency");
    expect(issue?.stepId).toBe("a");
    expect(issue?.field).toBe("depends_on");
  });

  it("flags a step depending on itself", () => {
    const issues = validateDraft(draft([step("a", "chunker", { depends_on: ["a"] })]), PLUGINS);
    expect(issues.find((i) => i.rule === "forward_dependency")?.message).toContain("itself");
  });
});

describe("rule: cycle", () => {
  it("flags every step involved in a cycle", () => {
    const issues = validateDraft(
      draft([
        step("a", "chunker", { depends_on: ["c"] }),
        step("b", "chunker", { depends_on: ["a"] }),
        step("c", "chunker", { depends_on: ["b"] }),
      ]),
      PLUGINS,
    );
    const cycles = issues.filter((issue) => issue.rule === "cycle");
    expect(cycles.map((issue) => issue.stepId).sort()).toEqual(["a", "b", "c"]);
    expect(cycles[0].message).toContain("→");
  });

  it("does not see a cycle in a diamond", () => {
    const issues = validateDraft(
      draft([
        step("a", "pdf_extractor"),
        step("b", "chunker", { depends_on: ["a"] }),
        step("c", "chunker", { depends_on: ["a"] }),
        step("d", "meili_indexer", { depends_on: ["b", "c"] }),
      ]),
      PLUGINS,
    );
    expect(rules(issues)).not.toContain("cycle");
  });
});

describe("rule: fan-out", () => {
  it("flags a fan-out step with more than one dependency", () => {
    const issues = validateDraft(
      draft([
        step("a", "pdf_extractor"),
        step("b", "chunker", { depends_on: ["a"] }),
        step("c", "llm_enricher", { depends_on: ["a", "b"], fan_out: "$.documents" }),
      ]),
      PLUGINS,
    );
    const issue = issues.find((i) => i.rule === "fan_out_arity");
    expect(issue?.stepId).toBe("c");
    expect(issue?.field).toBe("fan_out");
  });

  it("flags a fan-out step with no dependency", () => {
    const issues = validateDraft(
      draft([step("a", "llm_enricher", { fan_out: "$.documents" })]),
      PLUGINS,
    );
    expect(rules(issues)).toContain("fan_out_arity");
  });

  it("flags an unsupported fan-out path", () => {
    const pipeline = draft([
      step("a", "pdf_extractor"),
      // The draft type only allows the three valid paths; a hand-edited YAML
      // document can still carry something else.
      { ...step("b", "llm_enricher", { depends_on: ["a"] }), fan_out: "$.pages" } as unknown as StepDraft,
    ]);
    expect(rules(validateDraft(pipeline, PLUGINS))).toContain("fan_out_path");
  });

  it("flags fanning documents out of a step that produces bytes", () => {
    const issues = validateDraft(
      draft([
        step("a", "video_audio_extractor"),
        step("b", "llm_enricher", { depends_on: ["a"], fan_out: "$.documents" }),
      ]),
      PLUGINS,
    );
    expect(rules(issues)).toContain("fan_out_source");
  });
});

describe("rule: unknown plugin", () => {
  it("flags a plugin no worker registered", () => {
    const issues = validateDraft(draft([step("a", "llm_enrichr")]), PLUGINS);
    expect(issues.find((i) => i.rule === "unknown_plugin")?.field).toBe("plugin");
  });

  it("skips plugin rules entirely while the manifests are loading", () => {
    expect(validateDraft(draft([step("a", "whatever")]))).toEqual([]);
  });
});

describe("rule: type compatibility", () => {
  it("flags documents fed into a plugin that only accepts bytes", () => {
    const issues = validateDraft(
      draft([
        step("extract", "pdf_extractor"),
        step("transcribe", "audio_transcriber", { depends_on: ["extract"] }),
      ]),
      PLUGINS,
    );
    const issue = issues.find((i) => i.rule === "type_mismatch");
    expect(issue?.stepId).toBe("transcribe");
    expect(issue?.message).toContain("documents");
    expect(issue?.message).toContain("bytes");
  });

  it("flags bytes fed into a plugin that only accepts documents", () => {
    const issues = validateDraft(
      draft([
        step("audio", "video_audio_extractor"),
        step("enrich", "llm_enricher", { depends_on: ["audio"] }),
      ]),
      PLUGINS,
    );
    expect(rules(issues)).toContain("type_mismatch");
  });

  it("accepts bytes flowing into the chunker, which declares bytes", () => {
    const issues = validateDraft(
      draft([
        step("audio", "video_audio_extractor"),
        step("chunk", "chunker", { depends_on: ["audio"] }),
      ]),
      PLUGINS,
    );
    expect(rules(issues)).not.toContain("type_mismatch");
  });

  it("flags a step reading the terminal output of the indexer", () => {
    const issues = validateDraft(
      draft([
        step("extract", "pdf_extractor"),
        step("index", "meili_indexer", { depends_on: ["extract"] }),
        step("again", "llm_enricher", { depends_on: ["index"] }),
      ]),
      PLUGINS,
    );
    const issue = issues.find((i) => i.rule === "type_mismatch");
    expect(issue?.stepId).toBe("again");
    expect(issue?.message).toContain("empty");
  });

  it("requires `many` when a step merges several dependencies", () => {
    const issues = validateDraft(
      draft([
        step("a", "pdf_extractor"),
        step("b", "chunker", { depends_on: ["a"] }),
        step("c", "audio_transcriber", { depends_on: ["a", "b"] }),
      ]),
      PLUGINS,
    );
    expect(issues.find((i) => i.rule === "type_mismatch")?.message).toContain("many");
  });

  it("never type-checks a root step, whose input comes from the request", () => {
    expect(rules(validateDraft(draft([step("a", "llm_enricher")]), PLUGINS))).not.toContain(
      "type_mismatch",
    );
  });
});

describe("helpers", () => {
  it("maps every output kind to the input kind the next step sees", () => {
    expect(outputToInput("bytes")).toBe("bytes");
    expect(outputToInput("ref")).toBe("ref");
    expect(outputToInput("documents")).toBe("documents");
    expect(outputToInput("many")).toBe("many");
    expect(outputToInput("indexed")).toBe("empty");
    expect(outputToInput("empty")).toBe("empty");
  });

  it("lets a bytes plugin read a ref, which the worker resolves first", () => {
    const bytesOnly = PLUGINS[0];
    expect(acceptsInput(bytesOnly, "ref")).toBe(true);
    expect(acceptsInput(bytesOnly, "documents")).toBe(false);
  });

  it("groups issues by step, pipeline-level ones under an empty key", () => {
    const grouped = issuesByStep(validateDraft(draft([step("a", "nope")], "bad uid!"), PLUGINS));
    expect(grouped.get("")?.[0].rule).toBe("invalid_uid");
    expect(grouped.get("a")?.[0].rule).toBe("unknown_plugin");
  });
});
