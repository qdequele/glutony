import { describe, expect, it } from "vitest";

import {
  DEFAULT_RETRY,
  draftToPipeline,
  emptyDraft,
  normalizeDraft,
  pipelineToDraft,
  type PipelineDraft,
} from "./draft";
import { draftToYaml, yamlToDraft } from "./yaml";

const FULL_DRAFT: PipelineDraft = {
  uid: "my-pdf-with-enrichment",
  name: "PDF with LLM enrichment",
  description: "Extract, chunk, enrich, index.",
  trigger: {
    content_types: ["application/pdf"],
    filename_pattern: "contract_*.pdf",
    index_pattern: "contracts",
  },
  steps: [
    {
      id: "extract",
      plugin: "pdf_extractor",
      depends_on: [],
      config: { per_page: true },
      timeout_secs: 120,
      retry: { max_attempts: 3, backoff: "exponential", initial_interval_secs: 1 },
    },
    { id: "chunk", plugin: "chunker", depends_on: ["extract"], config: { chunk_size: 512 } },
    {
      id: "enrich",
      plugin: "llm_enricher",
      depends_on: ["chunk"],
      fan_out: "$.documents",
      config: { model: "gpt-4o-mini", temperature: 0 },
    },
    { id: "index", plugin: "meili_indexer", depends_on: ["enrich"], config: {} },
  ],
};

describe("draft → YAML → draft", () => {
  it("round-trips a full pipeline unchanged", () => {
    const result = yamlToDraft(draftToYaml(FULL_DRAFT));
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.draft).toEqual(FULL_DRAFT);
  });

  it("round-trips a minimal pipeline unchanged", () => {
    const minimal: PipelineDraft = {
      ...emptyDraft(),
      uid: "tiny",
      steps: [{ id: "index", plugin: "meili_indexer", depends_on: [], config: {} }],
    };
    const result = yamlToDraft(draftToYaml(minimal));
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.draft).toEqual(minimal);
  });

  it("round-trips nested config values", () => {
    const nested: PipelineDraft = {
      ...emptyDraft(),
      uid: "nested",
      steps: [
        {
          id: "csv",
          plugin: "csv_extractor",
          depends_on: [],
          config: {
            content_columns: ["title", "body"],
            delimiter: null,
            options: { trim: true, depth: 2 },
          },
        },
      ],
    };
    const result = yamlToDraft(draftToYaml(nested));
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.draft.steps[0].config).toEqual(nested.steps[0].config);
  });

  it("omits empty optionals instead of writing nulls", () => {
    const yaml = draftToYaml({
      ...emptyDraft(),
      uid: "bare",
      steps: [{ id: "a", plugin: "chunker", depends_on: [], config: {} }],
    });
    expect(yaml).not.toContain("trigger");
    expect(yaml).not.toContain("depends_on");
    expect(yaml).not.toContain("config");
    expect(yaml).not.toContain("null");
  });
});

describe("yamlToDraft", () => {
  it("accepts the pipeline from the README", () => {
    const result = yamlToDraft(`
uid: my-pdf-with-enrichment
trigger:
  content_types: [application/pdf]
  filename_pattern: "contract_*.pdf"
steps:
  - id: extract
    plugin: pdf_extractor
  - id: chunk
    plugin: chunker
    config: { strategy: sentence, chunk_size: 512, overlap: 64 }
  - id: enrich
    plugin: llm_enricher
    fan_out: "$.documents"
    config: { model: gpt-4o-mini }
  - id: index
    plugin: meili_indexer
`);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.draft.steps.map((s) => s.id)).toEqual(["extract", "chunk", "enrich", "index"]);
    expect(result.draft.trigger.content_types).toEqual(["application/pdf"]);
    expect(result.draft.trigger.index_pattern).toBe("");
  });

  it("accepts JSON, which is valid YAML", () => {
    const result = yamlToDraft('{"uid":"j","steps":[{"id":"a","plugin":"chunker"}]}');
    expect(result.ok).toBe(true);
  });

  it("fills partial retry blocks with the server defaults", () => {
    const result = yamlToDraft(`
uid: r
steps:
  - id: a
    plugin: chunker
    retry:
      max_attempts: 5
`);
    expect(result.ok).toBe(true);
    if (!result.ok) return;
    expect(result.draft.steps[0].retry).toEqual({
      ...DEFAULT_RETRY,
      max_attempts: 5,
    });
  });

  it("reports a syntax error instead of throwing", () => {
    const result = yamlToDraft("uid: [unclosed");
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error.length).toBeGreaterThan(0);
  });

  it("reports a shape error with the offending path", () => {
    const result = yamlToDraft("uid: ok\nsteps:\n  - plugin: chunker\n");
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("steps.0.id");
  });

  it("rejects a scalar document", () => {
    expect(yamlToDraft("just a string").ok).toBe(false);
  });

  it("rejects an empty document", () => {
    expect(yamlToDraft("   ").ok).toBe(false);
  });

  it("rejects an unsupported fan_out rather than dropping it", () => {
    const result = yamlToDraft(`
uid: f
steps:
  - id: a
    plugin: chunker
  - id: b
    plugin: llm_enricher
    depends_on: [a]
    fan_out: "$.pages"
`);
    expect(result.ok).toBe(false);
    if (result.ok) return;
    expect(result.error).toContain("$.pages");
  });
});

describe("draft ⇄ PipelineDefinition", () => {
  it("round-trips through the wire shape", () => {
    expect(pipelineToDraft(draftToPipeline(FULL_DRAFT))).toEqual(FULL_DRAFT);
  });

  it("drops the empty optionals from the saved definition", () => {
    const saved = draftToPipeline({
      ...emptyDraft(),
      uid: "bare",
      steps: [{ id: "a", plugin: "chunker", depends_on: [], config: {} }],
    });
    expect(saved).toEqual({ uid: "bare", steps: [{ id: "a", plugin: "chunker" }] });
  });

  it("keeps server-owned fields out of the draft", () => {
    const draft = pipelineToDraft({
      uid: "builtin.pdf",
      steps: [{ id: "a", plugin: "pdf_extractor" }],
      builtin: true,
      version: 7,
      project_id: "tenant-1",
    });
    expect(Object.keys(draft).sort()).toEqual([
      "description",
      "name",
      "steps",
      "trigger",
      "uid",
    ]);
  });
});

describe("normalizeDraft", () => {
  it("materialises every optional key", () => {
    expect(normalizeDraft({ uid: "x" })).toEqual({
      uid: "x",
      name: "",
      description: "",
      trigger: { content_types: [], filename_pattern: "", index_pattern: "" },
      steps: [],
    });
  });
});
