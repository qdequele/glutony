import { describe, expect, it } from "vitest";

import { singleStepPipeline } from "./generate-script";

describe("singleStepPipeline", () => {
  it("isolates one step so other steps cannot block the switch", () => {
    expect(singleStepPipeline("shape", { ops: [] })).toEqual({
      uid: "document-script-switch",
      steps: [{ id: "shape", plugin: "document_script", config: { ops: [] } }],
    });
  });
});
