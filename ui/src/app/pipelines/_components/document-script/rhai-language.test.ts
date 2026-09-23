import { StringStream } from "@codemirror/language";
import { describe, expect, it } from "vitest";

import { rhaiParser } from "./rhai-language";

function tokens(line: string): [string, string | null][] {
  const state = rhaiParser.startState!(2);
  const stream = new StringStream(line, 4, 2);
  const out: [string, string | null][] = [];
  while (!stream.eol()) {
    const style = rhaiParser.token(stream, state);
    const text = stream.current();
    if (text.trim().length > 0) out.push([text, style]);
    stream.start = stream.pos;
  }
  return out;
}

describe("rhaiParser", () => {
  it("highlights keywords, doc, strings, numbers and comments", () => {
    expect(tokens('let x = doc.fields["a\\"b"] + 12.5; // note')).toEqual([
      ["let", "keyword"],
      ["x", "variableName"],
      ["=", null],
      ["doc", "variableName.special"],
      [".", null],
      ["fields", "variableName"],
      ["[", null],
      ['"a\\"b"', "string"],
      ["]", null],
      ["+", null],
      ["12.5", "number"],
      [";", null],
      ["// note", "comment"],
    ]);
  });
});
