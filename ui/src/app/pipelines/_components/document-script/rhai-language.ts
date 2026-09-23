import { StreamLanguage, type StreamParser } from "@codemirror/language";

const KEYWORDS = new Set([
  "let", "const", "if", "else", "switch", "while", "loop", "for", "in", "do", "until",
  "break", "continue", "return", "throw", "try", "catch", "fn", "private", "true", "false", "this",
]);

interface RhaiState {
  inBlockComment: boolean;
}

/** A small stream tokenizer for rhai: enough for highlighting, not a parser. */
export const rhaiParser: StreamParser<RhaiState> = {
  name: "rhai",
  startState: () => ({ inBlockComment: false }),
  token(stream, state) {
    if (state.inBlockComment) {
      if (stream.skipTo("*/")) {
        stream.pos += 2;
        state.inBlockComment = false;
      } else {
        stream.skipToEnd();
      }
      return "comment";
    }
    if (stream.eatSpace()) return null;
    if (stream.match("//")) {
      stream.skipToEnd();
      return "comment";
    }
    if (stream.match("/*")) {
      state.inBlockComment = true;
      return "comment";
    }
    if (stream.match(/^"(?:[^"\\]|\\.)*"?/)) return "string";
    if (stream.match(/^`[^`]*`?/)) return "string";
    if (stream.match(/^\d[\d_]*(?:\.\d[\d_]*)?(?:[eE][+-]?\d+)?/)) return "number";
    if (stream.match(/^[A-Za-z_][A-Za-z0-9_]*/)) {
      const word = stream.current();
      if (KEYWORDS.has(word)) return "keyword";
      if (word === "doc") return "variableName.special";
      return "variableName";
    }
    stream.next();
    return null;
  },
};

export const rhai = StreamLanguage.define(rhaiParser);
