export interface LintRange {
  from: number;
  to: number;
}

/** Map a 1-based line/column onto an offset range in `text`, clamped to the text. */
export function diagnosticRange(text: string, line: number | undefined, column: number | undefined): LintRange {
  const lines = text.split("\n");
  if (line !== undefined && line > lines.length) return { from: text.length, to: text.length };
  const index = Math.max(line ?? 1, 1) - 1;
  let start = 0;
  for (let i = 0; i < index; i++) start += lines[i].length + 1;
  const lineText = lines[index];
  if (column === undefined) return { from: start, to: start + lineText.length };
  const from = start + Math.min(Math.max(column - 1, 0), lineText.length);
  return { from, to: Math.min(from + 1, start + lineText.length) };
}
