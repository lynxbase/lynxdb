import { setDiagnostics, type Diagnostic } from "@codemirror/lint";
import type { EditorView } from "@codemirror/view";
import type { ExplainResult } from "../api/client";

const POSITION_REGEX = /at position (\d+)/;

/**
 * Extract CodeMirror Diagnostic objects from a backend explain result.
 * Maps error messages with "at position N" to editor character offsets.
 */
export function extractDiagnostics(
  query: string,
  explainResult: ExplainResult,
): Diagnostic[] {
  if (explainResult.is_valid || !explainResult.errors?.length) return [];

  const diagnostics: Diagnostic[] = [];
  for (const err of explainResult.errors) {
    const match = err.message.match(POSITION_REGEX);
    const pos = match ? parseInt(match[1] ?? "0", 10) : 0;

    // Keep the marker inside the document. When the reported position is at
    // or past the end, anchor it on the last character so CodeMirror never
    // receives an out-of-range diagnostic.
    const len = query.length;
    let from = Math.min(Math.max(pos, 0), len);
    let to = from + 1;
    if (from >= len) {
      from = Math.max(0, len - 1);
      to = len;
    }
    to = Math.min(to, len);

    diagnostics.push({
      from,
      to,
      severity: "error",
      message: err.suggestion || err.message,
    });
  }

  return diagnostics;
}

/**
 * Dispatch diagnostics to the editor view from an explain result.
 */
export function dispatchDiagnostics(
  view: EditorView,
  query: string,
  explainResult: ExplainResult,
): void {
  const diagnostics = extractDiagnostics(query, explainResult);
  view.dispatch(setDiagnostics(view.state, diagnostics));
}

/**
 * Clear all diagnostics from the editor.
 */
export function clearEditorDiagnostics(view: EditorView): void {
  view.dispatch(setDiagnostics(view.state, []));
}
