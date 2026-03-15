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
    const pos = match ? parseInt(match[1], 10) : 0;

    // Clamp to valid range
    const from = Math.min(pos, query.length);
    const to = Math.min(from + 1, query.length);

    diagnostics.push({
      from,
      to: Math.max(to, from + 1), // at least 1 char wide
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
