/**
 * NDJSON control-line filtering for export-all streaming responses.
 *
 * The /api/v1/query/stream endpoint emits NDJSON where some lines are
 * control envelopes ({@link __meta}, {@link __error}) rather than data rows.
 * These must be stripped before writing to CSV/JSON export files.
 */

/**
 * Parse NDJSON text and return only data rows, dropping any line whose
 * parsed object has a `__meta` or `__error` property (control envelopes).
 *
 * Malformed JSON lines are silently skipped (same as streaming.processLine).
 */
export function filterNdjsonDataRows(
  ndjsonText: string,
): Record<string, unknown>[] {
  const rows: Record<string, unknown>[] = [];
  const lines = ndjsonText.trim().split("\n");
  for (const line of lines) {
    if (!line.trim()) continue;
    try {
      const parsed = JSON.parse(line) as Record<string, unknown>;
      if (parsed.__meta || parsed.__error) continue;
      rows.push(parsed);
    } catch {
      // Skip malformed JSON lines gracefully.
    }
  }
  return rows;
}
