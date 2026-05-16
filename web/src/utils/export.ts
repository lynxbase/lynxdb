/**
 * CSV/JSON export generation and file download utilities.
 */

/**
 * Generate a timestamped filename for export files.
 * Format: lynxdb-results-YYYYMMDD-HHmmss.ext
 */
export function generateFilename(ext: string): string {
  const now = new Date();
  const y = now.getFullYear();
  const mo = String(now.getMonth() + 1).padStart(2, "0");
  const d = String(now.getDate()).padStart(2, "0");
  const h = String(now.getHours()).padStart(2, "0");
  const mi = String(now.getMinutes()).padStart(2, "0");
  const s = String(now.getSeconds()).padStart(2, "0");
  return `lynxdb-results-${y}${mo}${d}-${h}${mi}${s}.${ext}`;
}

/**
 * Escape a CSV field value. Wraps in double-quotes if the value contains
 * commas, quotes, or newlines. Quotes within the value are escaped with "".
 */
function escapeCSVField(value: unknown): string {
  const str = value == null ? "" : String(value);
  if (
    str.includes(",") ||
    str.includes('"') ||
    str.includes("\n") ||
    str.includes("\r")
  ) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

/**
 * Generate CSV content from columns and rows.
 * Header row from column names, body rows from row data.
 */
export function generateCSV(
  columns: string[],
  rows: Record<string, unknown>[],
): string {
  const header = columns.map(escapeCSVField).join(",");
  const body = rows.map((row) =>
    columns.map((col) => escapeCSVField(row[col])).join(","),
  );
  return [header, ...body].join("\n");
}

/**
 * Generate pretty-printed JSON content from rows.
 */
export function generateJSON(rows: Record<string, unknown>[]): string {
  return JSON.stringify(rows, null, 2);
}

/**
 * Trigger a file download in the browser using a Blob and temporary anchor element.
 */
export function downloadFile(
  content: string,
  filename: string,
  mimeType: string,
): void {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  a.style.display = "none";
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}
