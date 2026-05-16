/**
 * Utilities for manipulating `| sort` clauses in SPL2 query text.
 *
 * Used by ResultsTable to implement click-to-sort on column headers.
 * Clicking a header toggles through: asc -> desc -> remove.
 */

/**
 * Regex to match a `| sort [+-]?field` clause.
 *
 * Matches patterns like:
 *   - ` | sort field`
 *   - ` | sort -field`
 *   - ` | sort +field`
 *   - ` | sort field.name`
 *
 * Anchored to either end-of-string or the next pipe command to avoid
 * matching inside string literals or unrelated parts of the query.
 */
const SORT_CLAUSE_RE = /\s*\|\s*sort\s+[+-]?[\w.]+(?:\s*$|\s*(?=\|))/i;

/**
 * Regex to extract the field name and direction from a sort clause.
 * Captures: optional sign prefix and the field name.
 */
const SORT_EXTRACT_RE = /\|\s*sort\s+([+-]?)([\w.]+)/i;

/**
 * Parse the current sort state from a query string.
 *
 * @returns The sorted field and direction, or null if no sort clause exists.
 */
export function parseSortFromQuery(
  query: string,
): { field: string; direction: "asc" | "desc" } | null {
  const match = query.match(SORT_EXTRACT_RE);
  if (!match) return null;

  const sign = match[1] ?? "";
  const field = match[2] ?? "";
  const direction: "asc" | "desc" = sign === "-" ? "desc" : "asc";

  return { field, direction };
}

/**
 * Update the sort clause in a query string.
 *
 * - Removes any existing `| sort ...` clause.
 * - If direction is null, returns the query without a sort clause (toggle off).
 * - If direction is "asc", appends ` | sort field`.
 * - If direction is "desc", appends ` | sort -field`.
 *
 * @param query     Current SPL2 query text
 * @param field     Column name to sort by
 * @param direction Sort direction, or null to remove sort
 * @returns         Modified query string
 */
export function updateSortInQuery(
  query: string,
  field: string,
  direction: "asc" | "desc" | null,
): string {
  // Remove existing sort clause
  const withoutSort = query.replace(SORT_CLAUSE_RE, "").trim();

  if (direction === null) {
    return withoutSort;
  }

  const prefix = direction === "desc" ? "-" : "";
  return `${withoutSort} | sort ${prefix}${field}`;
}
