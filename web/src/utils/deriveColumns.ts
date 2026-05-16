/**
 * Shared column-derivation utility.
 *
 * Extracts the ordered column list from a QueryResult. The logic is identical
 * to the original per-component implementations:
 *   priority order: _time, _raw, _source, source — then alphabetical.
 *
 * For aggregate/timechart results the columns array is returned as-is.
 *
 * PERF-03: For event results, sampling is capped and skipped entirely when
 * the row count is small enough that full iteration is cheaper than the
 * bookkeeping overhead.
 */

import type {
  QueryResult,
  EventsResult,
  AggregateResult,
} from "../api/client";

const PRIORITY = ["_time", "_raw", "_source", "source"] as const;

/**
 * Small-result threshold: if the event count is at or below this number we
 * scan every row instead of sampling. This avoids missing fields that only
 * appear outside the sample window.
 */
const SMALL_RESULT_THRESHOLD = 50;

/**
 * Maximum number of rows to sample for column discovery on large event
 * results. Keeps the cost bounded for 100K+ row payloads.
 */
const MAX_SAMPLE_ROWS = 100;

/** Derive ordered columns from an array of row objects. */
export function deriveColumnsFromRows(
  rows: Record<string, unknown>[],
): string[] {
  const keySet = new Set<string>();
  const limit =
    rows.length <= SMALL_RESULT_THRESHOLD
      ? rows.length
      : Math.min(rows.length, MAX_SAMPLE_ROWS);
  for (let i = 0; i < limit; i++) {
    const row = rows[i];
    if (row) {
      for (const key of Object.keys(row)) {
        keySet.add(key);
      }
    }
  }
  return applyPriority(keySet);
}

/** Derive ordered columns from an EventsResult's events array. */
export function deriveColumnsFromEvents(
  events: Record<string, unknown>[],
): string[] {
  return deriveColumnsFromRows(events);
}

/** Derive columns from any QueryResult. */
export function deriveColumns(result: QueryResult): string[] {
  if (result.type === "events") {
    return deriveColumnsFromEvents((result as EventsResult).events);
  }
  // Aggregate / timechart results carry their own column list.
  return (result as AggregateResult).columns;
}

/**
 * WeakMap-based memo for deriveColumns keyed on the result object reference.
 * Avoids re-deriving on every render when the result hasn't changed.
 */
const columnsCache = new WeakMap<QueryResult, string[]>();

export function deriveColumnsMemo(result: QueryResult): string[] {
  const cached = columnsCache.get(result);
  if (cached) return cached;
  const cols = deriveColumns(result);
  columnsCache.set(result, cols);
  return cols;
}

// ---------------------------------------------------------------------------
// Internal
// ---------------------------------------------------------------------------

function applyPriority(keySet: Set<string>): string[] {
  const ordered: string[] = [];
  for (const p of PRIORITY) {
    if (keySet.has(p)) {
      ordered.push(p);
      keySet.delete(p);
    }
  }
  const rest = Array.from(keySet).sort();
  return ordered.concat(rest);
}
