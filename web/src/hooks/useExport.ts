/**
 * Hook that encapsulates CSV/JSON export logic for current-page and
 * "export all" (streaming) modes.
 *
 * BUG-07 fix: the "export all" path now filters out __meta and __error
 * control envelope lines from the NDJSON response before building the
 * export file, preventing control lines from becoming CSV/JSON rows.
 */

import { useCallback } from "react";
import { authHeaders } from "../api/auth";
import { useSearchStore } from "../stores/search";
import {
  generateCSV,
  generateJSON,
  downloadFile,
  generateFilename,
} from "../utils/export";
import { filterNdjsonDataRows } from "../utils/ndjsonFilter";
import {
  deriveColumns,
  deriveColumnsFromRows,
} from "../utils/deriveColumns";
import type {
  QueryResult,
  EventsResult,
  AggregateResult,
} from "../api/client";

/** Shorthand for imperative store access */
const ss = useSearchStore;

/** Get rows as Record<string, unknown>[] from a QueryResult */
function getResultRows(r: QueryResult): Record<string, unknown>[] {
  if (r.type === "events") return (r as EventsResult).events;
  const agg = r as AggregateResult;
  return agg.rows.map((data) => {
    const row: Record<string, unknown> = {};
    for (let c = 0; c < agg.columns.length; c++) {
      const colName = agg.columns[c];
      if (colName !== undefined) {
        row[colName] = data[c];
      }
    }
    return row;
  });
}

export { deriveColumns, getResultRows };

export function useExport() {
  const handleExport = useCallback(
    async (format: "csv" | "json", scope: "page" | "all") => {
      let rows: Record<string, unknown>[];
      let columns: string[];

      if (scope === "page") {
        // Use current result data
        const r = ss.getState().result;
        if (!r) return;
        columns = deriveColumns(r);
        rows = getResultRows(r);
      } else {
        // Fetch all results via streaming endpoint
        const state = ss.getState();
        try {
          const resp = await fetch("/api/v1/query/stream", {
            method: "POST",
            headers: { "Content-Type": "application/json", ...authHeaders() },
            body: JSON.stringify({
              q: state.query,
              from: state.from,
              to: state.to,
            }),
          });
          if (!resp.ok) {
            // Fallback to current page data
            const r = state.result;
            if (!r) return;
            columns = deriveColumns(r);
            rows = getResultRows(r);
          } else {
            const text = await resp.text();
            // BUG-07: Filter out __meta and __error control envelope lines
            rows = filterNdjsonDataRows(text);
            if (rows.length > 0) {
              columns = deriveColumnsFromRows(rows);
            } else {
              return;
            }
          }
        } catch {
          // On network error, fallback to current page
          const r = state.result;
          if (!r) return;
          columns = deriveColumns(r);
          rows = getResultRows(r);
        }
      }

      if (format === "csv") {
        const csv = generateCSV(columns, rows);
        downloadFile(csv, generateFilename("csv"), "text/csv");
      } else {
        const json = generateJSON(rows);
        downloadFile(json, generateFilename("json"), "application/json");
      }
    },
    [],
  );

  return { handleExport };
}
