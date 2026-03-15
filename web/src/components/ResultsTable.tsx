import { useRef, useState, useCallback, useEffect, useLayoutEffect } from "preact/hooks";
import type { QueryResult, EventsResult, AggregateResult } from "../api/client";
import styles from "./ResultsTable.module.css";

interface ResultsTableProps {
  result: QueryResult | null;
  onRowClick: (row: Record<string, unknown>) => void;
  selectedRow: Record<string, unknown> | null;
}

const ROW_HEIGHT = 28;
const OVERSCAN = 5;

/** Column width heuristics: _time is narrower, _raw is wider */
function columnWidth(name: string): number {
  if (name === "_time") return 140;
  if (name === "_raw" || name === "message") return 500;
  return 180;
}

/** Format an ISO timestamp to HH:mm:ss.SSS */
function formatTime(value: unknown): string {
  if (typeof value !== "string") return String(value ?? "");
  try {
    const d = new Date(value);
    if (isNaN(d.getTime())) return String(value);
    const h = String(d.getHours()).padStart(2, "0");
    const m = String(d.getMinutes()).padStart(2, "0");
    const s = String(d.getSeconds()).padStart(2, "0");
    const ms = String(d.getMilliseconds()).padStart(3, "0");
    return `${h}:${m}:${s}.${ms}`;
  } catch {
    return String(value);
  }
}

/** Truncate a string to maxLen characters */
function truncate(value: unknown, maxLen = 200): string {
  const str = value == null ? "" : String(value);
  return str.length > maxLen ? str.slice(0, maxLen) + "\u2026" : str;
}

/** Derive columns from events: _time first, then _raw, _source, source, then alphabetical */
function deriveColumnsFromEvents(events: Record<string, unknown>[]): string[] {
  const keySet = new Set<string>();
  const limit = Math.min(events.length, 100);
  for (let i = 0; i < limit; i++) {
    for (const key of Object.keys(events[i])) {
      keySet.add(key);
    }
  }

  const priority = ["_time", "_raw", "_source", "source"];
  const ordered: string[] = [];
  for (const p of priority) {
    if (keySet.has(p)) {
      ordered.push(p);
      keySet.delete(p);
    }
  }

  const rest = Array.from(keySet).sort();
  return ordered.concat(rest);
}

/** Normalize result data into a uniform shape for rendering */
function useTableData(result: QueryResult | null): {
  columns: string[];
  rowCount: number;
  getRow: (index: number) => Record<string, unknown>;
} {
  if (!result) {
    return { columns: [], rowCount: 0, getRow: () => ({}) };
  }

  if (result.type === "events") {
    const evts = (result as EventsResult).events;
    const columns = deriveColumnsFromEvents(evts);
    return {
      columns,
      rowCount: evts.length,
      getRow: (i: number) => evts[i] ?? {},
    };
  }

  // aggregate or timechart
  const agg = result as AggregateResult;
  const columns = agg.columns;
  return {
    columns,
    rowCount: agg.rows.length,
    getRow: (i: number) => {
      const row: Record<string, unknown> = {};
      const data = agg.rows[i];
      if (data) {
        for (let c = 0; c < columns.length; c++) {
          row[columns[c]] = data[c];
        }
      }
      return row;
    },
  };
}

export function ResultsTable({ result, onRowClick, selectedRow }: ResultsTableProps) {
  const viewportRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(600);

  const { columns, rowCount, getRow } = useTableData(result);

  // Track viewport height — useLayoutEffect ensures measurement before paint
  useLayoutEffect(() => {
    const el = viewportRef.current;
    if (!el) return;

    const obs = new ResizeObserver((entries) => {
      for (const entry of entries) {
        setViewportHeight(entry.contentRect.height);
      }
    });
    obs.observe(el);
    setViewportHeight(el.clientHeight || 600);

    return () => obs.disconnect();
  }, []);

  const handleScroll = useCallback(() => {
    if (viewportRef.current) {
      setScrollTop(viewportRef.current.scrollTop);
    }
  }, []);

  if (!result) {
    return <div class={styles.empty}>No results</div>;
  }

  if (rowCount === 0) {
    return <div class={styles.empty}>Query returned no results</div>;
  }

  // Virtual scroll calculations
  const totalHeight = rowCount * ROW_HEIGHT;
  const startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const visibleCount = Math.ceil(viewportHeight / ROW_HEIGHT) + OVERSCAN * 2;
  const endIndex = Math.min(rowCount, startIndex + visibleCount);

  const totalWidth = columns.reduce((sum, col) => sum + columnWidth(col), 0);

  const visibleRows = [];
  for (let i = startIndex; i < endIndex; i++) {
    const row = getRow(i);
    const isSelected = selectedRow === row || (selectedRow !== null && selectedRow._time === row._time && selectedRow._raw === row._raw);
    const yOffset = i * ROW_HEIGHT;

    visibleRows.push(
      <div
        key={i}
        class={`${styles.row} ${isSelected ? styles.rowSelected : ""}`}
        style={{ transform: `translateY(${yOffset}px)` }}
        onClick={() => onRowClick(row)}
        role="row"
        aria-rowindex={i + 1}
      >
        {columns.map((col) => {
          const raw = row[col];
          const isTime = col === "_time";
          const display = isTime ? formatTime(raw) : truncate(raw);
          const fullValue = raw == null ? "" : String(raw);

          return (
            <div
              key={col}
              class={`${styles.cell} ${isTime ? styles.cellTime : ""}`}
              style={{ width: columnWidth(col) }}
              title={fullValue}
              role="cell"
            >
              {display}
            </div>
          );
        })}
      </div>
    );
  }

  return (
    <div class={styles.wrapper} role="table" aria-label="Query results">
      <div class={styles.headerRow} role="row" style={{ minWidth: totalWidth }}>
        {columns.map((col) => (
          <div
            key={col}
            class={styles.headerCell}
            style={{ width: columnWidth(col) }}
            role="columnheader"
          >
            {col}
          </div>
        ))}
      </div>
      <div
        ref={viewportRef}
        class={styles.viewport}
        onScroll={handleScroll}
      >
        <div
          class={styles.scrollContent}
          style={{ height: totalHeight, minWidth: totalWidth }}
        >
          {visibleRows}
        </div>
      </div>
    </div>
  );
}
