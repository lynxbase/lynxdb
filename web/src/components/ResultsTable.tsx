import { useRef, useState, useCallback, useLayoutEffect, useEffect, useMemo } from "preact/hooks";
import { ChevronRight, ChevronDown } from "lucide-preact";
import type { QueryResult, EventsResult, AggregateResult } from "../api/client";
import { updateSortInQuery, parseSortFromQuery } from "../utils/sortQuery";
import { EventDetailInline } from "./EventDetail";
import styles from "./ResultsTable.module.css";

// Types

interface ResultsTableProps {
  result: QueryResult | null;
  onSort?: (newQuery: string) => void;
  currentQuery?: string;
  isAggregation?: boolean;
  onFilter?: (field: string, value: string, exclude: boolean) => void;
}

// Constants

const ROW_HEIGHT = 28;
const OVERSCAN = 5;
const MIN_COL_WIDTH = 60;
const MAX_ACCORDION_HEIGHT = 400;

function getAccordionHeight(event: Record<string, unknown>): number {
  const fieldCount = Object.keys(event).length;
  return Math.min(fieldCount * 28 + 52, MAX_ACCORDION_HEIGHT);
}

/** Compute default pixel widths by sampling row data */
function computeDefaultWidths(
  columns: string[],
  getRow: (i: number) => Record<string, unknown>,
  rowCount: number,
): Record<string, number> {
  const widths: Record<string, number> = {};
  const sampleSize = Math.min(rowCount, 50);

  for (const col of columns) {
    if (col === "_time") {
      widths[col] = 140;
      continue;
    }

    let maxLen = col.length;
    for (let i = 0; i < sampleSize; i++) {
      const val = getRow(i)[col];
      const str = val == null ? "" : String(val);
      if (str.length > maxLen) maxLen = str.length;
    }

    let width = Math.max(80, Math.min(600, maxLen * 7.8 + 24));
    if (col === "_raw" || col === "message") {
      width = Math.max(200, Math.min(600, width));
    }
    widths[col] = Math.round(width);
  }

  return widths;
}

// Helpers

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

function isNumeric(value: unknown): boolean {
  return typeof value === "number" || (typeof value === "string" && value !== "" && !isNaN(Number(value)));
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
  isAgg: boolean;
} {
  if (!result) {
    return { columns: [], rowCount: 0, getRow: () => ({}), isAgg: false };
  }

  if (result.type === "events") {
    const evts = (result as EventsResult).events;
    const columns = deriveColumnsFromEvents(evts);
    return {
      columns,
      rowCount: evts.length,
      getRow: (i: number) => evts[i] ?? {},
      isAgg: false,
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
    isAgg: true,
  };
}

// Component

export function ResultsTable({
  result,
  onSort,
  currentQuery,
  isAggregation,
  onFilter,
}: ResultsTableProps) {
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(600);
  const [columnWidths, setColumnWidths] = useState<Record<string, number>>({});
  const [resizingCol, setResizingCol] = useState<string | null>(null);
  const [expandedRowIndex, setExpandedRowIndex] = useState<number | null>(null);

  const { columns, rowCount, getRow, isAgg } = useTableData(result);

  const defaultWidths = useMemo(
    () => computeDefaultWidths(columns, getRow, rowCount),
    [result],
  );

  const effectiveIsAgg = isAggregation ?? isAgg;

  const currentSort = currentQuery ? parseSortFromQuery(currentQuery) : null;

  // Close accordion on Escape
  useEffect(() => {
    if (expandedRowIndex === null) return;
    function onKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        setExpandedRowIndex(null);
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, [expandedRowIndex]);

  // Reset expanded row when result changes
  useEffect(() => {
    setExpandedRowIndex(null);
  }, [result]);

  // Track viewport height via ResizeObserver
  useLayoutEffect(() => {
    const el = scrollContainerRef.current;
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
    if (scrollContainerRef.current) {
      setScrollTop(scrollContainerRef.current.scrollTop);
    }
  }, []);

  // ---- Column resize handlers ----

  const handleResizeStart = useCallback(
    (e: PointerEvent, colName: string) => {
      e.preventDefault();
      e.stopPropagation();

      const target = e.currentTarget as HTMLElement;
      target.setPointerCapture(e.pointerId);

      const startX = e.clientX;
      // Get the actual rendered width of the column header cell
      const headerCell = target.parentElement;
      const startWidth = columnWidths[colName] || headerCell?.offsetWidth || 180;

      setResizingCol(colName);

      const onMove = (me: PointerEvent) => {
        const delta = me.clientX - startX;
        const newWidth = Math.max(MIN_COL_WIDTH, startWidth + delta);
        setColumnWidths((prev) => ({ ...prev, [colName]: newWidth }));
      };

      const onUp = () => {
        setResizingCol(null);
        target.removeEventListener("pointermove", onMove as EventListener);
        target.removeEventListener("pointerup", onUp);
      };

      target.addEventListener("pointermove", onMove as EventListener);
      target.addEventListener("pointerup", onUp);
    },
    [columnWidths],
  );

  const handleResizeDblClick = useCallback(
    (e: MouseEvent, colName: string) => {
      e.preventDefault();
      e.stopPropagation();
      // Remove stored width to reset to auto-fit (max-content)
      setColumnWidths((prev) => {
        const next = { ...prev };
        delete next[colName];
        return next;
      });
    },
    [],
  );

  // ---- Sort handler ----

  const handleHeaderClick = useCallback(
    (colName: string) => {
      if (!onSort || !currentQuery) return;

      let newDirection: "asc" | "desc" | null;
      if (!currentSort || currentSort.field !== colName) {
        newDirection = "asc";
      } else if (currentSort.direction === "asc") {
        newDirection = "desc";
      } else {
        newDirection = null;
      }

      const newQuery = updateSortInQuery(currentQuery, colName, newDirection);
      onSort(newQuery);
    },
    [onSort, currentQuery, currentSort],
  );

  // ---- Row expand handler ----

  const handleRowToggle = useCallback((index: number) => {
    setExpandedRowIndex((prev) => (prev === index ? null : index));
  }, []);

  // ---- Early returns ----

  if (!result) {
    return <div class={styles.empty}>No results</div>;
  }

  if (rowCount === 0) {
    return <div class={styles.empty}>Query returned no results</div>;
  }

  // ---- Grid template ----

  // Build grid-template-columns: gutter + data columns (fixed pixel widths for alignment)
  const dataColTemplate = columns
    .map((col) => {
      const stored = columnWidths[col];
      if (stored != null) return `${stored}px`;
      return `${defaultWidths[col] ?? 120}px`;
    })
    .join(" ");

  const gridTemplate = `24px ${dataColTemplate}`;
  const gridStyle = { gridTemplateColumns: gridTemplate };

  // ---- Determine if _time is sticky ----
  const hasTimeCol = !effectiveIsAgg && columns.includes("_time");

  // ---- Virtual scroll calculations (with accordion offset) ----
  const accordionHeight = expandedRowIndex !== null
    ? getAccordionHeight(getRow(expandedRowIndex))
    : 0;
  const totalHeight = rowCount * ROW_HEIGHT + accordionHeight;

  let startIndex: number;
  let endIndex: number;

  startIndex = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const visibleCount = Math.ceil(viewportHeight / ROW_HEIGHT) + OVERSCAN * 2;
  endIndex = Math.min(rowCount, startIndex + visibleCount);

  // Ensure expanded row is in the visible range
  if (expandedRowIndex !== null) {
    if (expandedRowIndex < startIndex) startIndex = expandedRowIndex;
    if (expandedRowIndex >= endIndex) endIndex = expandedRowIndex + 1;
    // Also extend the range to account for accordion pushing rows down
    const extraRows = Math.ceil(accordionHeight / ROW_HEIGHT);
    endIndex = Math.min(rowCount, endIndex + extraRows);
  }

  // ---- Render rows ----

  const visibleRows = [];
  for (let i = startIndex; i < endIndex; i++) {
    const row = getRow(i);
    const isExpanded = expandedRowIndex === i;

    // Calculate Y offset: rows after the expanded row are pushed down by accordion height
    let yOffset = i * ROW_HEIGHT;
    if (expandedRowIndex !== null && i > expandedRowIndex) {
      yOffset += accordionHeight;
    }

    const rowClasses = [
      styles.row,
      isExpanded ? styles.rowSelected : "",
    ]
      .filter(Boolean)
      .join(" ");

    const rowStyle = { ...gridStyle, transform: `translateY(${yOffset}px)` };

    visibleRows.push(
      <div
        key={i}
        class={rowClasses}
        style={rowStyle}
        role="row"
        aria-rowindex={i + 1}
        onClick={() => handleRowToggle(i)}
      >
        <div
          class={`${styles.gutter} ${isExpanded ? styles.gutterExpanded : ""}`}
          onClick={(e: MouseEvent) => { e.stopPropagation(); handleRowToggle(i); }}
          title={isExpanded ? "Collapse event" : "Expand event"}
        >
          {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        </div>
        {columns.map((col) => {
          const raw = row[col];
          const isTime = col === "_time";
          const display = isTime ? formatTime(raw) : truncate(raw);
          const fullValue = raw == null ? "" : String(raw);

          const cellClasses = [
            styles.cell,
            isTime ? styles.cellTime : "",
            hasTimeCol && isTime ? styles.cellSticky : "",
            effectiveIsAgg && isNumeric(raw) ? styles.cellNumber : "",
          ]
            .filter(Boolean)
            .join(" ");

          return (
            <div
              key={col}
              class={cellClasses}
              title={fullValue}
              role="cell"
            >
              {display}
            </div>
          );
        })}
      </div>,
    );

    // Render accordion below expanded row
    if (isExpanded) {
      const accordionY = (i + 1) * ROW_HEIGHT;
      visibleRows.push(
        <div
          key={`accordion-${i}`}
          class={styles.accordionRow}
          style={{ transform: `translateY(${accordionY}px)`, height: accordionHeight }}
        >
          <EventDetailInline event={row} onFilter={onFilter} />
        </div>,
      );
    }
  }

  // ---- Render ----

  return (
    <div class={styles.wrapper} role="table" aria-label="Query results">
      <div class={styles.scrollContainer} ref={scrollContainerRef} onScroll={handleScroll}>
        {/* Header row inside scroll container for sticky top:0 */}
        <div class={styles.headerRow} style={gridStyle} role="row">
          <div class={styles.gutterHeader} />
          {columns.map((col) => {
            const isSorted = currentSort?.field === col;
            const cellClasses = [
              styles.headerCell,
              hasTimeCol && col === "_time" ? styles.headerCellSticky : "",
            ]
              .filter(Boolean)
              .join(" ");

            return (
              <div
                key={col}
                class={cellClasses}
                role="columnheader"
                onClick={() => handleHeaderClick(col)}
              >
                <span>{col}</span>
                {isSorted && (
                  <span class={styles.sortIndicator}>
                    {currentSort!.direction === "asc" ? "\u25B2" : "\u25BC"}
                  </span>
                )}
                <div
                  class={`${styles.resizeHandle} ${resizingCol === col ? styles.resizeActive : ""}`}
                  onPointerDown={(e: PointerEvent) => handleResizeStart(e, col)}
                  onDblClick={(e: MouseEvent) => handleResizeDblClick(e, col)}
                />
              </div>
            );
          })}
        </div>

        {/* Scroll content area */}
        <div
          class={styles.scrollContent}
          style={{ height: totalHeight }}
        >
          {visibleRows}
        </div>
      </div>
    </div>
  );
}
