import React, {
  useRef,
  useState,
  useCallback,
  useLayoutEffect,
  useEffect,
  useMemo,
} from "react";
import {
  useReactTable,
  getCoreRowModel,
  flexRender,
  createColumnHelper,
  type ColumnDef,
  type SortingState,
} from "@tanstack/react-table";
import { ChevronRight, ChevronDown } from "lucide-react";
import type { QueryResult, EventsResult, AggregateResult } from "../api/client";
import { rowKey } from "../utils/rowKey";
import { updateSortInQuery, parseSortFromQuery } from "../utils/sortQuery";
import { deriveColumnsFromEvents } from "../utils/deriveColumns";
import { EventDetailInline } from "./EventDetail";
import { cn } from "@/lib/utils";

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
  return str.length > maxLen ? str.slice(0, maxLen) + "…" : str;
}

function isNumeric(value: unknown): boolean {
  return (
    typeof value === "number" ||
    (typeof value === "string" && value !== "" && !isNaN(Number(value)))
  );
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
          const colName = columns[c];
          if (colName !== undefined) {
            row[colName] = data[c];
          }
        }
      }
      return row;
    },
    isAgg: true,
  };
}

// Column helper for @tanstack/react-table
const columnHelper = createColumnHelper<Record<string, unknown>>();

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
    // eslint-disable-next-line react-hooks/exhaustive-deps -- recompute when result changes
    [result],
  );

  const effectiveIsAgg = isAggregation ?? isAgg;

  const currentSort = currentQuery ? parseSortFromQuery(currentQuery) : null;

  // Build @tanstack/react-table sorting state from currentSort
  const sorting: SortingState = useMemo(() => {
    if (!currentSort) return [];
    return [{ id: currentSort.field, desc: currentSort.direction === "desc" }];
  }, [currentSort]);

  // Build column defs for @tanstack/react-table
  const columnDefs = useMemo((): ColumnDef<Record<string, unknown>, unknown>[] => {
    return columns.map((col) =>
      columnHelper.accessor((row) => row[col], {
        id: col,
        header: col,
        size: columnWidths[col] ?? defaultWidths[col] ?? 120,
        minSize: MIN_COL_WIDTH,
        maxSize: 1200,
      }),
    );
  }, [columns, columnWidths, defaultWidths]);

  // Build data array for table (array of row objects)
  const data = useMemo(() => {
    const rows: Record<string, unknown>[] = [];
    for (let i = 0; i < rowCount; i++) {
      rows.push(getRow(i));
    }
    return rows;
  }, [rowCount, getRow]);

  // Create the table instance
  const table = useReactTable({
    data,
    columns: columnDefs,
    state: { sorting },
    getCoreRowModel: getCoreRowModel(),
    manualSorting: true,
    enableColumnResizing: true,
    columnResizeMode: "onChange",
  });

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

  // PERF-06: rAF-throttle the scroll handler to coalesce scrollTop updates
  const rafRef = useRef(0);
  const handleScroll = useCallback(() => {
    if (rafRef.current) return; // rAF already scheduled
    rafRef.current = requestAnimationFrame(() => {
      rafRef.current = 0;
      if (scrollContainerRef.current) {
        setScrollTop(scrollContainerRef.current.scrollTop);
      }
    });
  }, []);

  // Cleanup pending rAF on unmount
  useEffect(() => {
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
  }, []);

  // ---- Column resize handlers ----

  const handleResizeStart = useCallback(
    (e: React.PointerEvent<HTMLDivElement>, colName: string) => {
      e.preventDefault();
      e.stopPropagation();

      const target = e.currentTarget;
      target.setPointerCapture(e.pointerId);

      const startX = e.clientX;
      // Get the actual rendered width of the column header cell
      const headerCell = target.parentElement;
      const startWidth =
        columnWidths[colName] || headerCell?.offsetWidth || 180;

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

  const handleResizeDblClick = useCallback((e: React.MouseEvent<HTMLDivElement>, colName: string) => {
    e.preventDefault();
    e.stopPropagation();
    // Remove stored width to reset to auto-fit (max-content)
    setColumnWidths((prev) => {
      const next = { ...prev };
      delete next[colName];
      return next;
    });
  }, []);

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
    return (
      <div className="flex flex-1 items-center justify-center text-muted-foreground font-sans text-sm p-12">
        No results
      </div>
    );
  }

  if (rowCount === 0) {
    return (
      <div className="flex flex-1 items-center justify-center text-muted-foreground font-sans text-sm p-12">
        Query returned no results
      </div>
    );
  }

  // ---- Grid template ----

  const headerGroups = table.getHeaderGroups();

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
  const accordionHeight =
    expandedRowIndex !== null
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

    const rowStyle = { ...gridStyle, transform: `translateY(${yOffset}px)` };

    visibleRows.push(
      <div
        key={rowKey(row)}
        className={cn(
          "grid absolute w-full h-7 items-center cursor-pointer transition-colors duration-75 motion-reduce:transition-none focus-visible:outline-2 focus-visible:outline-ring focus-visible:outline-offset-[-2px]",
          isExpanded
            ? "bg-accent hover:bg-accent/80"
            : "hover:bg-muted/50",
        )}
        style={rowStyle}
        role="row"
        tabIndex={0}
        aria-rowindex={i + 1}
        onClick={() => handleRowToggle(i)}
        onKeyDown={(e) => {
          if (e.key === "Enter" || e.key === " ") {
            e.preventDefault();
            handleRowToggle(i);
          }
        }}
      >
        <button
          type="button"
          className={cn(
            "flex items-center justify-center text-muted-foreground cursor-pointer transition-colors duration-100 motion-reduce:transition-none bg-transparent border-none p-0",
            isExpanded ? "text-primary" : "hover:text-primary",
          )}
          onClick={(e: React.MouseEvent) => {
            e.stopPropagation();
            handleRowToggle(i);
          }}
          tabIndex={-1}
          aria-label={isExpanded ? "Collapse event" : "Expand event"}
          title={isExpanded ? "Collapse event" : "Expand event"}
        >
          {isExpanded ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
        </button>
        {columns.map((col) => {
          const raw = row[col];
          const isTime = col === "_time";
          const display = isTime ? formatTime(raw) : truncate(raw);
          const fullValue = raw == null ? "" : String(raw);

          return (
            <div
              key={col}
              className={cn(
                "px-3 overflow-hidden text-ellipsis whitespace-nowrap text-foreground leading-7",
                isTime && "text-muted-foreground",
                hasTimeCol && isTime && "sticky left-6 z-[1] bg-background",
                isExpanded && hasTimeCol && isTime && "bg-accent",
                effectiveIsAgg && isNumeric(raw) && "text-right",
              )}
              title={fullValue}
              role="gridcell"
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
          key={`acc-${rowKey(row)}`}
          className="absolute w-full overflow-hidden"
          style={{
            transform: `translateY(${accordionY}px)`,
            height: accordionHeight,
          }}
        >
          <EventDetailInline event={row} onFilter={onFilter} />
        </div>,
      );
    }
  }

  // ---- Render ----

  return (
    <div
      className="flex flex-1 flex-col overflow-hidden font-mono text-[0.8125rem]"
      role="grid"
      aria-label="Query results"
    >
      <div
        className="flex-1 overflow-auto relative"
        ref={scrollContainerRef}
        onScroll={handleScroll}
      >
        {/* Header row inside scroll container for sticky top:0 */}
        <div
          className="grid sticky top-0 z-[3] bg-card border-b border-border min-w-max"
          style={gridStyle}
          role="row"
        >
          <div />
          {headerGroups[0]?.headers.map((header) => {
            const col = header.id;
            const isSorted = currentSort?.field === col;
            const sortDir = isSorted ? currentSort!.direction : undefined;

            return (
              <div
                key={col}
                className={cn(
                  "relative px-3 py-1.5 text-muted-foreground font-semibold text-xs uppercase tracking-wide overflow-hidden text-ellipsis whitespace-nowrap select-none cursor-pointer flex items-center gap-1 hover:text-foreground focus-visible:outline-2 focus-visible:outline-ring focus-visible:outline-offset-[-2px]",
                  hasTimeCol && col === "_time" && "sticky left-6 z-[4] bg-card",
                )}
                role="columnheader"
                aria-sort={
                  isSorted
                    ? sortDir === "asc"
                      ? "ascending"
                      : "descending"
                    : "none"
                }
                tabIndex={0}
                onClick={() => handleHeaderClick(col)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    handleHeaderClick(col);
                  }
                }}
              >
                <span>
                  {flexRender(header.column.columnDef.header, header.getContext())}
                </span>
                {isSorted && (
                  <span className="text-primary text-[0.5625rem] leading-none shrink-0">
                    {sortDir === "asc" ? "▲" : "▼"}
                  </span>
                )}
                <div
                  className={cn(
                    "absolute right-0 top-0 bottom-0 w-1 cursor-col-resize z-[5] select-none touch-none",
                    resizingCol === col
                      ? "border-r-2 border-primary"
                      : "hover:border-r-2 hover:border-primary",
                  )}
                  onPointerDown={(e: React.PointerEvent<HTMLDivElement>) => handleResizeStart(e, col)}
                  onDoubleClick={(e: React.MouseEvent<HTMLDivElement>) => handleResizeDblClick(e, col)}
                />
              </div>
            );
          })}
        </div>

        {/* Scroll content area */}
        <div className="relative min-w-max" style={{ height: totalHeight }}>
          {visibleRows}
        </div>
      </div>
    </div>
  );
}
