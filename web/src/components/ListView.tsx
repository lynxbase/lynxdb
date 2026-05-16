import React, { useState, useCallback, useRef } from "react";
import type { QueryResult, EventsResult, AggregateResult } from "../api/client";
import { EventDetailInline } from "./EventDetail";
import { rowKey } from "../utils/rowKey";
import { deriveColumnsFromEvents } from "../utils/deriveColumns";
import { useRowVirtualizer } from "../hooks/useRowVirtualizer";
import { cn } from "@/lib/utils";

interface ListViewProps {
  result: QueryResult | null;
  onCellCopy?: (value: string, x: number, y: number) => void;
  onFilter?: (field: string, value: string, exclude: boolean) => void;
}

/** Estimated height of a collapsed event row (header + ~4 fields). */
const ESTIMATED_ROW_HEIGHT = 120;
/** Max accordion height for expanded detail. */
const MAX_ACCORDION_HEIGHT = 400;

/** Normalize result data into columns and rows */
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
  };
}

export function ListView({ result, onCellCopy, onFilter }: ListViewProps) {
  const { columns, rowCount, getRow } = useTableData(result);
  const [expandedIndex, setExpandedIndex] = useState<number | null>(null);
  const scrollRef = useRef<HTMLDivElement>(null);

  const handleToggle = useCallback((i: number) => {
    setExpandedIndex((prev) => (prev === i ? null : i));
  }, []);

  const estimateSize = useCallback(
    (index: number) => {
      const row = getRow(index);
      // Base height: header line + one line per field
      const fieldCount = columns.length;
      const baseHeight = 28 + fieldCount * 24 + 16; // header + fields + padding
      if (expandedIndex === index) {
        const detailFieldCount = Object.keys(row).length;
        const accordionHeight = Math.min(
          detailFieldCount * 28 + 52,
          MAX_ACCORDION_HEIGHT,
        );
        return baseHeight + accordionHeight;
      }
      return Math.max(ESTIMATED_ROW_HEIGHT, baseHeight);
    },
    [columns.length, expandedIndex, getRow],
  );

  const virtualizer = useRowVirtualizer({
    count: rowCount,
    scrollRef,
    estimateSize,
    overscan: 5,
  });

  if (!result || rowCount === 0) {
    return (
      <div className="flex flex-1 items-center justify-center text-muted-foreground font-sans text-sm p-12">
        No results
      </div>
    );
  }

  return (
    <div className="flex-1 overflow-auto p-0 font-mono text-[0.8125rem]" ref={scrollRef}>
      <div
        style={{
          height: virtualizer.getTotalSize(),
          width: "100%",
          position: "relative",
        }}
      >
        {virtualizer.getVirtualItems().map((virtualRow) => {
          const i = virtualRow.index;
          const row = getRow(i);
          const isExpanded = expandedIndex === i;

          return (
            <div
              key={rowKey(row)}
              data-index={i}
              ref={virtualizer.measureElement}
              style={{
                position: "absolute",
                top: 0,
                left: 0,
                width: "100%",
                transform: `translateY(${virtualRow.start}px)`,
              }}
            >
              <div
                role="button"
                tabIndex={0}
                aria-expanded={isExpanded}
                className={cn(
                  "border-b border-border px-3 py-2 cursor-pointer transition-colors duration-75 motion-reduce:transition-none focus-visible:outline-2 focus-visible:outline-ring",
                  isExpanded
                    ? "bg-accent hover:bg-accent/80"
                    : "hover:bg-muted/50",
                )}
                onClick={() => handleToggle(i)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    handleToggle(i);
                  }
                }}
              >
                <div className="font-sans text-[0.6875rem] font-semibold text-muted-foreground uppercase tracking-wider mb-1">
                  Event {i + 1}
                </div>
                {columns.map((col) => {
                  const value = row[col] == null ? "" : String(row[col]);
                  return (
                    <div key={col} className="flex items-baseline gap-3 py-px leading-relaxed">
                      <span className="shrink-0 basis-[120px] text-muted-foreground text-xs overflow-hidden text-ellipsis whitespace-nowrap">
                        {col}
                      </span>
                      <span
                        role="button"
                        tabIndex={0}
                        aria-label={`Copy ${col} value`}
                        className="flex-1 text-foreground break-words rounded-sm px-0.5 -mx-0.5 cursor-pointer transition-colors duration-100 motion-reduce:transition-none hover:bg-muted focus-visible:outline-2 focus-visible:outline-ring"
                        title={value}
                        onClick={(e: React.MouseEvent) => {
                          e.stopPropagation();
                          if (onCellCopy && value) {
                            onCellCopy(value, e.clientX, e.clientY);
                          }
                        }}
                        onKeyDown={(e) => {
                          if (e.key === "Enter" || e.key === " ") {
                            e.preventDefault();
                            if (onCellCopy && value) onCellCopy(value, 0, 0);
                          }
                        }}
                      >
                        {value}
                      </span>
                    </div>
                  );
                })}
              </div>

              {isExpanded && (
                <div className="border-b border-border max-h-[400px] overflow-hidden">
                  <EventDetailInline event={row} onFilter={onFilter} />
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}
