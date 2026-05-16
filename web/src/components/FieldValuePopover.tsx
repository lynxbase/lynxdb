import { useState, useEffect, useCallback } from "react";
import { fetchFieldValues } from "../api/client";
import type { FieldValue } from "../api/client";
import { Skeleton } from "./ui/skeleton";

interface FieldValuePopoverProps {
  fieldName: string;
  anchorRect: DOMRect;
  onFilter: (field: string, value: string, exclude: boolean) => void;
  onClose: () => void;
}

const POPOVER_WIDTH = 280;
const POPOVER_MAX_HEIGHT = 360;

function computePosition(anchorRect: DOMRect): { top: number; left: number } {
  const viewportWidth = window.innerWidth;
  const viewportHeight = window.innerHeight;

  let left = anchorRect.right + 8;
  let top = anchorRect.top;

  // If overflows right edge, position to the left of anchor
  if (left + POPOVER_WIDTH > viewportWidth) {
    left = anchorRect.left - POPOVER_WIDTH - 8;
  }

  // If overflows bottom, shift up
  if (top + POPOVER_MAX_HEIGHT > viewportHeight) {
    top = viewportHeight - POPOVER_MAX_HEIGHT - 8;
  }

  // Ensure top is never negative
  if (top < 8) top = 8;

  return { top, left };
}

export function FieldValuePopover({
  fieldName,
  anchorRect,
  onFilter,
  onClose,
}: FieldValuePopoverProps) {
  const [values, setValues] = useState<FieldValue[]>([]);
  const [loading, setLoading] = useState(true);
  const [fetchError, setFetchError] = useState(false);

  // Fetch top 10 values on mount
  useEffect(() => {
    setLoading(true);
    setFetchError(false);
    fetchFieldValues(fieldName, 10)
      .then((vals) => {
        setValues(vals);
        setLoading(false);
      })
      .catch(() => {
        setFetchError(true);
        setLoading(false);
      });
  }, [fieldName]);

  // Click-outside detection
  useEffect(() => {
    function handleClick(e: MouseEvent) {
      const target = e.target as Node;
      const el = document.querySelector("[data-field-popover]");
      if (el && !el.contains(target)) {
        onClose();
      }
    }
    // Delay listener attachment to avoid immediately closing on the triggering click
    const timer = setTimeout(() => {
      document.addEventListener("click", handleClick, true);
    }, 0);
    return () => {
      clearTimeout(timer);
      document.removeEventListener("click", handleClick, true);
    };
  }, [onClose]);

  // Escape key closes popover
  useEffect(() => {
    function handleKeyDown(e: KeyboardEvent) {
      if (e.key === "Escape") {
        onClose();
      }
    }
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const handleInclude = useCallback(
    (value: string) => {
      onFilter(fieldName, value, false);
    },
    [fieldName, onFilter],
  );

  const handleExclude = useCallback(
    (value: string) => {
      onFilter(fieldName, value, true);
    },
    [fieldName, onFilter],
  );

  const pos = computePosition(anchorRect);
  const maxCount =
    values.length > 0 ? Math.max(...values.map((v) => v.count)) : 1;

  return (
    <div
      data-field-popover
      className="fixed z-50 w-[280px] max-h-[360px] overflow-y-auto rounded-md border border-border bg-popover py-2 text-popover-foreground"
      style={{
        top: `${pos.top}px`,
        left: `${pos.left}px`,
      }}
    >
      <div className="px-3 pb-2 border-b border-border">
        <div className="text-[0.8125rem] font-medium text-foreground">{fieldName}</div>
        <div className="text-[0.6875rem] text-muted-foreground">Top 10 values</div>
      </div>

      {loading && (
        <div className="flex flex-col gap-2 p-3">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-4 w-full" />
          ))}
        </div>
      )}

      {fetchError && (
        <div className="p-4 text-center text-xs text-muted-foreground">Failed to load values</div>
      )}

      {!loading && !fetchError && values.length === 0 && (
        <div className="p-4 text-center text-xs text-muted-foreground">No values found</div>
      )}

      {!loading && !fetchError && values.length > 0 && (
        <div className="py-1">
          {values.map((v) => {
            const pct = maxCount > 0 ? (v.count / maxCount) * 100 : 0;
            return (
              <div className="flex items-center gap-1.5 px-3 py-0.5 hover:bg-accent" key={v.value}>
                <span
                  className="shrink-0 max-w-[7.5rem] truncate font-mono text-xs text-foreground"
                  title={v.value}
                >
                  {v.value}
                </span>
                <div className="flex-1 h-1.5 rounded-full bg-muted overflow-hidden">
                  <div
                    className="h-full rounded-full bg-primary/40"
                    style={{ width: `${pct}%` }}
                  />
                </div>
                <span className="min-w-[1.875rem] text-right text-[0.6875rem] text-muted-foreground whitespace-nowrap">
                  {v.count}
                </span>
                <div className="flex gap-0.5 shrink-0">
                  <button
                    type="button"
                    className="inline-flex size-5 items-center justify-center rounded-sm text-xs font-semibold text-primary hover:bg-primary/10 cursor-pointer"
                    onClick={() => handleInclude(v.value)}
                    title={`Add filter: ${fieldName}="${v.value}"`}
                    aria-label={`Include ${v.value}`}
                  >
                    +
                  </button>
                  <button
                    type="button"
                    className="inline-flex size-5 items-center justify-center rounded-sm text-xs font-semibold text-destructive hover:bg-destructive/10 cursor-pointer"
                    onClick={() => handleExclude(v.value)}
                    title={`Exclude: ${fieldName}!="${v.value}"`}
                    aria-label={`Exclude ${v.value}`}
                  >
                    -
                  </button>
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
