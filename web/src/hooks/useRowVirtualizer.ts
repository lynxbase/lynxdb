/**
 * Shared row virtualizer hook built on @tanstack/react-virtual.
 *
 * Used by ListView for basic list virtualization. ResultsTable has its own
 * bespoke scroll math (accordion offsets, sticky columns, grid rows) that
 * is too tightly coupled to replace safely, so this hook is currently only
 * consumed by ListView — but the API is generic enough to serve both.
 */

import { useVirtualizer } from "@tanstack/react-virtual";
import type { RefObject } from "react";

export interface UseRowVirtualizerOptions {
  /** Total number of rows in the dataset. */
  count: number;
  /** Ref to the scrollable container element. */
  scrollRef: RefObject<HTMLDivElement | null>;
  /** Estimated height of each row in pixels. */
  estimateSize: (index: number) => number;
  /** Number of rows to render outside the visible area (default: 5). */
  overscan?: number;
}

/**
 * Thin wrapper around @tanstack/react-virtual's useVirtualizer,
 * pre-configured for vertical row lists.
 */
export function useRowVirtualizer({
  count,
  scrollRef,
  estimateSize,
  overscan = 5,
}: UseRowVirtualizerOptions) {
  const virtualizer = useVirtualizer({
    count,
    getScrollElement: () => scrollRef.current,
    estimateSize,
    overscan,
  });

  return virtualizer;
}
