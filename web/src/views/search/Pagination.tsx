import { useCallback } from "react";
import { PaginationBar } from "../../components/PaginationBar";
import { useSearchStore } from "../../stores/search";
import type { QueryResult, EventsResult } from "../../api/client";

function resultCount(r: QueryResult | null): number {
  if (!r) return 0;
  if (r.type === "events") return r.events.length;
  return r.rows.length;
}

interface PaginationProps {
  onRunQuery: (
    q: string,
    fromVal: string,
    toVal: string | undefined,
    pg?: number,
    sz?: number,
  ) => void;
}

const ss = useSearchStore;

export function Pagination({ onRunQuery }: PaginationProps) {
  const page = useSearchStore((s) => s.page);
  const pageSize = useSearchStore((s) => s.pageSize);
  const result = useSearchStore((s) => s.result);
  const tailActive = useSearchStore((s) => s.tailActive);
  const tailEvents = useSearchStore((s) => s.tailEvents);

  // Build an EventsResult from live tail events
  const activeResult: QueryResult | null = tailActive
    ? ({
        type: "events",
        events: tailEvents as unknown as Record<string, unknown>[],
        total: tailEvents.length,
        has_more: false,
      } satisfies EventsResult)
    : result;

  const totalCount = activeResult
    ? activeResult.type === "events"
      ? activeResult.total
      : activeResult.rows.length
    : 0;
  const pageCount = resultCount(activeResult);
  const hasResults = activeResult && pageCount > 0 && !tailActive;

  const handlePageChange = useCallback(
    (newPage: number) => {
      ss.setState({ page: newPage });
      const state = ss.getState();
      onRunQuery(
        state.query.trim(),
        state.from,
        state.to,
        newPage,
        state.pageSize,
      );
    },
    [onRunQuery],
  );

  const handlePageSizeChange = useCallback(
    (newSize: number) => {
      ss.setState({ pageSize: newSize, page: 1 }); // Reset to first page
      const state = ss.getState();
      onRunQuery(state.query.trim(), state.from, state.to, 1, newSize);
    },
    [onRunQuery],
  );

  if (!hasResults) return null;

  return (
    <PaginationBar
      page={page}
      pageSize={pageSize}
      total={totalCount}
      onPageChange={handlePageChange}
      onPageSizeChange={handlePageSizeChange}
    />
  );
}
