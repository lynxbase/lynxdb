import { AlertCircle, Search, Terminal } from "lucide-react";
import { ResultsTable } from "../../components/ResultsTable";
import { ListView } from "../../components/ListView";
import { useSearchStore } from "../../stores/search";
import type { QueryResult, EventsResult } from "../../api/client";
import { Alert, AlertTitle, AlertDescription } from "../../components/ui/alert";
import { Button } from "../../components/ui/button";
import { Skeleton } from "../../components/ui/skeleton";

function resultCount(r: QueryResult | null): number {
  if (!r) return 0;
  if (r.type === "events") return r.events.length;
  return r.rows.length;
}

function EmptyStateInitial() {
  return (
    <div className="flex flex-col items-center justify-center flex-1 p-12 text-center gap-1.5">
      <Search className="size-8 text-muted-foreground/50 mb-2" />
      <div className="text-base font-medium text-muted-foreground">No events yet</div>
      <div className="text-[0.8125rem] text-muted-foreground leading-relaxed">
        Run a query to explore your data, or try:
      </div>
      <code className="inline-block my-2 px-3 py-1.5 bg-secondary border border-border rounded-sm font-mono text-[0.8125rem] text-primary select-all">
        lynxdb demo
      </code>
      <div className="text-xs text-muted-foreground">to generate sample log data</div>
    </div>
  );
}

function EmptyStateNoResults() {
  return (
    <div className="flex flex-col items-center justify-center flex-1 p-12 text-center gap-1.5">
      <Terminal className="size-8 text-muted-foreground/50 mb-2" />
      <div className="text-base font-medium text-muted-foreground">No matching events</div>
      <div className="text-[0.8125rem] text-muted-foreground leading-relaxed">
        Try widening the time range or adjusting your filters
      </div>
    </div>
  );
}

/** Skeleton rows to show while loading */
function LoadingSkeleton({ rows }: { rows: number }) {
  return (
    <div className="flex flex-col gap-1 p-3 flex-1">
      {Array.from({ length: rows }).map((_, i) => (
        <Skeleton key={i} className="h-7 w-full" />
      ))}
    </div>
  );
}

/** Error state with alert and retry */
function ErrorState({ error, onRetry }: { error: string; onRetry?: () => void }) {
  return (
    <div className="flex flex-col items-center justify-center flex-1 p-6">
      <Alert variant="destructive" className="max-w-lg">
        <AlertCircle className="size-4" />
        <AlertTitle>Query Error</AlertTitle>
        <AlertDescription>
          <p>{error}</p>
          {onRetry && (
            <Button variant="outline" size="xs" className="mt-2" onClick={onRetry}>
              Retry
            </Button>
          )}
        </AlertDescription>
      </Alert>
    </div>
  );
}

interface ResultsContainerProps {
  resultsAreaRef: React.RefObject<HTMLDivElement | null>;
  onSort: (newQuery: string) => void;
  onFilter: (field: string, value: string, exclude: boolean) => void;
  onCellCopy: (value: string, x: number, y: number) => void;
  onNewEventsBadgeClick: () => void;
}

export function ResultsContainer({
  resultsAreaRef,
  onSort,
  onFilter,
  onCellCopy,
  onNewEventsBadgeClick,
}: ResultsContainerProps) {
  const query = useSearchStore((s) => s.query);
  const result = useSearchStore((s) => s.result);
  const loading = useSearchStore((s) => s.loading);
  const error = useSearchStore((s) => s.error);
  const hasQueried = useSearchStore((s) => s.hasQueried);
  const tailActive = useSearchStore((s) => s.tailActive);
  const tailEvents = useSearchStore((s) => s.tailEvents);
  const tailNewCount = useSearchStore((s) => s.tailNewCount);
  const queryActive = useSearchStore((s) => s.queryActive);
  const canceled = useSearchStore((s) => s.canceled);
  const viewMode = useSearchStore((s) => s.viewMode);
  const pageSize = useSearchStore((s) => s.pageSize);

  // Build an EventsResult from live tail events for ResultsTable
  const activeResult: QueryResult | null = tailActive
    ? ({
        type: "events",
        events: tailEvents as unknown as Record<string, unknown>[],
        total: tailEvents.length,
        has_more: false,
      } satisfies EventsResult)
    : result;

  // Determine which content to show in the results area
  const showInitialEmpty =
    !tailActive && !hasQueried && !loading && !queryActive && !error;
  const showNoResults =
    !tailActive &&
    hasQueried &&
    !loading &&
    !queryActive &&
    !error &&
    !canceled &&
    resultCount(result) === 0;
  const showLoading =
    !tailActive && loading && !result;
  const showError =
    !tailActive && !loading && !queryActive && !!error && !canceled;

  return (
    <div className="flex flex-col flex-1 overflow-hidden relative" ref={resultsAreaRef}>
      {tailActive && tailNewCount > 0 && (
        <button
          type="button"
          className="absolute top-2 left-1/2 -translate-x-1/2 z-10 flex items-center gap-1 px-3 py-1 border border-[var(--success)] rounded-full bg-secondary text-[var(--success)] font-sans text-xs font-medium cursor-pointer whitespace-nowrap hover:bg-muted focus-visible:outline-2 focus-visible:outline-primary focus-visible:outline-offset-1 transition-colors"
          onClick={onNewEventsBadgeClick}
          aria-label={`${tailNewCount} new events, click to scroll to top`}
        >
          &#8593; {tailNewCount} new{" "}
          {tailNewCount === 1 ? "event" : "events"}
        </button>
      )}
      {showInitialEmpty && <EmptyStateInitial />}
      {showLoading && <LoadingSkeleton rows={Math.min(pageSize, 12)} />}
      {showError && <ErrorState error={error!} />}
      {showNoResults && <EmptyStateNoResults />}
      {!showInitialEmpty &&
        !showNoResults &&
        !showLoading &&
        !showError &&
        (viewMode === "table" ? (
          <ResultsTable
            result={activeResult}
            onSort={onSort}
            currentQuery={query}
            onFilter={onFilter}
          />
        ) : (
          <ListView
            result={activeResult}
            onCellCopy={onCellCopy}
            onFilter={onFilter}
          />
        ))}
    </div>
  );
}
