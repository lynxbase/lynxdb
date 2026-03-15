import { useCallback, useEffect, useRef } from "preact/hooks";
import { signal } from "@preact/signals";
import { QueryEditor } from "../editor/QueryEditor";
import type { QueryEditorHandle } from "../editor/QueryEditor";
import { TimeRangePicker } from "../components/TimeRangePicker";
import { ResultsTable } from "../components/ResultsTable";
import { EventDetail } from "../components/EventDetail";
import { QueryStatsBar } from "../components/QueryStats";
import { FlowSidebar } from "../components/FlowSidebar";
import { Timeline } from "../components/Timeline";
import { LiveTailButton } from "../components/LiveTailButton";
import { useKeyboardShortcuts } from "../hooks/useKeyboardShortcuts";
import {
  executeQuery,
  fetchHistogram,
  fetchIndexes,
  fetchViews,
  fetchExplain,
  fetchFields,
} from "../api/client";
import { startTail } from "../api/sse";
import type {
  QueryResult,
  QueryStats,
  EventsResult,
  IndexInfo,
  ViewSummary,
  ExplainResult,
  HistogramBucket,
} from "../api/client";
import type { TailEvent } from "../api/sse";
import styles from "./SearchView.module.css";

interface Props {
  path?: string;
}

const query = signal("");
const from = signal("-1h");
const to = signal<string | undefined>(undefined);
const result = signal<QueryResult | null>(null);
const stats = signal<QueryStats | null>(null);
const loading = signal(false);
const error = signal<string | null>(null);
const selectedEvent = signal<Record<string, unknown> | null>(null);

/* --- Part 3 signals --- */
const sidebarVisible = signal(true);
const timelineBuckets = signal<HistogramBucket[]>([]);
/** Track whether at least one query has been executed (controls timeline visibility) */
const hasQueried = signal(false);

/* --- Flow sidebar signals --- */
const sidebarIndexes = signal<IndexInfo[]>([]);
const sidebarViews = signal<ViewSummary[]>([]);
const explainResult = signal<ExplainResult | null>(null);
const fieldTypeMap = signal<Map<string, string>>(new Map());

/* --- Part 4: Live Tail signals --- */
const tailActive = signal(false);
const tailEvents = signal<TailEvent[]>([]);
const tailNewCount = signal(0);
const tailCatchupDone = signal(false);

/** Maximum events to keep in the live tail buffer */
const TAIL_BUFFER_CAP = 10_000;

function resultCount(r: QueryResult | null): number {
  if (!r) return 0;
  if (r.type === "events") return r.events.length;
  return r.rows.length;
}

/**
 * Run a query and update all relevant signals (result, stats, fields,
 * histogram). Reused by the primary execute handler, field-filter, and
 * timeline brush to avoid duplicating the orchestration logic.
 */
function runQueryAndRefresh(q: string, fromVal: string, toVal: string | undefined): void {
  if (!q || loading.value) return;

  loading.value = true;
  error.value = null;
  result.value = null;
  stats.value = null;
  selectedEvent.value = null;

  executeQuery(q, fromVal, toVal)
    .then((resp) => {
      result.value = resp.result;
      stats.value = resp.stats;
      hasQueried.value = true;

      // Fetch histogram and explain in parallel after query succeeds.
      // These are non-blocking -- failures are silently ignored so
      // the primary query result is never held back.
      fetchHistogram(fromVal, toVal, 60)
        .then((histResult) => { timelineBuckets.value = histResult.buckets; })
        .catch(() => { /* non-critical */ });

      fetchExplain(q, fromVal, toVal)
        .then((explain) => { explainResult.value = explain; })
        .catch(() => { /* non-critical -- explain is an enhancement */ });
    })
    .catch((err: unknown) => {
      const message = err instanceof Error ? err.message : "Unknown error";
      error.value = message;
    })
    .finally(() => {
      loading.value = false;
    });
}

// ---------------------------------------------------------------------------
// Empty state components
// ---------------------------------------------------------------------------

function EmptyStateInitial() {
  return (
    <div class={styles.emptyState}>
      <div class={styles.emptyTitle}>No events yet</div>
      <div class={styles.emptyHint}>
        Run a query to explore your data, or try:
      </div>
      <code class={styles.emptyCode}>lynxdb demo</code>
      <div class={styles.emptySubHint}>to generate sample log data</div>
    </div>
  );
}

function EmptyStateNoResults() {
  return (
    <div class={styles.emptyState}>
      <div class={styles.emptyTitle}>No matching events</div>
      <div class={styles.emptyHint}>
        Try adjusting your query or expanding the time range
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function SearchView(_props: Props) {
  const tailCleanupRef = useRef<(() => void) | null>(null);
  const resultsAreaRef = useRef<HTMLDivElement>(null);
  const editorHandleRef = useRef<QueryEditorHandle | null>(null);
  /** Tracks whether auto-scroll is paused (user scrolled away from top) */
  const autoScrollPaused = useRef(false);

  const handleQueryChange = useCallback((value: string) => {
    query.value = value;
  }, []);

  const handleExecute = useCallback(() => {
    if (tailActive.value) return; // block while tailing
    runQueryAndRefresh(query.value.trim(), from.value, to.value);
  }, []);

  const handleRowClick = useCallback((row: Record<string, unknown>) => {
    selectedEvent.value = row;
  }, []);

  const handleCloseDetail = useCallback(() => {
    selectedEvent.value = null;
  }, []);

  const handleSidebarToggle = useCallback(() => {
    sidebarVisible.value = !sidebarVisible.value;
  }, []);

  const handleInsertCommand = useCallback((template: string) => {
    const current = query.value.trim();
    query.value = current ? `${current} ${template}` : template;
    setTimeout(() => {
      editorHandleRef.current?.focus();
    }, 0);
  }, []);

  const handleSetSource = useCallback((name: string) => {
    query.value = `from ${name} `;
    // Focus the editor so the user can continue typing
    setTimeout(() => {
      editorHandleRef.current?.focus();
    }, 0);
  }, []);

  const handleTimelineBrush = useCallback((fromTs: number, toTs: number) => {
    // Convert epoch seconds to ISO strings for the time range
    from.value = new Date(fromTs * 1000).toISOString();
    to.value = new Date(toTs * 1000).toISOString();

    runQueryAndRefresh(query.value.trim(), from.value, to.value);
  }, []);

  /* --- Live Tail toggle --- */
  const handleTailToggle = useCallback(() => {
    if (tailActive.value) {
      // Stop tailing
      if (tailCleanupRef.current) {
        tailCleanupRef.current();
        tailCleanupRef.current = null;
      }
      tailActive.value = false;
      tailEvents.value = [];
      tailNewCount.value = 0;
      tailCatchupDone.value = false;
      autoScrollPaused.current = false;
      return;
    }

    // Start tailing
    const q = query.value.trim();
    tailActive.value = true;
    tailEvents.value = [];
    tailNewCount.value = 0;
    tailCatchupDone.value = false;
    result.value = null;
    stats.value = null;
    error.value = null;
    selectedEvent.value = null;
    autoScrollPaused.current = false;

    const cleanup = startTail(q, from.value, 100, {
      onEvent(event: TailEvent) {
        const prev = tailEvents.value;
        const next = [event, ...prev];
        tailEvents.value = next.length > TAIL_BUFFER_CAP
          ? next.slice(0, TAIL_BUFFER_CAP)
          : next;

        if (autoScrollPaused.current) {
          tailNewCount.value = tailNewCount.value + 1;
        }
      },
      onCatchupDone(_count: number) {
        tailCatchupDone.value = true;
      },
      onError(message: string) {
        error.value = message;
      },
      onWarning(message: string) {
        // Show warning briefly in the error slot, then clear
        error.value = message;
        setTimeout(() => {
          if (error.value === message) {
            error.value = null;
          }
        }, 3000);
      },
    });

    tailCleanupRef.current = cleanup;
  }, []);

  /** Click handler for the "new events" badge -- scroll back to top */
  const handleNewEventsBadgeClick = useCallback(() => {
    if (!resultsAreaRef.current) return;
    const viewport = resultsAreaRef.current.querySelector("[class*='viewport']");
    if (viewport) {
      viewport.scrollTop = 0;
    }
    autoScrollPaused.current = false;
    tailNewCount.value = 0;
  }, []);

  // Editor ref callback
  const handleEditorRef = useCallback((handle: QueryEditorHandle | null) => {
    editorHandleRef.current = handle;
  }, []);

  // --- Keyboard shortcuts ---
  useKeyboardShortcuts({
    onFocusEditor: () => editorHandleRef.current?.focus(),
    onToggleTail: handleTailToggle,
    onToggleSidebar: () => { sidebarVisible.value = !sidebarVisible.value; },
    onClosePanel: () => { selectedEvent.value = null; },
  });

  // Capture-phase scroll listener for auto-scroll pause detection.
  // Scroll events do not bubble, so we must capture them on the
  // results area container to intercept scrolls from the nested
  // ResultsTable viewport.
  useEffect(() => {
    const el = resultsAreaRef.current;
    if (!el) return;

    function onScroll(e: Event) {
      if (!tailActive.value) return;
      const target = e.target;
      if (!(target instanceof HTMLElement)) return;
      const scrolledFromTop = target.scrollTop;
      autoScrollPaused.current = scrolledFromTop > 10;
      if (!autoScrollPaused.current) {
        tailNewCount.value = 0;
      }
    }

    el.addEventListener("scroll", onScroll, true);
    return () => el.removeEventListener("scroll", onScroll, true);
  }, []);

  // Cleanup SSE on unmount
  useEffect(() => {
    return () => {
      if (tailCleanupRef.current) {
        tailCleanupRef.current();
        tailCleanupRef.current = null;
      }
    };
  }, []);

  // Fetch indexes, views, and field catalog on mount for the flow sidebar
  useEffect(() => {
    Promise.allSettled([fetchIndexes(), fetchViews(), fetchFields()])
      .then(([idx, views, fields]) => {
        if (idx.status === "fulfilled") sidebarIndexes.value = idx.value;
        if (views.status === "fulfilled") sidebarViews.value = views.value;
        if (fields.status === "fulfilled") {
          const m = new Map<string, string>();
          for (const f of fields.value) {
            m.set(f.name, f.type);
          }
          fieldTypeMap.value = m;
        }
      });
  }, []);

  // Build an EventsResult from live tail events for ResultsTable
  const activeResult: QueryResult | null = tailActive.value
    ? ({
        type: "events",
        events: tailEvents.value as unknown as Record<string, unknown>[],
        total: tailEvents.value.length,
        has_more: false,
      } satisfies EventsResult)
    : result.value;

  // Determine which content to show in the results area
  const showInitialEmpty = !tailActive.value && !hasQueried.value && !loading.value && !error.value;
  const showNoResults = !tailActive.value && hasQueried.value && !loading.value && !error.value && resultCount(result.value) === 0;

  return (
    <div class={styles.view}>
      <div class={styles.queryBar}>
        <QueryEditor
          value={query.value}
          onChange={handleQueryChange}
          onExecute={handleExecute}
          editorRef={handleEditorRef}
        />
        <button
          type="button"
          class={styles.runBtn}
          onClick={handleExecute}
          disabled={loading.value || tailActive.value}
          aria-label="Run query"
          title="Run query (Ctrl+Enter)"
        >
          &#9654;
        </button>
        <LiveTailButton
          active={tailActive.value}
          onToggle={handleTailToggle}
        />
        <TimeRangePicker from={from} to={to} />
      </div>

      <div class={styles.body}>
        <FlowSidebar
          visible={sidebarVisible.value}
          indexes={sidebarIndexes.value}
          views={sidebarViews.value}
          explainResult={explainResult.value}
          fieldTypes={fieldTypeMap.value}
          onToggle={handleSidebarToggle}
          onSelectSource={handleSetSource}
          onInsertCommand={handleInsertCommand}
        />

        <div class={styles.mainContent}>
          <Timeline
            from={from.value}
            to={to.value}
            buckets={timelineBuckets.value}
            visible={hasQueried.value && !tailActive.value}
            onBrush={handleTimelineBrush}
          />

          <QueryStatsBar
            stats={stats.value}
            loading={loading.value}
            error={error.value}
            resultCount={tailActive.value ? tailEvents.value.length : resultCount(result.value)}
            tailActive={tailActive.value}
            tailEventCount={tailEvents.value.length}
            tailCatchupDone={tailCatchupDone.value}
          />

          <div
            class={styles.resultsArea}
            ref={resultsAreaRef}
          >
            {tailActive.value && tailNewCount.value > 0 && (
              <button
                type="button"
                class={styles.newEventsBadge}
                onClick={handleNewEventsBadgeClick}
                aria-label={`${tailNewCount.value} new events, click to scroll to top`}
              >
                &#8593; {tailNewCount.value} new {tailNewCount.value === 1 ? "event" : "events"}
              </button>
            )}
            {showInitialEmpty && <EmptyStateInitial />}
            {showNoResults && <EmptyStateNoResults />}
            {!showInitialEmpty && !showNoResults && (
              <ResultsTable
                result={activeResult}
                onRowClick={handleRowClick}
                selectedRow={selectedEvent.value}
              />
            )}
            <EventDetail
              event={selectedEvent.value}
              onClose={handleCloseDetail}
            />
          </div>
        </div>
      </div>
    </div>
  );
}
