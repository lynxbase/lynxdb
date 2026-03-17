import { useCallback, useEffect, useRef } from "preact/hooks";
import { signal, batch, effect } from "@preact/signals";
import { QueryEditor } from "../editor/QueryEditor";
import type { QueryEditorHandle } from "../editor/QueryEditor";
import { TimeRangePicker } from "../components/TimeRangePicker";
import { ResultsTable } from "../components/ResultsTable";
import { EventDetail } from "../components/EventDetail";
import { QueryStatsBar } from "../components/QueryStats";
import { FlowSidebar } from "../components/FlowSidebar";
import { Timeline } from "../components/Timeline";
import { LiveTailButton } from "../components/LiveTailButton";
import { ExplainInspector } from "../components/ExplainInspector";
import { TableToolbar } from "../components/TableToolbar";
import { PaginationBar } from "../components/PaginationBar";
import { ListView } from "../components/ListView";
import { CopyTooltip } from "../components/CopyTooltip";
import { useKeyboardShortcuts } from "../hooks/useKeyboardShortcuts";
import {
  fetchHistogram,
  fetchHistogramGrouped,
  fetchIndexes,
  fetchViews,
  fetchExplain,
  fetchFields,
} from "../api/client";
import {
  submitHybridQuery,
  streamQuery,
  subscribeJobProgress,
} from "../api/streaming";
import { authHeaders } from "../api/auth";
import { startTail } from "../api/sse";
import { pushHistory } from "../stores/queryHistory";
import { writeQueryToHash, readQueryFromHash } from "../stores/queryUrl";
import { dispatchDiagnostics, clearEditorDiagnostics } from "../editor/diagnostics";
import {
  generateCSV,
  generateJSON,
  downloadFile,
  generateFilename,
} from "../utils/export";
import { appendFilter } from "../utils/filterQuery";
import {
  paletteOpen,
  helpOverlayOpen,
  paletteQuery,
  formatShortcut,
  SHORTCUTS,
} from "../utils/keyboard";
import type {
  QueryResult,
  QueryStats,
  EventsResult,
  AggregateResult,
  IndexInfo,
  ViewSummary,
  ExplainResult,
  HistogramBucket,
  HistogramBucketGrouped,
  FieldInfo,
} from "../api/client";
import type { TailEvent } from "../api/sse";
import styles from "./SearchView.module.css";

// Known log level keys for histogram grouping detection
const KNOWN_LEVELS = new Set(["debug", "info", "warn", "error"]);

/** Returns true if any bucket in a grouped histogram response contains a known level key. */
function hasKnownLevels(buckets: HistogramBucketGrouped[]): boolean {
  for (const b of buckets) {
    for (const key of Object.keys(b.counts)) {
      if (KNOWN_LEVELS.has(key.toLowerCase())) return true;
    }
  }
  return false;
}

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
const groupedBuckets = signal<HistogramBucketGrouped[]>([]);
/** Track whether user has brush-zoomed on the histogram */
const histogramBrushed = signal(false);
/** Track whether at least one query has been executed (controls timeline visibility) */
const hasQueried = signal(false);

/* --- Flow sidebar signals --- */
const sidebarIndexes = signal<IndexInfo[]>([]);
const sidebarViews = signal<ViewSummary[]>([]);
const explainResult = signal<ExplainResult | null>(null);
const fieldTypeMap = signal<Map<string, string>>(new Map());
const catalogFields = signal<FieldInfo[]>([]);

/* --- Part 4: Live Tail signals --- */
const tailActive = signal(false);
const tailEvents = signal<TailEvent[]>([]);
const tailNewCount = signal(0);
const tailCatchupDone = signal(false);
/** True when the SSE tail connection is in reconnecting state */
const tailReconnecting = signal(false);

/* --- Phase 6: Explain inspector toggle --- */
/** Controls whether the explain inspector panel is open (consumed by Plan 02) */
const explainOpen = signal(false);

/* --- Phase 5: Streaming & Progress signals --- */
/** True while any query execution mode is active (sync wait, streaming, progress) */
const queryActive = signal(false);
/** True while NDJSON streaming is in progress */
const streaming = signal(false);
/** Row count during streaming */
const streamingCount = signal(0);
/** Aggregation progress data */
const progressData = signal<{ percent: number; scanned: number; total: number; elapsedMs: number } | null>(null);
/** True when query was canceled */
const canceled = signal(false);
/** Live elapsed milliseconds since query started */
const elapsedMs = signal(0);

/* --- Pagination, view mode, toolbar signals --- */
const page = signal(1);
const pageSize = signal(100);
const viewMode = signal<"table" | "list">("table");
const wrap = signal(false);
const copyTooltip = signal<{ visible: boolean; x: number; y: number }>({ visible: false, x: 0, y: 0 });

/** Maximum events to keep in the live tail buffer */
const TAIL_BUFFER_CAP = 10_000;

/** Module-level getter for the current EditorView -- set by the component */
let getEditorView: (() => import("@codemirror/view").EditorView | null) | null = null;

/** Debounce timer for live explain diagnostics */
let explainDebounceTimer: ReturnType<typeof setTimeout> | undefined;

/** Timer for copy tooltip auto-hide */
let copyTooltipTimer: ReturnType<typeof setTimeout> | undefined;

/** Current AbortController for the active query -- null when idle */
let activeAbortController: AbortController | null = null;
/** SSE cleanup function for aggregation job progress */
let jobProgressCleanup: (() => void) | null = null;
/** Elapsed timer interval ID */
let elapsedTimerId: ReturnType<typeof setInterval> | null = null;
/** Monotonic query counter to discard stale responses (Pitfall 3) */
let queryGeneration = 0;

// ---------------------------------------------------------------------------
// Streaming helpers
// ---------------------------------------------------------------------------

function startElapsedTimer() {
  const startTime = performance.now();
  elapsedMs.value = 0;
  elapsedTimerId = setInterval(() => {
    elapsedMs.value = performance.now() - startTime;
  }, 100);
}

function stopElapsedTimer() {
  if (elapsedTimerId !== null) {
    clearInterval(elapsedTimerId);
    elapsedTimerId = null;
  }
}

function cleanupActiveQuery() {
  if (jobProgressCleanup) { jobProgressCleanup(); jobProgressCleanup = null; }
  stopElapsedTimer();
  activeAbortController = null;
}

/** Regex heuristic for detecting aggregation queries (Pitfall 7). */
const AGG_PATTERN = /\|\s*(stats|timechart|top|rare|chart|eventstats|streamstats)\b/i;
function detectResultType(q: string): "events" | "aggregate" {
  return AGG_PATTERN.test(q) ? "aggregate" : "events";
}

function resultCount(r: QueryResult | null): number {
  if (!r) return 0;
  if (r.type === "events") return r.events.length;
  return r.rows.length;
}

/** Derive columns from a QueryResult (used by export) */
function deriveColumns(r: QueryResult): string[] {
  if (r.type === "events") {
    const evts = (r as EventsResult).events;
    const keySet = new Set<string>();
    const limit = Math.min(evts.length, 100);
    for (let i = 0; i < limit; i++) {
      for (const key of Object.keys(evts[i])) {
        keySet.add(key);
      }
    }
    const priority = ["_time", "_raw", "_source", "source"];
    const ordered: string[] = [];
    for (const p of priority) {
      if (keySet.has(p)) { ordered.push(p); keySet.delete(p); }
    }
    return ordered.concat(Array.from(keySet).sort());
  }
  return (r as AggregateResult).columns;
}

/** Get rows as Record<string, unknown>[] from a QueryResult */
function getResultRows(r: QueryResult): Record<string, unknown>[] {
  if (r.type === "events") return (r as EventsResult).events;
  const agg = r as AggregateResult;
  return agg.rows.map((data) => {
    const row: Record<string, unknown> = {};
    for (let c = 0; c < agg.columns.length; c++) {
      row[agg.columns[c]] = data[c];
    }
    return row;
  });
}

/**
 * Post-query side effects: push history, update URL hash, clear diagnostics,
 * fetch histogram/explain/fields. Extracted so both sync and streaming paths
 * can call it after query completion.
 */
function runPostQueryEffects(
  q: string,
  fromVal: string,
  toVal: string | undefined,
  pg: number,
  sz: number,
): void {
  hasQueried.value = true;

  pushHistory(q);
  writeQueryToHash(q, fromVal, toVal, pg, sz);

  const view = getEditorView?.();
  if (view) clearEditorDiagnostics(view);

  // Fetch grouped histogram (with ungrouped fallback) and explain in
  // parallel after query succeeds. Non-blocking -- failures ignored.
  // If the grouped response contains no known level keys, fall through
  // to the ungrouped single-color display so the legend stays clean.
  fetchHistogramGrouped(fromVal, toVal, 60, "level")
    .then((histResult) => {
      if (histResult.buckets.length > 0 && hasKnownLevels(histResult.buckets)) {
        groupedBuckets.value = histResult.buckets;
        timelineBuckets.value = [];
      } else {
        // No known level keys — fall through to ungrouped display
        groupedBuckets.value = [];
        return fetchHistogram(fromVal, toVal, 60).then((h) => {
          timelineBuckets.value = h.buckets;
        });
      }
    })
    .catch(() => {
      fetchHistogram(fromVal, toVal, 60)
        .then((histResult) => {
          timelineBuckets.value = histResult.buckets;
          groupedBuckets.value = [];
        })
        .catch(() => { /* non-critical */ });
    });

  fetchExplain(q, fromVal, toVal)
    .then((explain) => { explainResult.value = explain; })
    .catch(() => { /* non-critical */ });

  fetchFields()
    .then((fields) => {
      catalogFields.value = fields;
      const m = new Map<string, string>();
      for (const f of fields) m.set(f.name, f.type);
      fieldTypeMap.value = m;
    })
    .catch(() => { /* non-critical */ });
}

/**
 * Run a query with adaptive sync/streaming execution.
 *
 * Flow: submit hybrid query (200ms sync wait). If fast, instant swap. If slow,
 * switch to NDJSON streaming (search) or SSE progress tracking (aggregation).
 * Previous results stay visible during the initial 200ms wait period.
 *
 * Accepts optional pg/sz params for pagination.
 */
function runQueryAndRefresh(
  q: string,
  fromVal: string,
  toVal: string | undefined,
  pg?: number,
  sz?: number,
): void {
  if (!q || queryActive.value) return;

  const currentPage = pg ?? page.value;
  const currentSize = sz ?? pageSize.value;
  const currentOffset = (currentPage - 1) * currentSize;

  // Increment generation counter to detect stale responses
  queryGeneration++;
  const gen = queryGeneration;

  // Cancel any previous query
  if (activeAbortController) activeAbortController.abort();
  cleanupActiveQuery();

  // Create new AbortController
  const controller = new AbortController();
  activeAbortController = controller;

  // Reset state -- do NOT clear result.value yet (previous results stay during 200ms wait)
  batch(() => {
    queryActive.value = true;
    canceled.value = false;
    streaming.value = false;
    streamingCount.value = 0;
    progressData.value = null;
    error.value = null;
    explainOpen.value = false;
  });

  // Start elapsed timer
  startElapsedTimer();

  // Detect result type for choosing streaming vs progress path
  const resultType = detectResultType(q);

  submitHybridQuery(q, fromVal, toVal, currentSize, currentOffset, controller.signal)
    .then((hybrid) => {
      // Discard stale responses
      if (gen !== queryGeneration) return;

      if (hybrid.status === "sync") {
        // FAST PATH: query completed within 200ms -- instant swap
        batch(() => {
          result.value = hybrid.syncResult!.result;
          stats.value = hybrid.syncResult!.stats;
          loading.value = false;
          queryActive.value = false;
        });
        stopElapsedTimer();
        elapsedMs.value = hybrid.syncResult!.stats.took_ms;
        runPostQueryEffects(q, fromVal, toVal, currentPage, currentSize);
        cleanupActiveQuery();
        return;
      }

      // SLOW PATH: query is async -- clear previous results
      batch(() => {
        result.value = null;
        stats.value = null;
        selectedEvent.value = null;
      });

      if (resultType === "events") {
        // --- NDJSON streaming for search queries ---
        streaming.value = true;
        streamingCount.value = 0;
        const rows: Record<string, unknown>[] = [];
        let streamMeta: { total?: number; scanned?: number; took_ms?: number } = {};

        streamQuery(q, fromVal, toVal, {
          onRow(row) {
            rows.push(row);
            // Only keep up to pageSize rows for display
            if (rows.length <= currentSize) {
              batch(() => {
                streamingCount.value = rows.length;
                // Update result incrementally for live display
                result.value = {
                  type: "events",
                  events: rows.slice(0, currentSize),
                  total: rows.length,
                  has_more: false,
                } satisfies EventsResult;
              });
            } else {
              streamingCount.value = rows.length;
            }
          },
          onMeta(meta) {
            streamMeta = meta;
          },
          onError(message) {
            error.value = message;
          },
        }, controller.signal, currentSize)
          .then(() => {
            if (gen !== queryGeneration) return;
            batch(() => {
              result.value = {
                type: "events",
                events: rows.slice(0, currentSize),
                total: streamMeta.total ?? rows.length,
                has_more: (streamMeta.total ?? rows.length) > currentSize,
              } satisfies EventsResult;
              stats.value = {
                took_ms: streamMeta.took_ms ?? elapsedMs.value,
                scanned: streamMeta.scanned ?? 0,
              };
              streaming.value = false;
              queryActive.value = false;
              loading.value = false;
            });
            stopElapsedTimer();
            runPostQueryEffects(q, fromVal, toVal, currentPage, currentSize);
            cleanupActiveQuery();
          })
          .catch((err: unknown) => {
            if (gen !== queryGeneration) return;
            if (err instanceof DOMException && err.name === "AbortError") {
              // Cancel: keep partial rows
              batch(() => {
                canceled.value = true;
                streaming.value = false;
                queryActive.value = false;
                loading.value = false;
              });
              stopElapsedTimer();
              cleanupActiveQuery();
              return;
            }
            const message = err instanceof Error ? err.message : "Unknown error";
            batch(() => {
              error.value = message;
              streaming.value = false;
              queryActive.value = false;
              loading.value = false;
            });
            stopElapsedTimer();
            cleanupActiveQuery();
          });
      } else {
        // --- SSE progress for aggregation queries ---
        loading.value = true;
        const jobId = hybrid.jobId;
        if (!jobId) {
          batch(() => {
            error.value = "No job ID returned for async query";
            loading.value = false;
            queryActive.value = false;
          });
          stopElapsedTimer();
          cleanupActiveQuery();
          return;
        }

        const unsubscribe = subscribeJobProgress(
          jobId,
          (p) => {
            // onProgress
            if (gen !== queryGeneration) return;
            progressData.value = {
              percent: p.percent,
              scanned: p.scanned,
              total: p.segments_total ?? 0,
              elapsedMs: p.elapsed_ms,
            };
          },
          (data: unknown) => {
            // onComplete -- parse result
            // The SSE complete event sends QueryResult directly (not wrapped in {data, meta}).
            if (gen !== queryGeneration) return;
            batch(() => {
              result.value = (data as QueryResult) ?? null;
              stats.value = {
                took_ms: elapsedMs.value,
                scanned: 0,
                query_id: jobId,
              };
              progressData.value = null;
              queryActive.value = false;
              loading.value = false;
            });
            stopElapsedTimer();
            runPostQueryEffects(q, fromVal, toVal, currentPage, currentSize);
            cleanupActiveQuery();
          },
          (message: string) => {
            // onFailed
            if (gen !== queryGeneration) return;
            batch(() => {
              error.value = message;
              progressData.value = null;
              queryActive.value = false;
              loading.value = false;
            });
            stopElapsedTimer();
            cleanupActiveQuery();
          },
          () => {
            // onCanceled
            if (gen !== queryGeneration) return;
            batch(() => {
              canceled.value = true;
              result.value = null;
              progressData.value = null;
              queryActive.value = false;
              loading.value = false;
            });
            stopElapsedTimer();
            cleanupActiveQuery();
          },
        );

        jobProgressCleanup = unsubscribe;
      }
    })
    .catch((err: unknown) => {
      if (gen !== queryGeneration) return;
      if (err instanceof DOMException && err.name === "AbortError") {
        // Cancel during hybrid submit phase
        batch(() => {
          canceled.value = true;
          queryActive.value = false;
          loading.value = false;
        });
        stopElapsedTimer();
        cleanupActiveQuery();
        return;
      }
      const message = err instanceof Error ? err.message : "Unknown error";
      batch(() => {
        error.value = message;
        queryActive.value = false;
        loading.value = false;
      });
      stopElapsedTimer();

      // On failure, fetch explain to show diagnostics in the editor
      const view = getEditorView?.();
      if (view) {
        fetchExplain(q, fromVal, toVal)
          .then((explain) => {
            if (!explain.is_valid) {
              dispatchDiagnostics(view, q, explain);
            }
          })
          .catch(() => { /* non-critical */ });
      }

      cleanupActiveQuery();
    });
}

/** Cancel the currently running query. */
function handleCancelQuery() {
  if (!activeAbortController) return;
  activeAbortController.abort();
  // For aggregation jobs, fire-and-forget the server-side cancel
  if (jobProgressCleanup) {
    // The abort will trigger onCanceled via SSE or the catch block
  }
  cleanupActiveQuery();
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

  // Set up module-level editor view getter so runQueryAndRefresh can access it
  getEditorView = () => editorHandleRef.current?.getView() ?? null;

  const handleQueryChange = useCallback((value: string) => {
    query.value = value;

    // Debounced explain for live inline diagnostics (500ms after typing stops)
    clearTimeout(explainDebounceTimer);
    if (value.trim()) {
      explainDebounceTimer = setTimeout(() => {
        const view = getEditorView?.();
        if (!view) return;
        fetchExplain(value, from.value, to.value)
          .then((explain) => {
            if (!explain.is_valid) {
              dispatchDiagnostics(view, value, explain);
            } else {
              clearEditorDiagnostics(view);
            }
          })
          .catch(() => { /* non-critical */ });
      }, 500);
    } else {
      // Clear diagnostics when query is empty
      const view = getEditorView?.();
      if (view) clearEditorDiagnostics(view);
    }
  }, []);

  const handleExecute = useCallback(() => {
    if (tailActive.value) return; // block while tailing
    // Ctrl+Enter while running -> cancel (dual behavior)
    if (queryActive.value) {
      handleCancelQuery();
      return;
    }
    // Reset to page 1 on new query execution (Pitfall 5)
    page.value = 1;
    runQueryAndRefresh(query.value.trim(), from.value, to.value, 1, pageSize.value);
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
    histogramBrushed.value = true;

    page.value = 1;
    runQueryAndRefresh(query.value.trim(), from.value, to.value, 1, pageSize.value);
  }, []);

  const handleHistogramReset = useCallback(() => {
    from.value = "-1h";
    to.value = undefined;
    histogramBrushed.value = false;
    page.value = 1;
    runQueryAndRefresh(query.value.trim(), from.value, to.value, 1, pageSize.value);
  }, []);

  /* --- Sort handler --- */
  const handleSort = useCallback((newQuery: string) => {
    query.value = newQuery;
    page.value = 1; // Reset to page 1 on sort change
    runQueryAndRefresh(newQuery, from.value, to.value, 1, pageSize.value);

    // Update editor content
    const view = getEditorView?.();
    if (view) {
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: newQuery },
      });
    }
  }, []);

  /* --- Filter handler (from EventDetail [+]/[-] buttons) --- */
  const handleFilter = useCallback((field: string, value: string, exclude: boolean) => {
    const newQuery = appendFilter(query.value, field, value, exclude);
    query.value = newQuery;
    page.value = 1; // Reset to page 1 on filter change (Pitfall 6)

    // Update editor content to show the new query
    const view = getEditorView?.();
    if (view) {
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: newQuery },
      });
    }

    runQueryAndRefresh(newQuery, from.value, to.value, 1, pageSize.value);
  }, []);

  /* --- Pagination handlers --- */
  const handlePageChange = useCallback((newPage: number) => {
    page.value = newPage;
    runQueryAndRefresh(query.value.trim(), from.value, to.value, newPage, pageSize.value);
  }, []);

  const handlePageSizeChange = useCallback((newSize: number) => {
    pageSize.value = newSize;
    page.value = 1; // Reset to first page
    runQueryAndRefresh(query.value.trim(), from.value, to.value, 1, newSize);
  }, []);

  /* --- View mode and wrap handlers --- */
  const handleViewModeChange = useCallback((mode: "table" | "list") => {
    viewMode.value = mode;
  }, []);

  const handleWrapChange = useCallback((w: boolean) => {
    wrap.value = w;
  }, []);

  /* --- Cell copy handler --- */
  const handleCellCopy = useCallback((value: string, x: number, y: number) => {
    navigator.clipboard.writeText(value).then(() => {
      clearTimeout(copyTooltipTimer);
      copyTooltip.value = { visible: true, x, y };
      copyTooltipTimer = setTimeout(() => {
        copyTooltip.value = { visible: false, x: 0, y: 0 };
      }, 1500);
    });
  }, []);

  /* --- Export handler --- */
  const handleExport = useCallback(async (format: "csv" | "json", scope: "page" | "all") => {
    let rows: Record<string, unknown>[];
    let columns: string[];

    if (scope === "page") {
      // Use current result data
      const r = result.value;
      if (!r) return;
      columns = deriveColumns(r);
      rows = getResultRows(r);
    } else {
      // Fetch all results via streaming endpoint
      try {
        const resp = await fetch("/api/v1/query/stream", {
          method: "POST",
          headers: { "Content-Type": "application/json", ...authHeaders() },
          body: JSON.stringify({ q: query.value, from: from.value, to: to.value }),
        });
        if (!resp.ok) {
          // Fallback to current page data
          const r = result.value;
          if (!r) return;
          columns = deriveColumns(r);
          rows = getResultRows(r);
        } else {
          const text = await resp.text();
          rows = text.trim().split("\n").filter(Boolean).map((line) => JSON.parse(line));
          if (rows.length > 0) {
            const keySet = new Set<string>();
            for (const row of rows.slice(0, 100)) {
              for (const key of Object.keys(row)) keySet.add(key);
            }
            const priority = ["_time", "_raw", "_source", "source"];
            const ordered: string[] = [];
            for (const p of priority) {
              if (keySet.has(p)) { ordered.push(p); keySet.delete(p); }
            }
            columns = ordered.concat(Array.from(keySet).sort());
          } else {
            return;
          }
        }
      } catch {
        // On network error, fallback to current page
        const r = result.value;
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
      tailReconnecting.value = false;
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
      onReconnecting(isReconnecting: boolean) {
        tailReconnecting.value = isReconnecting;
      },
    });

    tailCleanupRef.current = cleanup;
  }, []);

  /** Toggle the explain inspector panel */
  const handleExplainToggle = useCallback(() => {
    explainOpen.value = !explainOpen.value;
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
    onClosePanel: () => {
      // Layered close: event detail > explain inspector > blur editor
      if (selectedEvent.value) { selectedEvent.value = null; return; }
      if (explainOpen.value) { explainOpen.value = false; return; }
      editorHandleRef.current?.getView()?.contentDOM.blur();
    },
    onOpenPalette: () => {
      helpOverlayOpen.value = false; // Close help if open (Pitfall 7)
      paletteOpen.value = !paletteOpen.value;
    },
    onOpenHelp: () => {
      paletteOpen.value = false; // Close palette if open (Pitfall 7)
      helpOverlayOpen.value = !helpOverlayOpen.value;
    },
  });

  // Watch for queries loaded from the command palette
  useEffect(() => {
    return effect(() => {
      const q = paletteQuery.value;
      if (!q) return;
      paletteQuery.value = null;
      query.value = q;
      // Update editor content
      const view = getEditorView?.();
      if (view) {
        view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: q } });
      }
      page.value = 1;
      runQueryAndRefresh(q, from.value, to.value, 1, pageSize.value);
    });
  }, []);

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

  // Cleanup SSE and streaming on unmount
  useEffect(() => {
    return () => {
      if (tailCleanupRef.current) {
        tailCleanupRef.current();
        tailCleanupRef.current = null;
      }
      // Streaming/progress cleanup
      if (activeAbortController) activeAbortController.abort();
      cleanupActiveQuery();
    };
  }, []);

  // Fetch indexes, views, and field catalog on mount for the flow sidebar
  useEffect(() => {
    Promise.allSettled([fetchIndexes(), fetchViews(), fetchFields()])
      .then(([idx, views, fields]) => {
        if (idx.status === "fulfilled") sidebarIndexes.value = idx.value;
        if (views.status === "fulfilled") sidebarViews.value = views.value;
        if (fields.status === "fulfilled") {
          catalogFields.value = fields.value;
          const m = new Map<string, string>();
          for (const f of fields.value) {
            m.set(f.name, f.type);
          }
          fieldTypeMap.value = m;
        }
      });
  }, []);

  // Restore query, time range, and pagination from URL hash on mount (Pitfall 4: defer execution)
  useEffect(() => {
    const hashData = readQueryFromHash();
    if (hashData) {
      query.value = hashData.q;
      from.value = hashData.from || "-1h";
      to.value = hashData.to;
      if (hashData.page) page.value = hashData.page;
      if (hashData.size) pageSize.value = hashData.size;
      // Defer execution to ensure editor has rendered
      setTimeout(() => {
        runQueryAndRefresh(hashData.q, from.value, to.value, page.value, pageSize.value);
      }, 0);
    }
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
  const showInitialEmpty = !tailActive.value && !hasQueried.value && !loading.value && !queryActive.value && !error.value;
  const showNoResults = !tailActive.value && hasQueried.value && !loading.value && !queryActive.value && !error.value && !canceled.value && resultCount(result.value) === 0;

  // Compute total count for pagination and toolbar
  const totalCount = activeResult
    ? (activeResult.type === "events" ? activeResult.total : activeResult.rows.length)
    : 0;
  const pageCount = resultCount(activeResult);
  const hasResults = activeResult && pageCount > 0 && !tailActive.value;

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
          class={`${styles.runBtn}${queryActive.value ? ` ${styles.cancelBtn}` : ""}`}
          onClick={handleExecute}
          disabled={tailActive.value}
          aria-label={queryActive.value ? "Cancel query" : "Run query"}
          title={queryActive.value
            ? `Cancel query (${formatShortcut(SHORTCUTS.runQuery)})`
            : `Run query (${formatShortcut(SHORTCUTS.runQuery)})`}
        >
          {queryActive.value ? "\u25A0" : "\u25B6"}
        </button>
        <LiveTailButton
          active={tailActive.value}
          onToggle={handleTailToggle}
        />
        <TimeRangePicker from={from} to={to} onApply={() => {
          if (!tailActive.value) {
            histogramBrushed.value = false; // Reset brush state on manual time change
            page.value = 1; // Reset to page 1 on time range change
            runQueryAndRefresh(query.value.trim(), from.value, to.value, 1, pageSize.value);
          }
        }} />
      </div>

      <div class={styles.body}>
        <FlowSidebar
          visible={sidebarVisible.value}
          indexes={sidebarIndexes.value}
          views={sidebarViews.value}
          explainResult={explainResult.value}
          fieldTypes={fieldTypeMap.value}
          selectedFields={activeResult ? deriveColumns(activeResult) : []}
          catalogFields={catalogFields.value}
          onFilter={handleFilter}
          onToggle={handleSidebarToggle}
          onSelectSource={handleSetSource}
          onInsertCommand={handleInsertCommand}
        />

        <div class={styles.mainContent}>
          <Timeline
            from={from.value}
            to={to.value}
            buckets={timelineBuckets.value}
            groupedBuckets={groupedBuckets.value}
            visible={hasQueried.value && !tailActive.value}
            onBrush={handleTimelineBrush}
            onReset={handleHistogramReset}
            showReset={histogramBrushed.value}
          />

          <QueryStatsBar
            stats={stats.value}
            loading={loading.value}
            error={error.value}
            resultCount={tailActive.value ? tailEvents.value.length : resultCount(result.value)}
            tailActive={tailActive.value}
            tailEventCount={tailEvents.value.length}
            tailCatchupDone={tailCatchupDone.value}
            streaming={streaming.value}
            streamingCount={streamingCount.value}
            progress={progressData.value}
            canceled={canceled.value}
            elapsedMs={elapsedMs.value}
            onExplainToggle={handleExplainToggle}
            explainAvailable={!!(explainResult.value?.is_valid && explainResult.value?.parsed)}
            tailReconnecting={tailReconnecting.value}
          />

          {explainOpen.value && explainResult.value?.is_valid && explainResult.value?.parsed && (
            <ExplainInspector
              explain={explainResult.value}
              stats={stats.value}
            />
          )}

          {/* Table toolbar -- only show when results exist */}
          {hasResults && (
            <TableToolbar
              viewMode={viewMode.value}
              onViewModeChange={handleViewModeChange}
              wrap={wrap.value}
              onWrapChange={handleWrapChange}
              onExport={handleExport}
              totalCount={totalCount}
              pageCount={pageCount}
            />
          )}

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
              viewMode.value === "table" ? (
                <ResultsTable
                  result={activeResult}
                  onRowClick={handleRowClick}
                  selectedRow={selectedEvent.value}
                  onSort={handleSort}
                  currentQuery={query.value}
                  wrap={wrap.value}
                  onCellCopy={handleCellCopy}
                />
              ) : (
                <ListView
                  result={activeResult}
                  onRowClick={handleRowClick}
                  selectedRow={selectedEvent.value}
                  onCellCopy={handleCellCopy}
                />
              )
            )}
            <EventDetail
              event={selectedEvent.value}
              onClose={handleCloseDetail}
              onFilter={handleFilter}
            />
          </div>

          {/* Pagination bar -- only show for non-tail, non-empty results */}
          {hasResults && (
            <PaginationBar
              page={page.value}
              pageSize={pageSize.value}
              total={totalCount}
              onPageChange={handlePageChange}
              onPageSizeChange={handlePageSizeChange}
            />
          )}
        </div>
      </div>

      {/* Copy tooltip */}
      <CopyTooltip
        visible={copyTooltip.value.visible}
        x={copyTooltip.value.x}
        y={copyTooltip.value.y}
      />
    </div>
  );
}
