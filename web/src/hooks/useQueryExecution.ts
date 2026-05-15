/**
 * Hook that encapsulates the full query execution lifecycle:
 * submit hybrid query, track progress via SSE, handle cancel,
 * manage elapsed timer, post-query side effects, and stale-response guard.
 *
 * All imperative resources (AbortController, timers, generation counter)
 * are per-mount refs with proper useEffect cleanup.
 */

import { useCallback, useEffect, useRef } from "react";
import type { QueryEditorHandle } from "../editor/QueryEditor";
import {
  fetchHistogram,
  fetchHistogramGrouped,
  fetchExplain,
  fetchFields,
} from "../api/client";
import {
  submitHybridQuery,
  subscribeJobProgress,
  cancelJob,
} from "../api/streaming";
import { pushHistory } from "../stores/queryHistory";
import { useSearchStore } from "../stores/search";
import { writeQueryToHash } from "../stores/queryUrl";
// Diagnostics module is loaded lazily to avoid pulling @codemirror/lint
// into the entry chunk's static import graph (codemirror is a dynamic chunk).
type DiagnosticsModule = typeof import("../editor/diagnostics");
let _diagnostics: DiagnosticsModule | null = null;
async function getDiagnostics(): Promise<DiagnosticsModule> {
  if (!_diagnostics) {
    _diagnostics = await import("../editor/diagnostics");
  }
  return _diagnostics;
}
import type {
  QueryResult,
  EventsResult,
  HistogramBucketGrouped,
} from "../api/client";

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

/** Shorthand for imperative store access */
const ss = useSearchStore;

export interface UseQueryExecutionOptions {
  editorHandleRef: React.RefObject<QueryEditorHandle | null>;
}

export function useQueryExecution({ editorHandleRef }: UseQueryExecutionOptions) {
  // --- Per-mount refs for imperative resources (BUG-01/02/03) ---
  const activeAbortControllerRef = useRef<AbortController | null>(null);
  const jobProgressCleanupRef = useRef<(() => void) | null>(null);
  const elapsedTimerIdRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const queryGenerationRef = useRef(0);
  const explainDebounceTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const postQueryEffectsTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  const copyTooltipTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);
  /** Tracks the async job ID for server-side cancel (BUG-05) */
  const activeJobIdRef = useRef<string | null>(null);

  /** Per-mount getter for the current EditorView */
  const getEditorView = useCallback(() => {
    return editorHandleRef.current?.getView() ?? null;
  }, [editorHandleRef]);

  // --- Timer helpers ---

  const startElapsedTimer = useCallback(() => {
    const startTime = performance.now();
    ss.setState({ elapsedMs: 0 });
    elapsedTimerIdRef.current = setInterval(() => {
      ss.setState({ elapsedMs: performance.now() - startTime });
    }, 100);
  }, []);

  const stopElapsedTimer = useCallback(() => {
    if (elapsedTimerIdRef.current !== null) {
      clearInterval(elapsedTimerIdRef.current);
      elapsedTimerIdRef.current = null;
    }
  }, []);

  const cleanupActiveQuery = useCallback(() => {
    if (jobProgressCleanupRef.current) {
      jobProgressCleanupRef.current();
      jobProgressCleanupRef.current = null;
    }
    stopElapsedTimer();
    activeAbortControllerRef.current = null;
    activeJobIdRef.current = null;
  }, [stopElapsedTimer]);

  // --- Post-query side effects ---

  const runPostQueryEffects = useCallback(
    (
      q: string,
      fromVal: string,
      toVal: string | undefined,
      pg: number,
      sz: number,
    ): void => {
      ss.setState({ hasQueried: true });

      pushHistory(q);
      writeQueryToHash(q, fromVal, toVal, pg, sz);

      const view = getEditorView();
      if (view) getDiagnostics().then((d) => d.clearEditorDiagnostics(view));

      fetchHistogramGrouped(fromVal, toVal, 60, "level")
        .then((histResult) => {
          if (
            histResult.buckets.length > 0 &&
            hasKnownLevels(histResult.buckets)
          ) {
            ss.setState({
              groupedBuckets: histResult.buckets,
              timelineBuckets: [],
            });
          } else {
            ss.setState({ groupedBuckets: [] });
            return fetchHistogram(fromVal, toVal, 60).then((h) => {
              ss.setState({ timelineBuckets: h.buckets });
            });
          }
        })
        .catch(() => {
          fetchHistogram(fromVal, toVal, 60)
            .then((histResult) => {
              ss.setState({
                timelineBuckets: histResult.buckets,
                groupedBuckets: [],
              });
            })
            .catch(() => {
              /* non-critical */
            });
        });

      fetchExplain(q, fromVal, toVal)
        .then((explain) => {
          ss.setState({ explainResult: explain });
        })
        .catch(() => {
          /* non-critical */
        });

      fetchFields()
        .then((fields) => {
          const m = new Map<string, string>();
          for (const f of fields) m.set(f.name, f.type);
          ss.setState({ catalogFields: fields, fieldTypeMap: m });
        })
        .catch(() => {
          /* non-critical */
        });
    },
    [getEditorView],
  );

  const runPostQueryEffectsDebounced = useCallback(
    (
      q: string,
      fromVal: string,
      toVal: string | undefined,
      pg: number,
      sz: number,
    ): void => {
      clearTimeout(postQueryEffectsTimerRef.current);
      postQueryEffectsTimerRef.current = setTimeout(() => {
        runPostQueryEffects(q, fromVal, toVal, pg, sz);
      }, 300);
    },
    [runPostQueryEffects],
  );

  // --- Core query execution ---

  const runQueryAndRefresh = useCallback(
    (
      q: string,
      fromVal: string,
      toVal: string | undefined,
      pg?: number,
      sz?: number,
    ): void => {
      const state = ss.getState();
      if (!q || state.queryActive) return;

      const currentPage = pg ?? state.page;
      const currentSize = sz ?? state.pageSize;
      const currentOffset = (currentPage - 1) * currentSize;

      // Increment generation counter to detect stale responses
      queryGenerationRef.current++;
      const gen = queryGenerationRef.current;

      // Cancel any previous query
      if (activeAbortControllerRef.current)
        activeAbortControllerRef.current.abort();
      cleanupActiveQuery();

      const controller = new AbortController();
      activeAbortControllerRef.current = controller;

      // Reset state -- do NOT clear result yet (previous results stay during 200ms wait)
      ss.setState({
        queryActive: true,
        canceled: false,
        streaming: false,
        streamingCount: 0,
        progressData: null,
        error: null,
        explainOpen: false,
      });

      // Start elapsed timer
      startElapsedTimer();

      submitHybridQuery(
        q,
        fromVal,
        toVal,
        currentSize,
        currentOffset,
        controller.signal,
      )
        .then((hybrid) => {
          // Discard stale responses
          if (gen !== queryGenerationRef.current) return;

          if (hybrid.status === "sync") {
            // FAST PATH: query completed within 200ms -- instant swap
            ss.setState({
              result: hybrid.syncResult!.result,
              stats: hybrid.syncResult!.stats,
              loading: false,
              queryActive: false,
            });
            stopElapsedTimer();
            ss.setState({ elapsedMs: hybrid.syncResult!.stats.took_ms });
            runPostQueryEffects(q, fromVal, toVal, currentPage, currentSize);
            cleanupActiveQuery();
            return;
          }

          // SLOW PATH: query is async -- clear stats immediately; results cleared lazily on first row
          ss.setState({ stats: null });

          ss.setState({ loading: true });
          startElapsedTimer();
          const jobId = hybrid.jobId;
          if (!jobId) {
            ss.setState({
              error: "No job ID returned for async query",
              loading: false,
              queryActive: false,
            });
            stopElapsedTimer();
            cleanupActiveQuery();
            return;
          }

          // Persist job ID for server-side cancel (BUG-05)
          activeJobIdRef.current = jobId;

          const unsubscribe = subscribeJobProgress(
            jobId,
            (p) => {
              // onProgress
              if (gen !== queryGenerationRef.current) return;
              const updates: Partial<ReturnType<typeof ss.getState>> = {
                progressData: {
                  percent: p.percent,
                  scanned: p.scanned,
                  total: p.segments_total ?? 0,
                  elapsedMs: p.elapsed_ms,
                },
              };

              // Render preview rows while query is running
              if (p.preview && p.preview.length > 0) {
                updates.result = {
                  type: "events",
                  events: p.preview,
                  total: p.preview.length,
                  has_more: true,
                } satisfies EventsResult;
                updates.isPreview = true;
              }
              ss.setState(updates);
            },
            (data: unknown) => {
              // onComplete
              if (gen !== queryGenerationRef.current) return;
              const payload = data as
                | { data: QueryResult; meta?: Record<string, unknown> }
                | QueryResult;
              const queryResult: QueryResult =
                payload &&
                typeof payload === "object" &&
                "data" in payload &&
                "meta" in payload
                  ? (payload as { data: QueryResult }).data
                  : (payload as QueryResult);
              const metaStats =
                payload && typeof payload === "object" && "meta" in payload
                  ? (payload as { meta: Record<string, unknown> }).meta
                  : undefined;
              const detailedStats = metaStats?.stats as
                | Record<string, unknown>
                | undefined;

              ss.setState({
                result: queryResult ?? null,
                stats: {
                  took_ms:
                    (metaStats?.took_ms as number) ?? ss.getState().elapsedMs,
                  scanned: (metaStats?.scanned as number) ?? 0,
                  query_id: jobId,
                  stats: detailedStats
                    ? {
                        segments_total:
                          (detailedStats.segments_total as number) ?? 0,
                        segments_scanned:
                          (detailedStats.segments_scanned as number) ?? 0,
                        segments_skipped_bf:
                          (detailedStats.segments_skipped_bloom as number) ?? 0,
                        rows_scanned:
                          (detailedStats.rows_scanned as number) ?? 0,
                        took_ms:
                          (metaStats?.took_ms as number) ??
                          ss.getState().elapsedMs,
                      }
                    : undefined,
                },
                progressData: null,
                queryActive: false,
                loading: false,
                isPreview: false,
              });
              stopElapsedTimer();
              runPostQueryEffectsDebounced(
                q,
                fromVal,
                toVal,
                currentPage,
                currentSize,
              );
              cleanupActiveQuery();
            },
            (message: string) => {
              // onFailed
              if (gen !== queryGenerationRef.current) return;
              ss.setState({
                error: message,
                progressData: null,
                queryActive: false,
                loading: false,
                isPreview: false,
              });
              stopElapsedTimer();
              cleanupActiveQuery();
            },
            () => {
              // onCanceled
              if (gen !== queryGenerationRef.current) return;
              ss.setState({
                canceled: true,
                result: null,
                progressData: null,
                queryActive: false,
                loading: false,
                isPreview: false,
              });
              stopElapsedTimer();
              cleanupActiveQuery();
            },
          );

          jobProgressCleanupRef.current = unsubscribe;
        })
        .catch((err: unknown) => {
          if (gen !== queryGenerationRef.current) return;
          if (err instanceof DOMException && err.name === "AbortError") {
            // Cancel during hybrid submit phase
            ss.setState({
              canceled: true,
              queryActive: false,
              loading: false,
            });
            stopElapsedTimer();
            cleanupActiveQuery();
            return;
          }
          const message = err instanceof Error ? err.message : "Unknown error";
          ss.setState({
            error: message,
            queryActive: false,
            loading: false,
          });
          stopElapsedTimer();

          // On failure, fetch explain to show diagnostics in the editor
          const view = getEditorView();
          if (view) {
            fetchExplain(q, fromVal, toVal)
              .then(async (explain) => {
                if (!explain.is_valid) {
                  const d = await getDiagnostics();
                  d.dispatchDiagnostics(view, q, explain);
                }
              })
              .catch(() => {
                /* non-critical */
              });
          }

          cleanupActiveQuery();
        });
    },
    [
      cleanupActiveQuery,
      getEditorView,
      runPostQueryEffects,
      runPostQueryEffectsDebounced,
      startElapsedTimer,
      stopElapsedTimer,
    ],
  );

  // --- Cancel handler (BUG-05: actual server cancel) ---

  const handleCancelQuery = useCallback(() => {
    if (!activeAbortControllerRef.current) return;
    activeAbortControllerRef.current.abort();

    // Fire-and-forget server-side cancel for the async job (BUG-05)
    const jobId = activeJobIdRef.current;
    if (jobId) {
      cancelJob(jobId).catch(() => {});
    }

    cleanupActiveQuery();
  }, [cleanupActiveQuery]);

  // --- Query change handler with debounced explain ---

  const handleQueryChange = useCallback(
    (value: string) => {
      ss.setState({ query: value });

      // Debounced explain for live inline diagnostics (500ms after typing stops)
      clearTimeout(explainDebounceTimerRef.current);
      if (value.trim()) {
        explainDebounceTimerRef.current = setTimeout(() => {
          const view = getEditorView();
          if (!view) return;
          const { from: f, to: t } = ss.getState();
          fetchExplain(value, f, t)
            .then(async (explain) => {
              const d = await getDiagnostics();
              if (!explain.is_valid) {
                d.dispatchDiagnostics(view, value, explain);
              } else {
                d.clearEditorDiagnostics(view);
              }
            })
            .catch(() => {
              /* non-critical */
            });
        }, 500);
      } else {
        // Clear diagnostics when query is empty
        const view = getEditorView();
        if (view) getDiagnostics().then((d) => d.clearEditorDiagnostics(view));
      }
    },
    [getEditorView],
  );

  // --- Execute handler ---

  const handleExecute = useCallback(() => {
    const state = ss.getState();
    if (state.tailActive) return; // block while tailing
    // Ctrl+Enter while running -> cancel (dual behavior)
    if (state.queryActive) {
      handleCancelQuery();
      return;
    }
    // Reset to page 1 on new query execution
    ss.setState({ page: 1 });
    runQueryAndRefresh(
      state.query.trim(),
      state.from,
      state.to,
      1,
      state.pageSize,
    );
  }, [handleCancelQuery, runQueryAndRefresh]);

  // --- Cell copy handler ---

  const handleCellCopy = useCallback(
    (value: string, x: number, y: number) => {
      navigator.clipboard.writeText(value).then(() => {
        clearTimeout(copyTooltipTimerRef.current);
        ss.setState({ copyTooltip: { visible: true, x, y } });
        copyTooltipTimerRef.current = setTimeout(() => {
          ss.setState({ copyTooltip: { visible: false, x: 0, y: 0 } });
        }, 1500);
      });
    },
    [],
  );

  // --- Cleanup on unmount (BUG-01/02/03) ---

  useEffect(() => {
    return () => {
      // Abort any active query
      if (activeAbortControllerRef.current) {
        activeAbortControllerRef.current.abort();
      }
      // Clean up job progress SSE
      if (jobProgressCleanupRef.current) {
        jobProgressCleanupRef.current();
        jobProgressCleanupRef.current = null;
      }
      // Clear ALL timers
      if (elapsedTimerIdRef.current !== null) {
        clearInterval(elapsedTimerIdRef.current);
        elapsedTimerIdRef.current = null;
      }
      clearTimeout(explainDebounceTimerRef.current);
      explainDebounceTimerRef.current = undefined;
      clearTimeout(postQueryEffectsTimerRef.current);
      postQueryEffectsTimerRef.current = undefined;
      clearTimeout(copyTooltipTimerRef.current);
      copyTooltipTimerRef.current = undefined;
      // Null the controller ref
      activeAbortControllerRef.current = null;
      activeJobIdRef.current = null;
    };
  }, []);

  return {
    runQueryAndRefresh,
    handleCancelQuery,
    handleQueryChange,
    handleExecute,
    handleCellCopy,
    getEditorView,
  };
}
