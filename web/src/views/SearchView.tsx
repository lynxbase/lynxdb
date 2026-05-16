import { useCallback, useEffect, useRef } from "react";
import type { QueryEditorHandle } from "../editor/QueryEditor";
import { FlowSidebar } from "../components/FlowSidebar";
import { TableToolbar } from "../components/TableToolbar";
import { useKeyboardShortcuts } from "../hooks/useKeyboardShortcuts";
import { useQueryExecution } from "../hooks/useQueryExecution";
import { useLiveTail } from "../hooks/useLiveTail";
import { useExport } from "../hooks/useExport";
import { deriveColumns } from "../utils/deriveColumns";
import {
  fetchIndexes,
  fetchViews,
  fetchFields,
} from "../api/client";
import { useSearchStore } from "../stores/search";
import { useOverlayStore, setPaletteQuery } from "../utils/keyboard";
import { readQueryFromHash } from "../stores/queryUrl";
import { appendFilter } from "../utils/filterQuery";
import type { QueryResult, EventsResult } from "../api/client";
import {
  QueryBar,
  Histogram,
  StatsBar,
  ResultsContainer,
  Pagination,
} from "./search";

function resultCount(r: QueryResult | null): number {
  if (!r) return 0;
  if (r.type === "events") return r.events.length;
  return r.rows.length;
}

interface Props {
  path?: string;
}

/** Shorthand for imperative store access */
const ss = useSearchStore;

export function SearchView(_props: Props) {
  const editorHandleRef = useRef<QueryEditorHandle | null>(null);

  // --- Hooks ---
  const {
    runQueryAndRefresh,
    handleCancelQuery: _handleCancelQuery,
    handleQueryChange,
    handleExecute,
    handleCellCopy,
    getEditorView,
  } = useQueryExecution({ editorHandleRef });

  const { resultsAreaRef, handleTailToggle, handleNewEventsBadgeClick } =
    useLiveTail();

  const { handleExport } = useExport();

  // --- Store selectors (only what the composition root needs) ---
  const sidebarVisible = useSearchStore((s) => s.sidebarVisible);
  const sidebarIndexes = useSearchStore((s) => s.sidebarIndexes);
  const sidebarViews = useSearchStore((s) => s.sidebarViews);
  const explainResult = useSearchStore((s) => s.explainResult);
  const fieldTypeMap = useSearchStore((s) => s.fieldTypeMap);
  const catalogFields = useSearchStore((s) => s.catalogFields);
  const result = useSearchStore((s) => s.result);
  const tailActive = useSearchStore((s) => s.tailActive);
  const tailEvents = useSearchStore((s) => s.tailEvents);
  const viewMode = useSearchStore((s) => s.viewMode);

  // Build activeResult for FlowSidebar selectedFields and TableToolbar
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

  // --- Sidebar toggle ---
  const handleSidebarToggle = useCallback(() => {
    ss.setState((s) => ({ sidebarVisible: !s.sidebarVisible }));
  }, []);

  // --- Sidebar insert command ---
  const handleInsertCommand = useCallback((template: string) => {
    const current = ss.getState().query.trim();
    ss.setState({ query: current ? `${current} ${template}` : template });
    setTimeout(() => {
      editorHandleRef.current?.focus();
    }, 0);
  }, []);

  // --- Sidebar set source ---
  const handleSetSource = useCallback((name: string) => {
    ss.setState({ query: `from ${name} ` });
    setTimeout(() => {
      editorHandleRef.current?.focus();
    }, 0);
  }, []);

  // --- Timeline brush ---
  const handleTimelineBrush = useCallback(
    (fromTs: number, toTs: number) => {
      const newFrom = new Date(fromTs * 1000).toISOString();
      const newTo = new Date(toTs * 1000).toISOString();
      ss.setState({
        from: newFrom,
        to: newTo,
        histogramBrushed: true,
        page: 1,
      });
      const state = ss.getState();
      runQueryAndRefresh(
        state.query.trim(),
        state.from,
        state.to,
        1,
        state.pageSize,
      );
    },
    [runQueryAndRefresh],
  );

  // --- Histogram reset ---
  const handleHistogramReset = useCallback(() => {
    ss.setState({
      from: "-1h",
      to: undefined,
      histogramBrushed: false,
      page: 1,
    });
    const state = ss.getState();
    runQueryAndRefresh(
      state.query.trim(),
      state.from,
      state.to,
      1,
      state.pageSize,
    );
  }, [runQueryAndRefresh]);

  // --- Sort handler ---
  const handleSort = useCallback(
    (newQuery: string) => {
      ss.setState({ query: newQuery, page: 1 });
      const state = ss.getState();
      runQueryAndRefresh(newQuery, state.from, state.to, 1, state.pageSize);

      const view = getEditorView();
      if (view) {
        view.dispatch({
          changes: { from: 0, to: view.state.doc.length, insert: newQuery },
        });
      }
    },
    [runQueryAndRefresh, getEditorView],
  );

  // --- Filter handler (from EventDetail [+]/[-] buttons) ---
  const handleFilter = useCallback(
    (field: string, value: string, exclude: boolean) => {
      const state = ss.getState();
      const newQuery = appendFilter(state.query, field, value, exclude);
      ss.setState({ query: newQuery, page: 1 });

      const view = getEditorView();
      if (view) {
        view.dispatch({
          changes: { from: 0, to: view.state.doc.length, insert: newQuery },
        });
      }

      const updated = ss.getState();
      runQueryAndRefresh(
        newQuery,
        updated.from,
        updated.to,
        1,
        updated.pageSize,
      );
    },
    [runQueryAndRefresh, getEditorView],
  );

  // --- View mode handler ---
  const handleViewModeChange = useCallback((mode: "table" | "list") => {
    ss.setState({ viewMode: mode });
  }, []);

  // --- Time apply handler ---
  const handleTimeApply = useCallback(() => {
    if (!ss.getState().tailActive) {
      ss.setState({ histogramBrushed: false, page: 1 });
      const s = ss.getState();
      runQueryAndRefresh(s.query.trim(), s.from, s.to, 1, s.pageSize);
    }
  }, [runQueryAndRefresh]);

  // --- Editor ref callback ---
  const handleEditorRef = useCallback((handle: QueryEditorHandle | null) => {
    editorHandleRef.current = handle;
  }, []);

  // --- Keyboard shortcuts ---
  useKeyboardShortcuts({
    onFocusEditor: () => editorHandleRef.current?.focus(),
    onToggleTail: handleTailToggle,
    onToggleSidebar: () => {
      ss.setState((s) => ({ sidebarVisible: !s.sidebarVisible }));
    },
    onClosePanel: () => {
      if (ss.getState().explainOpen) {
        ss.setState({ explainOpen: false });
        return;
      }
      editorHandleRef.current?.getView()?.contentDOM.blur();
    },
    // Palette/help are app-shell shortcuts registered in App so they work
    // regardless of which route (or lazy chunk) is mounted.
  });

  // Watch for queries loaded from the command palette
  useEffect(() => {
    const unsubscribe = useOverlayStore.subscribe((state, prevState) => {
      const q = state.paletteQuery;
      if (!q || q === prevState.paletteQuery) return;
      setPaletteQuery(null);
      ss.setState({ query: q });
      const view = getEditorView();
      if (view) {
        view.dispatch({
          changes: { from: 0, to: view.state.doc.length, insert: q },
        });
      }
      ss.setState({ page: 1 });
      const s = ss.getState();
      runQueryAndRefresh(q, s.from, s.to, 1, s.pageSize);
    });
    return unsubscribe;
  }, [runQueryAndRefresh, getEditorView]);

  // Fetch indexes, views, and field catalog on mount for the flow sidebar
  useEffect(() => {
    Promise.allSettled([fetchIndexes(), fetchViews(), fetchFields()]).then(
      ([idx, views, fields]) => {
        if (idx.status === "fulfilled")
          ss.setState({ sidebarIndexes: idx.value });
        if (views.status === "fulfilled")
          ss.setState({ sidebarViews: views.value });
        if (fields.status === "fulfilled") {
          const m = new Map<string, string>();
          for (const f of fields.value) {
            m.set(f.name, f.type);
          }
          ss.setState({ catalogFields: fields.value, fieldTypeMap: m });
        }
      },
    );
  }, []);

  // Restore query, time range, and pagination from URL hash on mount
  useEffect(() => {
    const hashData = readQueryFromHash();
    if (hashData) {
      const updates: Record<string, unknown> = {
        query: hashData.q,
        from: hashData.from || "-1h",
        to: hashData.to,
      };
      if (hashData.page) updates.page = hashData.page;
      if (hashData.size) updates.pageSize = hashData.size;
      ss.setState(updates);
      // Defer execution to ensure editor has rendered
      setTimeout(() => {
        const s = ss.getState();
        runQueryAndRefresh(hashData.q, s.from, s.to, s.page, s.pageSize);
      }, 0);
    }
    // eslint-disable-next-line react-hooks/exhaustive-deps -- intentionally run once on mount
  }, []);

  return (
    <div className="flex flex-col h-dvh overflow-hidden">
      <QueryBar
        onQueryChange={handleQueryChange}
        onExecute={handleExecute}
        onTailToggle={handleTailToggle}
        onTimeApply={handleTimeApply}
        editorRef={handleEditorRef}
      />

      <div className="flex flex-row flex-1 overflow-hidden relative">
        <FlowSidebar
          visible={sidebarVisible}
          indexes={sidebarIndexes}
          views={sidebarViews}
          explainResult={explainResult}
          fieldTypes={fieldTypeMap}
          selectedFields={activeResult ? deriveColumns(activeResult) : []}
          catalogFields={catalogFields}
          onFilter={handleFilter}
          onToggle={handleSidebarToggle}
          onSelectSource={handleSetSource}
          onInsertCommand={handleInsertCommand}
        />

        <div className="flex flex-col flex-1 min-w-0 overflow-hidden">
          <Histogram
            onBrush={handleTimelineBrush}
            onReset={handleHistogramReset}
          />

          <StatsBar resultCount={resultCount(result)} />

          {/* Table toolbar -- only show when results exist */}
          {hasResults && (
            <TableToolbar
              viewMode={viewMode}
              onViewModeChange={handleViewModeChange}
              onExport={handleExport}
              totalCount={totalCount}
              pageCount={pageCount}
            />
          )}

          <ResultsContainer
            resultsAreaRef={resultsAreaRef}
            onSort={handleSort}
            onFilter={handleFilter}
            onCellCopy={handleCellCopy}
            onNewEventsBadgeClick={handleNewEventsBadgeClick}
          />

          {/* Pagination bar -- only show for non-tail, non-empty results */}
          <Pagination onRunQuery={runQueryAndRefresh} />
        </div>
      </div>
    </div>
  );
}
