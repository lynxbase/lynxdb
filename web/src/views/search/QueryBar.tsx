import { useCallback, lazy, Suspense } from "react";
import type { QueryEditorHandle } from "../../editor/QueryEditor";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import { LiveTailButton } from "../../components/LiveTailButton";
import { useSearchStore } from "../../stores/search";
import { formatShortcut, SHORTCUTS } from "../../utils/keyboard";
import styles from "../SearchView.module.css";

// CodeMirror is the largest dependency; load it off the initial bundle.
const QueryEditor = lazy(() => import("../../editor/QueryEditor"));

interface QueryBarProps {
  onQueryChange: (value: string) => void;
  onExecute: () => void;
  onTailToggle: () => void;
  onTimeApply: () => void;
  editorRef: (handle: QueryEditorHandle | null) => void;
}

const ss = useSearchStore;

export function QueryBar({
  onQueryChange,
  onExecute,
  onTailToggle,
  onTimeApply,
  editorRef,
}: QueryBarProps) {
  const query = useSearchStore((s) => s.query);
  const from = useSearchStore((s) => s.from);
  const to = useSearchStore((s) => s.to);
  const queryActive = useSearchStore((s) => s.queryActive);
  const tailActive = useSearchStore((s) => s.tailActive);

  const handleFromChange = useCallback((v: string) => {
    ss.setState({ from: v });
  }, []);

  const handleToChange = useCallback((v: string | undefined) => {
    ss.setState({ to: v });
  }, []);

  return (
    <div className={styles.queryBar}>
      <Suspense
        fallback={<div className={styles.editorContainer} aria-busy="true" />}
      >
        <QueryEditor
          value={query}
          onChange={onQueryChange}
          onExecute={onExecute}
          editorRef={editorRef}
        />
      </Suspense>
      <button
        type="button"
        className={`${styles.runBtn}${queryActive ? ` ${styles.cancelBtn}` : ""}`}
        onClick={onExecute}
        disabled={tailActive}
        aria-label={queryActive ? "Cancel query" : "Run query"}
        title={
          queryActive
            ? `Cancel query (${formatShortcut(SHORTCUTS.runQuery)})`
            : `Run query (${formatShortcut(SHORTCUTS.runQuery)})`
        }
      >
        {queryActive ? "■" : "▶"}
      </button>
      <LiveTailButton active={tailActive} onToggle={onTailToggle} />
      <TimeRangePicker
        from={from}
        to={to}
        onFromChange={handleFromChange}
        onToChange={handleToChange}
        onApply={onTimeApply}
      />
    </div>
  );
}
