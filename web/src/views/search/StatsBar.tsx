import { useCallback } from "react";
import { QueryStatsBar } from "../../components/QueryStats";
import { ExplainInspector } from "../../components/ExplainInspector";
import { useSearchStore } from "../../stores/search";

interface StatsBarProps {
  resultCount: number;
}

const ss = useSearchStore;

export function StatsBar({ resultCount }: StatsBarProps) {
  const stats = useSearchStore((s) => s.stats);
  const loading = useSearchStore((s) => s.loading);
  const error = useSearchStore((s) => s.error);
  const tailActive = useSearchStore((s) => s.tailActive);
  const tailEvents = useSearchStore((s) => s.tailEvents);
  const tailCatchupDone = useSearchStore((s) => s.tailCatchupDone);
  const tailReconnecting = useSearchStore((s) => s.tailReconnecting);
  const streaming = useSearchStore((s) => s.streaming);
  const streamingCount = useSearchStore((s) => s.streamingCount);
  const progressData = useSearchStore((s) => s.progressData);
  const canceled = useSearchStore((s) => s.canceled);
  const elapsedMs = useSearchStore((s) => s.elapsedMs);
  const isPreview = useSearchStore((s) => s.isPreview);
  const explainResult = useSearchStore((s) => s.explainResult);
  const explainOpen = useSearchStore((s) => s.explainOpen);

  const handleExplainToggle = useCallback(() => {
    ss.setState((s) => ({ explainOpen: !s.explainOpen }));
  }, []);

  return (
    <>
      <QueryStatsBar
        stats={stats}
        loading={loading}
        error={error}
        resultCount={
          tailActive ? tailEvents.length : resultCount
        }
        tailActive={tailActive}
        tailEventCount={tailEvents.length}
        tailCatchupDone={tailCatchupDone}
        streaming={streaming}
        streamingCount={streamingCount}
        progress={progressData}
        canceled={canceled}
        elapsedMs={elapsedMs}
        isPreview={isPreview}
        onExplainToggle={handleExplainToggle}
        explainAvailable={
          !!(explainResult?.is_valid && explainResult?.parsed)
        }
        tailReconnecting={tailReconnecting}
      />
      {explainOpen &&
        explainResult?.is_valid &&
        explainResult?.parsed && (
          <ExplainInspector
            explain={explainResult}
            stats={stats}
          />
        )}
    </>
  );
}
