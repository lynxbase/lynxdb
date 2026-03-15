import type { QueryStats as QueryStatsType } from "../api/client";
import { formatCount, formatMs } from "../utils/format";
import styles from "./QueryStats.module.css";

interface QueryStatsProps {
  stats: QueryStatsType | null;
  loading: boolean;
  error: string | null;
  resultCount: number;
  tailActive?: boolean;
  tailEventCount?: number;
  tailCatchupDone?: boolean;
}

export function QueryStatsBar({
  stats,
  loading,
  error,
  resultCount,
  tailActive,
  tailEventCount,
  tailCatchupDone,
}: QueryStatsProps) {
  /* --- Live Tail mode --- */
  if (tailActive) {
    const count = tailEventCount ?? 0;

    // Show error/warning inline even in tail mode
    if (error) {
      return (
        <div class={styles.bar} role="alert">
          <span class={styles.tailDot} aria-hidden="true" />
          <span class={styles.tailLabel}>Live Tail</span>
          <span class={styles.errorMsg}>{error}</span>
        </div>
      );
    }

    const statusText = tailCatchupDone
      ? `${formatCount(count)} ${count === 1 ? "event" : "events"}`
      : `Catching up\u2026 ${formatCount(count)} ${count === 1 ? "event" : "events"}`;

    return (
      <div class={styles.bar} role="status" aria-live="polite">
        <span class={styles.tailDot} aria-hidden="true" />
        <span class={styles.tailLabel}>Live Tail</span>
        <span class={styles.tailSep} aria-hidden="true">&mdash;</span>
        <span>{statusText}</span>
      </div>
    );
  }

  /* --- Standard query mode --- */
  if (loading) {
    return (
      <div class={styles.bar} role="status" aria-live="polite">
        <span class={styles.spinner} aria-hidden="true" />
        Running query...
      </div>
    );
  }

  if (error) {
    return (
      <div class={styles.bar} role="alert">
        <span class={styles.errorIcon} aria-hidden="true">&#9888;</span>
        <span class={styles.errorMsg}>{error}</span>
      </div>
    );
  }

  if (!stats) {
    return <div class={styles.bar}>Ready</div>;
  }

  const parts: string[] = [];
  parts.push(`${formatCount(resultCount)} ${resultCount === 1 ? "result" : "results"}`);
  parts.push(`in ${formatMs(stats.took_ms)}`);

  if (stats.scanned > 0) {
    parts.push(`(scanned ${formatCount(stats.scanned)})`);
  }

  return (
    <div class={styles.bar} role="status" aria-live="polite">
      <span class={styles.success} aria-hidden="true">&#10003;</span>
      {parts.join(" ")}
    </div>
  );
}
