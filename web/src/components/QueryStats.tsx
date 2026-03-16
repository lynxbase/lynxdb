import type { QueryStats as QueryStatsType } from "../api/client";
import { formatCount, formatMs } from "../utils/format";
import { formatElapsed } from "../utils/format";
import styles from "./QueryStats.module.css";

interface QueryStatsProps {
  stats: QueryStatsType | null;
  loading: boolean;
  error: string | null;
  resultCount: number;
  tailActive?: boolean;
  tailEventCount?: number;
  tailCatchupDone?: boolean;

  // Phase 5: Streaming & Progress
  /** True while NDJSON search stream is active */
  streaming?: boolean;
  /** Row count ticking up during streaming */
  streamingCount?: number;
  /** Aggregation progress data from SSE */
  progress?: { percent: number; scanned: number; total: number; elapsedMs: number } | null;
  /** True when query was canceled by user */
  canceled?: boolean;
  /** Elapsed milliseconds since query started (ticking live) */
  elapsedMs?: number;
}

export function QueryStatsBar({
  stats,
  loading,
  error,
  resultCount,
  tailActive,
  tailEventCount,
  tailCatchupDone,
  streaming,
  streamingCount,
  progress,
  canceled,
  elapsedMs,
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

  /* --- Canceled state --- */
  if (canceled) {
    const elapsed = formatElapsed(elapsedMs ?? 0);
    const hasPartialResults = streamingCount !== undefined && streamingCount > 0;

    return (
      <div class={styles.bar} role="status" aria-live="polite">
        <span class={styles.canceledIcon} aria-hidden="true">&#9888;</span>
        {hasPartialResults
          ? `Canceled \u2014 ${formatCount(streamingCount!)} partial results in ${elapsed}`
          : `Canceled \u2014 ${elapsed}`}
      </div>
    );
  }

  /* --- Streaming state (NDJSON search in progress) --- */
  if (streaming) {
    return (
      <div class={styles.bar} role="status" aria-live="polite">
        <span class={styles.streamingDot} aria-hidden="true" />
        {`${formatCount(streamingCount ?? 0)} results (streaming...) \u2014 ${formatElapsed(elapsedMs ?? 0)}`}
      </div>
    );
  }

  /* --- Progress state (aggregation with progress bar) --- */
  if (progress) {
    return (
      <div class={styles.bar} role="status" aria-live="polite">
        <div class={styles.progressTrack}>
          <div class={styles.progressFill} style={{ width: `${progress.percent}%` }} />
        </div>
        {`${formatCount(progress.scanned)}/${formatCount(progress.total)} segments (${Math.round(progress.percent)}%) \u2014 ${formatElapsed(elapsedMs ?? progress.elapsedMs)}`}
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

  // MV acceleration info from query response meta
  const acceleratedBy = stats.stats?.accelerated_by as string | undefined;
  const mvSpeedup = stats.stats?.mv_speedup as string | undefined;

  return (
    <div class={styles.bar} role="status" aria-live="polite">
      <span class={styles.success} aria-hidden="true">&#10003;</span>
      {parts.join(" ")}
      {acceleratedBy && (
        <span class={styles.mvBadge}>
          <span class={styles.mvIcon} aria-hidden="true">&#9889;</span>
          MV: {acceleratedBy}
          {mvSpeedup && ` (~${mvSpeedup})`}
        </span>
      )}
    </div>
  );
}
