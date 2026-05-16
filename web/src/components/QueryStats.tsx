import { useState, useEffect } from "react";
import type {
  QueryStats as QueryStatsType,
  DetailedStats,
} from "../api/client";
import { formatCount, formatMs, formatBytes } from "../utils/format";
import { formatElapsed } from "../utils/format";
import { Badge } from "./ui/badge";

interface QueryStatsProps {
  stats: QueryStatsType | null;
  loading: boolean;
  error: string | null;
  resultCount: number;
  tailActive?: boolean;
  tailEventCount?: number;
  tailCatchupDone?: boolean;

  // Streaming & Progress
  /** True while NDJSON search stream is active */
  streaming?: boolean;
  /** Row count ticking up during streaming */
  streamingCount?: number;
  /** Aggregation progress data from SSE */
  progress?: {
    percent: number;
    scanned: number;
    total: number;
    elapsedMs: number;
  } | null;
  /** True when query was canceled by user */
  canceled?: boolean;
  /** Elapsed milliseconds since query started (ticking live) */
  elapsedMs?: number;
  /** True when result is showing preview rows (not final) */
  isPreview?: boolean;

  // Detailed stats & explain
  /** Callback when user clicks the Explain button */
  onExplainToggle?: () => void;
  /** Whether explain data is available */
  explainAvailable?: boolean;
  /** Whether the SSE tail connection is reconnecting */
  tailReconnecting?: boolean;
}

/**
 * Produce the compact stats summary line.
 * Format: "142 results in 4.2ms -- 12/48 segments, 36 skipped (bloom: 24, time: 12)"
 */
function formatCompactStats(
  stats: QueryStatsType,
  resultCount: number,
): string {
  const ds = stats.stats as DetailedStats | undefined;

  const parts: string[] = [];
  parts.push(
    `${formatCount(resultCount)} ${resultCount === 1 ? "result" : "results"}`,
  );
  parts.push(`in ${formatMs(stats.took_ms)}`);

  if (ds?.segments_total != null && ds.segments_scanned != null) {
    const skipped = ds.segments_total - ds.segments_scanned;
    let segPart = `${ds.segments_scanned}/${ds.segments_total} segments`;
    if (skipped > 0) {
      const skipDetails: string[] = [];
      if (ds.segments_skipped_bloom && ds.segments_skipped_bloom > 0) {
        skipDetails.push(`bloom: ${ds.segments_skipped_bloom}`);
      }
      if (ds.segments_skipped_time && ds.segments_skipped_time > 0) {
        skipDetails.push(`time: ${ds.segments_skipped_time}`);
      }
      if (ds.segments_skipped_index && ds.segments_skipped_index > 0) {
        skipDetails.push(`index: ${ds.segments_skipped_index}`);
      }
      if (ds.segments_skipped_range && ds.segments_skipped_range > 0) {
        skipDetails.push(`range: ${ds.segments_skipped_range}`);
      }
      segPart += `, ${skipped} skipped`;
      if (skipDetails.length > 0) {
        segPart += ` (${skipDetails.join(", ")})`;
      }
    }
    parts.push(`— ${segPart}`);
  } else if (stats.scanned > 0) {
    parts.push(`(scanned ${formatCount(stats.scanned)})`);
  }

  return parts.join(" ");
}

/**
 * Return string array of active optimization badge names from detailed stats.
 */
function getOptimizationBadges(ds: DetailedStats): string[] {
  const badges: string[] = [];
  if (ds.cache_hit) badges.push("cache");
  if (ds.segments_skipped_bloom && ds.segments_skipped_bloom > 0)
    badges.push("bloom");
  if (ds.partial_agg_used) badges.push("partial-agg");
  if (ds.topk_used) badges.push("TopK");
  if (ds.vectorized_filter_used) badges.push("vectorized");
  if (ds.dict_filter_used) badges.push("dict-filter");
  if (ds.count_star_optimized) badges.push("count(*)");
  if (ds.inverted_index_hits && ds.inverted_index_hits > 0)
    badges.push("inverted-idx");
  return badges;
}

const barBase =
  "flex shrink-0 items-center gap-2 px-3 py-1.5 bg-secondary border-b border-border font-sans text-[0.8125rem] text-muted-foreground min-h-8";

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
  isPreview,
  onExplainToggle,
  explainAvailable,
  tailReconnecting,
}: QueryStatsProps) {
  // Expand/collapse state for detailed stats row. Resets on new stats (Pitfall 3).
  const [expanded, setExpanded] = useState(false);
  useEffect(() => {
    setExpanded(false);
  }, [stats]);

  /* --- Live Tail mode --- */
  if (tailActive) {
    const count = tailEventCount ?? 0;

    // Show error/warning inline even in tail mode
    if (error) {
      return (
        <div className={barBase} role="alert">
          <span
            className="inline-block size-2 shrink-0 rounded-full bg-[var(--success)] animate-[pulse_1.5s_ease-in-out_infinite] motion-reduce:animate-none"
            aria-hidden="true"
          />
          <span className="font-semibold text-[var(--success)]">Live Tail</span>
          <span className="text-destructive">{error}</span>
        </div>
      );
    }

    // Reconnecting state: amber dot and "Reconnecting..." label
    if (tailReconnecting) {
      return (
        <div className={barBase} role="status" aria-live="polite">
          <span
            className="inline-block size-2 shrink-0 rounded-full bg-[var(--warning,#f59e0b)] animate-[pulse_2.5s_ease-in-out_infinite] motion-reduce:animate-none"
            aria-hidden="true"
          />
          <span className="font-semibold text-[var(--warning,#f59e0b)]">Reconnecting...</span>
          <span className="text-muted-foreground" aria-hidden="true">&mdash;</span>
          <span>
            {formatCount(count)} {count === 1 ? "event" : "events"}
          </span>
        </div>
      );
    }

    const statusText = tailCatchupDone
      ? `${formatCount(count)} ${count === 1 ? "event" : "events"}`
      : `Catching up… ${formatCount(count)} ${count === 1 ? "event" : "events"}`;

    return (
      <div className={barBase} role="status" aria-live="polite">
        <span
          className="inline-block size-2 shrink-0 rounded-full bg-[var(--success)] animate-[pulse_1.5s_ease-in-out_infinite] motion-reduce:animate-none"
          aria-hidden="true"
        />
        <span className="font-semibold text-[var(--success)]">Live Tail</span>
        <span className="text-muted-foreground" aria-hidden="true">&mdash;</span>
        <span>{statusText}</span>
      </div>
    );
  }

  /* --- Canceled state --- */
  if (canceled) {
    const elapsed = formatElapsed(elapsedMs ?? 0);
    const hasPartialResults =
      streamingCount !== undefined && streamingCount > 0;

    return (
      <div className={barBase} role="status" aria-live="polite">
        <span className="text-muted-foreground" aria-hidden="true">&#9888;</span>
        {hasPartialResults
          ? `Canceled — ${formatCount(streamingCount!)} partial results in ${elapsed}`
          : `Canceled — ${elapsed}`}
      </div>
    );
  }

  /* --- Streaming state (NDJSON search in progress) --- */
  if (streaming) {
    return (
      <div className={barBase} role="status" aria-live="polite">
        <span
          className="inline-block size-2 shrink-0 rounded-full bg-primary animate-[pulse_1.5s_ease-in-out_infinite] motion-reduce:animate-none"
          aria-hidden="true"
        />
        {`${formatCount(streamingCount ?? 0)} results (streaming...) — ${formatElapsed(elapsedMs ?? 0)}`}
      </div>
    );
  }

  /* --- Progress state (aggregation with progress bar) --- */
  if (progress) {
    return (
      <div className={barBase} role="status" aria-live="polite">
        <div className="flex-1 max-w-[200px] h-1 bg-border rounded-sm overflow-hidden">
          <div
            className="h-full bg-primary rounded-sm transition-[width] duration-300"
            style={{ width: `${progress.percent}%` }}
          />
        </div>
        {`${formatCount(progress.scanned)}/${formatCount(progress.total)} segments (${Math.round(progress.percent)}%) — ${formatElapsed(elapsedMs ?? progress.elapsedMs)}`}
        {isPreview && (
          <span className="text-muted-foreground italic whitespace-nowrap">Showing partial results…</span>
        )}
      </div>
    );
  }

  /* --- Standard query mode --- */
  if (loading) {
    return (
      <div className={barBase} role="status" aria-live="polite">
        <span
          className="inline-block size-3.5 shrink-0 rounded-full border-2 border-border border-t-primary animate-spin"
          aria-hidden="true"
        />
        Running query...
      </div>
    );
  }

  if (error) {
    return (
      <div className={barBase} role="alert">
        <span className="text-destructive" aria-hidden="true">&#9888;</span>
        <span className="text-destructive">{error}</span>
      </div>
    );
  }

  if (!stats) {
    return <div className={barBase}>Ready</div>;
  }

  // --- Completed query with compact/expanded stats ---
  const compactText = formatCompactStats(stats, resultCount);
  const ds = stats.stats as DetailedStats | undefined;
  const badges = ds ? getOptimizationBadges(ds) : [];

  // MV acceleration info from query response meta
  const acceleratedBy = ds?.accelerated_by;
  const mvSpeedup = ds?.mv_speedup;

  // Determine if we have detail data to expand
  const hasDetail =
    ds &&
    (ds.scan_ms != null ||
      ds.pipeline_ms != null ||
      badges.length > 0 ||
      ds.processed_bytes != null);

  return (
    <div
      className={`${hasDetail ? "flex flex-col items-stretch gap-1" : "flex items-center"} shrink-0 px-3 py-1.5 bg-secondary border-b border-border font-sans text-[0.8125rem] text-muted-foreground min-h-8`}
      role="status"
      aria-live="polite"
    >
      <div className="flex items-center gap-2">
        <span className="text-[var(--success)]" aria-hidden="true">&#10003;</span>
        <span>{compactText}</span>
        {acceleratedBy && (
          <Badge variant="secondary" className="gap-1 ml-2 text-xs text-primary">
            <span aria-hidden="true">&#9889;</span>
            MV: {acceleratedBy}
            {mvSpeedup && ` (~${mvSpeedup})`}
          </Badge>
        )}
        {hasDetail && (
          <button
            type="button"
            className="cursor-pointer bg-transparent border-none p-0.5 text-muted-foreground text-xs leading-none inline-flex items-center hover:text-foreground"
            onClick={() => setExpanded(!expanded)}
            aria-label={expanded ? "Collapse details" : "Expand details"}
            aria-expanded={expanded}
          >
            {expanded ? "▲" : "▼"}
          </button>
        )}
        {explainAvailable && onExplainToggle && (
          <button
            type="button"
            className="cursor-pointer bg-transparent border border-border rounded-sm px-1.5 py-px text-muted-foreground text-[0.6875rem] font-sans leading-snug whitespace-nowrap hover:text-foreground hover:border-muted-foreground"
            onClick={onExplainToggle}
          >
            Explain
          </button>
        )}
      </div>
      {expanded && ds && (
        <div className="flex flex-wrap items-center gap-2 pt-1 border-t border-border text-xs text-muted-foreground">
          {ds.scan_ms != null && (
            <span className="whitespace-nowrap">Scan: {formatMs(ds.scan_ms)}</span>
          )}
          {ds.pipeline_ms != null && (
            <span className="whitespace-nowrap">Pipeline: {formatMs(ds.pipeline_ms)}</span>
          )}
          {ds.parse_ms != null && (
            <span className="whitespace-nowrap">Parse: {formatMs(ds.parse_ms)}</span>
          )}
          {ds.optimize_ms != null && (
            <span className="whitespace-nowrap">Optimize: {formatMs(ds.optimize_ms)}</span>
          )}
          {badges.map((b) => (
            <Badge key={b} variant="outline" className="text-[0.6875rem] px-1.5 py-0 h-auto text-muted-foreground">
              {b}
            </Badge>
          ))}
          {ds.processed_bytes != null && ds.processed_bytes > 0 && (
            <span className="whitespace-nowrap">
              {formatBytes(ds.processed_bytes)} processed
            </span>
          )}
        </div>
      )}
    </div>
  );
}
