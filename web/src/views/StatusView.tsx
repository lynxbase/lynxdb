import { useEffect, useCallback } from "preact/hooks";
import { signal } from "@preact/signals";
import { fetchStatus } from "../api/client";
import { formatUptime, formatBytes, formatCount } from "../utils/format";
import styles from "./StatusView.module.css";

interface Props {
  path?: string;
}

// ---------------------------------------------------------------------------
// State
// ---------------------------------------------------------------------------

const status = signal<Record<string, unknown> | null>(null);
const loading = signal(true);
const error = signal<string | null>(null);
const lastUpdatedAt = signal<Date | null>(null);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function safeNumber(value: unknown): number {
  if (typeof value === "number" && !isNaN(value)) return value;
  return 0;
}

function safeString(value: unknown, fallback = "--"): string {
  if (typeof value === "string" && value.length > 0) return value;
  return fallback;
}

function nested(obj: Record<string, unknown> | null, key: string): Record<string, unknown> {
  if (!obj) return {};
  const v = obj[key];
  if (v && typeof v === "object" && !Array.isArray(v)) {
    return v as Record<string, unknown>;
  }
  return {};
}

function healthClass(health: string): string {
  switch (health) {
    case "healthy":
      return styles.healthHealthy;
    case "degraded":
      return styles.healthDegraded;
    default:
      return styles.healthUnhealthy;
  }
}

function formatLastUpdated(date: Date | null): string {
  if (!date) return "";
  const h = String(date.getHours()).padStart(2, "0");
  const m = String(date.getMinutes()).padStart(2, "0");
  const s = String(date.getSeconds()).padStart(2, "0");
  return `Last updated ${h}:${m}:${s}`;
}

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export function StatusView(_props: Props) {
  const loadStatus = useCallback(async () => {
    try {
      const data = await fetchStatus();
      status.value = data;
      error.value = null;
      lastUpdatedAt.value = new Date();
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "Failed to fetch status";
      error.value = message;
    } finally {
      loading.value = false;
    }
  }, []);

  useEffect(() => {
    // Initial fetch
    loading.value = true;
    loadStatus();

    // Auto-refresh every 5 seconds
    const interval = setInterval(loadStatus, 5000);
    return () => clearInterval(interval);
  }, [loadStatus]);

  // Loading state (only on first load)
  if (loading.value && !status.value) {
    return (
      <div class={styles.loadingState} role="status" aria-live="polite">
        Loading status...
      </div>
    );
  }

  // Error state (only if we never got data)
  if (error.value && !status.value) {
    return (
      <div class={styles.errorState} role="alert">
        <div>Unable to connect to server</div>
        <div class={styles.errorMessage}>{error.value}</div>
        <button
          type="button"
          class={styles.retryBtn}
          onClick={loadStatus}
        >
          Retry
        </button>
      </div>
    );
  }

  const data = status.value;
  const storageData = nested(data, "storage");
  const eventsData = nested(data, "events");
  const queriesData = nested(data, "queries");
  const viewsData = nested(data, "views");
  const tailData = nested(data, "tail");

  const health = safeString(data?.health as string | undefined, "unknown");
  const version = safeString(data?.version as string | undefined, "unknown");
  const uptimeSeconds = safeNumber(data?.uptime_seconds);
  const usedBytes = safeNumber(storageData.used_bytes);
  const segmentCount = safeNumber(storageData.segment_count);
  const totalEvents = safeNumber(eventsData.total);
  const todayEvents = safeNumber(eventsData.today);
  const activeQueries = safeNumber(queriesData.active);
  const totalViews = safeNumber(viewsData.total);
  const activeViews = safeNumber(viewsData.active);
  const tailSessions = safeNumber(tailData.active_sessions);
  const tailDropped = safeNumber(tailData.total_dropped_events);

  return (
    <div class={styles.page}>
      <div class={styles.pageHeader}>
        <h1 class={styles.pageTitle}>Server Status</h1>
        <span class={styles.lastUpdated} aria-live="off">
          {formatLastUpdated(lastUpdatedAt.value)}
        </span>
      </div>

      <div class={styles.grid}>
        {/* Server card */}
        <div class={styles.card}>
          <div class={styles.cardTitle}>Server</div>
          <div class={styles.healthRow}>
            <span
              class={`${styles.healthDot} ${healthClass(health)}`}
              aria-hidden="true"
            />
            <span class={styles.healthLabel}>{health}</span>
          </div>
          <div class={styles.cardSubtext}>
            v{version} &middot; up {formatUptime(uptimeSeconds)}
          </div>
        </div>

        {/* Events card */}
        <div class={styles.card}>
          <div class={styles.cardTitle}>Events</div>
          <div class={styles.cardValue}>{formatCount(totalEvents)}</div>
          <div class={styles.cardSubtext}>
            {formatCount(todayEvents)} today
          </div>
        </div>

        {/* Storage card */}
        <div class={styles.card}>
          <div class={styles.cardTitle}>Storage</div>
          <div class={styles.cardValue}>{formatBytes(usedBytes)}</div>
          {segmentCount > 0 && (
            <div class={styles.cardSubtext}>
              {formatCount(segmentCount)} {segmentCount === 1 ? "segment" : "segments"}
            </div>
          )}
        </div>

        {/* Queries card */}
        <div class={styles.card}>
          <div class={styles.cardTitle}>Queries</div>
          <div class={styles.cardValue}>{activeQueries}</div>
          <div class={styles.cardSubtext}>active</div>
        </div>

        {/* Views card */}
        <div class={styles.card}>
          <div class={styles.cardTitle}>Materialized Views</div>
          <div class={styles.cardValue}>{totalViews}</div>
          <div class={styles.cardSubtext}>
            {activeViews} active
          </div>
        </div>

        {/* Tail card */}
        <div class={styles.card}>
          <div class={styles.cardTitle}>Live Tail</div>
          <div class={styles.cardValue}>{tailSessions}</div>
          <div class={styles.cardSubtext}>
            {tailSessions === 1 ? "session" : "sessions"}
            {tailDropped > 0 && ` \u00b7 ${formatCount(tailDropped)} dropped`}
          </div>
        </div>
      </div>
    </div>
  );
}
