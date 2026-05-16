import { useEffect, useCallback, useState, useRef } from "react";
import { AlertCircle } from "lucide-react";
import { fetchStatus } from "../api/client";
import { formatUptime, formatBytes, formatCount } from "../utils/format";
import { Card, CardContent } from "../components/ui/card";
import { Badge } from "../components/ui/badge";
import { Skeleton } from "../components/ui/skeleton";
import { Alert, AlertDescription, AlertTitle } from "../components/ui/alert";
import { Button } from "../components/ui/button";
import { PageContainer } from "../components/PageContainer";
import { cn } from "@/lib/utils";

// Helpers

function safeNumber(value: unknown): number {
  if (typeof value === "number" && !isNaN(value)) return value;
  return 0;
}

function safeString(value: unknown, fallback = "--"): string {
  if (typeof value === "string" && value.length > 0) return value;
  return fallback;
}

function nested(
  obj: Record<string, unknown> | null,
  key: string,
): Record<string, unknown> {
  if (!obj) return {};
  const v = obj[key];
  if (v && typeof v === "object" && !Array.isArray(v)) {
    return v as Record<string, unknown>;
  }
  return {};
}

function healthBadgeClass(health: string): string {
  switch (health) {
    case "healthy":
      return "border-transparent bg-chart-4/15 text-chart-4";
    case "degraded":
      return "border-transparent bg-chart-2/15 text-chart-2";
    default:
      return "border-transparent bg-destructive/15 text-destructive";
  }
}

function formatLastUpdated(date: Date | null): string {
  if (!date) return "";
  const h = String(date.getHours()).padStart(2, "0");
  const m = String(date.getMinutes()).padStart(2, "0");
  const s = String(date.getSeconds()).padStart(2, "0");
  return `Last updated ${h}:${m}:${s}`;
}

// Skeleton cards for loading
function StatusSkeleton() {
  return (
    <div className="grid grid-cols-[repeat(auto-fill,minmax(240px,1fr))] gap-3">
      {Array.from({ length: 6 }).map((_, i) => (
        <Card key={i} className="gap-2 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-2 px-0">
            <Skeleton className="h-3 w-20" />
            <Skeleton className="h-8 w-24" />
            <Skeleton className="h-3 w-32" />
          </CardContent>
        </Card>
      ))}
    </div>
  );
}

// Component

export function StatusView() {
  const [status, setStatus] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdatedAt, setLastUpdatedAt] = useState<Date | null>(null);

  // Use ref to keep loadStatus stable for the interval
  const loadStatusRef = useRef<(() => Promise<void>) | undefined>(undefined);

  const loadStatus = useCallback(async () => {
    try {
      const data = await fetchStatus();
      setStatus(data);
      setError(null);
      setLastUpdatedAt(new Date());
    } catch (err: unknown) {
      const message =
        err instanceof Error ? err.message : "Failed to fetch status";
      setError(message);
    } finally {
      setLoading(false);
    }
  }, []);

  loadStatusRef.current = loadStatus;

  useEffect(() => {
    // Initial fetch
    setLoading(true);
    loadStatus();

    // Auto-refresh every 5 seconds
    const interval = setInterval(() => loadStatusRef.current?.(), 5000);
    return () => clearInterval(interval);
  }, [loadStatus]);

  const lastUpdated = (
    <span className="text-xs text-muted-foreground" aria-live="off">
      {formatLastUpdated(lastUpdatedAt)}
    </span>
  );

  // Loading state (only on first load)
  if (loading && !status) {
    return (
      <PageContainer title="Server Status">
        <StatusSkeleton />
      </PageContainer>
    );
  }

  // Error state (only if we never got data)
  if (error && !status) {
    return (
      <PageContainer title="Server Status">
        <div className="flex flex-col items-center gap-3 py-16">
          <Alert variant="destructive" className="max-w-md rounded-md">
            <AlertCircle className="size-4" />
            <AlertTitle>Unable to connect to server</AlertTitle>
            <AlertDescription>{error}</AlertDescription>
          </Alert>
          <Button variant="outline" size="sm" onClick={loadStatus}>
            Retry
          </Button>
        </div>
      </PageContainer>
    );
  }

  const data = status;
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
    <PageContainer title="Server Status" actions={lastUpdated}>
      <div className="grid grid-cols-[repeat(auto-fill,minmax(220px,1fr))] gap-3">
        {/* Server card */}
        <Card className="gap-1 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-1 px-0">
            <div className="text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
              Server
            </div>
            <div className="flex items-center gap-2">
              <Badge
                variant="outline"
                className={cn("capitalize", healthBadgeClass(health))}
              >
                {health}
              </Badge>
            </div>
            <div className="text-[0.8125rem] text-muted-foreground">
              v{version} &middot; up {formatUptime(uptimeSeconds)}
            </div>
          </CardContent>
        </Card>

        {/* Events card */}
        <Card className="gap-1 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-1 px-0">
            <div className="text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
              Events
            </div>
            <div className="text-[1.75rem] font-semibold leading-tight tabular-nums text-foreground">
              {formatCount(totalEvents)}
            </div>
            <div className="text-[0.8125rem] text-muted-foreground">
              {formatCount(todayEvents)} today
            </div>
          </CardContent>
        </Card>

        {/* Storage card */}
        <Card className="gap-1 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-1 px-0">
            <div className="text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
              Storage
            </div>
            <div className="text-[1.75rem] font-semibold leading-tight tabular-nums text-foreground">
              {formatBytes(usedBytes)}
            </div>
            {segmentCount > 0 && (
              <div className="text-[0.8125rem] text-muted-foreground">
                {formatCount(segmentCount)}{" "}
                {segmentCount === 1 ? "segment" : "segments"}
              </div>
            )}
          </CardContent>
        </Card>

        {/* Queries card */}
        <Card className="gap-1 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-1 px-0">
            <div className="text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
              Queries
            </div>
            <div className="text-[1.75rem] font-semibold leading-tight tabular-nums text-foreground">
              {activeQueries}
            </div>
            <div className="text-[0.8125rem] text-muted-foreground">
              active
            </div>
          </CardContent>
        </Card>

        {/* Views card */}
        <Card className="gap-1 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-1 px-0">
            <div className="text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
              Materialized Views
            </div>
            <div className="text-[1.75rem] font-semibold leading-tight tabular-nums text-foreground">
              {totalViews}
            </div>
            <div className="text-[0.8125rem] text-muted-foreground">
              {activeViews} active
            </div>
          </CardContent>
        </Card>

        {/* Tail card */}
        <Card className="gap-1 rounded-md p-5 shadow-none">
          <CardContent className="flex flex-col gap-1 px-0">
            <div className="text-[0.6875rem] font-semibold uppercase tracking-wide text-muted-foreground">
              Live Tail
            </div>
            <div className="text-[1.75rem] font-semibold leading-tight tabular-nums text-foreground">
              {tailSessions}
            </div>
            <div className="text-[0.8125rem] text-muted-foreground">
              {tailSessions === 1 ? "session" : "sessions"}
              {tailDropped > 0 && ` · ${formatCount(tailDropped)} dropped`}
            </div>
          </CardContent>
        </Card>
      </div>
    </PageContainer>
  );
}

export default StatusView;
