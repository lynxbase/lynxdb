/**
 * Streaming & async query API layer.
 *
 * Provides four functions for adaptive query execution:
 *   - submitHybridQuery() -- POST /api/v1/query with wait:0.2 (sync or async)
 *   - streamQuery()       -- POST /api/v1/query/stream (NDJSON stream consumer)
 *   - subscribeJobProgress() -- GET /api/v1/query/jobs/{id}/stream (SSE)
 *   - cancelJob()         -- DELETE /api/v1/query/jobs/{id}
 */

import type { QueryResult, QueryStats } from "./client";
import { authHeaders, handleAuthError, token } from "./auth";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export interface HybridResult {
  status: "sync" | "async";
  /** Present when status is "sync" (200 response). */
  syncResult?: { result: QueryResult; stats: QueryStats };
  /** Present when status is "async" (202 response). */
  jobId?: string;
}

export interface StreamCallbacks {
  onRow: (row: Record<string, unknown>) => void;
  onMeta: (meta: { total?: number; scanned?: number; took_ms?: number }) => void;
  onError: (message: string) => void;
}

export interface ProgressData {
  phase: string;
  scanned: number;
  percent: number;
  events_matched: number;
  elapsed_ms: number;
  eta_ms?: number;
  /** Total number of segments to scan. Present in backend SearchProgress struct;
      used by Plan 02's onProgress handler to compute the 'total' for progress bar display. */
  segments_total?: number;
}

// ---------------------------------------------------------------------------
// submitHybridQuery
// ---------------------------------------------------------------------------

/**
 * Submit a query with hybrid execution (wait up to 200ms for sync result).
 *
 * - On 200: returns `{ status: "sync", syncResult }`.
 * - On 202: returns `{ status: "async", jobId }`.
 */
export async function submitHybridQuery(
  q: string,
  from?: string,
  to?: string,
  limit?: number,
  offset?: number,
  signal?: AbortSignal,
): Promise<HybridResult> {
  const body: Record<string, unknown> = { q, wait: 0.2 };
  if (from) body.from = from;
  if (to) body.to = to;
  if (limit) body.limit = limit;
  if (offset) body.offset = offset;

  const resp = await fetch("/api/v1/query", {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(body),
    signal,
  });

  handleAuthError(resp);

  if (!resp.ok && resp.status !== 202) {
    const err = await resp
      .json()
      .catch(() => ({ error: { message: resp.statusText } }));
    throw new Error(
      err.error?.message || err.data?.error || resp.statusText,
    );
  }

  const json = await resp.json();

  if (resp.status === 202) {
    return { status: "async", jobId: json.data?.job_id };
  }

  // 200 -- synchronous result
  return {
    status: "sync",
    syncResult: {
      result: json.data as QueryResult,
      stats: {
        took_ms: json.meta?.took_ms ?? 0,
        scanned: json.meta?.scanned ?? 0,
        query_id: json.meta?.query_id,
        stats: json.meta?.stats,
      },
    },
  };
}

// ---------------------------------------------------------------------------
// streamQuery
// ---------------------------------------------------------------------------

/**
 * Consume an NDJSON stream from /api/v1/query/stream.
 *
 * Handles partial line buffering (chunks may split across JSON boundaries).
 * Optionally stops after `maxRows` rows.
 */
export async function streamQuery(
  q: string,
  from?: string,
  to?: string,
  callbacks?: StreamCallbacks,
  signal?: AbortSignal,
  maxRows?: number,
): Promise<void> {
  if (!callbacks) return;

  const body: Record<string, unknown> = { q };
  if (from) body.from = from;
  if (to) body.to = to;

  const resp = await fetch("/api/v1/query/stream", {
    method: "POST",
    headers: { "Content-Type": "application/json", ...authHeaders() },
    body: JSON.stringify(body),
    signal,
  });

  handleAuthError(resp);

  if (!resp.ok) {
    const err = await resp
      .json()
      .catch(() => ({ error: { message: resp.statusText } }));
    callbacks.onError(
      err.error?.message || err.data?.error || resp.statusText,
    );
    return;
  }

  if (!resp.body) {
    callbacks.onError("Response body is not readable");
    return;
  }

  const reader = resp.body.pipeThrough(new TextDecoderStream()).getReader();
  let buffer = "";
  let rowCount = 0;

  try {
    for (;;) {
      const { done, value } = await reader.read();

      if (done) {
        // Process any remaining content in the buffer.
        if (buffer.trim()) {
          processLine(buffer, callbacks);
        }
        break;
      }

      buffer += value;
      const lines = buffer.split("\n");
      // Last element may be incomplete -- keep it in the buffer.
      buffer = lines.pop() ?? "";

      for (const line of lines) {
        if (!line.trim()) continue;
        const isMeta = processLine(line, callbacks);
        if (!isMeta) {
          rowCount++;
          if (maxRows !== undefined && rowCount >= maxRows) {
            await reader.cancel();
            return;
          }
        }
      }
    }
  } catch (err: unknown) {
    // AbortError is expected when the caller cancels via signal.
    if (err instanceof DOMException && err.name === "AbortError") return;
    throw err;
  }
}

/**
 * Parse and dispatch a single NDJSON line.
 * Returns true if the line was a __meta or __error control line.
 */
function processLine(line: string, callbacks: StreamCallbacks): boolean {
  try {
    const parsed = JSON.parse(line);
    if (parsed.__meta) {
      callbacks.onMeta(parsed.__meta);
      return true;
    }
    if (parsed.__error) {
      callbacks.onError(parsed.__error.message ?? "Stream error");
      return true;
    }
    callbacks.onRow(parsed);
    return false;
  } catch {
    // Skip malformed JSON lines gracefully.
    return true;
  }
}

// ---------------------------------------------------------------------------
// subscribeJobProgress
// ---------------------------------------------------------------------------

/**
 * Subscribe to job progress via SSE.
 *
 * Follows the same SSE pattern as `api/sse.ts`: uses `new EventSource` with
 * `_token` query param for auth.
 *
 * @returns Cleanup function that closes the EventSource.
 */
export function subscribeJobProgress(
  jobId: string,
  onProgress: (p: ProgressData) => void,
  onComplete: (data: unknown) => void,
  onFailed: (message: string) => void,
  onCanceled: () => void,
): () => void {
  const params = new URLSearchParams();
  if (token.value) {
    params.set("_token", token.value);
  }

  const qs = params.toString();
  const url = `/api/v1/query/jobs/${encodeURIComponent(jobId)}/stream${qs ? `?${qs}` : ""}`;
  const source = new EventSource(url);

  source.addEventListener("progress", (e: MessageEvent) => {
    try {
      onProgress(JSON.parse(e.data) as ProgressData);
    } catch {
      /* skip malformed progress data */
    }
  });

  source.addEventListener("complete", (e: MessageEvent) => {
    try {
      onComplete(JSON.parse(e.data));
    } catch {
      /* skip */
    }
    source.close();
  });

  source.addEventListener("failed", (e: MessageEvent) => {
    try {
      const data = JSON.parse(e.data);
      onFailed(data.message ?? data.code ?? "Query failed");
    } catch {
      onFailed("Query failed");
    }
    source.close();
  });

  source.addEventListener("canceled", () => {
    onCanceled();
    source.close();
  });

  source.onerror = () => {
    if (source.readyState === EventSource.CLOSED) {
      onFailed("Progress connection closed");
    }
    // EventSource auto-reconnects on transient errors -- no action needed.
  };

  return () => source.close();
}

// ---------------------------------------------------------------------------
// cancelJob
// ---------------------------------------------------------------------------

/**
 * Cancel a running async query job.
 */
export async function cancelJob(jobId: string): Promise<void> {
  const resp = await fetch(`/api/v1/query/jobs/${encodeURIComponent(jobId)}`, {
    method: "DELETE",
    headers: authHeaders(),
  });

  handleAuthError(resp);

  if (!resp.ok) {
    const err = await resp
      .json()
      .catch(() => ({ error: { message: resp.statusText } }));
    throw new Error(
      err.error?.message || err.data?.error || resp.statusText,
    );
  }
}
