/**
 * SSE client for the live tail endpoint.
 *
 * Uses the browser-native EventSource API to subscribe to
 * `GET /api/v1/tail` with named events (result, catchup_done,
 * warning, close, heartbeat).
 *
 * EventSource does not support custom headers, so the auth token
 * is passed as the `_token` query parameter when available.
 */

import { token } from "./auth";

export interface TailEvent {
  _time: string;
  _raw: string;
  _source?: string;
  [key: string]: unknown;
}

export interface TailCallbacks {
  onEvent: (event: TailEvent) => void;
  onCatchupDone: (count: number) => void;
  onError: (message: string) => void;
  onWarning: (message: string) => void;
}

/**
 * Open an SSE connection to the live tail endpoint and dispatch
 * events through the provided callbacks.
 *
 * @returns A cleanup function that closes the EventSource connection.
 */
export function startTail(
  query: string,
  from: string,
  count: number,
  callbacks: TailCallbacks,
): () => void {
  const params = new URLSearchParams({
    q: query,
    from,
    count: String(count),
  });

  // EventSource cannot set Authorization headers; pass token as query param.
  if (token.value) {
    params.set("_token", token.value);
  }

  const source = new EventSource(`/api/v1/tail?${params}`);

  source.addEventListener("result", (e: MessageEvent) => {
    try {
      callbacks.onEvent(JSON.parse(e.data) as TailEvent);
    } catch {
      /* skip malformed event data */
    }
  });

  source.addEventListener("catchup_done", (e: MessageEvent) => {
    try {
      const data = JSON.parse(e.data) as { count?: number };
      callbacks.onCatchupDone(data.count ?? 0);
    } catch {
      /* skip */
    }
  });

  source.addEventListener("warning", (e: MessageEvent) => {
    callbacks.onWarning(e.data);
  });

  source.addEventListener("close", () => {
    source.close();
  });

  source.onerror = () => {
    if (source.readyState === EventSource.CLOSED) {
      callbacks.onError("Tail connection closed");
    }
    // EventSource auto-reconnects on transient errors — no action needed
  };

  return () => source.close();
}
