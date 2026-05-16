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

import { useAuthStore } from "./auth";

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
  onReconnecting?: (isReconnecting: boolean) => void;
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
  const tokenValue = useAuthStore.getState().token;
  if (tokenValue) {
    params.set("_token", tokenValue);
  }

  const source = new EventSource(`/api/v1/tail?${params}`);

  // Debounced reconnection state: only fire onReconnecting(true) after 1s
  // of continuous disconnection to avoid flicker on transient errors (Pitfall 4).
  let reconnectTimer: ReturnType<typeof setTimeout> | undefined;

  source.onopen = () => {
    clearTimeout(reconnectTimer);
    callbacks.onReconnecting?.(false);
  };

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
    } else if (source.readyState === EventSource.CONNECTING) {
      // Debounce: only show reconnecting if disconnected > 1s (Pitfall 4)
      clearTimeout(reconnectTimer);
      reconnectTimer = setTimeout(() => {
        callbacks.onReconnecting?.(true);
      }, 1000);
    }
  };

  return () => {
    clearTimeout(reconnectTimer);
    source.close();
  };
}
