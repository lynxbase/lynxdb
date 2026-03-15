/**
 * URL hash encoding/decoding for query sharing.
 * Format: #q=level%3Derror&from=-1h&to=...
 */

/**
 * Write the current query and time range into the URL hash.
 * Uses replaceState to avoid polluting browser history.
 */
export function writeQueryToHash(
  query: string,
  fromRange: string,
  toRange?: string,
): void {
  const params = new URLSearchParams();
  params.set("q", query);
  params.set("from", fromRange);
  if (toRange) params.set("to", toRange);
  window.history.replaceState(null, "", "#" + params.toString());
}

/**
 * Read query and time range from the URL hash.
 * Returns null if hash is empty or has no `q` param.
 */
export function readQueryFromHash(): {
  q: string;
  from?: string;
  to?: string;
} | null {
  const hash = window.location.hash.slice(1);
  if (!hash) return null;
  const params = new URLSearchParams(hash);
  const q = params.get("q");
  if (!q) return null;
  return {
    q,
    from: params.get("from") || undefined,
    to: params.get("to") || undefined,
  };
}
