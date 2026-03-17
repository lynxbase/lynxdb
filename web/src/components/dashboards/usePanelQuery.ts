import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import { executeQuery } from "../../api/client";
import type { QueryResult } from "../../api/client";

/**
 * Hook for executing a panel's SPL2 query with generation counter
 * to prevent stale responses from overwriting newer results.
 */
export function usePanelQuery(
  query: string,
  from: string,
  to: string | undefined,
  variables: Record<string, string>,
  refreshTick?: number,
): {
  result: QueryResult | null;
  loading: boolean;
  error: string | null;
  refresh: () => void;
} {
  const [result, setResult] = useState<QueryResult | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const generation = useRef(0);

  const execute = useCallback(async () => {
    const gen = ++generation.current;
    setLoading(true);
    setError(null);
    try {
      const resp = await executeQuery(query, from, to, 1000, 0, variables);
      if (gen === generation.current) {
        setResult(resp.result);
      }
    } catch (err) {
      if (gen === generation.current) {
        setError(err instanceof Error ? err.message : "Query failed");
      }
    } finally {
      if (gen === generation.current) {
        setLoading(false);
      }
    }
  }, [query, from, to, JSON.stringify(variables)]);

  useEffect(() => {
    execute();
  }, [execute, refreshTick]);

  return { result, loading, error, refresh: execute };
}
