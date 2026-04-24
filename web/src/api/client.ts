import { authHeaders, handleAuthError } from "./auth";

const BASE = "";

/** All API responses are wrapped in {data: T, meta?: {...}} */
interface APIResponse<T> {
  data: T;
  meta?: {
    took_ms?: number;
    scanned?: number;
    query_id?: string;
    stats?: Record<string, unknown>;
  };
}

/** Wrapper around fetch that injects auth headers and handles 401. */
async function apiFetch(input: string, init?: RequestInit): Promise<Response> {
  const headers = { ...authHeaders(), ...init?.headers };
  const resp = await fetch(input, { ...init, headers });
  handleAuthError(resp);
  return resp;
}

/** Events query result */
export interface EventsResult {
  type: "events";
  events: Record<string, unknown>[];
  total: number;
  has_more: boolean;
}

/** Aggregate query result */
export interface AggregateResult {
  type: "aggregate" | "timechart";
  columns: string[];
  rows: unknown[][];
  total_rows: number;
}

export type QueryResult = EventsResult | AggregateResult;

/** A single optimizer rule applied during query optimization. */
export interface OptimizerRule {
  name: string;
  description?: string;
  count: number;
}

/** Detailed per-query performance stats from the backend metaStats envelope. */
export interface DetailedStats {
  rows_scanned?: number;
  rows_returned?: number;
  matched_rows?: number;
  segments_total?: number;
  segments_scanned?: number;
  segments_skipped_bloom?: number;
  segments_skipped_time?: number;
  segments_skipped_index?: number;
  segments_skipped_stats?: number;
  segments_skipped_range?: number;
  inverted_index_hits?: number;
  count_star_optimized?: boolean;
  partial_agg_used?: boolean;
  topk_used?: boolean;
  prefetch_used?: boolean;
  vectorized_filter_used?: boolean;
  dict_filter_used?: boolean;
  cache_hit?: boolean;
  scan_ms?: number;
  pipeline_ms?: number;
  parse_ms?: number;
  optimize_ms?: number;
  optimizer_rules?: OptimizerRule[];
  total_rules?: number;
  accelerated_by?: string;
  mv_speedup?: string;
  processed_bytes?: number;
  disk_bytes_read?: number;
  s3_bytes_read?: number;
  cache_bytes_read?: number;
}

/** Query execution stats from meta envelope */
export interface QueryStats {
  took_ms: number;
  scanned: number;
  query_id?: string;
  stats?: DetailedStats & { [key: string]: unknown };
}

export interface QueryResponse {
  result: QueryResult;
  stats: QueryStats;
}

export async function executeQuery(
  query: string,
  from?: string,
  to?: string,
  limit?: number,
  offset?: number,
  variables?: Record<string, string>,
): Promise<QueryResponse> {
  const body: Record<string, unknown> = { q: query };
  if (from) body.from = from;
  if (to) body.to = to;
  if (limit) body.limit = limit;
  if (offset) body.offset = offset;
  if (variables && Object.keys(variables).length > 0) body.variables = variables;

  const resp = await apiFetch(`${BASE}/api/v1/query`, {
    method: "POST",
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const err = await resp
      .json()
      .catch(() => ({ error: { message: resp.statusText } }));
    throw new Error(
      err.error?.message || err.data?.error || resp.statusText,
    );
  }

  const json = await resp.json();
  return {
    result: json.data as QueryResult,
    stats: {
      took_ms: json.meta?.took_ms ?? 0,
      scanned: json.meta?.scanned ?? 0,
      query_id: json.meta?.query_id,
      stats: json.meta?.stats,
    },
  };
}

export interface FieldInfo {
  name: string;
  type: string;
  count: number;
  coverage: number;
}

export async function fetchFields(): Promise<FieldInfo[]> {
  const resp = await apiFetch(`${BASE}/api/v1/fields`);
  if (!resp.ok) throw new Error("Failed to fetch fields");
  const json: APIResponse<{ fields: FieldInfo[] }> = await resp.json();
  return json.data.fields;
}

export interface FieldValue {
  value: string;
  count: number;
}

export async function fetchFieldValues(
  name: string,
  limit = 10,
): Promise<FieldValue[]> {
  const resp = await apiFetch(
    `${BASE}/api/v1/fields/${encodeURIComponent(name)}/values?limit=${limit}`,
  );
  if (!resp.ok) throw new Error("Failed to fetch field values");
  const json = await resp.json();
  return json.data.values ?? json.data ?? [];
}

export interface IndexInfo {
  name: string;
  retention_period: string;
  replication_factor: number;
}

export async function fetchIndexes(): Promise<IndexInfo[]> {
  const resp = await apiFetch(`${BASE}/api/v1/indexes`);
  if (!resp.ok) throw new Error("Failed to fetch indexes");
  const json: APIResponse<{ indexes: IndexInfo[] }> = await resp.json();
  return json.data.indexes;
}

export interface HistogramBucket {
  time: string;
  count: number;
}

export interface HistogramResult {
  interval: string;
  buckets: HistogramBucket[];
  total: number;
}

export async function fetchHistogram(
  from?: string,
  to?: string,
  buckets = 50,
  index?: string,
): Promise<HistogramResult> {
  const params = new URLSearchParams();
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  params.set("buckets", String(buckets));
  if (index) params.set("index", index);

  const resp = await apiFetch(`${BASE}/api/v1/histogram?${params}`);
  if (!resp.ok) throw new Error("Failed to fetch histogram");
  const json = await resp.json();
  return json.data;
}

export interface HistogramBucketGrouped {
  time: string;
  counts: Record<string, number>;
}

export interface HistogramResultGrouped {
  interval: string;
  buckets: HistogramBucketGrouped[];
  total: number;
}

export async function fetchHistogramGrouped(
  from?: string,
  to?: string,
  buckets = 60,
  groupBy = "level",
): Promise<HistogramResultGrouped> {
  const params = new URLSearchParams();
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  params.set("buckets", String(buckets));
  params.set("group_by", groupBy);

  const resp = await apiFetch(`${BASE}/api/v1/histogram?${params}`);
  if (!resp.ok) throw new Error("Failed to fetch grouped histogram");
  const json = await resp.json();
  return json.data;
}

export async function fetchStatus(): Promise<Record<string, unknown>> {
  const resp = await apiFetch(`${BASE}/api/v1/status`);
  if (!resp.ok) throw new Error("Failed to fetch status");
  const json = await resp.json();
  return json.data;
}

// Explain (pipeline introspection)

export interface PipelineStage {
  command: string;
  description?: string;
  fields_added?: string[];
  fields_removed?: string[];
  fields_out?: string[];
  fields_optional?: string[];
  fields_unknown?: boolean;
}

export interface ExplainResult {
  is_valid: boolean;
  errors?: { message: string; suggestion?: string }[];
  parsed?: {
    pipeline: PipelineStage[];
    result_type: string;
    fields_read: string[];
    source_scope?: {
      type: string;
      resolved_sources?: string[];
      pattern?: string;
      total_sources_available?: number;
    };
    // Extended fields for explain inspector
    estimated_cost?: string;
    uses_full_scan?: boolean;
    search_terms?: string[];
    has_time_bounds?: boolean;
    optimizer_stats?: Record<string, number>;
    optimizer_rules?: OptimizerRule[];
    total_rules?: number;
    parse_ms?: number;
    optimize_ms?: number;
    physical_plan?: {
      count_star_only?: boolean;
      partial_agg?: boolean;
      topk_agg?: boolean;
      topk?: number;
      join_strategy?: string;
    };
    optimizer_messages?: string[];
    optimizer_warnings?: string[];
  };
  acceleration?: { available: boolean };
}

export async function fetchExplain(
  q: string,
  from?: string,
  to?: string,
): Promise<ExplainResult> {
  const params = new URLSearchParams({ q });
  if (from) params.set("from", from);
  if (to) params.set("to", to);

  const resp = await apiFetch(`${BASE}/api/v1/query/explain?${params}`);
  if (!resp.ok) {
    const err = await resp
      .json()
      .catch(() => ({ error: { message: resp.statusText } }));
    throw new Error(
      err.error?.message || err.data?.error || resp.statusText,
    );
  }
  const json = await resp.json();
  return json.data as ExplainResult;
}

// Materialized views

export interface ViewSummary {
  name: string;
  status: string;
  query: string;
  type: string;
}

export interface ViewDetail extends ViewSummary {
  columns: { name: string; type: string }[];
  retention: string;
}

export async function fetchViews(): Promise<ViewSummary[]> {
  const resp = await apiFetch(`${BASE}/api/v1/views`);
  if (!resp.ok) throw new Error("Failed to fetch views");
  const json: APIResponse<{ views: ViewSummary[] }> = await resp.json();
  return json.data.views ?? [];
}

export async function fetchViewDetail(name: string): Promise<ViewDetail> {
  const resp = await apiFetch(
    `${BASE}/api/v1/views/${encodeURIComponent(name)}`,
  );
  if (!resp.ok) throw new Error("Failed to fetch view detail");
  const json: APIResponse<ViewDetail> = await resp.json();
  return json.data;
}

// Re-export streaming types for convenience

export type {
  HybridResult,
  StreamCallbacks,
  ProgressData,
} from "./streaming";

export {
  submitHybridQuery,
  streamQuery,
  subscribeJobProgress,
  cancelJob,
} from "./streaming";
