/**
 * Demo data + a tiny in-memory query engine. Enabled only via `dev-demo`
 * (VITE_DEMO=1); never imported in production builds.
 */

type Ev = Record<string, unknown>;

const LEVELS = ["error", "warn", "info", "debug"] as const;
const SOURCES = ["nginx", "api-gateway", "postgres", "redis"] as const;
const HOSTS = ["web-01", "web-02", "api-03", "db-01", "cache-02"];
const PATHS = [
  "/api/v1/login",
  "/api/v1/search",
  "/api/v1/users",
  "/api/v1/orders",
  "/healthz",
];
const MSGS = [
  "request completed",
  "auth failed for user",
  "connection refused upstream",
  "slow query detected",
  "cache miss for key",
  "rate limit exceeded",
  "timeout exceeded",
  "OOM killed worker",
];

function mulberry(seed: number) {
  return () => {
    seed |= 0;
    seed = (seed + 0x6d2b79f5) | 0;
    let t = Math.imul(seed ^ (seed >>> 15), 1 | seed);
    t = (t + Math.imul(t ^ (t >>> 7), 61 | t)) ^ t;
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

const NOW = Date.now();
const rnd = mulberry(42);

export const EVENTS: Ev[] = Array.from({ length: 420 }, (_, i) => {
  const level = LEVELS[Math.floor(rnd() * (i % 7 === 0 ? 4 : 3)) % 4];
  const source = SOURCES[Math.floor(rnd() * SOURCES.length)];
  const status = level === "error" ? [500, 503, 502][i % 3] : [200, 200, 404][i % 3];
  const host = HOSTS[Math.floor(rnd() * HOSTS.length)];
  const path = PATHS[Math.floor(rnd() * PATHS.length)];
  const msg = MSGS[Math.floor(rnd() * MSGS.length)];
  const ts = new Date(NOW - Math.floor(rnd() * 3600_000)).toISOString();
  const duration = +(rnd() * 1800).toFixed(1);
  return {
    _time: ts,
    _raw: `${ts} level=${level} host=${host} status=${status} path=${path} msg="${msg}" dur=${duration}ms`,
    _source: source,
    level,
    status,
    host,
    path,
    duration_ms: duration,
    message: msg,
    user_id: 1000 + (i % 57),
  };
}).sort((a, b) => String(b._time).localeCompare(String(a._time)));

const FIELD_TYPES: Record<string, string> = {
  _time: "datetime",
  _raw: "string",
  _source: "string",
  level: "string",
  status: "integer",
  host: "string",
  path: "string",
  duration_ms: "float",
  message: "string",
  user_id: "integer",
};

export function fieldCatalog() {
  const names = Object.keys(FIELD_TYPES);
  return {
    fields: names.map((name) => ({
      name,
      type: FIELD_TYPES[name] ?? "string",
      count: EVENTS.length,
      coverage: name === "user_id" ? 88 : 100,
    })),
  };
}

function matches(e: Ev, q: string): boolean {
  const filters = q.match(/(\w+)\s*(>=|<=|!=|=|>|<)\s*"?([^"|\s]+)"?/g) ?? [];
  for (const f of filters) {
    const m = f.match(/(\w+)\s*(>=|<=|!=|=|>|<)\s*"?([^"|\s]+)"?/);
    if (!m) continue;
    const [, k, op, v] = m;
    if (!k || !op || v === undefined || !(k in e)) continue;
    const ev = e[k];
    const nv = Number(v);
    const ne = Number(ev);
    const num = !isNaN(nv) && !isNaN(ne);
    if (op === "=" && String(ev) !== v) return false;
    if (op === "!=" && String(ev) === v) return false;
    if (op === ">=" && num && ne < nv) return false;
    if (op === "<=" && num && ne > nv) return false;
    if (op === ">" && num && ne <= nv) return false;
    if (op === "<" && num && ne >= nv) return false;
  }
  const text = q.match(/search\s+"([^"]+)"|^"([^"]+)"/);
  const term = text?.[1] ?? text?.[2];
  if (term && !String(e._raw).toLowerCase().includes(term.toLowerCase()))
    return false;
  return true;
}

export function runQuery(body: {
  q?: string;
  limit?: number;
  offset?: number;
}) {
  const q = (body.q ?? "").trim();
  let rows = EVENTS.filter((e) => matches(e, q));

  const statsBy = q.match(/\|\s*stats\s+count(?:\s*\(\s*\))?\s+by\s+(\w+)/i);
  const statsOnly = /\|\s*stats\s+count/i.test(q);
  const timechart = /\|\s*timechart/i.test(q);

  if (timechart) {
    const buckets = new Map<string, number>();
    for (const e of rows) {
      const k = String(e._time).slice(0, 16);
      buckets.set(k, (buckets.get(k) ?? 0) + 1);
    }
    const sorted = [...buckets.entries()].sort().slice(-30);
    return {
      data: {
        type: "timechart",
        columns: ["_time", "count"],
        rows: sorted.map(([t, c]) => [t, c]),
        total_rows: sorted.length,
      },
      meta: meta(rows.length),
    };
  }

  if (statsBy) {
    const by = statsBy[1] as string;
    const groups = new Map<string, number>();
    for (const e of rows)
      groups.set(String(e[by]), (groups.get(String(e[by])) ?? 0) + 1);
    const out = [...groups.entries()].sort((a, b) => b[1] - a[1]);
    return {
      data: {
        type: "aggregate",
        columns: [by, "count"],
        rows: out.map(([k, c]) => [k, c]),
        total_rows: out.length,
      },
      meta: meta(rows.length),
    };
  }

  if (statsOnly) {
    return {
      data: {
        type: "aggregate",
        columns: ["count"],
        rows: [[rows.length]],
        total_rows: 1,
      },
      meta: meta(rows.length),
    };
  }

  const head = q.match(/\|\s*head\s+(\d+)/i);
  if (head) rows = rows.slice(0, Number(head[1]));

  const total = rows.length;
  const offset = body.offset ?? 0;
  const limit = body.limit ?? 100;
  const page = rows.slice(offset, offset + limit);
  return {
    data: {
      type: "events",
      events: page,
      total,
      has_more: offset + limit < total,
    },
    meta: meta(total),
  };
}

function meta(scanned: number) {
  return {
    took_ms: +(2 + Math.random() * 9).toFixed(1),
    scanned,
    query_id: `qry_${Math.random().toString(16).slice(2, 10)}`,
    stats: {
      rows_scanned: EVENTS.length,
      rows_returned: scanned,
      matched_rows: scanned,
      segments_total: 12,
      segments_scanned: 9,
      segments_skipped_bloom: 2,
      segments_skipped_index: 1,
      parse_ms: 0.4,
      optimize_ms: 0.6,
      pipeline_ms: 1.2,
      indexes_used: ["main"],
      optimizer_rules: [
        { name: "PredicatePushdown", description: "Push filters to scan", count: 1 },
        { name: "ColumnPruning", description: "Drop unused columns", count: 1 },
      ],
      total_rules: 41,
    },
  };
}

export function histogram(grouped: boolean) {
  const buckets: Record<string, unknown>[] = [];
  for (let i = 30; i >= 0; i--) {
    const t = new Date(NOW - i * 120_000).toISOString();
    if (grouped) {
      buckets.push({
        time: t,
        counts: {
          error: Math.floor(Math.random() * 8),
          warn: Math.floor(Math.random() * 14),
          info: Math.floor(Math.random() * 40),
          debug: Math.floor(Math.random() * 20),
        },
      });
    } else {
      buckets.push({ time: t, count: Math.floor(10 + Math.random() * 70) });
    }
  }
  return { interval: "2m", buckets, total: EVENTS.length };
}

export function status() {
  return {
    health: "healthy",
    version: "0.1.0-demo",
    uptime_seconds: 18432,
    storage: { used_bytes: 184_320_512, segment_count: 12 },
    events: { total: EVENTS.length, today: EVENTS.length },
    queries: { active: 0 },
    views: { total: 2, active: 2 },
    tail: { active_sessions: 0, total_dropped_events: 0 },
  };
}

export function explain(q: string) {
  return {
    is_valid: true,
    parsed: {
      pipeline: [
        { command: "search", description: q || "match all" },
        { command: "where", description: "level filter" },
        { command: "stats", description: "aggregate" },
      ],
      result_type: "events",
      fields_read: ["level", "status", "_source"],
      estimated_cost: "low",
      optimizer_rules: [
        { name: "PredicatePushdown", count: 1 },
        { name: "TimeRangePruning", count: 1 },
      ],
      total_rules: 41,
    },
    acceleration: { available: false },
  };
}

export const SAVED_QUERIES = [
  {
    id: "sq_errors",
    name: "Errors by service",
    q: 'level=error | stats count by _source',
    created_at: new Date(NOW - 86400_000).toISOString(),
    updated_at: new Date(NOW - 86400_000).toISOString(),
  },
  {
    id: "sq_slow",
    name: "Slow requests",
    q: "duration_ms>1000 | sort -duration_ms | head 50",
    created_at: new Date(NOW - 3600_000).toISOString(),
    updated_at: new Date(NOW - 3600_000).toISOString(),
  },
];

export const CONFIG = {
  listen: "0.0.0.0:3100",
  data_dir: "/var/lib/lynxdb",
  retention: "30d",
  log_level: "info",
  query: {
    max_concurrent: 20,
    default_result_limit: 1000,
    max_result_limit: 50000,
  },
};

export const INDEXES = [
  { name: "main", retention_period: "30d", replication_factor: 1 },
  { name: "audit", retention_period: "90d", replication_factor: 2 },
];

export const VIEWS = [
  { name: "mv_errors_5m", status: "active", query: "level=error | stats count by _source", type: "materialized" },
  { name: "mv_5xx_hourly", status: "active", query: "status>=500 | timechart count", type: "materialized" },
];

export function tailEvent(): Ev {
  const e = EVENTS[Math.floor(Math.random() * EVENTS.length)] ?? EVENTS[0]!;
  return { ...e, _time: new Date().toISOString() };
}
