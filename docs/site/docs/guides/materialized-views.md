---
title: Accelerate Queries with Materialized Views
description: How to create, manage, and benefit from materialized views in LynxDB for up to 400x faster queries through precomputed aggregations.
---

# Accelerate Queries with Materialized Views

Materialized views precompute aggregations in the background so that repeated queries return results up to 400x faster. You define a view once, and LynxDB automatically uses it to accelerate matching queries without any changes to your query syntax.

## How materialized views work

1. You define a view with an SPL2 aggregation query and a time bucket.
2. LynxDB runs the aggregation continuously against incoming data and stores the precomputed results.
3. When you run a query that matches the view's aggregation pattern, the optimizer rewrites the query to read from the view instead of scanning raw events.
4. The view also backfills historical data automatically.

---

## Create a materialized view

### Via the CLI

Use [`lynxdb mv create`](/docs/cli/mv):

```bash
lynxdb mv create mv_errors_5m \
  'level=error | stats count, avg(duration) by source, time_bucket(_timestamp, "5m") AS bucket' \
  --retention 90d
```

This creates a view named `mv_errors_5m` that:

- Filters for error-level events
- Computes `count` and `avg(duration)` per source per 5-minute bucket
- Retains precomputed data for 90 days

### Via the REST API

Use [`POST /api/v1/views`](/docs/api/views):

```bash
curl -X POST localhost:3100/api/v1/views -d '{
  "name": "mv_errors_5m",
  "q": "level=error | stats count, avg(duration) by source, time_bucket(_timestamp, \"5m\") AS bucket",
  "retention": "90d"
}'
```

---

## Automatic query acceleration

Once the view is active, matching queries are automatically accelerated. You do not change your queries:

```bash
lynxdb query 'level=error | stats count by source' --since 7d
```

LynxDB detects that this query can be satisfied by `mv_errors_5m` and reads from the view instead of scanning raw events:

```
  SOURCE      COUNT
  ─────────────────────
  nginx       142,847
  api-gw       89,234

  Accelerated by mv_errors_5m (~400x, 3ms vs ~1.2s)
```

The response metadata includes the acceleration info so you can verify it is working.

---

## Check view status

### Via the CLI

```bash
lynxdb mv status mv_errors_5m
```

```
Name:       mv_errors_5m
Status:     active
Query:      level=error | stats count, avg(duration) by source, time_bucket(...)
Retention:  90d
```

### Via the API

```bash
curl -s localhost:3100/api/v1/views/mv_errors_5m | jq .
```

### View statuses

| Status | Meaning |
|--------|---------|
| `backfilling` | Processing historical data. Partial results are available during backfill. |
| `active` | Fully caught up and processing new data in real time. |
| `paused` | Manually paused. No new data is processed. |
| `error` | View encountered an error. Check logs for details. |

---

## List all views

```bash
lynxdb mv list
```

```
NAME            STATUS       QUERY
mv_errors_5m    active       level=error | stats count, avg(duration) by ...
mv_5xx_hourly   backfilling  source=nginx status>=500 | stats count, p95(dur...
```

---

## Cascading views

Build views on top of other views to create multi-granularity rollups. This is useful when you want both fine-grained (5-minute) and coarse (hourly, daily) aggregations:

```bash
# Base view: 5-minute buckets
lynxdb mv create mv_errors_5m \
  'level=error | stats count, avg(duration) by source, time_bucket(_timestamp, "5m") AS bucket' \
  --retention 90d

# Hourly rollup (reads from the 5-minute view, not raw events)
lynxdb mv create mv_errors_1h \
  '| from mv_errors_5m | stats sum(count) AS count by source, time_bucket(bucket, "1h") AS hour' \
  --retention 365d

# Daily rollup (reads from the hourly view)
lynxdb mv create mv_errors_1d \
  '| from mv_errors_1h | stats sum(count) AS count by source, time_bucket(hour, "1d") AS day' \
  --retention 730d
```

Cascading views are efficient because each level reads from precomputed data rather than re-scanning raw events.

---

## Backfill behavior

When you create a view, LynxDB automatically backfills it with existing historical data. During backfill:

- The view status is `backfilling`.
- Partial results are available immediately. Queries that hit the view will return results for the time ranges that have been processed so far.
- New incoming data is processed in parallel with the backfill.
- Backfill completes in the background. The view transitions to `active` when all historical data has been processed.

You do not need to wait for backfill to complete before querying.

---

## Retention policies

Set a retention period to automatically discard old precomputed data:

```bash
lynxdb mv create mv_5xx_hourly \
  '_source=nginx status>=500 | stats count, perc95(duration_ms) by uri, time_bucket(_timestamp, "1h") AS hour' \
  --retention 30d
```

After 30 days, the oldest buckets are dropped. This keeps storage usage bounded.

---

## Pause and resume

Temporarily stop a view from processing new data:

```bash
lynxdb mv pause mv_errors_5m
lynxdb mv resume mv_errors_5m
```

Pausing is useful during maintenance or when debugging unexpected results. The view retains its existing data and catches up when resumed.

---

## Drop a view

Delete a materialized view and all its precomputed data:

```bash
lynxdb mv drop mv_errors_5m
```

Use `--force` to skip the confirmation prompt:

```bash
lynxdb mv drop mv_errors_5m --force
```

Use `--dry-run` to see what would be deleted without actually deleting:

```bash
lynxdb mv drop mv_errors_5m --dry-run
```

---

## Versioned rebuilds

When you update a view definition, LynxDB performs a versioned rebuild with zero downtime:

1. The new view definition starts backfilling alongside the old version.
2. Queries continue to use the old version during the rebuild.
3. When the new version catches up, queries switch to it.
4. The old version is dropped.

This means you can update views without any query downtime.

---

## View design guidelines

### Choose the right time bucket

| Use case | Suggested bucket | Retention |
|----------|-----------------|-----------|
| Real-time monitoring | `5m` | 30-90 days |
| Hourly reports | `1h` | 90-365 days |
| Daily trend analysis | `1d` | 1-2 years |

### Include the right aggregations

Think about what queries you run most often and include those aggregations in the view:

```bash
# If you often query count, avg, and p99, include all three:
lynxdb mv create mv_nginx_5m \
  '_source=nginx | stats count, avg(duration_ms), perc99(duration_ms) by uri, time_bucket(_timestamp, "5m") AS bucket' \
  --retention 90d
```

### Match your query patterns

The optimizer can only rewrite queries that match the view's aggregation pattern. If your view groups by `source` and `bucket`, queries that group by `source` (without bucket) will match. Queries that group by `host` will not.

---

## Practical example: full monitoring stack

```bash
# 1. Fine-grained error tracking
lynxdb mv create mv_errors_5m \
  'level=error | stats count by source, time_bucket(_timestamp, "5m") AS bucket' \
  --retention 90d

# 2. Nginx latency tracking
lynxdb mv create mv_nginx_latency_5m \
  '_source=nginx | stats count, avg(duration_ms), perc95(duration_ms), perc99(duration_ms) by uri, time_bucket(_timestamp, "5m") AS bucket' \
  --retention 90d

# 3. Hourly rollup for long-term trends
lynxdb mv create mv_errors_1h \
  '| from mv_errors_5m | stats sum(count) AS count by source, time_bucket(bucket, "1h") AS hour' \
  --retention 365d

# 4. Now your repeat queries are instant:
lynxdb query 'level=error | stats count by source' --since 7d
lynxdb query '_source=nginx | stats avg(duration_ms), p99(duration_ms) by uri' --since 24h
```

---

## Materialized views in cluster mode

In a distributed LynxDB cluster, materialized view definitions are stored in the Raft FSM, making them consistent across all nodes. The view lifecycle works across the cluster automatically.

### How cluster views work

- **Registration**: When you create a view, the definition is committed to the Raft FSM via the `RegisterView` RPC. All meta nodes store the same view definition.
- **Coordination**: Each view has a `CoordinatorNode` that manages its lifecycle (backfill, status transitions). If the coordinator node fails, a new one is assigned.
- **Backfill**: The backfill coordinator uses scatter-gather to process historical data across all shards, then aggregates results.
- **Status tracking**: View status (`pending` → `running` → `complete`) and backfill progress are tracked in the FSM, visible from any node.
- **Query acceleration**: The optimizer on any query node can rewrite queries to use cluster-wide views. The acceleration metadata (view name, speedup) is included in the query response.

### View operations in cluster mode

All view operations (create, list, status, drop, pause, resume) work identically in single-node and cluster modes. The CLI and API commands are the same -- the cluster coordination is transparent.

---

## Next steps

- [Time series analysis](/docs/guides/time-series) -- understand time bucketing patterns
- [CLI: `mv`](/docs/cli/mv) -- complete CLI reference for materialized view commands
- [REST API: Views](/docs/api/views) -- full API reference for view CRUD
