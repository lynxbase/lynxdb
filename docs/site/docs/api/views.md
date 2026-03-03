---
sidebar_position: 9
title: Materialized Views
description: CRUD /views -- create, manage, and monitor materialized views for query acceleration.
---

# Materialized Views API

Materialized views (MVs) are precomputed aggregations and projections that accelerate repeated queries by 100--400x. LynxDB automatically routes matching queries through MVs -- no query changes required.

## GET /views

List all materialized views with operational status, storage, lag, and backfill progress.

```bash
curl -s localhost:3100/api/v1/views | jq .
```

**Response (200):**

```json
{
  "data": {
    "views": [
      {
        "name": "mv_errors_5m",
        "kind": "aggregation",
        "query": "level=error | stats count, avg(duration) by source, time_bucket(timestamp, '5m') AS bucket",
        "retention": "90d",
        "status": "active",
        "version": 1,
        "rows": 142847,
        "segments": 12,
        "storage_bytes": 12582912,
        "lag_ms": 1200,
        "created_at": "2026-02-12T10:00:00Z",
        "last_event": "2026-02-14T14:52:01Z"
      },
      {
        "name": "mv_5xx_hourly",
        "kind": "aggregation",
        "query": "source=nginx status>=500 | stats count, p95(duration) by uri, time_bucket(timestamp, '1h') AS hour",
        "retention": "365d",
        "status": "backfilling",
        "version": 1,
        "rows": 6720,
        "segments": 3,
        "storage_bytes": 348160,
        "lag_ms": null,
        "backfill": {
          "total": 12600000,
          "processed": 8400000,
          "percent": 66.7,
          "eta_seconds": 134
        },
        "created_at": "2026-02-14T14:30:00Z",
        "last_event": null
      }
    ]
  }
}
```

### View Summary Object

| Field | Type | Description |
|---|---|---|
| `name` | string | View name (must start with `mv_`) |
| `kind` | string | `aggregation` or `projection` |
| `query` | string | Source SPL2 pipeline |
| `retention` | string | Data retention period |
| `status` | string | `active`, `backfilling`, `rebuilding`, `paused`, `error` |
| `version` | integer | Schema version (incremented on rebuild) |
| `rows` | integer | Total rows in the view |
| `segments` | integer | Number of storage segments |
| `storage_bytes` | integer | Disk space used |
| `lag_ms` | integer | Milliseconds behind real-time (null during backfill) |
| `backfill` | object | Backfill progress (present when `status` is `backfilling` or `rebuilding`) |
| `created_at` | string | Creation timestamp |
| `last_event` | string | Most recent event processed (null during initial backfill) |

---

## POST /views

Create a materialized view (or trigger a versioned rebuild of an existing one). This is the REST equivalent of `| materialize`.

### Request Body

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | View name (must start with `mv_`, lowercase, alphanumeric with underscores) |
| `q` | string | Yes | SPL2 pipeline (without `| materialize`) |
| `retention` | string | No | Retention period (default: same as index retention) |
| `partition_by` | string | No | Partition strategy expression |

### Headers

| Header | Required | Description |
|---|---|---|
| `If-None-Match` | No | Set to `*` to prevent accidental rebuild of existing view (returns `409` if view exists) |

### Create an Aggregation MV

```bash
curl -X POST localhost:3100/api/v1/views \
  -d '{
    "name": "mv_errors_5m",
    "q": "level=error | stats count, avg(duration) by source, time_bucket(timestamp, '\''5m'\'') AS bucket",
    "retention": "90d"
  }'
```

**Response -- new view created (201):**

```json
{
  "data": {
    "name": "mv_errors_5m",
    "kind": "aggregation",
    "query": "level=error | stats count, avg(duration) by source, time_bucket(timestamp, '5m') AS bucket",
    "retention": "90d",
    "status": "backfilling",
    "version": 1,
    "backfill": {
      "total": 12400000,
      "processed": 0,
      "percent": 0
    }
  }
}
```

### Create a Projection MV

```bash
curl -X POST localhost:3100/api/v1/views \
  -d '{
    "name": "mv_access",
    "q": "source=nginx | extract timestamp, method, uri, status, size, duration",
    "retention": "14d",
    "partition_by": "date(timestamp)"
  }'
```

### Create a Cascading MV (Built on Another MV)

```bash
curl -X POST localhost:3100/api/v1/views \
  -d '{
    "name": "mv_errors_1h",
    "q": "| from mv_errors_5m | stats sum(count) AS count by source, time_bucket(bucket, '\''1h'\'') AS hour",
    "retention": "365d"
  }'
```

### Rebuild an Existing MV

If a view with the same name already exists, POSTing triggers a **versioned rebuild**. The old version keeps serving queries while the new version builds in the background. Once complete, an atomic swap is performed.

**Response -- rebuild triggered (200):**

```json
{
  "data": {
    "name": "mv_errors_5m",
    "kind": "aggregation",
    "query": "level=error | stats count, avg(duration), p99(duration) by source, time_bucket(timestamp, '5m') AS bucket",
    "retention": "90d",
    "status": "rebuilding",
    "version": 2,
    "previous_version": {
      "version": 1,
      "status": "active"
    },
    "backfill": {
      "total": 12400000,
      "processed": 0,
      "percent": 0
    }
  }
}
```

### Prevent Accidental Rebuild

Use the `If-None-Match: *` header to get a `409 Conflict` if the view already exists:

```bash
curl -X POST localhost:3100/api/v1/views \
  -H "If-None-Match: *" \
  -d '{
    "name": "mv_errors_5m",
    "q": "level=error | stats count by source, time_bucket(timestamp, '\''5m'\'') AS bucket",
    "retention": "90d"
  }'
```

**Response (409):**

```json
{
  "error": {
    "code": "ALREADY_EXISTS",
    "message": "View 'mv_errors_5m' already exists. Remove If-None-Match header to trigger rebuild."
  }
}
```

---

## GET /views/\{name\}

Get full MV details including column schema, aggregation info, and operational statistics.

```bash
curl -s localhost:3100/api/v1/views/mv_errors_5m | jq .
```

**Response (200):**

```json
{
  "data": {
    "name": "mv_errors_5m",
    "kind": "aggregation",
    "query": "level=error | stats count, avg(duration) by source, time_bucket(timestamp, '5m') AS bucket",
    "retention": "90d",
    "status": "active",
    "version": 1,
    "columns": [
      {"name": "source", "type": "string", "encoding": "dictionary"},
      {"name": "bucket", "type": "timestamp", "encoding": "delta-of-delta"},
      {"name": "count", "type": "int64", "encoding": "delta-varint"},
      {"name": "avg(duration)", "type": "float64", "derived_from": ["_sum_duration", "_count_duration"]}
    ],
    "group_by": ["source", "bucket"],
    "aggregations": ["count", "avg(duration)"],
    "stats": {
      "rows": 142847,
      "segments": 12,
      "segments_pending_merge": 3,
      "storage_bytes": 12582912,
      "compression_ratio": 18.4,
      "lag_ms": 1200,
      "ingest_rate": 234.5,
      "oldest_event": "2026-01-15T00:00:01Z",
      "newest_event": "2026-02-14T14:52:01Z"
    },
    "source_view": null,
    "created_at": "2026-02-12T10:00:00Z"
  }
}
```

### Column Object

| Field | Type | Description |
|---|---|---|
| `name` | string | Column name |
| `type` | string | Data type: `string`, `int64`, `float64`, `timestamp`, `boolean` |
| `encoding` | string | Storage encoding: `dictionary`, `delta-of-delta`, `delta-varint`, `gorilla`, `bitpacked` |
| `derived_from` | array | Internal state columns for computed aggregates (e.g., `avg` = `sum/count`) |

### Stats Object

| Field | Type | Description |
|---|---|---|
| `rows` | integer | Total rows |
| `segments` | integer | Storage segments |
| `segments_pending_merge` | integer | Segments awaiting compaction |
| `storage_bytes` | integer | Disk space used |
| `compression_ratio` | number | Compression ratio |
| `lag_ms` | integer | Milliseconds behind real-time |
| `ingest_rate` | number | Current ingest rate (rows per second) |
| `oldest_event` | string | Earliest event timestamp |
| `newest_event` | string | Latest event timestamp |

---

## PATCH /views/\{name\}

Update mutable MV properties. Only `retention` and `paused` can be changed without a rebuild. To change `q`, `partition_by`, or `group_by`, use `POST /views` with the same name (triggers a rebuild).

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `name` | Yes | View name |

### Change Retention

```bash
curl -X PATCH localhost:3100/api/v1/views/mv_errors_5m \
  -d '{"retention": "30d"}'
```

### Pause MV Pipeline

```bash
curl -X PATCH localhost:3100/api/v1/views/mv_errors_5m \
  -d '{"paused": true}'
```

### Resume MV Pipeline

```bash
curl -X PATCH localhost:3100/api/v1/views/mv_errors_5m \
  -d '{"paused": false}'
```

**Response (200):** Updated view summary object.

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | View not found |

---

## DELETE /views/\{name\}

Delete an MV and all its data.

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `name` | Yes | View name |

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `force` | No | `false` | Delete view and all cascading dependents |

```bash
curl -X DELETE localhost:3100/api/v1/views/mv_errors_5m
```

**Response:** `204 No Content`

### Cascading Dependents

If the view has dependent views (cascading MVs built on top of it), the delete is rejected:

```json
{
  "error": {
    "code": "HAS_DEPENDENTS",
    "message": "Cannot delete 'mv_errors_5m': view 'mv_errors_1h' depends on it.",
    "dependents": ["mv_errors_1h"],
    "suggestion": "Delete dependent views first, or use ?force=true to delete all."
  }
}
```

Force delete with all dependents:

```bash
curl -X DELETE "localhost:3100/api/v1/views/mv_errors_5m?force=true"
```

---

## GET /views/\{name\}/backfill

Get backfill or rebuild progress for a view.

```bash
curl -s localhost:3100/api/v1/views/mv_5xx_hourly/backfill | jq .
```

**Response -- backfill in progress (200):**

```json
{
  "data": {
    "status": "backfilling",
    "version": 1,
    "total": 12600000,
    "processed": 8400000,
    "percent": 66.7,
    "rate": 62500,
    "eta_seconds": 67,
    "started_at": "2026-02-14T14:30:00Z",
    "cursor": "seg-004:offset-847291",
    "errors": 0
  }
}
```

**Response -- no active backfill (200):**

```json
{
  "data": {
    "status": "idle",
    "last_completed": "2026-02-12T10:04:23Z"
  }
}
```

### Backfill Object

| Field | Type | Description |
|---|---|---|
| `status` | string | `backfilling`, `rebuilding`, or `idle` |
| `version` | integer | Version being built |
| `total` | integer | Total events to process |
| `processed` | integer | Events processed so far |
| `percent` | number | Progress percentage |
| `rate` | number | Processing rate (events per second) |
| `eta_seconds` | integer | Estimated time remaining |
| `started_at` | string | Backfill start time |
| `cursor` | string | Internal bookmark (segment:offset) |
| `errors` | integer | Number of processing errors |

## View Statuses

| Status | Description |
|---|---|
| `active` | View is up to date and serving queries |
| `backfilling` | Initial data population in progress (partial results available) |
| `rebuilding` | Versioned rebuild in progress (old version still serving) |
| `paused` | Pipeline paused (view stops consuming new events) |
| `error` | Processing error (check backfill endpoint for details) |

## Related

- **[`lynxdb mv` CLI command](/docs/cli/mv)** -- manage MVs from the command line
- **[Materialized Views guide](/docs/guides/materialized-views)** -- design patterns and best practices
- **[Query API](/docs/api/query)** -- queries are automatically accelerated by matching MVs
- **[Architecture: Storage Engine](/docs/architecture/storage-engine)** -- how MVs are stored
