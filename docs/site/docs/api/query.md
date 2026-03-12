---
sidebar_position: 3
title: Query
description: POST /query (sync/async/hybrid), streaming export, query explain, and async job management.
---

# Query API

Execute SPL2 queries against LynxDB. Supports synchronous, asynchronous, and hybrid execution modes, NDJSON streaming export, query explain, and full async job management with progress tracking.

## POST /query

Core search endpoint. Executes any SPL2 pipeline including search, aggregation, and management commands.

### Request Body

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `q` | string | Yes | -- | SPL2 query string |
| `from` | string | No | `"-15m"` | Start time: relative (`-1h`, `-7d`) or ISO 8601 |
| `to` | string | No | `"now"` | End time: relative or ISO 8601 |
| `limit` | integer | No | `1000` | Max events to return (max 50,000) |
| `offset` | integer | No | `0` | Offset for pagination (tabular results only) |
| `format` | string | No | `"json"` | Output format: `json`, `csv`, `raw` |
| `wait` | number | No | `null` | Execution mode control (see below) |

### Execution Modes

The `wait` parameter controls sync/async behavior:

| `wait` value | Mode | Behavior | Response |
|---|---|---|---|
| `null` (default) | **Sync** | Block until complete or server timeout (30s) | `200` with results, or `408` on timeout |
| `0` | **Async** | Return immediately with a job handle | `202` with job handle |
| `N` (seconds) | **Hybrid** | Wait up to N seconds | `200` if done in time, `202` + job handle otherwise |

Hybrid mode is ideal for Web UI -- fast queries return instantly, slow queries degrade gracefully to async with progress tracking.

### Sync Query (Default)

```bash
curl -s localhost:3100/api/v1/query \
  -d '{
    "q": "level=error",
    "from": "-1h",
    "limit": 100
  }' | jq .
```

**Response -- events result (200):**

```json
{
  "data": {
    "type": "events",
    "events": [
      {
        "_id": "01JKNM3VXQP...",
        "_timestamp": "2026-02-14T14:52:01.234Z",
        "_source": "nginx",
        "level": "error",
        "status": 502,
        "uri": "/api/v1/users",
        "method": "GET",
        "duration_ms": 12
      }
    ],
    "total": 1247,
    "has_more": true
  },
  "meta": {
    "took_ms": 89,
    "scanned": 12400000,
    "query_id": "qry_7f3a..."
  }
}
```

### Aggregation Query

```bash
curl -s localhost:3100/api/v1/query \
  -d '{
    "q": "source=nginx status>=500 | stats count by uri | sort -count | head 10",
    "from": "-1h",
    "to": "now"
  }' | jq .
```

**Response -- aggregate result (200):**

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["uri", "count"],
    "rows": [
      ["/api/v1/users", 1247],
      ["/api/v1/orders", 893],
      ["/health", 412]
    ],
    "total_rows": 42
  },
  "meta": {
    "took_ms": 34,
    "scanned": 12400000,
    "query_id": "qry_8b2c..."
  }
}
```

### Timechart Query

```bash
curl -s localhost:3100/api/v1/query \
  -d '{
    "q": "level=error | timechart count span=5m",
    "from": "-6h"
  }' | jq .
```

**Response -- timechart result (200):**

```json
{
  "data": {
    "type": "timechart",
    "interval": "5m",
    "columns": ["_time", "count"],
    "rows": [
      ["2026-02-14T14:00:00Z", 42],
      ["2026-02-14T14:05:00Z", 87],
      ["2026-02-14T14:10:00Z", 156]
    ]
  },
  "meta": {
    "took_ms": 45,
    "scanned": 12400000,
    "query_id": "qry_9c1d..."
  }
}
```

### MV-Accelerated Query

When a [materialized view](/docs/api/views) covers the query, LynxDB automatically uses it. The `meta.accelerated_by` field indicates acceleration:

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["source", "count"],
    "rows": [
      ["nginx", 142847],
      ["api-gw", 89234]
    ],
    "total_rows": 5
  },
  "meta": {
    "took_ms": 3,
    "scanned": 142847,
    "query_id": "qry_d4e1...",
    "accelerated_by": {
      "view": "mv_errors_5m",
      "original_scan": 12400000,
      "speedup": "~400x"
    }
  }
}
```

If the MV is still backfilling, you get partial results with coverage info:

```json
{
  "meta": {
    "accelerated_by": {
      "view": "mv_errors_5m",
      "status": "backfilling",
      "coverage_percent": 66.7
    }
  }
}
```

### Hybrid Mode

Wait up to 5 seconds, then fall back to async:

```bash
curl -s localhost:3100/api/v1/query \
  -d '{
    "q": "* | stats count by source, status",
    "from": "-30d",
    "wait": 5
  }' | jq .
```

If the query finishes within 5 seconds, you get `200` with results. If not, you get `202` with a job handle:

**Response -- hybrid fallback (202):**

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_7f3a2b",
    "status": "running",
    "query": "* | stats count by source",
    "from": "-30d",
    "to": "now",
    "progress": {
      "phase": "scanning",
      "scanned": 2100000000,
      "total_estimate": 10400000000,
      "percent": 20.2,
      "events_matched": 847291,
      "elapsed_ms": 5000,
      "eta_ms": 19700
    },
    "partial_results": {
      "type": "aggregate",
      "columns": ["source", "count"],
      "rows": [
        ["nginx", 142000],
        ["api-gw", 71000]
      ],
      "note": "Based on 20% of data. Final values will change."
    }
  }
}
```

### Async Mode

Return a job handle immediately:

```bash
curl -s localhost:3100/api/v1/query \
  -d '{
    "q": "* | stats dc(user_id) by source",
    "from": "-90d",
    "wait": 0
  }' | jq .
```

**Response (202):**

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "running",
    "query": "* | stats dc(user_id) by source",
    "from": "-90d",
    "to": "now",
    "progress": {
      "phase": "scanning",
      "scanned": 0,
      "total_estimate": 84700000000,
      "percent": 0,
      "events_matched": 0,
      "elapsed_ms": 0,
      "eta_ms": null
    }
  }
}
```

Poll progress with `GET /query/jobs/{job_id}` or subscribe to SSE with `GET /query/jobs/{job_id}/stream`.

### SPL2 Management Commands

Management commands also flow through this endpoint:

```bash
# Create a materialized view
curl -s localhost:3100/api/v1/query \
  -d '{
    "q": "level=error | stats count, avg(duration) by source, time_bucket(timestamp, '\''5m'\'') AS bucket | materialize \"mv_errors_5m\" retention=90d"
  }'

# Query a materialized view
curl -s localhost:3100/api/v1/query \
  -d '{"q": "| from mv_errors_5m | where source=\"nginx\" | sort -count | head 10"}'
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `400` | `INVALID_QUERY` | SPL2 syntax error (includes `suggestion` field) |
| `408` | `QUERY_TIMEOUT` | Query exceeded server timeout |
| `429` | `RATE_LIMITED` | Too many concurrent queries |

---

## GET /query

GET convenience variant for simple queries. Use POST for complex or long queries.

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `q` | Yes | -- | SPL2 query string |
| `from` | No | `"-15m"` | Start time |
| `to` | No | `"now"` | End time |
| `limit` | No | `1000` | Max events (max 50,000) |
| `format` | No | `"json"` | Output format: `json`, `csv`, `raw` |

```bash
curl -s "localhost:3100/api/v1/query?q=level%3Derror&from=-1h&limit=10" | jq .
```

---

## POST /query/stream

NDJSON streaming export. Same input as `POST /query`, but returns one JSON object per line with `Transfer-Encoding: chunked`. Designed for large exports, piping to files, and data pipelines.

```bash
curl -s localhost:3100/api/v1/query/stream \
  -d '{"q": "level=error", "from": "-24h"}'
```

**Response (200, `application/x-ndjson`):**

```
{"_id":"01JKN...","_timestamp":"2026-02-14T14:52:01Z","level":"error","message":"timeout"}
{"_id":"01JKN...","_timestamp":"2026-02-14T14:51:58Z","level":"error","message":"refused"}
{"__meta":{"total":8432,"scanned":12400000,"took_ms":342}}
```

### Behavior

- No default `limit` -- streaming is for full export. Client disconnect cancels the query.
- The `wait` parameter is ignored -- streaming always blocks until complete.
- The last line is always `{"__meta": {...}}` with stream summary stats.

### Piping to a File

```bash
curl -s localhost:3100/api/v1/query/stream \
  -d '{"q": "source=nginx", "from": "-7d"}' > nginx_export.ndjson
```

:::note Streaming vs. Job SSE
`POST /query/stream` produces **NDJSON data export** (one event per line, for `curl | jq` and pipelines). `GET /query/jobs/\{id\}/stream` produces **SSE progress events** (for Web UI real-time updates). They serve different purposes.
:::

---

## GET /query/explain

Parse and explain a query without executing it. Returns the parsed pipeline, estimated cost, fields involved, and materialized view acceleration availability.

```bash
curl -s "localhost:3100/api/v1/query/explain?q=source%3Dnginx+%7C+stats+count+by+uri" | jq .
```

**Response -- valid query with MV acceleration (200):**

```json
{
  "data": {
    "parsed": {
      "pipeline": [
        {
          "type": "search",
          "filters": [{"field": "source", "op": "=", "value": "nginx"}]
        },
        {
          "type": "stats",
          "aggregations": [{"fn": "count"}],
          "group_by": ["uri"]
        }
      ],
      "result_type": "aggregate",
      "estimated_cost": "low",
      "uses_full_scan": false,
      "fields_read": ["source", "uri"],
      "fields_produced": ["uri", "count"]
    },
    "acceleration": {
      "available": true,
      "view": "mv_nginx_parsed",
      "reason": "MV covers filter (source=nginx) and GROUP BY (uri) with count aggregate",
      "estimated_speedup": "~200x"
    },
    "is_valid": true
  }
}
```

**Response -- invalid query (200):**

```json
{
  "data": {
    "is_valid": false,
    "errors": [
      {
        "position": 24,
        "length": 6,
        "message": "Unknown command 'staats'",
        "suggestion": "stats"
      }
    ]
  }
}
```

This endpoint powers autocomplete, red-underline validation, and query planner UI in the Web UI.

---

## GET /query/jobs

List active and recently completed query jobs.

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `status` | No | -- | Filter by status: `running`, `complete`, `failed`, `cancelled` |

```bash
curl -s localhost:3100/api/v1/query/jobs | jq .
```

**Response (200):**

```json
{
  "data": {
    "jobs": [
      {
        "job_id": "qry_9c1d4e",
        "status": "running",
        "query": "* | stats dc(user_id) by source",
        "from": "-90d",
        "to": "now",
        "created_at": "2026-02-14T14:50:00Z",
        "progress": {
          "phase": "scanning",
          "percent": 45.2,
          "elapsed_ms": 18000,
          "eta_ms": 21800
        }
      },
      {
        "job_id": "qry_7f3a2b",
        "status": "complete",
        "query": "level=error | stats count by source",
        "from": "-7d",
        "to": "now",
        "created_at": "2026-02-14T14:48:12Z",
        "completed_at": "2026-02-14T14:48:49Z",
        "expires_at": "2026-02-14T14:53:49Z",
        "progress": {
          "phase": "complete",
          "percent": 100,
          "elapsed_ms": 37200
        }
      }
    ],
    "meta": {
      "max_concurrent": 10,
      "active": 1
    }
  }
}
```

Recently completed jobs are kept for `job_ttl` (default 5 minutes) before garbage collection.

---

## GET /query/jobs/\{jobId\}

Poll a specific job for status, progress, and results.

The response shape depends on the job `status`:

| Status | Contains |
|---|---|
| `running` | `progress` + optional `partial_results` |
| `complete` | `progress` + `results` (final) |
| `failed` | `progress` + `error` |
| `cancelled` | `progress` at time of cancellation |

```bash
curl -s localhost:3100/api/v1/query/jobs/qry_9c1d4e | jq .
```

**Response -- running with partial results (200):**

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "running",
    "query": "* | stats dc(user_id) by source, status",
    "from": "-90d",
    "to": "now",
    "created_at": "2026-02-14T14:50:00Z",
    "progress": {
      "phase": "scanning",
      "scanned": 42350000000,
      "total_estimate": 84700000000,
      "percent": 50.0,
      "events_matched": 423000000,
      "elapsed_ms": 18400,
      "eta_ms": 18400
    },
    "partial_results": {
      "type": "aggregate",
      "columns": ["source", "status", "dc(user_id)"],
      "rows": [
        ["nginx", 200, 892341],
        ["nginx", 404, 42891],
        ["api-gw", 200, 612044]
      ],
      "note": "Based on 50% of data. Final values will change."
    }
  }
}
```

**Response -- completed (200):**

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "complete",
    "query": "* | stats dc(user_id) by source, status",
    "from": "-90d",
    "to": "now",
    "created_at": "2026-02-14T14:50:00Z",
    "completed_at": "2026-02-14T14:50:37Z",
    "expires_at": "2026-02-14T14:55:37Z",
    "progress": {
      "phase": "complete",
      "scanned": 84700000000,
      "total_estimate": 84700000000,
      "percent": 100,
      "events_matched": 847000000,
      "elapsed_ms": 37200,
      "eta_ms": 0
    },
    "results": {
      "type": "aggregate",
      "columns": ["source", "status", "dc(user_id)"],
      "rows": [
        ["nginx", 200, 1784682],
        ["nginx", 404, 85762],
        ["api-gw", 200, 1224088],
        ["api-gw", 500, 12044]
      ],
      "total_rows": 14
    }
  },
  "meta": {
    "took_ms": 37200,
    "scanned": 84700000000
  }
}
```

**Response -- failed (200):**

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_d4e1f2",
    "status": "failed",
    "query": "* | stats count by uri",
    "from": "-365d",
    "to": "now",
    "created_at": "2026-02-14T14:50:00Z",
    "failed_at": "2026-02-14T14:50:42Z",
    "progress": {
      "phase": "scanning",
      "scanned": 62100000000,
      "total_estimate": 310000000000,
      "percent": 20.0,
      "events_matched": 62100000,
      "elapsed_ms": 42000,
      "eta_ms": null
    },
    "error": {
      "code": "QUERY_MEMORY_EXCEEDED",
      "message": "Query exceeded 512 MB memory limit at 20% scan. Too many unique 'uri' values (>2M) for GROUP BY.",
      "suggestion": "Add a filter to reduce cardinality, or increase max_query_memory_mb in /config."
    }
  }
}
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Job ID not found |
| `410` | `JOB_EXPIRED` | Job completed but results have expired past TTL |

---

## DELETE /query/jobs/\{jobId\}

Cancel a running query job. If the job is already complete, returns the completed status. Partial results scanned up to the cancellation point are preserved.

```bash
curl -X DELETE localhost:3100/api/v1/query/jobs/qry_9c1d4e | jq .
```

**Response -- cancelled (200):**

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "cancelled",
    "progress": {
      "phase": "scanning",
      "scanned": 42350000000,
      "total_estimate": 84700000000,
      "percent": 50.0,
      "elapsed_ms": 18400
    },
    "partial_results": {
      "type": "aggregate",
      "columns": ["source", "count"],
      "rows": [
        ["nginx", 284000],
        ["api-gw", 139000]
      ],
      "note": "Partial results at cancellation (50% scanned)."
    }
  }
}
```

---

## GET /query/jobs/\{jobId\}/stream

Server-Sent Events (SSE) stream for real-time job progress tracking. Preferred over polling for Web UI.

```bash
curl -N localhost:3100/api/v1/query/jobs/qry_9c1d4e/stream
```

**SSE event stream:**

```
event: progress
data: {"phase":"scanning","scanned":2100000000,"total_estimate":10400000000,"percent":20.2,"events_matched":847291,"elapsed_ms":5000,"eta_ms":19700}

event: partial
data: {"type":"aggregate","columns":["source","count"],"rows":[["nginx",142000],["api-gw",71000]],"note":"Based on 20% of data. Final values will change."}

event: progress
data: {"phase":"scanning","scanned":5200000000,"total_estimate":10400000000,"percent":50.0,"events_matched":2100000,"elapsed_ms":12500,"eta_ms":12500}

event: partial
data: {"type":"aggregate","columns":["source","count"],"rows":[["nginx",355000],["api-gw",178000]],"note":"Based on 50% of data. Final values will change."}

event: progress
data: {"phase":"aggregating","scanned":10400000000,"total_estimate":10400000000,"percent":92.0,"events_matched":4200000,"elapsed_ms":23100,"eta_ms":2000}

event: complete
data: {"type":"aggregate","columns":["source","count"],"rows":[["nginx",712345],["api-gw",356789]],"total_rows":5}
```

### Event Types

| Event | When | Data |
|---|---|---|
| `progress` | Every ~1s while running | Phase, percent, scanned, events matched, ETA |
| `partial` | Periodically (~every 10% progress) | Intermediate results (same shape as final) |
| `complete` | Query finished | Final results |
| `failed` | Query errored | Error object |
| `cancelled` | Job was cancelled | Progress at cancellation |

### JavaScript Example

```javascript
const es = new EventSource("/api/v1/query/jobs/qry_xxx/stream");

es.addEventListener("progress", (e) => {
  updateProgressBar(JSON.parse(e.data));
});

es.addEventListener("partial", (e) => {
  updateTable(JSON.parse(e.data));
});

es.addEventListener("complete", (e) => {
  showFinalResults(JSON.parse(e.data));
  es.close();
});

es.addEventListener("failed", (e) => {
  showError(JSON.parse(e.data));
  es.close();
});
```

### Progress Phases

| Phase | Description |
|---|---|
| `scanning` | Reading raw segments, matching filters |
| `aggregating` | Computing stats/group-by after scan |
| `sorting` | Sorting results (for `| sort`) |
| `complete` | Done |

---

## Response Data Types

The `data.type` field in query responses determines how results should be rendered:

| Type | Description | Rendering |
|---|---|---|
| `events` | Raw log events | Log viewer |
| `aggregate` | Stats/group-by results | Table |
| `timechart` | Time-series data | Line/area chart |
| `view_created` | Materialized view confirmation | Status message |
| `job` | Async job handle | Progress bar + polling |

## Related

- **[`lynxdb query` CLI command](/docs/cli/query)** -- query from the command line
- **[Lynx Flow Reference](/docs/lynx-flow/overview)** -- query language reference
- **[Materialized Views](/docs/api/views)** -- query acceleration
- **[Live Tail](/docs/api/tail-histogram)** -- real-time streaming
