---
sidebar_position: 3
title: Query
description: Query execution, NDJSON export, explain responses, async job polling, and job-progress SSE.
---

# Query API

LynxDB exposes three query-facing paths:

- `POST /api/v1/query` for JSON-envelope query execution
- `POST /api/v1/query/stream` for NDJSON export
- `GET /api/v1/query/explain` for parse and plan inspection

Async work created by `POST /query` is tracked under `/api/v1/query/jobs/...`.

## POST /query

Execute an SPL2 query and return either final results or a job handle.

### Request Body

| Field | Type | Required | Description |
|---|---|---|---|
| `q` | string | Yes* | Query text |
| `query` | string | Yes* | Alias for `q` |
| `from` | string | No | Start time bound |
| `earliest` | string | No | Alias for `from` |
| `to` | string | No | End time bound |
| `latest` | string | No | Alias for `to` |
| `limit` | integer | No | Result row limit. Defaults to `query.default_result_limit`, capped by `query.max_result_limit` |
| `offset` | integer | No | Offset for paginated event or aggregate results |
| `wait` | number | No | Execution mode selector |
| `profile` | string | No | Profiling level: `basic`, `full`, or `trace` |
| `variables` | object | No | Replaces `$name` tokens in the query with quoted, escaped string values |
| `format` | string | No | Response format. Only empty or `json` are accepted on this endpoint. Use `POST /api/v1/query/stream` for NDJSON output |

\* One of `q` or `query` is required.

### Execution Modes

`wait` controls when the server returns:

| `wait` value | Mode | Behavior |
|---|---|---|
| omitted or `null` | sync window | Wait until completion. If the query does not finish within `query.sync_timeout` (default `30s`), LynxDB detaches the job and returns `202 Accepted` with a job handle |
| `0` | async | Return `202 Accepted` immediately with a job handle |
| positive number | hybrid | Wait up to `N` seconds. If the query is still running, return the same `202 Accepted` job handle |

### Event Result Example

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "q": "FROM main | where level=\"error\" | head 2",
    "from": "-1h"
  }' | jq .
```

```json
{
  "data": {
    "type": "events",
    "events": [
      {
        "_time": "2026-02-14T14:52:01.234Z",
        "_raw": "level=ERROR status=502 uri=/api/users",
        "source": "nginx",
        "host": "web-01",
        "level": "ERROR",
        "status": 502,
        "uri": "/api/users"
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

### Aggregate Result Example

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "q": "FROM main | where source=\"nginx\" and status>=500 | stats count by uri | sort -count | head 10",
    "from": "-1h"
  }' | jq .
```

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["uri", "count"],
    "rows": [
      ["/api/v1/users", 1247],
      ["/api/v1/orders", 893]
    ],
    "total_rows": 42,
    "has_more": true
  },
  "meta": {
    "took_ms": 34,
    "scanned": 12400000,
    "query_id": "qry_8b2c..."
  }
}
```

### Async or Promoted Job Handle Example

When a query is explicitly async, or when it misses the sync window, `POST /query` returns `202 Accepted`:

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "running"
  },
  "meta": {
    "query_id": "qry_9c1d4e"
  }
}
```

Current handlers return a minimal job handle only. They do not include `query`, `from`, `to`, or partial result rows in the `202` response body.

### Variable Substitution

`variables` replaces `$name` tokens with quoted string literals before planning:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{
    "q": "FROM main | where source=$source and level=$level | head 5",
    "variables": {
      "source": "nginx",
      "level": "error"
    }
  }' | jq .
```

### Response Behavior

- `POST /query` always returns the standard JSON envelope.
- For NDJSON export, use [`POST /query/stream`](#post-querystream).
- `format` values other than `json` are rejected with `400 VALIDATION_ERROR`.
- Unsupported SPL2 commands are rejected with `UNSUPPORTED_COMMAND`.

### Error Responses

| Status | Code | When |
|---|---|---|
| `400` | `INVALID_JSON` | Request body is not valid JSON |
| `400` | `VALIDATION_ERROR` | Query is missing |
| `400` | `INVALID_QUERY` | Parse or planning failed |
| `400` | `QUERY_TOO_LARGE` | Query exceeds `query.max_query_length` |
| `400` | `QUERY_MEMORY_EXCEEDED` | Query exceeded its per-query memory budget |
| `400` | `UNSUPPORTED_COMMAND` | Query contains a command LynxDB rejects for this path |
| `401` | `AUTH_REQUIRED` / `INVALID_TOKEN` | Authentication failure |
| `403` | `FORBIDDEN` | Token lacks query scope |
| `429` | `TOO_MANY_REQUESTS` | Query concurrency limit reached |
| `503` | `QUERY_POOL_EXHAUSTED` | Global query memory pool is exhausted |

## GET /query

GET convenience variant for simple queries.

### Query Parameters

| Parameter | Required | Description |
|---|---|---|
| `q` | Yes | Query text |
| `from` | No | Start time bound |
| `to` | No | End time bound |
| `limit` | No | Result row limit |
| `format` | No | Optional response format. Only empty or `json` are accepted |

```bash
curl -s "localhost:3100/api/v1/query?q=FROM+main+%7C+head+5&limit=5" | jq .
```

`GET /query` returns the same JSON envelope and result shapes as `POST /query`. As with `POST /query`, `format=csv`, `format=ndjson`, and other non-JSON values are rejected.

## POST /query/stream

Stream query results as newline-delimited JSON.

This path is for export and pipeline use, not job management.

### Request Body

`POST /query/stream` accepts a subset of the `POST /query` request body:

- `q` or `query`
- `from` or `earliest`
- `to` or `latest`
- `variables`

`wait`, `limit`, `offset`, `profile`, and `format` are not silently ignored on this path. If any of them are present, the handler returns `400 VALIDATION_ERROR`.

### Example

```bash
curl -s localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q":"FROM main | where level=\"error\"","from":"-24h"}'
```

Example response:

```json
{"_time":"2026-02-14T14:52:01Z","_raw":"level=ERROR msg=\"timeout\"","level":"ERROR","message":"timeout"}
{"_time":"2026-02-14T14:51:58Z","_raw":"level=ERROR msg=\"refused\"","level":"ERROR","message":"refused"}
{"__meta":{"total":8432,"scanned":12400000,"took_ms":342}}
```

### Behavior

- The response content type is `application/x-ndjson`.
- There is no default result limit on this path.
- The final line is `{"__meta": ...}` with stream summary data.
- If a streaming error occurs after output has started, LynxDB writes a final `{"__error": ...}` line.
- Client disconnect cancels the streaming query.

## GET /query/explain

Parse and explain a query without executing it.

### Query Parameters

| Parameter | Required | Description |
|---|---|---|
| `q` | Yes* | Query text |
| `query` | Yes* | Alias for `q` |
| `from` | No | Start time bound |
| `to` | No | End time bound |
| `analyze` | No | When `true`, also executes the query with `profile=full` and adds `execution` stats |

\* One of `q` or `query` is required.

### Valid Response Example

```bash
curl -s "localhost:3100/api/v1/query/explain?q=FROM%20main%20%7C%20search%20%22error%22%20%7C%20head%2010" | jq .
```

```json
{
  "data": {
    "is_valid": true,
    "parsed": {
      "pipeline": [
        {
          "command": "search",
          "description": "search \"error\""
        },
        {
          "command": "head",
          "description": "head 10"
        }
      ],
      "result_type": "events",
      "estimated_cost": "low",
      "uses_full_scan": false,
      "fields_read": [],
      "search_terms": ["error"],
      "has_time_bounds": false
    },
    "errors": [],
    "acceleration": {
      "available": false
    }
  }
}
```

### Invalid Response Example

`GET /query/explain` still returns `200 OK` for invalid queries:

```json
{
  "data": {
    "is_valid": false,
    "errors": [
      {
        "message": "Unknown command 'staats'",
        "suggestion": "stats"
      }
    ]
  }
}
```

### `analyze=true`

When `analyze=true`, the response keeps the normal explain payload and adds an `execution` object with actual runtime stats from a profiled execution.

## GET /query/jobs

List active and recently completed jobs.

```bash
curl -s localhost:3100/api/v1/query/jobs | jq .
```

```json
{
  "data": {
    "jobs": [
      {
        "job_id": "qry_9c1d4e",
        "query": "FROM main | head 5",
        "status": "running",
        "created_at": "2026-02-14T14:50:00Z"
      }
    ]
  }
}
```

### Notes

- `GET /query/jobs` accepts an optional `status` query parameter. Supported values are `running`, `done`, `error`, `canceled`, plus aliases `complete`, `failed`, and `cancelled`.
- Completed, errored, and canceled jobs are garbage-collected after `query.job_ttl` (default `5m`).

## `GET /query/jobs/{id}`

Fetch a specific job.

### Running Job

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "running",
    "query": "FROM main | head 5",
    "progress": {
      "phase": "scanning_segments",
      "segments_total": 30,
      "segments_scanned": 12,
      "segments_dispatched": 18,
      "segments_skipped_index": 4,
      "segments_skipped_time": 2,
      "segments_skipped_stats": 1,
      "segments_skipped_bloom": 3,
      "segments_skipped_range": 0,
      "buffered_events": 1200,
      "rows_read_so_far": 120000,
      "elapsed_ms": 18400
    }
  }
}
```

### Completed Job

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_9c1d4e",
    "status": "done",
    "query": "FROM main | stats count by source",
    "created_at": "2026-02-14T14:50:00Z",
    "completed_at": "2026-02-14T14:50:37Z",
    "results": {
      "type": "aggregate",
      "columns": ["source", "count"],
      "rows": [
        ["nginx", 142847]
      ],
      "total_rows": 5,
      "has_more": false
    }
  },
  "meta": {
    "took_ms": 37200,
    "scanned": 84700000000,
    "query_id": "qry_9c1d4e"
  }
}
```

### Errored or Canceled Job

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_d4e1f2",
    "status": "error",
    "error": {
      "code": "QUERY_MEMORY_EXCEEDED",
      "message": "query exceeded memory budget"
    }
  }
}
```

Canceled jobs use the same envelope shape, with `status: "canceled"` and an error message such as `canceled by user`.

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Job ID not found |

## `DELETE /query/jobs/{id}`

Cancel a job.

This endpoint requires admin scope when auth is enabled.

```bash
curl -X DELETE localhost:3100/api/v1/query/jobs/qry_9c1d4e | jq .
```

```json
{
  "data": {
    "job_id": "qry_9c1d4e",
    "status": "canceled",
    "canceled": true,
    "completed_at": "2026-02-14T14:50:37Z"
  }
}
```

`canceled` reports whether this request actually stopped a running job. If the job had already finished, the endpoint still returns `200 OK`, but `canceled` is `false` and `status` reflects the final state.

## `GET /query/jobs/{id}/stream`

Stream job progress over Server-Sent Events (SSE).

```bash
curl -N localhost:3100/api/v1/query/jobs/qry_9c1d4e/stream
```

### Event Types

| Event | When | Data |
|---|---|---|
| `progress` | About once per second while the job is running | Progress information. If preview rows are available, they are embedded in the same event as `preview` and `preview_version` |
| `complete` | Job finished successfully | Final payload shaped as `{data: ..., meta: ...}` |
| `failed` | Job errored | `{code, message}` |
| `canceled` | Job was canceled | Progress snapshot |

### Example Stream

```text
event: progress
data: {"phase":"scanning","segments_total":30,"scanned":12,"percent":40,"elapsed_ms":5000}

event: progress
data: {"phase":"scanning","segments_total":30,"scanned":21,"percent":70,"elapsed_ms":9000,"preview":[{"source":"nginx","count":142000}],"preview_version":1}

event: complete
data: {"data":{"type":"aggregate","columns":["source","count"],"rows":[["nginx",284000]],"total_rows":1,"has_more":false},"meta":{"took_ms":10400,"scanned":847000,"stats":{"rows_scanned":847000}}}
```

The current implementation does not emit a separate `partial` event type. Preview rows, when present, are attached to `progress` events.

## Related

- **[REST API overview](/docs/api/overview)** for authentication, envelopes, and endpoint map
- **[Live Tail & Histogram](/docs/api/tail-histogram)** for SSE log streaming
- **[`lynxdb query`](/docs/cli/query)** for the CLI behavior on top of these endpoints
- **[Lynx Flow Reference](/docs/lynx-flow/overview)** for SPL2 syntax
