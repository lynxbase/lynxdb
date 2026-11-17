---
sidebar_position: 3
title: Query
description: Run PPL queries over HTTP - sync, async and hybrid modes, the result envelope, error codes, explain, the PPL catalog, jobs and NDJSON streaming.
---

# Query API

Queries are written in [OpenSearch PPL](../ppl/overview.md). Every query starts with a source (`source=main ...`) or a source command such as `describe main`, followed by `| command` stages.

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/api/v1/query` | Run a query and return the result or a job handle |
| `GET` | `/api/v1/query` | Same, with query-string parameters |
| `POST` | `/api/v1/query/stream` | Run a query and stream the rows as NDJSON |
| `GET` | `/api/v1/query/explain` | Compile a query and return its plans without running it |
| `GET` | `/api/v1/catalog` | List the PPL commands and functions and their support level |
| `GET` | `/api/v1/query/jobs` | List jobs |
| `GET` | `/api/v1/query/jobs/{id}` | Get a job's status and result |
| `DELETE` | `/api/v1/query/jobs/{id}` | Cancel a job |
| `GET` | `/api/v1/query/jobs/{id}/stream` | Follow a job over Server-Sent Events |

When authentication is on, these endpoints need a key with `query` scope, except `DELETE /query/jobs/{id}`, which needs `admin` scope, and `GET /query/explain` and `GET /catalog`, which accept any valid key.

## POST /query

### Request body

| Field | Type | Description |
|---|---|---|
| `q` | string | The PPL query. Required unless `query` is set |
| `query` | string | Alias for `q` |
| `from` | string | Start of the time range: relative (`-1h`, `-7d`, `-1d@d`), `now`, or ISO 8601 (`2026-11-17T08:00:00Z`) |
| `to` | string | End of the time range, same formats |
| `earliest`, `latest` | string | Aliases for `from` and `to` |
| `limit` | integer | Rows to return. Default `query.default_result_limit` (1000), capped at `query.max_result_limit` (50000) |
| `offset` | integer | Rows to skip before `limit` applies |
| `wait` | number | Execution mode, see [below](#execution-modes) |
| `profile` | string | `basic`, `full` or `trace` |
| `format` | string | `json` or empty. Any other value is a `400 VALIDATION_ERROR`; use [`/query/stream`](#post-querystream) for NDJSON |
| `lint` | boolean | `false` turns off advisory lints in `meta.lints` |
| `suggestions` | boolean | `false` turns off advisory suggestions in `meta.suggestions` |
| `lint_limit` | integer | Maximum lints returned. Default 5 |
| `lint_full` | boolean | `true` returns every lint |

Without `from` and `to` the query reads all time, unless the query sets its own bounds with `earliest=` and `latest=` in the source command (`source=main earliest=-1h`).

### Event results

A query without an aggregation returns events:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main level=ERROR status>=500 | fields host, status, path | head 3"}'
```

```json
{
  "data": {
    "type": "events",
    "events": [
      {"host": "api-02", "path": "/api/v1/orders", "status": 500},
      {"host": "web-01", "path": "/api/v1/users", "status": 503},
      {"host": "api-01", "path": "/api/v1/search", "status": 503}
    ],
    "total": 3,
    "has_more": false
  },
  "meta": {
    "took_ms": 0,
    "scanned": 33,
    "query_id": "qry_f24fb85c3a02ebe2",
    "stats": {"rows_scanned": 33, "rows_returned": 3, "indexes_used": ["main"]}
  }
}
```

Each event is a JSON object keyed by field name. `total` counts the events the query produced before `offset` and `limit` applied.

### Aggregate results

A query whose result comes from `stats`, `timechart`, `chart`, `top`, `rare`, `describe` or `transpose` (possibly followed by row-preserving commands such as `eval`, `where`, `sort`, `head`, `fields`) returns a table:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main status>=500 | stats count() as errors, avg(response_time) as avg_ms by host | eval avg_ms = round(avg_ms, 1) | sort - errors | head 3"}'
```

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["host", "avg_ms", "errors"],
    "rows": [
      ["api-02", 439, 48],
      ["web-02", 355.4, 43],
      ["web-03", 439.3, 41]
    ],
    "total_rows": 3,
    "has_more": false
  },
  "meta": {
    "took_ms": 1,
    "scanned": 1003,
    "query_id": "qry_4ab386b7cc5e94aa"
  }
}
```

`columns` lists the built-in fields first (`_time`, `_raw`, `index`, `_source`, `_sourcetype`, `source`, `sourcetype`, `host`, in that order), then every other column in alphabetical order. Each entry of `rows` follows `columns`. `total_rows` counts rows before `offset` and `limit`.

Queries that start with `explain` (`explain source=main | stats count() by host`) return a one-row table with a `plan` column holding the logical and optimized plans as text. [`GET /query/explain`](#get-queryexplain) returns the same plans as structured JSON.

### Response meta

| Field | Description |
|---|---|
| `took_ms` | Execution time |
| `scanned` | Rows read from storage |
| `query_id` | Job ID, also sent as the `X-Query-ID` header |
| `stats` | Execution statistics: `rows_scanned`, `rows_returned`, segment counts and skips (`segments_total`, `segments_scanned`, `segments_skipped_index`, `segments_skipped_time`, `segments_skipped_stats`, `segments_skipped_bloom`, `segments_skipped_range`), `indexes_used`, `parse_ms`, `optimizer_rules`, `processed_bytes`, `peak_memory_bytes`, spill counters, `cpu_user_ms`, `cpu_sys_ms`, `cache_hit` |
| `explain` | Summary of the scan: `source_scope`, `segments`, `candidate_rows`, `scanned_bytes` |
| `segments_errored` | Segments that failed to read; present only when non-zero |
| `warnings` | Warnings about the query |
| `lints` | Advisory lints, see [Lints](#lints) |
| `suggestions`, `rewrites` | Advisory suggestions and query rewrites, when there are any |

`profile: "trace"` adds `stats.segment_details` with per-segment rows, rows after filtering and read time when the query reads segments from storage.

### Execution modes

`wait` decides when the server answers:

| `wait` | Mode | Behavior |
|---|---|---|
| absent or `null` | sync | Wait for the result. If the query runs longer than `query.sync_timeout` (default 30s), answer `202 Accepted` with a job handle |
| `0` | async | Answer `202 Accepted` with a job handle at once |
| `N` > 0 | hybrid | Wait up to `N` seconds. Answer `200` with the result if it finishes in time, otherwise `202` with a job handle |

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | stats count() as n, avg(response_time) as avg_ms by level", "wait": 0}'
```

```json
{
  "data": {"type": "job", "job_id": "qry_0f8eeb0e72818e85", "status": "running"},
  "meta": {"query_id": "qry_0f8eeb0e72818e85"}
}
```

The handle can carry a `progress` object when the job has started scanning. Poll [`GET /query/jobs/{id}`](#get-queryjobsid) or follow [`GET /query/jobs/{id}/stream`](#get-queryjobsidstream) for the result. Async jobs keep running when the client disconnects; a sync or hybrid query that has not been handed off as a job is canceled when the client disconnects.

### Limits and pagination

- `limit` defaults to 1000 and is capped at 50000 (`query.default_result_limit`, `query.max_result_limit`).
- `offset` skips rows before `limit` applies. `has_more` is `true` when rows remain after the returned page.
- An event query without a `head` stops after `query.max_result_limit` rows. Aggregations are not capped; their row count is the number of groups.
- Use `head N from M` inside the query to page at the source instead of in the response.

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | stats count() as n by path | sort - n", "limit": 3, "offset": 2}'
```

The response holds rows 3 to 5 of the sorted table, `total_rows: 10` and `has_more: true`.

### Lints

Queries that read every index (`source=*` or `index=*`) can carry advisory lints in `meta.lints`, each with `code`, `message` and `position`:

| Code | When |
|---|---|
| `ALL_SOURCES_HIGH_VOLUME` | The query read at least `query.broad_source_lint_threshold` indexes (default 10) |
| `BROAD_SEARCH` | The query also searches text (a `search` command or a regular expression) and read that many indexes or at least `query.broad_segment_lint_threshold` segments (default 1000) |

Lints never change the result. Turn them off with `"lint": false`.

### Errors

Errors use the `error` envelope. Compile errors carry the position of the problem, the tokens the parser expected, and a hint:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | where status >"}'
```

```json
{
  "error": {
    "code": "SYNTAX_ERROR",
    "message": "syntax error at line 1, column 29: expected an expression",
    "position": {"line": 1, "column": 29},
    "expected": ["expression"]
  }
}
```

```json
{
  "error": {
    "code": "SEMANTIC_ERROR",
    "message": "semantic error at line 1, column 50: field status not found (available fields: n, host)",
    "position": {"line": 1, "column": 50},
    "suggestion": "available fields: n, host"
  }
}
```

That second error comes from `source=main | stats count() as n by host | where status > 500`: after `stats`, only `n` and `host` exist.

| Field | Description |
|---|---|
| `code` | Machine-readable code from the table below |
| `message` | `<kind> error at line L, column C: <detail>`, with the hint in parentheses when there is one |
| `position` | `{"line", "column"}` of the start of the problem, 1-based |
| `expected` | Tokens the parser would have accepted (syntax errors only) |
| `suggestion` | A hint, such as the fields available at that point or the supported alternative |

| Code | HTTP | Meaning |
|---|---|---|
| `SYNTAX_ERROR` | 400 | The query does not match the PPL grammar: unknown command, missing expression, unbalanced quotes or brackets. Queries over 256 KiB, nested deeper than 256 levels, or with more than 1000 commands in one pipeline also fail with this code |
| `SEMANTIC_ERROR` | 400 | The query is well formed but refers to a field that does not exist at that point, or uses a command inconsistently (invalid regex, a parse pattern without named groups, a negative count) |
| `TYPE_ERROR` | 400 | Operand types do not fit: `where 1 + 2`, a malformed `DATE` literal, comparing incompatible types |
| `PLANNING_ERROR` | 400 | The query compiled but could not be planned, for example an unparsable `from` or `to` |
| `UNSUPPORTED_COMMAND` | 422 | A PPL command LynxDB does not run, such as `fieldformat`, `ml` or `kmeans` |
| `UNSUPPORTED_FUNCTION` | 422 | A PPL function LynxDB does not run, such as `geoip` or `query_string` |
| `UNSUPPORTED_FEATURE` | 422 | A command option or language feature LynxDB does not run, such as `bin ... bins=10` |
| `EXECUTION_ERROR` | 500 | The query failed while running |
| `QUERY_TOO_LARGE` | 400 | The query is longer than `query.max_query_length` bytes |
| `QUERY_MEMORY_EXCEEDED` | 400 | The query went over its memory budget (`query.max_query_memory_bytes`). The suggestion names ways to narrow it |
| `QUERY_POOL_EXHAUSTED` | 503 | The server-wide query memory pool is full. Retry later |
| `TOO_MANY_REQUESTS` | 429 | `query.max_concurrent` queries are already running, or the per-IP rate limit was hit. The `Retry-After` header says when to retry |
| `VALIDATION_ERROR` | 400 | The request is malformed: no query, a `format` other than `json`, an unsupported field on `/query/stream`, an invalid job status filter |
| `INVALID_JSON` | 400 | The body is not valid JSON |
| `INTERNAL_ERROR` | 500 | An unexpected server failure |

A construct the compiler accepts but the execution engine cannot run is found only when the plan is built for execution. `POST /query` reports it as `500 EXECUTION_ERROR` with a message that starts with `physical.Build: not supported by the execution engine`, and `GET /query/explain` reports such a query as valid. For example, `stats earliest(status, t2)` with an ordering column other than `_time` fails this way.

See [PPL errors](../ppl/errors.md) for the error kinds in detail and the [feature matrix](../ppl/feature-matrix.md) for what each command and function supports.

## GET /query

Runs a query from query-string parameters and returns the same envelope as `POST /query`.

| Parameter | Description |
|---|---|
| `q` | The PPL query (required) |
| `from`, `to` | Time range |
| `limit` | Rows to return |
| `format` | `json` or empty |

```bash
curl -s -G localhost:3100/api/v1/query \
  --data-urlencode 'q=source=main status>=500 | stats count() as errors by host' \
  --data-urlencode 'limit=2'
```

`GET /query` always runs in sync mode and has no `offset`, `wait` or `profile` parameters.

## POST /query/stream

Runs a query and writes one JSON object per row as NDJSON (`Content-Type: application/x-ndjson`).

The body accepts `q` or `query`, `from` or `earliest`, and `to` or `latest`. `limit`, `offset`, `wait`, `profile` and `format` are rejected with `400 VALIDATION_ERROR`. There is no row cap: limit the output with `head` in the query.

```bash
curl -s localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | stats count() as n by level"}'
```

```json
{"level":"WARN","n":347}
{"level":"INFO","n":359}
{"level":"ERROR","n":294}
{"__meta":{"scanned":1000,"took_ms":0,"total":3}}
```

- The last line is `{"__meta": {"total", "scanned", "took_ms"}}`.
- A query that fails to compile answers with the normal error envelope and HTTP status before any row is written.
- If execution fails after the stream has started, the last line is an error record and no `__meta` line follows:

  ```json
  {"__error":{"code":"EXECUTION_ERROR","message":"..."}}
  ```

- Closing the connection cancels the query.

## GET /query/explain

Compiles a query and returns its logical plan, optimized plan and scan hints without running it.

| Parameter | Description |
|---|---|
| `q` or `query` | The PPL query (required) |
| `from`, `to` | Time range; it appears in the `Scan` node of the plans |
| `analyze` | `true` also runs the query and adds its execution statistics as `execution` |

```bash
curl -s -G localhost:3100/api/v1/query/explain \
  --data-urlencode 'q=source=main status>=500 | stats count() as errors by host | sort - errors | head 5'
```

```json
{
  "data": {
    "is_valid": true,
    "errors": [],
    "parsed": {
      "result_type": "aggregate",
      "estimated_cost": "high",
      "uses_full_scan": true,
      "fields_read": ["_time", "host", "status"],
      "search_terms": null,
      "has_time_bounds": false,
      "logical_plan": "Limit(5)\n└─ Sort(errors DESC NULLS LAST)\n   └─ Project(errors, host)\n      └─ Aggregate(errors = count() BY host)\n         └─ Filter(status >= 500)\n            └─ Scan(main)\n",
      "optimized_plan": "TopN(5, errors DESC NULLS LAST)\n└─ Project(errors, host)\n   └─ Aggregate(errors = count() BY host)\n      └─ Filter(status >= 500)\n         └─ Scan(main) columns=[_time, host, status]\n",
      "compile_ms": 0.057,
      "optimizer_rules": [{"name": "sort-limit-to-topn", "count": 1}],
      "source_scope": {"type": "single", "sources": ["main"], "total_sources_available": 2}
    }
  }
}
```

| Field | Description |
|---|---|
| `result_type` | `events` or `aggregate`, the shape `POST /query` would return |
| `estimated_cost` | `low` when the scan has both time bounds and search or field hints to prune with, `medium` with one of them, `high` with neither |
| `uses_full_scan` | `true` when the scan has neither time bounds nor pruning hints |
| `fields_read` | Columns the scan reads |
| `search_terms` | Free-text terms used to prune segments, or `null` |
| `has_time_bounds` | Whether the query or the request bounds time |
| `logical_plan`, `optimized_plan` | The plan before and after optimization, as text trees |
| `compile_ms` | Compile time |
| `optimizer_rules` | Rules that changed the plan and how often each fired |
| `source_scope` | The indexes the query reads |

An invalid query still answers `200`, with `is_valid: false` and the compile errors. Each error has `kind` (`syntax`, `semantic`, `type`, `unsupported_command`, `unsupported_function`, `unsupported_feature`), `message`, and, when known, `position`, `expected` and `suggestion`:

```json
{
  "data": {
    "is_valid": false,
    "errors": [
      {
        "kind": "semantic",
        "message": "field status not found",
        "position": {"line": 1, "column": 50},
        "suggestion": "available fields: n, host"
      }
    ]
  }
}
```

With `analyze=true` on a valid query, the response adds `execution` with the same fields as `meta.stats` of `POST /query`.

The CLI equivalent is [`lynxdb explain`](../cli/query.md).

## GET /catalog

Returns the PPL commands and functions LynxDB knows, with their support level. Editors and clients use it for completion and validation.

```bash
curl -s localhost:3100/api/v1/catalog
```

```json
{
  "data": {
    "language": "ppl",
    "commands": [
      {"name": "ad", "support": "unsupported", "notes": "it runs the OpenSearch ML Commons plugin, which LynxDB does not have"},
      {"name": "bin", "support": "partial", "notes": "span= with numeric and time units; bins, minspan, aligntime, start, end and logarithmic spans are unsupported"},
      {"name": "dedup", "support": "full"}
    ],
    "functions": [
      {"name": "round", "kind": "scalar", "category": "math", "support": "full", "doc": "x rounded half away from zero to d decimal places (default 0)."},
      {"name": "if", "kind": "scalar", "support": "full", "lowering": "CASE"},
      {"name": "geoip", "kind": "scalar", "support": "unsupported", "unsupported": "LynxDB has no GeoIP database"},
      {"name": "percentile", "kind": "aggregate", "category": "aggregation", "support": "full", "notes": "t-digest; the optional compression argument is accepted and not used", "doc": "t-digest estimate of the percent-th percentile (0-100) of the numeric values."}
    ]
  }
}
```

| Field | Description |
|---|---|
| `commands[].support` | `full`, `partial` or `unsupported` |
| `commands[].notes` | What a partial command supports, or why a command is unsupported |
| `functions[].kind` | `scalar` or `aggregate` |
| `functions[].support` | `full` or `unsupported` |
| `functions[].category` | Function family, such as `math`, `string` or `aggregation` |
| `functions[].lowering` | The function or operator a PPL alias compiles to |
| `functions[].doc` | One-line description |
| `functions[].unsupported` | Why the function is not supported |

The catalog is fixed for a given binary. Responses carry `ETag` and `Cache-Control: public, max-age=86400`; a request with a matching `If-None-Match` gets `304 Not Modified`. The same data is rendered in [Commands](../ppl/commands.md) and [Functions](../ppl/functions.md).

## GET /query/jobs

Lists running jobs and finished jobs that have not expired (`query.job_ttl`, default 5m). Every query, sync ones included, is a job.

| Parameter | Description |
|---|---|
| `status` | `running`, `done`, `error` or `canceled`. `complete`, `failed` and `cancelled` are aliases |

```json
{
  "data": {
    "jobs": [
      {
        "job_id": "qry_0f8eeb0e72818e85",
        "query": "source=main | stats count() as n, avg(response_time) as avg_ms by level",
        "status": "done",
        "created_at": "2026-11-17T10:38:26.755295Z"
      }
    ]
  }
}
```

## GET /query/jobs/\{id\}

Returns one job. The shape depends on `status`.

Running:

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_0f8eeb0e72818e85",
    "status": "running",
    "query": "source=main | stats count() as n by level",
    "progress": {
      "phase": "scanning_segments",
      "segments_total": 30,
      "segments_scanned": 12,
      "segments_dispatched": 18,
      "segments_skipped_index": 4,
      "segments_skipped_time": 2,
      "segments_skipped_stats": 0,
      "segments_skipped_bloom": 3,
      "segments_skipped_range": 0,
      "buffered_events": 0,
      "rows_read_so_far": 120000,
      "elapsed_ms": 1840
    }
  },
  "meta": {"query_id": "qry_0f8eeb0e72818e85"}
}
```

`phase` is one of `parsing`, `scanning_buffer`, `filtering_segments`, `scanning_segments`, `executing_pipeline`.

Done: `results` holds the same object as `data` of a sync response, limited to `query.default_result_limit` rows, and `meta` carries the statistics.

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_0f8eeb0e72818e85",
    "status": "done",
    "query": "source=main | stats count() as n, avg(response_time) as avg_ms by level",
    "created_at": "2026-11-17T10:38:26.755295Z",
    "completed_at": "2026-11-17T10:38:26.756746Z",
    "results": {
      "type": "aggregate",
      "columns": ["avg_ms", "level", "n"],
      "rows": [
        [411.21325648414984, "WARN", 347],
        [414.4930362116992, "INFO", 359],
        [408.1224489795918, "ERROR", 294]
      ],
      "total_rows": 3,
      "has_more": false
    }
  },
  "meta": {"took_ms": 1, "scanned": 1000, "query_id": "qry_0f8eeb0e72818e85"}
}
```

Failed or canceled: `status` is `error` or `canceled`, and `error` holds `code` and `message`.

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_d4e1f2a0b1c2d3e4",
    "status": "error",
    "error": {"code": "QUERY_MEMORY_EXCEEDED", "message": "query exceeded memory budget"}
  }
}
```

An unknown ID answers `404 NOT_FOUND`.

## DELETE /query/jobs/\{id\}

Cancels a job. Needs `admin` scope when authentication is on.

```bash
curl -s -X DELETE localhost:3100/api/v1/query/jobs/qry_0f8eeb0e72818e85
```

```json
{"data": {"job_id": "qry_0f8eeb0e72818e85", "status": "done", "canceled": false, "completed_at": "2026-11-17T10:38:26.756746Z"}}
```

`canceled` is `true` when this request stopped a running job. For a job that had already finished, it is `false` and `status` is the final status.

## GET /query/jobs/\{id\}/stream

Follows a job over Server-Sent Events until it finishes.

| Event | When | Data |
|---|---|---|
| `progress` | About once a second while running | `phase`, `scanned`, `segments_total`, `percent`, `events_matched`, `elapsed_ms`, `eta_ms`; `preview` and `preview_version` when preview rows are available |
| `complete` | The job finished | `{"data": <result>, "meta": {"took_ms", "scanned", "stats"}}` |
| `failed` | The job failed | `{"code", "message"}` |
| `canceled` | The job was canceled | `{"scanned", "elapsed_ms"}` |

```bash
curl -N localhost:3100/api/v1/query/jobs/qry_0f8eeb0e72818e85/stream
```

```text
event: complete
data: {"data":{"columns":["avg_ms","level","n"],"has_more":false,"rows":[[411.21325648414984,"WARN",347],[414.4930362116992,"INFO",359],[408.1224489795918,"ERROR",294]],"total_rows":3,"type":"aggregate"},"meta":{"scanned":1000,"stats":{"rows_scanned":1000,"rows_returned":3},"took_ms":1}}
```

## Related

- [API overview](./overview.md): authentication, envelopes, endpoint map
- [Live tail and histogram](./tail-histogram.md)
- [`lynxdb query`](../cli/query.md)
- [PPL overview](../ppl/overview.md), [PPL errors](../ppl/errors.md), [feature matrix](../ppl/feature-matrix.md)
