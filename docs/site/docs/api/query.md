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
| `GET` | `/api/v1/query/jobs/{id}` | Get a job's state and result |
| `DELETE` | `/api/v1/query/jobs/{id}` | Cancel a job |

When authentication is on, these endpoints need a key with `query` scope, except `DELETE /query/jobs/{id}`, which needs `admin` scope, and `GET /query/explain` and `GET /catalog`, which accept any valid key.

Before a query runs, the server writes the events ingested so far, so the query sees every event whose ingest request has been answered.

## POST /query

### Request body

| Field | Type | Description |
|---|---|---|
| `q` | string | The PPL query. Required unless `query` is set |
| `query` | string | Alias for `q` |
| `from` | string | Start of the time range, inclusive |
| `to` | string | End of the time range, exclusive |
| `earliest`, `latest` | string | Aliases for `from` and `to` |
| `limit` | integer | Rows to return. Default `query.default_result_limit` (1000), capped at `query.max_result_limit` (50000) |
| `offset` | integer | Rows to skip before `limit` applies |
| `wait` | number | Execution mode, see [below](#execution-modes) |
| `format` | string | `json` or empty. Any other value is a `400 VALIDATION_ERROR`; use [`/query/stream`](#post-querystream) for NDJSON |

Other fields are ignored.

`from` and `to` take one of these forms:

- `now`.
- A relative time: a count of `s`, `m`, `h`, `d` or `w` back from now, such as `-15m`, `-1h`, `-7d` or `-2w`.
- A snap, `@` and a unit, which truncates a relative time or now to the start of that unit in UTC; a week starts on Monday. `-1d@d` is the start of yesterday, `@d` the start of today, `@w` the start of this week.
- An absolute time: `2026-11-17T08:00:00Z`, `2026-11-17T08:00:00.250+01:00`, `2026-11-17T08:00:00`, `2026-11-17 08:00:00`, `2026-11-17 08:00` or `2026-11-17`. A time without a zone is UTC.

Any other value, such as `yesterday`, `1h30m` or `-1h@x`, answers `400 VALIDATION_ERROR` with a message that quotes the value and lists these forms.

`from` and `to` narrow every scan of the query, those of its subsearches included; bounds the query sets itself (`source=main earliest=-1h`) still apply, and the narrower of the two wins. Without `from`, `to` or bounds in the query, the query reads all time.

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
      {"host": "api-02", "status": 500, "path": "/api/v1/orders"},
      {"host": "web-01", "status": 503, "path": "/api/v1/users"},
      {"host": "api-01", "status": 503, "path": "/api/v1/search"}
    ],
    "total": 3,
    "has_more": false
  },
  "meta": {"took_ms": 0.19, "query_id": "qry_f8b21fe1891075c3", "rows": 3}
}
```

Each event is a JSON object keyed by field name. An event holds only the fields it has: a field it lacks is absent, not `null`. `total` counts the events the query produced before `offset` and `limit` applied.

### Aggregate results

A query whose result comes from `stats`, `timechart`, `chart`, `top`, `rare`, `describe` or `transpose`, possibly followed by commands that keep its rows (such as `eval`, `where`, `sort`, `head`, `fields`), returns a table:

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main status>=500 | stats count() as errors, avg(response_time) as avg_ms by host | eval avg_ms = round(avg_ms, 1) | sort - errors | head 3"}'
```

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["errors", "avg_ms", "host"],
    "rows": [
      [48, 439, "api-02"],
      [43, 355.4, "web-02"],
      [41, 439.3, "web-03"]
    ],
    "total_rows": 3,
    "has_more": false
  },
  "meta": {"took_ms": 0.218, "query_id": "qry_ff9a60bb26c70c81", "rows": 3}
}
```

`columns` lists the columns the query fixes, in its order. Columns the query does not fix follow: the built-in fields first (`_time`, `_raw`, `index`, `_source`, `_sourcetype`, `source`, `sourcetype`, `host`, in that order), then the others alphabetically. Each entry of `rows` follows `columns`, with `null` where a row has no value. `total_rows` counts rows before `offset` and `limit`.

Times are written as RFC 3339 text and durations as integer nanoseconds.

Queries that start with `explain` (`explain source=main | stats count() by host`) return a one-row table with a `plan` column holding the logical and optimized plans as text. [`GET /query/explain`](#get-queryexplain) returns the same plans as structured JSON.

### Response meta

| Field | Description |
|---|---|
| `took_ms` | Time the query ran, in milliseconds |
| `query_id` | Job ID, also sent as the `X-Query-ID` header |
| `rows` | Rows the query produced, before `offset` and `limit` |
| `warnings` | Present when the query has something to tell that is not an error: a field that the query reads and that no event of the indexes it scans holds (`no event of the index main holds the field nosuchfield`), or that the latest events of an index could not be written and are not in the result |

### Execution modes

Every query runs as a job. `wait` decides when the server answers:

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
  "data": {
    "type": "job",
    "job_id": "qry_2d2d123959b57199",
    "status": "running",
    "progress": {"status": "running", "elapsed_ms": 0}
  },
  "meta": {"query_id": "qry_2d2d123959b57199"}
}
```

Poll [`GET /query/jobs/{id}`](#get-queryjobsid) for the result. A job keeps running after its handle is returned, whether or not the client stays connected. A client that disconnects while a sync or hybrid query is still waiting cancels the job.

### Limits and pagination

- `limit` defaults to `query.default_result_limit` (1000) and is capped at `query.max_result_limit` (50000). A `limit` of zero or less takes the default.
- `offset` skips rows before `limit` applies. `has_more` is `true` when rows remain after the returned page.
- The query itself produces every row; `limit` and `offset` choose which ones the response carries. Use `head` in the query to stop it early.

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | stats count() as n by path | sort - n", "limit": 3, "offset": 2}'
```

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["n", "path"],
    "rows": [[110, "/api/v1/users"], [107, "/api/v1/search"], [101, "/api/v1/health"]],
    "total_rows": 10,
    "has_more": true
  },
  "meta": {"took_ms": 0.102, "query_id": "qry_fe23c50e143ae202", "rows": 10}
}
```

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
| `suggestion` | A hint, such as the fields available at that point |

| Code | HTTP | Meaning |
|---|---|---|
| `SYNTAX_ERROR` | 400 | The query does not match the PPL grammar: unknown command, missing expression, unbalanced quotes or brackets. Queries over 256 KiB, nested deeper than 256 levels, or with more than 1000 commands in one pipeline also fail with this code |
| `SEMANTIC_ERROR` | 400 | The query is well formed but refers to a field that does not exist at that point, or uses a command inconsistently (invalid regex, a parse pattern without named groups, a negative count) |
| `TYPE_ERROR` | 400 | Operand types do not fit: `where 1 + 2`, a malformed `DATE` literal, comparing incompatible types |
| `PLANNING_ERROR` | 400 | The query parsed but no plan could be built for it |
| `UNSUPPORTED_COMMAND` | 422 | A PPL command LynxDB does not run, such as `ml` or `kmeans` |
| `UNSUPPORTED_FUNCTION` | 422 | A PPL function LynxDB does not run, such as `geoip` |
| `UNSUPPORTED_FEATURE` | 422 | A command option or language feature LynxDB does not run, such as `patterns method=brain` or `bin ... bins=10`. A construct that compiles but that the executor cannot run answers the same when it runs |
| `NOT_FOUND` | 404 | A source of the query names an index there is not: `no such index: <name>`. A pattern such as `logs-*` may match no index, and `main` is there before its first event |
| `EXECUTION_ERROR` | 500 | The query failed while running |
| `QUERY_TOO_LARGE` | 400 | The query is longer than `query.max_query_length` bytes |
| `TOO_MANY_REQUESTS` | 429 | `query.max_concurrent` queries are already running, or the per-IP rate limit was hit. The `Retry-After` header says when to retry |
| `VALIDATION_ERROR` | 400 | The request is malformed: no query, a `format` other than `json`, an unparsable `from` or `to`, `from` after `to`, an unsupported field on `/query/stream`, an invalid job status filter |
| `INVALID_JSON` | 400 | The body is not valid JSON |

```bash
curl -s localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | patterns method=brain msg"}'
```

```json
{
  "error": {
    "code": "UNSUPPORTED_FEATURE",
    "message": "unsupported_feature error at line 1, column 31: patterns method=brain is not supported; use method=simple_pattern",
    "position": {"line": 1, "column": 31}
  }
}
```

See [PPL errors](../ppl/errors.md) for the error kinds in detail and the [feature matrix](../ppl/feature-matrix.md) for what each command and function supports.

## GET /query

Runs a query from query-string parameters, in sync mode, and returns the same envelope as `POST /query`.

| Parameter | Description |
|---|---|
| `q` | The PPL query (required) |
| `from`, `to` | Time range |
| `limit`, `offset` | Rows to return and rows to skip |
| `format` | `json` or empty |

```bash
curl -s -G localhost:3100/api/v1/query \
  --data-urlencode 'q=source=main status>=500 | stats count() as errors by host | sort - errors' \
  --data-urlencode 'limit=2'
```

```json
{
  "data": {
    "type": "aggregate",
    "columns": ["errors", "host"],
    "rows": [[48, "api-02"], [43, "web-02"]],
    "total_rows": 5,
    "has_more": true
  },
  "meta": {"took_ms": 0.204, "query_id": "qry_398ea83db1049ca2", "rows": 5}
}
```

## POST /query/stream

Runs a query and writes one JSON object per row as NDJSON (`Content-Type: application/x-ndjson`), then a `__meta` line with the row count, the time the query took and its `warnings`, if any.

The body accepts `q` or `query`, `from` or `earliest`, and `to` or `latest`. `limit`, `offset`, `wait`, and `format` are rejected with `400 VALIDATION_ERROR`. There is no row cap: limit the output with `head` in the query.

```bash
curl -s localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | stats count() as n by level"}'
```

```json
{"n":294,"level":"ERROR"}
{"n":359,"level":"INFO"}
{"n":347,"level":"WARN"}
{"__meta":{"total":3,"took_ms":0}}
```

- The query runs to the end before the first row is written, so a query that fails, at compile time or while it runs, answers with the normal error envelope and HTTP status.
- `took_ms` is a whole number of milliseconds.
- Closing the connection while the query runs cancels it.

## GET /query/explain

Compiles a query and returns its logical plan, optimized plan and scan hints without running it.

| Parameter | Description |
|---|---|
| `q` or `query` | The PPL query (required) |
| `from`, `to` | Time range; it counts toward `has_time_bounds` and `estimated_cost` |
| `analyze` | `true` also runs the query and adds its time and row count as `execution` |

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
      "compile_ms": 0.042,
      "optimizer_rules": [{"name": "sort-limit-to-topn", "count": 1}],
      "source_scope": {"type": "single", "sources": ["main"], "total_sources_available": 3}
    }
  }
}
```

| Field | Description |
|---|---|
| `result_type` | `events` or `aggregate`, the shape `POST /query` would return |
| `estimated_cost` | `low` when the scan has both time bounds and search terms or field predicates, `medium` with one of them, `high` with neither |
| `uses_full_scan` | `true` when the scan has neither time bounds nor search terms or field predicates |
| `fields_read` | Columns the scan reads, or `null` when it reads whole events |
| `search_terms` | Free-text terms of the query, or `null` |
| `has_time_bounds` | Whether the query or the request bounds time |
| `logical_plan`, `optimized_plan` | The plan before and after optimization, as text trees |
| `compile_ms` | Compile time |
| `optimizer_rules` | Rules that changed the plan and how often each fired |
| `source_scope` | The indexes the query reads: `type` (`single`, `list`, `glob` or `all`), `sources` or `pattern`, and `total_sources_available`, the number of indexes |

With `analyze=true` on a valid query, the response adds `execution`:

```json
"execution": {"took_ms": 0.069, "rows": 5}
```

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

A missing `q`, an unparsable `from` or `to` answer `400 VALIDATION_ERROR`, and a query over `query.max_query_length` answers `400 QUERY_TOO_LARGE`.

The CLI equivalent is [`lynxdb explain`](../cli/explain.md).

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
      {"name": "avg", "kind": "aggregate", "category": "aggregation", "support": "full", "doc": "Arithmetic mean of the numeric values."}
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
| `functions[].notes` | Details of how an aggregate function counts or computes |
| `functions[].doc` | One-line description |
| `functions[].unsupported` | Why the function is not supported |

The catalog is fixed for a given binary. Responses carry `ETag` and `Cache-Control: public, max-age=86400`; a request with a matching `If-None-Match` gets `304 Not Modified`. The same data is rendered in [Commands](../ppl/commands.md) and [Functions](../ppl/functions.md).

## GET /query/jobs

Lists the jobs, the latest first. Every query run through `POST` or `GET /query` is a job; a finished job is forgotten `query.job_ttl` (default 5m) after it ends.

| Parameter | Description |
|---|---|
| `status` | `running`, `done`, `error` or `canceled` |

```bash
curl -s 'localhost:3100/api/v1/query/jobs?status=running'
```

```json
{
  "data": {
    "jobs": [
      {
        "job_id": "qry_1dc74244284168b9",
        "query": "source=main | join type=cross [ source=main ] | stats count() as n",
        "status": "running",
        "created_at": "2026-11-17T15:50:29.981615Z"
      }
    ]
  }
}
```

## GET /query/jobs/\{id\}

Returns one job. `status` is `running`, `done`, `error` or `canceled`, and the shape depends on it. A running job reports how long it has run; that and the row count of a finished job are the only progress there is.

Running:

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_1dc74244284168b9",
    "status": "running",
    "query": "source=main | join type=cross [ source=main ] | stats count() as n",
    "created_at": "2026-11-17T15:50:29.981615Z",
    "progress": {"status": "running", "elapsed_ms": 1256}
  },
  "meta": {"query_id": "qry_1dc74244284168b9"}
}
```

Done: `results` holds the same object as `data` of a sync response, with at most `query.default_result_limit` rows, and `meta.rows` counts every row.

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_2d2d123959b57199",
    "status": "done",
    "query": "source=main | stats count() as n, avg(response_time) as avg_ms by level",
    "created_at": "2026-11-17T15:44:47.307993Z",
    "completed_at": "2026-11-17T15:44:47.308683708Z",
    "results": {
      "type": "aggregate",
      "columns": ["n", "avg_ms", "level"],
      "rows": [
        [347, 411.21325648414984, "WARN"],
        [359, 414.4930362116992, "INFO"],
        [294, 408.1224489795918, "ERROR"]
      ],
      "total_rows": 3,
      "has_more": false
    }
  },
  "meta": {"took_ms": 0.69, "query_id": "qry_2d2d123959b57199", "rows": 3}
}
```

A job whose own request took the whole answer, a sync or hybrid query answered in time, keeps no rows: it is listed and reads back `done` with `completed_at` and `meta.rows`, without `results`.

Failed or canceled: `error` holds `code` and `message`.

```json
{
  "data": {
    "type": "job",
    "job_id": "qry_1dc74244284168b9",
    "status": "canceled",
    "query": "source=main | join type=cross [ source=main ] | stats count() as n",
    "created_at": "2026-11-17T15:50:29.981615Z",
    "error": {"code": "EXECUTION_ERROR", "message": "context canceled"}
  },
  "meta": {"took_ms": 2638.104, "query_id": "qry_1dc74244284168b9"}
}
```

An unknown ID answers `404 NOT_FOUND`.

## DELETE /query/jobs/\{id\}

Cancels a job. Needs `admin` scope when authentication is on.

```bash
curl -s -X DELETE localhost:3100/api/v1/query/jobs/qry_1dc74244284168b9
```

```json
{"data": {"job_id": "qry_1dc74244284168b9", "status": "running", "canceled": true}}
```

`canceled` is `true` when the job was running when the request came. The job stops at the next point where its operators check for cancellation; the server waits up to a second for that and reports the state it sees then, with `completed_at` once the job has ended. For a job that had already finished, `canceled` is `false` and `status` is its final state.

## Related

- [API overview](./overview.md): authentication, envelopes, endpoint map
- [Histogram](./histogram.md)
- [`lynxdb query`](../cli/query.md)
- [PPL overview](../ppl/overview.md), [PPL errors](../ppl/errors.md), [feature matrix](../ppl/feature-matrix.md)
