---
title: Error handling
sidebar_label: Error handling
description: PPL error kinds, their positions and hints, and the REST error codes they map to.
---

# Error handling

LynxDB checks a query completely before it reads data. Compilation errors
carry a kind, the line and column where the problem starts, and, when
available, the tokens the parser expected and a suggestion. Unsupported
features are reported as errors; nothing unsupported is silently
approximated.

## Error kinds

| Kind (`pkg/ppl`) | API code | HTTP | Raised when |
| --- | --- | --- | --- |
| `syntax` | `SYNTAX_ERROR` | 400 | the text does not match the [grammar](./grammar.md); the query is too long, too deeply nested or has more than 1000 commands |
| `semantic` | `SEMANTIC_ERROR` | 400 | an unknown field after a closed schema, an invalid option value or time modifier, an aggregation outside an aggregating command |
| `type` | `TYPE_ERROR` | 400 | incompatible operand or argument types, a wrong argument count, a non-boolean condition |
| `unsupported_command` | `UNSUPPORTED_COMMAND` | 422 | a PPL command LynxDB does not execute |
| `unsupported_function` | `UNSUPPORTED_FUNCTION` | 422 | a PPL function LynxDB does not execute, or an unknown function name |
| `unsupported_feature` | `UNSUPPORTED_FEATURE` | 422 | a command option or language feature LynxDB does not execute |
| planning | `PLANNING_ERROR` | 400 | the server cannot plan an accepted query, for example an unparsable `from`/`to` |
| execution | `EXECUTION_ERROR` | 500 | the query failed while running |

The source of the mapping is `pkg/api/rest/query_exec.go`.

## Examples

```
source=main | where
```
```json
{"error": {"code": "SYNTAX_ERROR",
           "message": "syntax error at line 1, column 20: expected an expression",
           "position": {"line": 1, "column": 20},
           "expected": ["expression"]}}
```

```
source=main | stats count() by host | where nope = 1
```
```json
{"error": {"code": "SEMANTIC_ERROR",
           "message": "semantic error at line 1, column 45: field nope not found (available fields: count(), host)",
           "position": {"line": 1, "column": 45},
           "suggestion": "available fields: count(), host"}}
```

```
source=main | where 1 + 1
```
```json
{"error": {"code": "TYPE_ERROR",
           "message": "type error at line 1, column 21: expected a boolean expression, got int",
           "position": {"line": 1, "column": 21}}}
```

```
source=main | kmeans centroids=3
```
```json
{"error": {"code": "UNSUPPORTED_COMMAND",
           "message": "unsupported_command error at line 1, column 15: the kmeans command is not supported: it runs the OpenSearch ML Commons plugin, which LynxDB does not have",
           "position": {"line": 1, "column": 15}}}
```

```
source=main | bin response_time bins=10
```
```json
{"error": {"code": "UNSUPPORTED_FEATURE",
           "message": "unsupported_feature error at line 1, column 38: bin bins= is not supported; use span=",
           "position": {"line": 1, "column": 38}}}
```

`source=main | eval x = geoip(host)` answers `UNSUPPORTED_FUNCTION` with
"function geoip is not supported: LynxDB has no GeoIP database".

## Where errors appear

- **`POST /api/v1/query`, `GET /api/v1/query`**: the error object above,
  with the HTTP status of the table.
- **`POST /api/v1/query/stream`**: compile errors answer before the stream
  starts. An error while rows stream ends the stream with
  `{"__error": {"code": "EXECUTION_ERROR", "message": "..."}}`.
- **`GET /api/v1/query/explain`**: answers 200 with `"is_valid": false` and
  `"errors": [{"kind": "syntax", "message": ..., "position": ...,
  "expected": ..., "suggestion": ...}]`.
- **`GET /api/v1/tail`**: compile errors as above; a query with a command
  that needs all its input before producing output (such as `stats` or
  `sort`) answers 422 `{"error": "Aggregate is not supported in live tail:
  it needs all input rows before producing output"}`.
- **CLI**: `lynxdb query` prints the query with a caret under the position,
  then the message:

  ```
    source=main | stats count() by nosuchfield | where bad = 1
                                                       ^
    ERROR: semantic error at line 1, column 52: field bad not found (available fields: count(), nosuchfield)
  ```

## Errors at run time

Most problems in data do not fail a query: a value that cannot be converted,
an invalid date string or a division by zero gives NULL for that row.
Queries fail at run time when:

- the execution engine cannot run a construct the compiler accepted. The
  message starts with `not supported by the execution engine:`; for example,
  `earliest(x, t)` with a time field other than `_time`.
- a query exceeds its memory budget (`QUERY_MEMORY_EXCEEDED`, 400) or the
  server's query pool is exhausted (`QUERY_POOL_EXHAUSTED`, 503);
- it is cancelled or times out.

## Other request errors

| Code | HTTP | Meaning |
| --- | --- | --- |
| `QUERY_TOO_LARGE` | 400 | the query is longer than `query.max_query_length` |
| `VALIDATION_ERROR` | 400 | a missing query, an unsupported `format`, invalid bounds |
| `INVALID_JSON` | 400 | the request body is not valid JSON |
| `TOO_MANY_REQUESTS` | 429 | the concurrent query limit is reached; retry after `Retry-After` seconds |

See the [REST query API](../api/query.md) for the full response format.
