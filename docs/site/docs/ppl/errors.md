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
| planning | `PLANNING_ERROR` | 400 | the query does not compile for a reason other than the kinds above |
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
- **`POST /api/v1/query/stream`**: the query runs before the first row is
  written, so compile and run-time errors answer with the same error object
  and status instead of a stream.
- **`GET /api/v1/query/explain`**: answers 200 with `"is_valid": false` and
  `"errors": [{"kind": "syntax", "message": ..., "position": ...,
  "expected": ..., "suggestion": ...}]`.
- **CLI**: `lynxdb query` prints the query with a caret under the position,
  then the message, and exits with code 4:

  ```
    source=main | stats count() by nosuchfield | where bad = 1
                                                       ^
    ERROR: semantic error at line 1, column 52: field bad not found (available fields: count(), nosuchfield)
  ```

## Errors at run time

Most problems in data do not fail a query: a value that cannot be converted,
an invalid date string or a division by zero gives NULL for that row.
Queries fail at run time when:

- the row operators cannot run a construct the compiler accepted. The
  message starts with `not supported by the execution engine:` and the REST
  API answers `422 UNSUPPORTED_FEATURE`, as for `fillnull value=host`, whose
  fill value is not a constant:

  ```
  ERROR: not supported by the execution engine: fillnull over all fields with a non-constant value
  ```

- a `sort` that the row operators run holds more than 10,000,000 rows;
- the query is cancelled or times out.

## Other request errors

| Code | HTTP | Meaning |
| --- | --- | --- |
| `QUERY_TOO_LARGE` | 400 | the query is longer than `query.max_query_length` |
| `VALIDATION_ERROR` | 400 | a missing query, an unsupported `format`, an unparsable `from` or `to` |
| `INVALID_JSON` | 400 | the request body is not valid JSON |
| `TOO_MANY_REQUESTS` | 429 | `query.max_concurrent` queries are running; retry after `Retry-After` seconds |

See the [REST query API](../api/query.md) for the full response format.
