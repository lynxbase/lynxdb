---
sidebar_position: 5
title: Your First Query
description: PPL basics in LynxDB - search, filter, aggregate, sort and extract fields.
---

# Your First Query

LynxDB queries are written in [OpenSearch PPL](../ppl/overview.md), the
Piped Processing Language. A query names an index, then passes rows through
commands separated by `|`, left to right.

The examples run over lines such as:

```text
2025-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 bytes=32228 msg="GET /api/v1/users completed"
```

Run them on a file with `lynxdb query --file access.log '<query>'`, or on a
server with `lynxdb query '<query>'`.

## Search

Every query starts with a source. Terms after it keep only matching events:

```
source=main error
source=main "connection refused"
source=main level=ERROR host=web-*
source=main status>=500
source=main level IN (WARN, ERROR) NOT host=api-01
```

A bare word matches a whole token of the raw text, ignoring case. A field
comparison such as `level=ERROR` is exact and case-sensitive. See
[Search semantics](../ppl/search-and-types.md).

## Filter with `where`

Search terms belong to the source command. Later in the pipeline, filter
with `where` and an expression:

```
source=main | where status >= 500 AND response_time > 700
source=main | where like(path, '/api/v1/auth%')
source=main | where level = "ERROR" OR status = 503
```

## Aggregate with `stats`

```
source=main | stats count()
source=main level=ERROR | stats count() as errors by host
source=main | stats count() as requests, avg(response_time) as avg_ms, p95(response_time) as p95_ms by host
```

Name an aggregate with `as` so later commands can refer to it. Unnamed, the
column is called `count()`.

## Sort and limit

```
source=main level=ERROR | stats count() as errors by path | sort - errors | head 3
```

```text
errors,path
40,/api/v1/users
38,/api/v1/orders
35,/api/v1/search
```

`sort - errors` sorts descending; `sort errors` ascending. `top 3 path`
returns the three most frequent values with their counts.

## Choose columns

```
source=main | fields host, status, msg
source=main | fields - _raw
source=main | rename response_time as rt
```

## Compute fields with `eval`

```
source=main
| stats count() as total, count(eval(status >= 500)) as errors by path
| eval error_pct = round(errors * 100.0 / total, 1)
| sort - error_pct
| head 3
```

```text
error_pct,errors,path,total
25.5,28,/api/v1/users,110
24.8,25,/api/v1/health,101
22.5,27,/api/v1/orders,120
```

Write spaces around `-` and `*` in arithmetic: `bytes * 8`, not `bytes*8`.
Integer division truncates, so multiply by `100.0` for a percentage.

## Count over time

```
source=main | timechart span=1m count() by level
source=main | stats count() by span(_time, 5m), host
```

See [Time series](../ppl/time-series.md).

## Extract fields

`parse` pulls fields out of text with named regular expression groups:

```
source=main
| parse msg '(?<verb>[A-Z]+) (?<route>\S+) completed'
| stats count() as n by verb
| sort - n
```

```text
n,verb
260,PUT
251,GET
249,DELETE
240,POST
```

`rex`, `grok` and `spath` cover other cases; see
[Field Extraction](../guides/field-extraction.md).

## Quick reference

| Command | Purpose | Example |
|---------|---------|---------|
| `source=` | Read an index, search terms | `source=main level=ERROR` |
| `where` | Filter rows | `where status >= 500` |
| `stats` | Aggregate | `stats count() as n by host` |
| `eval` | Compute fields | `eval kb = bytes / 1024.0` |
| `sort` | Order rows | `sort - n` |
| `head` | Limit rows | `head 10` |
| `fields` | Keep or drop columns | `fields host, status` |
| `rename` | Rename a column | `rename response_time as rt` |
| `top` / `rare` | Most and least frequent values | `top 10 path` |
| `dedup` | Keep the first row per value | `dedup host` |
| `timechart` | Aggregate over time buckets | `timechart span=5m count() by level` |
| `parse` | Extract with a regex | `parse msg '(?<verb>[A-Z]+) .*'` |

## Next steps

- [Getting started with PPL](../ppl/getting-started.md): a longer walkthrough
- [Command guide](../ppl/command-guide.md): the main commands in depth
- [Function guide](../ppl/function-guide.md) and [Aggregation guide](../ppl/aggregation-guide.md)
- [Examples](../ppl/examples.md): queries for common log questions
- [Supported commands](../ppl/commands.md): what LynxDB implements
