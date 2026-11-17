---
title: Getting started with PPL
sidebar_label: Getting started
description: Run your first PPL queries on a log file and on a LynxDB server.
---

# Getting started with PPL

## Run a query

On a file, without a server:

```bash
lynxdb query --file access.log 'source=main | stats count() by level'
```

The CLI loads the file into an in-process index named `main`, detects common
log formats, extracts `key=value` pairs and the fields of combined, syslog
and docker lines, and runs the query. Piped input works the same way:

```bash
cat access.log | lynxdb query 'source=main | where status >= 500 | top 5 path'
```

On a server (default `http://localhost:3100`), the same text queries the
stored indexes:

```bash
lynxdb query 'source=main | stats count() by level' --since 1h
curl -s -X POST localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main | stats count() by level", "from": "-1h"}'
```

The examples on this page run over lines such as:

```
2025-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 bytes=32228 msg="GET /api/v1/users completed"
```

## Anatomy of a query

```
source=main level=ERROR | stats count() as errors by host | sort - errors
```

- `source=main` names the index to read. `search source=main` and
  `index=main` mean the same.
- `level=ERROR` is a search term of the source command. Terms after the
  source keep only matching events; see [Search semantics](./search-and-types.md).
- `| stats ...`, `| sort ...` are commands. Each one reads the rows of the
  command before it.

Command names and keywords are case-insensitive; field names are
case-sensitive.

## Filter

Free-text and field terms in the source command:

```
source=main "auth/login" status>=500 earliest=-1h
```

Boolean expressions with `where`:

```
source=main | where status >= 500 AND host LIKE 'web%'
source=main | where path IN ('/api/v1/users', '/api/v1/orders')
```

## Shape the rows

```
source=main
| where response_time > 500
| eval kb = bytes / 1024
| fields _time, host, path, response_time, kb
| head 3
```

```
_time                     host    path            response_time  kb
2025-01-15 08:00:00.105   web-03  /api/v1/users   786            31
2025-01-15 08:00:03.072   web-02  /api/v1/orders  588            21
2025-01-15 08:00:03.556   web-01  /api/v1/health  776            48
```

Write spaces around `-` and `*`: `bytes-1` and `bytes*8` are field names in
PPL, so arithmetic is `bytes - 1` and `bytes * 8`. See [Syntax](./syntax.md).

## Aggregate

```
source=main status>=500
| stats count() as errors, p95(response_time) as p95 by host
| sort - errors
```

```
host    errors  p95
api-02  48      777.9
web-02  43      775.5
web-03  41      784
web-01  38      792.1
api-01  34      747.8
```

An aggregate without an alias produces a column named after its text, such
as `count()`. Give it an alias when a later command refers to it.

## Over time

```
source=main | timechart span=5m count() by level
source=main | stats count() by span(_time, 1h), status
```

See [Time series](./time-series.md).

## Inspect a plan

`explain` before a query, `lynxdb query --explain` or `lynxdb explain`
print the plan instead of running the query:

```bash
lynxdb query --explain 'source=main error | where host = "web-01" | stats count() by status'
```

```
Logical plan:
Project(`count()`, status)
└─ Aggregate(`count()` = count() BY status)
   └─ Filter(host = "web-01")
      └─ Filter(SEARCH_WORD(_raw, "error"))
         └─ Scan(main)

Optimized plan:
Project(`count()`, status)
└─ Aggregate(`count()` = count() BY status)
   └─ Filter(SEARCH_WORD(_raw, "error") AND (host = "web-01"))
      └─ Scan(main) columns=[_raw, _time, host, status] prune=SEARCH_WORD(_raw, "error") prune=host = "web-01"

Optimizer rules:
  merge-filters x1
  prune-scan x1
```

## When a query fails

Errors carry a kind and a position:

```
source=main | stats count() by host | where nope = 1
                                            ^
semantic error at line 1, column 45: field nope not found (available fields: count(), host)
```

See [Error handling](./errors.md). Next: the [command guide](./command-guide.md)
and the [examples](./examples.md).
