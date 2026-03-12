---
title: Search and Filter Logs
description: How to search and filter logs in LynxDB using full-text search, field-value filters, boolean operators, wildcards, and the WHERE command.
---

# Search and Filter Logs

LynxDB provides multiple ways to find the events you need: full-text search across raw log text, field-value filters, boolean operators, wildcards, and the `WHERE` command for typed expressions. This guide shows how to use each technique.

## Full-text search

The simplest way to find events is to search for text that appears anywhere in the raw log line.

### Search for a keyword

```bash
lynxdb query 'search "connection refused"'
```

The [`SEARCH`](/docs/lynx-flow/commands/search) command scans the `_raw` field of every event. LynxDB uses an FST-based inverted index with bloom filters, so full-text search is fast even over millions of events.

### Search for multiple terms

Terms separated by spaces are ANDed together:

```bash
lynxdb query 'search "timeout" "redis"'
```

This returns events containing both "timeout" and "redis".

### Search without the SEARCH keyword

When your query starts with a bare string, LynxDB treats it as an implicit search:

```bash
lynxdb query '"connection refused"'
```

---

## Field-value filters

If your events have structured fields (JSON logs, or fields extracted at ingest time), filter directly on field values.

### Exact match

```bash
lynxdb query 'level=error'
lynxdb query '_source=nginx'
lynxdb query 'host="web-01"'
```

### Numeric comparison

```bash
lynxdb query 'status>=500'
lynxdb query 'duration_ms>1000'
lynxdb query 'status!=200'
```

### Combine multiple filters

Multiple field-value pairs are ANDed together:

```bash
lynxdb query '_source=nginx status>=500'
```

This returns events where `source` is "nginx" AND `status` is 500 or above.

---

## Boolean operators

Use `AND`, `OR`, and `NOT` for more complex filter logic. These work in both the implicit search and the [`WHERE`](/docs/lynx-flow/commands/where) command.

### In the search expression

```bash
lynxdb query 'level=error OR level=warn'
lynxdb query '_source=nginx NOT status=200'
lynxdb query '(level=error OR level=warn) source=nginx'
```

### In a WHERE clause

The `WHERE` command supports full boolean logic with typed expressions:

```bash
lynxdb query '| where level="error" OR level="warn"'
lynxdb query '| where status>=500 AND source="nginx"'
lynxdb query '| where NOT (status>=200 AND status<300)'
```

---

## Wildcards

Use `*` as a wildcard in field-value filters:

```bash
# Match any host starting with "web-"
lynxdb query 'host=web-*'

# Match any source ending in "-gateway"
lynxdb query '_source=*-gateway'

# Match paths containing "api"
lynxdb query 'path=*api*'
```

Wildcards work in the search expression. For more complex pattern matching inside `WHERE`, use the `match()` eval function:

```bash
lynxdb query '| where match(path, "^/api/v[0-9]+")'
```

See the [eval functions reference](/docs/lynx-flow/functions/eval-functions) for details on `match()`.

---

## The WHERE command

[`WHERE`](/docs/lynx-flow/commands/where) is the primary filtering command in SPL2 pipelines. It evaluates a typed boolean expression and keeps only events where the expression is true.

### Basic filtering

```bash
lynxdb query '| where level="error"'
lynxdb query '| where status>=500'
lynxdb query '| where duration_ms > 1000 AND source="nginx"'
```

### Using functions in WHERE

```bash
# Case-insensitive match
lynxdb query '| where lower(level)="error"'

# Regular expression match
lynxdb query '| where match(message, "timeout|refused")'

# Null checks
lynxdb query '| where isnotnull(user_id)'
lynxdb query '| where isnull(response_code)'
```

### Filtering after aggregation

`WHERE` can appear anywhere in the pipeline, including after `STATS`:

```bash
lynxdb query '_source=nginx | stats count by uri | where count > 100'
```

---

## The FROM command

Use [`FROM`](/docs/lynx-flow/commands/from) to query a specific index:

```bash
lynxdb query 'FROM production | where level="error" | stats count by service'
```

When you omit `FROM`, LynxDB queries the default `main` index. If your query starts with `|`, `FROM main` is prepended automatically.

---

## Time range filters

Narrow your search to a specific time window.

### Relative time

```bash
lynxdb query 'level=error' --since 1h
lynxdb query 'level=error' --since 15m
lynxdb query 'level=error' --since 7d
```

### Absolute time

```bash
lynxdb query 'level=error' \
  --from 2026-01-15T00:00:00Z \
  --to 2026-01-15T23:59:59Z
```

See the [time ranges reference](/docs/lynx-flow/time-ranges) for all supported formats.

---

## Limiting results

### Head and tail

Use [`HEAD`](/docs/lynx-flow/commands/head) to return only the first N results:

```bash
lynxdb query '_source=nginx status>=500 | head 10'
```

Use [`TAIL`](/docs/lynx-flow/commands/tail) for the last N:

```bash
lynxdb query '_source=nginx | tail 5'
```

### Dedup

Remove duplicate events based on a field with [`DEDUP`](/docs/lynx-flow/commands/dedup):

```bash
lynxdb query 'level=error | dedup host'
```

---

## Selecting and renaming fields

### TABLE -- pick specific columns

```bash
lynxdb query 'level=error | table _timestamp, source, message'
```

### FIELDS -- include or exclude

```bash
lynxdb query 'level=error | fields source, message'
lynxdb query 'level=error | fields - _raw'
```

### RENAME -- change field names

```bash
lynxdb query '| stats count by source | rename count AS total_events'
```

See the [`TABLE`](/docs/lynx-flow/commands/table), [`FIELDS`](/docs/lynx-flow/commands/fields), and [`RENAME`](/docs/lynx-flow/commands/rename) command references.

---

## Searching local files (no server)

All the search techniques above work in pipe mode and file mode:

```bash
# Search a local file
lynxdb query --file access.log '| where status>=500 | head 20'

# Search stdin
cat /var/log/syslog | lynxdb query '| where level="ERROR" | stats count by service'

# Search multiple files with glob
lynxdb query --file '/var/log/nginx/*.log' 'status>=500 | top 10 uri'
```

See the [pipe mode guide](/docs/getting-started/pipe-mode) for details.

---

## Quick-access shortcuts

LynxDB provides shortcut commands for common search patterns:

```bash
# Quick count
lynxdb count 'level=error' --since 1h

# Peek at data shape
lynxdb sample 5 '_source=nginx'

# See field catalog
lynxdb fields status --values
```

See the [CLI shortcuts reference](/docs/cli/shortcuts) for the full list.

---

## Next steps

- [Run aggregations](/docs/guides/aggregations) -- compute statistics from your filtered events
- [Extract fields at query time](/docs/guides/field-extraction) -- parse unstructured logs with REX and EVAL
- [SPL2 search syntax](/docs/lynx-flow/search-syntax) -- full reference for search expressions
- [WHERE command](/docs/lynx-flow/commands/where) -- complete WHERE syntax and examples
