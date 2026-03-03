---
sidebar_position: 1
title: SPL2 Overview
description: Introduction to LynxDB's SPL2 query language -- pipelines, implicit fields, and execution model.
---

# SPL2 Overview

SPL2 is LynxDB's query language, inspired by Splunk's SPL. It's a **pipeline language**: data flows left-to-right through a series of commands separated by `|` (pipe).

SPL2 is used everywhere in LynxDB -- CLI queries, REST API, alerts, dashboards, materialized views, and saved queries. Learn it once, use it everywhere.

## Pipeline Concept

```
[search expression] | command1 args | command2 args | command3 args
```

Each command receives a stream of events, transforms it, and passes the result to the next command. This is analogous to Unix pipes:

```bash
# Unix: find | grep | sort | head
# SPL2: search | where | stats | sort | head
```

### Example Pipeline

```spl
source=nginx status>=500
  | stats count, avg(duration_ms) by uri
  | sort -count
  | head 10
  | table uri, count, avg(duration_ms)
```

This reads as:
1. **Search** nginx events with status 500+
2. **Aggregate** count and average duration by URI
3. **Sort** by count descending
4. **Limit** to top 10
5. **Project** specific columns

## Implicit Fields

Every event in LynxDB has these built-in fields:

| Field | Type | Description |
|-------|------|-------------|
| `_time` / `_timestamp` | datetime | Event timestamp (auto-detected or assigned at ingest) |
| `_raw` | string | Original raw text of the event |
| `_source` | string | Source identifier (e.g., "nginx", "api-gateway") |
| `_id` | string | Unique event ID (ULID, assigned at ingest) |

## Implicit FROM

If your query starts with `|` or a command name (not a search expression), LynxDB automatically prepends `FROM main`:

```spl
-- These are equivalent:
| stats count
FROM main | stats count

-- These are also equivalent:
| where level="error" | stats count
FROM main | where level="error" | stats count
```

## Search Expression

The first part of a pipeline (before the first `|`) is the **search expression**. It supports:

```spl
-- Keywords (full-text search)
error
"connection refused"

-- Field=value
level=error
status=500

-- Comparisons
status>=500
duration_ms>1000

-- Boolean operators
level=error source=nginx           -- implicit AND
level=error OR level=warn
level=error NOT source=redis

-- Wildcards
host=web-*
uri="/api/*"

-- FROM clause (explicit data source)
FROM main WHERE level="error"
FROM mv_errors_5m WHERE source="nginx"
```

## Command Categories

### Filtering
| Command | Description |
|---------|-------------|
| [`search`](/docs/spl2/commands/search) | Full-text keyword search |
| [`where`](/docs/spl2/commands/where) | Filter rows by expression |
| [`dedup`](/docs/spl2/commands/dedup) | Remove duplicate values |
| [`head`](/docs/spl2/commands/head) | Take first N results |
| [`tail`](/docs/spl2/commands/tail) | Take last N results |

### Aggregation
| Command | Description |
|---------|-------------|
| [`stats`](/docs/spl2/commands/stats) | Compute aggregations |
| [`timechart`](/docs/spl2/commands/timechart) | Time-series aggregation |
| [`top`](/docs/spl2/commands/top) | Most common values |
| [`rare`](/docs/spl2/commands/rare) | Least common values |
| [`eventstats`](/docs/spl2/commands/eventstats) | Aggregation without grouping |
| [`streamstats`](/docs/spl2/commands/streamstats) | Running aggregation |

### Transformation
| Command | Description |
|---------|-------------|
| [`eval`](/docs/spl2/commands/eval) | Compute new fields |
| [`rex`](/docs/spl2/commands/rex) | Extract fields via regex |
| [`rename`](/docs/spl2/commands/rename) | Rename fields |
| [`bin`](/docs/spl2/commands/bin) | Bucket numeric/time values |
| [`fillnull`](/docs/spl2/commands/fillnull) | Replace null values |

### Output
| Command | Description |
|---------|-------------|
| [`table`](/docs/spl2/commands/table) | Select and order columns |
| [`fields`](/docs/spl2/commands/fields) | Include or exclude fields |
| [`sort`](/docs/spl2/commands/sort) | Order results |

### Combining Data
| Command | Description |
|---------|-------------|
| [`join`](/docs/spl2/commands/join) | Join two datasets |
| [`append`](/docs/spl2/commands/append) | Append results from subsearch |
| [`multisearch`](/docs/spl2/commands/multisearch) | Union multiple searches |
| [`transaction`](/docs/spl2/commands/transaction) | Group related events |

### Data Source
| Command | Description |
|---------|-------------|
| [`from`](/docs/spl2/commands/from) | Read from index or view |

## Aggregation Functions

Used in `stats`, `timechart`, `eventstats`, and `streamstats`:

| Function | Description |
|----------|-------------|
| `count` | Count events |
| `sum(field)` | Sum values |
| `avg(field)` | Average value |
| `min(field)` | Minimum value |
| `max(field)` | Maximum value |
| `dc(field)` | Distinct count |
| `values(field)` | List distinct values |
| `stdev(field)` | Standard deviation |
| `perc50(field)` | 50th percentile (median) |
| `perc75(field)` | 75th percentile |
| `perc90(field)` | 90th percentile |
| `perc95(field)` | 95th percentile |
| `perc99(field)` | 99th percentile |
| `earliest(field)` | First value by time |
| `latest(field)` | Last value by time |

See [Aggregation Functions](/docs/spl2/functions/aggregation-functions) for details.

## Eval Functions

Used in `eval` and `where` expressions:

| Function | Description |
|----------|-------------|
| `IF(cond, true, false)` | Conditional |
| `CASE(c1,v1, c2,v2, ...)` | Multi-way conditional |
| `coalesce(a, b, ...)` | First non-null value |
| `tonumber(s)` | Convert to number |
| `tostring(n)` | Convert to string |
| `round(n, d)` | Round to d decimal places |
| `substr(s, start, len)` | Substring |
| `lower(s)` / `upper(s)` | Case conversion |
| `len(s)` | String length |
| `match(s, regex)` | Regex match |
| `strftime(t, fmt)` | Format timestamp |

See [Eval Functions](/docs/spl2/functions/eval-functions) for the full list.

## CTEs (Common Table Expressions)

Define reusable intermediate results:

```spl
$threats = FROM idx_backend WHERE threat_type IN ("sqli", "path_traversal") | FIELDS client_ip, threat_type;
$logins = FROM idx_audit WHERE type="USER_LOGIN" AND res="failed" | STATS count AS failures BY src_ip;
FROM $threats | JOIN type=inner client_ip [$logins] | WHERE failures > 5
  | TABLE client_ip, threat_type, failures
```

## Splunk Compatibility

LynxDB's SPL2 is inspired by Splunk's SPL but has some differences. When you accidentally use SPL1 syntax, LynxDB detects it and suggests the SPL2 equivalent:

```
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

See [Migrating from Splunk SPL](/docs/spl2/splunk-migration) for a full compatibility guide.
