---
sidebar_position: 1
title: Lynx Flow Overview
description: Introduction to LynxDB's query language -- Lynx Flow syntax with full SPL2 compatibility.
---

# Lynx Flow Overview

Lynx Flow is LynxDB's query language. It is a **pipeline language** -- data flows left-to-right through a series of commands separated by `|` (pipe). Both the Lynx Flow surface syntax and SPL2 syntax are first-class citizens -- the parser accepts either, and they compile to the same AST and execution plan.

Lynx Flow is used everywhere in LynxDB -- CLI queries, REST API, alerts, materialized views, and saved queries. Learn it once, use it everywhere.

## Pipeline Concept

```
from <source> | command1 args | command2 args | command3 args
```

Each command receives a stream of events, transforms it, and passes the result to the next command. This is analogous to Unix pipes:

```bash
# Unix: find | grep | sort | head
# Lynx Flow: from nginx | where status>=500 | group by uri compute count() | order by count desc | take 10
```

## Canonical Pipeline Rhythm

Every Lynx Flow query follows the same top-down rhythm:

```
from <source>            -- 1. Where does data come from?
| search ...             -- 2. Full-text / term-level pre-filter
| parse ...              -- 3. Extract structure from raw text
| let ...                -- 4. Derive new fields
| where ...              -- 5. Filter rows
| group / every ...      -- 6. Aggregate
| order by / take ...    -- 7. Order and limit
| table / select ...     -- 8. Shape output
```

Not every stage is required. A minimal query is just a source and a filter:

```
from nginx
| where status >= 500
```

### Example Pipeline

```
from nginx
| parse combined(_raw)
| where status >= 500
| group by uri compute count() as hits, avg(duration_ms) as avg_latency
| order by hits desc
| take 10
| table uri, hits, avg_latency
```

This reads as:
1. **Source** -- read nginx events
2. **Parse** -- extract fields from combined log format
3. **Filter** -- keep only 5xx errors
4. **Aggregate** -- count hits and average latency by URI
5. **Order** -- sort by hit count descending
6. **Limit** -- top 10
7. **Present** -- format output columns

## Design Principles

### Intent-Based Naming

Commands are named for what they *do*, not how they do it:

| Intent | Lynx Flow | SPL2 Equivalent |
|--------|-----------|-----------------|
| Derive a field | `let` | `eval` |
| Keep only these fields | `keep` | `fields` |
| Remove these fields | `omit` | `fields -` |
| First N rows | `take` | `head` |
| Aggregate by groups | `group by ... compute` | `stats ... by` |
| Time-bucket aggregation | `every 5m compute` | `timechart span=5m` |
| Streaming window agg | `running` | `streamstats` |
| Per-event global agg | `enrich` | `eventstats` |
| Ordered projection | `select` | -- |
| Parse structured formats | `parse json(...)` | `unpack_json` |
| Extract regex | `parse regex(...)` | `rex` |
| Expand arrays | `explode` | `unroll` |
| Full sort | `order by` | `sort` |

### One Stage = One Shape Transition

Each command transforms the data shape in exactly one way:

- `parse` adds columns (extraction)
- `let` adds or replaces columns (derivation)
- `where` removes rows (filtering)
- `keep` / `omit` removes columns (projection)
- `group` collapses rows (aggregation)
- `order by` reorders rows (ordering)
- `take` limits rows (truncation)

### Parse is a First-Class Step

Raw text is the starting point, not an afterthought. The `parse` command bridges `_raw` text and typed fields:

```
from nginx
| parse combined(_raw)
| where status >= 500
```

17 built-in formats: json, logfmt, syslog, combined, clf, regex, pattern, nginx_error, cef, kv, docker, redis, apache_error, postgres, mysql_slow, haproxy, leef, w3c.

### Core + Sugar Layers

| Layer | Purpose | Examples |
|-------|---------|---------|
| **Core** | Primitive operations (1:1 with pipeline operators) | `from`, `parse`, `let`, `where`, `keep`, `omit`, `group`, `every`, `bucket`, `order by`, `take`, `join`, `running`, `enrich`, `table`, `pack` |
| **Sugar** | Convenience shortcuts (desugar to core operators) | `top`, `bottom`, `rare`, `rank`, `topby`, `bottomby`, `latency`, `errors`, `rate`, `percentiles`, `slowest`, `lookup`, `head`, `tail`, `sort` |

Sugar commands have documented desugarings. Use core operators when you need more control.

## Implicit Fields

Every event in LynxDB has these built-in fields:

| Field | Type | Description |
|-------|------|-------------|
| `_time` / `_timestamp` | datetime | Event timestamp (auto-detected or assigned at ingest) |
| `_raw` | string | Original raw text of the event |
| `_source` | string | Source identifier (e.g., "nginx", "api-gateway") |
| `_id` | string | Unique event ID (ULID, assigned at ingest) |

## Implicit FROM

If your query starts with `|` (bare pipeline), LynxDB automatically prepends `from main`:

```
-- These are equivalent:
| where level="error" | group compute count()
from main | where level="error" | group compute count()
```

## SPL2 Compatibility

Lynx Flow and SPL2 are both first-class syntaxes. The parser accepts either and they compile to the same AST. You can mix them freely during migration.

```
-- Pure Lynx Flow
from nginx | where status >= 500 | group by uri compute count() as hits | order by hits desc | take 10

-- Pure SPL2
FROM nginx | where status >= 500 | stats count as hits by uri | sort -hits | head 10

-- Mixed (both valid)
from nginx | where status >= 500 | stats count as hits by uri | order by hits desc | take 10
```

When SPL2 syntax is used, advisory hints suggest the Lynx Flow equivalent (informational only).

## Command Categories

### Source & Search
| Command | Description |
|---------|-------------|
| [`from`](/docs/lynx-flow/commands/from) / `index` | Select data source |
| [`search`](/docs/lynx-flow/commands/search) | Full-text keyword search |

### Parsing
| Command | Description |
|---------|-------------|
| `parse <format>(...)` | Extract structure from raw text (17 formats) |
| `explode` / [`unroll`](/docs/lynx-flow/commands/unroll) | Expand array fields into rows |

### Derivation & Filtering
| Command | Description |
|---------|-------------|
| `let` / [`eval`](/docs/lynx-flow/commands/eval) | Compute new fields |
| [`where`](/docs/lynx-flow/commands/where) | Filter rows by expression |
| [`fillnull`](/docs/lynx-flow/commands/fillnull) | Replace null values |

### Field Shaping
| Command | Description |
|---------|-------------|
| `keep` / [`fields`](/docs/lynx-flow/commands/fields) | Include only listed fields |
| `omit` / `fields -` | Exclude listed fields |
| `select` | Ordered projection with inline rename |
| [`rename`](/docs/lynx-flow/commands/rename) | Rename fields |

### Aggregation
| Command | Description |
|---------|-------------|
| `group by ... compute` / [`stats`](/docs/lynx-flow/commands/stats) | Grouped or global aggregation |
| `every <span> compute` / [`timechart`](/docs/lynx-flow/commands/timechart) | Time-bucketed aggregation |
| `bucket` / [`bin`](/docs/lynx-flow/commands/bin) | Add time-bucket column |
| `running` / [`streamstats`](/docs/lynx-flow/commands/streamstats) | Streaming window aggregation |
| `enrich` / [`eventstats`](/docs/lynx-flow/commands/eventstats) | Per-event global aggregation |

### Ranking & Ordering
| Command | Description |
|---------|-------------|
| `order by` / [`sort`](/docs/lynx-flow/commands/sort) | Order results |
| `take` / [`head`](/docs/lynx-flow/commands/head) | First N results |
| [`tail`](/docs/lynx-flow/commands/tail) | Last N results |
| `rank top/bottom` | Row-level ranking |
| `topby` / `bottomby` | Grouped metric ranking |
| [`top`](/docs/lynx-flow/commands/top) / `bottom` / [`rare`](/docs/lynx-flow/commands/rare) | Frequency ranking |
| [`dedup`](/docs/lynx-flow/commands/dedup) | Remove duplicates |

### Combining Data
| Command | Description |
|---------|-------------|
| [`join`](/docs/lynx-flow/commands/join) | Join two datasets |
| `lookup` | Enrich with a dataset (sugar for left join) |
| [`append`](/docs/lynx-flow/commands/append) | Append results from subsearch |
| [`multisearch`](/docs/lynx-flow/commands/multisearch) | Union multiple searches |
| [`transaction`](/docs/lynx-flow/commands/transaction) | Group related events |

### Presentation
| Command | Description |
|---------|-------------|
| [`table`](/docs/lynx-flow/commands/table) | Format output columns |
| [`xyseries`](/docs/lynx-flow/commands/xyseries) | Pivot to cross-tabulation |
| `pack` / [`pack_json`](/docs/lynx-flow/commands/pack-json) | Assemble fields into JSON |

### Domain Sugar
| Command | Description |
|---------|-------------|
| `latency` | Percentile time-series for duration fields |
| `errors` | Error analysis shortcut |
| `rate` | Event rate over time |
| `percentiles` | Multi-percentile summary |
| `slowest` | Top N by duration |

### Views & CTEs
| Command | Description |
|---------|-------------|
| `materialize` | Create materialized view |
| `views` | List/inspect materialized views |
| `dropview` | Delete a materialized view |
| `$name = ...` | Define CTE (Common Table Expression) |

## Aggregation Functions

Used in `group compute`, `every compute`, `running`, `enrich`, `stats`, `timechart`, `eventstats`, and `streamstats`:

| Function | Description |
|----------|-------------|
| `count()` | Count events |
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

See [Aggregation Functions](/docs/lynx-flow/functions/aggregation-functions) for details.

## Eval Functions

Used in `let`, `eval`, `where`, and any expression context:

| Function | Description |
|----------|-------------|
| `if(cond, true, false)` | Conditional |
| `case(c1,v1, c2,v2, ...)` | Multi-way conditional |
| `coalesce(a, b, ...)` | First non-null value |
| `tonumber(s)` | Convert to number |
| `tostring(n)` | Convert to string |
| `round(n, d)` | Round to d decimal places |
| `substr(s, start, len)` | Substring |
| `lower(s)` / `upper(s)` | Case conversion |
| `len(s)` | String length |
| `match(s, regex)` | Regex match |
| `strftime(t, fmt)` | Format timestamp |

See [Eval Functions](/docs/lynx-flow/functions/eval-functions) for the full list.

## CTEs (Common Table Expressions)

Define reusable intermediate results:

```
$threats = from threat_intel
  | where threat_type in ("sqli", "path_traversal")
  | keep client_ip, threat_type;

$logins = from audit
  | where type = "login" AND result = "failed"
  | group by src_ip compute count() as failures;

from $threats
| join type=inner client_ip [from $logins | rename src_ip as client_ip]
| table client_ip, threat_type, failures
```

## Quick Reference

For the complete command-by-command reference with syntax, examples, desugarings, and SPL2 equivalents, see the [Lynx Flow Reference](/docs/lynx-flow/reference).

## See Also

- [Lynx Flow Reference](/docs/lynx-flow/reference) -- Complete language reference
- [Search Syntax](/docs/lynx-flow/search-syntax) -- Boolean operators, wildcards, field=value
- [Data Types](/docs/lynx-flow/data-types) -- String, number, boolean, timestamp, null
- [Time Ranges](/docs/lynx-flow/time-ranges) -- Relative and absolute time ranges
- [Functions](/docs/lynx-flow/functions/aggregation-functions) -- Aggregation functions
- [Migrating from Splunk](/docs/lynx-flow/splunk-migration) -- SPL to Lynx Flow migration guide
