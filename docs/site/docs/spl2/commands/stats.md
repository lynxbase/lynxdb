---
title: stats
description: Compute aggregations over events, optionally grouped by fields.
---

# stats

Compute aggregations over events. Optionally group results by one or more fields.

## Syntax

```spl
| stats <agg-function> [AS <alias>] [, <agg-function> [AS <alias>] ...] [BY <field> [, <field> ...]]
```

## Aggregation Functions

| Function | Description |
|----------|-------------|
| `count` | Count events |
| `count(field)` | Count events where field is not null |
| `count(eval(expr))` | Count events matching expression |
| `sum(field)` | Sum of values |
| `avg(field)` | Average value |
| `min(field)` | Minimum value |
| `max(field)` | Maximum value |
| `dc(field)` | Distinct count |
| `values(field)` | List of distinct values |
| `stdev(field)` | Standard deviation |
| `perc50(field)` | 50th percentile (median) |
| `perc75(field)` | 75th percentile |
| `perc90(field)` | 90th percentile |
| `perc95(field)` | 95th percentile |
| `perc99(field)` | 99th percentile |
| `earliest(field)` | First value by time |
| `latest(field)` | Last value by time |

## Examples

```spl
-- Count all events
| stats count

-- Count by field
level=error | stats count by source

-- Multiple aggregations
source=nginx | stats count, avg(duration_ms), p99(duration_ms) by uri

-- With aliases
source=nginx | stats count as requests, avg(duration_ms) as avg_latency by uri

-- Conditional counting
source=nginx | stats count as total, count(eval(status>=500)) as errors by uri

-- Multiple group-by fields
| stats count by source, level

-- Distinct count
| stats dc(user_id) as unique_users by endpoint

-- Percentiles
| stats perc50(duration_ms) as median, perc95(duration_ms) as p95, perc99(duration_ms) as p99 by service
```

## Notes

- `stats` is a **transforming** command: it replaces the event stream with aggregated rows.
- For running aggregations (maintaining the original event stream), use [`streamstats`](/docs/spl2/commands/streamstats).
- For adding aggregation columns without collapsing rows, use [`eventstats`](/docs/spl2/commands/eventstats).
- The optimizer applies **partial aggregation**: each segment computes partial results that are merged globally.
- `count(eval(...))` counts events where the eval expression is truthy.

## See Also

- [timechart](/docs/spl2/commands/timechart) -- Time-series aggregation
- [top](/docs/spl2/commands/top) -- Most common values
- [eventstats](/docs/spl2/commands/eventstats) -- Add stats without grouping
- [streamstats](/docs/spl2/commands/streamstats) -- Running aggregation
- [Aggregation Functions](/docs/spl2/functions/aggregation-functions) -- Detailed function reference
