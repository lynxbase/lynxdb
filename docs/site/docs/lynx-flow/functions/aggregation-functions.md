---
title: Aggregation Functions
description: Complete reference for aggregation functions -- count, sum, avg, percentiles, and more.
---

# Aggregation Functions

Aggregation functions are used in `group compute`, `every compute`, `running`, `enrich` (Lynx Flow) and [`stats`](/docs/lynx-flow/commands/stats), [`timechart`](/docs/lynx-flow/commands/timechart), [`eventstats`](/docs/lynx-flow/commands/eventstats), [`streamstats`](/docs/lynx-flow/commands/streamstats) (SPL2).

## count

Count the number of events.

```spl
| stats count
| stats count AS total_events
| stats count(field)                    -- count non-null values
| stats count(eval(status>=500)) AS errors  -- conditional count
```

## sum

Sum of numeric field values.

```spl
| stats sum(bytes) AS total_bytes by source
```

## avg

Average (mean) of numeric field values.

```spl
| stats avg(duration_ms) AS avg_latency by endpoint
```

## min / max

Minimum and maximum values.

```spl
| stats min(duration_ms) AS fastest, max(duration_ms) AS slowest by uri
```

## dc (Distinct Count)

Count of unique values.

```spl
| stats dc(user_id) AS unique_users by endpoint
| stats dc(source) AS source_count
```

## values

List of distinct values (returned as a multivalue field).

```spl
| stats values(level) AS seen_levels by source
```

## stdev

Standard deviation.

```spl
| stats stdev(duration_ms) AS latency_stddev by endpoint
```

## Percentiles

Compute percentile values: `perc50`, `perc75`, `perc90`, `perc95`, `perc99`.

```spl
| stats perc50(duration_ms) AS median,
        perc95(duration_ms) AS p95,
        perc99(duration_ms) AS p99
  by endpoint
```

`perc50` is the median. Percentiles use the t-digest algorithm for memory-efficient approximate computation.

## earliest / latest

First and last values by `_time`.

```spl
| stats earliest(status) AS first_status, latest(status) AS last_status by host
```

## Conditional Aggregation

Use `eval()` inside `count` or other functions for conditional aggregation:

```spl
| stats count AS total,
        count(eval(status>=500)) AS errors,
        count(eval(status>=200 AND status<300)) AS success
  by uri
| eval error_rate = round(errors/total*100, 1)
```

## Summary Table

| Function | Description | Example |
|----------|-------------|---------|
| `count` | Count events | `count`, `count(field)`, `count(eval(...))` |
| `sum(f)` | Sum values | `sum(bytes)` |
| `avg(f)` | Average | `avg(duration_ms)` |
| `min(f)` | Minimum | `min(duration_ms)` |
| `max(f)` | Maximum | `max(duration_ms)` |
| `dc(f)` | Distinct count | `dc(user_id)` |
| `values(f)` | Distinct values list | `values(level)` |
| `stdev(f)` | Standard deviation | `stdev(duration_ms)` |
| `perc50(f)` | Median (50th pct) | `perc50(duration_ms)` |
| `perc75(f)` | 75th percentile | `perc75(duration_ms)` |
| `perc90(f)` | 90th percentile | `perc90(duration_ms)` |
| `perc95(f)` | 95th percentile | `perc95(duration_ms)` |
| `perc99(f)` | 99th percentile | `perc99(duration_ms)` |
| `earliest(f)` | First by time | `earliest(status)` |
| `latest(f)` | Last by time | `latest(status)` |
