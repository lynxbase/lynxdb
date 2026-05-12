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

## sumsq

Sum of squared numeric values.

```spl
| stats sumsq(duration_ms) AS duration_squares by endpoint
```

## dc (Distinct Count)

Count of unique values.

```spl
| stats dc(user_id) AS unique_users by endpoint
| stats dc(source) AS source_count
| stats estdc(user_id) AS estimated_unique_users
| stats estdc_error(user_id) AS estimated_unique_error
```

## values

List of distinct values (returned as a multivalue field).

```spl
| stats values(level) AS seen_levels by source
```

## mode

Most frequent value, compared as a string.

```spl
| stats mode(status) AS common_status by endpoint
```

## per_second / per_minute / per_hour / per_day

Scale numeric bucket totals to a fixed time period in `timechart`.

```spl
| timechart span=5m per_minute(bytes) AS bytes_per_minute
```

## earliest_time / latest_time / rate

Return timestamp bounds or per-second counter change by event time.

```spl
| stats earliest_time(counter) AS first_seen,
        latest_time(counter) AS last_seen,
        rate(counter) AS counter_rate
  by host
```

## stdev

Standard deviation.

```spl
| stats stdev(duration_ms) AS latency_stddev by endpoint
| stats stdevp(duration_ms) AS population_stddev,
        var(duration_ms) AS sample_variance,
        varp(duration_ms) AS population_variance
```

## Percentiles

Compute percentile values with the fixed percentile aggregations: `perc25`, `perc50`, `perc75`, `perc90`, `perc95`, `perc99`.

```spl
| stats perc25(duration_ms) AS p25,
        perc50(duration_ms) AS median,
        perc95(duration_ms) AS p95,
        perc99(duration_ms) AS p99
  by endpoint
```

`perc50` is the median. Generic forms such as `perc(duration_ms, 95)` and `percentile(duration_ms, 95)` normalize to the fixed percentile aggregations when the percentile is one of the supported values. Arbitrary variable-percentile syntax such as `percentile(duration_ms, 99.9)` is not currently supported.

## earliest / latest

First and last values by `_time`. `first` is an alias of `earliest`, and `last` is an alias of `latest`.

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
| `perc25(f)` | 25th percentile | `perc25(duration_ms)` |
| `perc50(f)` | Median (50th pct) | `perc50(duration_ms)` |
| `perc75(f)` | 75th percentile | `perc75(duration_ms)` |
| `perc90(f)` | 90th percentile | `perc90(duration_ms)` |
| `perc95(f)` | 95th percentile | `perc95(duration_ms)` |
| `perc99(f)` | 99th percentile | `perc99(duration_ms)` |
| `earliest(f)` / `first(f)` | First by time | `earliest(status)` |
| `latest(f)` / `last(f)` | Last by time | `latest(status)` |
