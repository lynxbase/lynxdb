---
title: Time Series Analysis
description: How to analyze log data over time using TIMECHART, BIN, time_bucket, and the span parameter in LynxDB.
---

# Time Series Analysis

Log analytics often requires understanding trends over time: when do errors spike? Is latency increasing? How does traffic change throughout the day? LynxDB provides [`TIMECHART`](/docs/spl2/commands/timechart), [`BIN`](/docs/spl2/commands/bin), and the `time_bucket()` eval function for time-based aggregation.

## TIMECHART -- time series aggregation

The [`TIMECHART`](/docs/spl2/commands/timechart) command is the primary tool for time series analysis. It buckets events into time intervals and computes aggregations for each bucket.

### Basic time series

Count events per 5-minute interval:

```bash
lynxdb query 'level=error | timechart count span=5m'
```

### Choose the time span

The `span` parameter controls the bucket size:

```bash
# 1-minute granularity (high detail)
lynxdb query 'level=error | timechart count span=1m' --since 1h

# 1-hour granularity (overview)
lynxdb query 'level=error | timechart count span=1h' --since 7d

# 1-day granularity (trend)
lynxdb query 'level=error | timechart count span=1d' --since 30d
```

Common span values: `1m`, `5m`, `10m`, `15m`, `30m`, `1h`, `6h`, `12h`, `1d`.

### Aggregation functions in TIMECHART

Use any aggregation function, not just count:

```bash
# Average latency over time
lynxdb query '_source=nginx | timechart avg(duration_ms) span=5m'

# P99 latency over time
lynxdb query '_source=nginx | timechart perc99(duration_ms) AS p99 span=5m'

# Multiple aggregations
lynxdb query '_source=nginx | timechart count, avg(duration_ms) AS avg_lat, perc99(duration_ms) AS p99_lat span=5m'

# Sum of bytes transferred
lynxdb query '_source=nginx | timechart sum(bytes) AS total_bytes span=1h'
```

### Split by a field

Use `by <field>` to produce separate series for each value:

```bash
# Error count over time, split by source
lynxdb query 'level=error | timechart count span=5m by source'

# Latency by endpoint
lynxdb query '_source=nginx | timechart avg(duration_ms) span=5m by uri'

# Status code distribution over time
lynxdb query '_source=nginx | timechart count span=5m by status'
```

---

## BIN -- bucket timestamps

The [`BIN`](/docs/spl2/commands/bin) command groups the `_timestamp` field into fixed-size time buckets without aggregating. This is useful when you want to assign events to time windows and then aggregate with `STATS`.

### Basic binning

```bash
lynxdb query '_source=nginx
  | bin _timestamp span=5m
  | stats count, avg(duration_ms) AS avg_lat by _timestamp'
```

### Correlate metrics with binned timestamps

```bash
lynxdb query '_source=postgres duration_ms>1000
  | bin _timestamp span=5m
  | stats count AS slow_queries, avg(duration_ms) AS avg_latency by _timestamp
  | where slow_queries > 10'
```

### BIN vs TIMECHART

| Feature | TIMECHART | BIN + STATS |
|---------|-----------|-------------|
| Convenience | One command | Two commands |
| Split by | Built-in `by` clause | Manual `by` in STATS |
| Flexibility | Fixed to time grouping | Can combine time with other groupings |
| Typical use | Quick time series charts | Complex multi-dimensional analysis |

Use `TIMECHART` for straightforward time series. Use `BIN + STATS` when you need to combine time bucketing with grouping by multiple other fields.

---

## time_bucket() in EVAL and STATS

The `time_bucket()` function works inside [`EVAL`](/docs/spl2/commands/eval) and [`STATS`](/docs/spl2/commands/stats) expressions for fine-grained control.

### Use in STATS

```bash
lynxdb query 'level=error
  | stats count by source, time_bucket(_timestamp, "5m") AS bucket
  | sort bucket'
```

### Use in EVAL

```bash
lynxdb query '_source=nginx
  | eval hour_bucket = time_bucket(_timestamp, "1h")
  | stats avg(duration_ms) AS avg_lat, count by hour_bucket
  | sort hour_bucket'
```

### time_bucket() in materialized views

`time_bucket()` is essential for defining [materialized views](/docs/guides/materialized-views):

```bash
lynxdb mv create mv_errors_5m \
  'level=error | stats count, avg(duration) by source, time_bucket(_timestamp, "5m") AS bucket' \
  --retention 90d
```

---

## Practical examples

### Find error spikes

Identify 5-minute windows with abnormally high error counts:

```bash
lynxdb query 'level=error
  | timechart count span=5m
  | where count > 100' --since 24h
```

### Compare latency across endpoints

```bash
lynxdb query '_source=nginx
  | timechart avg(duration_ms) span=10m by uri' --since 6h
```

### Traffic pattern analysis

See how request volume changes hour by hour:

```bash
lynxdb query '_source=nginx
  | timechart count span=1h' --since 7d
```

### Correlate errors with slow queries

```bash
lynxdb query 'level=error OR (source=postgres AND duration_ms>1000)
  | eval event_type = if(source="postgres", "slow_query", "error")
  | timechart count span=5m by event_type' --since 6h
```

### Compute moving averages with STREAMSTATS

Smooth out spiky time series with a running average:

```bash
lynxdb query '_source=nginx
  | timechart avg(duration_ms) AS avg_lat span=5m
  | streamstats avg(avg_lat) AS moving_avg window=6
  | table _timestamp, avg_lat, moving_avg'
```

The `window=6` parameter computes the average over the previous 6 rows (30 minutes at 5-minute intervals).

---

## Time series on local files

Time analysis works in pipe mode and file mode:

```bash
# Analyze a local access log
lynxdb query --file access.log '| timechart count span=1h'

# Analyze kubectl output over time
kubectl logs deploy/api --since=6h | lynxdb query '
  | bin _timestamp span=5m
  | stats count, avg(duration_ms) by _timestamp'
```

---

## Output format for time series

Time series data often needs to be consumed by other tools. Use `--format csv` for spreadsheet compatibility or pipe to `jq` for JSON processing:

```bash
# CSV for spreadsheets or graphing tools
lynxdb query 'level=error | timechart count span=1h' --since 7d --format csv > errors_by_hour.csv

# JSON for programmatic use
lynxdb query 'level=error | timechart count span=1h' --since 7d --format json
```

---

## Next steps

- [Run aggregations](/docs/guides/aggregations) -- general aggregation patterns beyond time series
- [Materialized views](/docs/guides/materialized-views) -- precompute time-bucketed aggregations
- [TIMECHART command reference](/docs/spl2/commands/timechart) -- full TIMECHART syntax
- [BIN command reference](/docs/spl2/commands/bin) -- full BIN syntax
- [Time ranges reference](/docs/spl2/time-ranges) -- all supported time range formats
