---
title: Run Aggregations
description: How to aggregate log data in LynxDB using stats, count, avg, percentiles, conditional counting, group by, and multi-level aggregations.
---

# Run Aggregations

The [`STATS`](/docs/spl2/commands/stats) command is the core of log analytics in LynxDB. It computes aggregation functions over events, optionally grouped by one or more fields. This guide covers all the aggregation patterns you need for day-to-day log analysis.

## Basic counting

### Count all events

```bash
lynxdb query '| stats count'
```

### Count with a filter

```bash
lynxdb query 'level=error | stats count'
```

### Quick count shortcut

The [`lynxdb count`](/docs/cli/shortcuts) command is a faster way to get a simple count:

```bash
lynxdb count 'level=error' --since 1h
```

---

## Group by a field

Add `by <field>` to break results down by category:

```bash
lynxdb query 'level=error | stats count by source'
```

### Group by multiple fields

```bash
lynxdb query '_source=nginx | stats count by status, uri'
```

### Name your aggregation

Use `AS` to give the result column a meaningful name:

```bash
lynxdb query 'level=error | stats count AS error_count by source'
```

---

## Aggregation functions

LynxDB supports 15+ aggregation functions. Here are the most common ones.

### Count, sum, avg

```bash
lynxdb query '_source=nginx | stats count, sum(bytes), avg(duration_ms) by uri'
```

### Min and max

```bash
lynxdb query '_source=nginx | stats min(duration_ms) AS fastest, max(duration_ms) AS slowest by uri'
```

### Distinct count

Count unique values with `dc()`:

```bash
lynxdb query '_source=nginx | stats dc(client_ip) AS unique_visitors by uri'
```

### Percentiles

Compute latency percentiles:

```bash
lynxdb query '_source=nginx | stats avg(duration_ms) AS avg_lat, perc50(duration_ms) AS p50, perc95(duration_ms) AS p95, perc99(duration_ms) AS p99 by uri'
```

Available percentile functions: `perc50`, `perc75`, `perc90`, `perc95`, `perc99`.

### Standard deviation

```bash
lynxdb query '_source=nginx | stats avg(duration_ms) AS mean, stdev(duration_ms) AS stddev by uri'
```

### Collect values

The `values()` function collects all distinct values of a field into a multivalue result:

```bash
lynxdb query 'level=error | stats count, values(source) AS sources by host'
```

### Earliest and latest

Get the first and last value seen (by time):

```bash
lynxdb query '| stats earliest(message) AS first_msg, latest(message) AS last_msg by source'
```

See the [aggregation functions reference](/docs/spl2/functions/aggregation-functions) for the complete list.

---

## Conditional counting

Count events that match a condition using `count(eval(...))`:

```bash
lynxdb query '_source=nginx | stats count AS total, count(eval(status>=500)) AS errors by uri'
```

### Compute error rates

Combine conditional counting with [`EVAL`](/docs/spl2/commands/eval) to calculate ratios:

```bash
lynxdb query '_source=nginx
  | stats count AS total, count(eval(status>=500)) AS errors by uri
  | eval error_rate = round(errors / total * 100, 1)
  | where error_rate > 5
  | sort -error_rate
  | table uri, total, errors, error_rate'
```

---

## Sorting results

Pipe aggregation results into [`SORT`](/docs/spl2/commands/sort) to order them:

```bash
# Sort descending by count (prefix with -)
lynxdb query 'level=error | stats count by source | sort -count'

# Sort ascending
lynxdb query 'level=error | stats count by source | sort count'

# Sort by multiple fields
lynxdb query '_source=nginx | stats count by status, uri | sort status, -count'
```

---

## Top and rare

The [`TOP`](/docs/spl2/commands/top) and [`RARE`](/docs/spl2/commands/rare) commands are shortcuts for the most and least common values:

```bash
# Top 10 URIs by request count
lynxdb query '_source=nginx | top 10 uri'

# Rarest error messages
lynxdb query 'level=error | rare 10 message'
```

These are equivalent to `stats count by <field> | sort -count | head N` but more concise.

---

## Multi-level aggregation

You can chain multiple `STATS` commands in a pipeline. Each one aggregates the output of the previous step:

```bash
# First: count errors per host per source
# Then: find hosts with more than 100 total errors
lynxdb query 'level=error
  | stats count by host, source
  | stats sum(count) AS total_errors by host
  | where total_errors > 100
  | sort -total_errors'
```

---

## Streaming aggregations

### STREAMSTATS -- running aggregations

[`STREAMSTATS`](/docs/spl2/commands/streamstats) computes running (cumulative) aggregations without collapsing events:

```bash
lynxdb query '_source=nginx
  | sort _timestamp
  | streamstats count AS request_num, avg(duration_ms) AS running_avg_latency'
```

### EVENTSTATS -- enrich events with aggregates

[`EVENTSTATS`](/docs/spl2/commands/eventstats) adds aggregation values to each event without collapsing:

```bash
lynxdb query '_source=nginx
  | eventstats avg(duration_ms) AS global_avg by uri
  | where duration_ms > global_avg * 3
  | table _timestamp, uri, duration_ms, global_avg'
```

This is useful for finding outliers: events where the latency is more than 3x the average.

---

## Aggregations on local files

All aggregation commands work in pipe mode and file mode:

```bash
# Aggregate a local file
lynxdb query --file access.log '| stats count by status'

# Aggregate piped input
kubectl logs deploy/api | lynxdb query '| stats avg(duration_ms), p99(duration_ms) by endpoint'

# Combine with Unix tools
lynxdb query --file access.log '| stats count by status' --format csv | sort -t, -k2 -rn
```

---

## Performance tips

- **Time range first**: Always add `--since` to narrow the scan window. `lynxdb query 'level=error | stats count' --since 1h` is much faster than scanning all data.
- **Filter before aggregating**: Place `WHERE` before `STATS` to reduce the number of events processed.
- **Use materialized views**: For queries you run repeatedly, create a [materialized view](/docs/guides/materialized-views) to precompute the aggregation and get results up to 400x faster.
- **Partial aggregation**: LynxDB automatically uses two-phase partial aggregation (per-segment, then global merge), so `STATS` scales linearly with data size.

---

## Next steps

- [Time series analysis](/docs/guides/time-series) -- aggregate over time windows with TIMECHART and BIN
- [Materialized views](/docs/guides/materialized-views) -- precompute aggregations for repeated queries
- [STATS command reference](/docs/spl2/commands/stats) -- full syntax and all options
- [Aggregation functions reference](/docs/spl2/functions/aggregation-functions) -- complete list of aggregation functions
