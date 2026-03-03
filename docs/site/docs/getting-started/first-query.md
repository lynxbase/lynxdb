---
sidebar_position: 5
title: Your First SPL2 Query
description: Learn SPL2 basics -- search, filter, aggregate, and visualize log data.
---

# Your First SPL2 Query

SPL2 is LynxDB's query language. It's a pipeline language inspired by Splunk's SPL -- data flows left to right through pipe (`|`) operators.

## The Pipeline Concept

Every SPL2 query is a pipeline of commands:

```
[search terms] | command1 args | command2 args | command3 args
```

Data starts on the left (a search or data source) and flows through each command. Each command transforms the data and passes it to the next.

## Step 1: Search

The simplest query is a keyword search:

```spl
error
```

This finds all events containing the word "error". You can also search specific fields:

```spl
level=error
```

Combine terms with boolean operators:

```spl
level=error source=nginx
level=error OR level=warn
level=error NOT source=redis
```

:::tip
If your query starts with `|`, LynxDB automatically prepends `FROM main`. So `| stats count` is equivalent to `FROM main | stats count`.
:::

## Step 2: Filter with WHERE

Use `WHERE` for precise filtering:

```spl
source=nginx | where status >= 500
source=nginx | where status >= 500 AND duration_ms > 1000
source=nginx | where uri LIKE "%/api/%"
```

## Step 3: Aggregate with STATS

`STATS` computes aggregations:

```spl
# Count events
| stats count

# Count by field
level=error | stats count by source

# Multiple aggregations
source=nginx | stats count, avg(duration_ms), p99(duration_ms) by uri

# With renaming
source=nginx | stats count as requests, avg(duration_ms) as avg_latency by uri
```

## Step 4: Sort and Limit

```spl
# Sort descending (prefix with -)
source=nginx | stats count by uri | sort -count

# Take top N
source=nginx | stats count by uri | sort -count | head 10

# Or use the TOP shortcut
source=nginx | top 10 uri
```

## Step 5: Select Columns

```spl
# Pick specific fields
level=error | table _time, source, message

# Remove fields
level=error | fields - _raw
```

## Step 6: Transform with EVAL

Create computed fields:

```spl
source=nginx
  | stats count as total, count(eval(status>=500)) as errors by uri
  | eval error_rate = round(errors/total*100, 1)
  | where error_rate > 5
  | sort -error_rate
  | table uri, total, errors, error_rate
```

## Step 7: Time Series with TIMECHART

Aggregate over time buckets:

```spl
# Error count per 5-minute bucket
level=error | timechart count span=5m

# Error count by source per 5 minutes
level=error | timechart count span=5m by source
```

## Step 8: Extract Fields with REX

Extract new fields from raw text using regex:

```spl
search "connection refused"
  | rex field=_raw "host=(?P<host>\S+) port=(?P<port>\d+)"
  | stats count by host, port
  | sort -count
```

## Putting It All Together

Here's a real-world query that finds the slowest API endpoints with high error rates:

```spl
source=nginx
  | stats count as total,
          count(eval(status>=500)) as errors,
          avg(duration_ms) as avg_latency,
          p99(duration_ms) as p99_latency
    by uri
  | eval error_rate = round(errors/total*100, 1)
  | where error_rate > 5 OR p99_latency > 1000
  | sort -error_rate
  | table uri, total, errors, error_rate, avg_latency, p99_latency
```

## Command Quick Reference

| Command | What it does | Example |
|---------|-------------|---------|
| `search` | Full-text search | `search "connection refused"` |
| `where` | Filter rows | `\| where status >= 500` |
| `stats` | Aggregate | `\| stats count, avg(x) by y` |
| `eval` | Compute fields | `\| eval rate = errors/total*100` |
| `sort` | Order results | `\| sort -count` |
| `head` | Limit results | `\| head 10` |
| `table` | Select columns | `\| table uri, count` |
| `fields` | Add/remove fields | `\| fields - _raw` |
| `rex` | Extract via regex | `\| rex "host=(?P<host>\S+)"` |
| `timechart` | Time series | `\| timechart count span=5m` |
| `top` | Top N values | `\| top 10 uri` |
| `dedup` | Remove duplicates | `\| dedup host` |

## Next Steps

- **[SPL2 Overview](/docs/spl2/overview)** -- Full language reference
- **[Searching & Filtering](/docs/guides/search-and-filter)** -- Advanced search techniques
- **[Aggregations](/docs/guides/aggregations)** -- All aggregation functions
- **[SPL2 Commands](/docs/spl2/commands/stats)** -- Detailed command reference
