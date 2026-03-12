---
title: timechart
description: Create time-series aggregations.
---

# timechart

Create time-series aggregations by bucketing events into time intervals.

## Syntax

```spl
| timechart <agg-function> [AS <alias>] [BY <split-field>] span=<interval>
```

## Arguments

| Argument | Description |
|----------|-------------|
| `agg-function` | Aggregation function (count, sum, avg, etc.) |
| `BY` | Optional field to split series by |
| `span` | Time bucket size (e.g., `1m`, `5m`, `1h`, `1d`) |

## Examples

```spl
-- Error count per 5-minute bucket
level=error | timechart count span=5m

-- Split by source
level=error | timechart count span=5m by source

-- Average latency over time
source=nginx | timechart avg(duration_ms) span=1h

-- Multiple aggregations
source=nginx | timechart count, avg(duration_ms) span=5m

-- With alias
level=error | timechart count AS error_count span=5m by source
```

## Output

`timechart` produces one row per time bucket, with a `_time` column and one column per series:

| _time | nginx | api-gw | redis |
|-------|-------|--------|-------|
| 2026-01-15T10:00:00Z | 42 | 18 | 5 |
| 2026-01-15T10:05:00Z | 38 | 22 | 3 |

## Notes

- `timechart` is equivalent to `| bin _time span=X | stats <agg> by _time [, split_field]` with automatic pivoting.
- The API returns `data.type: "timechart"` so clients can render charts.
- Time ranges are controlled by `--since`/`--from`/`--to` on the CLI or `from`/`to` in the API.

## See Also

- [bin](/docs/lynx-flow/commands/bin) -- Manual bucketing
- [stats](/docs/lynx-flow/commands/stats) -- General aggregation
- [Time Series Guide](/docs/guides/time-series) -- Time series analysis patterns
