---
title: bin
description: Bucket continuous values into discrete bins.
---

# bin

Bucket numeric or timestamp values into discrete bins for aggregation.

## Syntax

```spl
| bin <field> [span=<value>] [AS <alias>]
```

## Arguments

| Argument | Description |
|----------|-------------|
| `field` | Field to bucket |
| `span` | Bucket size (e.g., `5m`, `1h`, `1d` for time; `10`, `100` for numbers) |
| `AS` | Optional alias for the bucketed field |

## Examples

```spl
-- Bucket timestamps into 5-minute intervals
| bin _time span=5m

-- Bucket and aggregate
| bin _time span=1h | stats count by _time

-- Bucket numeric values
| bin duration_ms span=100 | stats count by duration_ms

-- With alias
| bin _time span=5m AS time_bucket | stats count by time_bucket
```

## Notes

- `bin` is often used as a building block for time-series aggregations. For a simpler syntax, use [`timechart`](/docs/spl2/commands/timechart).

## See Also

- [timechart](/docs/spl2/commands/timechart) -- Time-series aggregation (combines bin + stats)
- [Time Ranges](/docs/spl2/time-ranges) -- Time range syntax
