---
title: streamstats
description: Compute running aggregations over the event stream.
---

# streamstats

Compute running (cumulative) aggregations while preserving the original event stream. Each event gets new fields with the running aggregate up to that point.

## Syntax

```spl
| streamstats <agg-function> [AS <alias>] [BY <field>]
```

## Examples

```spl
-- Running count
| streamstats count AS row_num

-- Running average of latency
| streamstats avg(duration_ms) AS running_avg_latency

-- Running sum by group
| streamstats sum(bytes) AS cumulative_bytes by source

-- Running count of errors
level=error | streamstats count AS error_count by host
```

## Notes

- Unlike `stats`, `streamstats` preserves the original event stream and adds aggregate fields.
- `streamstats` processes events in the order they appear in the pipeline.
- This is a **streaming** operator -- it runs on the coordinator, not pushed to shards.

## See Also

- [stats](/docs/lynx-flow/commands/stats) -- Aggregation (replaces events)
- [eventstats](/docs/lynx-flow/commands/eventstats) -- Global stats added to each event
