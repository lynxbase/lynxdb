---
title: eventstats
description: Add aggregation results to each event without collapsing.
---

# eventstats

Compute aggregations and add the results as new fields to each event, without replacing the event stream. Every event gets the aggregate value for its group.

## Syntax

```spl
| eventstats <agg-function> [AS <alias>] [BY <field>]
```

## Examples

```spl
-- Add total count to each event
| eventstats count AS total_events

-- Add group average to each event
| eventstats avg(duration_ms) AS avg_duration_for_source by source

-- Filter using the added stats
| eventstats avg(duration_ms) AS avg_dur by endpoint
| where duration_ms > avg_dur * 2
| table _time, endpoint, duration_ms, avg_dur
```

## Notes

- `eventstats` is a two-pass operator: it first computes the aggregation, then enriches each event.
- Unlike `stats`, original events are preserved with the aggregate values added.

## See Also

- [stats](/docs/lynx-flow/commands/stats) -- Aggregation (replaces events)
- [streamstats](/docs/lynx-flow/commands/streamstats) -- Running aggregation
