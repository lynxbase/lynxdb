---
title: transaction
description: Group related events into transactions.
---

# transaction

Group related events into transactions based on common field values and time proximity.

## Syntax

```spl
| transaction <field> [maxspan=<time>] [maxpause=<time>]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | Required | Field to group events by |
| `maxspan` | none | Maximum duration of a transaction |
| `maxpause` | none | Maximum gap between events in a transaction |

## Examples

```spl
-- Group events by session ID
| transaction session_id

-- With time constraints
| transaction request_id maxspan=5m

-- With max pause between events
| transaction user_id maxpause=30s maxspan=1h
```

## Output

Each transaction becomes a single event with:
- `duration` -- total transaction duration
- `eventcount` -- number of events in the transaction
- All fields from the grouped events

## See Also

- [stats](/docs/spl2/commands/stats) -- Aggregate without grouping into transactions
- [dedup](/docs/spl2/commands/dedup) -- Remove duplicate events
