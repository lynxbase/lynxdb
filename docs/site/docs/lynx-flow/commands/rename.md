---
title: rename
description: Rename fields in the output.
---

# rename

Rename one or more fields.

## Syntax

```spl
| rename <old-name> AS <new-name> [, <old-name> AS <new-name> ...]
```

## Examples

```spl
-- Rename a single field
| stats count by source | rename count AS total_events

-- Rename multiple fields
| rename duration_ms AS latency, source AS service_name

-- Rename after aggregation
| stats avg(duration_ms) AS avg_duration | rename avg_duration AS "Average Latency (ms)"
```

## See Also

- [fields](/docs/lynx-flow/commands/fields) -- Include or exclude fields
- [eval](/docs/lynx-flow/commands/eval) -- Create computed fields
