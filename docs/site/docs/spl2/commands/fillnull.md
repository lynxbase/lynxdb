---
title: fillnull
description: Replace null values with a specified value.
---

# fillnull

Replace null (missing) field values with a specified default.

## Syntax

```spl
| fillnull [value=<value>] [<field> ...]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `value` | `0` | Replacement value |
| `field` | All fields | Specific fields to fill |

## Examples

```spl
-- Fill all nulls with 0
| fillnull

-- Fill nulls with a specific value
| fillnull value="N/A"

-- Fill specific fields only
| fillnull value=0 duration_ms, bytes_sent

-- After aggregation
| stats count by source, level | fillnull value=0 count
```

## See Also

- [eval coalesce()](/docs/spl2/commands/eval) -- Pick first non-null value
- [where IS NOT NULL](/docs/spl2/commands/where) -- Filter out nulls
