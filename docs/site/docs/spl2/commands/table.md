---
title: table
description: Select and order output columns.
---

# table

Select specific fields to display in the output, in the specified order.

## Syntax

```spl
| table <field> [, <field> ...]
```

## Examples

```spl
-- Select specific columns
level=error | table _time, source, message

-- After aggregation
| stats count, avg(duration_ms) by uri | sort -count | table uri, count, avg(duration_ms)

-- Rename inline with stats, then table
| stats count as requests, p99(duration_ms) as p99_lat by uri | table uri, requests, p99_lat
```

## Notes

- `table` is equivalent to `fields` with only include mode.
- Column order in the output matches the order specified in the command.
- The optimizer uses `table` for column pruning -- only referenced columns are read from storage.

## See Also

- [fields](/docs/spl2/commands/fields) -- Include or exclude fields
- [rename](/docs/spl2/commands/rename) -- Rename fields
