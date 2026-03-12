---
title: xyseries
description: Convert tabular data into a series format for charting.
---

# xyseries

Convert tabular data into a series (pivot) format, suitable for charting.

## Syntax

```spl
| xyseries <x-field> <y-field> <data-field>
```

## Examples

```spl
-- Pivot status counts by source
| stats count by source, status | xyseries source status count
```

## Notes

- `xyseries` pivots rows into columns. The x-field becomes the row key, the y-field values become column headers, and the data-field provides the cell values.

## See Also

- [timechart](/docs/lynx-flow/commands/timechart) -- Time-series pivoting
- [stats](/docs/lynx-flow/commands/stats) -- Aggregation
