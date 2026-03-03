---
title: sort
description: Order results by one or more fields.
---

# sort

Order results by one or more fields, ascending or descending.

## Syntax

```spl
| sort [+|-]<field> [, [+|-]<field> ...]
```

Prefix with `-` for descending, `+` for ascending (default).

## Examples

```spl
-- Sort by count descending
| stats count by source | sort -count

-- Sort ascending (default)
| sort duration_ms

-- Multiple sort keys
| sort -count, +source

-- Sort with limit (optimized: TopK pushdown)
| sort -count | head 10
```

## Notes

- `| sort -field | head N` is automatically optimized into a TopK operation.
- Sort is a blocking operator -- it must consume all input before producing output.

## See Also

- [head](/docs/spl2/commands/head) -- Limit results
- [top](/docs/spl2/commands/top) -- Shortcut for sort + head
