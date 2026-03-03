---
title: head
description: Return the first N results.
---

# head

Return only the first N results from the pipeline.

## Syntax

```spl
| head [N]
```

Default: `N = 10`.

## Examples

```spl
-- First 10 results (default)
| head

-- First 5 results
level=error | head 5

-- Combined with sort for top-N
| stats count by uri | sort -count | head 10
```

## Notes

- `head` is a streaming operator -- it stops reading as soon as N results are emitted. On 100M events, `head 10` reads only one batch (1024 rows), not the entire dataset.
- `| sort -field | head N` is automatically optimized into a TopK heap operation by the query planner.

## See Also

- [tail](/docs/spl2/commands/tail) -- Last N results
- [top](/docs/spl2/commands/top) -- Most common values
- [sort](/docs/spl2/commands/sort) -- Order results
