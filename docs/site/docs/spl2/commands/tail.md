---
title: tail
description: Return the last N results.
---

# tail

Return only the last N results from the pipeline.

## Syntax

```spl
| tail [N]
```

Default: `N = 10`.

## Examples

```spl
-- Last 10 results
| tail

-- Last 20 error events
level=error | tail 20

-- Last events by time
| sort _time | tail 5
```

## Notes

- Unlike `head`, `tail` must consume all input before producing output (blocking operator).

## See Also

- [head](/docs/spl2/commands/head) -- First N results
- [sort](/docs/spl2/commands/sort) -- Order results
