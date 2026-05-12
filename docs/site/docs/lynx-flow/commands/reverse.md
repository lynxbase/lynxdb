---
title: reverse
description: Reverse the current result order.
---

# reverse

Reverse the current row order without sorting by a field.

## Syntax

```spl
| reverse
```

## Examples

```spl
-- Reverse whatever order the upstream pipeline produced
| reverse

-- Oldest rows after a newest-first sort
| sort -_time | reverse

-- Reverse the top results after limiting
| stats count by uri | sort -count | head 10 | reverse
```

## Notes

- `reverse` does not choose an ordering key. Use `sort` first when rows need a defined order.
- `reverse` must consume all input before producing output, so it is a blocking operator.

## See Also

- [sort](/docs/lynx-flow/commands/sort) -- Order rows by field
- [head](/docs/lynx-flow/commands/head) -- First N results
- [tail](/docs/lynx-flow/commands/tail) -- Last N results
