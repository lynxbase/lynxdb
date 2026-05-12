---
title: appendcols
description: Append subsearch fields to current rows by row position.
---

# appendcols

Append subsearch fields to the current result rows by row position.

## Syntax

```spl
| appendcols [override=<bool>] [maxout=<n>] [maxtime=<n>] [timeout=<n>] [<subsearch>]
```

## Examples

```spl
-- Add a single threshold column to the first result row
| appendcols [makeresults | eval threshold=30]

-- Let subsearch fields replace colliding main fields
| appendcols override=true [makeresults | eval host="synthetic"]
```

## Notes

- The first subsearch row is merged with the first current row, the second with the second, and so on.
- Internal fields from the subsearch, such as `_time`, are not appended.
- `override=false` is the default; main row values win field-name conflicts.
- `maxout` limits the number of subsearch rows appended.
- `maxtime` and `timeout` parse for compatibility but are not enforced yet.

## See Also

- [append](/docs/lynx-flow/commands/append) -- Append subsearch rows
- [appendpipe](/docs/lynx-flow/commands/appendpipe) -- Append a subpipe result
