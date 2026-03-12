---
title: multisearch
description: Run multiple searches and union their results.
---

# multisearch

Run multiple independent searches and combine (union) their results.

## Syntax

```spl
| multisearch [<search1>] [<search2>] [<search3>] ...
```

## Examples

```spl
-- Union results from multiple sources
| multisearch
    [search source=nginx | stats count by uri]
    [search source=api-gw | stats count by uri]
    [search source=redis | stats count by command]
```

## Notes

- Each subsearch runs independently and results are concatenated.
- For joining on common fields, use [`join`](/docs/lynx-flow/commands/join) instead.

## See Also

- [append](/docs/lynx-flow/commands/append) -- Append a single subsearch
- [join](/docs/lynx-flow/commands/join) -- Join on common fields
