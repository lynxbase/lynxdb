---
title: append
description: Append results from a subsearch to the current results.
---

# append

Append the results of a subsearch to the current result set (union).

## Syntax

```spl
| append [<subsearch>]
```

## Examples

```spl
-- Combine results from two searches
source=nginx | stats count by uri
  | append [search source=api-gw | stats count by uri]

-- Append with different fields
level=error | stats count AS error_count by source
  | append [search level=warn | stats count AS warn_count by source]
```

## See Also

- [multisearch](/docs/spl2/commands/multisearch) -- Union multiple searches
- [join](/docs/spl2/commands/join) -- Join on common fields
