---
title: top
description: Find the most common values of a field.
---

# top

Find the most common values of a field. Equivalent to `| stats count by field | sort -count | head N`.

## Syntax

```spl
| top [N] <field> [BY <split-field>]
```

Default: `N = 10`.

## Examples

```spl
-- Top 10 URIs (default)
source=nginx | top uri

-- Top 5 error sources
level=error | top 5 source

-- Top status codes per source
| top 10 status by source
```

## Notes

- The optimizer applies TopK pushdown for `top`, computing results efficiently without full sort.

## See Also

- [rare](/docs/lynx-flow/commands/rare) -- Least common values
- [stats](/docs/lynx-flow/commands/stats) -- General aggregation
- [sort](/docs/lynx-flow/commands/sort) -- Custom ordering
