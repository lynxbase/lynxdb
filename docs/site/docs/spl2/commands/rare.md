---
title: rare
description: Find the least common values of a field.
---

# rare

Find the least common values of a field. The inverse of [`top`](/docs/spl2/commands/top).

## Syntax

```spl
| rare [N] <field> [BY <split-field>]
```

Default: `N = 10`.

## Examples

```spl
-- Least common status codes
source=nginx | rare status

-- Rarest 5 error messages
level=error | rare 5 message

-- Rare values per source
| rare 10 uri by source
```

## See Also

- [top](/docs/spl2/commands/top) -- Most common values
- [stats](/docs/spl2/commands/stats) -- General aggregation
