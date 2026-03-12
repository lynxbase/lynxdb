---
title: fields
description: Include or exclude fields from the output.
---

# fields

Include or exclude fields from the event stream.

## Syntax

```spl
| fields [+|-] <field> [, <field> ...]
```

- `+` (default): Include only these fields
- `-`: Remove these fields

## Examples

```spl
-- Include specific fields
| fields source, level, message

-- Remove fields
| fields - _raw, _id

-- Keep only what you need
level=error | fields + _time, source, message
```

## Notes

- `fields` without a prefix defaults to include mode (same as `fields +`).
- `fields +` is equivalent to [`table`](/docs/lynx-flow/commands/table).
- The optimizer uses field lists for column pruning, reducing I/O.

## See Also

- [table](/docs/lynx-flow/commands/table) -- Select and order columns
- [rename](/docs/lynx-flow/commands/rename) -- Rename fields
