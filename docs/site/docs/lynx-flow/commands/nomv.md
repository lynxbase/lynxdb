---
title: nomv
description: Convert a multivalue field into one newline-delimited value.
---

# nomv

Convert the values of a multivalue field into one single value separated by newline characters.

## Syntax

```spl
| nomv <field>
```

## Examples

```spl
-- Combine sender values before ranking them
eventtype="sendmail"
| nomv senders
| top senders

-- Convert an array-like field to a display string
| nomv tags
```

## Notes

- LynxDB converts JSON array strings and internal multivalue strings into newline-delimited strings.
- Scalar, missing, and null values pass through unchanged.

## See Also

- [mvexpand](/docs/lynx-flow/commands/mvexpand) -- Expand multivalue fields into rows
- [eval](/docs/lynx-flow/commands/eval) -- Use `mvjoin()` when a custom delimiter is needed
