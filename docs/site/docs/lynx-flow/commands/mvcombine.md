---
title: mvcombine
description: Combine matching rows into one row with a multivalue field.
---

# mvcombine

Combine rows that are identical except for one field. The selected field becomes a multivalue field containing the values from the combined rows.

## Syntax

```spl
| mvcombine <field>
| mvcombine delim=<string> <field>
```

The default delimiter is a single space in Splunk's alternate single-value representation.

## Examples

```spl
-- Combine matching host rows
| stats max(bytes) as max min(bytes) as min by host
| mvcombine host

-- Display the combined multivalue field as one newline-delimited string
| stats max(bytes) as max min(bytes) as min by host
| mvcombine host
| nomv host
```

## Notes

- `mvcombine` is blocking because it must compare rows across the input.
- LynxDB groups rows by every field except the selected field.
- `delim` is parsed for compatibility, but delimiter-specific alternate display strings are not represented separately yet.

## See Also

- [makemv](/docs/lynx-flow/commands/makemv) -- Split one value into multivalue values
- [nomv](/docs/lynx-flow/commands/nomv) -- Convert multivalue values to one newline-delimited value
- [mvexpand](/docs/lynx-flow/commands/mvexpand) -- Expand multivalue values into separate rows
