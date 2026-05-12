---
title: makeresults
description: Generate temporary result rows.
---

# makeresults

Generate temporary rows for examples, tests, and ad hoc query construction. Each generated row includes `_time`.

## Syntax

```spl
| makeresults
| makeresults count=<N>
| makeresults count=<N> annotate=<bool>
| makeresults <N>
| makeresults format=<csv|json> data="<inline-data>"
```

Omitting `count` creates one row. `count=0` creates no rows. `annotate=true` adds Splunk-compatible metadata fields. `format` and `data` parse for compatibility but execution is deferred.

## Examples

```spl
-- Generate one row
| makeresults

-- Generate three rows
| makeresults count=3

-- SPL2 positional count spelling
| makeresults 3

-- Add Splunk-compatible metadata fields
| makeresults count=2 annotate=true
```

## Notes

- LynxDB supports row generation, `_time`, and `annotate=true` metadata fields.
- Splunk `format` and `data` inline datasets parse but are not implemented yet.

## See Also

- [eval](/docs/lynx-flow/commands/eval) -- Add fields to generated rows
- [stats](/docs/lynx-flow/commands/stats) -- Aggregate generated rows
