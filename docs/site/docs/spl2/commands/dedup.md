---
title: dedup
description: Remove events with duplicate field values.
---

# dedup

Remove events with duplicate values for the specified fields. Keeps the first occurrence.

## Syntax

```spl
| dedup [N] <field> [, <field> ...]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `N` | 1 | Keep first N events per unique combination |
| `field` | Required | One or more fields to deduplicate on |

## Examples

```spl
-- Keep one event per host
| dedup host

-- Keep first 3 events per source
| dedup 3 source

-- Dedup on multiple fields
| dedup source, level

-- Dedup after filtering
level=error | dedup host | table _time, host, message
```

## See Also

- [stats dc()](/docs/spl2/commands/stats) -- Count distinct values
- [top](/docs/spl2/commands/top) -- Most common values
