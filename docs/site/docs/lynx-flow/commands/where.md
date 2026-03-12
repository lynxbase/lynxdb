---
title: where
description: Filter events by a boolean expression.
---

# where

Filter events using a boolean expression. Only events where the expression evaluates to `true` pass through.

## Syntax

```spl
| where <expression>
```

## Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `=`, `!=` | Equality | `status = 200` |
| `>`, `>=`, `<`, `<=` | Comparison | `duration_ms > 1000` |
| `AND`, `OR`, `NOT` | Boolean | `status >= 500 AND source = "nginx"` |
| `IN (...)` | Set membership | `level IN ("error", "warn")` |
| `LIKE` | Pattern match | `uri LIKE "%/api/%"` |
| `IS NULL`, `IS NOT NULL` | Null check | `error_msg IS NOT NULL` |

## Examples

```spl
-- Simple comparison
| where status >= 500

-- Multiple conditions
| where status >= 500 AND duration_ms > 1000

-- OR conditions
| where level = "error" OR level = "warn"

-- IN operator
| where level IN ("error", "warn", "fatal")

-- Pattern matching
| where uri LIKE "%/api/v2/%"

-- Null checks
| where error_message IS NOT NULL

-- Computed expressions
| where duration_ms / 1000 > 5

-- Using eval functions
| where match(uri, "^/api/v[0-9]+/users")
| where len(message) > 500
```

## Notes

- `where` evaluates expressions using the bytecode VM (22ns/op for simple predicates).
- The optimizer pushes `where` predicates down to the scan level when possible, enabling bloom filter and time range pruning.
- Use `where` for programmatic filtering; use `search` for full-text keyword search.

## See Also

- [search](/docs/lynx-flow/commands/search) -- Full-text search
- [eval](/docs/lynx-flow/commands/eval) -- Compute new fields
- [Eval Functions](/docs/lynx-flow/functions/eval-functions) -- Functions usable in where expressions
