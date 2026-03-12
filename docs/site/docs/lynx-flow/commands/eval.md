---
title: eval
description: Create computed fields using expressions and functions.
---

# eval

Create new fields or overwrite existing ones using expressions and functions.

## Syntax

```spl
| eval <field>=<expression> [, <field>=<expression> ...]
```

## Examples

```spl
-- Simple arithmetic
| eval duration_sec = duration_ms / 1000

-- String operations
| eval service_upper = upper(source)

-- Conditional logic
| eval severity = IF(status >= 500, "critical", IF(status >= 400, "warning", "ok"))

-- Multi-way conditional
| eval tier = CASE(
    duration_ms < 100, "fast",
    duration_ms < 1000, "normal",
    duration_ms < 5000, "slow",
    1=1, "very_slow"
  )

-- Computed metrics
| eval error_rate = round(errors / total * 100, 1)

-- Multiple fields in one eval
| eval
    duration_sec = duration_ms / 1000,
    is_error = IF(status >= 500, "yes", "no"),
    source_upper = upper(source)

-- Using coalesce for defaults
| eval display_name = coalesce(username, email, "anonymous")

-- String concatenation
| eval full_msg = source . ": " . message

-- Timestamp formatting
| eval formatted_time = strftime(_time, "%Y-%m-%d %H:%M:%S")
```

## Available Functions

See [Eval Functions](/docs/lynx-flow/functions/eval-functions) for the complete reference.

Common functions:

| Function | Description |
|----------|-------------|
| `IF(cond, true_val, false_val)` | Conditional |
| `CASE(c1, v1, c2, v2, ...)` | Multi-way conditional |
| `coalesce(a, b, ...)` | First non-null |
| `round(n, decimals)` | Round number |
| `tonumber(s)` | String to number |
| `tostring(n)` | Number to string |
| `lower(s)` / `upper(s)` | Case conversion |
| `substr(s, start, len)` | Substring |
| `len(s)` | String length |
| `match(s, regex)` | Regex match (boolean) |

## Notes

- `eval` creates a new field or overwrites an existing one.
- Multiple assignments can be comma-separated in a single `eval` command.
- Expressions are evaluated using the zero-allocation bytecode VM.
- String concatenation uses the `.` operator.

## See Also

- [where](/docs/lynx-flow/commands/where) -- Filter using expressions
- [stats](/docs/lynx-flow/commands/stats) -- Aggregate with expressions
- [Eval Functions](/docs/lynx-flow/functions/eval-functions) -- Complete function reference
