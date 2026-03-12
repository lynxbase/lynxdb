---
title: Eval Functions
description: Complete reference for eval functions -- conditionals, string, math, and multivalue.
---

# Eval Functions

Eval functions are used in `let`, [`eval`](/docs/lynx-flow/commands/eval), [`where`](/docs/lynx-flow/commands/where), and any expression context.

## Conditional Functions

### IF

```spl
| eval severity = IF(status >= 500, "critical", "ok")
| eval label = IF(duration_ms > 1000, "slow", IF(duration_ms > 100, "normal", "fast"))
```

### CASE

Multi-way conditional (like a switch statement):

```spl
| eval tier = CASE(
    duration_ms < 100, "fast",
    duration_ms < 1000, "normal",
    duration_ms < 5000, "slow",
    1=1, "very_slow"
  )
```

The last `1=1` acts as a default/else clause.

### coalesce

Returns the first non-null argument:

```spl
| eval name = coalesce(display_name, username, email, "anonymous")
```

### isnotnull / isnull

Check for null values:

```spl
| where isnotnull(error_message)
| eval has_error = IF(isnotnull(error_code), "yes", "no")
```

## String Functions

### lower / upper

```spl
| eval level_upper = upper(level)
| eval host_lower = lower(host)
```

### substr

```spl
| eval prefix = substr(uri, 1, 4)     -- first 4 characters
| eval domain = substr(host, 5)        -- from position 5 to end
```

### len

```spl
| eval msg_length = len(message)
| where len(uri) > 100
```

### match

Regex match (returns boolean):

```spl
| where match(uri, "^/api/v[0-9]+/users")
| eval is_api = IF(match(uri, "^/api/"), "yes", "no")
```

## Type Conversion Functions

### tonumber

```spl
| eval status_num = tonumber(status_str)
```

### tostring

```spl
| eval status_str = tostring(status)
```

## Math Functions

### round

```spl
| eval rate = round(errors/total*100, 1)    -- 1 decimal place
| eval whole = round(value)                  -- nearest integer
```

### ln

Natural logarithm:

```spl
| eval log_duration = ln(duration_ms)
```

## Timestamp Functions

### strftime

Format a timestamp:

```spl
| eval formatted = strftime(_time, "%Y-%m-%d %H:%M:%S")
| eval hour = strftime(_time, "%H")
| eval day = strftime(_time, "%A")
```

Common format specifiers:

| Specifier | Description | Example |
|-----------|-------------|---------|
| `%Y` | 4-digit year | `2026` |
| `%m` | Month (01-12) | `03` |
| `%d` | Day (01-31) | `15` |
| `%H` | Hour (00-23) | `14` |
| `%M` | Minute (00-59) | `30` |
| `%S` | Second (00-59) | `45` |
| `%A` | Weekday name | `Monday` |

## Multivalue Functions

### mvjoin

Join multivalue field into a string:

```spl
| eval all_levels = mvjoin(values(level), ", ")
```

### mvappend

Append values to a multivalue field:

```spl
| eval tags = mvappend(source, level)
```

### mvdedup

Remove duplicates from a multivalue field:

```spl
| eval unique_hosts = mvdedup(hosts)
```

## String Concatenation

Use the `.` operator:

```spl
| eval full_msg = source . ": " . message
| eval url = "https://" . host . uri
```

## Summary Table

| Function | Description | Example |
|----------|-------------|---------|
| `IF(c, t, f)` | Conditional | `IF(x>0, "pos", "neg")` |
| `CASE(c1,v1,...)` | Multi-way conditional | `CASE(x<0,"neg", x>0,"pos", 1=1,"zero")` |
| `coalesce(a,b,...)` | First non-null | `coalesce(name, "unknown")` |
| `tonumber(s)` | String to number | `tonumber("42")` |
| `tostring(n)` | Number to string | `tostring(200)` |
| `round(n, d)` | Round | `round(3.14159, 2)` |
| `substr(s, i, n)` | Substring | `substr("hello", 1, 3)` |
| `lower(s)` | Lowercase | `lower("ERROR")` |
| `upper(s)` | Uppercase | `upper("info")` |
| `len(s)` | Length | `len(message)` |
| `ln(n)` | Natural log | `ln(duration_ms)` |
| `match(s, re)` | Regex match | `match(uri, "^/api")` |
| `strftime(t, f)` | Format time | `strftime(_time, "%H:%M")` |
| `isnotnull(f)` | Not null check | `isnotnull(error)` |
| `isnull(f)` | Null check | `isnull(error)` |
| `mvjoin(mv, d)` | Join multivalue | `mvjoin(hosts, ",")` |
| `mvappend(a,b)` | Append multivalue | `mvappend(src, dst)` |
| `mvdedup(mv)` | Dedup multivalue | `mvdedup(tags)` |
