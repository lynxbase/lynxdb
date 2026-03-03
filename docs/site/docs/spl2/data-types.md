---
title: Data Types
description: SPL2 data types -- string, number, boolean, timestamp, and null.
---

# Data Types

SPL2 supports five data types. Fields are dynamically typed -- the same field can hold different types in different events (schema-on-read).

## String

Text values. Enclose in double quotes in queries:

```spl
| where level = "error"
| eval greeting = "hello " . name
```

## Number

Integer and floating-point values. LynxDB auto-detects numeric fields:

```spl
| where status >= 500
| eval rate = round(errors / total * 100, 1)
```

Numeric literals: `42`, `3.14`, `-1`, `1e6`.

## Boolean

True/false values. Used in `where` and `eval` expressions:

```spl
| where isnotnull(error_msg)
| eval is_error = IF(status >= 500, true, false)
```

## Timestamp / Datetime

Timestamps are first-class. The `_time` field is always a timestamp. At ingest, LynxDB auto-detects timestamps from: `_timestamp`, `timestamp`, `@timestamp`, `time`, `ts`, `datetime`.

```spl
| where _time >= "2026-01-15T00:00:00Z"
| eval hour = strftime(_time, "%H")
```

Supported formats: ISO 8601, RFC 3339, Unix epoch (seconds and milliseconds), and common log formats.

## Null

Missing or undefined values. Use `isnull()` / `isnotnull()` to test:

```spl
| where isnotnull(error_message)
| eval name = coalesce(display_name, username, "unknown")
| fillnull value=0 duration_ms
```

## Type Coercion

- String to number: `tonumber("42")` returns `42`
- Number to string: `tostring(200)` returns `"200"`
- Comparison operators auto-coerce when possible (e.g., `status >= 500` works even if `status` is stored as a string "500")
