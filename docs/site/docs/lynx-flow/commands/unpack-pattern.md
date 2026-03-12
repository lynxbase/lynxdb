---
title: unpack_pattern
description: Parse log lines using a user-defined extraction pattern.
---

# unpack_pattern

Parse a field using a custom extraction pattern with `%{name}` placeholders. Each placeholder maps to a named capture group in the generated regex. This is useful when your log format doesn't match any of the built-in parsers.

## Syntax

```spl
| unpack_pattern pattern="<pattern>" [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `pattern` | (required) | Extraction pattern with `%{name}` placeholders |
| `field` | `_raw` | Source field to extract from |
| `fields` | (all) | Comma-separated list of specific placeholders to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Placeholder Types

| Placeholder | Regex | Description |
|-------------|-------|-------------|
| `%{name}` | `\S+` | Non-whitespace (default) |
| `%{name:int}` | `-?\d+` | Integer value |
| `%{name:float}` | `-?\d+\.?\d*` | Float value |
| `%{name:timestamp}` | `.+?` | Lazy match (for timestamps with spaces) |
| `%{name:rest}` | `.*` | Greedy match (rest of line) |

## Examples

```spl
-- Custom application log format
-- Input: web-01 [2026-02-14 14:52:01] 200 45ms "GET /api/users"
| unpack_pattern pattern="%{host} [%{timestamp:timestamp}] %{status:int} %{duration}ms \"%{request:rest}\""

-- Parse a log format with type-hinted fields
-- Input: ERROR 2026-02-14 count=42 ratio=0.85 msg=connection refused
| unpack_pattern pattern="%{level} %{date} count=%{count:int} ratio=%{ratio:float} msg=%{message:rest}"

-- Simple space-delimited format
-- Input: 10.0.0.1 GET /api 200 123
| unpack_pattern pattern="%{ip} %{method} %{path} %{status:int} %{bytes:int}"
| where status >= 500
| stats count by ip

-- Combine with aggregation
| unpack_pattern pattern="%{host} %{level} %{message:rest}"
| where level="ERROR"
| stats count by host, message
| sort -count
```

## Notes

- Literal text between `%{...}` placeholders is escaped automatically (no need to worry about regex metacharacters in fixed-format parts of the pattern).
- Type-hinted placeholders (`%{name:int}`, `%{name:float}`) automatically convert matched text to the appropriate numeric type. On conversion failure, the value is returned as a string.
- The `%{name:timestamp}` type uses a lazy match (`.+?`), which is useful for timestamps that contain spaces.
- The `%{name:rest}` type uses a greedy match (`.*`), suitable for the last field on a line that may contain spaces.
- An empty pattern or malformed `%{...}` syntax (unclosed brace) produces a parse error.
- Lines that don't match the pattern are silently skipped (schema-on-read).
- `unpack_pattern` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [rex](/docs/lynx-flow/commands/rex) -- Extract fields with full regex named capture groups
- [unpack_kv](/docs/lynx-flow/commands/unpack-kv) -- Parse key=value pairs
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
