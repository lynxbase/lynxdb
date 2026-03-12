---
title: unpack_json
description: Parse JSON fields and extract all keys as top-level fields.
---

# unpack_json

Parse a field containing JSON text and extract all keys as top-level fields with automatic type inference.

## Syntax

```spl
| unpack_json [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing JSON text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Examples

```spl
-- Extract all JSON fields from _raw
| unpack_json

-- Extract from a specific field
| unpack_json field=message

-- Extract only specific keys
| unpack_json fields=level,duration_ms,user_id

-- Add a prefix to avoid collisions
| unpack_json field=metadata prefix=meta_

-- Keep the original JSON field
| unpack_json keep_original=true | table _raw, level, status
```

### Common patterns

```spl
-- Docker logs: JSON outer + inner application log
| unpack_json
| unpack_json field=log prefix=app_

-- Parse and aggregate
| unpack_json
| where level="error"
| stats count by service

-- Extract, convert, and filter
| unpack_json
| eval dur = tonumber(duration_ms)
| where dur > 1000
| table _timestamp, service, dur
```

## Notes

- Nested JSON objects are stored as JSON strings in the extracted field. Use dot-notation (`response.status`) or chain another `unpack_json` to access nested values.
- Type inference converts numbers, booleans, and null automatically. Strings remain strings.
- `unpack_json` is a streaming operator -- it processes events one at a time without buffering.
- When `fields` is specified, only listed keys are extracted, which is faster for wide JSON objects.
- For quick JSON extraction in ad-hoc queries, use the shorthand [`json`](/docs/lynx-flow/commands/json-cmd) command.

## See Also

- [json](/docs/lynx-flow/commands/json-cmd) -- Shorthand for common JSON extraction
- [unpack_logfmt](/docs/lynx-flow/commands/unpack-logfmt) -- Parse logfmt key=value pairs
- [JSON Functions](/docs/lynx-flow/functions/json-functions) -- json_extract, json_keys, json_valid, etc.
- [Working with JSON Logs](/docs/guides/json-processing) -- Comprehensive JSON processing guide
