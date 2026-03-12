---
title: json
description: Shorthand command for quick JSON field extraction.
---

# json

Shorthand command for extracting fields from JSON data. Equivalent to `unpack_json` with a simpler syntax for common use cases.

## Syntax

```spl
| json [field=<field>] [<path1>, <path2>, ...]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing JSON text |
| paths | (all) | Optional list of specific JSON keys to extract |

## Examples

```spl
-- Extract all JSON fields from _raw
| json

-- Extract from a specific field
| json field=message

-- Extract specific paths only
| json level, status, duration_ms

-- Chain with filtering
| json | where level="error" | stats count by service

-- Extract from nested JSON (combined with dot-notation)
| json | where response.status >= 500
```

## Notes

- `| json` is a convenience alias for `| unpack_json`. For advanced options like `prefix` or `keep_original`, use [`unpack_json`](/docs/lynx-flow/commands/unpack-json) directly.
- When paths are specified, only those keys are extracted, improving performance for wide JSON objects.
- `json` is a streaming operator -- it processes events one at a time without buffering.
- Works seamlessly with dot-notation for accessing nested fields after extraction.

## See Also

- [unpack_json](/docs/lynx-flow/commands/unpack-json) -- Full JSON extraction with all options
- [JSON Functions](/docs/lynx-flow/functions/json-functions) -- json_extract, json_keys, json_valid, etc.
- [Working with JSON Logs](/docs/guides/json-processing) -- Comprehensive JSON processing guide
