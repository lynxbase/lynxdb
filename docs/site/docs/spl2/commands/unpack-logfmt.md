---
title: unpack_logfmt
description: Parse logfmt key=value pairs into fields.
---

# unpack_logfmt

Parse a field containing logfmt-style `key=value` pairs and extract each pair as a top-level field.

## Syntax

```spl
| unpack_logfmt [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing logfmt text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Examples

```spl
-- Parse logfmt from _raw
-- Input: level=info msg="request completed" duration=245ms status=200
| unpack_logfmt

-- Parse from a specific field
| unpack_logfmt field=message

-- Extract only specific keys
| unpack_logfmt fields=level,duration,status

-- Add a prefix
| unpack_logfmt prefix=log_

-- Combine with aggregation
| unpack_logfmt
| where level="error"
| stats count by msg
| sort -count
| head 10
```

## Notes

- Supports both bare values (`key=value`) and quoted values (`key="value with spaces"`).
- Type inference converts numeric and boolean values automatically.
- Logfmt is common in Go applications (e.g., `log/slog`, zerolog, logrus text format).
- `unpack_logfmt` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_json](/docs/spl2/commands/unpack-json) -- Parse JSON fields
- [unpack_kv](/docs/spl2/commands/unpack-kv) -- Generic key=value parsing with custom delimiters
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
