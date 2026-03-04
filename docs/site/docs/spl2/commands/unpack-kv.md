---
title: unpack_kv
description: Parse generic key=value pairs with configurable delimiters.
---

# unpack_kv

Parse a field containing key=value pairs with configurable delimiters and extract each pair as a top-level field.

## Syntax

```spl
| unpack_kv [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false] [delim=<char>] [assign=<char>] [quote=<char>]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing key=value text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |
| `delim` | ` ` (space) | Character that separates key=value pairs |
| `assign` | `=` | Character that separates key from value |
| `quote` | `"` | Character used to quote values containing delimiters |

## Examples

```spl
-- Default: space-delimited, equals-separated
-- Input: host=web-01 status=200 duration=45ms
| unpack_kv

-- Comma-delimited pairs
-- Input: host=web-01,status=200,duration=45ms
| unpack_kv delim=","

-- Colon-separated key:value with semicolon delimiter
-- Input: host:web-01;status:200;duration:45ms
| unpack_kv delim=";" assign=":"

-- Custom quote character
-- Input: host='web 01' status=200
| unpack_kv quote="'"

-- Extract from specific field with prefix
| unpack_kv field=metadata prefix=meta_ delim=","
```

## Notes

- `unpack_kv` is more flexible than `unpack_logfmt` -- it supports configurable delimiters for non-standard formats.
- For standard logfmt (`key=value` with space delimiter and quoted strings), use [`unpack_logfmt`](/docs/spl2/commands/unpack-logfmt) which is purpose-built and faster.
- Type inference converts numeric strings to integers/floats and `true`/`false` to booleans.
- Quoted values support escape sequences (e.g., `\"` within double-quoted values).
- `unpack_kv` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_logfmt](/docs/spl2/commands/unpack-logfmt) -- Parse standard logfmt format
- [unpack_json](/docs/spl2/commands/unpack-json) -- Parse JSON fields
- [rex](/docs/spl2/commands/rex) -- Custom regex extraction for complex formats
