---
title: unpack_leef
description: Parse IBM LEEF (Log Event Extended Format) lines into fields.
---

# unpack_leef

Parse a field containing IBM LEEF (Log Event Extended Format) log lines and extract structured header fields and extension key-value pairs.

## Syntax

```spl
| unpack_leef [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing LEEF text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

**Header fields** (pipe-delimited):

| Field | Type | Description |
|-------|------|-------------|
| `leef_version` | string | LEEF version (e.g., `1.0`, `2.0`) |
| `device_vendor` | string | Vendor name |
| `device_product` | string | Product name |
| `device_version` | string | Product version |
| `event_id` | string | Event identifier |

**Extension fields** are extracted dynamically from key=value pairs in the extension block. Common extension keys include `src`, `dst`, `proto`, `sev`, `cat`, `msg`, etc.

## Examples

```spl
-- Parse LEEF 1.0 (tab-delimited extensions)
-- Input: LEEF:1.0|IBM|QRadar|7.0|PortScan|src=10.0.0.1	dst=192.168.1.1	proto=TCP
| unpack_leef

-- Parse LEEF 2.0 (custom delimiter)
-- Input: LEEF:2.0|IBM|QRadar|7.5|Login|0x09|src=10.0.0.1	dst=192.168.1.1
| unpack_leef

-- Security event analysis
| unpack_leef
| stats count by device_product, event_id
| sort -count

-- Source IP correlation
| unpack_leef
| where src != ""
| stats dc(event_id) as event_types, count by src
| sort -count
| head 20
```

## Notes

- LEEF 1.0 uses tab (`\t`) as the default extension delimiter.
- LEEF 2.0 supports a custom delimiter specified as a 6th pipe-delimited header field. Common formats include `0x09` (tab), `0x20` (space), or a literal single character.
- Extension values undergo automatic type inference (numbers, booleans).
- LEEF is structurally similar to [CEF](/docs/lynx-flow/commands/unpack-cef) but uses a different header layout.
- `unpack_leef` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_cef](/docs/lynx-flow/commands/unpack-cef) -- Parse CEF (Common Event Format) logs
- [unpack_kv](/docs/lynx-flow/commands/unpack-kv) -- Generic key=value parsing
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
