---
title: unpack_cef
description: Parse Common Event Format (CEF) log messages into fields.
---

# unpack_cef

Parse a field containing Common Event Format (CEF) messages and extract the header fields and extension key-value pairs.

## Syntax

```spl
| unpack_cef [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing the CEF message |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

### Header fields

| Field | Type | Description |
|-------|------|-------------|
| `cef_version` | string | CEF version (e.g., `0`) |
| `device_vendor` | string | Vendor name |
| `device_product` | string | Product name |
| `device_version` | string | Product version |
| `signature_id` | string | Event class ID / signature |
| `name` | string | Event name |
| `severity` | integer | Event severity (0-10) |

### Extension fields

All key=value pairs in the CEF extension are extracted as additional fields with automatic type inference.

## Examples

```spl
-- Parse a CEF message
-- Input: CEF:0|Acme|Firewall|1.0|100|Connection Blocked|7|src=10.0.0.1 dst=192.168.1.1 dpt=443 proto=TCP
| unpack_cef

-- Filter by severity
| unpack_cef
| where severity >= 7
| table name, src, dst, dpt

-- Stats by vendor and event
| unpack_cef
| stats count by device_vendor, name
| sort -count

-- Escaped pipes in vendor name
-- Input: CEF:0|Acme \| Inc|Product|1.0|200|Alert|5|msg=test
| unpack_cef
-- device_vendor = "Acme | Inc"
```

## Notes

- CEF messages start with `CEF:` followed by 7 pipe-delimited header fields, then optional `key=value` extension pairs.
- Escaped pipes (`\|`) in header fields are handled correctly.
- Extension values may contain spaces; the parser uses lookahead to find the next `key=` boundary.
- `unpack_cef` is a streaming operator -- it processes events one at a time without buffering.
- CEF is commonly used by SIEM products (ArcSight, QRadar, Splunk) and network security appliances.

## See Also

- [unpack_kv](/docs/spl2/commands/unpack-kv) -- Generic key=value parsing
- [unpack_syslog](/docs/spl2/commands/unpack-syslog) -- Parse syslog wrapper around CEF
