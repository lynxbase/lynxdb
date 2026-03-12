---
title: unpack_w3c
description: Parse W3C Extended Log Format lines into fields.
---

# unpack_w3c

Parse a field containing W3C Extended Log Format data and extract fields based on a `#Fields:` directive. This format is used by IIS, some CDNs, and other web servers.

## Syntax

```spl
| unpack_w3c [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false] [header=<fields_directive>]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing W3C log text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |
| `header` | (auto) | W3C `#Fields:` directive defining column names. When omitted, the parser detects embedded `#Fields:` lines in the data. |

## Extracted Fields

Fields are defined by the `#Fields:` directive. Common W3C fields include:

| Field | Type | Description |
|-------|------|-------------|
| `date` | string | Request date |
| `time` | string | Request time |
| `s_ip` | string | Server IP address |
| `cs_method` | string | HTTP method |
| `cs_uri_stem` | string | URI stem (path) |
| `cs_uri_query` | string | URI query string |
| `s_port` | integer | Server port |
| `cs_username` | string | Authenticated username |
| `c_ip` | string | Client IP address |
| `cs_user_agent` | string | User agent string |
| `sc_status` | integer | HTTP status code |
| `sc_substatus` | integer | HTTP sub-status code |
| `sc_bytes` | integer | Bytes sent to client |
| `cs_bytes` | integer | Bytes received from client |
| `time_taken` | integer | Time taken in milliseconds |

## Examples

```spl
-- Parse W3C log with embedded #Fields directive
-- Input lines:
-- #Fields: date time s-ip cs-method cs-uri-stem sc-status
-- 2026-02-14 14:52:01 10.0.0.1 GET /api/health 200
| unpack_w3c

-- Parse with explicit header
| unpack_w3c header="date time c-ip cs-method cs-uri-stem sc-status sc-bytes time-taken"

-- IIS log analysis: slow requests
| unpack_w3c
| where time_taken > 5000
| stats count, avg(time_taken) by cs_uri_stem
| sort -count

-- Status code distribution
| unpack_w3c
| stats count by sc_status
| sort -count
```

## Notes

- Field names containing hyphens are automatically normalized to underscores (e.g., `cs-uri-stem` becomes `cs_uri_stem`) so they are valid SPL2 identifiers.
- Comment/directive lines starting with `#` are used to update field definitions but do not emit data rows.
- The value `-` is treated as missing (null) per the W3C Extended Log Format specification.
- Values undergo automatic type inference (numbers, booleans).
- If no `#Fields:` directive is provided and none is found in the data, no fields are extracted.
- `unpack_w3c` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_combined](/docs/lynx-flow/commands/unpack-combined) -- Parse Apache/Nginx combined access logs
- [unpack_clf](/docs/lynx-flow/commands/unpack-clf) -- Parse Common Log Format
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
