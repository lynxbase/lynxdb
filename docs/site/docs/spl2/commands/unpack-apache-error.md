---
title: unpack_apache_error
description: Parse Apache 2.4+ error log lines into fields.
---

# unpack_apache_error

Parse a field containing Apache 2.4+ error log output and extract structured fields including timestamp, module, severity, PID/TID, client IP, error code, and message.

## Syntax

```spl
| unpack_apache_error [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing Apache error log text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | Log timestamp from bracket field |
| `module` | string | Apache module name (e.g., `ssl`, `core`, `proxy`) |
| `severity` | string | Log severity (e.g., `error`, `warn`, `info`) |
| `pid` | integer | Apache process ID |
| `tid` | integer | Thread ID |
| `client_ip` | string | Client IP address (when present) |
| `client_port` | integer | Client port number (when present) |
| `error_code` | string | Apache error code (e.g., `AH02032`) |
| `message` | string | Error message body |

## Examples

```spl
-- Parse Apache error log
-- Input: [Fri Feb 14 14:52:01.234567 2026] [ssl:error] [pid 12345:tid 67890] [client 192.168.1.100:52436] AH02032: Hostname provided via SNI not found
| unpack_apache_error

-- Filter by severity
| unpack_apache_error
| where severity="error"
| stats count by module

-- Find top error codes
| unpack_apache_error
| where error_code != ""
| stats count by error_code
| sort -count
| head 10

-- Track errors by client IP
| unpack_apache_error
| where severity="error" AND client_ip != ""
| stats count by client_ip
| sort -count
```

## Notes

- Supports the Apache 2.4+ error log format with bracket-delimited fields.
- The `[module:severity]` field is split into separate `module` and `severity` fields.
- Apache error codes (e.g., `AH02032:`) are automatically extracted from the message prefix.
- The `[client ip:port]` bracket field is optional and only present for client-initiated errors.
- `unpack_apache_error` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_combined](/docs/spl2/commands/unpack-combined) -- Parse Apache/Nginx access logs
- [unpack_nginx_error](/docs/spl2/commands/unpack-nginx-error) -- Parse Nginx error logs
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
