---
title: unpack_nginx_error
description: Parse nginx error log format into structured fields.
---

# unpack_nginx_error

Parse a field containing nginx error log lines and extract structured fields including timestamp, severity, process info, and key-value metadata.

## Syntax

```spl
| unpack_nginx_error [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing the nginx error log line |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | Error timestamp (`YYYY/MM/DD HH:MM:SS`) |
| `severity` | string | Log level (e.g., `error`, `warn`, `crit`) |
| `pid` | integer | Worker process ID |
| `tid` | integer | Thread ID |
| `cid` | integer | Connection ID |
| `message` | string | Error message text |
| `client` | string | Client IP address |
| `server` | string | Server name |
| `request` | string | Full HTTP request line |
| `method` | string | HTTP method (derived from request) |
| `uri` | string | Request URI (derived from request) |
| `upstream` | string | Upstream server address |
| `host` | string | Host header value |

## Examples

```spl
-- Parse nginx error log
-- Input: 2026/02/14 14:52:01 [error] 12345#67: *890 connect() failed, client: 10.0.1.5, server: api.example.com, request: "GET /health HTTP/1.1"
| unpack_nginx_error

-- Find errors by severity
| unpack_nginx_error
| stats count by severity
| sort -count

-- Upstream failures
| unpack_nginx_error
| where upstream != ""
| stats count by upstream, message
| sort -count

-- Client error analysis
| unpack_nginx_error
| stats count by client, uri
| sort -count
| head 20
```

## Notes

- The nginx error log format includes a structured trailer of `key: value` pairs (note: colon-space separated, not `=`). The parser extracts known keys: `client`, `server`, `request`, `upstream`, `host`.
- When a `request` field is found, `method` and `uri` are derived automatically.
- `unpack_nginx_error` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_combined](/docs/spl2/commands/unpack-combined) -- Parse nginx/Apache access logs
- [unpack_clf](/docs/spl2/commands/unpack-clf) -- Parse Common Log Format
