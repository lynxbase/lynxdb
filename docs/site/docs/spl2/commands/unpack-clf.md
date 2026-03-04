---
title: unpack_clf
description: Parse NCSA Common Log Format into fields.
---

# unpack_clf

Parse a field containing NCSA Common Log Format (CLF) lines and extract structured fields.

## Syntax

```spl
| unpack_clf [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing the CLF log line |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `client_ip` | string | Client IP address |
| `ident` | string | RFC 1413 identity (usually `-`) |
| `user` | string | Authenticated user (usually `-`) |
| `timestamp` | string | Request timestamp |
| `request` | string | Full request line |
| `method` | string | HTTP method (derived from request) |
| `uri` | string | Request URI (derived from request) |
| `protocol` | string | HTTP protocol (derived from request) |
| `status` | integer | HTTP status code |
| `bytes` | integer | Response body size in bytes |

## Examples

```spl
-- Parse CLF access logs
-- Input: 127.0.0.1 - frank [10/Oct/2025:13:55:36 -0700] "GET /api HTTP/1.1" 200 2326
| unpack_clf

-- Find large responses
| unpack_clf
| where bytes > 100000
| table client_ip, uri, bytes

-- Status code distribution
| unpack_clf
| stats count by status
| sort -count
```

## Notes

- CLF is identical to the Combined format but without the `referer` and `user_agent` fields. For Combined logs, use [`unpack_combined`](/docs/spl2/commands/unpack-combined).
- Dash (`-`) values for `ident` or `user` are treated as null.
- `unpack_clf` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_combined](/docs/spl2/commands/unpack-combined) -- Parse Combined Log Format (CLF + referer/user_agent)
- [unpack_nginx_error](/docs/spl2/commands/unpack-nginx-error) -- Parse nginx error logs
