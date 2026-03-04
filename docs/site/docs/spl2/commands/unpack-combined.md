---
title: unpack_combined
description: Parse NCSA Combined access log format into fields.
---

# unpack_combined

Parse a field containing NCSA Combined Log Format lines (Apache/Nginx access logs) and extract structured fields.

## Syntax

```spl
| unpack_combined [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing the access log line |
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
| `request` | string | Full request line (e.g., `GET /api HTTP/1.1`) |
| `method` | string | HTTP method (derived from request) |
| `uri` | string | Request URI (derived from request) |
| `protocol` | string | HTTP protocol (derived from request) |
| `status` | integer | HTTP status code |
| `bytes` | integer | Response body size in bytes |
| `referer` | string | HTTP Referer header |
| `user_agent` | string | HTTP User-Agent header |

## Examples

```spl
-- Parse nginx/Apache access logs
-- Input: 10.0.1.5 - frank [10/Oct/2025:13:55:36 -0700] "GET /api/v1/users HTTP/1.1" 200 2326 "https://example.com" "Mozilla/5.0"
| unpack_combined

-- Find 5xx errors
| unpack_combined
| where status >= 500
| stats count by uri, status
| sort -count

-- Top user agents
| unpack_combined
| stats count by user_agent
| sort -count
| head 10

-- Traffic by method and status
| unpack_combined
| stats count by method, status
```

## Notes

- The Combined format extends the Common Log Format (CLF) by adding `referer` and `user_agent` fields. For CLF-only logs, use [`unpack_clf`](/docs/spl2/commands/unpack-clf).
- The `request` line is automatically split into `method`, `uri`, and `protocol` fields.
- Dash (`-`) values for `ident`, `user`, `referer`, or `user_agent` are treated as null.
- `unpack_combined` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_clf](/docs/spl2/commands/unpack-clf) -- Parse Common Log Format (without referer/user_agent)
- [unpack_nginx_error](/docs/spl2/commands/unpack-nginx-error) -- Parse nginx error logs
- [rex](/docs/spl2/commands/rex) -- Custom regex extraction for non-standard formats
