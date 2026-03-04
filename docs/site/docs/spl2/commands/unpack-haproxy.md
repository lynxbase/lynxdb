---
title: unpack_haproxy
description: Parse HAProxy HTTP log lines into fields.
---

# unpack_haproxy

Parse a field containing HAProxy HTTP log format output and extract structured fields including client info, timestamps, backend/server names, timing metrics, status codes, and HTTP request details.

## Syntax

```spl
| unpack_haproxy [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing HAProxy log text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `client_ip` | string | Client IP address |
| `client_port` | integer | Client port number |
| `timestamp` | string | Accept date (from bracket field) |
| `frontend` | string | Frontend name (SSL indicator `~` stripped) |
| `backend` | string | Backend name |
| `server` | string | Server name |
| `tq` | integer | Request queue time (ms) |
| `tw` | integer | Time waiting in queue (ms) |
| `tc` | integer | Time to connect to server (ms) |
| `tr` | integer | Server response time (ms) |
| `tt` | integer | Total session duration (ms) |
| `status` | integer | HTTP status code |
| `bytes` | integer | Bytes read by client |
| `term_state` | string | Termination state (4 chars, e.g., `----` or `LR--`) |
| `actconn` | integer | Active connections |
| `feconn` | integer | Frontend connections |
| `beconn` | integer | Backend connections |
| `srv_conn` | integer | Server connections |
| `retries` | integer | Connection retries |
| `method` | string | HTTP method (e.g., `GET`, `POST`) |
| `uri` | string | Request URI |
| `protocol` | string | HTTP protocol (e.g., `HTTP/1.1`) |

## Examples

```spl
-- Parse HAProxy HTTP log
-- Input: 10.0.0.1:56000 [14/Feb/2026:14:52:01.234] web~ app/srv1 10/0/30/69/109 200 1234 - - ---- 1/1/0/0/0 0/0 "GET /api/health HTTP/1.1"
| unpack_haproxy

-- Slow backend responses
| unpack_haproxy
| where tr > 500
| stats count, avg(tr), p95(tr) by backend, server
| sort -count

-- Error rate by backend
| unpack_haproxy
| where status >= 500
| stats count by backend, server, status
| sort -count

-- Connection retries indicating backend issues
| unpack_haproxy
| where retries > 0
| stats count, sum(retries) by backend
| sort -count

-- Client traffic analysis
| unpack_haproxy
| stats count, sum(bytes) as total_bytes by client_ip
| sort -total_bytes
| head 20
```

## Notes

- Supports the standard HAProxy `option httplog` format.
- Automatically handles optional syslog prefix (e.g., `Feb 14 14:52:01 hostname haproxy[pid]:`) by detecting and skipping it.
- The SSL indicator `~` on frontend names is automatically stripped.
- Timing fields (`tq`, `tw`, `tc`, `tr`, `tt`) are in milliseconds; a value of `-1` indicates the timer was not set.
- `unpack_haproxy` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_combined](/docs/spl2/commands/unpack-combined) -- Parse HTTP access logs
- [unpack_nginx_error](/docs/spl2/commands/unpack-nginx-error) -- Parse Nginx error logs
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
