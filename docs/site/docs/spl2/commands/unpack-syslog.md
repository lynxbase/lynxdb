---
title: unpack_syslog
description: Parse syslog messages (RFC 3164 and RFC 5424) into fields.
---

# unpack_syslog

Parse a field containing syslog messages and extract structured fields including priority, facility, severity, hostname, and message.

## Syntax

```spl
| unpack_syslog [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing syslog text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `priority` | integer | Raw PRI value (0-191) |
| `facility` | integer | Syslog facility code (priority / 8) |
| `severity` | integer | Syslog severity code (priority % 8) |
| `timestamp` | string | Syslog timestamp |
| `hostname` | string | Originating host |
| `appname` | string | Application name (RFC 5424) or process tag (RFC 3164) |
| `procid` | string | Process ID (RFC 5424 only) |
| `msgid` | string | Message ID (RFC 5424 only) |
| `message` | string | Message body |

## Examples

```spl
-- Parse syslog from _raw
-- Input: <134>Jan 15 14:23:01 web-01 nginx: connection reset
| unpack_syslog

-- RFC 5424 message
-- Input: <165>1 2026-01-15T14:23:01Z web-01 myapp 1234 - - User logged in
| unpack_syslog

-- Filter by severity
| unpack_syslog
| where severity <= 3
| table timestamp, hostname, appname, message

-- Stats by facility
| unpack_syslog
| stats count by facility, hostname
| sort -count
```

## Notes

- Auto-detects RFC 3164 (BSD syslog) and RFC 5424 formats.
- The `<PRI>` header is parsed to derive both `facility` and `severity` codes.
- `unpack_syslog` is a streaming operator -- it processes events one at a time without buffering.
- For syslog messages that embed JSON in the message body, chain with `unpack_json`:
  ```spl
  | unpack_syslog | unpack_json field=message prefix=app_
  ```

## See Also

- [unpack_json](/docs/spl2/commands/unpack-json) -- Parse embedded JSON
- [unpack_combined](/docs/spl2/commands/unpack-combined) -- Parse HTTP access logs
- [Working with JSON Logs](/docs/guides/json-processing) -- Chaining parsers for nested formats
