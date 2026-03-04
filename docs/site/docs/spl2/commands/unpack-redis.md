---
title: unpack_redis
description: Parse Redis server log lines into fields.
---

# unpack_redis

Parse a field containing Redis server log output and extract structured fields including PID, role, timestamp, log level, and message.

## Syntax

```spl
| unpack_redis [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing Redis log text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `pid` | integer | Redis process ID |
| `role_char` | string | Single character role (`M`, `S`, `C`, `X`) |
| `role` | string | Human-readable role (`master`, `replica`, `rdb_child`, `sentinel`) |
| `timestamp` | string | Log timestamp (e.g., `14 Feb 2026 14:52:01.234`) |
| `level_char` | string | Single character level (`.`, `-`, `*`, `#`) |
| `level` | string | Human-readable level (`debug`, `verbose`, `notice`, `warning`) |
| `message` | string | Log message body |

## Examples

```spl
-- Parse Redis server log
-- Input: 12345:M 14 Feb 2026 14:52:01.234 * Ready to accept connections
| unpack_redis

-- Filter by severity
| unpack_redis
| where level="warning"
| table timestamp, role, message

-- Stats by role
| unpack_redis
| stats count by role, level
| sort -count

-- Monitor replication issues
| unpack_redis
| where role="replica" AND level="warning"
| stats count by message
```

## Notes

- Redis role characters: `M` = master, `S` = replica, `C` = RDB/AOF child, `X` = sentinel.
- Redis level characters: `.` = debug, `-` = verbose, `*` = notice, `#` = warning.
- `unpack_redis` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_logfmt](/docs/spl2/commands/unpack-logfmt) -- Parse logfmt output
- [unpack_syslog](/docs/spl2/commands/unpack-syslog) -- Parse syslog messages
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
