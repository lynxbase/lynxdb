---
title: unpack_postgres
description: Parse PostgreSQL stderr log lines into fields.
---

# unpack_postgres

Parse a field containing PostgreSQL stderr log format output and extract structured fields including timestamp, PID, user, database, severity, message, and query duration.

## Syntax

```spl
| unpack_postgres [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing PostgreSQL log text |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | Log timestamp (e.g., `2026-02-14 14:52:01.234 UTC`) |
| `pid` | integer | Backend process ID |
| `user` | string | Database user (when present) |
| `database` | string | Database name (when present) |
| `severity` | string | Log severity (`LOG`, `ERROR`, `WARNING`, `FATAL`, `PANIC`, `INFO`, `NOTICE`, `DEBUG1`-`DEBUG5`) |
| `message` | string | Log message body |
| `duration_ms` | float | Query duration in milliseconds (auto-extracted from `duration: N ms` patterns) |

## Examples

```spl
-- Parse PostgreSQL stderr log
-- Input: 2026-02-14 14:52:01.234 UTC [12345] postgres@mydb LOG:  connection authorized
| unpack_postgres

-- Find slow queries (duration > 100ms)
| unpack_postgres
| where duration_ms > 100
| stats avg(duration_ms), max(duration_ms) by user, database

-- Error rate by database
| unpack_postgres
| where severity="ERROR"
| stats count by database
| sort -count

-- Fatal errors requiring attention
| unpack_postgres
| where severity IN ("FATAL", "PANIC")
| table timestamp, user, database, message
```

## Notes

- Supports the standard PostgreSQL stderr log format: `YYYY-MM-DD HH:MM:SS.mmm TZ [pid] user@database SEVERITY:  message`.
- Automatically extracts `duration_ms` from `duration: N.NNN ms` patterns commonly found in slow query logs and `auto_explain` output.
- PostgreSQL uses double spaces between the severity colon and the message (`SEVERITY:  message`).
- The `user@database` portion is optional and may be absent in some log lines (e.g., startup messages).
- `unpack_postgres` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_mysql_slow](/docs/lynx-flow/commands/unpack-mysql-slow) -- Parse MySQL slow query logs
- [unpack_syslog](/docs/lynx-flow/commands/unpack-syslog) -- Parse syslog-wrapped database logs
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
