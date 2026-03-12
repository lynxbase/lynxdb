---
title: unpack_mysql_slow
description: Parse MySQL slow query log entries into fields.
---

# unpack_mysql_slow

Parse a field containing MySQL slow query log entries and extract structured fields including timestamp, user, host, query metrics, and the SQL statement.

## Syntax

```spl
| unpack_mysql_slow [field=<field>] [fields=<field1>,<field2>,...] [prefix=<prefix>] [keep_original=true|false]
```

## Arguments

| Argument | Default | Description |
|----------|---------|-------------|
| `field` | `_raw` | Source field containing MySQL slow query log entry |
| `fields` | (all) | Comma-separated list of specific keys to extract |
| `prefix` | (none) | Prefix to prepend to extracted field names |
| `keep_original` | `false` | When `true`, keep the original source field unchanged |

## Extracted Fields

| Field | Type | Description |
|-------|------|-------------|
| `timestamp` | string | Query execution time (from `# Time:` line) |
| `user` | string | MySQL user who executed the query |
| `host` | string | Client hostname |
| `client_ip` | string | Client IP address |
| `connection_id` | integer | MySQL connection ID |
| `query_time` | float | Total query execution time in seconds |
| `lock_time` | float | Time spent waiting for locks in seconds |
| `rows_sent` | integer | Number of rows returned to the client |
| `rows_examined` | integer | Number of rows scanned by the query |
| `schema` | string | Database schema (when present) |
| `statement` | string | The SQL statement (trailing semicolons stripped) |

## Examples

```spl
-- Parse MySQL slow query log
-- Input (multi-line, joined with \n):
-- # Time: 2026-02-14T14:52:01.234567Z
-- # User@Host: root[root] @ localhost [127.0.0.1]  Id:   42
-- # Query_time: 3.456789  Lock_time: 0.000123  Rows_sent: 10  Rows_examined: 50000
-- SELECT * FROM users WHERE status = 'active'
| unpack_mysql_slow

-- Find the slowest queries
| unpack_mysql_slow
| where query_time > 1.0
| sort -query_time
| table timestamp, user, query_time, rows_examined, statement
| head 20

-- Queries with high row scan ratio
| unpack_mysql_slow
| where rows_examined > 0 AND rows_sent > 0
| eval scan_ratio = rows_examined / rows_sent
| where scan_ratio > 100
| stats count, avg(query_time) by statement

-- Lock contention analysis
| unpack_mysql_slow
| where lock_time > 0.01
| stats count, avg(lock_time), max(lock_time) by user
| sort -count
```

## Notes

- Each slow query log entry is multi-line. Events should be pre-joined into a single `_raw` string (lines joined with `\n`) before parsing.
- Supports the standard MySQL 5.7+ and MariaDB slow query log format.
- Comment lines starting with `#` are parsed for metadata; non-comment lines are treated as the SQL statement.
- Trailing semicolons are automatically stripped from the SQL statement.
- `unpack_mysql_slow` is a streaming operator -- it processes events one at a time without buffering.

## See Also

- [unpack_postgres](/docs/lynx-flow/commands/unpack-postgres) -- Parse PostgreSQL logs
- [Field Extraction Guide](/docs/guides/field-extraction) -- Detailed extraction patterns
