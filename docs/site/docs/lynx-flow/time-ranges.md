---
title: Time Ranges
description: Time range syntax for queries -- relative, absolute, and modifiers.
---

# Time Ranges

Every query in LynxDB operates over a time range. Time ranges can be specified on the CLI, in the API, or within the query itself.

## Relative Time Ranges

Relative to "now":

| Syntax | Meaning |
|--------|---------|
| `15m` | Last 15 minutes |
| `1h` | Last 1 hour |
| `6h` | Last 6 hours |
| `24h` or `1d` | Last 24 hours |
| `7d` | Last 7 days |
| `30d` | Last 30 days |

### CLI Usage

```bash
lynxdb query 'level=error | stats count' --since 1h
lynxdb query 'level=error | stats count' --since 7d
```

### API Usage

```json
{"q": "level=error | stats count", "from": "-1h"}
{"q": "level=error | stats count", "from": "-7d", "to": "now"}
```

## Absolute Time Ranges

ISO 8601 / RFC 3339 format:

```bash
lynxdb query 'level=error | stats count' \
  --from 2026-01-15T00:00:00Z \
  --to 2026-01-16T00:00:00Z
```

```json
{
  "q": "level=error | stats count",
  "from": "2026-01-15T00:00:00Z",
  "to": "2026-01-16T00:00:00Z"
}
```

## Default Time Range

When no time range is specified:

- **CLI:** Queries all data (no time restriction)
- **API:** Queries all data (no time restriction)
- **Shell:** Last 15 minutes (configurable with `.set since`)

## Time Range in Queries

The `bin` and `timechart` commands use `span` to control bucket size:

```spl
| timechart count span=5m              -- 5-minute buckets
| bin _time span=1h                     -- 1-hour buckets
| bin _time span=1d                     -- 1-day buckets
```

Supported span units: `s` (seconds), `m` (minutes), `h` (hours), `d` (days).

## Timestamp Auto-Detection

At ingest, LynxDB auto-detects timestamp fields in this order:

1. `_timestamp`
2. `timestamp`
3. `@timestamp`
4. `time`
5. `ts`
6. `datetime`

If no timestamp field is found, the current server time is assigned.

Supported timestamp formats:
- ISO 8601: `2026-01-15T10:30:00Z`
- RFC 3339: `2026-01-15T10:30:00+00:00`
- Unix epoch (seconds): `1737000000`
- Unix epoch (milliseconds): `1737000000000`
- Common log format: `15/Jan/2026:10:30:00 +0000`
