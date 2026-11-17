---
sidebar_position: 7
title: status, fields, jobs & more
description: Inspect a LynxDB server - status, health, indexes, cache, query jobs, the field catalog and the CLI version.
---

# status, fields, jobs & more

Short commands that read state from the server named by `--server`. All of them accept `--format json` for scripts; table formats (`table`, `csv`, `tsv`, ...) print plain rows.

## status

```
lynxdb status
```

**Alias:** `st`

Server version, uptime, health, storage size, event counts, segments, events waiting in the ingest buffer, indexes, the top sources, the oldest event, and the query memory pool.

```
  LynxDB 1.0.0 — uptime 2d 5h 30m — healthy

  Storage   1.2 GB
  Events    3,456,789 total  123,456 today
  Segments  42  buffer: 0 events
  Indexes   3
  Sources   nginx (45%), api-gateway (30%), postgres (25%)
  Oldest    2026-01-08T10:30:00Z
```

```bash
lynxdb status --format json
```

```json
{
  "buffered_events": 0,
  "events_today": 116,
  "health": "healthy",
  "index_count": 2,
  "oldest_event": "2025-01-15T08:00:00Z",
  "segment_count": 6,
  "storage_bytes": 245559,
  "total_events": 1006,
  "uptime_seconds": 445
}
```

## health

```
lynxdb health
```

Calls `GET /health` and prints `Server is healthy`, or the reported status (`degraded`, `unhealthy`). Use it in readiness checks:

```bash
lynxdb health --format json
```

## indexes

```
lynxdb indexes
```

Lists indexes with their retention and replication factor.

```
┌─────────┬───────────┬─────────────┐
│ NAME    │ RETENTION │ REPLICATION │
├─────────┼───────────┼─────────────┤
│ main    │ 2160h0m0s │           1 │
│ nginx   │ 2160h0m0s │           1 │
└─────────┴───────────┴─────────────┘
```

## cache

```
lynxdb cache stats
lynxdb cache clear [--force]
```

`cache stats` prints hits, misses, hit rate, entries, size and evictions of the query result cache. `cache clear` empties it after a confirmation prompt; `--force` skips the prompt.

## jobs

```
lynxdb jobs [job_id] [flags]
```

A server query that outlives the server's synchronous wait becomes an asynchronous job (`lynxdb query` waits for it automatically). `jobs` lists them, shows one, or cancels one.

| Flag | Default | Description |
|------|---------|-------------|
| `--status` | | List only jobs in this state: `running`, `done`, `error` |
| `--cancel` | `false` | Cancel the job given as the argument |

```bash
lynxdb jobs
lynxdb jobs --status running
lynxdb jobs qry_8c5ca731a36c88d7
lynxdb jobs qry_8c5ca731a36c88d7 --cancel
```

```
┌──────────────────────┬────────┬──────────┬──────────────────────────────────────────────────┐
│ ID                   │ STATUS │ PROGRESS │ QUERY                                            │
├──────────────────────┼────────┼──────────┼──────────────────────────────────────────────────┤
│ qry_8c5ca731a36c88d7 │ done   │ 100%     │ source=main level=ERROR | stats count() by host  │
└──────────────────────┴────────┴──────────┴──────────────────────────────────────────────────┘
```

A single job shows its status, query, progress, segments scanned, elapsed time and error, if any. Job IDs tab-complete when [shell completion](./completion.md) is set up. With `--format json`, the list prints one JSON object per job.

## fields

```
lynxdb fields [name] [flags]
```

**Alias:** `f`

The server's field catalog: every field seen in stored events, with its type, its coverage (percentage of events that have it) and its most common values.

| Flag | Short | Description |
|------|-------|-------------|
| `--values` | | With a field name: list its top 50 values with counts and percentages |
| `--since` | `-s` | Only events from a relative window: `15m`, `1h`, `7d` |
| `--from` | | Absolute start time (ISO 8601) |
| `--to` | | Absolute end time (ISO 8601) |
| `--source` | | Only events from this source |
| `--prefix` | | Only fields whose names start with this text |

```bash
lynxdb fields
lynxdb fields --prefix st
lynxdb fields status
lynxdb fields status --values
lynxdb fields --source nginx --since 1h
lynxdb fields --format json
```

```
  status — top values

┌───────┬───────┬──────┐
│ VALUE │ COUNT │ %    │
├───────┼───────┼──────┤
│ 200   │   355 │ 35.5 │
│ 500   │    81 │  8.1 │
│ 503   │    72 │  7.2 │
└───────┴───────┴──────┘
```

An unknown field name fails with the closest known names. Field names tab-complete when shell completion is set up. Use the names in queries such as `source=main | stats count() by status`.

## version

```
lynxdb version [--short | --json]
```

| Flag | Description |
|------|-------------|
| `--short` | Print only the version string |
| `--json` | Print name, version, commit, build date, Go version, OS and architecture as JSON |

On a terminal the command prints a short table; when stdout is not a terminal it prints the JSON form.

## See also

- [Server API](../api/server.md)
- [Fields API](../api/fields.md)
- [query](./query.md)
