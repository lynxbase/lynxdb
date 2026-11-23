---
sidebar_position: 7
title: status, fields, jobs & more
description: Inspect a LynxDB server - status and health, indexes, query jobs, the field catalog and the CLI version.
---

# status, fields, jobs & more

Short commands that read state from the server named by `--server`. All of them accept `--format json` for scripts; the table formats (`table`, `csv`, `tsv`, ...) print plain rows.

## status

```
lynxdb status
```

**Alias:** `st`

Server version, uptime and health, then the storage size, the number of events, parts and indexes, each index with its share of the events, and the times of the oldest and newest events. The server writes the events ingested so far before it answers, so the counts include them.

`status` exits with code 3 when the server cannot be reached, and with code 1, after printing the status, when the server reports a health other than `healthy`. That makes it a readiness check for scripts:

```bash
lynxdb status --quiet > /dev/null || echo "lynxdb is down"
```

```bash
lynxdb status --format json
```

```json
{
  "health": "healthy",
  "index_count": 3,
  "indexes": [
    {
      "name": "backend",
      "events": 26,
      "bytes": 5959,
      "parts": 1,
      "first_event": "2026-02-10T08:12:03.044999837Z",
      "last_event": "2026-02-10T10:45:00.5Z"
    },
    {
      "name": "main",
      "events": 1000,
      "bytes": 46640,
      "parts": 2,
      "first_event": "2025-01-15T08:00:00.105Z",
      "last_event": "2026-11-17T15:45:41.513196Z"
    },
    {
      "name": "nginx",
      "events": 34,
      "bytes": 2806,
      "parts": 1,
      "first_event": "2026-11-17T15:45:41.536077Z",
      "last_event": "2026-11-17T15:45:41.536077Z"
    }
  ],
  "newest_event": "2026-11-17T15:45:41Z",
  "oldest_event": "2025-01-15T08:00:00Z",
  "part_count": 4,
  "storage_bytes": 55405,
  "total_events": 1060,
  "uptime_seconds": 84
}
```

`parts` counts the immutable column files that hold an index's events on disk; see [Storage settings](../configuration/storage.md).

## indexes

```
lynxdb indexes
```

Lists the indexes with their events, bytes, parts and the times of their first and last events.

```
┌─────────┬────────┬─────────┬───────┬────────────────────────────────┬─────────────────────────────┐
│ NAME    │ EVENTS │ BYTES   │ PARTS │ FIRST                          │ LAST                        │
├─────────┼────────┼─────────┼───────┼────────────────────────────────┼─────────────────────────────┤
│ backend │     26 │  5.8 KB │     1 │ 2026-02-10T08:12:03.044999837Z │ 2026-02-10T10:45:00.5Z      │
│ main    │  1,000 │ 45.5 KB │     2 │ 2025-01-15T08:00:00.105Z       │ 2026-11-17T15:45:41.513196Z │
│ nginx   │     34 │  2.7 KB │     1 │ 2026-11-17T15:45:41.536077Z    │ 2026-11-17T15:45:41.536077Z │
└─────────┴────────┴─────────┴───────┴────────────────────────────────┴─────────────────────────────┘
(3 rows)
3 indexes
```

## jobs

```
lynxdb jobs [job_id] [flags]
```

Every query on the server runs as a job: a query that finishes within the server's synchronous wait answers inline, and a longer one answers with its job handle (`lynxdb query` waits for it). The server keeps a finished job for `query.job_ttl`. `jobs` lists the jobs, the latest first, shows one, or cancels one.

| Flag | Default | Description |
|------|---------|-------------|
| `--status` | | List only jobs in this state: `running`, `done`, `error`, `canceled` |
| `--cancel` | `false` | Cancel the job given as the argument |

```bash
lynxdb jobs
lynxdb jobs --status running
lynxdb jobs qry_4a30e10b34a4d6a5
lynxdb jobs qry_4a30e10b34a4d6a5 --cancel
```

```
┌──────────────────────┬────────┬─────────────────────────────┬─────────────────────────────────────────┐
│ ID                   │ STATUS │ CREATED                     │ QUERY                                   │
├──────────────────────┼────────┼─────────────────────────────┼─────────────────────────────────────────┤
│ qry_4a30e10b34a4d6a5 │ done   │ 2026-11-17T15:47:00.446126Z │ source=main | stats count() by host     │
│ qry_3c14688355c400cf │ done   │ 2026-11-17T15:47:00.436396Z │ source=main | stats count() by level    │
│ qry_2b36ccf3a1002827 │ done   │ 2026-11-17T15:46:16.916648Z │ source=main level=ERROR | stats count() │
└──────────────────────┴────────┴─────────────────────────────┴─────────────────────────────────────────┘
(3 rows)
```

A single job shows its state, its query, the time it has run and its error, if any. A job's progress is its state and elapsed time; a finished job also has its rows. With `--format json`, a single job prints the server's job document, results included, and the list prints one JSON object per job. Canceling a job needs a key with the `admin` or `full` scope when auth is on. Job IDs tab-complete when [shell completion](./completion.md) is set up.

## fields

```
lynxdb fields [name] [flags]
```

**Alias:** `f`

The server's field catalog: every field path that the indexes hold, with the indexes that hold it. With a field name, the indexes that hold it and its 20 most common values; with `--values`, its 50 most common values and their counts.

| Flag | Short | Description |
|------|-------|-------------|
| `--values` | | With a field name: list its top values with counts |
| `--since` | `-s` | With `--values`: only events from a relative window, such as `15m`, `1h`, `7d` |
| `--from` | | With `--values`: absolute start time (ISO 8601) |
| `--to` | | With `--values`: absolute end time (ISO 8601) |
| `--source` | | Only this index |
| `--prefix` | | Only fields whose names start with this text |

```bash
lynxdb fields
lynxdb fields --prefix st
lynxdb fields status
lynxdb fields status --values --since 1h
lynxdb fields --source nginx
lynxdb fields --format json
```

```
  status — top values

┌───────┬───────┐
│ VALUE │ COUNT │
├───────┼───────┤
│ 200   │   369 │
│ 500   │    86 │
│ 503   │    72 │
│ 404   │    71 │
└───────┴───────┘
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
