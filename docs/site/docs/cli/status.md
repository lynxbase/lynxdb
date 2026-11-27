---
sidebar_position: 7
title: lynxdb status, fields and jobs
sidebar_label: status, fields, jobs & more
description: Inspect a LynxDB server - status and health, indexes, query jobs, the field catalog and the CLI version.
---

# status, fields, jobs & more

These commands use the server named by `--server`. With `--format json` or `ndjson`, and when stdout is not a terminal, `status` and a single field or job print one JSON object, and the lists (`indexes`, `partitions list`, `fields`, `jobs`, `auth list`, `config list-profiles`) print one JSON object per item, keyed by lowercase snake_case names. `table` and `markdown` print tables, and `csv` and `tsv` rows under the table headers.

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

With `--format json` or `ndjson`, and when stdout is not a terminal, it prints one JSON object:

```bash
lynxdb status --format json
```

```json
{"uptime_seconds":1,"storage_bytes":66407,"total_events":1002,"index_count":2,"part_count":3,"oldest_event":"2025-01-15T08:00:00Z","newest_event":"2026-11-23T11:19:25Z","health":"healthy","indexes":[{"name":"main","events":1000,"bytes":65772,"parts":2,"first_event":"2025-01-15T08:00:00.105Z","last_event":"2026-11-23T11:19:25.207039Z"},{"name":"web","events":2,"bytes":635,"parts":1,"first_event":"2026-01-15T08:00:00Z","last_event":"2026-01-15T08:00:05Z"}]}
```

`parts` counts the immutable column files that hold an index's events on disk; see [Storage settings](../configuration/storage.md).

## indexes

```
lynxdb indexes
```

Lists the indexes with their events, bytes, parts and the times of their first and last events. In JSON, each index is an object with the keys `name`, `events`, `bytes`, `parts`, `first_event` and `last_event`:

```bash
lynxdb indexes --format json
```

```json
{"name":"main","events":1000,"bytes":65772,"parts":2,"first_event":"2025-01-15T08:00:00.105Z","last_event":"2026-11-23T11:19:25.207039Z"}
{"name":"web","events":2,"bytes":635,"parts":1,"first_event":"2026-01-15T08:00:00Z","last_event":"2026-01-15T08:00:05Z"}
```

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

## partitions

List the event-time partitions of an index to see how much space each one uses.

```bash
lynxdb partitions list web --format json
```

Each row gives `start` (inclusive), `end` (exclusive), `events`, `bytes` and `parts`. Use the exact `start` from the list when dropping or detaching a partition. A date such as `2026-10-01` means midnight UTC. Listing requires `query` or `admin` scope when authentication is on. Changes require `admin` scope. The CLI waits for the operation to finish. Set `--timeout` to bound the wait, for example `--timeout 30m`.

To move a partition, detach it into a new directory on the source server. Copy that directory to the destination server, then attach it there.

```bash
lynxdb --server https://source.example.org partitions detach web 2026-10-01 --to /mnt/archive/web-20261001
lynxdb --server https://destination.example.org partitions attach web /mnt/archive/web-20261001
```

Use absolute server paths outside the data directory. The parent of the detach directory must exist. Attach keeps the archive and requires an existing index with matching storage settings and an empty partition for that time span. If late events have filled the span since detach, attach returns a conflict. An attached partition outside the retention window expires on the next [retention pass](../operations/retention.md).

To permanently remove a partition, use `drop`. The command asks for confirmation when standard input is a terminal. Pass `--yes` to skip the prompt.

```bash
lynxdb partitions drop web 2026-10-01
```

Drop, detach and attach print the affected partition's totals. New queries see the change immediately. Queries already reading a removed partition finish with their original data, and LynxDB reclaims its files once those readers finish.

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

A single job shows its state, its query, the time it has run and its error, if any. A job's progress is its state and elapsed time; a finished job also has its rows. In JSON, a single job prints the server's job document, results included, and the list prints one object per job with the keys `job_id`, `status`, `query` and `created_at`. Canceling a job needs a key with the `admin` or `full` scope when auth is on. Job IDs tab-complete when [shell completion](./completion.md) is set up.

## fields

```
lynxdb fields [name] [flags]
```

**Alias:** `f`

The server's field catalog: every field path that the indexes hold, with the indexes that hold it. With a field name, the indexes that hold it and its 20 most common values; with `--values`, its 50 most common values and their counts.

| Flag | Short | Description |
|------|-------|-------------|
| `--values` | | With a field name: list its top values with counts |
| `--since` | `-s` | With `--values`: only events from this time until now, such as `15m`, `-1h` or `@d` |
| `--from` | | With `--values`: start of the time range, inclusive |
| `--to` | | With `--values`: end of the time range, exclusive |
| `--source` | | Only this index |
| `--prefix` | | Only fields whose names start with this text |

`--since`, `--from` and `--to` take the forms and follow the rules of [the time range of `query`](./query.md#time-range): `--from` or `--to` works alone, and `--since` with either is a usage error.

```bash
lynxdb fields
lynxdb fields --prefix st
lynxdb fields status
lynxdb fields status --values --since 1h
lynxdb fields status --values --from -1d@d --to @d
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
