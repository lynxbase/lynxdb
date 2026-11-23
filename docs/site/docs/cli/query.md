---
sidebar_position: 2
title: query
description: Run PPL queries against a LynxDB server, local files or standard input.
---

# query

Run a [PPL](../ppl/overview.md) query against a server, or with `--file` against local files or standard input.

```
lynxdb query '<PPL query>' [flags]
lynxdb query --file <path|glob|-> '<PPL query>' [flags]
```

**Alias:** `q`

A server query starts with a source, for example `source=main`, and continues with `| command` stages:

```bash
lynxdb query 'source=main level=ERROR | stats count() as errors by host | sort - errors'
```

Quote the query with single quotes so the shell leaves `|`, `>` and double-quoted strings alone. Several arguments are joined with spaces into one query.

## Flags

| Flag | Short | Default | Modes | Description |
|------|-------|---------|-------|-------------|
| `--since` | `-s` | | all | Relative time range: `30s`, `15m`, `1h`, `7d`, `2w` |
| `--from` | | | all | Absolute start time (ISO 8601); needs `--to` |
| `--to` | | | all | Absolute end time (ISO 8601); needs `--from` |
| `--timeout` | | `6m` | server | Client-side limit for the whole query, as a Go duration: `30s`, `10m` |
| `--copy` | | `false` | server | Copy the result rows to the clipboard as TSV |
| `--file` | `-f` | | local | Load a file or glob pattern, or standard input for `-`, and query it in process |
| `--raw` | | `false` | local | Do not detect a log format while loading |
| `--source` | | file path or `stdin` | local | `source` field of the loaded text lines |
| `--sourcetype` | | | local | `sourcetype` field of the loaded text lines |
| `--output` | `-o` | | all | Write the result rows to a file instead of stdout |
| `--analyze` | | `false` | all | Print an execution profile instead of the statistics line |
| `--fail-on-empty` | | `false` | all | Exit with code 6 when the query returns no rows |
| `--explain` | | `false` | all | Print the compiled plans and exit without running the query |

`--timeout` has no effect on a local query, and `--copy` fails with an error there. `--since`, `--from` and `--to` select events by their time, in `[from, to)`; on a local query, `--since` counts back from the moment the input finished loading, so lines without a timestamp, which take their load time, fall inside it. With all three absent, a query covers all time unless it sets its own range with `earliest=` and `latest=`.

## Server mode

Without `--file`, `lynxdb query` sends the query to `POST /api/v1/query` on the server named by `--server` (see [Query API](../api/query.md)). It never reads standard input, so it behaves the same in a terminal, a pipeline, a CI step or a cron job.

```bash
lynxdb query 'source=main status>=500 | top 10 path' --since 24h
lynxdb query 'source=main | stats count() by host' \
  --from 2026-01-15T00:00:00Z --to 2026-01-16T00:00:00Z
lynxdb q 'source=main | timechart span=5m count() by level' --since 1h
```

The server syncs the events ingested so far before it runs the query, so a query sees everything ingested before it. Every query runs on the server as a job. The server answers inline when the job finishes within its `query.sync_timeout`; otherwise it answers with the job handle, and the CLI polls the job and prints the result when it finishes, until `--timeout` expires. `lynxdb jobs` lists the jobs.

When the server cannot be reached and standard input is not a terminal, the connection error also shows the same query with `--file -`, in case the input was meant to be queried.

## Local mode: files and stdin

With `--file`, the CLI runs the query without a server; `--file -` reads standard input:

1. It compiles the query first, so a query that does not compile fails before any input is read.
2. It opens a database of its own in a temporary directory and loads every line of the input into the index `main`.
3. It runs the query in process with the same engine the server uses, prints the result, and removes the directory.

```bash
lynxdb query --file access.log 'stats count() by status'
lynxdb query --file '/var/log/app/*.log' 'level=ERROR | stats count() by source'
cat app.log | lynxdb query --file - 'where response_time > 500 | stats count() by path'
zcat archive.log.gz | lynxdb 'status>=500 | top 10 path'
```

`--file` takes a path, a glob pattern or `-`; quote a glob so the shell does not expand it. `lynxdb '<query>'`, a query with no command, is short for `lynxdb query --file - '<query>'`; it fails when standard input is a terminal.

A local query may leave out the source; it then reads `main`, the index the input is loaded into:

- A query that starts with `|`, such as `| where status >= 500`, runs as `source=main | where status >= 500`.
- A query whose first word names a command, such as `stats count() by host`, runs as `source=main | stats count() by host`.
- Otherwise the leading words are search terms: `level=ERROR timeout | stats count()` runs as `search source=main level=ERROR timeout | stats count()`.

A server query has no such default, and a `|` at the start of any query must be followed by a command: `| tstats count` fails with `unknown command "tstats"`.

Lines load the way [`POST /api/v1/ingest/raw`](../api/ingest.md) takes them:

- A line that is a JSON object is stored as it is: its members become fields, and the first of the [time fields](../configuration/storage.md#event-time) it holds becomes its time. It has no `_raw`.
- Any other line becomes an event with the line in `_raw`, the timestamp found in the line as its time (the load time when there is none), its `key=value` pairs as fields, and the fields `source` (the file path, `stdin` or `--source`) and `sourcetype` (`--sourcetype`, absent by default).
- The CLI reads the first 20 lines to detect a log format. When it finds Apache/Nginx combined log format, syslog or Docker JSON logs, it extracts that format's fields from the text lines as well (for combined: `client_ip`, `ident`, `user`, `timestamp`, `request`, `method`, `uri`, `protocol`, `status`, `bytes`, `referer`, `user_agent`) and says so on stderr:

  ```
    ℹ Detected Combined (100% of lines); fields are extracted while loading. Use --raw to disable.
  ```

`--raw` turns format detection off; the timestamp and `key=value` extraction still apply. Detection runs on the first file that matches `--file` and applies to all of them. A line longer than 1 MiB stops the load with an error.

A local query prints statistics to stderr, so stdout carries only rows:

```bash
lynxdb query --file access.log --format csv 'stats count() by level' > levels.csv
```

## Plans: `--explain`

`--explain` compiles the query in the CLI and prints the logical plan, the optimized plan and the optimizer rules that fired. It reads no input and contacts no server. A local query that starts with `explain`, such as `explain level=ERROR | stats count()`, prints the same plans and reads no input either.

```bash
lynxdb query --explain 'source=main level=ERROR | stats count() as n by host | sort - n | head 3'
```

```
Logical plan:
Limit(3)
└─ Sort(n DESC NULLS LAST)
   └─ Project(n, host)
      └─ Aggregate(n = count() BY host)
         └─ Filter(level = "ERROR")
            └─ Scan(main)

Optimized plan:
TopN(3, n DESC NULLS LAST)
└─ Project(n, host)
   └─ Aggregate(n = count() BY host)
      └─ Filter(level = "ERROR")
         └─ Scan(main) columns=[_time, host, level] prune=level = "ERROR"

Optimizer rules:
  prune-scan x1
  sort-limit-to-topn x1
```

To see how the server plans a query, with its result type, cost class and the fields it reads, use [`lynxdb explain`](./explain.md). [Pipeline execution](../ppl/pipeline-execution.md) explains the operators in these plans.

## Profiles: `--analyze`

`--analyze` prints an execution profile to stderr after the rows, in place of the statistics line: the time spent in each phase and the total, and for a local query the rows loaded and returned.

```bash
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze
lynxdb query --file access.log 'level=ERROR | stats count() by host' --analyze
```

```
  ══════ Query Profile ══════

  Phase Breakdown:
    parse             1µs     0%   █
    exec            187µs     0%   ████████████████████
    serialize        80µs     0%   ████████
                 ──────────
    total            39ms

  Rows:  1,000 scanned  →  5 results
```

With `--format json`, the profile is printed as JSON.

## Errors

A query that does not compile prints the query with a caret under the position and the compiler's message, then exits with code 4:

```bash
lynxdb query --file access.log 'source=main | where status >'
```

```
  source=main | where status >
                              ^

  ✖ ERROR: syntax error at line 1, column 29: expected an expression
```

Unsupported commands and functions fail the same way, with a message that names what is missing. [PPL errors](../ppl/errors.md) lists every error code. [Exit codes](./overview.md#exit-codes) lists the other failures.

## Output

Rows go to stdout in the format chosen by `--format`; statistics, hints and warnings go to stderr.

A query can come back with warnings: what the query could not know to be an error, such as a field that none of the indexes it reads holds. The CLI prints each on its own `⚠` line after the rows, in server and local mode, unless `--quiet` is set. Warnings do not change the exit code.

- On a terminal, `auto` prints a table, or a bare value for a single-column, single-row result, followed by a footer with the row count and the time the query took. `--verbose` adds the full statistics block.
- When stdout is not a terminal, `auto` prints one JSON object per row and the statistics as one JSON line on stderr. `--no-stats` drops the statistics.

```bash
lynxdb query 'source=main | stats count() by host' --format csv > hosts.csv
lynxdb query 'source=main level=ERROR | fields _time, host, path' --since 1h | jq -r .path
lynxdb query 'source=main | stats count() as n by status | sort - n' --format table
```

[Output Formats](./output-formats.md) describes each format.

## Scripting

```bash
# Fail a CI step when errors appear
lynxdb query 'source=main level=ERROR' --since 15m --fail-on-empty --quiet > /dev/null \
  && echo "errors in the last 15 minutes"

# Pull rows from the server, then aggregate them locally
lynxdb query 'source=main status>=500 | fields _raw' --since 1h --format raw \
  | lynxdb 'stats count() by path'

# Copy a result to paste into a spreadsheet
lynxdb query 'source=main | stats count() by host' --copy
```

## See also

- [explain](./explain.md) for server-side plans
- [Search semantics](../ppl/search-and-types.md) for how `source=main error` and `field=value` terms match
- [PPL command guide](../ppl/command-guide.md)
- [Output Formats](./output-formats.md)
