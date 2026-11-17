---
sidebar_position: 2
title: query
description: Run PPL queries against a LynxDB server, local files or standard input.
---

# query

Run a [PPL](../ppl/overview.md) query against a server, local files or standard input.

```
lynxdb query '<PPL query>' [flags]
```

**Alias:** `q`

Every query starts with a source, for example `source=main`, and continues with `| command` stages:

```bash
lynxdb query 'source=main level=ERROR | stats count() as errors by host | sort - errors'
```

Quote the query with single quotes so the shell leaves `|`, `>` and double-quoted strings alone. Several arguments are joined with spaces into one query.

## Flags

| Flag | Short | Default | Modes | Description |
|------|-------|---------|-------|-------------|
| `--since` | `-s` | | server | Relative time range: `15m`, `1h`, `7d` |
| `--from` | | | server | Absolute start time (ISO 8601); needs `--to` |
| `--to` | | | server | Absolute end time (ISO 8601); needs `--from` |
| `--timeout` | | `6m` | server | Client-side limit for the whole query: `30s`, `10m` |
| `--copy` | | `false` | server | Copy the result rows to the clipboard as TSV |
| `--no-lint` | | `false` | server | Ask the server not to return advisory lints |
| `--no-suggestions` | | `false` | server | Ask the server not to return advisory suggestions |
| `--file` | `-f` | | local | Load a file or glob pattern and query it in process |
| `--raw` | | `false` | local | Do not detect a log format while loading |
| `--source` | | file path or `stdin` | local | `_source` value of the loaded events |
| `--sourcetype` | | | local | `_sourcetype` value of the loaded events |
| `--max-memory` | | | local | Memory limit for the in-process query: `512mb`, `1gb` |
| `--output` | `-o` | | all | Write the result rows to a file instead of stdout |
| `--analyze` | | | all | Print an execution profile: `basic` (bare flag), `full`, `trace` |
| `--fail-on-empty` | | `false` | all | Exit with code 6 when the query returns no rows |
| `--explain` | | `false` | all | Print the compiled plans and exit without running the query |

Flags marked "server" have no effect on a local query, and `--copy` fails with an error there. With `--since`, `--from` and `--to` absent, a server query covers all time unless the query sets its own range with `earliest=` and `latest=`.

A `\G` at the end of the query switches `--format auto` to vertical output, one field per line:

```bash
lynxdb query 'source=main level=ERROR | head 1 \G'
```

## Server mode

Without `--file` and with stdin attached to a terminal, `lynxdb query` sends the query to `POST /api/v1/query` on the server named by `--server` (see [Query API](../api/query.md)).

```bash
lynxdb query 'source=main status>=500 | top 10 path' --since 24h
lynxdb query 'source=main | stats count() by host' \
  --from 2026-01-15T00:00:00Z --to 2026-01-16T00:00:00Z
lynxdb q 'source=main | timechart span=5m count() by level' --since 1h
```

The server answers small queries inline. A query that runs longer than the server's `query.sync_timeout` becomes an asynchronous job; the CLI polls the job and prints the result when it finishes, until `--timeout` expires. `lynxdb jobs` lists those jobs.

Advisory lints from the server, such as a search over every index (`source=*`) on a server with many sources, print to stderr after the rows. `--no-lint` turns them off.

The CLI picks local mode whenever stdin is a pipe. A script that runs under a pipe, such as a CI step, keeps server mode by redirecting stdin: `lynxdb query '...' < /dev/null`.

## Local mode: files and stdin

With `--file`, or when stdin is a pipe, the CLI runs the query without a server:

1. It compiles the query first, so a query that does not compile fails before any input is read.
2. It loads every line of the input as one event into an in-memory index named `main`.
3. It runs the compiled plan in process with the same physical operators the server uses, then prints the result and exits.

```bash
lynxdb query --file access.log 'source=main | stats count() by status'
lynxdb query --file '/var/log/app/*.log' 'source=main level=ERROR | stats count() by _source'
cat app.log | lynxdb query 'source=main | where response_time > 500 | stats count() by path'
zcat archive.log.gz | lynxdb query 'source=main status>=500 | top 10 path'
```

`--file` takes a path or a glob pattern; quote a glob so the shell does not expand it. Using `--file` and piped stdin together is an error.

While loading, the CLI extracts fields from each line:

- A JSON object line yields its fields.
- `key=value` pairs in a line become fields.
- A recognized timestamp in the line becomes `_time`; a line without one gets the load time.
- The CLI reads the first 20 lines to detect a log format. When it finds Apache/Nginx combined log format, syslog or Docker JSON logs, it extracts that format's fields as well (for combined: `client_ip`, `ident`, `user`, `timestamp`, `request`, `method`, `uri`, `protocol`, `status`, `bytes`, `referer`, `user_agent`) and says so on stderr:

  ```
    ℹ Detected Combined (100% of lines); fields are extracted while loading. Use --raw to disable.
  ```

`--raw` turns format detection off. JSON and `key=value` extraction still apply. Detection runs on the first file that matches `--file` and applies to all of them.

Each loaded event carries `index=main`, `_source` (the file path, `stdin`, or `--source`) and `_sourcetype` (`--sourcetype`, empty by default). A line longer than 1 MiB stops the load with an error.

A local query prints statistics to stderr, so stdout carries only rows:

```bash
lynxdb query --file access.log --format csv 'source=main | stats count() by level' > levels.csv
```

## Plans: `--explain`

`--explain` compiles the query in the CLI and prints the logical plan, the optimized plan and the optimizer rules that fired. It reads no input and contacts no server.

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

To see how the server plans a query against its stored data, with cost and scan hints, use [`lynxdb explain`](./explain.md). [Pipeline execution](../ppl/pipeline-execution.md) explains the operators in these plans.

## Profiles: `--analyze`

`--analyze` prints an execution profile to stderr after the rows: phase timings, segments scanned and skipped, rows scanned, optimizer rules, memory and CPU, and recommendations. A bare `--analyze` means `basic`; `full` adds per-stage detail and `trace` adds per-segment detail.

```bash
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze full --since 1h
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

Rows go to stdout in the format chosen by `--format`; statistics, hints and lints go to stderr.

- On a terminal, `auto` prints a table, or a bare value for a single-column, single-row result, followed by a summary of rows, time, rows scanned, segments skipped and bytes read. `--verbose` adds the full statistics block.
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
lynxdb query 'source=main level=ERROR' --since 15m --fail-on-empty --quiet < /dev/null > /dev/null \
  && echo "errors in the last 15 minutes"

# Pull rows from the server, then aggregate them locally
lynxdb query 'source=main status>=500 | fields _raw' --since 1h --format raw \
  | lynxdb query 'source=main | stats count() by path'

# Copy a result to paste into a spreadsheet
lynxdb query 'source=main | stats count() by host' --copy
```

## See also

- [explain](./explain.md) for server-side plans
- [Search semantics](../ppl/search-and-types.md) for how `source=main error` and `field=value` terms match
- [PPL command guide](../ppl/command-guide.md)
- [Output Formats](./output-formats.md)
