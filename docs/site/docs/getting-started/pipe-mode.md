---
sidebar_position: 3
title: Pipe Mode (No Server)
description: Query local files and standard input with LynxDB without running a server.
---

# Pipe Mode (No Server)

`lynxdb query --file` runs a query over local files without a server;
`--file -` reads standard input. It works like `grep` or `jq`: read the
input, run a [PPL](../ppl/overview.md) query, print the result, exit.
Without `--file`, `lynxdb query` always sends the query to a server.

## How it works

With `--file`, the CLI:

1. Compiles the query first, so a bad query fails before any input is read.
2. Opens a database in a temporary directory and loads every line into the
   index `main`.
3. Keeps a line that is a JSON object as it is, with its members as fields.
   Any other line becomes an event holding the text in `_raw`, with its
   timestamp, its `key=value` pairs, and the fields of combined access logs
   and syslog lines extracted. The timestamp is the first time near the
   start of the line in RFC 3339, syslog or access log form
   (`2026-01-15T08:00:00.1Z`, `Jan 15 08:00:00`,
   `15/Jan/2026:08:00:00 +0000`); see
   [raw ingest](../api/ingest.md#post-ingestraw) for the forms.
4. Runs the same compiled plan a server would run, prints the rows, deletes
   the temporary directory and exits.

No daemon or config file is involved. A local query may leave out the
source, and then reads `main`:

| Query | Runs as |
|-------|---------|
| `where status >= 500 \| stats count()` | `source=main \| where status >= 500 \| stats count()` |
| `\| where status >= 500` | `source=main \| where status >= 500` |
| `level=ERROR timeout \| stats count()` | `search source=main level=ERROR timeout \| stats count()` |

A query whose first word names a command reads `main` and runs that command;
otherwise its leading words are search terms. A query that names a source,
such as `source=main level=ERROR`, runs as written.

## Query local files

```bash
# One file
lynxdb query --file access.log 'stats count() by status'

# A glob (quote it so the shell does not expand it)
lynxdb query --file '/var/log/nginx/*.log' 'status>=500 | top 10 uri'
```

A text line's `source` field is the path of the file it came from;
`--source` overrides it and `--sourcetype` sets `sourcetype`. JSON lines keep
their own fields only.

`--since`, `--from` and `--to` select events by their time, as on a server:

```bash
lynxdb query --file access.log --from 2025-01-15T08:00:00Z --to 2025-01-15T09:00:00Z 'level=ERROR | stats count() by host'
```

## Pipe from standard input

`--file -` reads standard input. `lynxdb '<query>'`, with a query and no
command, is the short form:

```bash
# Kubernetes logs
kubectl logs deploy/api | lynxdb query --file - 'stats avg(duration_ms), p99(duration_ms) by service'

# Docker logs
docker logs myapp 2>&1 | lynxdb 'OOM | fields _time, _raw | head 20'

# Compressed files
zcat archive.log.gz | lynxdb 'where status >= 500 | top 10 path'

# Pre-filter with grep
grep "2026-01-15" /var/log/app.log | lynxdb 'stats count() by level'
```

Piped text lines get the `source` `stdin` unless `--source` is set. The short
form fails when standard input is a terminal, and `lynxdb` with no arguments
prints a welcome message without reading standard input.

`head` shows which fields were extracted:

```bash
head -5 access.log | lynxdb 'head 1' --format json --no-stats
```

```json
{"_raw":"2025-01-15T08:00:00.105Z host=web-03 level=ERROR method=GET path=/api/v1/users status=502 user=grace response_time=786 bytes=32228 msg=\"GET /api/v1/users completed\"","_time":"2025-01-15T08:00:00.105Z","bytes":32228,"host":"web-03","level":"ERROR","method":"GET","msg":"GET /api/v1/users completed","path":"/api/v1/users","response_time":786,"source":"stdin","status":502,"user":"grace"}
```

## Format detection

The CLI samples the first 20 lines. When at least 70% of them are combined
access log or syslog lines, it prints a `Detected ...` notice on stderr and
extracts that format's fields:

| Format | Fields |
|--------|--------|
| Combined access log | `client_ip`, `ident`, `user`, `timestamp`, `request`, `method`, `uri`, `protocol`, `status`, `bytes`, `referer`, `user_agent` |
| Syslog (RFC 3164 and RFC 5424 with `<PRI>`) | `_time` and `host` from the header, `facility`, `severity`, `facility_label`, `severity_label`, `level`, `app_name`, `procid`, `msgid`, `message`, and `sd_<SD-ID>_<param>` for structured data, as the [syslog receiver](../configuration/syslog.md#extracted-fields) extracts them; an RFC 3164 time is read in UTC and the current year |

A JSON line keeps its fields whatever the detected format; a Docker
`json-file` line, for example, yields `log`, `stream` and `time`, and members
of nested objects become fields named like `http.path`. `--raw` turns off
format detection. Anything
else is extracted at query time with `parse`, `rex`, `grok` or `spath`; see
[Field Extraction](../guides/field-extraction.md).

## Combine with Unix tools

Results go to stdout; the statistics footer and hints go to stderr.

```bash
# CSV, sorted by the shell
lynxdb query --file access.log 'stats count() as n by status' --format csv | sort -t, -k1 -rn

# JSON lines into jq
lynxdb query --file app.json 'stats count() as n by endpoint' --format json | jq '.n'

# Pull rows from a server, aggregate them locally
lynxdb query 'source=main status>=500 | fields host, path, status' --since 1h --format ndjson \
  | lynxdb 'stats count() as errors by path | sort - errors'

# Write the result to a file
lynxdb query --file access.log 'level=ERROR' --format json --output errors.json
```

## Output

The default format depends on where stdout goes:

- **Terminal:** a table (or a single value for a one-cell result), followed
  by a short statistics footer on stderr.
- **Pipe or file:** one JSON object per line. The statistics go to stderr as
  JSON; `--no-stats` drops them.

```bash
# Table on the terminal
lynxdb query --file access.log 'stats count() by status'

# JSON lines for jq
lynxdb query --file access.log 'stats count() by status' --no-stats | jq .
```

`--format` picks any other format: `table`, `csv`, `tsv`, `vertical`,
`markdown`, `raw` and more. See [Output Formats](../cli/output-formats.md).

## See the plan

`--explain` compiles the query and prints its logical plan, optimized plan
and the optimizer rules that fired, without reading any input or contacting
a server:

```bash
lynxdb query --file access.log --explain 'level=ERROR | stats count() as errors by host | sort - errors | head 3'
```

```text
Logical plan:
Limit(3)
└─ Sort(errors DESC NULLS LAST)
   └─ Project(errors, host)
      └─ Aggregate(errors = count() BY host)
         └─ Filter(level = "ERROR")
            └─ Scan(main)

Optimized plan:
TopN(3, errors DESC NULLS LAST)
└─ Project(errors, host)
   └─ Aggregate(errors = count() BY host)
      └─ Filter(level = "ERROR")
         └─ Scan(main) columns=[_time, host, level] prune=level = "ERROR"

Optimizer rules:
  prune-scan x1
  sort-limit-to-topn x1
```

## Examples

### Find the most common errors

```bash
tail -n 100000 /var/log/app.log | lynxdb '
  level=ERROR
  | stats count() as n by msg
  | sort - n
  | head 10'
```

### Error rates of an nginx access log

```bash
lynxdb query --file /var/log/nginx/access.log '
  stats count() as total, count(eval(status >= 500)) as errors by uri
  | eval error_pct = round(errors * 100.0 / total, 1)
  | where error_pct > 5
  | sort - error_pct
  | head 20'
```

### Kubernetes pods

```bash
kubectl logs -l app=api --all-containers --since=1h | lynxdb '
  stats count() as total, count(eval(level = "error")) as errors by service
  | eval error_pct = round(errors * 100.0 / total, 1)
  | sort - error_pct'
```

## Next steps

- [Server Mode](./server-mode.md): persistent storage
- [Your First Query](./first-query.md): PPL basics
- [Scripting with LynxDB](../guides/scripting-and-pipes.md): more piping patterns
