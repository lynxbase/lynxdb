---
sidebar_position: 3
title: Pipe Mode (No Server)
description: Query local files and standard input with LynxDB without running a server.
---

# Pipe Mode (No Server)

`lynxdb query` runs without a server when you give it a file or pipe data
into it. It works like `grep` or `jq`: read the input, run a
[PPL](../ppl/overview.md) query, print the result, exit.

## How it works

With `--file` or piped standard input, the CLI:

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

No daemon or config file is involved. Queries start with `source=main`, as
on a server.

## Query local files

```bash
# One file
lynxdb query --file access.log 'source=main | stats count() by status'

# A glob (quote it so the shell does not expand it)
lynxdb query --file '/var/log/nginx/*.log' 'source=main status>=500 | top 10 uri'
```

A text line's `source` field is the path of the file it came from;
`--source` overrides it and `--sourcetype` sets `sourcetype`. JSON lines keep
their own fields only.

## Pipe from standard input

```bash
# Kubernetes logs
kubectl logs deploy/api | lynxdb query 'source=main | stats avg(duration_ms), p99(duration_ms) by service'

# Docker logs
docker logs myapp 2>&1 | lynxdb query 'source=main OOM | fields _time, _raw | head 20'

# Compressed files
zcat archive.log.gz | lynxdb query 'source=main | where status >= 500 | top 10 path'

# Pre-filter with grep
grep "2026-01-15" /var/log/app.log | lynxdb query 'source=main | stats count() by level'
```

Piped text lines get the `source` `stdin` unless `--source` is set. `--file` and
piped input cannot be combined in one command.

Piping into `lynxdb` with no command prints the first ten events, which is a
quick way to see which fields were extracted:

```bash
cat app.log | lynxdb
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
lynxdb query --file access.log 'source=main | stats count() as n by status' --format csv | sort -t, -k1 -rn

# JSON lines into jq
lynxdb query --file app.json 'source=main | stats count() as n by endpoint' --format json | jq '.n'

# Pull rows from a server, aggregate them locally
lynxdb query 'source=main status>=500 | fields host, path, status' --since 1h --format ndjson \
  | lynxdb query 'source=main | stats count() as errors by path | sort - errors'

# Write the result to a file
lynxdb query --file access.log 'source=main level=ERROR' --format json --output errors.json
```

## Output

The default format depends on where stdout goes:

- **Terminal:** a table (or a single value for a one-cell result), followed
  by a short statistics footer on stderr.
- **Pipe or file:** one JSON object per line. The statistics go to stderr as
  JSON; `--no-stats` drops them.

```bash
# Table on the terminal
lynxdb query --file access.log 'source=main | stats count() by status'

# JSON lines for jq
lynxdb query --file access.log 'source=main | stats count() by status' --no-stats | jq .
```

`--format` picks any other format: `table`, `csv`, `tsv`, `vertical`,
`markdown`, `raw` and more. See [Output Formats](../cli/output-formats.md).

## See the plan

`--explain` compiles the query and prints its logical plan, optimized plan
and the optimizer rules that fired, without reading any input or contacting
a server:

```bash
lynxdb query --explain 'source=main level=ERROR | stats count() as errors by host | sort - errors | head 3'
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
tail -n 100000 /var/log/app.log | lynxdb query '
  source=main level=ERROR
  | stats count() as n by msg
  | sort - n
  | head 10'
```

### Error rates of an nginx access log

```bash
lynxdb query --file /var/log/nginx/access.log '
  source=main
  | stats count() as total, count(eval(status >= 500)) as errors by uri
  | eval error_pct = round(errors * 100.0 / total, 1)
  | where error_pct > 5
  | sort - error_pct
  | head 20'
```

### Kubernetes pods

```bash
kubectl logs -l app=api --all-containers --since=1h | lynxdb query '
  source=main
  | stats count() as total, count(eval(level = "error")) as errors by service
  | eval error_pct = round(errors * 100.0 / total, 1)
  | sort - error_pct'
```

## Next steps

- [Server Mode](./server-mode.md): persistent storage
- [Your First Query](./first-query.md): PPL basics
- [Scripting with LynxDB](../guides/scripting-and-pipes.md): more piping patterns
