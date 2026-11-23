---
title: Scripting with LynxDB
description: Use LynxDB in shell scripts, Unix pipelines, CI jobs and scheduled reports, with JSON lines, CSV, jq and exit codes.
---

# Scripting with LynxDB

`lynxdb query` behaves like a Unix filter. When stdout is not a terminal it
prints one JSON object per line, writes statistics and hints to stderr, and
reports the outcome through its exit code. This guide collects patterns for
scripts, pipelines and CI jobs.

`lynxdb query '<query>'` always asks the server, also under cron, CI or ssh,
where standard input is not a terminal. Local input is queried only when the
command says so, with `--file`.

## Pipe mode: no server required

The CLI queries local files and standard input in process. `--file -`
reads standard input, and `lynxdb '<query>'` is short for it. A local query
that names no source reads `main`, the index the input is loaded into:

```bash
# Any log source through PPL
cat /var/log/app.log | lynxdb query --file - 'level=ERROR | stats count() by service'

# A local file
lynxdb query --file access.log 'stats count() by status'

# Several files
lynxdb query --file '/var/log/nginx/*.log' 'status>=500 | top 10 uri'

# Compressed input
zcat archive.log.gz | lynxdb 'where status >= 500 | top 10 path'

# kubectl output
kubectl logs deploy/api --since=1h | lynxdb 'stats avg(duration_ms), p99(duration_ms) by endpoint'
```

See [Pipe Mode](../getting-started/pipe-mode.md) for format detection and
loading details.

## Output formats

`--format` (`-F`) picks the output format:

| Format | Output |
|--------|--------|
| `auto` | Table on a terminal, JSON lines otherwise (default) |
| `json`, `ndjson` | One JSON object per line |
| `table`, `markdown` | Tables |
| `csv`, `tsv` | Header row, then one line per row |
| `raw` | The `_raw` text of each row |

See [Output Formats](../cli/output-formats.md) for the full list.

### Data on stdout, everything else on stderr

Rows go to stdout. The statistics footer and hints go to stderr, so
redirection captures only data:

```bash
lynxdb query --file access.log 'stats count()' > result.json
```

On a terminal the statistics are a summary footer; otherwise they are one
JSON line on stderr. `--no-stats` drops them and `--quiet` (`-q`) drops all
non-data output.

## Integration with jq

`json` output is one object per line, the shape `jq` reads by default.

```bash
# One field
lynxdb query 'source=main | stats count() as n by path | sort - n | head 10' | jq -r '.path'

# Filter rows
lynxdb query 'source=main | stats count() as n by path' | jq 'select(.n > 100)'

# Reshape
lynxdb query 'source=main | stats count() as n by host' | jq '{host, events: .n}'

# Sum across rows
lynxdb query 'source=main | stats count() as n by status' | jq -s 'map(.n) | add'
```

## CSV export

```bash
# A report file
lynxdb query 'source=main | stats count() as n, avg(response_time) as avg_ms by path | sort - n' \
  --format csv > report.csv

# Sort with standard tools
lynxdb query --file access.log 'stats count() as n by status' --format csv | sort -t, -k2 -rn

# Tab-separated
lynxdb query 'source=main | stats count() by host' --format tsv > data.tsv
```

## Chain queries

Pull rows from the server, then aggregate them locally in pipe mode:

```bash
lynxdb query 'source=main status>=500 | fields host, path, status' --since 1h --format ndjson \
  | lynxdb 'stats count() as errors by path | sort - errors | head 5'
```

## Write results to a file

Use shell redirection:

```bash
lynxdb query 'source=main level=ERROR' --since 24h --format json > errors.json
lynxdb query 'source=main level=ERROR' --since 24h --format csv > errors.csv
```

`--output <file>` writes the rows to a file instead; set `--format` with
it, since `auto` picks the format from the terminal, not from the file.

## CI/CD

### Fail on matching events

`--fail-on-empty` exits with code 6 when a query returns no rows. Turn it
around to fail a job when errors do appear:

```bash
if lynxdb query 'source=main level=FATAL | where source = "api"' --since 10m --fail-on-empty --quiet > /dev/null; then
  echo "FATAL events after deployment"
  exit 1
fi
echo "No fatal events"
```

### Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Other failure |
| 2 | Invalid flags or missing arguments |
| 3 | The server is unreachable |
| 4 | The query does not compile |
| 5 | The query timed out |
| 6 | No rows, with `--fail-on-empty` |
| 7 | Missing or invalid API key |
| 130 | Interrupted with Ctrl+C |

```bash
lynxdb query 'source=main level=FATAL' --since 1h --fail-on-empty --quiet > /dev/null
case $? in
  0) echo "fatal events found" ;;
  6) echo "no fatal events" ;;
  3) echo "server unreachable" ;;
  4) echo "query does not compile" ;;
  *) echo "query failed" ;;
esac
```

See [CLI overview](../cli/overview.md#exit-codes) for the complete list.

### Post-deployment smoke test

```bash
#!/bin/bash
# post-deploy-check.sh
set -euo pipefail

sleep 60

ERRORS=$(lynxdb query 'source=main level=ERROR | where source = "api" | stats count() as n' --since 2m --no-stats | jq '.n')
FATAL=$(lynxdb query 'source=main level=FATAL | where source = "api" | stats count() as n' --since 2m --no-stats | jq '.n')

echo "errors=$ERRORS fatal=$FATAL"

if [ "$FATAL" -gt 0 ] || [ "$ERRORS" -gt 50 ]; then
  echo "rolling back"
  exit 1
fi
```

### Scheduled reports

```bash
#!/bin/bash
# daily-report.sh, run from cron: 0 9 * * * /opt/scripts/daily-report.sh
DATE=$(date +%Y-%m-%d)
lynxdb query 'source=main level=ERROR | stats count() as n by source | sort - n' \
  --since 24h --format csv --no-stats > "/reports/errors-${DATE}.csv"
```

## Quiet and plain output

```bash
# Only data
lynxdb query 'source=main level=ERROR | stats count()' --quiet

# No statistics footer
lynxdb query 'source=main level=ERROR | stats count()' --no-stats

# No colors
NO_COLOR=1 lynxdb query 'source=main level=ERROR | stats count()'
```

## Connection settings

Set these once instead of passing flags to every command:

```bash
export LYNXDB_SERVER=https://lynxdb.example.com:3100
export LYNXDB_TOKEN=lynx_ak_your_key_here
export LYNXDB_PROFILE=production
```

A `.lynxdbrc` file in the working directory or a parent sets project
defaults (`profile`, `default_format`, `default_since`, `default_source`);
it cannot set the server. See [CLI overview](../cli/overview.md) and
[Environment variables](../configuration/environment-variables.md).

## Examples

### Alert on an error rate

```bash
#!/bin/bash
RATE=$(lynxdb query 'source=main
  | stats count() as total, count(eval(status >= 500)) as errors
  | eval rate = round(errors * 100.0 / total, 2)' \
  --since 5m --no-stats | jq -r '.rate')

if (( $(echo "$RATE > 5.0" | bc -l) )); then
  curl -X POST "https://hooks.slack.com/services/..." \
    -d "{\"text\": \"5xx rate is ${RATE}%\"}"
fi
```

### One query, several formats

```bash
#!/bin/bash
QUERY='source=main | stats count() as n, avg(response_time) as avg_ms, p99(response_time) as p99_ms by path | sort - n | head 20'

lynxdb query "$QUERY" --since 24h --format csv > report.csv
lynxdb query "$QUERY" --since 24h --format json > report.json
lynxdb query "$QUERY" --since 24h --format table
```

## Next steps

- [Pipe Mode](../getting-started/pipe-mode.md)
- [Output Formats](../cli/output-formats.md)
- [CLI: `query`](../cli/query.md)
- [CLI overview](../cli/overview.md)
