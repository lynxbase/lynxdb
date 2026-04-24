---
title: Scripting with LynxDB
description: How to use LynxDB in scripts, Unix pipelines, CI/CD workflows, and automated reporting with CSV, NDJSON, and jq integration.
---

# Scripting with LynxDB

LynxDB is designed to work as a composable Unix tool. When stdout is not a terminal, it outputs plain NDJSON by default, making it easy to pipe results into `jq`, `awk`, `sort`, or any other tool. This guide covers scripting patterns, output formats, CI/CD integration, and automation.

## Pipe mode: no server required

LynxDB can query local files and stdin without a running server. This makes it a powerful replacement for `grep` + `awk` in log analysis pipelines:

```bash
# Pipe any log source through SPL2
cat /var/log/syslog | lynxdb query '| where level="ERROR" | stats count by service'

# Query a local file
lynxdb query --file access.log '| stats count by status'

# Glob multiple files
lynxdb query --file '/var/log/nginx/*.log' '| where status>=500 | top 10 uri'

# Decompress and query
zcat archive.log.gz | lynxdb query '| where status>=500 | top 10 path'

# Process kubectl output
kubectl logs deploy/api --since=1h | lynxdb query '| stats avg(duration_ms), p99(duration_ms) by endpoint'

# Docker logs
docker logs myapp 2>&1 | lynxdb query '| search "OOM" | stats count by container'
```

See the [pipe mode guide](/docs/getting-started/pipe-mode) for more details on serverless usage.

---

## Output formats

Control the output format with the `--format` / `-F` flag:

| Format | Flag | Description |
|--------|------|-------------|
| Auto | `--format auto` | JSON in TTY, NDJSON when piped (default) |
| JSON | `--format json` | Pretty-printed JSON |
| NDJSON | `--format ndjson` | One JSON object per line |
| Table | `--format table` | Aligned columns with headers |
| CSV | `--format csv` | RFC 4180 CSV with header row |
| TSV | `--format tsv` | Tab-separated values with header row |
| Raw | `--format raw` | `_raw` field value per line |

### Automatic format detection

When stdout is a terminal (TTY), LynxDB shows colorized interactive output. When stdout is piped (non-TTY), it switches to plain NDJSON automatically. You never need to configure this for simple pipelines.

### Metadata goes to stderr

Summary lines and query statistics are written to stderr, so they never pollute piped output:

```bash
# Only JSON goes to the file; stats go to the terminal
lynxdb query --file access.log '| stats count' > result.json
# stderr shows: Scanned 50,000 events | 1 results | 89ms
```

---

## Integration with jq

[jq](https://jqlang.github.io/jq/) is the most common companion tool for LynxDB output.

### Extract a single field

```bash
lynxdb query '_source=nginx | stats count by uri | sort -count | head 10' | jq '.uri'
```

### Filter results

```bash
lynxdb query '_source=nginx | stats count by uri' | jq 'select(.count > 1000)'
```

### Transform to a different shape

```bash
lynxdb query '| stats count by source' | jq '{source: .source, events: .count}'
```

### Aggregate in jq

```bash
lynxdb query '_source=nginx | stats count by status' | jq -s 'map(.count) | add'
```

### Pretty-print

```bash
lynxdb query '| stats count by level' | jq .
```

---

## CSV export

Export query results as CSV for spreadsheets, data tools, or further processing:

```bash
# Export to CSV file
lynxdb query '_source=nginx | stats count, avg(duration_ms) by uri | sort -count' \
  --format csv > report.csv

# Open in a spreadsheet
lynxdb query 'level=error | stats count by source, level' --format csv | open -f

# Pipe to standard Unix tools
lynxdb query --file access.log '| stats count by status' --format csv | sort -t, -k2 -rn
```

### TSV export

```bash
lynxdb query '| stats count by source' --format tsv > data.tsv
```

---

## Chain LynxDB commands

Pipe the output of one LynxDB query into another:

```bash
# Query server, then post-process locally
lynxdb query '_source=nginx status>=500' --since 1h \
  | lynxdb query '| stats count by uri | sort -count | head 5'
```

The first query fetches data from the server. The second query processes the results locally using pipe mode.

---

## Write results to a file

Use `--output` to write directly to a file:

```bash
lynxdb query 'level=error' --since 24h --output errors.json
```

Or use shell redirection:

```bash
lynxdb query 'level=error' --since 24h > errors.json
lynxdb query 'level=error' --since 24h --format csv > errors.csv
```

---

## CI/CD integration

### Fail on matching events

Use `--fail-on-empty` to exit with code 6 when a query returns no results. Invert the logic for error detection:

```bash
# Check for errors after deployment
if lynxdb query 'level=fatal source=api' --since 10m --fail-on-empty 2>/dev/null; then
  echo "FATAL: Errors detected after deployment"
  exit 1
fi
echo "No fatal errors found"
```

### Exit codes for scripting

Use LynxDB's [exit codes](/docs/cli/overview) in scripts:

```bash
lynxdb query 'level=FATAL' --fail-on-empty 2>/dev/null
case $? in
  0) echo "Fatal errors found!" ;;
  6) echo "No fatal errors -- all clear" ;;
  3) echo "Server unreachable" ;;
  4) echo "Bad query syntax" ;;
  *) echo "Unexpected error" ;;
esac
```

| Code | Meaning |
|------|---------|
| 0 | Query succeeded with results |
| 3 | Cannot reach server |
| 4 | SPL2 syntax error |
| 5 | Query timeout |
| 6 | No results (with `--fail-on-empty`) |
| 130 | Interrupted (Ctrl+C) |

### Post-deployment smoke test

```bash
#!/bin/bash
# post-deploy-check.sh

echo "Waiting 60s for logs to accumulate..."
sleep 60

ERROR_COUNT=$(lynxdb query 'level=error source=api' --since 2m --format json | jq -s 'length')
FATAL_COUNT=$(lynxdb query 'level=fatal source=api' --since 2m --format json | jq -s 'length')

echo "Errors: $ERROR_COUNT, Fatal: $FATAL_COUNT"

if [ "$FATAL_COUNT" -gt 0 ]; then
  echo "FATAL errors detected -- rolling back"
  exit 1
fi

if [ "$ERROR_COUNT" -gt 50 ]; then
  echo "Error rate too high -- rolling back"
  exit 1
fi

echo "Deployment looks healthy"
```

### Scheduled reports with cron

```bash
# Generate a daily error report
# Add to crontab: 0 9 * * * /path/to/daily-report.sh

#!/bin/bash
DATE=$(date +%Y-%m-%d)
lynxdb query 'level=error | stats count by source | sort -count' \
  --since 24h --format csv > "/reports/errors-${DATE}.csv"
```

### Use saved queries in CI

Save queries once, run them in any pipeline:

```bash
# One-time setup
lynxdb save "ci-health" 'level=fatal OR level=error | stats count AS errors by source | where errors > 0'

# In CI pipeline
lynxdb run ci-health --since 5m --format json | jq -e 'length == 0' || exit 1
```

See [saved queries](/docs/guides/saved-queries) for more details.

---

## Suppress non-data output

Use `--quiet` / `-q` to suppress all non-data output (stats, progress, hints):

```bash
lynxdb query 'level=error | stats count' --quiet
```

Use `--no-stats` to suppress only the query statistics footer:

```bash
lynxdb query 'level=error | stats count' --no-stats
```

Use `--no-color` or set `NO_COLOR=1` to disable colored output:

```bash
NO_COLOR=1 lynxdb query 'level=error | stats count'
```

---

## Environment variables for automation

Set these environment variables to avoid passing flags repeatedly:

```bash
export LYNXDB_SERVER=https://lynxdb.company.com
export LYNXDB_TOKEN=lxk_your_api_key
export LYNXDB_PROFILE=production
```

See the [environment variables reference](/docs/configuration/environment-variables) for the full list.

---

## Practical script examples

### Monitor error rate and notify via external tool

```bash
#!/bin/bash
ERROR_RATE=$(lynxdb query '_source=nginx | stats count AS total, count(eval(status>=500)) AS errors | eval rate=round(errors/total*100,2)' \
  --since 5m --format json | jq -r '.rate')

if (( $(echo "$ERROR_RATE > 5.0" | bc -l) )); then
  curl -X POST "https://hooks.slack.com/services/..." \
    -d "{\"text\": \"Error rate is ${ERROR_RATE}% -- investigate!\"}"
fi
```

### Generate a multi-format report

```bash
#!/bin/bash
QUERY='_source=nginx | stats count, avg(duration_ms) AS avg_lat, p99(duration_ms) AS p99_lat by uri | sort -count | head 20'

lynxdb query "$QUERY" --since 24h --format csv > report.csv
lynxdb query "$QUERY" --since 24h --format json > report.json
lynxdb query "$QUERY" --since 24h --format table
```

### Tail and filter with standard tools

```bash
# Tail and pipe through grep for secondary filtering
lynxdb tail '_source=nginx' --format json | jq -r 'select(.status >= 500) | "\(.uri) \(.status)"'
```

---

## Next steps

- [Pipe mode](/docs/getting-started/pipe-mode) -- detailed guide to serverless file and stdin queries
- [Output formats](/docs/cli/output-formats) -- complete reference for all output format options
- [Saved queries](/docs/guides/saved-queries) -- save and reuse queries in scripts
- [CLI overview](/docs/cli/overview) -- complete CLI reference with all commands and flags
