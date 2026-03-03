---
title: Upgrading from grep/awk
description: Upgrade from grep, awk, and jq to LynxDB pipe mode -- same Unix philosophy, with aggregations, statistics, and structured output.
---

# Upgrading from grep/awk

If you live in the terminal and your log analysis toolkit is `grep`, `awk`, `sed`, and `jq`, LynxDB pipe mode gives you the power of a full analytics engine with zero setup. Same philosophy: read from stdin, process, write to stdout. No server, no config file, no daemon.

## How Pipe Mode Works

LynxDB's `query` command detects when data is piped via stdin. It creates an ephemeral in-memory engine, ingests the data, runs your SPL2 query, prints results, and exits. Nothing is saved to disk.

```bash
cat app.log | lynxdb query '| stats count by level'
```

This is the equivalent of a full analytics pipeline in a single command.

## Side-by-Side Comparisons

### Count Lines Matching a Pattern

```bash
# grep
grep -c "ERROR" app.log

# LynxDB
lynxdb query --file app.log 'level=error | stats count'
```

### Count by Field Value

```bash
# grep + sort + uniq
grep -oP 'level=\K\w+' app.log | sort | uniq -c | sort -rn

# awk
awk -F'level=' '{print $2}' app.log | awk '{print $1}' | sort | uniq -c | sort -rn

# LynxDB
lynxdb query --file app.log '| stats count by level | sort -count'
```

### Filter and Aggregate

```bash
# grep + awk (fragile, depends on log format)
grep "status=5" access.log | awk '{print $7}' | sort | uniq -c | sort -rn | head -10

# LynxDB (works with any log format)
lynxdb query --file access.log '| where status>=500 | stats count by uri | sort -count | head 10'
```

### Average of a Numeric Field

```bash
# awk
awk '{sum+=$NF; n++} END {print sum/n}' data.log

# LynxDB
lynxdb query --file data.log '| stats avg(duration_ms)'
```

### Percentiles

```bash
# awk (requires writing a percentile function)
# ... complex multi-line awk script ...

# LynxDB
lynxdb query --file data.log '| stats p50(duration_ms), p95(duration_ms), p99(duration_ms)'
```

### Time-Based Aggregation

```bash
# awk (requires parsing timestamps, bucketing, counting)
# ... very complex awk script ...

# LynxDB
lynxdb query --file app.log 'level=error | timechart count span=5m'
```

### Top Values

```bash
# grep + sort + uniq + head
grep -oP 'host=\K\S+' app.log | sort | uniq -c | sort -rn | head -5

# LynxDB
lynxdb query --file app.log '| top 5 host'
```

### JSON Logs

```bash
# jq (one field at a time)
cat app.json | jq -r '.level' | sort | uniq -c | sort -rn

# jq (complex aggregation -- difficult)
cat app.json | jq -r '[.level, .source] | @tsv' | sort | uniq -c | sort -rn

# LynxDB (handles JSON natively)
cat app.json | lynxdb query '| stats count by level, source | sort -count'
```

### Extracting Fields with Regex

```bash
# grep -oP
grep -oP 'duration=\K\d+' app.log

# LynxDB (named capture groups)
lynxdb query --file app.log '| rex field=_raw "duration=(?P<dur>\d+)" | table dur'
```

### Chaining with Unix Tools

LynxDB outputs NDJSON when piped, so it composes with standard tools:

```bash
# LynxDB aggregation -> jq for further processing
lynxdb query --file app.log '| stats count by host' | jq '.host'

# LynxDB filter -> CSV export -> sort
lynxdb query --file app.log '| stats count by status' --format csv | sort -t, -k2 -rn

# LynxDB as a filter in a pipeline
cat huge.log | lynxdb query '| where level="ERROR"' | wc -l
```

## Common Recipes

### Quick Error Count

```bash
cat app.log | lynxdb query '| where level="ERROR" | stats count'
```

### Errors Per Service in the Last Hour

```bash
# Against a running server
lynxdb query 'level=error | stats count by source' --since 1h

# Against a local file
lynxdb query --file app.log '| where level="ERROR" | stats count by source'
```

### Slow Requests

```bash
kubectl logs deploy/api | lynxdb query '| where duration_ms > 1000 | stats avg(duration_ms), count by endpoint | sort -count'
```

### HTTP Status Code Distribution

```bash
lynxdb query --file access.log '| stats count by status | sort -count'
```

### Unique Visitors

```bash
lynxdb query --file access.log '| stats dc(client_ip) as unique_visitors'
```

### Error Spike Detection

```bash
lynxdb query --file app.log 'level=error | timechart count span=1m'
```

### Parse Unstructured Logs

```bash
# Extract IP and status from Apache combined log format
lynxdb query --file access.log \
  '| rex field=_raw "^(?P<ip>\S+) .* \"(?P<method>\S+) (?P<uri>\S+) .*\" (?P<status>\d+)"
   | stats count by status | sort -count'
```

### Docker/Kubernetes Logs

```bash
# Docker
docker logs myapp 2>&1 | lynxdb query '| search "OOM" | stats count by container'

# Kubernetes
kubectl logs deploy/api --since=1h | lynxdb query '| stats avg(duration_ms) by endpoint'

# Multiple pods
kubectl logs -l app=api --all-containers | lynxdb query '| where level="ERROR" | stats count by pod'
```

### Process Compressed Logs

```bash
zcat /var/log/app.log.gz | lynxdb query '| stats count by level'
```

## Why LynxDB Over grep/awk

| Capability | grep/awk/jq | LynxDB Pipe Mode |
|---|---|---|
| Simple text search | Easy | Easy |
| Count by field | Awkward (`sort \| uniq -c`) | `stats count by field` |
| Averages, percentiles | Write your own function | Built-in (`avg`, `p99`, etc.) |
| Time-based buckets | Very difficult | `timechart count span=5m` |
| JSON parsing | jq (separate tool) | Native |
| Multiple aggregations | Near impossible | `stats count, avg(x), p99(x) by y` |
| Top-N | `sort \| head` (no ties) | `top 10 field` |
| Joins | Not possible | `JOIN`, CTEs |
| Output formats | Text only | JSON, table, CSV, TSV |

## When to Keep Using grep

- Simple text search in a single file: `grep "error" app.log` is faster for one-off searches
- When you need regex match highlighting
- When you need line numbers: `grep -n "pattern" file`

LynxDB complements grep rather than replacing it. Use grep for quick text searches, and LynxDB when you need aggregation, statistics, or structured analysis.

## Next Steps

- [Pipe Mode Guide](/docs/getting-started/pipe-mode) -- full pipe mode documentation
- [SPL2 Overview](/docs/spl2/overview) -- learn the query language
- [First Query](/docs/getting-started/first-query) -- your first SPL2 query
- [Server Mode](/docs/getting-started/server-mode) -- when you need persistence
