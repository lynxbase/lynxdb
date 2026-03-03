---
sidebar_position: 3
title: Pipe Mode (No Server)
description: Query local files and stdin using LynxDB without running a server.
---

# Pipe Mode (No Server)

LynxDB works without a running server. The CLI can query local files and stdin using the full SPL2 engine with zero network overhead. Think of it as `grep` with the power of a full analytics engine.

## How It Works

When you use `--file` or pipe data through stdin, LynxDB:

1. Creates an ephemeral in-memory engine
2. Ingests the data (auto-detects JSON, NDJSON, or plain text)
3. Runs the full SPL2 pipeline
4. Prints results
5. Exits

No daemon, no config file, no data directory. Everything happens in-process.

## Query Local Files

```bash
# Single file
lynxdb query --file access.log '| stats count by status'

# Glob patterns
lynxdb query --file '/var/log/nginx/*.log' '| where status>=500 | top 10 uri'

# Multiple file types
lynxdb query --file '/var/log/*.log' '| stats count by source'
```

## Pipe from Stdin

```bash
# Kubernetes logs
kubectl logs deploy/api | lynxdb query '| stats avg(duration_ms), p99(duration_ms) by endpoint'

# Docker logs
docker logs myapp 2>&1 | lynxdb query '| search "OOM" | stats count by container'

# System logs
cat /var/log/syslog | lynxdb query '| where level="ERROR" | stats count by service'

# Compressed files
zcat archive.log.gz | lynxdb query '| where status>=500 | top 10 path'

# Filter before querying
grep "2026-01-15" /var/log/app.log | lynxdb query '| stats count by level'
```

## Combine with Unix Tools

LynxDB plays well with the Unix pipeline:

```bash
# Output as CSV and sort with system tools
lynxdb query --file access.log '| stats count by status' --format csv | sort -t, -k2 -rn

# Chain with jq for JSON post-processing
lynxdb query --file app.json '| stats count by endpoint' --format json | jq '.count'

# Feed results into another LynxDB query
lynxdb query 'FROM main | where status>=500' --since 1h \
  | lynxdb query '| stats count by path'

# Export to file
lynxdb query --file access.log '| where level="ERROR"' > errors.json
```

## Output Behavior

LynxDB is TTY-aware:

- **Terminal (interactive):** Colorized JSON output with query statistics
- **Pipe (non-TTY):** Plain NDJSON for scripting

```bash
# Terminal: colorized output with stats
lynxdb query --file access.log '| stats count by status'

# Pipe: raw JSON, stats go to stderr
lynxdb query --file access.log '| stats count by status' | jq .
```

:::tip
Metadata and statistics are always written to stderr, so they never pollute piped output. Only result data goes to stdout.
:::

## Memory Management

For large files, you can limit memory usage:

```bash
lynxdb query --file huge.log '| stats count by level' --max-memory 512mb
```

When the memory limit is reached, LynxDB spills intermediate results to disk automatically.

## Real-World Examples

### Debug a Production Issue

```bash
# Find the most common errors in the last hour of logs
tail -n 100000 /var/log/app.log | lynxdb query '
  | where level="ERROR"
  | stats count by message
  | sort -count
  | head 10'
```

### Analyze Nginx Access Logs

```bash
# Top slow endpoints with error rates
lynxdb query --file /var/log/nginx/access.log '
  | stats count as total,
          count(eval(status>=500)) as errors,
          avg(duration_ms) as avg_lat,
          p99(duration_ms) as p99_lat
    by uri
  | eval error_rate = round(errors/total*100, 1)
  | where error_rate > 5
  | sort -error_rate
  | table uri, total, errors, error_rate, avg_lat, p99_lat'
```

### Monitor Kubernetes Pods

```bash
# Error rate by service across all pods
kubectl logs -l app=api --all-containers --since=1h | lynxdb query '
  | stats count as total, count(eval(level="error")) as errors by service
  | eval error_pct = round(errors/total*100, 1)
  | sort -error_pct'
```

## Next Steps

- **[Server Mode](/docs/getting-started/server-mode)** -- Set up persistent storage
- **[Your First SPL2 Query](/docs/getting-started/first-query)** -- Learn the query language
- **[Scripting with LynxDB](/docs/guides/scripting-and-pipes)** -- Advanced piping patterns
