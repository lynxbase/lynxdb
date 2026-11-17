---
title: Troubleshooting
description: Troubleshoot common LynxDB issues -- connectivity, query errors, storage, memory, and cluster problems.
---

# Troubleshooting

This page covers common issues and how to resolve them. Start with `lynxdb health` and `lynxdb status`: they show whether the server is reachable, healthy, and holding the data you expect.

```bash
lynxdb health
lynxdb status
```

## Connection Issues

### "Cannot reach server"

**Symptoms:** `lynxdb query` exits with code 3 (connection).

**Checks:**

```bash
# Is the server running?
sudo systemctl status lynxdb
# or
pgrep -f 'lynxdb server'

# Is it listening on the expected port?
ss -tlnp | grep 3100

# Can you reach it?
curl http://localhost:3100/health

# Check server logs
sudo journalctl -u lynxdb --since "10 minutes ago"
```

**Common causes:**

- The server is not running -- start it with `lynxdb server` or `systemctl start lynxdb`
- The server listens on a different address -- check `lynxdb config get listen`
- A firewall blocks the port -- open port 3100
- TLS is enabled but the client uses `http://` -- switch to `https://`
- Wrong server address -- check the `LYNXDB_SERVER` environment variable or the `--server` flag

### "Missing or invalid authentication token"

**Symptoms:** Exit code 7 (auth).

```bash
# Check auth status
lynxdb auth status

# Re-authenticate
lynxdb login

# Or set the token directly
export LYNXDB_TOKEN=lxk_your_token_here
```

### TLS Certificate Errors

```bash
# Skip verification (development only)
lynxdb query 'source=main level=ERROR | head 10' --tls-skip-verify

# Or re-trust the certificate
lynxdb login --server https://localhost:3100
```

## Query Issues

### Query Does Not Compile

**Symptoms:** Exit code 4. The server answers `400` with `SYNTAX_ERROR`, `SEMANTIC_ERROR`, `TYPE_ERROR` or `PLANNING_ERROR`, or `422` with `UNSUPPORTED_COMMAND`, `UNSUPPORTED_FUNCTION` or `UNSUPPORTED_FEATURE`.

The error names the position and, where it can, the fix:

```
✖ SEMANTIC_ERROR: semantic error at line 1, column 56: field avg_rt not found (available fields: avg(response_time), host)

  source=main | stats avg(response_time) by host | where avg_rt > 100
                                                         ^
  Hint: available fields: avg(response_time), host
```

Common mistakes:

| Wrong | Right | Why |
|-------|-------|-----|
| `main \| head 10` | `source=main \| head 10` | A query starts with a source |
| `source=main \| search error` | `source=main error` or `source=main \| where match(_raw, 'error')` | `search` is only the first command; filter later stages with `where` |
| `stats avg(response_time) by host \| where avg_rt > 100` | `stats avg(response_time) as avg_rt by host \| where avg_rt > 100` | An unaliased aggregate is named `avg(response_time)` |
| `eval d = response_time-100` | `eval d = response_time - 100` | `a-b` is one identifier; put spaces around `-` and `*` |
| `bin _time bins=10` | `bin _time span=5m` | `bins=` is not supported |

Check a query without running it:

```bash
lynxdb query --explain 'source=main level=ERROR | stats count() by host'
```

See [PPL errors](../ppl/errors.md) for every error code and [PPL syntax](../ppl/syntax.md) for the grammar.

### No Results

A field that does not exist is not an error: LynxDB is schema-on-read, so `where lvl = "ERROR"` returns no rows when the field is `level`. Check the data and field names:

```bash
# How many events are there?
lynxdb query 'source=main | stats count()'
lynxdb query 'source=main | stats count()' --since 1h

# Which fields exist?
lynxdb fields
lynxdb fields --prefix lev   # "level", "Level" or "log_level"?

# What does an event look like?
lynxdb query 'source=main | head 5'

# Which indexes exist?
lynxdb indexes
```

Field comparisons are exact and case-sensitive (`level=ERROR` does not match `error`); free-text terms are case-insensitive.

### "Query timeout"

**Symptoms:** Exit code 5 (timeout).

```bash
# Raise the client-side timeout
lynxdb query 'source=main | stats count() by host' --timeout 5m

# Raise the server-side limit
lynxdb config set query.max_query_runtime 30m
lynxdb config reload
```

**Fixes:**

- Bound the time range: `--since 1h` or `earliest=-1h` in the source
- End event searches with `head`: `... | head 100`
- Profile the query with `--analyze` to find the slow stage

### `/api/v1/query/stream` returns VALIDATION_ERROR

`POST /api/v1/query/stream` is an NDJSON export path, not a drop-in replacement for `POST /api/v1/query`. It accepts only:

- `q` or `query`
- `from` or `earliest`
- `to` or `latest`

A request that also sends `limit`, `offset`, `wait`, `profile`, or `format` is rejected.

```bash
# Wrong for /query/stream
curl -X POST localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q":"source=main | head 10","limit":10}'

# Right: bound the rows in the query
curl -X POST localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q":"source=main | head 10"}'

# Or use /query for bounded JSON responses
curl -X POST localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q":"source=main | head 10","limit":10}'
```

### Slow Queries

```bash
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze=full
```

Check the profile for:

- **Segments scanned vs skipped**: if most segments are scanned, the query has no time range or selective term
- **Rows scanned vs returned**: many rows read for few results means the filter does not reach the scan
- **Per-operator time**: which stage dominates

**Fixes:**

- Add a time range: `--since 1h`
- Put selective terms and field comparisons in the source command: `source=main host=web-01 status>=500` instead of filtering later with `where`
- See [Performance Tuning](/docs/operations/performance-tuning#query-shape)

## Storage Issues

### Disk Full

**Symptoms:** The server stops accepting writes; compaction fails.

```bash
# Check disk usage
df -h /var/lib/lynxdb

# Check LynxDB storage size
lynxdb status

# Immediate relief: reduce retention (expired partitions go at the next hourly pass)
lynxdb config set retention 3d
lynxdb config reload

# Clear the query result cache
lynxdb cache clear --force
```

**Long-term fixes:**

- Increase disk size
- Configure S3 tiering to move old data off local disk
- Reduce the retention period
- Switch compression to `zstd`

### Buffered Events Keep Growing

If buffered events keep growing, flushes are not keeping up:

```bash
# Check buffered events and segment counts
lynxdb status --format json | jq '{buffered_events, segment_count}'

# Check server logs for flush errors
sudo journalctl -u lynxdb | grep -i flush
```

**Common causes:**

- The disk is full -- free space for part writes
- Sustained ingest exceeds the available disk bandwidth

### Compaction Backlog

If the segment count keeps growing, compaction falls behind. `lynxdb_compaction_queue_depth` on `/metrics` shows pending jobs.

```bash
# Check the segment count
lynxdb status --format json | jq .segment_count

# Increase compaction workers, then restart the server
lynxdb config set storage.compaction_workers 4
sudo systemctl restart lynxdb
```

`storage.compaction_workers` is read at startup. `config reload` updates hot-reloadable settings but does not re-create the compaction scheduler with a new worker count.

### Server Refuses to Start: Format Errors

LynxDB checks the `FORMAT` marker and every segment header at startup. It refuses to start, with exit code 78, when:

- the data directory holds segments but no `FORMAT` marker
- the marker names a format newer than the binary supports (the data directory was written by a newer LynxDB)
- a segment uses an unknown format version or required capability

Run a LynxDB release that supports the data directory's format. See [Upgrading](/docs/operations/upgrading#storage-format). A segment with a checksum failure is logged as a warning and does not block startup.

## Server Issues

### High Memory Usage

```bash
# Check running queries
lynxdb jobs --status running

# Cancel an expensive query
lynxdb jobs qry_9c1d4e --cancel

# Lower the query memory pool, then restart
lynxdb config set query.global_query_pool_bytes 4gb
sudo systemctl restart lynxdb
```

`lynxdb status --format json` and `GET /api/v1/status` show memory per governor class.

### Server Won't Start

```bash
# Check for port conflicts
ss -tlnp | grep 3100

# Check data directory permissions
ls -la /var/lib/lynxdb

# Run with debug logging
lynxdb server --log-level debug
```

### Crash Recovery

After a crash, LynxDB scans the data directory, removes stale temporary files, and rebuilds its in-memory part registry on the next startup. Events that were buffered but not yet flushed to a part are lost; shippers that retry on failure resend them.

```bash
# Start the server again
sudo systemctl start lynxdb

# Watch the logs for the part scan
sudo journalctl -u lynxdb -f
```

## Cluster Settings Have No Effect

`lynxdb server` validates the `cluster` section of the configuration, but it does not start the cluster runtime: it opens no gRPC listener on `cluster.grpc_port`, joins no Raft group, and routes no shards. Each server runs as a single node with its own data directory. See [Distributed Architecture](/docs/architecture/distributed) for the cluster components in `pkg/cluster`.

## CLI Issues

### Shell Completion Not Working

```bash
lynxdb completion bash >> ~/.bashrc                              # Bash
lynxdb completion zsh >> ~/.zshrc                                # Zsh
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish  # Fish

# Reload the shell
source ~/.bashrc  # or ~/.zshrc
```

### Config File Not Found

```bash
# Where LynxDB looks
lynxdb config path

# Write a default config
lynxdb config init

# Validate an existing config
lynxdb config validate
```

## Exit Codes Reference

| Code | Name | Meaning |
|------|------|---------|
| 0 | OK | Success |
| 1 | General | Unspecified failure |
| 2 | Usage | Invalid flags or missing arguments |
| 3 | Connection | Cannot reach server |
| 4 | Query | The query does not compile |
| 5 | Timeout | Server timeout or `--timeout` exceeded |
| 6 | NoResults | No results (with `--fail-on-empty`) |
| 7 | Auth | Missing or invalid authentication token |
| 10 | Aborted | User declined a confirmation |
| 78 | Config | Configuration error or incompatible on-disk format |
| 130 | Interrupted | Ctrl+C |

## Getting Help

```bash
lynxdb --help
lynxdb query --help
```

## Next Steps

- [Monitoring](/docs/operations/monitoring) -- track server health
- [Performance Tuning](/docs/operations/performance-tuning) -- tune for your workload
- [Configuration Overview](/docs/configuration/overview) -- configuration reference
- [PPL overview](../ppl/overview.md) -- query language reference
