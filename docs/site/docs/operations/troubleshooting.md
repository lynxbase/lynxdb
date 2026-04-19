---
title: Troubleshooting
description: Troubleshoot common LynxDB issues -- lynxdb doctor, connectivity problems, query errors, storage issues, and performance problems.
---

# Troubleshooting

This guide covers common issues and how to resolve them. Start with `lynxdb doctor` for an automated diagnostic.

## lynxdb doctor

The `doctor` command runs a comprehensive health check:

```bash
lynxdb doctor
```

```
ok Binary        v0.1.0 (linux/amd64, go1.25.4)
ok Config        /home/user/.config/lynxdb/config.yaml (valid)
ok Data dir      /var/lib/lynxdb (42 GB free)
ok Server        localhost:3100 (healthy, uptime 2d 5h)
ok Events        3.4M total
ok Storage       1.2 GB
ok Retention     7d
ok Completion    zsh detected

All checks passed.
```

For machine-readable output:

```bash
lynxdb doctor --format json
```

## Connection Issues

### "Cannot reach server"

**Symptoms:** `lynxdb query` returns exit code 3 (Connection).

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
- Server is not running -- start it with `lynxdb server` or `systemctl start lynxdb`
- Server is listening on a different address -- check `lynxdb config get listen`
- Firewall is blocking the port -- open port 3100 in your firewall
- TLS is enabled but client is using `http://` -- switch to `https://`
- Wrong server address -- check `LYNXDB_SERVER` env var or `--server` flag

### "Missing or invalid authentication token"

**Symptoms:** Exit code 7 (Auth).

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
lynxdb query 'level=error' --tls-skip-verify

# Or re-trust the certificate
lynxdb login --server https://localhost:3100
```

## Query Issues

### "Bad SPL2 syntax"

**Symptoms:** Exit code 4 (QueryParse).

```bash
# Check your query syntax
lynxdb explain 'your query here'

# Common mistakes:
# Wrong: index=main sourcetype=nginx
# Right: FROM main WHERE sourcetype="nginx"  (or: source=nginx)

# Wrong: stats count BY host
# Right: stats count by host  (case-insensitive, but check quotes)
```

LynxDB provides compatibility hints for common Splunk SPL1 patterns:

```
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

See the [Lynx Flow Reference](/docs/lynx-flow/overview) for the full query language reference.

### "Query timeout"

**Symptoms:** Exit code 5 (QueryTimeout).

```bash
# Increase client-side timeout
lynxdb query 'your expensive query' --timeout 5m

# Increase server-side limits
lynxdb config set query.max_query_runtime 30m
lynxdb config reload
```

**Optimization tips:**
- Add time range constraints: `--since 1h` instead of scanning all data
- Use `head` to limit results: `... | head 100`
- Create a materialized view for expensive aggregations
- Use `--analyze` to identify bottlenecks

### "`/api/v1/query/stream` returns VALIDATION_ERROR"

`POST /api/v1/query/stream` is an NDJSON export path, not a drop-in replacement for `POST /api/v1/query`.

It only accepts:

- `q` or `query`
- `from` or `earliest`
- `to` or `latest`
- `variables`

If you send `limit`, `offset`, `wait`, `profile`, or `format`, LynxDB rejects the request.

```bash
# Wrong for /query/stream
curl -X POST localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q":"FROM main | head 10","limit":10}'

# Right: use /query for bounded JSON responses
curl -X POST localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q":"FROM main | head 10","limit":10}'
```

### Slow Queries

```bash
# Profile the query
lynxdb query 'your query' --analyze full
```

Check the output for:
- **Segments scanned vs skipped**: If most segments are scanned, the query lacks selective predicates
- **Bloom filter effectiveness**: Low skip rate means your search terms are very common
- **High filter ratio**: Consider materialized views

**Common fixes:**
- Add time range to narrow the scan: `--since 1h`
- Use more selective filters: `source=nginx AND status>=500` instead of just `source=nginx`
- Create materialized views for repeated queries
- Increase cache size if hit rate is low

### No Results

```bash
# Check if data exists
lynxdb count
lynxdb count --since 1h

# Check available fields
lynxdb fields

# Check available sources
lynxdb sample 5

# Common issue: field name mismatch
lynxdb fields --prefix lev  # Check if it's "level" or "Level" or "log_level"
```

## Storage Issues

### Disk Full

**Symptoms:** Server stops accepting writes, compaction fails.

```bash
# Check disk usage
df -h /var/lib/lynxdb

# Check LynxDB storage size
lynxdb status

# Immediate relief: reduce retention
lynxdb config set retention 3d
lynxdb config reload
# Wait for compaction to delete old segments

# Clear query cache
lynxdb cache clear --force
```

**Long-term fixes:**
- Increase disk size
- Enable S3 tiering to move old data off local disk
- Reduce retention period
- Increase compression (switch to `zstd`)

### Buffered Events Keep Growing

If buffered events keep growing, flushes or compaction are not keeping up:

```bash
# Check buffered events and segment counts
lynxdb status --format json | jq '{buffered_events, segment_count}'

# Check server logs for flush errors
sudo journalctl -u lynxdb | grep -i flush
```

**Common causes:**
- Disk is full -- free space for part writes
- Compaction is stuck -- check `compaction_workers` and logs
- Sustained ingest exceeds the available disk bandwidth

### Compaction Backlog

If L0 segment count is growing:

```bash
# Check segment count
lynxdb status --format json | jq .segments

# Increase compaction workers, then restart the server
lynxdb config set storage.compaction_workers 4
sudo systemctl restart lynxdb
```

`storage.compaction_workers` is a startup-time setting. `config reload` updates hot-reloadable settings, but it does not re-create the compaction scheduler with a new worker count.

## Server Issues

### High Memory Usage

```bash
# Check active queries
lynxdb jobs --status running

# Cancel expensive queries
lynxdb jobs qry_xxx --cancel

# Set a lower query pool and restart with it
lynxdb config set query.global_query_pool_bytes 4gb
sudo systemctl restart lynxdb
```

### Server Won't Start

```bash
# Check for port conflicts
ss -tlnp | grep 3100

# Check for data directory permissions
ls -la /var/lib/lynxdb

# Run with debug logging
lynxdb server --log-level debug
```

### Crash Recovery

If the server crashes, LynxDB scans the data directory, removes stale temporary files, and rebuilds its in-memory part registry on the next startup:

```bash
# Just start the server again
sudo systemctl start lynxdb

# Watch the logs for recovery and part scan progress
sudo journalctl -u lynxdb -f
```

## Cluster Issues

### Node Not Joining

```bash
# Check that seed addresses are reachable
telnet meta-1.example.com 9400

# Check that the node_id is unique
grep node_id /etc/lynxdb/config.yaml

# Check cluster port is open
ss -tlnp | grep 9400
```

### Raft Quorum Lost

If 2 of 3 meta nodes are down:

- Writes will fail (no quorum for Raft consensus)
- Reads may still work from cached data
- Bring at least 1 meta node back to restore quorum

```bash
# Check which meta nodes are available
for host in meta-1 meta-2 meta-3; do
  echo -n "$host: "
  curl -s "http://$host:3100/health" && echo " OK" || echo " UNREACHABLE"
done
```

## CLI Issues

### Shell Completion Not Working

```bash
# Regenerate completions
lynxdb completion bash >> ~/.bashrc  # Bash
lynxdb completion zsh >> ~/.zshrc    # Zsh
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish  # Fish

# Reload shell
source ~/.bashrc  # or ~/.zshrc
```

### Config File Not Found

```bash
# Check where LynxDB is looking
lynxdb config path

# Create a default config
lynxdb config init

# Validate existing config
lynxdb config validate
```

## Exit Codes Reference

| Code | Name | Meaning |
|------|------|---------|
| 0 | OK | Success |
| 1 | General | Unspecified failure |
| 2 | Usage | Invalid flags or missing arguments |
| 3 | Connection | Cannot reach server |
| 4 | QueryParse | Bad SPL2 syntax |
| 5 | QueryTimeout | Query timed out |
| 6 | NoResults | No results (with `--fail-on-empty`) |
| 7 | Auth | Authentication failure |
| 10 | Aborted | User declined confirmation |
| 130 | Interrupted | Ctrl+C |

## Getting Help

```bash
# Built-in help
lynxdb --help
lynxdb query --help

# Show examples
lynxdb examples

# Interactive shell with tab completion
lynxdb shell
```

## Next Steps

- [Monitoring](/docs/operations/monitoring) -- track server health
- [Performance Tuning](/docs/operations/performance-tuning) -- optimize performance
- [Configuration Overview](/docs/configuration/overview) -- configuration reference
- [Lynx Flow Reference](/docs/lynx-flow/overview) -- query language reference
