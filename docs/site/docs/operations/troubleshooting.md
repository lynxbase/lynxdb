---
title: Troubleshooting
description: Troubleshoot common LynxDB issues -- connectivity, query errors, ingest, storage and startup problems.
---

# Troubleshooting

This page covers common issues and how to resolve them. Start with `lynxdb health` and `lynxdb status`: they show whether the server is reachable, healthy, and holding the data you expect.

```bash
lynxdb health
lynxdb status
```

## Connection Issues

### "Cannot connect"

**Symptoms:** `lynxdb query` exits with code 3 (connection).

```text
  ✖ CONNECTION: Cannot connect to http://localhost:3100
```

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

**Symptoms:** Exit code 7 (auth). The server answers `401 AUTH_REQUIRED` or `401 INVALID_TOKEN`, or `403 FORBIDDEN` for a key whose scope does not allow the request.

```bash
# Check auth status
lynxdb auth status

# Log in again
lynxdb login

# Or set the key directly
export LYNXDB_TOKEN=lynx_ak_your_key_here
```

### TLS Certificate Errors

```bash
# Skip verification (development only)
lynxdb query 'source=main level=ERROR | head 10' --tls-skip-verify

# Or trust the certificate again
lynxdb logout --server https://localhost:3100
lynxdb login --server https://localhost:3100
```

## Query Issues

### Query Does Not Compile

**Symptoms:** Exit code 4. The server answers `400` with `SYNTAX_ERROR`, `SEMANTIC_ERROR`, `TYPE_ERROR` or `PLANNING_ERROR`, or `422` with `UNSUPPORTED_COMMAND`, `UNSUPPORTED_FUNCTION` or `UNSUPPORTED_FEATURE`.

The error names the position and, where it can, the fix:

```text
  ✖ SEMANTIC_ERROR: semantic error at line 1, column 57: field avg_rt not found (available fields: avg(duration_ms), service)

    source=main | stats avg(duration_ms) by service | where avg_rt > 100
                                                            ^
    Hint: available fields: avg(duration_ms), service
```

Common mistakes:

| Wrong | Right | Why |
|-------|-------|-----|
| `main \| head 10` | `source=main \| head 10` | A query starts with a source |
| `source=main \| search error` | `source=main error` or `source=main \| where match(_raw, 'error')` | `search` is only the first command; filter later stages with `where` |
| `stats avg(duration_ms) by service \| where avg_rt > 100` | `stats avg(duration_ms) as avg_rt by service \| where avg_rt > 100` | An unaliased aggregate is named `avg(duration_ms)` |
| `eval d = duration_ms-100` | `eval d = duration_ms - 100` | `a-b` is one field name; put spaces around `-` |
| `bin _time bins=10` | `bin _time span=5m` | `bins=` is not supported |

Check a query without running it:

```bash
lynxdb query --explain 'source=main level=ERROR | stats count() by host'
```

See [PPL errors](../ppl/errors.md) for every error code and [PPL syntax](../ppl/syntax.md) for the grammar.

### No Results

A field that does not exist is not an error: LynxDB reads schemas on query, so `where lvl = "ERROR"` returns no rows when the field is `level`. Check the data and field names:

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

Field comparisons are exact and case-sensitive (`level=ERROR` does not match `error`); search terms are case-insensitive.

A field of a nested JSON object is a column named by its path, such as `http.status` for `{"http":{"status":502}}`. An event that lacks a field has no value for it, so `where` comparisons on that field are false for the event.

Events without a time field get the time they arrived. A query with `--since` or `earliest=` selects by that time, so events loaded from an old file may fall outside the range; see [`storage.time_fields`](/docs/configuration/storage).

Events whose own time is older than `retention` (default `7d`) are removed by the next retention pass, within about a minute of arriving. The server log then shows `msg="retention removed events older than the cutoff"` for the index. Set `retention: 0s` to keep old events; see [Retention](/docs/operations/retention).

### "Query timeout"

**Symptoms:** Exit code 5 (timeout).

The server does not stop a query on its own. A query that runs longer than `query.sync_timeout` (default `30s`) becomes a job, and the CLI polls it until `--timeout` (default `6m`) expires.

```bash
# Raise the client-side timeout
lynxdb query 'source=main | stats count() by host' --timeout 15m
```

**Fixes:**

- Bound the time range: `--since 1h` or `earliest=-1h` in the search
- End event searches with `head`: `... | head 100`
- Look at the plan with `lynxdb explain` (see [Performance Tuning](/docs/operations/performance-tuning#query-shape))

### "Too many concurrent queries"

**Symptoms:** `429 TOO_MANY_REQUESTS` with `Retry-After: 1`. `query.max_concurrent` queries, synchronous and asynchronous, are running. Retry after a second, cancel jobs you do not need (`lynxdb jobs --status running`, `lynxdb jobs <id> --cancel`), or raise `query.max_concurrent` and restart the server.

### `/api/v1/query/stream` returns VALIDATION_ERROR

`POST /api/v1/query/stream` is an NDJSON export path, not a drop-in replacement for `POST /api/v1/query`. It accepts only:

- `q` or `query`
- `from` or `earliest`
- `to` or `latest`

A request that also sends `limit`, `offset`, `wait`, or `format` is rejected.

```bash
# Wrong for /query/stream
curl -X POST localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q":"source=main | head 10","limit":10}'
```

```json
{"error":{"code":"VALIDATION_ERROR","message":"unsupported fields for /query/stream: limit","suggestion":"Use only q/query, from/to and earliest/latest with /query/stream."}}
```

```bash
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
lynxdb explain 'source=main level=ERROR | stats count() by host'
```

Check the `Scan` line of the optimized plan:

- **No time range**: the scan reads every partition. Add `earliest=` or `--since`.
- **Many columns**: name the fields you need with `fields`.
- **Filters far from the scan**: filters placed after `eval` or `stats` cannot narrow the scan.

See [Performance Tuning](/docs/operations/performance-tuning#query-shape).

## Ingest Issues

### Events Do Not Appear

A query syncs the database before it runs, so it sees every event whose ingest request was answered. When events are missing:

- Check the response of the ingest request: `accepted` counts the events stored. A line that is not a JSON object is kept as an event holding the text in `_raw`, so search for its text rather than for fields.
- Check the index: `?index=` and `X-Index` choose it for `/api/v1/ingest` and `/api/v1/ingest/raw`, the bulk `_index` for Elasticsearch shippers. `lynxdb indexes` lists the names.
- Check the time: an event's time comes from the first of `storage.time_fields` it holds. A query with a time range leaves out events whose time falls outside it.
- Check retention: events whose time is older than `retention` are removed within about a minute of arriving.

### `400 VALIDATION_ERROR` on an Index Name

Index names hold letters, digits, `.`, `-` and `_`, and do not start with a dot. Elasticsearch bulk requests take lowercase index names only.

### `413` or Truncated Bodies

A body over its limit is refused with `413`, and the message names the setting that sets the limit:

```json
{"error":{"reason":"compressed body exceeds limit 1024 set by ingest.max_body_size","type":"request_body_too_large"},"status":413}
```

`POST /api/v1/ingest` and `/api/v1/ingest/raw` are bounded by `ingest.max_body_size`, compressed or not. The other routes (Elasticsearch bulk, Splunk HEC, OTLP on the API port) are bounded by `ingest.limits.max_compressed_body_bytes` and `ingest.limits.max_decompressed_body_bytes`. A raw body sent without a length that runs past the limit part-way keeps the lines already stored and answers with `"truncated": true`. Send smaller batches, or raise the setting and restart.

## Storage Issues

### Disk Full

**Symptoms:** Ingest requests fail; the server log shows write errors.

```bash
# Check disk usage
df -h /var/lib/lynxdb

# Check what each index holds
lynxdb indexes
```

**Fixes:**

- Free or add disk space
- Reduce the retention period (`lynxdb config set retention 3d`, then restart); the first retention pass after the restart deletes the expired partitions

### Part Count Keeps Growing

`part_count` in `/api/v1/stats` rises with ingest and falls as the store merges parts. If it only grows, merges cannot keep up with the ingest rate. Check the disk's throughput and the server log for write errors, and send fewer, larger batches.

## Server Issues

### Server Won't Start

```bash
# Check for port conflicts
ss -tlnp | grep 3100

# Check data directory permissions
ls -la /var/lib/lynxdb

# Run with debug logging
lynxdb server --log-level debug
```

### Damaged Index

When an index's manifest names a part file that is missing or damaged, the server stops at startup and names the index and the file:

```text
  ✖ ERROR: server init: open database: columnar: index main: colstore: manifest is damaged or names a missing or damaged part: 0000000000000000.part: open /var/lib/lynxdb/main/0000000000000000.part: no such file or directory
```

The server leaves the files as they are. Restore the index directory from a backup (see [Backup and Restore](/docs/operations/backup-restore#one-index)), or move the index directory out of the data directory to start without that index.

### Crash Recovery

After a crash, each index opens from its manifest on the next start: files that no manifest record names are removed, or moved to `lost+found/` in the index directory when the manifest may have lost records. Events whose ingest request was answered but that were not yet written are lost; shippers that retry on failure send them again. See [Durability](/docs/operations/backup-restore#durability).

```bash
sudo systemctl start lynxdb
sudo journalctl -u lynxdb -f
```

## CLI Issues

### Shell Completion Not Working

```bash
lynxdb completion bash >> ~/.bashrc                              # Bash
lynxdb completion zsh > "${fpath[1]}/_lynxdb"                    # Zsh
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
| 1 | General | Unspecified failure, including a server that fails to start |
| 2 | Usage | Invalid flags or missing arguments |
| 3 | Connection | Cannot reach server |
| 4 | Query | The query does not compile |
| 5 | Timeout | Server timeout or `--timeout` exceeded |
| 6 | NoResults | No results (with `--fail-on-empty`) |
| 7 | Auth | Missing or invalid authentication token |
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
