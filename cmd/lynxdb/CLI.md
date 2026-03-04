# LynxDB CLI Reference

LynxDB -- single-binary columnar log storage and search engine with SPL2 query language.

```
lynxdb [command] [flags]
```

## Global Flags

Available on all commands:

| Flag | Short | Default | Env Var | Description |
|------|-------|---------|---------|-------------|
| `--config` | | (auto) | | Path to config file |
| `--server` | | `http://localhost:3100` | `LYNXDB_SERVER` | LynxDB server address |
| `--token` | | | `LYNXDB_TOKEN` | API key for authentication |
| `--profile` | `-p` | | `LYNXDB_PROFILE` | Connection profile name |
| `--format` | `-F` | `auto` | | Output format: auto, json, ndjson, table, csv, tsv, raw |
| `--quiet` | `-q` | `false` | | Suppress non-data output |
| `--verbose` | `-v` | `false` | | Show extra detail |
| `--no-stats` | | `false` | | Suppress query statistics |
| `--no-color` | | `false` | | Disable colored output |
| `--debug` | | `false` | | Enable debug logging to stderr |
| `--tls-skip-verify` | | `false` | `LYNXDB_TLS_SKIP_VERIFY` | Skip TLS certificate verification |

---

## Table of Contents

- [Modes of Operation](#modes-of-operation)
- [Core Data Commands](#core-data-commands)
  - [query](#query)
  - [ingest](#ingest)
  - [import](#import)
- [Quick Access Commands](#quick-access-commands)
  - [count](#count)
  - [sample](#sample)
  - [last](#last)
  - [fields](#fields)
  - [explain](#explain)
  - [examples](#examples)
- [Real-Time Commands](#real-time-commands)
  - [tail](#tail)
  - [top](#top)
  - [watch](#watch)
  - [diff](#diff)
- [Server & Ops Commands](#server--ops-commands)
  - [server](#server)
  - [status](#status)
  - [health](#health)
  - [indexes](#indexes)
  - [cache](#cache)
  - [config](#config)
  - [doctor](#doctor)
- [Saved Queries](#saved-queries)
  - [saved](#saved)
  - [save](#save)
  - [run](#run)
- [Materialized Views](#materialized-views)
  - [mv](#mv)
- [Alerts](#alerts)
  - [alerts](#alerts-1)
- [Dashboards](#dashboards)
  - [dashboards](#dashboards-1)
- [Authentication](#authentication)
  - [login](#login)
  - [logout](#logout)
  - [auth](#auth)
  - [jobs](#jobs)
- [Interactive](#interactive)
  - [shell](#shell)
- [Browser](#browser)
  - [ui](#ui)
  - [open](#open)
  - [share](#share)
- [Performance](#performance)
  - [bench](#bench)
  - [demo](#demo)
- [Utility](#utility)
  - [version](#version)
  - [completion](#completion)
- [Output Formats](#output-formats)
- [Pipe & Stdin Integration](#pipe--stdin-integration)
- [Configuration](#configuration)
- [Environment Variables](#environment-variables)
- [TUI Mode](#tui-mode)
- [Exit Codes](#exit-codes)
- [Signal Handling](#signal-handling)
- [SPL Compatibility Hints](#spl-compatibility-hints)

---

## Modes of Operation

LynxDB CLI works in three distinct modes depending on the command and context:

### 1. Server mode (`lynxdb server`)

Starts a persistent HTTP server. Data is stored on disk (or in-memory if `data_dir` is empty).
Other commands (`query`, `ingest`, `status`, etc.) connect to this server over HTTP.

### 2. Local file mode (`lynxdb query --file ...`)

Queries run directly against local files without a running server.
LynxDB creates an ephemeral in-memory engine, ingests the file(s), executes the query, and exits.

### 3. Stdin pipe mode (`cat file | lynxdb query '...'`)

Same as file mode, but reads data from stdin. Detected automatically when stdin is not a terminal.
No server required.

---

## Core Data Commands

### `query`

Execute an SPL2 query against a running server, a local file, or stdin.

```
lynxdb query [SPL2 query] [flags]
```

**Alias:** `q`

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range (e.g., `15m`, `1h`, `7d`) |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |
| `--file` | `-f` | | Query a local file instead of server |
| `--sourcetype` | | | Override source type detection (file/stdin mode) |
| `--source` | | | Set source metadata (file/stdin mode) |
| `--output` | `-o` | | Write results to file |
| `--timeout` | | | Query timeout (e.g., `10s`, `5m`) |
| `--analyze` | | | Profile query execution (`basic`, `full`, `trace`) |
| `--max-memory` | | | Max memory for ephemeral query (e.g., `512mb`, `1gb`) |
| `--fail-on-empty` | | `false` | Exit with code 6 if no results |
| `--copy` | | `false` | Copy results to clipboard as TSV |
| `--explain` | | `false` | Show query plan without executing |

The query argument is required. `FROM main` is automatically prepended if the query starts with `|` or a command name.

`--analyze` accepts an optional value; bare `--analyze` defaults to `basic`.

**Three execution modes:**

1. **Server mode** (default) -- sends query to a running LynxDB server via HTTP.
2. **File mode** (`--file`) -- ingests file(s) into ephemeral engine and queries locally.
3. **Stdin mode** (pipe detected) -- reads stdin into ephemeral engine and queries locally.

You cannot use `--file` and stdin simultaneously. `--copy` is only supported in server mode.

**Examples:**

```bash
# Query a running server
lynxdb query 'level=error | stats count by source'
lynxdb q 'FROM main | where status>=500 | top 10 path' --since 24h

# With absolute time range
lynxdb query 'FROM main | stats count by host' \
  --from 2026-01-15T00:00:00Z --to 2026-01-15T23:59:59Z

# Short form (FROM main is auto-prepended)
lynxdb query 'level=error | stats count'
lynxdb query '| stats count by source'

# Query local files (no server needed)
lynxdb query --file access.log '| stats count by status'
lynxdb query --file '/var/log/*.log' '| where level="ERROR" | stats count by source'

# Pipe from stdin (no server needed)
cat app.json | lynxdb query '| stats count by level'
kubectl logs deploy/api | lynxdb query '| where duration_ms > 1000 | stats avg(duration_ms) by endpoint'

# Profile query performance
lynxdb query 'level=error | stats count' --analyze
lynxdb query 'level=error | stats count' --analyze full

# Query with timeout
lynxdb query 'level=error | stats count' --timeout 30s

# Force output format
lynxdb query 'FROM main | stats count by host' --format csv
lynxdb query 'FROM main | stats count by host' --format table

# Write results to file
lynxdb query 'level=error' --output errors.json

# Exit code 6 when empty (for CI/scripting)
lynxdb query 'level=FATAL' --fail-on-empty || echo "No fatal errors found"

# Copy results to clipboard
lynxdb query 'level=error | stats count by source' --copy
```

**Console output -- server mode, TTY (interactive TUI):**

When stdout is a terminal and format is `auto`, the query runs in TUI mode with a live progress spinner:

```
  . Scanning segments...  1.23s

    Memtable:  4,200 events
    Segments:  3 scanned / 5 to scan / 12 total
    Skipped:   7 (bloom:3, time:2, index:1, stats:1)
    Rows read: 156,000
```

On completion, results are shown as colorized JSON with stats:

```
  #1 {
    "host": "web-01",
    "count": 42530
  }

  --------------------------------------------------
  Results:      2 results
  Scanned:      150,000 events  (149,998 filtered, 100.0%)
  Segments:     5 scanned / 12 total  (7 skipped)
  Query time:   45ms
```

**Console output -- server mode, pipe (non-TTY):**

When stdout is not a terminal (pipe), output is newline-delimited JSON:

```json
{"host":"web-01","count":42530}
{"host":"api-01","count":31204}
```

---

### `ingest`

Ingest logs from file or stdin into a running LynxDB server.

```
lynxdb ingest [file] [flags]
```

**Alias:** `i`

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | | Source metadata for events |
| `--sourcetype` | | Sourcetype metadata for events |
| `--index` | | Target index name |
| `--batch-size` | `5000` | Number of lines per batch |

Data is sent in batches via `POST /api/v1/ingest/raw`. Empty lines are skipped. Shows a progress bar (file mode) or counter (stdin mode) on TTY.

**Examples:**

```bash
lynxdb ingest access.log
lynxdb ingest access.log --source web-01 --sourcetype nginx
lynxdb ingest data.log --index production
cat events.json | lynxdb ingest
lynxdb ingest huge.log --batch-size 10000
```

---

### `import`

Bulk import structured data from files (NDJSON, CSV, Elasticsearch `_bulk` export).

```
lynxdb import <file> [flags]
```

Unlike `ingest` which handles raw log lines, `import` understands structured formats and preserves field types, timestamps, and metadata from the source system.

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `auto` | Input format: `auto`, `ndjson`, `csv`, `esbulk` |
| `--source` | | Source metadata for all events |
| `--index` | | Target index name |
| `--batch-size` | `5000` | Number of events per batch |
| `--dry-run` | `false` | Validate and count events without importing |
| `--transform` | | SPL2 pipeline to apply during import |
| `--delimiter` | `,` | Field delimiter for CSV format |

Format is auto-detected from file extension and content. Use `-` as the file argument to read from stdin (requires `--format`).

**Examples:**

```bash
# Import NDJSON (auto-detected)
lynxdb import events.json
lynxdb import events.ndjson

# Import CSV with headers
lynxdb import splunk_export.csv
lynxdb import data.csv --source web-01 --index nginx

# Import Elasticsearch _bulk export
lynxdb import es_dump.json --format esbulk

# Validate without importing
lynxdb import events.json --dry-run

# Apply SPL2 transform during import
lynxdb import events.json --transform '| where level!="DEBUG"'

# Import from stdin
cat events.ndjson | lynxdb import - --format ndjson

# Import TSV with tab delimiter
lynxdb import data.tsv --format csv --delimiter '\t'
```

---

## Quick Access Commands

### `count`

Quick event count shortcut. Faster than `query ... | stats count`.

```
lynxdb count [filter] [flags]
```

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range (e.g., `15m`, `1h`, `7d`) |

**Examples:**

```bash
lynxdb count                       # Count all events
lynxdb count 'level=error'         # Count errors
lynxdb count --since 1h            # Count events in last hour
```

---

### `sample`

Show a sample of recent events, useful for exploring data structure.

```
lynxdb sample [count] [filter] [flags]
```

The first argument is parsed as a number (sample size, default 5). Any remaining arguments are treated as an SPL2 filter.

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range |

**Examples:**

```bash
lynxdb sample                          # 5 random events (default)
lynxdb sample 10                       # 10 events
lynxdb sample 5 '_source=nginx'         # 5 nginx events
lynxdb sample 3 --format json | jq .   # JSON for inspecting structure
```

---

### `last`

Re-run the most recently executed query. Optionally override the time range or output format.

```
lynxdb last [flags]
```

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Override time range |

**Examples:**

```bash
lynxdb last                     # Repeat last query
lynxdb last --since 24h         # Same query, wider time range
lynxdb last -F csv              # Same query, CSV output
```

---

### `fields`

Show field catalog from server -- all known fields with types, coverage, and top values.

```
lynxdb fields [name] [flags]
```

**Alias:** `f`

When a field name is provided, shows details for that specific field (with fuzzy matching for typos). Use `--values` to see the top 50 values for a field.

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--values` | | `false` | Show top values for the specified field |
| `--since` | `-s` | | Restrict stats to time range (e.g., `15m`, `1h`, `7d`) |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |
| `--source` | | | Filter by source |
| `--prefix` | | | Filter fields by name prefix |

**Examples:**

```bash
lynxdb fields                             # All fields
lynxdb fields status                      # Detail for 'status' field
lynxdb fields status --values             # Top values for 'status'
lynxdb fields --source nginx              # Fields seen from nginx
lynxdb fields --prefix sta                # Autocomplete helper
lynxdb fields --since 1h                  # Fields seen in last hour
lynxdb fields --format json               # JSON output for scripting
```

**Console output:**

```
FIELD                     TYPE       COVERAGE   TOP VALUES
--------------------------------------------------------------------------------
_time                     timestamp    100%
host                      string       100%     web-01(40%), api-01(35%), db-01(25%)
level                     string        95%     INFO(70%), WARN(20%), ERROR(10%)
status                    number        80%     200(60%), 404(15%), 500(5%)

7 fields total
```

---

### `explain`

Show query execution plan without running the query.

```
lynxdb explain [SPL2 query] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--analyze` | `false` | Execute query and show plan with actual execution stats |

**Examples:**

```bash
lynxdb explain 'level=error | stats count by source'
lynxdb explain 'status>=500 | top 10 uri' --format json
lynxdb explain --analyze 'level=error | stats count'
```

**Console output:**

```
Plan:
  FROM → WHERE → STATS

Estimated cost: low

Fields read: level, source
```

---

### `examples`

Show a cookbook of common SPL2 query patterns.

```
lynxdb examples
```

**Alias:** `cookbook`

Displays categorized examples for Search & Filter, Aggregation, Time Analysis, Transformation, and Local File Queries.

---

## Real-Time Commands

### `tail`

Live tail logs from server via Server-Sent Events (SSE).

```
lynxdb tail [filter] [flags]
```

**Alias:** `t`

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--count` | `-n` | `100` | Number of historical events to fetch before live streaming |
| `--from` | | `-1h` | Historical lookback period |

**Examples:**

```bash
lynxdb tail                                    # Stream all events
lynxdb tail 'level=error'                      # Stream errors only
lynxdb tail '_source=nginx status>=500'         # Stream 5xx from nginx
lynxdb tail --count 50 --from -1h              # Last 50 events + live
```

**Console output:**

Events are colorized by level with timestamp, source, and message:

```
2026-01-15T14:23:01Z [ERROR] nginx: connection refused to upstream
2026-01-15T14:23:02Z [INFO] api-gateway: request handled in 45ms
--- historical catchup complete (47 events, 312ms) — streaming live ---
2026-01-15T14:23:03Z [ERROR] nginx: timeout exceeded
```

Press `Ctrl+C` to stop.

---

### `top`

Full-screen live TUI dashboard of server metrics.

```
lynxdb top [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `2s` | Refresh interval (e.g., `2s`, `5s`) |

Shows four panels: Ingest (rate, today, total), Queries (active, cache hit rate, views, tail sessions), Storage (size, segments, memtable, indexes), and Sources (bar chart of events by source).

Press `q` or `Ctrl+C` to quit.

**Examples:**

```bash
lynxdb top
lynxdb top --interval 5s
```

---

### `watch`

Re-run a query at regular intervals with a live-updating TUI display.

```
lynxdb watch [SPL2 query] [flags]
```

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--interval` | | `5s` | Refresh interval (e.g., `5s`, `30s`, `1m`) |
| `--since` | `-s` | `-15m` | Time range for each execution |
| `--diff` | | `false` | Show delta from previous run |

Press `q` or `Ctrl+C` to quit.

**Examples:**

```bash
lynxdb watch 'level=error | stats count by source'
lynxdb watch 'level=error | stats count' --interval 10s
lynxdb watch '| stats count by level' --since 1h --diff
```

---

### `diff`

Compare query results across two consecutive time periods. Useful for spotting trends and anomalies.

```
lynxdb diff [SPL2 query] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--period` | `1h` | Compare last N vs previous N (e.g., `1h`, `6h`, `24h`) |

**Examples:**

```bash
lynxdb diff 'level=error | stats count by source'
lynxdb diff 'level=error | stats count by source' --period 1h
lynxdb diff 'status>=500 | stats count by uri' --period 24h
```

**Console output:**

```
  Comparing: last 1h vs previous 1h

  source          NOW     PREV    CHANGE
  nginx           340     280     +21.4%
  api-gateway     120     150     -20.0%

  Total: 460 vs 430 (+7.0%)
```

---

## Server & Ops Commands

### `server`

Start the LynxDB server.

```
lynxdb server [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | (from config) | Listen address (overrides config) |
| `--data-dir` | (from config) | Root directory for data storage |
| `--s3-bucket` | | S3 bucket for warm/cold storage |
| `--s3-region` | | AWS region |
| `--s3-prefix` | | Key prefix in S3 |
| `--compaction-interval` | | Compaction check interval |
| `--tiering-interval` | | Tier evaluation interval |
| `--cache-max-mb` | | Max cache size (e.g., `1gb`, `512mb`) |
| `--log-level` | | Log level: debug, info, warn, error |
| `--auth` | `false` | Enable API key authentication |
| `--tls` | `false` | Enable TLS (auto-generates self-signed cert if no `--tls-cert`) |
| `--tls-cert` | | Path to TLS certificate PEM file |
| `--tls-key` | | Path to TLS private key PEM file |
| `--max-query-pool` | | Global query memory pool (e.g., `2gb`, `4gb`) |
| `--spill-dir` | | Directory for temporary spill files (default: OS temp dir) |

**Examples:**

```bash
# Start with defaults (localhost:3100)
lynxdb server

# Start with persistent storage and custom address
lynxdb server --addr 0.0.0.0:8080 --data-dir /var/lib/lynxdb

# In-memory mode (no persistence)
lynxdb server --data-dir ""

# With S3 tiering
lynxdb server --s3-bucket my-logs --s3-region eu-west-1

# With TLS and auth
lynxdb server --tls --auth --data-dir /var/lib/lynxdb

# With your own certificates
lynxdb server --tls-cert /etc/ssl/lynxdb.crt --tls-key /etc/ssl/lynxdb.key
```

**Startup output:**

```
  Config:  /home/user/.config/lynxdb/config.yaml
  Overrides: --addr, --log-level
  Data:    /var/lib/lynxdb
  Listen:  0.0.0.0:8080

time=2026-01-15T10:00:00.000Z level=INFO msg="starting LynxDB" version=0.1.0 addr=0.0.0.0:8080
```

When `--auth` is enabled and no keys exist, a root key is generated and displayed once:

```
  Auth enabled — no API keys exist. Generated root key:

    lxk_a1b2c3d4e5f6...

  Save this key now. It will NOT be shown again.
```

**Signals:**

- `SIGINT` / `SIGTERM` -- graceful shutdown (finish in-flight requests, flush, exit)
- `SIGHUP` -- hot-reload configuration from file

---

### `status`

Show detailed server status.

```
lynxdb status
```

**Alias:** `st`

Supports `--format json` for machine-readable output. Includes memory pool info when the unified buffer manager is active.

**Console output:**

```
  LynxDB v0.1.0 — uptime 2d 5h 30m — healthy

  Storage:     1.2 GB
  Events:      3,456,789 total    123,456 today
  Segments:    42    Memtable: 8200 events
  Sources:     nginx (45%), api-gateway (30%), postgres (25%)
  Oldest:      2025-01-08T10:30:00Z
  Indexes:     3
```

---

### `health`

Quick health check of a running server.

```
lynxdb health
```

Supports `--format json`.

---

### `indexes`

List all indexes on the server.

```
lynxdb indexes
```

---

### `cache`

Cache management (subcommand group).

#### `cache stats`

Show cache statistics (hits, misses, hit rate, entries, size, evictions).

```
lynxdb cache stats
```

Supports `--format json`.

#### `cache clear`

Clear the query cache.

```
lynxdb cache clear [--force]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--force` | `false` | Skip confirmation prompt |

---

### `config`

Show and manage configuration. When run without a subcommand, displays the resolved configuration with all overrides.

```
lynxdb config [subcommand] [flags]
```

**Persistent flags (available on config and all subcommands):**

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | | Listen address override |
| `--data-dir` | | Data directory override |
| `--s3-bucket` | | S3 bucket override |
| `--s3-region` | | AWS region override |
| `--s3-prefix` | | S3 prefix override |
| `--compaction-interval` | | Compaction interval override |
| `--tiering-interval` | | Tiering interval override |
| `--cache-max-mb` | | Cache max size override |
| `--log-level` | | Log level override |

#### `config init`

Create a default config file with all documented settings.

```
lynxdb config init [--system]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--system` | `false` | Write to `/etc/lynxdb/config.yaml` |

#### `config validate`

Validate config file and show non-default values. Warns on unknown keys.

```
lynxdb config validate
```

#### `config reload`

Send SIGHUP to a running server to reload its configuration.

```
lynxdb config reload
```

#### `config get`

Show a single config value.

```
lynxdb config get <key>
```

Tab-completes known config keys.

```bash
lynxdb config get retention
lynxdb config get storage.compression
```

#### `config set`

Set a config value in the config file.

```
lynxdb config set <key> <value> [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | `false` | Show what would change without applying |

```bash
lynxdb config set retention 30d
lynxdb config set storage.compression zstd
lynxdb config set retention 1d --dry-run
```

#### `config edit`

Open config file in `$EDITOR`. Creates the file with defaults if it doesn't exist.

```
lynxdb config edit
```

#### `config path`

Print the config file path.

```
lynxdb config path
```

#### `config reset`

Reset config file to defaults (with confirmation).

```
lynxdb config reset [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | `false` | Show what would be reset without applying |

#### `config add-profile`

Add or update a connection profile for quick server switching.

```
lynxdb config add-profile <name> --url <url> [--token <token>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | (required) | Server URL for the profile |
| `--token` | | Authentication token (optional) |

```bash
lynxdb config add-profile prod --url https://lynxdb.company.com --token xxx
lynxdb config add-profile staging --url https://staging.company.com
```

#### `config list-profiles`

List all connection profiles.

```
lynxdb config list-profiles
```

#### `config remove-profile`

Remove a connection profile.

```
lynxdb config remove-profile <name>
```

---

### `doctor`

Run environment diagnostics. Checks binary, config, data directory, server connectivity, disk space, retention, and shell completion.

```
lynxdb doctor
```

Supports `--format json`.

**Console output:**

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

---

### `init`

Interactive setup wizard for first-time LynxDB configuration. Creates a config file and data directory.

```
lynxdb init [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--data-dir` | | Data directory (skips interactive prompt) |
| `--retention` | | Retention period, e.g. `7d`, `30d`, `90d` (skips prompt) |
| `--no-interactive` | `false` | Non-interactive mode (use defaults + flags) |

In interactive mode (default when stdin is a TTY), prompts for data directory and retention period. In non-interactive mode, uses flag values or built-in defaults.

**Examples:**

```bash
# Interactive wizard
lynxdb init

# Non-interactive (CI/automation)
lynxdb init --data-dir /data/lynxdb --retention 30d --no-interactive

# Just set retention, prompt for the rest
lynxdb init --retention 90d
```

---

## Saved Queries

### `saved`

Manage saved queries (subcommand group). Without a subcommand, lists all saved queries.

```
lynxdb saved
```

#### `saved create`

```
lynxdb saved create <name> <query>
```

#### `saved run`

```
lynxdb saved run <name> [--since] [--from] [--to]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |

Tab-completes saved query names.

#### `saved delete`

```
lynxdb saved delete <name> [--force]
```

### `save`

Shortcut for `saved create`.

```
lynxdb save <name> <query>
```

```bash
lynxdb save "5xx-rate" '_source=nginx status>=500 | stats count by uri | sort -count'
```

### `run`

Shortcut for `saved run`.

```
lynxdb run <name> [--since] [--from] [--to]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |

```bash
lynxdb run 5xx-rate
lynxdb run 5xx-rate --since 24h
lynxdb run 5xx-rate --format csv > report.csv
```

---

## Materialized Views

### `mv`

Manage materialized views (subcommand group).

```
lynxdb mv <subcommand>
```

#### `mv create`

```
lynxdb mv create <name> <query> [--retention <duration>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--retention` | | Retention period (e.g., `30d`, `90d`) |

```bash
lynxdb mv create errors_by_host 'FROM main | where level="ERROR" | stats count by host'
lynxdb mv create daily_summary 'FROM main | stats count by source' --retention 90d
```

#### `mv list`

```
lynxdb mv list
```

Supports `--format json`.

#### `mv status`

```
lynxdb mv status <name>
```

Tab-completes view names. Supports `--format json`.

#### `mv drop`

```
lynxdb mv drop <name> [--force] [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--force` | `false` | Skip confirmation prompt |
| `--dry-run` | `false` | Show what would be deleted without applying |

#### `mv pause`

Pause a materialized view pipeline.

```
lynxdb mv pause <name>
```

#### `mv resume`

Resume a paused materialized view pipeline.

```
lynxdb mv resume <name>
```

---

## Alerts

### `alerts`

Manage alerts (subcommand group). Without a subcommand, lists all alerts. With an ID argument, shows alert details.

```
lynxdb alerts [id]
```

#### `alerts create`

```
lynxdb alerts create --name <name> --query <query> [--interval <duration>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | (required) | Alert name |
| `--query` | (required) | SPL2 query |
| `--interval` | `5m` | Check interval |

```bash
lynxdb alerts create --name "High errors" \
  --query 'level=error | stats count as errors | where errors > 100' \
  --interval 5m
```

#### `alerts test`

Test alert evaluation without sending notifications.

```
lynxdb alerts test <id>
```

#### `alerts test-channels`

Send a test notification to all configured channels.

```
lynxdb alerts test-channels <id>
```

#### `alerts enable`

```
lynxdb alerts enable <id>
```

#### `alerts disable`

```
lynxdb alerts disable <id>
```

#### `alerts delete`

```
lynxdb alerts delete <id> [--force]
```

---

## Dashboards

### `dashboards`

Manage dashboards (subcommand group). Without a subcommand, lists all dashboards. With an ID argument, shows dashboard details.

```
lynxdb dashboards [id]
```

**Alias:** `dash`

#### `dashboards create`

Create a dashboard from a JSON file.

```
lynxdb dashboards create --file <path>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | (required) | Path to dashboard JSON file |

#### `dashboards open`

Open a dashboard in the Web UI.

```
lynxdb dashboards open <id>
```

#### `dashboards export`

Export a dashboard as JSON to stdout.

```
lynxdb dashboards export <id>
```

```bash
lynxdb dashboards export dash_abc123 > dashboard-backup.json
```

#### `dashboards delete`

```
lynxdb dashboards delete <id> [--force]
```

---

## Authentication

### `login`

Authenticate to a LynxDB server. Prompts for an API key interactively (hidden input) or accepts `--token` for non-interactive use.

For HTTPS servers with self-signed certificates, implements Trust-On-First-Use (TOFU): the certificate fingerprint is displayed and saved after user confirmation.

```
lynxdb login [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--token` | | API key (non-interactive) |

**Examples:**

```bash
lynxdb login                                              # Interactive prompt
lynxdb login --server https://lynxdb.company.com          # Specify server
lynxdb login --token "$LYNXDB_TOKEN"                      # Non-interactive
```

---

### `logout`

Remove saved credentials for a server.

```
lynxdb logout [flags]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--all` | `false` | Remove ALL saved credentials |

**Examples:**

```bash
lynxdb logout                                                  # Current server
lynxdb logout --server https://lynxdb.company.com             # Specific server
lynxdb logout --all                                            # All servers
```

---

### `auth`

Manage API keys on the server (subcommand group). Requires an authenticated connection.

```
lynxdb auth <subcommand>
```

#### `auth create-key`

Create a new API key.

```
lynxdb auth create-key --name <name>
```

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | (required) | Human-readable name for the key |

The token is displayed once and cannot be retrieved again.

```bash
lynxdb auth create-key --name ci-pipeline
lynxdb auth create-key --name grafana --server https://lynxdb.prod.com
```

#### `auth list-keys`

List all API keys (ID, name, prefix, created, last used).

```
lynxdb auth list-keys
```

**Alias:** `ls`

#### `auth revoke-key`

Revoke an API key.

```
lynxdb auth revoke-key <id> [-y]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--yes` | `-y` | `false` | Skip confirmation prompt |

#### `auth rotate-root`

Generate a new root key and revoke the current one. Auto-updates the local credentials file.

```
lynxdb auth rotate-root [-y]
```

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--yes` | `-y` | `false` | Skip confirmation prompt |

#### `auth status`

Show current authentication state (server, TLS mode, credential status).

```
lynxdb auth status
```

---

### `jobs`

List or manage async query jobs.

```
lynxdb jobs [job_id] [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--status` | | Filter by status: `running`, `done`, `error` |
| `--cancel` | `false` | Cancel a running job |

**Examples:**

```bash
lynxdb jobs                            # List all jobs
lynxdb jobs --status running           # Only running jobs
lynxdb jobs qry_9c1d4e                 # Show specific job
lynxdb jobs qry_9c1d4e --cancel        # Cancel a running job
```

---

## Interactive

### `shell`

Start an interactive SPL2 REPL with tab completion, query history, and dot commands.

```
lynxdb shell [flags]
```

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--file` | `-f` | | File mode (load file into ephemeral engine) |
| `--since` | `-s` | `15m` | Default time range for queries |

**Features:**

- Tab completion for SPL2 commands, aggregation functions, and field names
- Query history with arrow keys (persisted to `~/.local/share/lynxdb/history`)
- Multi-line input: end a line with `|` to continue on the next line
- Dot commands for shell meta-operations

**Dot commands:**

| Command | Description |
|---------|-------------|
| `.help` | Show help |
| `.quit` / `.exit` | Exit the shell (or Ctrl+D) |
| `.clear` | Clear the screen |
| `.history` | Show query history |
| `.fields` | List known fields (server mode) |
| `.sources` | List event sources (server mode) |
| `.explain <query>` | Show query execution plan |
| `.set since <val>` | Change default time range |

**Examples:**

```bash
lynxdb shell                          # Connect to server
lynxdb shell --file access.log        # Query a local file
lynxdb shell --since 1h               # Default time range
```

**Console output:**

```
  LynxDB v0.1.0 — Interactive Shell
  Connected to http://localhost:3100
  Type .help for commands, Ctrl+D to exit.

lynxdb> level=error | stats count by source
  source          count
  nginx           340
  api-gateway     120

  2 rows — 45ms

lynxdb>
```

---

## Browser

### `ui`

Open the LynxDB Web UI in the default browser.

```
lynxdb ui
```

```bash
lynxdb ui
lynxdb ui --server https://prod:3100
```

---

### `open`

Open a query in the Web UI (constructs a search URL and launches the browser).

```
lynxdb open [query] [flags]
```

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |

```bash
lynxdb open 'level=error | stats count by source'
lynxdb open 'level=error | stats count by source' --since 1h
```

---

### `share`

Generate a shareable URL for a query (prints to stdout, no browser launch).

```
lynxdb share [query] [flags]
```

**Flags:**

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |

```bash
lynxdb share 'level=error | stats count by source' --since 1h
# http://localhost:3100/search?from=-1h&q=level%3Derror+%7C+stats+count+by+source
```

---

## Performance

### `bench`

Run a local performance benchmark. Generates synthetic events and measures ingest + query performance. No server needed -- uses an ephemeral in-memory engine.

```
lynxdb bench [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--events` | `100000` | Number of events to generate |

**Examples:**

```bash
lynxdb bench
lynxdb bench --events 1000000
```

**Console output:**

```
  LynxDB Benchmark — 100,000 events
  ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
  Generating events... 100000 lines

  Ingest:  100,000 events in 245ms (408,163 events/sec)

  QUERY                                         RESULTS       TIME
  Filtered aggregate                                  1       12ms
  Full scan aggregate                                 5        8ms
  Full-text search                                  340       15ms
  Range filter + top                                 10       11ms
  Time bucketed                                      96       14ms

  Done.
```

---

### `demo`

Run a live demo that continuously generates realistic log events from 4 sources (nginx, api-gateway, postgres, redis). Uses an in-memory engine. No server needed.

```
lynxdb demo [flags]
```

**Flags:**

| Flag | Default | Description |
|------|---------|-------------|
| `--rate` | `200` | Events per second |

**Examples:**

```bash
lynxdb demo
lynxdb demo --rate 500
```

Press `Ctrl+C` to stop.

---

## Utility

### `version`

Print version information.

```
lynxdb version
```

**Console output:**

```
LynxDB v0.1.0 (abc1234) built 2026-01-15T10:00:00Z
Go: go1.25.4 darwin/arm64
```

---

### `completion`

Generate shell completion scripts.

```
lynxdb completion [bash|zsh|fish|powershell]
```

**Examples:**

```bash
# Bash
lynxdb completion bash >> ~/.bashrc

# Zsh
lynxdb completion zsh >> ~/.zshrc

# Fish
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish

# PowerShell
lynxdb completion powershell | Out-String | Invoke-Expression
```

---

## Output Formats

Controlled by the `--format` / `-F` global flag:

| Format | Description | When used with `auto` |
|--------|---------------------------------------------|---------------------------------|
| `auto` | Auto-detect based on context | (default) |
| `json` | Pretty-printed JSON (one object per line) | Pipe (non-TTY) |
| `ndjson`| Newline-delimited JSON | Never auto-selected |
| `table` | Aligned columns with headers and separator | TTY + multiple results |
| `csv` | RFC 4180 CSV with header row | Only when explicitly set |
| `tsv` | Tab-separated values with header row | Only when explicitly set |
| `raw` | `_raw` field value per line, or tab-separated k=v | Only when explicitly set |

**`auto` behavior:**

- TTY + single scalar result -> plain value
- TTY + multiple results -> colorized JSON with numbered results
- Non-TTY (pipe) -> `json`

The `NO_COLOR` environment variable disables colored output when set to any non-empty value.

---

## Pipe & Stdin Integration

LynxDB detects whether stdin is a pipe (not a terminal) automatically. When a pipe is detected:

- `lynxdb query` reads data from stdin into an ephemeral engine, then executes the query.
- `lynxdb ingest` reads lines from stdin and sends them in batches to the server.

### Query pipeline examples

```bash
# Filter logs before querying
grep "2026-01-15" /var/log/app.log | lynxdb query '| stats count by level'

# Decompress and query
zcat archive.log.gz | lynxdb query '| where status>=500 | top 10 path'

# Chain with other tools
curl -s http://api/logs | lynxdb query '| stats count by endpoint' --format json | jq '.count'

# Tail-and-query (will process until stdin EOF)
tail -1000 /var/log/app.log | lynxdb query '| where level="ERROR"'
```

### Output piping

When stdout is not a terminal, `lynxdb query` outputs newline-delimited JSON regardless of `--format auto`, making it easy to compose with other tools:

```bash
# Pipe query results to jq
lynxdb query 'FROM main | stats count by host' | jq '.host'

# Export to file
lynxdb query 'FROM main | where level="ERROR"' --since 24h > errors.json

# Chain queries (query server, then local post-process)
lynxdb query 'FROM main | where status>=500' --since 1h \
  | lynxdb query '| stats count by path'

# Export as CSV
lynxdb query 'FROM main | stats count by host' --format csv > report.csv
```

### Metadata to stderr

Summary/stats lines are always written to **stderr**, so they don't pollute piped output:

```bash
# Only JSON goes to file; stats go to terminal
lynxdb query --file access.log '| stats count' > result.json
# stderr shows: Scanned 50,000 events | 1 results | 89ms
```

---

## Configuration

### Precedence (highest to lowest)

1. CLI flags (`--addr`, `--data-dir`, etc.)
2. Environment variables (`LYNXDB_LISTEN`, `LYNXDB_DATA_DIR`, etc.)
3. Project file (`.lynxdbrc` in current or parent directory)
4. Config file (YAML)
5. Compiled defaults

### Config file search order

When `--config` is not specified:

1. `LYNXDB_CONFIG` environment variable
2. `./lynxdb.yaml` (current directory)
3. `$XDG_CONFIG_HOME/lynxdb/config.yaml` (or `~/.config/lynxdb/config.yaml`)
4. `~/.lynxdb/config.yaml`
5. `/etc/lynxdb/config.yaml`

The first file found is used. If none exist, compiled defaults apply.

### Default data directory

- `$XDG_DATA_HOME/lynxdb` if `XDG_DATA_HOME` is set
- `~/.local/share/lynxdb` otherwise
- `.lynxdb/data` as last resort

### Project file (`.lynxdbrc`)

A `.lynxdbrc` YAML file in the current or any parent directory can set per-project defaults:

```yaml
server: https://staging.company.com
format: table
profile: staging
```

These are applied as defaults and are overridden by explicit CLI flags.

### Example config file (`lynxdb.yaml`)

```yaml
listen: "localhost:3100"
data_dir: "/var/lib/lynxdb"
retention: "7d"
log_level: "info"

storage:
  compression: "lz4"
  row_group_size: 65536
  flush_threshold: "512mb"
  compaction_interval: "30s"
  compaction_workers: 2
  s3_bucket: ""
  s3_region: "us-east-1"
  cache_max_bytes: "1gb"
  cache_ttl: "5m"

query:
  sync_timeout: "30s"
  max_query_runtime: "5m"
  max_concurrent: 10
  default_result_limit: 1000
  max_result_limit: 50000

ingest:
  max_body_size: "10mb"
  max_batch_size: 1000

http:
  idle_timeout: "2m"
  shutdown_timeout: "30s"
```

### Hot-reloadable settings (via SIGHUP or `config reload`)

These settings take effect without restarting the server:

- `log_level`
- `retention`
- `query.max_concurrent`
- `query.default_result_limit`
- `query.max_result_limit`
- `query.max_query_runtime`

Settings that require a restart (server will warn on reload):

- `listen`
- `data_dir`

---

## Environment Variables

Every config key can be overridden with an environment variable. Prefix: `LYNXDB_`.

### Client

| Env Var | Description |
|---------|-------------|
| `LYNXDB_SERVER` | Default server address |
| `LYNXDB_TOKEN` | API key for authentication |
| `LYNXDB_PROFILE` | Default connection profile |
| `LYNXDB_TLS_SKIP_VERIFY` | Skip TLS cert verification (`true`/`1`/`yes`) |
| `LYNXDB_CONFIG` | Path to config file |
| `NO_COLOR` | Disable colored output (any non-empty value) |

### Server

| Env Var | Config Key |
|---------|------------|
| `LYNXDB_LISTEN` | `listen` |
| `LYNXDB_DATA_DIR` | `data_dir` |
| `LYNXDB_RETENTION` | `retention` |
| `LYNXDB_LOG_LEVEL` | `log_level` |

### Storage

| Env Var | Config Key |
|---------|------------|
| `LYNXDB_STORAGE_COMPRESSION` | `storage.compression` |
| `LYNXDB_STORAGE_ROW_GROUP_SIZE` | `storage.row_group_size` |
| `LYNXDB_STORAGE_WAL_SYNC_INTERVAL` | `storage.wal_sync_interval` |
| `LYNXDB_STORAGE_WAL_SYNC_BYTES` | `storage.wal_sync_bytes` |
| `LYNXDB_STORAGE_WAL_MAX_SEGMENT_SIZE` | `storage.wal_max_segment_size` |
| `LYNXDB_STORAGE_WAL_SYNC_MODE` | `storage.wal_sync_mode` |
| `LYNXDB_STORAGE_FLUSH_THRESHOLD` | `storage.flush_threshold` |
| `LYNXDB_STORAGE_MEMTABLE_SHARDS` | `storage.memtable_shards` |
| `LYNXDB_STORAGE_MAX_IMMUTABLE` | `storage.max_immutable` |
| `LYNXDB_STORAGE_COMPACTION_INTERVAL` | `storage.compaction_interval` |
| `LYNXDB_STORAGE_COMPACTION_WORKERS` | `storage.compaction_workers` |
| `LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB` | `storage.compaction_rate_limit_mb` |
| `LYNXDB_STORAGE_L0_THRESHOLD` | `storage.l0_threshold` |
| `LYNXDB_STORAGE_L1_THRESHOLD` | `storage.l1_threshold` |
| `LYNXDB_STORAGE_L2_TARGET_SIZE` | `storage.l2_target_size` |
| `LYNXDB_STORAGE_S3_BUCKET` | `storage.s3_bucket` |
| `LYNXDB_STORAGE_S3_REGION` | `storage.s3_region` |
| `LYNXDB_STORAGE_S3_PREFIX` | `storage.s3_prefix` |
| `LYNXDB_STORAGE_S3_ENDPOINT` | `storage.s3_endpoint` |
| `LYNXDB_STORAGE_S3_FORCE_PATH_STYLE` | `storage.s3_force_path_style` |
| `LYNXDB_STORAGE_TIERING_INTERVAL` | `storage.tiering_interval` |
| `LYNXDB_STORAGE_TIERING_PARALLELISM` | `storage.tiering_parallelism` |
| `LYNXDB_STORAGE_SEGMENT_CACHE_SIZE` | `storage.segment_cache_size` |
| `LYNXDB_STORAGE_CACHE_MAX_BYTES` | `storage.cache_max_bytes` |
| `LYNXDB_STORAGE_CACHE_TTL` | `storage.cache_ttl` |

### Query

| Env Var | Config Key |
|---------|------------|
| `LYNXDB_QUERY_SYNC_TIMEOUT` | `query.sync_timeout` |
| `LYNXDB_QUERY_JOB_TTL` | `query.job_ttl` |
| `LYNXDB_QUERY_JOB_GC_INTERVAL` | `query.job_gc_interval` |
| `LYNXDB_QUERY_MAX_CONCURRENT` | `query.max_concurrent` |
| `LYNXDB_QUERY_DEFAULT_RESULT_LIMIT` | `query.default_result_limit` |
| `LYNXDB_QUERY_MAX_RESULT_LIMIT` | `query.max_result_limit` |

### Ingest

| Env Var | Config Key |
|---------|------------|
| `LYNXDB_INGEST_MAX_BODY_SIZE` | `ingest.max_body_size` |
| `LYNXDB_INGEST_MAX_BATCH_SIZE` | `ingest.max_batch_size` |

### HTTP

| Env Var | Config Key |
|---------|------------|
| `LYNXDB_HTTP_IDLE_TIMEOUT` | `http.idle_timeout` |
| `LYNXDB_HTTP_SHUTDOWN_TIMEOUT` | `http.shutdown_timeout` |

**Example:**

```bash
LYNXDB_LISTEN=0.0.0.0:4000 LYNXDB_LOG_LEVEL=debug lynxdb server
```

---

## TUI Mode

When `lynxdb query` runs against a server (not file/stdin) and stdout is a terminal with `--format auto`, it launches an interactive TUI.

**Features:**

- Animated spinner during query execution
- Live progress reporting (phase, segment scan progress, rows read, skip stats)
- Colorized JSON output with syntax highlighting
- Numbered results (`#1`, `#2`, ...)
- Detailed stats footer (results, scanned events, filter ratio, segments, memtable, timing)
- `Ctrl+C` to cancel

**Query execution flow in TUI mode:**

1. Job submitted to server in async mode (`wait: 0`)
2. TUI polls `/api/v1/query/jobs/{id}` every 80ms for progress updates
3. Progress is displayed in real-time (phase, segments scanned/skipped, rows read)
4. On completion, results and stats are rendered

**To disable TUI** (force plain output even in terminal):

```bash
lynxdb query 'FROM main | stats count' --format json
```

Or pipe through cat:

```bash
lynxdb query 'FROM main | stats count' | cat
```

---

## Exit Codes

| Code | Name | Meaning |
|------|------|---------|
| 0 | OK | Command completed successfully |
| 1 | General | Unspecified failure |
| 2 | Usage | Invalid flags or missing arguments |
| 3 | Connection | Cannot reach server |
| 4 | QueryParse | Bad SPL2 syntax |
| 5 | QueryTimeout | Server timeout or `--timeout` exceeded |
| 6 | NoResults | Query returned 0 results (with `--fail-on-empty`) |
| 7 | Auth | Missing or invalid authentication token |
| 10 | Aborted | User declined destructive action confirmation |
| 124 | Timeout | Generic timeout (GNU convention) |
| 130 | Interrupted | User pressed Ctrl+C (SIGINT) |

**Usage in scripts:**

```bash
# Check for data
lynxdb query 'level=FATAL' --fail-on-empty 2>/dev/null
case $? in
  0) echo "Fatal errors found!" ;;
  6) echo "No fatal errors" ;;
  3) echo "Server unreachable" ;;
  *) echo "Unexpected error" ;;
esac
```

---

## Signal Handling

### Server (`lynxdb server`)

| Signal | Action |
|--------|--------|
| `SIGINT` | Graceful shutdown (finish in-flight requests, flush, exit) |
| `SIGTERM` | Graceful shutdown (same as SIGINT) |
| `SIGHUP` | Hot-reload configuration from file |

### Demo (`lynxdb demo`)

| Signal | Action |
|--------|--------|
| `SIGINT` | Stop generation, print summary, exit |
| `SIGTERM` | Stop generation, print summary, exit |

### Query / Tail / Watch / Top

| Signal | Action |
|--------|--------|
| `SIGINT` | Cancel current operation, exit |

---

## SPL Compatibility Hints

When using file/stdin query mode, LynxDB detects common Splunk SPL1 syntax and prints compatibility hints to stderr:

```
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

This helps users transitioning from Splunk. The `examples` command provides a full SPL2 cookbook.
