---
sidebar_position: 1
title: CLI Overview
description: LynxDB CLI modes of operation, global flags, TTY behavior, and output format auto-detection.
---

# CLI Overview

LynxDB ships as a single binary that covers every workflow -- pipe-mode analytics, persistent server, cluster node, interactive shell, and admin tooling. No separate installers, no plugins.

```
lynxdb [command] [flags]
```

## Modes of Operation

The CLI works in three distinct modes depending on the command and context:

### 1. Server mode

```bash
lynxdb server
```

Starts a persistent HTTP server. Data is stored on disk (or in-memory if `data_dir` is empty). Other commands (`query`, `ingest`, `status`, etc.) connect to this server over HTTP.

### 2. Local file mode

```bash
lynxdb query --file access.log '| stats count by status'
```

Queries run directly against local files without a running server. LynxDB creates an ephemeral in-memory engine, ingests the file(s), executes the query, and exits.

### 3. Stdin pipe mode

```bash
cat app.log | lynxdb query '| stats count by level'
```

Same as file mode, but reads data from stdin. Detected automatically when stdin is not a terminal. No server required.

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

## TTY Detection and Output Behavior

LynxDB adjusts its output based on whether stdout is a terminal (TTY) or a pipe:

**TTY (interactive terminal):**

- `--format auto` renders colorized JSON with numbered results and a stats footer
- Query commands show a live progress spinner with segment scan stats
- Commands like `ingest` show progress bars

**Pipe (non-TTY):**

- `--format auto` outputs newline-delimited JSON (one object per line)
- No colors, no spinners, no progress bars
- Stats and metadata go to stderr so they do not pollute piped data

```bash
# TTY -- colorized JSON with stats
lynxdb query 'level=error | stats count by source'

# Pipe -- clean NDJSON to jq
lynxdb query 'level=error | stats count by source' | jq '.source'

# Force a specific format regardless of TTY
lynxdb query 'level=error | stats count by source' --format table
```

The `NO_COLOR` environment variable disables colored output when set to any non-empty value.

To disable the TUI and get plain output in a terminal:

```bash
lynxdb query 'FROM main | stats count' --format json
# or pipe through cat:
lynxdb query 'FROM main | stats count' | cat
```

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
lynxdb query 'level=FATAL' --fail-on-empty 2>/dev/null
case $? in
  0) echo "Fatal errors found!" ;;
  6) echo "No fatal errors" ;;
  3) echo "Server unreachable" ;;
  *) echo "Unexpected error" ;;
esac
```

## Signal Handling

| Context | Signal | Action |
|---------|--------|--------|
| `lynxdb server` | `SIGINT` / `SIGTERM` | Graceful shutdown (finish in-flight requests, flush, exit) |
| `lynxdb server` | `SIGHUP` | Hot-reload configuration from file |
| `lynxdb demo` | `SIGINT` / `SIGTERM` | Stop generation, print summary, exit |
| `lynxdb query` / `tail` / `watch` / `top` | `SIGINT` | Cancel current operation, exit |

## Command Map

| Category | Commands |
|----------|----------|
| Core data | [`query`](/docs/cli/query), [`ingest`](/docs/cli/ingest), [`import`](/docs/cli/ingest#import) |
| Real-time | [`tail`](/docs/cli/tail), [`top`](/docs/cli/tail#top), [`watch`](/docs/cli/tail#watch), [`diff`](/docs/cli/tail#diff) |
| Quick access | [`count`](/docs/cli/shortcuts#count), [`sample`](/docs/cli/shortcuts#sample), [`last`](/docs/cli/shortcuts#last), [`fields`](/docs/cli/shortcuts#fields), [`explain`](/docs/cli/shortcuts#explain), [`examples`](/docs/cli/shortcuts#examples) |
| Server & ops | [`server`](/docs/cli/server), `status`, `health`, `indexes`, `cache` |
| Materialized views | [`mv`](/docs/cli/mv) |
| Alerts | [`alerts`](/docs/cli/alerts) |
| Configuration | [`config`](/docs/cli/config-cmd), [`doctor`](/docs/cli/config-cmd#doctor) |
| Interactive | [`shell`](/docs/cli/shell) |
| Performance | [`bench`](/docs/cli/bench-demo), [`demo`](/docs/cli/bench-demo#demo) |
| Output | [`--format`](/docs/cli/output-formats) |
| Setup | [`install`](/docs/cli/install), [`uninstall`](/docs/cli/install#uninstall) |
| Completion | [`completion`](/docs/cli/completion) |

## SPL Compatibility Hints

When using file/stdin query mode, LynxDB detects common Splunk SPL1 syntax and prints compatibility hints to stderr:

```
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

This helps users transitioning from Splunk. Run `lynxdb examples` for a full SPL2 cookbook.
