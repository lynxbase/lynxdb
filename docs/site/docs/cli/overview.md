---
sidebar_position: 1
title: CLI Overview
description: LynxDB CLI modes, global flags, root-command behavior, and the full command map.
---

# CLI Overview

LynxDB ships as a single binary. The same executable provides:

- ad hoc analytics on local files and stdin
- a persistent server
- a Web UI launcher
- administrative commands for auth, status, cache, config, jobs, and views

```
lynxdb [command] [flags]
```

## Modes of Operation

The CLI changes behavior depending on the command and on whether stdin is piped.

### Server mode

```bash
lynxdb server
```

Starts the HTTP server. Commands such as `query`, `ingest`, `status`, `fields`, `jobs`, `alerts`, and `mv` then talk to that server over HTTP.

### Local file mode

```bash
lynxdb query --file access.log '| stats count by status'
```

Queries files directly without a running server. LynxDB creates an ephemeral in-memory engine, ingests the matching file set, runs the query, prints results, and exits.

### Stdin pipe mode

```bash
cat app.log | lynxdb query '| stats count by level'
```

This is the same ephemeral execution path as file mode, but the input comes from stdin.

### Bare stdin mode

If stdin is piped and you run `lynxdb` with no subcommand, LynxDB defaults to a preview query:

```bash
cat app.log | lynxdb
```

That runs the equivalent of `| take 10` after format detection.

If stdin is piped and the first argument is not a known subcommand, LynxDB treats the remaining arguments as a query:

```bash
cat app.log | lynxdb 'level=error | stats count by source'
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

## Output Mode

With `--format auto`, LynxDB chooses a human-friendly format for interactive terminals and a machine-friendly format when stdout is piped. Use `--format` to force a stable format for scripts.

```bash
# Stable JSON for scripts
lynxdb query 'level=error | stats count by source' --format json

# Stable CSV export
lynxdb query 'level=error | stats count by source' --format csv

# Human-readable table output
lynxdb query 'level=error | stats count by source' --format table
```

`NO_COLOR` disables colored output when set.

## Exit Codes

The root command advertises these exit codes:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Usage error |
| 3 | Connection error |
| 4 | Query parse error |
| 5 | Query timeout |
| 6 | No results (`--fail-on-empty`) |
| 7 | Authentication error |
| 10 | Aborted by user |
| 130 | Interrupted |

## Command Map

Some commands have dedicated reference pages in this documentation set. Others are currently discoverable via `lynxdb <command> --help`.

| Category | Commands |
|----------|----------|
| Querying and ingest | [`query`](/docs/cli/query), [`ingest`](/docs/cli/ingest), `import`, [`tail`](/docs/cli/tail), `fields`, `count`, `sample`, `watch`, `diff`, `last`, `explain`, `examples` |
| Server and operations | [`server`](/docs/cli/server), `status`, `health`, `indexes`, `cache`, `jobs`, `doctor` |
| Saved objects | [`mv`](/docs/cli/mv), [`alerts`](/docs/cli/alerts), `saved`, `save`, `run`, `dashboards` |
| Authentication and connection | `login`, `logout`, `auth`, [`config`](/docs/cli/config-cmd) |
| Interactive and UI | [`shell`](/docs/cli/shell), `ui`, `open`, `share`, `top` |
| Setup and maintenance | `init`, [`install`](/docs/cli/install), `uninstall`, `upgrade`, `version`, [`completion`](/docs/cli/completion) |
| Diagnostics and misc | [`bench`](/docs/cli/bench-demo), `demo`, `grammar`, `explain-error` |

## SPL Compatibility Hints

When using file or stdin query mode, LynxDB detects several common Splunk-style patterns and prints compatibility hints to stderr.

Example:

```
hint: "index=main" is Splunk SPL syntax. In LynxDB SPL2, use "FROM main" instead.
```

Run `lynxdb examples` for a built-in query cookbook.

## Related

- [Query Command](/docs/cli/query)
- [Server Command](/docs/cli/server)
- [config & doctor](/docs/cli/config-cmd)
- [Output Formats](/docs/cli/output-formats)
