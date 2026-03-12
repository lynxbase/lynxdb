---
sidebar_position: 10
title: Interactive Shell
description: LynxDB interactive SPL2 REPL with tab completion, query history, and dot commands.
---

# Interactive Shell

Start an interactive SPL2 REPL with tab completion, query history, and dot commands.

```
lynxdb shell [flags]
```

## Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--file` | `-f` | | File mode (load file into ephemeral engine) |
| `--since` | `-s` | `15m` | Default time range for queries |

## Examples

```bash
# Connect to server
lynxdb shell

# Query a local file
lynxdb shell --file access.log

# Default time range
lynxdb shell --since 1h
```

## Console Output

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

## Features

### Tab Completion

The shell tab-completes:

- SPL2 command names (`stats`, `where`, `eval`, `sort`, etc.)
- Aggregation function names (`count`, `avg`, `sum`, `p99`, etc.)
- Field names from the server's field catalog

### Query History

- Navigate previous queries with the up/down arrow keys
- History is persisted to `~/.local/share/lynxdb/history`
- History survives between shell sessions

### Multi-Line Input

End a line with `|` to continue on the next line:

```
lynxdb> level=error |
   ...> stats count by source |
   ...> sort -count |
   ...> head 10
```

## Dot Commands

Meta-operations available inside the shell:

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

### Examples

```
lynxdb> .fields
FIELD          TYPE       COVERAGE
_time          timestamp    100%
host           string       100%
level          string        95%

lynxdb> .set since 1h
Default time range set to 1h

lynxdb> .explain level=error | stats count by source
Plan:
  FROM -> WHERE -> STATS
Estimated cost: low
Fields read: level, source

lynxdb> .history
  1  level=error | stats count by source
  2  source=nginx | top 10 uri
  3  | timechart count span=5m

lynxdb> .quit
```

## File Mode

When started with `--file`, the shell loads the file into an ephemeral in-memory engine. All queries run locally without a server:

```bash
lynxdb shell --file /var/log/nginx/access.log
```

```
  LynxDB v0.1.0 — Interactive Shell
  Loaded: /var/log/nginx/access.log (50,000 events)
  Type .help for commands, Ctrl+D to exit.

lynxdb> | stats count by status
  status    count
  200       45000
  404       3000
  500       2000
```

## See Also

- [query](/docs/cli/query) for one-shot queries
- [Shortcuts](/docs/cli/shortcuts) for quick access commands
- [Lynx Flow Reference](/docs/lynx-flow/overview) for the query language reference
