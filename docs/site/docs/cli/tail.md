---
sidebar_position: 4
title: "Real-Time: tail, top, watch, diff"
description: Real-time CLI commands for live log tailing, server monitoring, query watching, and period comparison.
---

# Real-Time Commands

Commands for live monitoring and time-based comparison: `tail`, `top`, `watch`, and `diff`.

## tail

Live tail logs from server via Server-Sent Events (SSE).

```
lynxdb tail [filter] [flags]
```

**Alias:** `t`

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--count` | `-n` | `100` | Number of historical events to fetch before live streaming |
| `--from` | | `-1h` | Historical lookback period |

### Examples

```bash
# Stream all events
lynxdb tail

# Stream errors only
lynxdb tail 'level=error'

# Stream 5xx from nginx
lynxdb tail 'source=nginx status>=500'

# Last 50 events + live
lynxdb tail --count 50 --from -1h
```

### Console Output

Events are colorized by level with timestamp, source, and message:

```
2026-01-15T14:23:01Z [ERROR] nginx: connection refused to upstream
2026-01-15T14:23:02Z [INFO] api-gateway: request handled in 45ms
--- historical catchup complete (47 events, 312ms) — streaming live ---
2026-01-15T14:23:03Z [ERROR] nginx: timeout exceeded
```

Press `Ctrl+C` to stop.

---

## top

Full-screen live TUI dashboard of server metrics.

```
lynxdb top [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--interval` | `2s` | Refresh interval (e.g., `2s`, `5s`) |

Shows four panels:

| Panel | Metrics |
|-------|---------|
| **Ingest** | Rate, today count, total count |
| **Queries** | Active, cache hit rate, views, tail sessions |
| **Storage** | Size, segments, memtable, indexes |
| **Sources** | Bar chart of events by source |

Press `q` or `Ctrl+C` to quit.

### Examples

```bash
lynxdb top
lynxdb top --interval 5s
```

---

## watch

Re-run a query at regular intervals with a live-updating TUI display.

```
lynxdb watch [SPL2 query] [flags]
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--interval` | | `5s` | Refresh interval (e.g., `5s`, `30s`, `1m`) |
| `--since` | `-s` | `-15m` | Time range for each execution |
| `--diff` | | `false` | Show delta from previous run |

Press `q` or `Ctrl+C` to quit.

### Examples

```bash
# Watch error counts by source, refresh every 5s
lynxdb watch 'level=error | stats count by source'

# Custom interval
lynxdb watch 'level=error | stats count' --interval 10s

# Show changes between refreshes
lynxdb watch '| stats count by level' --since 1h --diff
```

---

## diff

Compare query results across two consecutive time periods. Useful for spotting trends and anomalies.

```
lynxdb diff [SPL2 query] [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--period` | `1h` | Compare last N vs previous N (e.g., `1h`, `6h`, `24h`) |

### Examples

```bash
# Compare error counts: last hour vs previous hour
lynxdb diff 'level=error | stats count by source'

# Explicit period
lynxdb diff 'level=error | stats count by source' --period 1h

# Compare 5xx over 24h
lynxdb diff 'status>=500 | stats count by uri' --period 24h
```

### Console Output

```
  Comparing: last 1h vs previous 1h

  source          NOW     PREV    CHANGE
  nginx           340     280     +21.4%
  api-gateway     120     150     -20.0%

  Total: 460 vs 430 (+7.0%)
```

## See Also

- [query](/docs/cli/query) for one-shot queries
- [Shortcuts](/docs/cli/shortcuts) for quick access commands like `count` and `sample`
- [Interactive Shell](/docs/cli/shell) for an interactive REPL
