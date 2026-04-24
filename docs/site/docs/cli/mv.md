---
sidebar_position: 7
title: mv (Materialized Views)
description: Create, manage, pause, and drop materialized views with the LynxDB CLI.
---

# mv (Materialized Views)

Manage materialized views -- precomputed aggregations that accelerate repeated queries by 100-400x.

```
lynxdb mv <subcommand>
```

## mv create

Create a new materialized view from an SPL2 aggregation query.

```
lynxdb mv create <name> <query> [--retention <duration>]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--retention` | | Retention period (e.g., `30d`, `90d`) |

### Examples

```bash
# Create a view for error counts by host
lynxdb mv create errors_by_host \
  'FROM main | where level="ERROR" | stats count by host'

# With retention
lynxdb mv create daily_summary \
  'FROM main | stats count by source' --retention 90d

# Time-bucketed view for repeated aggregations
lynxdb mv create errors_5m \
  'FROM main | where level="ERROR" | stats count, avg(duration) by source, time_bucket(timestamp, "5m") AS bucket' \
  --retention 90d

# Cascading view (build on top of another view)
lynxdb mv create errors_1h \
  '| from errors_5m | stats sum(count) AS count by source, time_bucket(bucket, "1h") AS hour' \
  --retention 365d
```

The view begins backfilling automatically after creation. Queries that match the view pattern are automatically accelerated.

---

## mv list

List all materialized views.

```
lynxdb mv list
```

Supports `--format json`.

### Console Output

```
NAME            STATUS       QUERY
mv_errors_5m    active       level=error | stats count, avg(duration) by ...
mv_5xx_hourly   backfilling  source=nginx status>=500 | stats count, p95(dur...
```

---

## mv status

Show detailed status for a specific view.

```
lynxdb mv status <name>
```

Tab-completes view names. Supports `--format json`.

### Console Output

```
Name:       mv_errors_5m
Status:     active
Query:      level=error | stats count, avg(duration) by source, time_bucket(...)
Retention:  90d
```

---

## mv drop

Drop a materialized view and its stored data.

```
lynxdb mv drop <name> [--force] [--dry-run]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--force` | `false` | Skip confirmation prompt |
| `--dry-run` | `false` | Show what would be deleted without applying |

### Examples

```bash
# Drop with confirmation prompt
lynxdb mv drop errors_by_host

# Skip confirmation
lynxdb mv drop errors_by_host --force

# Preview what would be deleted
lynxdb mv drop errors_by_host --dry-run
```

---

## mv pause

Pause a materialized view pipeline. The view stops processing new data but retains its existing computed data.

```
lynxdb mv pause <name>
```

```bash
lynxdb mv pause errors_5m
```

---

## mv resume

Resume a paused materialized view pipeline. The view catches up on data ingested while it was paused.

```
lynxdb mv resume <name>
```

```bash
lynxdb mv resume errors_5m
```

## How Acceleration Works

When you run a query that matches a materialized view, LynxDB automatically rewrites the query to read from the view instead of scanning raw data:

```bash
# This query:
lynxdb query 'level=error | stats count by source'

# Is automatically accelerated by mv_errors_5m if it matches
# Response metadata shows:
#   meta.accelerated_by: {view: mv_errors_5m, speedup: "~400x"}
```

## See Also

- [query](/docs/cli/query) for running queries that can be accelerated by views
- [Server](/docs/cli/server) for server configuration that affects view processing
