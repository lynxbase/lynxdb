---
sidebar_position: 6
title: "Shortcuts: count, sample, last, fields, explain, examples"
description: Quick access CLI commands for common operations -- event counting, sampling, field discovery, and query explanation.
---

# Quick Access Commands

Shortcut commands for frequent operations. Each wraps a common SPL2 pattern into a single, easy-to-type command.

## count

Quick event count shortcut. Faster than `query ... | stats count`.

```
lynxdb count [filter] [flags]
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range (e.g., `15m`, `1h`, `7d`) |

### Examples

```bash
# Count all events
lynxdb count

# Count errors
lynxdb count 'level=error'

# Count events in last hour
lynxdb count --since 1h
```

---

## sample

Show a sample of recent events, useful for exploring data structure.

```
lynxdb sample [count] [filter] [flags]
```

The first argument is parsed as a number (sample size, default 5). Any remaining arguments are treated as an SPL2 filter.

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range |

### Examples

```bash
# 5 random events (default)
lynxdb sample

# 10 events
lynxdb sample 10

# 5 nginx events
lynxdb sample 5 '_source=nginx'

# JSON for inspecting structure
lynxdb sample 3 --format json | jq .
```

---

## last

Re-run the most recently executed query. Optionally override the time range or output format.

```
lynxdb last [flags]
```

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Override time range |

### Examples

```bash
# Repeat last query
lynxdb last

# Same query, wider time range
lynxdb last --since 24h

# Same query, CSV output
lynxdb last -F csv
```

---

## fields

Show the field catalog from server -- all known fields with types, coverage, and top values.

```
lynxdb fields [name] [flags]
```

**Alias:** `f`

When a field name is provided, shows details for that specific field (with fuzzy matching for typos). Use `--values` to see the top 50 values for a field.

### Flags

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--values` | | `false` | Show top values for the specified field |
| `--since` | `-s` | | Restrict stats to time range (e.g., `15m`, `1h`, `7d`) |
| `--from` | | | Absolute start time (ISO 8601) |
| `--to` | | | Absolute end time (ISO 8601) |
| `--source` | | | Filter by source |
| `--prefix` | | | Filter fields by name prefix |

### Examples

```bash
# All fields
lynxdb fields

# Detail for 'status' field
lynxdb fields status

# Top values for 'status'
lynxdb fields status --values

# Fields seen from nginx
lynxdb fields --source nginx

# Autocomplete helper
lynxdb fields --prefix sta

# Fields seen in last hour
lynxdb fields --since 1h

# JSON output for scripting
lynxdb fields --format json
```

### Console Output

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

## explain

Show query execution plan without running the query.

```
lynxdb explain [SPL2 query] [flags]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--analyze` | `false` | Execute query and show plan with actual execution stats |

### Examples

```bash
# Show plan
lynxdb explain 'level=error | stats count by source'

# JSON format
lynxdb explain 'status>=500 | top 10 uri' --format json

# Execute and show actual stats
lynxdb explain --analyze 'level=error | stats count'
```

### Console Output

```
Plan:
  FROM -> WHERE -> STATS

Estimated cost: low

Fields read: level, source
```

---

## examples

Show a cookbook of common SPL2 query patterns.

```
lynxdb examples
```

**Alias:** `cookbook`

Displays categorized examples for:

- Search and Filter
- Aggregation
- Time Analysis
- Transformation
- Local File Queries

Run this command when you need a quick reference for SPL2 syntax and patterns.

## See Also

- [query](/docs/cli/query) for the full query command reference
- [Real-Time Commands](/docs/cli/tail) for `tail`, `top`, `watch`, and `diff`
- [Lynx Flow Reference](/docs/lynx-flow/overview) for the complete query language reference
- [Output Formats](/docs/cli/output-formats) for `--format` options
