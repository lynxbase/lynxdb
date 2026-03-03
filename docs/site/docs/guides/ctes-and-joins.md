---
title: CTEs, Joins, and Subsearches
description: How to use Common Table Expressions (CTEs), JOIN, APPEND, and MULTISEARCH in LynxDB for cross-source correlation and complex analysis.
---

# CTEs, Joins, and Subsearches

When a single pipeline is not enough, LynxDB supports Common Table Expressions (CTEs), the [`JOIN`](/docs/spl2/commands/join) command, [`APPEND`](/docs/spl2/commands/append), and [`MULTISEARCH`](/docs/spl2/commands/multisearch) for combining data from multiple sources or running multi-step analysis.

## Common Table Expressions (CTEs)

CTEs let you define named result sets and reference them later in the query. This is the most powerful way to build complex multi-source queries.

### CTE syntax

Define a CTE with `$name = <query>;` and reference it with `FROM $name`:

```spl
$threats = FROM idx_backend WHERE threat_type IN ("sqli", "path_traversal") | FIELDS client_ip, threat_type;
$logins = FROM idx_audit WHERE type="USER_LOGIN" AND res="failed" | STATS count AS failures BY src_ip;
FROM $threats | JOIN type=inner client_ip [$logins] | TABLE client_ip, threat_type, failures
```

### CTE rules

- CTE names start with `$` and are assigned with `=`.
- Each CTE ends with a semicolon `;`.
- CTEs are evaluated in order, from top to bottom.
- The final query (after all CTE definitions) produces the result.
- CTEs can reference earlier CTEs.

### Example: security correlation

Find IPs that triggered threat detection AND had failed logins:

```bash
lynxdb query '
  $threats = FROM main WHERE threat_type IN ("sqli", "path_traversal")
    | FIELDS client_ip, threat_type;
  $failed_logins = FROM main WHERE type="USER_LOGIN" AND res="failed"
    | STATS count AS failures BY src_ip;
  FROM $threats
    | JOIN type=inner client_ip [$failed_logins]
    | WHERE failures > 5
    | TABLE client_ip, threat_type, failures
    | SORT -failures'
```

### Example: compare two time periods

```bash
lynxdb query '
  $current = FROM main WHERE level="error"
    | STATS count AS current_errors BY source;
  $previous = FROM main WHERE level="error"
    | STATS count AS previous_errors BY source;
  FROM $current
    | JOIN type=outer source [$previous]
    | EVAL change_pct = round((current_errors - previous_errors) / previous_errors * 100, 1)
    | TABLE source, current_errors, previous_errors, change_pct
    | SORT -change_pct'
```

---

## JOIN

The [`JOIN`](/docs/spl2/commands/join) command combines events from two datasets based on a shared field.

### Inner join

Keep only events that have a match in both datasets:

```bash
lynxdb query 'source=nginx
  | JOIN type=inner client_ip [
      FROM main WHERE source="auth" type="login"
      | FIELDS client_ip, user_id
    ]
  | TABLE client_ip, user_id, uri, status'
```

### Outer join (left join)

Keep all events from the left side, with null values for unmatched right-side fields:

```bash
lynxdb query 'source=nginx
  | JOIN type=outer client_ip [
      FROM main WHERE source="geo"
      | FIELDS client_ip, country, city
    ]
  | TABLE client_ip, uri, country, city'
```

### Join syntax

```
| JOIN type=<inner|outer> <field> [<subsearch>]
```

| Parameter | Description |
|-----------|-------------|
| `type` | `inner` (only matches) or `outer` (left join, keep all from primary) |
| `field` | The field to join on (must exist in both datasets) |
| `[subsearch]` | The secondary dataset in square brackets |

### Join on multiple fields

When you need to join on multiple fields, list them separated by commas:

```bash
lynxdb query 'source=nginx
  | JOIN type=inner host, timestamp [
      FROM main WHERE source="metrics"
      | FIELDS host, timestamp, cpu_pct, mem_pct
    ]
  | TABLE host, timestamp, uri, cpu_pct, mem_pct'
```

---

## APPEND

The [`APPEND`](/docs/spl2/commands/append) command concatenates the results of a subsearch to the end of the current result set:

```bash
lynxdb query 'source=nginx status>=500 | stats count AS errors by uri
  | APPEND [
      source=nginx | stats count AS total by uri
    ]
  | TABLE uri, errors, total'
```

### Use case: combine different aggregations

When you need two different aggregations that cannot be combined in a single `STATS`:

```bash
lynxdb query '| stats count AS total_events
  | APPEND [
      level=error | stats count AS total_errors
    ]
  | APPEND [
      source=nginx status>=500 | stats count AS nginx_5xx
    ]'
```

---

## MULTISEARCH

The [`MULTISEARCH`](/docs/spl2/commands/multisearch) command runs multiple independent searches and unions the results:

```bash
lynxdb query '| MULTISEARCH
  [ source=nginx status>=500 | stats count AS errors, avg(duration_ms) AS avg_lat | eval source="nginx" ]
  [ source=api-gateway level=error | stats count AS errors, avg(duration_ms) AS avg_lat | eval source="api-gw" ]
  [ source=postgres duration_ms>1000 | stats count AS errors, avg(duration_ms) AS avg_lat | eval source="postgres" ]
  | TABLE source, errors, avg_lat
  | SORT -errors'
```

### MULTISEARCH vs APPEND

| Feature | MULTISEARCH | APPEND |
|---------|-------------|--------|
| Number of searches | 2+ | 1 primary + 1 appended |
| Result order | Union (interleaved) | Primary first, then appended |
| Syntax | All searches in brackets | Primary pipeline + appended |
| Use case | Run several independent analyses | Extend one analysis with another |

---

## TRANSACTION

The [`TRANSACTION`](/docs/spl2/commands/transaction) command groups events into transactions (sequences of related events) based on shared field values:

```bash
lynxdb query 'source=api-gateway
  | TRANSACTION session_id startswith="request started" endswith="request completed"
  | EVAL duration = latest_time - earliest_time
  | TABLE session_id, duration, eventcount'
```

Transactions are useful for:

- Grouping request start/end events into sessions
- Computing end-to-end latency across multiple log lines
- Finding incomplete transactions (missing end event)

---

## Practical patterns

### Find users hitting rate limits AND generating errors

```bash
lynxdb query '
  $rate_limited = FROM main WHERE source="api-gateway" AND status=429
    | STATS count AS rate_limit_hits BY user_id;
  $errors = FROM main WHERE source="api-gateway" AND status>=500
    | STATS count AS error_count BY user_id;
  FROM $rate_limited
    | JOIN type=inner user_id [$errors]
    | WHERE rate_limit_hits > 10 AND error_count > 5
    | TABLE user_id, rate_limit_hits, error_count
    | SORT -rate_limit_hits'
```

### Enrich nginx logs with geo data

```bash
lynxdb query 'source=nginx status>=500
  | JOIN type=outer client_ip [
      FROM main WHERE source="geoip"
      | DEDUP client_ip
      | FIELDS client_ip, country, city
    ]
  | STATS count BY country, city
  | SORT -count
  | HEAD 20'
```

### Compare error rates across services

```bash
lynxdb query '| MULTISEARCH
  [ source=nginx | stats count AS total, count(eval(status>=500)) AS errors | eval service="nginx" ]
  [ source=api-gateway | stats count AS total, count(eval(level="error")) AS errors | eval service="api-gw" ]
  [ source=postgres | stats count AS total, count(eval(level="error")) AS errors | eval service="postgres" ]
  | EVAL error_rate = round(errors/total*100, 2)
  | TABLE service, total, errors, error_rate
  | SORT -error_rate'
```

---

## Performance considerations

- **JOIN**: The right side (subsearch) is loaded into memory. Keep subsearch results small by filtering and aggregating before joining. Avoid joining two large unfiltered datasets.
- **CTEs**: Each CTE is evaluated independently. Use filters and aggregations in CTEs to reduce intermediate result sizes.
- **MULTISEARCH**: All searches run in parallel. This is more efficient than running them sequentially.
- **APPEND**: The appended subsearch runs after the primary pipeline. Keep appended results small.

---

## Next steps

- [Search and filter logs](/docs/guides/search-and-filter) -- write effective filters for subsearches
- [Run aggregations](/docs/guides/aggregations) -- build aggregations for CTE pipelines
- [JOIN command reference](/docs/spl2/commands/join) -- full JOIN syntax and options
- [APPEND command reference](/docs/spl2/commands/append) -- full APPEND syntax
- [MULTISEARCH command reference](/docs/spl2/commands/multisearch) -- full MULTISEARCH syntax
- [TRANSACTION command reference](/docs/spl2/commands/transaction) -- full TRANSACTION syntax
