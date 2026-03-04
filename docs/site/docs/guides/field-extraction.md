---
title: Extract Fields at Query Time
description: How to extract fields from unstructured logs at query time using REX, EVAL, and LynxDB's schema-on-read approach.
---

# Extract Fields at Query Time

LynxDB follows a schema-on-read philosophy: you do not need to define a schema before ingesting data. Send any JSON, any text, any format. Fields from JSON events are indexed automatically. For unstructured text logs, use [`REX`](/docs/spl2/commands/rex) and [`EVAL`](/docs/spl2/commands/eval) to extract and compute fields at query time.

## How schema-on-read works

When you ingest data, LynxDB automatically discovers fields:

- **JSON events**: All top-level keys become searchable fields with their types preserved.
- **Raw text**: The full line is stored in the `_raw` field. You can extract structure from it at query time.

Check what fields LynxDB has discovered:

```bash
lynxdb fields
```

```
FIELD                     TYPE       COVERAGE   TOP VALUES
--------------------------------------------------------------------------------
_timestamp                datetime      100%
level                     string        100%    INFO(72%), ERROR(17%), WARN(11%)
status                    integer        50%    200(90%), 404(5%), 500(3%)
duration_ms               float          50%    min=0.1, max=30001.0, avg=145.3
source                    string        100%    nginx(50%), api-gw(37%), redis(13%)
```

See the [`lynxdb fields`](/docs/cli/shortcuts) command reference for details.

---

## Extract fields with REX

The [`REX`](/docs/spl2/commands/rex) command extracts fields from a text field using named capture groups in a regular expression.

### Basic extraction

Given raw log lines like:

```
2026-01-15 14:23:01 host=web-01 service=api duration=245ms status=200
```

Extract `host`, `service`, `duration`, and `status`:

```bash
lynxdb query 'search "duration"
  | rex field=_raw "host=(?P<host>\S+) service=(?P<service>\S+) duration=(?P<duration>\d+)ms status=(?P<status>\d+)"
  | table _timestamp, host, service, duration, status'
```

### Named capture group syntax

REX uses Go-style named capture groups: `(?P<field_name>pattern)`.

| Pattern | Matches |
|---------|---------|
| `(?P<ip>\d+\.\d+\.\d+\.\d+)` | An IPv4 address |
| `(?P<host>\S+)` | A non-whitespace token |
| `(?P<code>\d{3})` | A 3-digit status code |
| `(?P<path>[^ "]+)` | A path (no spaces or quotes) |
| `(?P<msg>.+)` | Everything to end of line |

### Extract from Apache/Nginx access logs

```bash
lynxdb query --file access.log '
  | rex field=_raw "(?P<client_ip>\S+) \S+ \S+ \[(?P<timestamp>[^\]]+)\] \"(?P<method>\w+) (?P<uri>\S+) \S+\" (?P<status>\d+) (?P<bytes>\d+)"
  | stats count by method, status
  | sort -count'
```

### Extract from application logs

```bash
lynxdb query 'search "connection refused"
  | rex field=_raw "host=(?P<host>\S+)"
  | stats count by host
  | sort -count'
```

### Extract from a non-default field

By default, `REX` operates on `_raw`. Use `field=` to extract from any field:

```bash
lynxdb query '| rex field=message "user_id=(?P<uid>\d+)"
  | stats dc(uid) AS unique_users'
```

---

## Compute fields with EVAL

The [`EVAL`](/docs/spl2/commands/eval) command creates new fields by evaluating expressions.

### Create a computed field

```bash
lynxdb query '_source=nginx
  | eval duration_sec = duration_ms / 1000
  | table uri, duration_ms, duration_sec'
```

### Conditional fields with IF

```bash
lynxdb query '_source=nginx
  | eval severity = if(status >= 500, "critical", if(status >= 400, "warning", "ok"))
  | stats count by severity'
```

### Conditional fields with CASE

```bash
lynxdb query '_source=nginx
  | eval category = case(
      status >= 500, "5xx",
      status >= 400, "4xx",
      status >= 300, "3xx",
      status >= 200, "2xx",
      1=1, "other"
    )
  | stats count by category'
```

### String manipulation

```bash
# Convert to lowercase
lynxdb query '| eval level_lower = lower(level) | stats count by level_lower'

# Extract substring
lynxdb query '| eval short_path = substr(uri, 1, 20) | stats count by short_path'

# String length
lynxdb query '| eval msg_len = len(message) | where msg_len > 500 | table _timestamp, msg_len, message'
```

### Type conversion

```bash
# Convert a string field to a number
lynxdb query '| eval status_num = tonumber(status) | where status_num >= 500'

# Convert a number to string for display
lynxdb query '| eval status_str = tostring(status) | table status_str, uri'
```

### Coalesce (first non-null)

```bash
lynxdb query '| eval display_time = coalesce(timestamp, @timestamp, _timestamp)
  | table display_time, message'
```

### Time formatting

```bash
lynxdb query '| eval human_time = strftime(_timestamp, "%Y-%m-%d %H:%M:%S")
  | table human_time, level, message'
```

See the [eval functions reference](/docs/spl2/functions/eval-functions) for the complete list of available functions.

---

## Combine REX and EVAL

Extract raw values with REX, then transform them with EVAL:

```bash
lynxdb query 'search "request completed"
  | rex field=_raw "duration=(?P<dur_str>\d+)ms"
  | eval duration_ms = tonumber(dur_str)
  | eval is_slow = if(duration_ms > 1000, "slow", "fast")
  | stats count by is_slow'
```

---

## Multivalue field operations

When a field contains multiple values (for example, from `values()` aggregation or structured input), use multivalue functions:

```bash
# Join multivalue into a string
lynxdb query 'level=error | stats values(source) AS sources by host | eval src_list = mvjoin(sources, ", ")'

# Deduplicate multivalue
lynxdb query '| eval unique_tags = mvdedup(tags)'

# Append to multivalue
lynxdb query '| eval all_ids = mvappend(primary_id, secondary_id)'
```

---

## Null handling

Check for missing or null fields:

```bash
# Find events missing a field
lynxdb query '| where isnull(user_id) | stats count by source'

# Find events that have a field
lynxdb query '| where isnotnull(duration_ms) | stats avg(duration_ms)'

# Replace null with a default
lynxdb query '| eval region = coalesce(region, "unknown") | stats count by region'
```

---

## Field extraction on local files

All extraction commands work in pipe mode:

```bash
# Extract from a local file
lynxdb query --file /var/log/syslog '
  | rex field=_raw "(?P<process>\w+)\[(?P<pid>\d+)\]"
  | stats count by process
  | sort -count
  | head 10'

# Extract from piped input
kubectl logs deploy/api | lynxdb query '
  | rex field=_raw "endpoint=(?P<ep>\S+) status=(?P<code>\d+) duration=(?P<dur>\d+)ms"
  | eval dur_num = tonumber(dur)
  | stats avg(dur_num) AS avg_ms, p99(dur_num) AS p99_ms by ep'
```

---

## Next steps

- [Search and filter logs](/docs/guides/search-and-filter) -- filter before extracting fields
- [Run aggregations](/docs/guides/aggregations) -- aggregate over extracted fields
- [REX command reference](/docs/spl2/commands/rex) -- full REX syntax and options
- [EVAL command reference](/docs/spl2/commands/eval) -- full EVAL syntax and all functions
- [Eval functions reference](/docs/spl2/functions/eval-functions) -- complete function list
