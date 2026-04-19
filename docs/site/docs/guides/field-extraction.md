---
title: Extract Fields at Query Time
description: How to extract fields from unstructured logs at query time using REX, EVAL, and LynxDB's schema-on-read approach.
---

# Extract Fields at Query Time

LynxDB follows a schema-on-read philosophy: you do not need to define a schema before ingesting data. JSON and text logs remain queryable without an upfront schema, but each ingest transport has its own endpoint contract. Fields from JSON events are indexed automatically. For unstructured text logs, use [`REX`](/docs/lynx-flow/commands/rex) and [`EVAL`](/docs/lynx-flow/commands/eval) to extract and compute fields at query time.

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

The [`REX`](/docs/lynx-flow/commands/rex) command extracts fields from a text field using named capture groups in a regular expression.

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

The [`EVAL`](/docs/lynx-flow/commands/eval) command creates new fields by evaluating expressions.

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

See the [eval functions reference](/docs/lynx-flow/functions/eval-functions) for the complete list of available functions.

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

## Structured log parsing with unpack

For structured log formats, LynxDB provides purpose-built `unpack_*` commands that are faster and more accurate than regex extraction:

| Format | Command | Example input |
|--------|---------|---------------|
| JSON | [`unpack_json`](/docs/lynx-flow/commands/unpack-json) | `{"level":"error","msg":"timeout"}` |
| logfmt | [`unpack_logfmt`](/docs/lynx-flow/commands/unpack-logfmt) | `level=error msg="request failed" duration=245ms` |
| Syslog | [`unpack_syslog`](/docs/lynx-flow/commands/unpack-syslog) | `<134>Jan 15 14:23:01 web-01 nginx: connection reset` |
| Combined (access log) | [`unpack_combined`](/docs/lynx-flow/commands/unpack-combined) | `10.0.1.5 - - [10/Oct/2025:13:55:36 -0700] "GET /api HTTP/1.1" 200 2326 "-" "curl/7.64"` |
| CLF | [`unpack_clf`](/docs/lynx-flow/commands/unpack-clf) | `127.0.0.1 - frank [10/Oct/2025:13:55:36 -0700] "GET /api HTTP/1.1" 200 2326` |
| Nginx error | [`unpack_nginx_error`](/docs/lynx-flow/commands/unpack-nginx-error) | `2026/02/14 14:52:01 [error] 12345#67: *890 message, client: 10.0.1.5` |
| CEF | [`unpack_cef`](/docs/lynx-flow/commands/unpack-cef) | `CEF:0\|Vendor\|Product\|1.0\|100\|Alert\|7\|src=10.0.0.1` |
| Key=value | [`unpack_kv`](/docs/lynx-flow/commands/unpack-kv) | `host=web-01 status=200 duration=45ms` |

```bash
# Parse logfmt and aggregate
cat app.log | lynxdb query '| unpack_logfmt | stats count by level'

# Parse nginx access logs
lynxdb query --file access.log '| unpack_combined | where status >= 500 | stats count by uri'
```

For JSON-specific workflows (dot-notation, json_extract, unroll), see the [Working with JSON Logs](/docs/guides/json-processing) guide.

---

## Next steps

- [Working with JSON Logs](/docs/guides/json-processing) -- dot-notation, json commands, unroll
- [Search and filter logs](/docs/guides/search-and-filter) -- filter before extracting fields
- [Run aggregations](/docs/guides/aggregations) -- aggregate over extracted fields
- [REX command reference](/docs/lynx-flow/commands/rex) -- full REX syntax and options
- [EVAL command reference](/docs/lynx-flow/commands/eval) -- full EVAL syntax and all functions
- [Eval functions reference](/docs/lynx-flow/functions/eval-functions) -- complete function list
