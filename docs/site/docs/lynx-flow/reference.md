---
sidebar_position: 2
title: Lynx Flow Reference
description: Complete Lynx Flow language reference -- all commands, operators, functions, and SPL2 equivalents.
---

# Lynx Flow Reference

Complete reference for the Lynx Flow query language. Every command shows its Lynx Flow (primary) and SPL2 (compatible) syntax.

Both syntaxes compile to the same AST and execution plan. The parser accepts either, and they can be mixed freely.

---

## Source Selection

### from

Select a data source. This is always the first command in a canonical query.

**Syntax:**

```
from <source>
from <s1>, <s2>           -- multiple sources
from <glob>               -- wildcard pattern
from *                    -- all sources
from $var                 -- CTE variable
from view.<name>          -- materialized view
```

**SPL2:** `FROM <source> [WHERE <expr>]`

**Examples:**

```
from nginx
from nginx, api_gw
from logs_*
from view.mv_errors_5m
```

### index (alias for from)

`index` and `from` are interchangeable. `from` is the canonical form.

```
index nginx              -- same as: from nginx
index="nginx"            -- SPL1 compat: same as: from nginx
```

When `index=` has trailing predicates, they desugar to a `search` command:

```
index="nginx" status>=500 method="POST"
-- desugars to: from nginx | search status>=500 method="POST"
```

### Implicit from

When a query starts with `|`, `from main` is prepended automatically:

```
| where level="error" | group compute count()
-- equivalent to: from main | where level="error" | group compute count()
```

---

## Search and Filtering

### search

Full-text and term-level filtering. Use before `parse` for text matching on `_raw`.

**Syntax:**

```
search "<text>"                     -- full-text search
search <field>=<value>              -- field match
search <field>>=<value>             -- comparison
search <terms> AND/OR NOT           -- boolean operators
search <field> IN (v1, v2)          -- membership
search TERM("<exact>")              -- exact term match
search CASE("<sensitive>")          -- case-sensitive
search <field>=<glob>               -- wildcard
```

**SPL2:** `SEARCH ...` (identical syntax)

**Examples:**

```
from nginx
| search "connection timeout"
| where status >= 500

from app
| search level="error" service="payment"
```

### where

Structured expression filtering. Use after `parse` for typed field comparisons.

**Syntax:**

```
where <expression>
```

**SPL2:** `where <expression>` (identical)

**Operators:**

| Operator | Description | Example |
|----------|-------------|---------|
| `=`, `!=` | Equality | `where status = 200` |
| `>`, `>=`, `<`, `<=` | Comparison | `where duration_ms > 1000` |
| `AND`, `OR`, `NOT` | Boolean | `where status >= 500 AND source = "nginx"` |
| `in (...)` | Set membership | `where level in ("error", "fatal")` |
| `not in (...)` | Negative membership | `where status not in (200, 301)` |
| `between A and B` | Range (inclusive) | `where duration_ms between 100 and 5000` |
| `like "<pattern>"` | Wildcard pattern (case-sensitive) | `where uri like "/api/%"` |
| `not like` | Negative wildcard | `where uri not like "/health%"` |
| `=~` | Regex match | `where uri =~ "^/api/v[0-9]+"` |
| `<field>?` | Existence test (not null) | `where trace_id?` |
| `is null` | Null check | `where error_code is null` |
| `is not null` | Not-null check | `where user_id is not null` |

**Examples:**

```
from nginx
| where status >= 500 AND method = "POST"
| where uri not like "/health%"
| where trace_id? AND duration_ms between 100 and 5000
```

**Cognitive rule:** Use `search` for text matching (grep-like, before `parse`). Use `where` for structured filtering (SQL-like, after `parse`).

---

## Parsing and Extraction

### parse

Extract structure from raw text. 17 built-in formats plus regex and pattern.

**Syntax:**

```
parse <format>(<field>) [as <namespace>] [extract (<f1>, <f2>)] [if_missing]
```

**SPL2 equivalents:** `unpack_json`, `unpack_logfmt`, `unpack_syslog`, `rex`, etc.

**Supported formats:**

| Format | Description | SPL2 |
|--------|-------------|------|
| `json` | JSON object | `unpack_json` |
| `logfmt` | key=value logfmt | `unpack_logfmt` |
| `syslog` | RFC 3164/5424 | `unpack_syslog` |
| `combined` | Apache combined | `unpack_combined` |
| `clf` | Common Log Format | `unpack_clf` |
| `regex` | Named capture groups | `rex` |
| `pattern` | User-defined pattern | `unpack_pattern` |
| `nginx_error` | Nginx error log | `unpack_nginx_error` |
| `cef` | Common Event Format | `unpack_cef` |
| `kv` | Generic key=value | `unpack_kv` |
| `docker` | Docker JSON log | `unpack_docker` |
| `redis` | Redis log | `unpack_redis` |
| `apache_error` | Apache error log | `unpack_apache_error` |
| `postgres` | PostgreSQL log | `unpack_postgres` |
| `mysql_slow` | MySQL slow query log | `unpack_mysql_slow` |
| `haproxy` | HAProxy log | `unpack_haproxy` |
| `leef` | Log Event Extended Format | `unpack_leef` |
| `w3c` | W3C extended log | `unpack_w3c` |

**Modifiers:**

| Modifier | Purpose | Example |
|----------|---------|---------|
| `as <namespace>` | Prefix all fields with namespace | `parse syslog(_raw) as env` |
| `extract (<fields>)` | Extract only listed fields | `parse json(_raw) extract (user_id, level)` |
| `if_missing` | Only fill in null/missing fields | `parse json(_raw) if_missing` |

**Examples:**

```
-- Parse JSON with namespace
from api_gw
| parse json(_raw) as req
| where req.status >= 500

-- Selective extraction
from nginx
| parse combined(_raw) extract (status, uri)
| where status >= 500

-- Chained parsing: syslog envelope + JSON payload
from mixed
| parse syslog(_raw) as envelope extract (severity, message)
| parse json(envelope.message) as payload extract (user_id, action)
| where envelope.severity = "err"

-- Regex extraction
from app
| parse regex(_raw, "host=(?P<host>\\S+) port=(?P<port>\\d+)")
| group by host compute count()
```

### explode

Expand a JSON array field into one row per element.

**Syntax:**

```
explode <field> [as <alias>]
```

**SPL2:** `unroll field=<field>`

**Example:**

```
from events
| parse json(_raw)
| explode tags as tag
| group by tag compute count()
```

---

## Derivation

### let

Create or replace fields by evaluating expressions.

**Syntax:**

```
let <field> = <expression> [, <field> = <expression> ...]
```

**SPL2:** `eval <field>=<expression>`

**Examples:**

```
-- Simple computation
from nginx
| let duration_s = duration_ms / 1000

-- Multiple assignments
from nginx
| let status_group = floor(status / 100) * 100,
      is_error = status >= 500,
      duration_s = round(duration_ms / 1000, 2)

-- Conditional logic
from app
| let severity = if(status >= 500, "critical", if(status >= 400, "warning", "ok"))

-- Null coalesce
from app
| let display_name = username ?? email ?? "anonymous"

-- String concatenation
from app
| let full_msg = source . ": " . message
```

All assignments in a single `let` see the same input row. For chained computations, use separate `let` commands:

```
| let duration_s = duration_ms / 1000
| let is_slow = duration_s > 5.0
```

---

## Field Shaping

### keep

Include only listed fields. Column order preserved from input.

**Syntax:**

```
keep <field1>, <field2>, ...
```

**SPL2:** `fields <field1>, <field2>`

### omit

Exclude listed fields. Column order preserved from input.

**Syntax:**

```
omit <field1>, <field2>, ...
```

**SPL2:** `fields - <field1>, <field2>`

### select

Ordered projection with optional inline rename. Column order enforced by command.

**Syntax:**

```
select <field1> [as <alias>], <field2> [as <alias>], ...
```

No direct SPL2 equivalent.

### rename

Change field names without affecting which fields are present.

**Syntax:**

```
rename <old> as <new> [, <old2> as <new2> ...]
```

**SPL2:** `rename <old> AS <new>`

**Examples:**

```
-- Projection
from nginx | parse combined(_raw) | keep uri, status, duration_ms

-- Exclusion
from app | omit _raw, _source, _timestamp

-- Ordered with rename
from app
| select _timestamp as time, uri as path, status as http_status

-- Rename
from firewall
| rename src_addr as source, dst_addr as destination
```

**Choosing the right command:**

| Command | Purpose | Column Order |
|---------|---------|--------------|
| `keep` | Mid-pipeline column pruning | From input |
| `omit` | Remove unwanted columns | From input |
| `select` | Ordered projection + rename | From command |
| `table` | Final output formatting | From command |

---

## Aggregation

### group

Grouped or global aggregation.

**Syntax:**

```
group by <key1>, <key2> compute <agg1> [as <alias>], <agg2> [as <alias>] ...
group compute <agg1> [as <alias>], ...
```

**SPL2:** `stats <agg> [as <alias>] ... by <field> ...`

**Examples:**

```
-- Grouped aggregation
from nginx
| group by service, status compute count() as hits, avg(duration_ms) as avg_dur

-- Global aggregation (no group-by)
from nginx
| where status >= 500
| group compute count() as total_errors, dc(uri) as unique_uris

-- Multiple aggregation functions
from nginx
| group by service compute
    count() as requests,
    sum(bytes) as total_bytes,
    avg(duration_ms) as avg_latency,
    perc95(duration_ms) as p95_latency,
    dc(client_ip) as unique_clients
```

### every

Time-bucketed aggregation. The bucket field is always `_timestamp`.

**Syntax:**

```
every <span> compute <aggs>
every <span> by <field> compute <aggs>
```

**SPL2:** `timechart span=<span> <agg> [by <field>]`

**Span formats:** `30s`, `5m`, `1h`, `1d`

**Examples:**

```
-- 5-minute time series
from nginx
| every 5m compute count() as events

-- Split by service
from nginx
| every 5m by service compute count() as events, avg(duration_ms) as avg_latency

-- Time series with derived field
from nginx
| let status_class = floor(status / 100) * 100
| every 5m by status_class compute count() as hits
```

### bucket

Add a time-bucket column without aggregating.

**Syntax:**

```
bucket <field> span=<duration> [as <alias>]
```

**SPL2:** `bin <field> span=<duration> [AS <alias>]`

**Example:**

```
from nginx
| bucket _timestamp span=1h as hour
| group by hour, level compute count() as cnt
| order by hour asc, cnt desc
```

---

## Ranking and Ordering

### order by

Order rows by one or more expressions.

**Syntax:**

```
order by <expr> [asc|desc] [, <expr> [asc|desc] ...]
```

Default direction is `asc`.

**SPL2:** `sort [-|+]<field>` (prefix `-` for desc, `+` for asc)

**Examples:**

```
from nginx | order by duration_ms desc
from nginx | order by status desc, uri asc, duration_ms desc
```

### sort (compatibility alias)

`sort` is accepted as an alias for `order by`. Both SPL-style prefix syntax and explicit direction syntax work:

| Form | Equivalent |
|------|------------|
| `sort -duration_ms` | `order by duration_ms desc` |
| `sort +_timestamp` | `order by _timestamp asc` |
| `sort -status, +uri` | `order by status desc, uri asc` |

### take / head / tail

**Syntax:**

```
take <N>           -- primary limit command
head <N>           -- alias for take
tail <N>           -- last N rows (blocking)
```

**SPL2:** `head <N>`, `tail <N>`

`take` and `head` are streaming (stop after N rows). `tail` is blocking (must buffer all input).

### rank

Row-level ranking using a heap (O(N log K) -- more efficient than full sort for large datasets).

**Syntax:**

```
rank top <N> by <expr>
rank bottom <N> by <expr>
```

No direct SPL2 equivalent. Desugars to `order by <expr> desc|asc | take N`.

**Example:**

```
from nginx
| group by uri compute count() as requests, avg(duration_ms) as avg_latency
| rank top 20 by avg_latency
```

### topby / bottomby

Grouped metric ranking -- group by a field, rank groups by an arbitrary aggregate, all in one step.

**Syntax:**

```
topby <N> <field> using <agg(expr)> [compute <extra_aggs>]
bottomby <N> <field> using <agg(expr)> [compute <extra_aggs>]
```

**Example:**

```
from nginx
| topby 10 uri using avg(duration_ms) compute count() as requests, perc99(duration_ms) as p99
```

**Desugaring:**

```
topby 20 sku using avg(duration_ms) compute count() as purchases
-- becomes:
| group by sku compute avg(duration_ms) as avg_duration_ms, count() as purchases
| rank top 20 by avg_duration_ms
```

### top / bottom / rare

Frequency ranking -- find the most or least common values of a field.

**Syntax:**

```
top <N> <field>                   -- most frequent values
top <N> <field> by <agg(expr)>    -- alias for topby
bottom <N> <field>                -- least frequent values
rare <N> <field>                  -- alias for bottom (Splunk compat)
```

**SPL2:** `top <N> <field>`, `rare <N> <field>`

**Examples:**

```
from nginx | top 10 status
from nginx | top 5 uri by avg(duration_ms)   -- alias for topby
from nginx | rare 20 status
```

### dedup

Remove duplicate rows based on one or more fields.

**Syntax:**

```
dedup [<N>] <field1>, <field2>, ...
```

**SPL2:** `dedup <field1>, <field2>`

**Example:**

```
from app | order by _timestamp desc | dedup service
from app | dedup 3 service   -- keep up to 3 per service
```

---

## Window Operations

### running

Streaming window aggregation. Computes a running aggregate that updates as each row arrives.

**Syntax:**

```
running [window=<N>] [current=true|false] <aggs> [by <fields>]
```

**SPL2:** `streamstats [window=<N>] <agg> [by <fields>]`

**Examples:**

```
-- Running row number
from app | order by _timestamp asc | running count() as row_num

-- Sliding window average
from nginx
| order by _timestamp asc
| running window=10 avg(duration_ms) as rolling_avg

-- Look-back only (exclude current row)
from nginx
| running window=5 current=false avg(duration_ms) as prev_avg by service

-- Multiple aggregations per group
from nginx
| running window=50 avg(duration_ms) as rolling_avg, max(duration_ms) as rolling_max by service
```

### enrich

Per-event enrichment. Computes a global aggregate and attaches it to every row.

**Syntax:**

```
enrich <aggs> [by <fields>]
```

**SPL2:** `eventstats <agg> [by <fields>]`

**Examples:**

```
-- Global average attached to every row
from nginx
| enrich avg(duration_ms) as global_avg
| let deviation = duration_ms - global_avg

-- Per-service average
from nginx
| enrich avg(duration_ms) as service_avg by service
| where duration_ms > service_avg * 2

-- Percentage of total
from nginx
| group by service compute count() as hits
| enrich sum(hits) as total
| let pct = round(hits / total * 100, 1)
```

---

## Null Handling

### fillnull

Replace null values with a default.

**Syntax:**

```
fillnull [value=<val>] [<field1>, <field2>, ...]
fillnull                              -- all fields, default ""
fillnull value=0                      -- all fields, value 0
fillnull value="N/A" region, dc       -- specific fields
```

**SPL2:** `fillnull [value=<val>] [<fields>]` (identical)

### Null operators

| Operator | Description | Example |
|----------|-------------|---------|
| `??` | Null coalesce | `let region = region ?? "unknown"` |
| `?` (postfix) | Existence test (not null) | `where trace_id?` |
| `is null` | Null check | `where error_code is null` |
| `is not null` | Not-null check | `where user_id is not null` |
| `coalesce(a, b, ...)` | First non-null argument | `let name = coalesce(display, user, "anon")` |
| `isnull(f)` | Returns true if null | `where isnull(error)` |
| `isnotnull(f)` | Returns true if not null | `where isnotnull(user_id)` |

---

## Combining Pipelines

### join

Correlate the current pipeline with a subquery on a shared field.

**Syntax:**

```
join [type=inner|left] <field> [subquery]
```

**SPL2:** `join [type=inner|left] <field> [subquery]` (identical)

**Examples:**

```
-- Inner join with threat intel
from nginx
| where status >= 500
| join type=inner client_ip [
    from threat_intel | keep client_ip, threat_type, risk_score
  ]

-- Left join for enrichment
from nginx
| join type=left client_ip [from geo_db | keep client_ip, country, city]
```

### lookup

Sugar for a left join against a named dataset.

**Syntax:**

```
lookup <dataset> on <field>
```

**Desugaring:** `join type=left <field> [from <dataset>]`

**Example:**

```
from nginx | lookup geo_db on client_ip
```

### append

Concatenate results of a subquery below the current pipeline.

**Syntax:**

```
append [subquery]
```

**SPL2:** `append [subquery]` (identical)

### multisearch

Execute multiple queries in parallel and union results.

**Syntax:**

```
multisearch [query1] [query2] ...
```

**SPL2:** `multisearch [query1] [query2]` (identical)

**Example:**

```
| multisearch
  [from nginx | where status >= 500 | keep _timestamp, service, message]
  [from app | where level = "error" | keep _timestamp, service, message]
| order by _timestamp desc
```

### transaction

Group related events by a shared field.

**Syntax:**

```
transaction <field> [maxspan=<dur>] [startswith="<predicate>"] [endswith="<predicate>"]
```

**SPL2:** `transaction <field> [maxspan=<dur>] [startswith=...] [endswith=...]` (identical)

**Example:**

```
from web
| transaction session_id maxspan=30m startswith="type=login" endswith="type=logout"
| where eventcount > 5
| select session_id, duration, eventcount
```

---

## Presentation

### table

Final output formatting. Select which fields appear and in what order.

**Syntax:**

```
table <field1>, <field2>, ...
table *
```

**SPL2:** `table <fields>` (identical)

Use `table` as the last pipeline command. For mid-pipeline projection, use `keep` or `select`.

### xyseries

Pivot grouped data into a cross-tabulation matrix.

**Syntax:**

```
xyseries <x-field> <y-field> <value-field>
```

**SPL2:** `xyseries <x> <y> <value>` (identical)

**Example:**

```
from nginx
| every 1h by level compute count() as events
| xyseries _timestamp level events
```

### pack

Assemble fields into a JSON string.

**Syntax:**

```
pack <field1>, <field2> into <target>
pack into <target>                      -- all non-internal fields
```

**SPL2:** `pack_json <fields> into <target>`

---

## Domain Sugar

High-level shortcuts for common log analytics patterns. Each desugars to core operators.

### latency

Percentile time-series for a duration field.

**Syntax:**

```
latency <field> every <span> [by <group>] [compute <percentiles>]
```

Default percentiles: `p50, p95, p99, count`.

**Example and desugaring:**

```
from nginx | latency duration_ms every 5m by service

-- desugars to:
| every 5m by service compute
    perc50(duration_ms) as p50,
    perc95(duration_ms) as p95,
    perc99(duration_ms) as p99,
    count() as count
```

Custom percentiles:

```
| latency duration_ms every 1m compute p50, p75, p90, p95, p99, avg, max
```

### errors

Error analysis shortcut. Pre-filters on `level in ("error", "fatal")`.

**Syntax:**

```
errors [by <field>] [compute <aggs>]
```

Default aggregation: `count()`.

**Example and desugaring:**

```
from app | errors by service compute count(), dc(user_id)

-- desugars to:
| where level in ("error", "fatal")
| group by service compute count(), dc(user_id)
```

### rate

Event rate over time.

**Syntax:**

```
rate [per <span>] [by <field>]
```

Default span: `1m`.

**Example and desugaring:**

```
from nginx | rate per 5m by service

-- desugars to:
| every 5m by service compute count() as rate
```

### percentiles

Multi-percentile summary of a numeric field.

**Syntax:**

```
percentiles <field> [by <group>]
```

Always produces: `p50`, `p75`, `p90`, `p95`, `p99`.

**Example and desugaring:**

```
from nginx | percentiles duration_ms by service

-- desugars to:
| group by service compute
    perc50(duration_ms) as p50,
    perc75(duration_ms) as p75,
    perc90(duration_ms) as p90,
    perc95(duration_ms) as p95,
    perc99(duration_ms) as p99
```

### slowest

Top N by duration. Two modes:

**Row-level (no group field):**

```
from nginx | slowest 10 by duration_ms
-- desugars to: | rank top 10 by duration_ms
```

**Grouped (with group field):**

```
from nginx | slowest 10 uri by duration_ms
-- desugars to: | topby 10 uri using max(duration_ms)
```

Default duration field: `duration_ms`.

---

## Views and CTEs

### materialize

Save the pipeline result as a materialized view.

**Syntax:**

```
materialize "<name>" [retention=<dur>] [partition_by=<fields>]
```

**Example:**

```
from nginx
| where level = "error"
| every 5m by source compute count() as errors
| materialize "mv_errors_5m" retention=90d
```

### from view.\<name\>

Query a materialized view.

```
from view.mv_errors_5m
| where source = "nginx"
| order by errors desc
| take 10
```

The optimizer automatically uses matching views to accelerate queries.

### views

List, inspect, or alter materialized views.

```
| views                                -- list all views
| views "mv_errors_5m"                 -- show details
| views "mv_errors_5m" retention=180d  -- alter retention
```

### dropview

Delete a materialized view.

```
| dropview "mv_errors_5m"
```

### CTEs (Common Table Expressions)

Define named intermediate datasets within a query.

```
$threats = from threat_intel
  | where threat_type in ("sqli", "path_traversal")
  | keep client_ip, threat_type;

$failures = from audit
  | where type = "login" AND result = "failed"
  | group by src_ip compute count() as failures;

from $threats
| join type=inner client_ip [from $failures | rename src_ip as client_ip]
| table client_ip, threat_type, failures
```

CTEs are separated by `;`, scoped to the current query, and evaluated once.

---

## Expression Language

Shared by `let`, `where`, `group compute`, and all expression contexts.

### Arithmetic Operators

| Operator | Description | Precedence |
|----------|-------------|------------|
| `*`, `/`, `%` | Multiply, divide, modulo | 1 (highest) |
| `+`, `-` | Add, subtract | 2 |

### Comparison Operators

| Operator | Description |
|----------|-------------|
| `=` or `==` | Equality |
| `!=` | Inequality |
| `<`, `<=`, `>`, `>=` | Comparison |
| `=~`, `!~` | Regex match / non-match |
| `like`, `not like` | Wildcard pattern (case-sensitive) |
| `in (...)`, `not in (...)` | Set membership |
| `between A and B` | Range (inclusive) |
| `is null`, `is not null` | Null test |
| `?` (postfix) | Existence test |
| `??` | Null coalesce |

### Logical Operators

| Operator | Description | Precedence |
|----------|-------------|------------|
| `NOT` | Negation | 5 |
| `AND` | Logical AND | 6 |
| `OR` | Logical OR | 7 (lowest) |

### Evaluation Order

1. Parentheses `()`
2. Unary: `NOT`, `-` (negation)
3. Multiplicative: `*`, `/`, `%`
4. Additive: `+`, `-`
5. Comparison: `<`, `<=`, `>`, `>=`
6. Equality: `=`, `!=`, `=~`, `!~`, `like`
7. Logical AND
8. Logical OR

### String Concatenation

Use the `.` operator:

```
| let full_msg = source . ": " . message
```

---

## Aggregation Functions

Used in `group compute`, `every compute`, `running`, `enrich`, `stats`, `timechart`, `eventstats`, `streamstats`.

| Function | Description | Example |
|----------|-------------|---------|
| `count()` | Count all rows | `count() as total` |
| `count(<field>)` | Count non-null values | `count(user_id) as users` |
| `sum(<field>)` | Sum of values | `sum(bytes) as total_bytes` |
| `avg(<field>)` | Average (mean) | `avg(duration_ms) as avg_latency` |
| `min(<field>)` | Minimum | `min(duration_ms) as fastest` |
| `max(<field>)` | Maximum | `max(duration_ms) as slowest` |
| `dc(<field>)` | Distinct count | `dc(client_ip) as unique_clients` |
| `values(<field>)` | List of distinct values | `values(level) as seen_levels` |
| `stdev(<field>)` | Standard deviation | `stdev(duration_ms) as stdev_latency` |
| `percentile(<field>, <pct>)` | Any percentile (0-100) | `percentile(duration_ms, 99.9) as p999` |
| `perc50(<field>)` | Median (50th pct) | `perc50(duration_ms) as median` |
| `perc75(<field>)` | 75th percentile | `perc75(duration_ms) as p75` |
| `perc90(<field>)` | 90th percentile | `perc90(duration_ms) as p90` |
| `perc95(<field>)` | 95th percentile | `perc95(duration_ms) as p95` |
| `perc99(<field>)` | 99th percentile | `perc99(duration_ms) as p99` |
| `earliest(<field>)` / `first(<field>)` | First value by time | `earliest(status) as first_status` |
| `latest(<field>)` / `last(<field>)` | Last value by time | `latest(status) as last_status` |

All aggregation functions skip null values except `count()` (no argument).

---

## Eval Functions

Used in `let`, `eval`, `where`, and any expression context.

### Conditional

| Function | Description | Example |
|----------|-------------|---------|
| `if(cond, then, else)` | Ternary conditional | `if(status >= 500, "error", "ok")` |
| `case(c1, v1, c2, v2, ..., [default])` | Multi-way conditional | `case(x < 0, "neg", x > 0, "pos", "zero")` |

### Null

| Function | Description | Example |
|----------|-------------|---------|
| `coalesce(a, b, ...)` | First non-null argument | `coalesce(name, "unknown")` |
| `isnull(field)` | True if null | `isnull(error_code)` |
| `isnotnull(field)` | True if not null | `isnotnull(user_id)` |
| `null()` | Null constant | `null()` |

### Type Conversion

| Function | Description | Example |
|----------|-------------|---------|
| `tonumber(val)` | Convert to number | `tonumber("42.5")` |
| `tostring(val)` | Convert to string | `tostring(status)` |

### Type Checking

| Function | Description | Example |
|----------|-------------|---------|
| `isnum(val)` | True if numeric | `isnum(response_time)` |
| `isint(val)` | True if integer | `isint(count_str)` |

### String

| Function | Description | Example |
|----------|-------------|---------|
| `len(s)` | String length | `len(message)` |
| `lower(s)` | Lowercase | `lower(level)` |
| `upper(s)` | Uppercase | `upper(method)` |
| `substr(s, start, len)` | Substring (1-indexed) | `substr(uri, 1, 50)` |
| `match(s, pattern)` | Regex match (boolean) | `match(uri, "^/api/")` |
| `replace(s, pattern, repl)` | Replace regex matches | `replace(msg, "\\d+", "N")` |
| `split(s, delim)` | Split into array | `split(tags, ",")` |
| `startswith(s, prefix)` | Starts with prefix | `startswith(uri, "/api")` |
| `endswith(s, suffix)` | Ends with suffix | `endswith(file, ".log")` |
| `contains(s, substr)` | Contains substring | `contains(msg, "timeout")` |
| `ilike(s, pattern)` | Case-insensitive wildcard | `ilike(path, "/API/%")` |

### Math

| Function | Description | Example |
|----------|-------------|---------|
| `round(val, [decimals])` | Round to N decimal places | `round(ratio, 2)` |
| `abs(val)` | Absolute value | `abs(delta)` |
| `ceil(val)` / `ceiling(val)` | Round up | `ceil(ratio)` |
| `floor(val)` | Round down | `floor(status / 100) * 100` |
| `sqrt(val)` | Square root | `sqrt(variance)` |
| `ln(val)` | Natural logarithm | `ln(count + 1)` |

### Multivalue

| Function | Description | Example |
|----------|-------------|---------|
| `mvappend(v1, v2, ...)` | Concatenate into multivalue | `mvappend(tag1, tag2)` |
| `mvjoin(array, sep)` | Join array into string | `mvjoin(tags, ", ")` |
| `mvdedup(array)` | Remove duplicates | `mvdedup(tags)` |
| `mvcount(array)` | Count elements | `mvcount(tags)` |

### Time

| Function | Description | Example |
|----------|-------------|---------|
| `strftime(timestamp, format)` | Format timestamp | `strftime(_timestamp, "%Y-%m-%d %H:%M")` |

### Network

| Function | Description | Example |
|----------|-------------|---------|
| `cidrmatch(cidr, ip)` | IP in CIDR range | `cidrmatch("10.0.0.0/8", src_ip)` |

### JSON

| Function | Description | Example |
|----------|-------------|---------|
| `json_extract(f, path)` | Extract value by path | `json_extract(data, "user.name")` |
| `json_valid(f)` | Validate JSON | `json_valid(body)` |
| `json_keys(f, [path])` | Get object keys | `json_keys(config)` |
| `json_array_length(f, [path])` | Get array length | `json_array_length(items)` |
| `json_type(f, [path])` | Get value type | `json_type(data, "user.age")` |
| `json_object(k, v, ...)` | Build JSON object | `json_object("a", 1, "b", 2)` |
| `json_array(v, ...)` | Build JSON array | `json_array(1, 2, 3)` |
| `json_set(j, path, val)` | Set value at path | `json_set(cfg, "db.host", "\"new\"")` |
| `json_remove(j, path)` | Remove key at path | `json_remove(cfg, "password")` |
| `json_merge(j1, j2)` | Merge two objects | `json_merge(defaults, overrides)` |

---

## SPL2 Equivalence Table

Every SPL2 command has a Lynx Flow counterpart. Both syntaxes are first-class.

| SPL2 | Lynx Flow | Notes |
|------|-----------|-------|
| `FROM` | `from` / `index` | Identical semantics |
| `SEARCH` | `search` | Identical semantics |
| `WHERE` | `where` | Lynx Flow adds `between`, `??`, `?` |
| `EVAL` | `let` | Same expression language |
| `STATS ... BY` | `group by ... compute` | Reversed clause order |
| `TIMECHART span=` | `every <span> compute` | Integrated syntax |
| `BIN` | `bucket` | Same semantics |
| `SORT -field` | `order by field desc` | Explicit direction |
| `HEAD N` | `take N` | Identical |
| `TAIL N` | `tail N` | Identical |
| `TOP N field` | `top N field` | Identical |
| `RARE N field` | `rare N field` / `bottom N field` | `rare` is alias for `bottom` |
| `DEDUP` | `dedup` | Identical |
| `FIELDS` | `keep` | Include mode |
| `FIELDS -` | `omit` | Exclude mode |
| `TABLE` | `table` | Identical |
| `RENAME` | `rename` | Identical |
| `REX` | `parse regex(...)` | Unified parse syntax |
| `unpack_json` | `parse json(...)` | Unified parse syntax |
| `unpack_logfmt` | `parse logfmt(...)` | Unified parse syntax |
| `unpack_syslog` | `parse syslog(...)` | Unified parse syntax |
| `UNROLL` | `explode` | Clearer name |
| `PACK_JSON` | `pack ... into` | Clearer syntax |
| `STREAMSTATS` | `running` | Clearer name |
| `EVENTSTATS` | `enrich` | Clearer name |
| `JOIN` | `join` | Identical |
| `APPEND` | `append` | Identical |
| `MULTISEARCH` | `multisearch` | Identical |
| `TRANSACTION` | `transaction` | Identical |
| `XYSERIES` | `xyseries` | Identical |
| `FILLNULL` | `fillnull` | Lynx Flow adds `??` operator |
| `MATERIALIZE` | `materialize` | Identical |
| `VIEWS` | `views` | Identical |
| `DROPVIEW` | `dropview` | Identical |

---

## Syntax Classification

| Level | Meaning | Formatter Behavior |
|-------|---------|-------------------|
| **Canonical** | Preferred, documented form | Output as-is |
| **Accepted alias** | Convenience synonym, fine in interactive use | Normalized on format |
| **Legacy** | Exists for SPL1/SPL2 migration only | Normalized + linter hint |

| Form | Level | Canonical Equivalent |
|------|-------|---------------------|
| `from <source>` | Canonical | -- |
| `index <source>` | Accepted alias | `from` |
| `index="name"` | Legacy | `from` |
| `where <expr>` | Canonical | -- |
| `order by` | Canonical | -- |
| `sort` | Accepted alias | `order by` |
| `take N` | Canonical | -- |
| `head N` | Accepted alias | `take` |
| `let` | Canonical | -- |
| `eval` | Accepted alias | `let` |
| `group by ... compute` | Canonical | -- |
| `stats ... by` | Accepted alias | `group by ... compute` |
| `keep` | Canonical | -- |
| `fields` | Accepted alias | `keep` |
| `omit` | Canonical | -- |
| `fields -` | Accepted alias | `omit` |
| `every <span> compute` | Canonical | -- |
| `timechart span=` | Accepted alias | `every ... compute` |
| `running` | Canonical | -- |
| `streamstats` | Accepted alias | `running` |
| `enrich` | Canonical | -- |
| `eventstats` | Accepted alias | `enrich` |
| `parse <format>(...)` | Canonical | -- |
| `unpack_*` / `rex` | Accepted alias | `parse ...` |
| `explode` | Canonical | -- |
| `unroll` | Accepted alias | `explode` |
| `pack ... into` | Canonical | -- |
| `pack_json` | Accepted alias | `pack ... into` |
| `bucket` | Canonical | -- |
| `bin` | Accepted alias | `bucket` |
| `rare` | Legacy | `bottom` |
