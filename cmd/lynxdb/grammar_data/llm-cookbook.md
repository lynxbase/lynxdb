# SPL2 LLM Cookbook

Prompt patterns and examples for translating natural language to LynxDB SPL2 queries.

---

## 1. System Prompt Template

Use this system prompt for NL→SPL2 translation tasks:

```
You are an expert at translating natural language questions into LynxDB SPL2 queries.

## Rules
- Output ONLY the SPL2 query. No explanation unless asked.
- Use implicit FROM when querying the main index.
- Prefer search for keyword matching, WHERE for expression filtering.
- Use pipe (|) to chain commands. First pipe is optional.
- Strings use double quotes. Numbers are bare.
- Field names are unquoted identifiers.
- Aggregations: count, sum, avg, min, max, dc, values, stdev, perc50..perc99
- Eval functions: if, case, coalesce, isnull, isnotnull, tonumber, tostring,
  round, ceil, floor, abs, sqrt, ln, len, lower, upper, substr, match, like,
  replace, split, strftime, mvappend, mvjoin, mvdedup, mvcount

## Commands
Filter:    search, where
Aggregate: stats, timechart, top, rare, eventstats, streamstats
Transform: eval, rex, rename, table, fields, keep, omit, fillnull, bin
Order:     sort, head, tail, take, dedup
Join:      join, append
Session:   transaction, sessionize
Explore:   glimpse, describe
Advanced:  materialize, from (views), correlate, topology, tee
Structured: unpack_json, unpack_logfmt, json, unroll, pack_json, parse, explode, pack

## Key syntax
- Implicit search:  field=value field2=value2
- Explicit search:  search "keyword"
- WHERE:            | where expr
- STATS:            | stats func(field) by group_field
- EVAL:             | eval new_field = expr
- TIMECHART:        | timechart func(field) span=5m
- REX:              | rex field=_raw "(?P<name>pattern)"
- JOIN:             | join type=inner field [$variable]
- CTE:              $name = query; | from $name
- Materialized view: query | materialize "name" retention=90d
```

---

## 2. Schema Injection Pattern

Inject your field catalog into the prompt so the LLM generates valid field names.

```
## Available Fields
| Field         | Type     | Coverage | Example Values              |
|---------------|----------|----------|-----------------------------|
| _timestamp    | datetime | 100%     | 2026-03-23T14:30:00Z       |
| _raw          | string   | 100%     | Full log line text          |
| _source       | string   | 100%     | nginx, api-gw, redis        |
| level         | string   | 100%     | INFO, WARN, ERROR, DEBUG    |
| status        | integer  | 50%      | 200, 404, 500              |
| duration_ms   | float    | 50%      | 0.1 to 30001.0             |
| source        | string   | 100%     | nginx, api-gw, redis        |
| host          | string   | 80%      | web-01, web-02, api-01     |
| endpoint      | string   | 60%      | /api/users, /health         |
| method        | string   | 40%      | GET, POST, PUT, DELETE      |
| client_ip     | string   | 70%      | 192.168.1.1                |
| user_id       | integer  | 30%      | 1 to 99999                 |
| message       | string   | 95%      | Human-readable log message  |
| error_type    | string   | 20%      | timeout, oom, panic         |
| service       | string   | 85%      | auth, billing, gateway      |
```

### Dynamic injection example

```
## Available Fields
{{#each fields}}
| {{name}} | {{type}} | {{coverage}}% | {{top_values}} |
{{/each}}
```

---

## 3. Few-Shot Examples

Include 5-10 examples covering common patterns:

```
Q: Count errors by source
A: level=error | stats count by source

Q: Average latency per endpoint over the last hour
A: | stats avg(duration_ms) by endpoint

Q: Show error rate over time in 5-minute buckets
A: level=error | timechart count span=5m

Q: Top 10 slowest endpoints
A: | stats avg(duration_ms) as avg_dur by endpoint | sort -avg_dur | head 10

Q: Find all 500 errors from nginx
A: source=nginx status=500

Q: Extract user agent and count requests
A: | rex field=_raw "user_agent\":\"(?P<ua>[^\"]+)\"" | stats count by ua | sort -count

Q: Compare error counts across services
A: level=error | stats count by source | sort -count

Q: Create a 5-minute error summary view
A: level=error | stats count by source, time_bucket(timestamp, "5m") as bucket | materialize "mv_errors_5m" retention=90d

Q: Find IP addresses hitting the server most
A: | stats count by client_ip | sort -count | head 20

Q: Show percentile latencies
A: | stats perc50(duration_ms), perc90(duration_ms), perc95(duration_ms), perc99(duration_ms)
```

---

## 4. Error Correction Loop

When the LLM generates invalid SPL2, use this correction pattern:

### Step 1: Parse and validate

```
lynxdb query --explain '<generated_query>'
```

### Step 2: Inject error feedback

```
The previous query had an error:

  Error: unknown function "percent" at position 42
  Hint: did you mean "percentile"?

Available aggregation functions: count, sum, avg, min, max, dc, values, stdev,
percentile, perc50, perc75, perc90, perc95, perc99, earliest, latest

Please correct the query.
```

### Step 3: Retry prompt template

```
Your previous SPL2 query was invalid:

  Query: {original_query}
  Error: {error_message}
  Suggestion: {hint_if_available}

Common mistakes:
- Use perc95() not percentile(95)
- Use dc() not distinct_count()
- Use | not || for pipe
- Field names are unquoted: source not "source"
- String values are quoted: level="error" not level=error (for strings)
- Numbers are unquoted: status=500 not status="500" (for numbers)
- Use "as" not "AS" for aliases (though both work, lowercase is conventional)
- Time spans: span=5m not span="5m"

Generate a corrected query:
```

### Automated correction pipeline

```python
def generate_and_correct(nl_query, max_retries=3):
    system_prompt = SPL2_SYSTEM_PROMPT
    schema = load_field_catalog()

    for attempt in range(max_retries):
        query = llm.generate(system_prompt, schema, nl_query)
        result = subprocess.run(
            ["lynxdb", "query", "--explain", query],
            capture_output=True, text=True
        )

        if result.returncode == 0:
            return query

        error = result.stderr
        hint = extract_hint(error)
        nl_query = f"""Previous attempt failed:
Query: {query}
Error: {error}
Hint: {hint}
Please fix the query."""

    raise MaxRetriesExceeded()
```

---

## 5. Common Patterns

### 5.1 Filter patterns

| Natural language | SPL2 |
|---|---|
| "show errors" | `level=error` |
| "from nginx" | `source=nginx` |
| "status 500" | `status=500` |
| "slow requests" | `duration_ms>1000` |
| "search for X" | `search "X"` |
| "not debug" | `level!=debug` |
| "errors from nginx" | `level=error source=nginx` |
| "500 or 502 errors" | `status in (500, 502)` |

### 5.2 Aggregation patterns

| Natural language | SPL2 |
|---|---|
| "count events" | `\| stats count` |
| "count by source" | `\| stats count by source` |
| "average latency" | `\| stats avg(duration_ms)` |
| "max duration" | `\| stats max(duration_ms)` |
| "unique users" | `\| stats dc(user_id)` |
| "95th percentile" | `\| stats perc95(duration_ms)` |
| "top 10 URIs" | `\| top 10 uri` |

### 5.3 Time series patterns

| Natural language | SPL2 |
|---|---|
| "errors per minute" | `level=error \| timechart count span=1m` |
| "hourly rate" | `\| timechart count span=1h` |
| "by service over time" | `\| timechart count by source span=5m` |
| "daily aggregation" | `\| bin _time span=1d \| stats count by _time` |

### 5.4 Transform patterns

| Natural language | SPL2 |
|---|---|
| "extract IP" | `\| rex field=_raw "(?P<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)"` |
| "add flag field" | `\| eval is_error=if(status>=500, true, false)` |
| "rename column" | `\| rename status as http_status` |
| "select columns" | `\| table _time, source, level, message` |
| "fill missing values" | `\| fillnull value=0 duration_ms` |

### 5.5 Advanced patterns

| Natural language | SPL2 |
|---|---|
| "join with user data" | `\| join type=left user_id [$users]` |
| "group into sessions" | `\| transaction session_id maxspan=30m` |
| "create a view" | `\| materialize "name" retention=90d` |
| "query a view" | `\| from view_name` |
| "rolling average" | `\| streamstats window=10 avg(duration_ms)` |
| "cumulative sum" | `\| streamstats sum(count) as total` |

### 5.6 Edge cases to handle

1. **Numeric vs string comparison**: `status=500` (numeric) vs `level="error"` (string)
2. **Implicit AND in search**: `level=error source=nginx` (both must match)
3. **Search precedence**: OR binds tighter than AND in search expressions
4. **CTE references**: `$name` in FROM vs `$name` in JOIN subsearch
5. **Span units**: `5m`, `1h`, `30s`, `1d`, `1w` — no quotes around span values
6. **Function names**: `dc` not `distinct_count`, `perc95` not `percentile(95)`
7. **AS keyword**: lowercase `as` preferred: `stats count as total`
8. **Pipe optional**: First pipe is implicit: `level=error | stats count` == `| level=error | stats count`

---

## 6. Token Budget Optimization

For long prompts, prioritize:

1. **System prompt** (~200 tokens): Always include
2. **Schema fields** (~50-200 tokens): Include top 15-20 fields by coverage
3. **Few-shot examples** (~300-500 tokens): 5-8 examples covering key patterns
4. **Error context** (~100 tokens): Only on retry

Total budget: ~650-1000 tokens for the prompt, leaving room for query generation.

### Minimal prompt (for token-constrained models)

```
Translate to SPL2. Rules: field=value for filter, | stats func(field) by group,
| timechart func span=5m, | eval x=expr, | sort -field, | head N.
Aggregations: count, sum, avg, min, max, dc, perc50-99.
Eval: if, coalesce, match, lower, upper, round, substr.
No explanation. Query only.
```

---

## 7. Validation Checklist

Before deploying an NL→SPL2 system:

- [ ] System prompt covers all commands your users need
- [ ] Schema includes fields with >10% coverage
- [ ] Few-shot examples match your data domain
- [ ] Error correction loop is wired up
- [ ] Edge cases (numeric vs string, span units) are documented
- [ ] Token budget fits your model's context window
- [ ] Generated queries are validated with `lynxdb query --explain`
- [ ] Output format is constrained (query only, no explanation)
