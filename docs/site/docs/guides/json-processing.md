---
title: Working with JSON Logs
description: How to parse, query, and transform JSON log data using dot-notation, unpack commands, JSON functions, and unroll.
---

# Working with JSON Logs

LynxDB provides multiple tools for working with JSON log data, from zero-config dot-notation to full structural extraction and array manipulation. This guide covers all the approaches and when to use each one.

## Quick reference

| Approach | Use when | Example |
|----------|----------|---------|
| **Dot-notation** | Ad-hoc queries on nested JSON | `\| where response.status >= 500` |
| **`\| json`** | Quick extraction of all/some fields | `\| json \| stats count by level` |
| **`\| unpack_json`** | Production pipelines with prefix/field control | `\| unpack_json prefix=app_` |
| **`json_extract()`** | Extracting one value in an eval/where | `\| eval name = json_extract(data, "user.name")` |
| **`\| unroll`** | Exploding JSON arrays into rows | `\| unroll field=items` |

---

## Dot-notation: zero-config nested access

LynxDB supports dot-notation for accessing nested JSON fields directly in `where`, `eval`, and `stats` without any explicit extraction step.

```bash
# Filter on a nested field -- no unpack needed
echo '{"level":"error","request":{"method":"POST","duration_ms":5012}}' \
  | lynxdb query '| where request.duration_ms > 1000 | table level, request.method'
```

### How it works

When the VM encounters a field like `request.method`:

1. **Direct lookup** -- checks if a field literally named `request.method` exists (e.g., from a previous `unpack_json`).
2. **Root extraction** -- if not found, looks for a field named `request` and extracts `method` from its JSON value.
3. **_raw fallback** -- if the root field doesn't exist either, tries to extract `request.method` from `_raw`.

This means dot-notation works whether or not you've explicitly extracted fields.

### Multi-level nesting

```spl
-- Access deeply nested values
| where response.headers.content_type = "application/json"
| eval origin = request.headers.origin
| stats count by response.status
```

---

## `| json` -- quick extraction

The `| json` command extracts all JSON keys from `_raw` (or a specified field) into top-level fields.

```bash
# Extract all fields
cat app.json | lynxdb query '| json | stats count by level'

# Extract specific paths only (faster for wide objects)
cat app.json | lynxdb query '| json level, status, duration_ms | where status >= 500'

# Extract from a non-default field
cat logs.json | lynxdb query '| json field=message | table level, service'
```

---

## `| unpack_json` -- production extraction

For production pipelines, `unpack_json` provides full control over extraction:

```spl
-- Add a prefix to avoid field name collisions
| unpack_json prefix=app_
| where app_level = "error"

-- Extract only specific fields (performance optimization)
| unpack_json fields=level,status,duration_ms

-- Keep the original _raw field
| unpack_json keep_original=true

-- Extract from a specific field
| unpack_json field=metadata prefix=meta_
```

---

## JSON eval functions

Use JSON functions in `eval` and `where` for precise extraction and validation.

### Extract a single value

```spl
| eval user = json_extract(payload, "user.name")
| eval first_tag = json_extract(metadata, "tags.0")
```

### Validate JSON before processing

```spl
| where json_valid(message) = true
| unpack_json field=message
```

### Inspect structure

```spl
-- Get keys of a JSON object
| eval keys = json_keys(config)

-- Get length of a JSON array
| eval num_items = json_array_length(order, "items")
| where num_items > 10
```

### Build JSON output

```spl
-- Construct a JSON object for export
| stats count by host, level
| eval summary = json_object("host", host, "level", level, "count", count)
| table summary
```

See the [JSON Functions Reference](/docs/lynx-flow/functions/json-functions) for the complete API.

---

## `| unroll` -- exploding arrays

When a field contains a JSON array, `| unroll` creates one row per element.

```bash
echo '{"order":"ORD-1","items":[{"sku":"A1","qty":2},{"sku":"B3","qty":1}]}' \
  | lynxdb query '| json | unroll field=items | table order, items.sku, items.qty'
```

Output:

```
order    items.sku    items.qty
ORD-1    A1           2
ORD-1    B3           1
```

### Array of objects

Object elements are flattened with dot-notation. For `field=items`:
- `items.sku` = the `sku` key from each element
- `items.qty` = the `qty` key from each element

### Array of scalars

Scalar elements replace the field value directly:

```spl
-- Input: {"name": "alice", "tags": ["admin", "user"]}
| json | unroll field=tags
-- Row 1: name=alice, tags=admin
-- Row 2: name=alice, tags=user
```

### Aggregate over unrolled data

```spl
| json
| unroll field=items
| stats sum(items.qty) AS total_sold, dc(order_id) AS orders by items.sku
| sort -total_sold
| head 20
```

---

## Chaining parsers

Real-world logs often have nested formats. Chain extraction commands to handle them:

### Docker JSON logs with embedded application log

```bash
# Docker wraps each log line in JSON: {"log":"...","stream":"stdout","time":"..."}
# The inner "log" field contains the application's logfmt output
cat docker-logs.json | lynxdb query '
  | json
  | unpack_logfmt field=log prefix=app_
  | where app_level = "error"
  | stats count by app_service'
```

### Syslog with embedded JSON

```bash
# Syslog header wraps a JSON application message
cat syslog.log | lynxdb query '
  | unpack_syslog
  | unpack_json field=message prefix=app_
  | stats count by hostname, app_level'
```

### Nginx access log with JSON body field

```bash
cat access.log | lynxdb query '
  | unpack_combined
  | unpack_json field=request_body prefix=body_
  | where body_action = "purchase"
  | stats sum(body_amount) by client_ip'
```

---

## Performance tips

1. **Use dot-notation for ad-hoc queries.** No extraction overhead for fields you don't materialize.

2. **Specify `fields=` when using `unpack_json`.** Extracting 3 fields from a 50-key JSON object is much faster than extracting all 50.

3. **Use `json_extract()` in eval/where for single values.** When you only need one field, `json_extract` avoids parsing the entire object.

4. **Place extraction early in the pipeline.** Extract before `where` so the filter can operate on typed fields:
   ```spl
   | json | where status >= 500   -- fast: status is an integer
   ```

5. **Use `json_valid()` to guard against malformed data.** Avoid parse errors in production pipelines:
   ```spl
   | where json_valid(_raw) = true | json
   ```

---

## Next steps

- [json command reference](/docs/lynx-flow/commands/json-cmd)
- [unpack_json reference](/docs/lynx-flow/commands/unpack-json)
- [unroll reference](/docs/lynx-flow/commands/unroll)
- [JSON Functions reference](/docs/lynx-flow/functions/json-functions)
- [Field Extraction Guide](/docs/guides/field-extraction) -- REX, EVAL, and schema-on-read
