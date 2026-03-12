---
title: JSON Functions
description: Functions for extracting, validating, inspecting, constructing, and mutating JSON data.
---

# JSON Functions

LynxDB provides a set of eval functions for working with JSON data inside expressions. Use these in `eval`, `where`, and `stats` contexts.

## Extraction

### json_extract(field, path)

Extract a value from a JSON string by dot-notation path. Supports bracket notation for array indexing: `items[0]`, `items[-1]`, `items[*]`.

```spl
| eval user_name = json_extract(metadata, "user.name")
| eval first_item = json_extract(data, "items[0].sku")
| eval all_names = json_extract(data, "users[*].name")
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `field` | Field containing JSON text |
| `path` | Dot-notation path to the value (supports bracket array indexing) |

**Returns:** The extracted value with automatic type inference (string, integer, float, boolean, or null). Nested objects and arrays are returned as JSON strings. Wildcard `[*]` returns a JSON array of all matching values.

### json_keys(field [, path])

Return the keys of a JSON object as a JSON array string.

```spl
| eval top_keys = json_keys(metadata)
| eval nested_keys = json_keys(config, "database")
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `field` | Field containing JSON text |
| `path` | Optional dot-notation path to a nested object |

**Returns:** A JSON array string of key names (e.g., `["name","age","email"]`). Returns null if the target is not an object.

### json_array_length(field [, path])

Return the length of a JSON array.

```spl
| eval num_items = json_array_length(order, "items")
| where json_array_length(tags) > 3
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `field` | Field containing JSON text |
| `path` | Optional dot-notation path to a nested array |

**Returns:** An integer count of array elements. Returns null if the target is not an array.

## Validation

### json_valid(field)

Test whether a field contains valid JSON.

```spl
| eval is_json = json_valid(message)
| where json_valid(_raw) = true
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `field` | Field to validate |

**Returns:** Boolean `true` if the field contains valid JSON, `false` otherwise.

## Inspection

### json_type(field [, path])

Return the JSON type of a value at the given path.

```spl
| eval field_type = json_type(data, "user.age")
| where json_type(payload) = "object"
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `field` | Field containing JSON text |
| `path` | Optional dot-notation path to inspect |

**Returns:** One of: `"string"`, `"number"`, `"boolean"`, `"array"`, `"object"`, or `"null"`. Returns null for non-JSON input or missing paths.

## Construction

### json_object(key1, value1, key2, value2, ...)

Build a JSON object from key-value pairs.

```spl
| eval summary = json_object("host", host, "count", count, "status", "ok")
| eval envelope = json_object("type", "metric", "data", json_object("value", metric_val))
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| key-value pairs | Alternating key (string) and value arguments. Must be an even number of arguments. |

**Returns:** A JSON object string (e.g., `{"host":"web-01","count":42,"status":"ok"}`).

### json_array(value1, value2, ...)

Build a JSON array from values.

```spl
| eval tags_json = json_array("admin", "user", "dev")
| eval ids = json_array(id1, id2, id3)
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| values | One or more values to include in the array |

**Returns:** A JSON array string (e.g., `["admin","user","dev"]`).

## Mutation

### json_set(json, path, value)

Set or create a value at a dot-notation path in a JSON object. Creates intermediate objects as needed.

```spl
| eval updated = json_set(config, "database.host", "\"db-02\"")
| eval enriched = json_set(metadata, "processed", "true")
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `json` | Field containing a JSON object string |
| `path` | Dot-notation path to set |
| `value` | JSON value to set (must be valid JSON: `"\"string\""`, `42`, `true`, `null`, etc.) |

**Returns:** A new JSON string with the value set at the specified path. Returns null if the input is not a valid JSON object.

### json_remove(json, path)

Remove a key at a dot-notation path from a JSON object.

```spl
| eval cleaned = json_remove(metadata, "internal_id")
| eval stripped = json_remove(config, "database.password")
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `json` | Field containing a JSON object string |
| `path` | Dot-notation path to the key to remove |

**Returns:** A new JSON string with the key removed. Returns the original JSON unchanged if the path doesn't exist. Returns null if the input is not a valid JSON object.

### json_merge(json1, json2)

Shallow-merge two JSON objects. Keys from the second object overwrite the first on conflict.

```spl
| eval combined = json_merge(defaults, overrides)
| eval enriched = json_merge(event_json, json_object("processed_at", now()))
```

**Arguments:**

| Argument | Description |
|----------|-------------|
| `json1` | First JSON object string (base) |
| `json2` | Second JSON object string (overrides on conflict) |

**Returns:** A new JSON string with all keys from both objects. Returns null if either input is not a valid JSON object.

## Summary

| Function | Purpose | Example |
|----------|---------|---------|
| `json_extract(f, path)` | Extract value by path | `json_extract(data, "user.name")` |
| `json_keys(f [, path])` | Get object keys | `json_keys(config)` |
| `json_array_length(f [, path])` | Get array length | `json_array_length(items)` |
| `json_valid(f)` | Validate JSON | `json_valid(message)` |
| `json_type(f [, path])` | Get value type | `json_type(data, "user.age")` |
| `json_object(k, v, ...)` | Build JSON object | `json_object("a", 1, "b", 2)` |
| `json_array(v, ...)` | Build JSON array | `json_array(1, 2, 3)` |
| `json_set(j, path, val)` | Set value at path | `json_set(cfg, "db.host", "\"new\"")` |
| `json_remove(j, path)` | Remove key at path | `json_remove(cfg, "password")` |
| `json_merge(j1, j2)` | Merge two objects | `json_merge(defaults, overrides)` |

## See Also

- [json command](/docs/lynx-flow/commands/json-cmd) -- Quick JSON field extraction
- [unpack_json](/docs/lynx-flow/commands/unpack-json) -- Full JSON extraction pipeline command
- [pack_json](/docs/lynx-flow/commands/pack-json) -- Assemble fields into JSON
- [Working with JSON Logs](/docs/guides/json-processing) -- Comprehensive JSON processing guide
- [Eval Functions](/docs/lynx-flow/functions/eval-functions) -- All eval functions
