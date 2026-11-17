---
title: Working with JSON Logs
description: How LynxDB stores JSON events and how to query them with dotted field names, expand, flatten, spath and the json_* functions.
---

# Working with JSON Logs

LynxDB stores a JSON event as the object it is. Every key is a field with its
JSON type, the members of a nested object are fields named by their path, and
arrays keep their elements. Queries reach nested values by name, without an
extraction step.

The examples use events like this one:

```json
{"timestamp":"2026-03-02T09:00:01.000Z","level":"error","service":"checkout","http":{"method":"POST","status":502},"duration_ms":5012,"user":{"id":"u-17","plan":"pro"},"items":[{"sku":"A1","qty":2},{"sku":"B3","qty":1}],"tags":["payments","retry"]}
```

## How JSON events are stored

A JSON object sent to `POST /api/v1/ingest`, `POST /api/v1/ingest/raw`,
`lynxdb ingest`, an Elasticsearch bulk request or pipe mode is stored as sent:

- Each top-level string, number, boolean and null is a field with its type:
  `level`, `service`, `duration_ms`.
- Each member of a nested object is a field named by its path:
  `http.method`, `http.status`, `user.id`, `user.plan`.
- An array is one field holding the array: `items`, `tags`.
- The event time comes from the first of `_time`, `timestamp`, `@timestamp`,
  `time` and `ts` that the event holds (`storage.time_fields`), otherwise from
  the time it arrived.
- The event has no `_raw`. Search terms match the text of its string fields.

The first event above reads back as:

```json
{"_time":"2026-03-02T09:00:01Z","duration_ms":5012,"http.method":"POST","http.status":502,"items":[{"qty":2,"sku":"A1"},{"qty":1,"sku":"B3"}],"level":"error","service":"checkout","tags":["payments","retry"],"timestamp":"2026-03-02T09:00:01.000Z","user.id":"u-17","user.plan":"pro"}
```

An event that lacks a field has no value for it: the field is absent from its
row, comparisons on it are false, and `stats ... by` puts the event in a
group whose key is null.

## Dotted field names

A nested field is referred to by its path. Backticks quote a name
explicitly:

```
source=main | where http.status >= 400 | stats count() as n by service, http.method
```

```text
http.method,n,service
POST,2,checkout
```

```
source=main | where `http.status` >= 400 | fields service, `http.status`
```

```text
http.status,service
502,checkout
503,checkout
```

```
source=main | where user.plan = "pro" | fields service, duration_ms
source=main | stats count() as requests by user.id | sort - requests, user.id
```

```text
requests,user.id
2,u-17
1,u-04
1,u-22
```

## Arrays into rows

`expand` (or `mvexpand`) emits one row per array element:

```
source=main | expand tags | stats count() as n by tags | sort - n, tags
```

```text
n,tags
2,payments
1,
1,browse
1,retry
```

An event whose array is empty yields one row without a value, the empty
group above.

For an array of objects, `flatten` then turns the members of each element
into columns:

```
source=main
| expand items | flatten items
| stats sum(qty) as units by sku
| sort - units
```

```text
sku,units
C9,4
A1,3
B3,1
,0
```

`json_array_length` counts the elements of an array:

```
source=main | eval n_items = json_array_length(items) | fields service, user.plan, n_items
```

```text
n_items,service,user.plan
2,checkout,pro
0,catalog,free
1,checkout,pro
1,checkout,free
```

## JSON inside a text field

When JSON is embedded in a string field, such as the `log` field of Docker
`json-file` lines or the `message` of a syslog line, `spath` and the `json_*`
functions read it. The examples use Docker lines such as:

```json
{"log":"{\"status\":500,\"request\":{\"id\":\"r-1\"},\"msg\":\"upstream timeout\"}\n","stream":"stdout","time":"2026-03-02T09:10:00.000000000Z"}
```

`spath input=<field> path=<path>` extracts one value into `output=<field>`.
Use dots for object members and `{N}` for array positions:

```
source=main | spath input=log path=request.id output=request_id | where isnotnull(request_id) | stats count() as n by request_id | sort request_id
```

```text
n,request_id
1,r-1
1,r-2
1,r-3
```

Without `path=`, `spath` parses the whole document into one object, and
`flatten` turns its members into columns:

```
source=main | spath input=log output=doc | flatten doc | where isnotnull(status) | fields status, request.id, msg
```

```text
msg,request.id,status
upstream timeout,r-1,500
ok,r-2,200
db error,r-3,500
```

`json_extract` reads a value at a JSONPath and keeps its JSON type:

```
source=main | eval code = json_extract(log, '$.status') | where code = 500 | fields stream, code
```

```text
code,stream
500,stdout
500,stdout
```

When a field mixes JSON and plain text, `json_valid` separates them:

```
source=main | eval is_json = json_valid(log) | stats count() as n by is_json
```

```text
is_json,n
false,1
true,3
```

## JSON functions

These functions work in `eval` and `where`:

| Function | Result |
|----------|--------|
| `json_valid(s)` | true when `s` is valid JSON |
| `json(s)` | `s` when it is valid JSON, otherwise null |
| `json_extract(s, path, ...)` | the value at a JSONPath such as `'$.user.plan'`; several paths return an array |
| `json_keys(s)` | the keys of a JSON object, as a JSON array |
| `json_array_length(s)` | the number of elements of a JSON array |
| `json_object(k1, v1, ...)` | a JSON object built from key/value pairs |
| `json_array(v1, ...)` | a JSON array |
| `json_set(s, path, value)` | `s` with the value at `path` replaced |
| `json_delete(s, path, ...)` | `s` without the given paths |
| `json_append(s, path, value)` | `s` with `value` appended to the array at `path` |
| `json_extend(s, path, value)` | `s` with the array at `path` extended by another array |

Build a compact JSON summary for export:

```
source=main | eval summary = json_object('service', service, 'status', http.status) | fields summary
```

```text
{"summary":"{\"service\":\"checkout\",\"status\":502}"}
{"summary":"{\"service\":\"catalog\",\"status\":200}"}
...
```

## Tips

- Search terms first. `source=main checkout | ...` lets the scan skip blocks
  without the term before any value is read.
- Name the fields you need with `fields`: each nested field is a column of
  its own, and a query reads only the columns it uses.
- `spath path=` reads less than `spath | flatten` when you need a few values
  out of embedded JSON.

## Next steps

- [Field Extraction](./field-extraction.md): `parse`, `rex`, `grok` and ingest-time extraction
- [Function guide](../ppl/function-guide.md): every JSON, string and conversion function
- [Command guide](../ppl/command-guide.md): `spath`, `flatten`, `expand`
