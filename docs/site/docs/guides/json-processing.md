---
title: Working with JSON Logs
description: How LynxDB stores JSON events and how to query them with dotted field names, spath, flatten, expand and the json_* functions.
---

# Working with JSON Logs

LynxDB stores a JSON event's text in `_raw` and extracts its top-level
values as typed fields when it ingests the event. Nested objects and arrays
are reached at query time with `spath`, `flatten`, `expand` and the `json_*`
functions.

The examples use events like this one:

```json
{"timestamp":"2026-03-02T09:00:01.000Z","level":"error","service":"checkout","http.method":"POST","http.status":502,"duration_ms":5012,"user":{"id":"u-17","plan":"pro"},"items":[{"sku":"A1","qty":2},{"sku":"B3","qty":1}],"tags":["payments","retry"]}
```

## What ingestion extracts

For a JSON line sent as raw text (`lynxdb ingest`, `POST /api/v1/ingest/raw`,
pipe mode):

- Each top-level string, number and boolean becomes a field with its type:
  `level`, `service`, `http.method`, `http.status`, `duration_ms`.
- Nested objects (`user`) and arrays (`items`, `tags`) are not extracted.
  They remain in `_raw`.
- A timestamp near the start of the line becomes `_time`; see
  [Field Extraction](./field-extraction.md#what-ingestion-extracts).

Documents sent through the Elasticsearch bulk API are stored with every key,
and nested objects are kept as object values: the `kubernetes` object that
Fluent Bit adds is a column of type object. OTLP attributes arrive as flat dotted
names such as `resource.service.name`.

## Dotted field names

A field whose name contains dots, such as `http.status` from the JSON key
`"http.status"` or `resource.service.name` from OTLP, is referred to by its
name as written. Backticks quote it explicitly:

```
source=main | where http.status >= 400 | stats count() as n by service, http.method
source=main | where `http.status` >= 400 | fields service, `http.status`
```

```text
http.method,n,service
POST,2,checkout
```

A dotted name refers to a column, not to a path inside an object: `user.id`
is empty until `spath` or `flatten` creates a column with that name.

## Extract one value with `spath`

`spath path=<path>` reads a JSON value out of `_raw` (or `input=<field>`)
and stores it in `output=<field>` (by default, a field named like the
path). Use dots for object members and `{N}` for array positions:

```
source=main | spath path=user.id output=user_id | stats count() as requests by user_id
```

```text
requests,user_id
2,u-17
1,u-04
1,u-22
```

```
source=main | spath path=user.plan output=plan | where plan = "pro" | fields service, duration_ms
source=main | spath path=items{0}.sku output=first_sku | fields service, first_sku
```

To read inside an object column, for example from an Elasticsearch bulk
document, set `input=`:

```
source=fluent-bit | spath input=kubernetes path=pod_name output=pod | stats count() by pod
```

## Extract everything with `spath` and `flatten`

`spath` without `path=` parses the whole document into one object column.
`flatten` then turns each member into a column named by its path:

```
source=main | spath output=doc | flatten doc | fields service, user.id, user.plan
```

```text
service,user.id,user.plan
checkout,u-17,pro
catalog,u-04,free
checkout,u-17,pro
checkout,u-22,free
```

`flatten` also works directly on an object column: on Fluent Bit events,
`flatten kubernetes` adds `pod_name`, `namespace_name` and the other
members of `kubernetes`.

## Arrays into rows

`expand` (or `mvexpand`) emits one row per array element. Combined with
`flatten`, it unpacks arrays of objects:

```
source=main
| spath output=doc | flatten doc
| expand items | flatten items
| stats sum(qty) as units by sku
| sort - units
```

```text
sku,units
C9,4
A1,3
B3,1
```

An event whose array is empty yields one row with empty values.

## JSON functions

These functions take and return JSON text and work in `eval` and `where`:

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

```
source=main
| eval plan = json_extract(_raw, '$.user.plan'),
       n_items = json_array_length(json_extract(_raw, '$.items'))
| fields service, plan, n_items
```

```text
n_items,plan,service
2,pro,checkout
0,free,catalog
1,pro,checkout
1,free,checkout
```

Filter on a nested value without creating a column:

```
source=main | where json_extract(_raw, '$.user.plan') = 'pro' | stats count() as n by service
```

Build a compact JSON summary for export:

```
source=main | eval summary = json_object('service', service, 'status', http.status) | fields summary
```

```text
{"service":"checkout","status":502}
{"service":"catalog","status":200}
...
```

## Mixed JSON and text

When a stream mixes JSON and plain lines, `json_valid` separates them:

```
source=main | eval is_json = json_valid(_raw) | stats count() as n by is_json
source=main | where NOT json_valid(_raw) | fields _raw | head 10
```

## JSON inside a text field

When JSON is embedded in a string field, such as the `log` field of Docker
`json-file` lines or the `message` of a syslog line, point `spath` or
`json_extract` at that field:

```
source=main | spath input=message path=request.id output=request_id | stats count() by request_id
source=main | eval code = json_extract(log, '$.status') | where code = '500' | fields _time, log
```

## Tips

- Search terms first. `source=main checkout | spath ...` lets the term index
  skip events before any JSON is parsed.
- Extract one path instead of the whole document when you need a few
  values; `spath path=` reads less than `spath | flatten`.
- Values that you filter or group on in most queries are cheaper as
  top-level keys, which ingestion stores as typed columns.

## Next steps

- [Field Extraction](./field-extraction.md): `parse`, `rex`, `grok` and ingest-time extraction
- [Function guide](../ppl/function-guide.md): every JSON, string and conversion function
- [Command guide](../ppl/command-guide.md): `spath`, `flatten`, `expand`
