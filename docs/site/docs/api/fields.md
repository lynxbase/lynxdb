---
sidebar_position: 5
title: Fields & Sources
description: The field catalog, the top values of a field, and the event sources with their counts.
---

# Fields & Sources API

These endpoints describe stored data from segment metadata, without running a query. Clients use them for field lists and completion. Any valid key can call them when authentication is on.

## GET /fields

Lists the fields found in stored segments, with their type and how many events carry them.

| Parameter | Description |
|---|---|
| `prefix` | Return only fields whose name starts with this prefix |

```bash
curl -s 'localhost:3100/api/v1/fields?prefix=st'
```

```json
{"data": {"fields": [{"name": "status", "type": "int", "coverage": 100, "total_count": 1000}]}}
```

| Field | Description |
|---|---|
| `name` | Field name |
| `type` | Stored type: `string`, `int`, `float`, `bool`, `datetime` |
| `coverage` | Percentage of events that have the field, 0 to 100 |
| `total_count` | Events that have a value for the field |
| `top_values` | Up to five common values with counts, when known; omitted otherwise |

The catalog covers every index. Built-in fields (`_time`, `_raw`, `_source`, `_sourcetype`, `index`, `host`) appear alongside extracted ones.

## GET /fields/\{name\}/values

Returns the most common values of one field.

| Parameter | Default | Description |
|---|---|---|
| `limit` | `10` | Values to return, at most 100 |
| `from` (or `since`) | | Start of the time range: relative or ISO 8601 |
| `to` | | End of the time range |

```bash
curl -s 'localhost:3100/api/v1/fields/level/values?limit=3'
```

```json
{
  "data": {
    "field": "level",
    "values": [
      {"value": "INFO", "count": 359, "percent": 35.72},
      {"value": "WARN", "count": 347, "percent": 34.53},
      {"value": "ERROR", "count": 294, "percent": 29.25}
    ],
    "unique_count": 4,
    "total_count": 1005
  },
  "meta": {"took_ms": 0}
}
```

| Field | Description |
|---|---|
| `values[].value` | The value, as a string |
| `values[].count` | Events with this value |
| `values[].percent` | `count` as a percentage of `total_count` |
| `unique_count` | Distinct values |
| `total_count` | Events that have the field |

An unknown field returns empty `values` and zero counts. To compute the same breakdown with filters, use a query such as `source=main status>=500 | top 10 path`.

## GET /sources

Lists the event sources (the `source` label set at ingest) with their event counts and time span.

| Parameter | Description |
|---|---|
| `pattern` | Glob on the source name: `*` matches any run of characters, `?` one character |

```bash
curl -s 'localhost:3100/api/v1/sources?pattern=ng*'
```

```json
{
  "data": {
    "pattern": "ng*",
    "sources": [
      {"name": "nginx", "event_count": 2, "first_event": "2026-11-17T10:43:02Z", "last_event": "2026-11-17T10:43:02Z"}
    ]
  },
  "meta": {"took_ms": 0}
}
```

| Field | Description |
|---|---|
| `name` | Source label |
| `event_count` | Stored events from the source |
| `first_event`, `last_event` | Time of the oldest and newest event (RFC 3339) |

Indexes are listed by [`GET /api/v1/indexes`](./ingest.md#indexes). To query one index, start the query with `source=<index>`.

## Related

- [PPL catalog](./query.md#get-catalog): the commands and functions a query can use
- [Search and types in PPL](../ppl/search-and-types.md): how field types affect comparisons
- [Field extraction guide](../guides/field-extraction.md)
- [CLI overview](../cli/overview.md): `lynxdb fields`
