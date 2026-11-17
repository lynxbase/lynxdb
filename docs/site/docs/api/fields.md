---
sidebar_position: 5
title: Fields & Sources
description: The field catalog, the top values of a field, and the indexes with their event counts.
---

# Fields & Sources API

These endpoints describe stored data for field lists and completion. Any valid key can call them when authentication is on. Like a query, each one first writes the events ingested so far.

## GET /fields

Lists the fields the indexes hold, each with the indexes that hold it, sorted by name.

| Parameter | Description |
|---|---|
| `index` (or `source`) | Only this index |
| `prefix` | Only fields whose name starts with this prefix |

```bash
curl -s 'localhost:3100/api/v1/fields?prefix=st'
```

```json
{"data": {"fields": [{"name": "status", "indexes": ["backend", "main"]}]}}
```

| Field | Description |
|---|---|
| `name` | Field name. The members of a nested object are listed by their path, such as `order.id` |
| `indexes` | The indexes that hold the field, sorted by name |

The list includes `_time`, and `_raw` and the ingest labels (`source`, `sourcetype`, `host`) where events carry them. An unknown index gives an empty list.

## GET /fields/\{name\}/values

Returns the most frequent values of one field among the events that hold it, with their counts, the most frequent first.

| Parameter | Default | Description |
|---|---|---|
| `limit` | `10` | Values to return, at most 100 |
| `index` | all indexes | Only this index |
| `from` | | Start of the time range: relative (`-1h`) or absolute |
| `since` | | Start of the time range as a span back from now (`1h`); `from` wins when both are set |
| `to` | | End of the time range |

```bash
curl -s 'localhost:3100/api/v1/fields/level/values?limit=3'
```

```json
{
  "data": {
    "field": "level",
    "values": [
      {"value": "INFO", "count": 374},
      {"value": "WARN", "count": 353},
      {"value": "ERROR", "count": 299}
    ]
  },
  "meta": {"took_ms": 0.314}
}
```

Each `value` keeps its type, so a numeric field returns numbers. An unknown field returns an empty `values` list. A field name or `index` that holds a backtick or a line break, or an unparsable time, answers `400 VALIDATION_ERROR`.

To compute the same breakdown with filters, run a query such as `source=main status>=500 | top 10 path`.

## GET /sources

Lists the indexes with their event counts and the times of their earliest and latest events.

| Parameter | Description |
|---|---|
| `pattern` | Glob on the index name: `*` matches any run of characters, `?` one character |

```bash
curl -s 'localhost:3100/api/v1/sources?pattern=ng*'
```

```json
{
  "data": {
    "pattern": "ng*",
    "sources": [
      {"name": "nginx", "event_count": 34, "first_event": "2026-11-17T15:43:40Z", "last_event": "2026-11-17T15:43:40Z"}
    ]
  },
  "meta": {"took_ms": 0.009}
}
```

| Field | Description |
|---|---|
| `name` | Index name |
| `event_count` | Events stored in the index |
| `first_event`, `last_event` | Times of the earliest and latest event (RFC 3339, whole seconds); absent for an index without stored events |

[`GET /api/v1/indexes`](./ingest.md#get-indexes) lists the same indexes with their sizes. To query one, start the query with `source=<index>`.

## Related

- [PPL catalog](./query.md#get-catalog): the commands and functions a query can use
- [Search and types in PPL](../ppl/search-and-types.md): how field types affect comparisons
- [Field extraction guide](../guides/field-extraction.md)
- [CLI overview](../cli/overview.md): `lynxdb fields`
