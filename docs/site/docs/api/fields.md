---
sidebar_position: 5
title: REST API field catalog and top values
sidebar_label: Fields
description: The field catalog and the top values of a field.
---

# Fields API

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
| `from` | | Start of the time range, inclusive, in a form that [`from` of `POST /query`](./query.md#request-body) takes: `-1h`, `-1d@d`, `@d`, `2026-11-17T08:00:00Z` |
| `since` | | Start of the time range as a span back from now (`1h`); `from` wins when both are set |
| `to` | | End of the time range, exclusive, in the same forms as `from` |

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

## Related

- [PPL catalog](./query.md#get-catalog): the commands and functions a query can use
- [Search and types in PPL](../ppl/search-and-types.md): how field types affect comparisons
- [Field extraction guide](../guides/field-extraction.md)
- [CLI overview](../cli/overview.md): `lynxdb fields`
