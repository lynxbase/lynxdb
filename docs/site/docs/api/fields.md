---
sidebar_position: 5
title: Fields & Sources
description: GET /fields, /fields/\{name\}/values, and /sources -- field catalog, value stats, and log source discovery.
---

# Fields & Sources API

Automatic field discovery with types, coverage stats, and top values. These endpoints power the Fields sidebar, autocomplete, and Quick Stats in the Web UI.

## GET /fields

List all discovered fields with types, coverage statistics, and top values.

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `from` | No | -- | Restrict stats to time range start |
| `to` | No | -- | Restrict stats to time range end |
| `prefix` | No | -- | Filter fields by name prefix (for autocomplete) |
| `source` | No | -- | Only fields seen from a specific source |

### List All Fields

```bash
curl -s localhost:3100/api/v1/fields | jq .
```

**Response (200):**

```json
{
  "data": {
    "fields": [
      {
        "name": "_timestamp",
        "type": "datetime",
        "count": 847000000,
        "coverage": 1.0
      },
      {
        "name": "level",
        "type": "string",
        "count": 847000000,
        "coverage": 1.0,
        "top_values": [
          {"value": "info", "count": 612000000},
          {"value": "error", "count": 142000000},
          {"value": "warn", "count": 93000000}
        ]
      },
      {
        "name": "status",
        "type": "integer",
        "count": 423000000,
        "coverage": 0.50,
        "min": 200,
        "max": 504,
        "top_values": [
          {"value": 200, "count": 380000000},
          {"value": 404, "count": 21000000}
        ]
      },
      {
        "name": "duration_ms",
        "type": "float",
        "count": 423000000,
        "coverage": 0.50,
        "min": 0.1,
        "max": 30001.0,
        "avg": 145.3,
        "p50": 42.0,
        "p99": 3200.0
      }
    ]
  }
}
```

### Field Object Properties

| Field | Type | Description |
|---|---|---|
| `name` | string | Field name |
| `type` | string | Detected type: `string`, `integer`, `float`, `boolean`, `datetime` |
| `count` | integer | Total events containing this field |
| `coverage` | number | Fraction of all events containing this field (0.0--1.0) |
| `min` | number | Minimum value (numeric/datetime fields only) |
| `max` | number | Maximum value (numeric/datetime fields only) |
| `avg` | number | Average value (numeric fields only) |
| `p50` | number | 50th percentile (numeric fields only) |
| `p99` | number | 99th percentile (numeric fields only) |
| `top_values` | array | Most common values with counts (string/integer fields) |

### Filter by Prefix (Autocomplete)

```bash
curl -s "localhost:3100/api/v1/fields?prefix=sta" | jq .
```

Returns only fields whose names start with `sta` (e.g., `status`, `state`, `start_time`).

### Filter by Source

```bash
curl -s "localhost:3100/api/v1/fields?source=nginx" | jq .
```

Returns only fields seen in events tagged with `source=nginx`.

### Scoped to Time Range

```bash
curl -s "localhost:3100/api/v1/fields?from=-1h&to=now" | jq .
```

---

## GET /fields/\{name\}/values

Top values for a specific field. Powers the "Quick Stats" sidebar and value autocomplete in the search bar.

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `name` | Yes | Field name |

### Query Parameters

| Parameter | Required | Default | Description |
|---|---|---|---|
| `from` | No | -- | Restrict to time range start |
| `to` | No | -- | Restrict to time range end |
| `limit` | No | `10` | Number of top values to return (max 100) |

### Example

```bash
curl -s "localhost:3100/api/v1/fields/status/values?from=-1h&limit=10" | jq .
```

**Response (200):**

```json
{
  "data": {
    "field": "status",
    "type": "integer",
    "values": [
      {"value": 200, "count": 89421, "percent": 72.3},
      {"value": 404, "count": 18234, "percent": 14.7},
      {"value": 500, "count": 9123, "percent": 7.4},
      {"value": 502, "count": 4521, "percent": 3.7},
      {"value": 504, "count": 2401, "percent": 1.9}
    ],
    "unique_count": 14,
    "total_count": 123700
  },
  "meta": {
    "from": "2026-02-14T13:52:00Z",
    "to": "2026-02-14T14:52:00Z"
  }
}
```

### Value Object Properties

| Field | Type | Description |
|---|---|---|
| `value` | any | The field value |
| `count` | integer | Number of events with this value |
| `percent` | number | Percentage of total events for this field |

### Response Metadata

| Field | Type | Description |
|---|---|---|
| `data.field` | string | Field name |
| `data.type` | string | Detected field type |
| `data.unique_count` | integer | Total number of distinct values |
| `data.total_count` | integer | Total events containing this field |
| `meta.from` | string | Effective time range start (ISO 8601) |
| `meta.to` | string | Effective time range end (ISO 8601) |

---

## GET /sources

List all log sources with volume statistics.

```bash
curl -s localhost:3100/api/v1/sources | jq .
```

**Response (200):**

```json
{
  "data": {
    "sources": [
      {
        "name": "nginx",
        "event_count": 423000000,
        "last_event": "2026-02-14T14:52:01Z",
        "first_event": "2026-01-15T00:00:01Z",
        "rate": 1200.5,
        "storage_bytes": 5200000000
      },
      {
        "name": "api-gateway",
        "event_count": 312000000,
        "last_event": "2026-02-14T14:52:00Z",
        "first_event": "2026-01-15T00:00:02Z",
        "rate": 890.2,
        "storage_bytes": 4100000000
      }
    ]
  }
}
```

### Source Object Properties

| Field | Type | Description |
|---|---|---|
| `name` | string | Source name (set via `X-Source` header or `source` field) |
| `event_count` | integer | Total events from this source |
| `last_event` | string | Timestamp of most recent event (ISO 8601) |
| `first_event` | string | Timestamp of earliest event (ISO 8601) |
| `rate` | number | Current ingest rate (events per second) |
| `storage_bytes` | integer | Storage used by this source |

## Related

- **[`lynxdb fields` CLI command](/docs/cli/shortcuts)** -- field catalog from the command line
- **[Schema-on-Read](/docs/architecture/storage-engine)** -- how fields are discovered automatically
- **[Field Extraction guide](/docs/guides/field-extraction)** -- extracting fields at query time with `rex` and `eval`
- **[Search Syntax](/docs/lynx-flow/search-syntax)** -- using field values in queries
