---
sidebar_position: 8
title: Dashboards
description: CRUD /dashboards -- panel-based dashboards with SPL2 queries, grid layout, and template variables.
---

# Dashboards API

Panel-based dashboards with SPL2 queries, 12-column grid layout, and template variables. Each panel runs an independent SPL2 query with its own time range.

## GET /dashboards

List all dashboards.

```bash
curl -s localhost:3100/api/v1/dashboards | jq .
```

**Response (200):**

```json
{
  "data": {
    "dashboards": [
      {
        "id": "dsh_abc123",
        "name": "Production Overview",
        "created_at": "2026-02-10T12:00:00Z",
        "updated_at": "2026-02-14T08:00:00Z",
        "panels_count": 6
      },
      {
        "id": "dsh_def456",
        "name": "API Performance",
        "created_at": "2026-02-11T09:00:00Z",
        "updated_at": "2026-02-13T16:30:00Z",
        "panels_count": 4
      }
    ]
  }
}
```

---

## POST /dashboards

Create a dashboard.

### Request Body

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Dashboard name |
| `panels` | array | Yes | Array of panel definitions |
| `variables` | array | No | Template variables for dynamic filtering |

### Panel Object

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `id` | string | Yes | -- | Unique panel identifier (within dashboard) |
| `title` | string | Yes | -- | Panel display title |
| `type` | string | Yes | -- | Visualization: `timechart`, `table`, `bar`, `line`, `area`, `stat`, `pie` |
| `q` | string | Yes | -- | SPL2 query |
| `from` | string | No | `"-1h"` | Time range start |
| `position` | object | Yes | -- | Grid position `{x, y, w, h}` |

### Position Object

The dashboard uses a 12-column grid layout:

| Field | Type | Description |
|---|---|---|
| `x` | integer | Column position (0--11) |
| `y` | integer | Row position (0-based) |
| `w` | integer | Width in grid units (1--12) |
| `h` | integer | Height in grid units |

### Variable Object

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Variable name (used as `$name` in queries) |
| `type` | string | Yes | `field_values` (auto-populated from field) or `custom` |
| `field` | string | Yes | Field to populate values from |
| `default` | string | No | Default value |
| `label` | string | No | Display label |

### Full Example

```bash
curl -X POST localhost:3100/api/v1/dashboards \
  -d '{
    "name": "Production Overview",
    "panels": [
      {
        "id": "p1",
        "title": "Error Rate",
        "type": "timechart",
        "q": "level=error | timechart count span=5m",
        "from": "-6h",
        "position": {"x": 0, "y": 0, "w": 6, "h": 4}
      },
      {
        "id": "p2",
        "title": "Top Error Sources",
        "type": "table",
        "q": "level=error | stats count by source | sort -count | head 10",
        "from": "-1h",
        "position": {"x": 6, "y": 0, "w": 6, "h": 4}
      },
      {
        "id": "p3",
        "title": "Total Events",
        "type": "stat",
        "q": "| stats count",
        "from": "-24h",
        "position": {"x": 0, "y": 4, "w": 3, "h": 2}
      },
      {
        "id": "p4",
        "title": "Error Percentage",
        "type": "stat",
        "q": "| stats count(eval(level=\"error\")) as errors, count as total | eval pct=round(errors/total*100, 1)",
        "from": "-1h",
        "position": {"x": 3, "y": 4, "w": 3, "h": 2}
      },
      {
        "id": "p5",
        "title": "Status Distribution",
        "type": "pie",
        "q": "source=nginx | stats count by status",
        "from": "-1h",
        "position": {"x": 6, "y": 4, "w": 6, "h": 4}
      },
      {
        "id": "p6",
        "title": "Latency by Endpoint",
        "type": "bar",
        "q": "source=api-gateway | stats avg(duration_ms) as avg_lat, p99(duration_ms) as p99_lat by endpoint | sort -avg_lat | head 10",
        "from": "-1h",
        "position": {"x": 0, "y": 6, "w": 6, "h": 4}
      }
    ],
    "variables": [
      {
        "name": "source",
        "type": "field_values",
        "field": "source",
        "default": "*",
        "label": "Source"
      }
    ]
  }'
```

**Response (201):**

```json
{
  "data": {
    "id": "dsh_abc123",
    "name": "Production Overview",
    "panels": [
      {
        "id": "p1",
        "title": "Error Rate",
        "type": "timechart",
        "q": "level=error | timechart count span=5m",
        "from": "-6h",
        "position": {"x": 0, "y": 0, "w": 6, "h": 4}
      }
    ],
    "variables": [
      {
        "name": "source",
        "type": "field_values",
        "field": "source",
        "default": "*",
        "label": "Source"
      }
    ],
    "created_at": "2026-02-14T14:52:00Z",
    "updated_at": "2026-02-14T14:52:00Z"
  }
}
```

---

## GET /dashboards/\{id\}

Get the full dashboard definition including all panels and variables.

```bash
curl -s localhost:3100/api/v1/dashboards/dsh_abc123 | jq .
```

**Response (200):**

```json
{
  "data": {
    "id": "dsh_abc123",
    "name": "Production Overview",
    "panels": [
      {
        "id": "p1",
        "title": "Error Rate",
        "type": "timechart",
        "q": "level=error | timechart count span=5m",
        "from": "-6h",
        "position": {"x": 0, "y": 0, "w": 6, "h": 4}
      },
      {
        "id": "p2",
        "title": "Top Error Sources",
        "type": "table",
        "q": "level=error | stats count by source | sort -count | head 10",
        "from": "-1h",
        "position": {"x": 6, "y": 0, "w": 6, "h": 4}
      }
    ],
    "variables": [
      {
        "name": "source",
        "type": "field_values",
        "field": "source",
        "default": "*",
        "label": "Source"
      }
    ],
    "created_at": "2026-02-10T12:00:00Z",
    "updated_at": "2026-02-14T08:00:00Z"
  }
}
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Dashboard not found |

---

## PUT /dashboards/\{id\}

Replace a dashboard definition. This is a full replacement -- include all panels and variables.

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `id` | Yes | Dashboard ID |

```bash
curl -X PUT localhost:3100/api/v1/dashboards/dsh_abc123 \
  -d '{
    "name": "Production Overview (v2)",
    "panels": [
      {
        "id": "p1",
        "title": "Error Rate (5m buckets)",
        "type": "timechart",
        "q": "level=error | timechart count span=5m",
        "from": "-12h",
        "position": {"x": 0, "y": 0, "w": 12, "h": 4}
      },
      {
        "id": "p2",
        "title": "Top Error Sources",
        "type": "table",
        "q": "level=error | stats count by source | sort -count | head 20",
        "from": "-1h",
        "position": {"x": 0, "y": 4, "w": 6, "h": 4}
      },
      {
        "id": "p3",
        "title": "Status Codes",
        "type": "pie",
        "q": "source=nginx | stats count by status",
        "from": "-1h",
        "position": {"x": 6, "y": 4, "w": 6, "h": 4}
      }
    ],
    "variables": [
      {
        "name": "source",
        "type": "field_values",
        "field": "source",
        "default": "*",
        "label": "Source"
      }
    ]
  }'
```

**Response (200):** Updated dashboard object.

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Dashboard not found |

---

## DELETE /dashboards/\{id\}

Delete a dashboard.

```bash
curl -X DELETE localhost:3100/api/v1/dashboards/dsh_abc123
```

**Response:** `204 No Content`

---

## Panel Types

| Type | Description | Best For |
|---|---|---|
| `timechart` | Time-series line chart | Trends over time (`timechart` command) |
| `table` | Tabular data | `stats`, `top`, `table` results |
| `bar` | Bar chart | Comparing values across categories |
| `line` | Line chart | Non-time numeric series |
| `area` | Area chart | Stacked or filled time series |
| `stat` | Single number | KPIs, counters, percentages |
| `pie` | Pie/donut chart | Distribution of values |

## Template Variables

Template variables enable dynamic filtering across all panels. When a user selects a value from the variable dropdown, panels re-execute their queries with the variable value substituted.

Reference variables in panel queries with `$variable_name`:

```json
{
  "panels": [
    {
      "id": "p1",
      "title": "Error Rate by Source",
      "type": "timechart",
      "q": "source=$source level=error | timechart count span=5m",
      "from": "-6h",
      "position": {"x": 0, "y": 0, "w": 12, "h": 4}
    }
  ],
  "variables": [
    {
      "name": "source",
      "type": "field_values",
      "field": "source",
      "default": "*",
      "label": "Source"
    }
  ]
}
```

Variable types:
- **`field_values`** -- auto-populated from the `GET /fields/{name}/values` endpoint
- **`custom`** -- manually defined list of values

## Related

- **[Dashboards guide](/docs/guides/dashboards)** -- building effective dashboards
- **[Query API](/docs/api/query)** -- the query engine behind each panel
- **[SPL2 `timechart` command](/docs/lynx-flow/commands/timechart)** -- time-series queries
- **[Fields API](/docs/api/fields)** -- powers template variable dropdowns
