---
sidebar_position: 6
title: Saved Queries
description: CRUD operations for persisting and reusing SPL2 queries via the REST API.
---

# Saved Queries API

Persist and reuse SPL2 queries. Saved queries can be referenced from dashboards, shared across teams, and serve as a library of common searches.

## GET /queries

List all saved queries.

```bash
curl -s localhost:3100/api/v1/queries | jq .
```

**Response (200):**

```json
{
  "data": {
    "queries": [
      {
        "id": "sq_abc123",
        "name": "High 5xx rate",
        "q": "source=nginx status>=500 | stats count by uri | sort -count",
        "from": "-1h",
        "created_at": "2026-02-10T12:00:00Z",
        "updated_at": "2026-02-14T08:00:00Z"
      },
      {
        "id": "sq_def456",
        "name": "Slow API calls",
        "q": "source=api-gateway duration_ms>5000 | stats count, avg(duration_ms) by endpoint",
        "from": "-30m",
        "created_at": "2026-02-12T09:30:00Z",
        "updated_at": "2026-02-12T09:30:00Z"
      }
    ]
  }
}
```

### Saved Query Object

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique identifier (prefixed `sq_`) |
| `name` | string | Human-readable name |
| `q` | string | SPL2 query string |
| `from` | string | Default time range start (relative or ISO 8601) |
| `created_at` | string | Creation timestamp (ISO 8601) |
| `updated_at` | string | Last modification timestamp (ISO 8601) |

---

## POST /queries

Create a saved query.

### Request Body

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Human-readable name |
| `q` | string | Yes | SPL2 query string |
| `from` | string | No | Default time range start |

```bash
curl -X POST localhost:3100/api/v1/queries \
  -d '{
    "name": "High 5xx rate",
    "q": "source=nginx status>=500 | stats count by uri | sort -count",
    "from": "-1h"
  }'
```

**Response (201):**

```json
{
  "data": {
    "id": "sq_abc123",
    "name": "High 5xx rate",
    "q": "source=nginx status>=500 | stats count by uri | sort -count",
    "from": "-1h",
    "created_at": "2026-02-14T14:52:00Z",
    "updated_at": "2026-02-14T14:52:00Z"
  }
}
```

---

## PUT /queries/\{id\}

Replace a saved query definition.

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `id` | Yes | Saved query ID |

```bash
curl -X PUT localhost:3100/api/v1/queries/sq_abc123 \
  -d '{
    "name": "High 5xx rate (updated)",
    "q": "source=nginx status>=500 | stats count, avg(duration_ms) by uri | sort -count | head 20",
    "from": "-2h"
  }'
```

**Response (200):**

```json
{
  "data": {
    "id": "sq_abc123",
    "name": "High 5xx rate (updated)",
    "q": "source=nginx status>=500 | stats count, avg(duration_ms) by uri | sort -count | head 20",
    "from": "-2h",
    "created_at": "2026-02-10T12:00:00Z",
    "updated_at": "2026-02-14T15:00:00Z"
  }
}
```

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Saved query not found |

---

## DELETE /queries/\{id\}

Delete a saved query.

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `id` | Yes | Saved query ID |

```bash
curl -X DELETE localhost:3100/api/v1/queries/sq_abc123
```

**Response:** `204 No Content`

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Saved query not found |

---

## Using Saved Queries

Saved queries are referenced by ID or name in the Web UI. You can also execute them via the query API:

```bash
# Look up the query
QUERY=$(curl -s localhost:3100/api/v1/queries/sq_abc123 | jq -r '.data.q')

# Execute it
curl -s localhost:3100/api/v1/query \
  -d "{\"q\": \"$QUERY\", \"from\": \"-1h\"}" | jq .
```

## Related

- **[Saved Queries guide](/docs/guides/saved-queries)** -- patterns for organizing and sharing queries
- **[Query API](/docs/api/query)** -- executing SPL2 queries
- **[Dashboards API](/docs/api/dashboards)** -- using queries in dashboard panels
- **[SPL2 Overview](/docs/spl2/overview)** -- query language reference
