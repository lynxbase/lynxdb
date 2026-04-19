---
sidebar_position: 7
title: Alerts
description: CRUD /alerts endpoints, alert payloads, test endpoints, and the currently implemented notification channel backends.
---

# Alerts API

SPL2-powered threshold alerting. Each alert runs a query on a schedule and fires notifications when the query returns one or more rows.

:::note Current implementation
The built-in notifier registry and alert validation currently support exactly three channel types: `webhook`, `slack`, and `telegram`.
:::

## Alert Object

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique identifier, prefixed `alt_` |
| `name` | string | Human-readable name |
| `query` | string | SPL2 query run at each interval |
| `interval` | string | Go duration such as `30s`, `1m`, `5m`, `1h` |
| `channels` | array | Notification destinations |
| `enabled` | boolean | Whether the alert is active |
| `last_triggered` | string | Last trigger time, nullable |
| `last_checked` | string | Last evaluation time, nullable |
| `status` | string | `ok`, `triggered`, or `error` |

`interval` must parse as a Go duration string and must be at least `10s`.

## GET /alerts

List all alerts.

```bash
curl -s localhost:3100/api/v1/alerts | jq .
```

**Response (200):**

```json
{
  "data": {
    "alerts": [
      {
        "id": "alt_xyz789",
        "name": "High error rate",
        "query": "level=error | stats count as errors | where errors > 100",
        "interval": "5m",
        "channels": [
          {
            "type": "slack",
            "name": "Slack Ops",
            "config": {
              "webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"
            }
          }
        ],
        "enabled": true,
        "last_triggered": "2026-02-14T13:25:00Z",
        "last_checked": "2026-02-14T14:50:00Z",
        "status": "ok"
      }
    ]
  }
}
```

## `GET /alerts/{id}`

Fetch a single stored alert definition.

```bash
curl -s localhost:3100/api/v1/alerts/alt_xyz789 | jq .
```

## POST /alerts

Create an alert.

### Request Body

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Human-readable name |
| `query` | string | Yes | SPL2 query run at each interval |
| `interval` | string | Yes | Evaluation interval |
| `channels` | array | Yes | At least one notification channel |
| `enabled` | boolean | No | Default: `true` |

### Example

```bash
curl -X POST localhost:3100/api/v1/alerts \
  -d '{
    "name": "High error rate",
    "query": "level=error | stats count as errors | where errors > 100",
    "interval": "5m",
    "channels": [
      {
        "type": "slack",
        "name": "Slack Ops",
        "config": {
          "webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"
        }
      },
      {
        "type": "telegram",
        "name": "SRE Chat",
        "config": {
          "bot_token": "TOKEN",
          "chat_id": "CHANNEL"
        }
      }
    ]
  }'
```

**Response (201):** the created alert object inside the standard `data` envelope.

### Validation Errors

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "channels[0].config.bot_token is required for type 'telegram'"
  }
}
```

## Notification Channels

Each alert can send to multiple channels simultaneously. Each channel has an independent `enabled` flag so you can mute it without removing it.

### `webhook`

| Config Field | Required | Default | Description |
|---|---|---|---|
| `url` | Yes | -- | Target URL |
| `method` | No | `POST` | HTTP method (`POST` or `PUT`) |
| `headers` | No | -- | Custom HTTP headers |
| `body_template` | No | -- | Go template for request body |

### `slack`

| Config Field | Required | Default | Description |
|---|---|---|---|
| `webhook_url` | Yes | -- | Slack incoming webhook URL |
| `channel` | No | -- | Override channel |
| `username` | No | -- | Override username |
| `icon_emoji` | No | -- | Override icon |

### `telegram`

| Config Field | Required | Default | Description |
|---|---|---|---|
| `bot_token` | Yes | -- | Telegram Bot API token |
| `chat_id` | Yes | -- | Chat or group ID |
| `message_template` | No | built-in message | Go template for the message body |
| `parse_mode` | No | server default | Telegram parse mode |

## `PUT /alerts/{id}`

Replace an alert definition. Send the full object you want stored.

```bash
curl -X PUT localhost:3100/api/v1/alerts/alt_xyz789 \
  -d '{
    "name": "High error rate (updated threshold)",
    "query": "level=error | stats count as errors | where errors > 200",
    "interval": "5m",
    "enabled": false,
    "channels": [
      {
        "type": "slack",
        "config": {
          "webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"
        }
      }
    ]
  }'
```

**Response (200):** the updated alert object.

## `PATCH /alerts/{id}`

Patch an alert's enabled state without resending the full definition.

```bash
curl -X PATCH localhost:3100/api/v1/alerts/alt_xyz789 \
  -d '{"enabled": false}'
```

**Response (200):** the updated alert object.

## `DELETE /alerts/{id}`

Delete an alert.

```bash
curl -X DELETE localhost:3100/api/v1/alerts/alt_xyz789
```

**Response:** `204 No Content`

## `POST /alerts/{id}/test`

Run the alert query without sending notifications.

```bash
curl -X POST localhost:3100/api/v1/alerts/alt_xyz789/test | jq .
```

**Response (200):**

```json
{
  "data": {
    "would_trigger": true,
    "result": {
      "rows": [
        {"errors": 247}
      ],
      "count": 1
    },
    "channels_that_would_fire": [
      "Slack Ops",
      "SRE Chat"
    ],
    "message": "Alert \"High error rate\" would trigger — query returned 1 rows"
  }
}
```

## `POST /alerts/{id}/test-channels`

Send a test notification to every enabled channel on the alert.

```bash
curl -X POST localhost:3100/api/v1/alerts/alt_xyz789/test-channels | jq .
```

**Response (200):**

```json
{
  "data": {
    "results": [
      {"type": "slack", "name": "Slack Ops", "status": "ok", "latency_ms": 142},
      {"type": "telegram", "name": "SRE Chat", "status": "ok", "latency_ms": 310}
    ]
  }
}
```

## Related

- [Alerts guide](/docs/guides/alerts) for end-to-end usage
- [CLI: `alerts`](/docs/cli/alerts) for current CLI coverage
- [Query API](/docs/api/query) for the query engine behind alert evaluation
