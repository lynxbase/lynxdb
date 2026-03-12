---
sidebar_position: 7
title: Alerts
description: CRUD /alerts with multi-channel notifications -- Slack, PagerDuty, Telegram, OpsGenie, email, webhook, and more.
---

# Alerts API

SPL2-powered threshold alerting with multi-channel notifications. Each alert runs an SPL2 query on a schedule and fires notifications when the condition is met.

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
        "q": "level=error | stats count as errors | where errors > 100",
        "interval": "5m",
        "channels": [
          {
            "type": "webhook",
            "name": "Slack Ops",
            "config": {
              "url": "https://hooks.slack.com/services/T00/B00/xxx"
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

### Alert Object

| Field | Type | Description |
|---|---|---|
| `id` | string | Unique identifier (prefixed `alt_`) |
| `name` | string | Human-readable name |
| `q` | string | SPL2 query (must produce a numeric result to evaluate as condition) |
| `interval` | string | Check frequency: `30s`, `1m`, `5m`, `15m`, `1h` |
| `channels` | array | Notification destinations (see channel types below) |
| `enabled` | boolean | Whether the alert is active |
| `last_triggered` | string | Last time the condition was met (ISO 8601, nullable) |
| `last_checked` | string | Last evaluation time (ISO 8601, nullable) |
| `status` | string | `ok` (last check didn't fire), `triggered` (last check fired), `error` (query failed) |

---

## POST /alerts

Create an alert with multi-channel notifications.

### Request Body

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | string | Yes | Human-readable name |
| `q` | string | Yes | SPL2 query (must produce a numeric result) |
| `interval` | string | Yes | Check frequency |
| `channels` | array | Yes | At least one notification channel |
| `enabled` | boolean | No | Default: `true` |

### Multi-Channel Example (Webhook + Telegram + PagerDuty)

```bash
curl -X POST localhost:3100/api/v1/alerts \
  -d '{
    "name": "High error rate",
    "q": "level=error | stats count as errors | where errors > 100",
    "interval": "5m",
    "channels": [
      {
        "type": "webhook",
        "name": "Slack Ops",
        "config": {
          "url": "https://hooks.slack.com/services/T00/B00/xxx"
        }
      },
      {
        "type": "telegram",
        "name": "SRE Chat",
        "config": {
          "bot_token": "TOKEN",
          "chat_id": "CHANNEL",
          "message_template": "{{.alert.name}}: {{.result.errors}} errors in last {{.alert.interval}}"
        }
      },
      {
        "type": "pagerduty",
        "name": "P1 Escalation",
        "config": {
          "routing_key": "R012ABCDEF...",
          "severity": "critical"
        }
      }
    ]
  }'
```

**Response (201):**

```json
{
  "data": {
    "id": "alt_xyz789",
    "name": "High error rate",
    "q": "level=error | stats count as errors | where errors > 100",
    "interval": "5m",
    "channels": [
      {"type": "webhook", "name": "Slack Ops", "enabled": true, "config": {"url": "https://hooks.slack.com/services/T00/B00/xxx"}},
      {"type": "telegram", "name": "SRE Chat", "enabled": true, "config": {"bot_token": "110201543:AAH...", "chat_id": "-1001234567890"}},
      {"type": "pagerduty", "name": "P1 Escalation", "enabled": true, "config": {"routing_key": "R012ABC...", "severity": "critical"}}
    ],
    "enabled": true,
    "last_triggered": null,
    "last_checked": null,
    "status": "ok"
  }
}
```

### Simple Webhook Alert

```bash
curl -X POST localhost:3100/api/v1/alerts \
  -d '{
    "name": "5xx spike",
    "q": "source=nginx status>=500 | stats count as cnt | where cnt > 50",
    "interval": "1m",
    "channels": [
      {
        "type": "webhook",
        "config": {
          "url": "https://hooks.slack.com/services/T00/B00/xxx"
        }
      }
    ]
  }'
```

### Slack Alert

```bash
curl -X POST localhost:3100/api/v1/alerts \
  -d '{
    "name": "Database connection failures",
    "q": "source=api-gateway message=\"connection refused\" | stats count as failures | where failures > 10",
    "interval": "2m",
    "channels": [
      {
        "type": "slack",
        "name": "DB Alerts",
        "config": {
          "webhook_url": "https://hooks.slack.com/services/T00/B00/yyy",
          "channel": "#db-alerts",
          "username": "LynxDB",
          "icon_emoji": ":rotating_light:"
        }
      }
    ]
  }'
```

### Incident.io + Slack Alert

```bash
curl -X POST localhost:3100/api/v1/alerts \
  -d '{
    "name": "Database connection failures",
    "q": "source=api-gateway message=\"connection refused\" | stats count as failures | where failures > 10",
    "interval": "2m",
    "channels": [
      {
        "type": "incidentio",
        "name": "DB Incident",
        "config": {
          "api_key": "inc_live_xxxx",
          "severity": "major",
          "mode": "real"
        }
      },
      {
        "type": "slack",
        "name": "DB Alerts",
        "config": {
          "webhook_url": "https://hooks.slack.com/services/T00/B00/yyy",
          "channel": "#db-alerts"
        }
      }
    ]
  }'
```

### Validation Errors (422)

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "channels[1].config.bot_token is required for type 'telegram'"
  }
}
```

---

## Notification Channel Types

Each alert can send to multiple channels simultaneously. Each channel has an independent `enabled` flag so you can mute a channel without removing it.

### webhook

Generic webhook with configurable URL, method, headers, and body template.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `url` | Yes | -- | Webhook URL |
| `method` | No | `POST` | HTTP method (`POST` or `PUT`) |
| `headers` | No | -- | Custom HTTP headers (key-value object) |
| `body_template` | No | -- | Go template for body. Variables: `{{.alert.*}}`, `{{.result.*}}`, `{{.timestamp}}` |

### slack

Slack incoming webhook.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `webhook_url` | Yes | -- | Slack incoming webhook URL |
| `channel` | No | -- | Override channel (e.g., `#alerts`) |
| `username` | No | `LynxDB` | Bot username |
| `icon_emoji` | No | `:rotating_light:` | Bot icon |

### telegram

Telegram bot message.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `bot_token` | Yes | -- | Telegram Bot API token |
| `chat_id` | Yes | -- | Chat or group ID (prefix group IDs with `-100`) |
| `message_template` | No | Default template | Go template for message body |
| `parse_mode` | No | `HTML` | Message format: `HTML` or `MarkdownV2` |

### pagerduty

PagerDuty Events API v2.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `routing_key` | Yes | -- | Events API v2 routing key |
| `severity` | No | `error` | Incident severity: `critical`, `error`, `warning`, `info` |

### opsgenie

Opsgenie alert.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `api_key` | Yes | -- | Opsgenie API key |
| `priority` | No | `P3` | Alert priority: `P1`--`P5` |
| `tags` | No | -- | Array of tags |

### email

Email via SMTP.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `to` | Yes | -- | Array of recipient email addresses |
| `from` | Yes | -- | Sender email address |
| `smtp_host` | No | `localhost` | SMTP server host |
| `smtp_port` | No | `587` | SMTP server port |

### incidentio

Incident.io integration.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `api_key` | Yes | -- | incident.io API key |
| `severity` | No | `major` | Incident severity: `minor`, `major`, `critical` |
| `mode` | No | `real` | `real` creates real incidents, `test` creates test incidents that don't page |

### generic_http

Generic HTTP integration for any webhook-based service.

| Config Field | Required | Default | Description |
|---|---|---|---|
| `url` | Yes | -- | Target URL |
| `method` | Yes | -- | HTTP method: `POST`, `PUT`, `PATCH` |
| `headers` | No | -- | Custom HTTP headers |
| `body_template` | No | -- | Go template for request body |

---

## PUT /alerts/\{id\}

Replace an alert definition.

### Path Parameters

| Parameter | Required | Description |
|---|---|---|
| `id` | Yes | Alert ID |

```bash
curl -X PUT localhost:3100/api/v1/alerts/alt_xyz789 \
  -d '{
    "name": "High error rate (updated threshold)",
    "q": "level=error | stats count as errors | where errors > 200",
    "interval": "5m",
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

**Response (200):** Updated alert object.

### Error Responses

| Status | Code | Description |
|---|---|---|
| `404` | `NOT_FOUND` | Alert not found |

---

## DELETE /alerts/\{id\}

Delete an alert.

```bash
curl -X DELETE localhost:3100/api/v1/alerts/alt_xyz789
```

**Response:** `204 No Content`

---

## POST /alerts/\{id\}/test

Test an alert without sending notifications. Executes the alert query and evaluates the condition, but does not send any notifications. Returns the query result and which channels would have fired.

```bash
curl -X POST localhost:3100/api/v1/alerts/alt_xyz789/test | jq .
```

**Response (200):**

```json
{
  "data": {
    "would_trigger": true,
    "result": {
      "errors": 247
    },
    "channels_that_would_fire": [
      {"type": "webhook", "name": "Slack Ops", "status": "reachable"},
      {"type": "telegram", "name": "SRE Chat", "status": "reachable"},
      {"type": "pagerduty", "name": "P1 Escalation", "status": "reachable"}
    ],
    "message": "Condition met: errors (247) > 100. Notifications NOT sent (test mode)."
  }
}
```

---

## POST /alerts/\{id\}/test-channels

Send a test notification (clearly marked as `[TEST]`) to every enabled channel on an alert. Use to verify connectivity before going live.

```bash
curl -X POST localhost:3100/api/v1/alerts/alt_xyz789/test-channels | jq .
```

**Response (200):**

```json
{
  "data": {
    "results": [
      {"type": "webhook", "name": "Slack Ops", "status": "ok", "latency_ms": 142},
      {"type": "telegram", "name": "SRE Chat", "status": "ok", "latency_ms": 310},
      {"type": "pagerduty", "name": "P1 Escalation", "status": "error", "error": "HTTP 403: invalid routing_key"}
    ]
  }
}
```

## Related

- **[`lynxdb alerts` CLI command](/docs/cli/alerts)** -- manage alerts from the command line
- **[Alerts guide](/docs/guides/alerts)** -- end-to-end alerting walkthrough
- **[Query API](/docs/api/query)** -- the query engine that powers alert evaluation
- **[SPL2 `stats` command](/docs/lynx-flow/commands/stats)** -- aggregations used in alert queries
