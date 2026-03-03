---
title: Set Up Alerts
description: How to create, configure, test, and manage SPL2-powered alerts with multi-channel notifications in LynxDB.
---

# Set Up Alerts

LynxDB alerts evaluate an SPL2 query on a schedule and send notifications when the query returns results. Use alerts to detect error spikes, latency anomalies, missing heartbeats, or any condition you can express in SPL2.

## How alerts work

1. You define an SPL2 query that returns results when a problem is detected.
2. LynxDB evaluates the query at a configurable interval (for example, every 5 minutes).
3. If the query returns one or more results, LynxDB sends notifications to all configured channels.

The alert query should produce results only when the alert condition is met. Typically this means aggregating events and filtering with `WHERE`.

---

## Create an alert via the CLI

Use [`lynxdb alerts create`](/docs/cli/alerts):

```bash
lynxdb alerts create \
  --name "High error rate" \
  --query 'level=error | stats count AS errors | where errors > 100' \
  --interval 5m
```

This checks every 5 minutes whether the error count exceeds 100 in the evaluation window.

---

## Create an alert via the REST API

Use [`POST /api/v1/alerts`](/docs/api/alerts):

```bash
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "High error rate",
  "q": "level=error | stats count as errors | where errors > 100",
  "interval": "5m",
  "channels": [
    {"type": "slack", "config": {"webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"}}
  ]
}'
```

---

## Configure notification channels

Alerts support 8 notification channel types. Each alert can have multiple channels.

### Slack

```bash
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "5xx spike",
  "q": "source=nginx status>=500 | stats count AS errors | where errors > 50",
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

### Telegram

```json
{
  "type": "telegram",
  "config": {
    "bot_token": "123456:ABC-DEF...",
    "chat_id": "-1001234567890"
  }
}
```

### PagerDuty

```json
{
  "type": "pagerduty",
  "config": {
    "routing_key": "your-integration-key",
    "severity": "critical"
  }
}
```

### OpsGenie

```json
{
  "type": "opsgenie",
  "config": {
    "api_key": "your-opsgenie-api-key",
    "priority": "P1"
  }
}
```

### Email

```json
{
  "type": "email",
  "config": {
    "to": "oncall@company.com",
    "subject": "LynxDB Alert: {{.Name}}"
  }
}
```

### Webhook (generic HTTP)

```json
{
  "type": "webhook",
  "config": {
    "url": "https://your-service.com/webhook",
    "method": "POST"
  }
}
```

### incident.io

```json
{
  "type": "incidentio",
  "config": {
    "api_key": "your-incidentio-key"
  }
}
```

### Multiple channels on one alert

Combine channels for redundancy:

```bash
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "Critical error rate",
  "q": "level=error | stats count as errors | where errors > 500",
  "interval": "5m",
  "channels": [
    {"type": "slack", "config": {"webhook_url": "https://hooks.slack.com/..."}},
    {"type": "pagerduty", "config": {"routing_key": "...", "severity": "critical"}},
    {"type": "email", "config": {"to": "oncall@company.com"}}
  ]
}'
```

### CLI shorthand for channels

The CLI supports a shorthand syntax for adding channels:

```bash
lynxdb alerts create \
  --name "Error rate spike" \
  --query 'level=error | stats count as errors | where errors > 100' \
  --interval 5m \
  --channel slack:webhook_url=https://hooks.slack.com/... \
  --channel pagerduty:routing_key=...,severity=critical
```

---

## Test an alert

### Dry-run the query

Test alert evaluation without sending any notifications:

```bash
lynxdb alerts test <alert_id>
```

This runs the alert query and shows what results it would produce. Use this to verify your query logic before enabling the alert.

### Test notification channels

Send a test notification to all configured channels to verify connectivity:

```bash
lynxdb alerts test-channels <alert_id>
```

---

## Manage alerts

### List all alerts

```bash
lynxdb alerts
```

Or via the API:

```bash
curl -s localhost:3100/api/v1/alerts | jq .
```

### View alert details

```bash
lynxdb alerts <alert_id>
```

### Enable and disable

```bash
lynxdb alerts enable <alert_id>
lynxdb alerts disable <alert_id>
```

### Delete an alert

```bash
lynxdb alerts delete <alert_id>
lynxdb alerts delete <alert_id> --force   # skip confirmation
```

---

## Alert query patterns

### Error rate above threshold

```spl
level=error | stats count AS errors | where errors > 100
```

### Error rate as a percentage

```spl
source=nginx
  | stats count AS total, count(eval(status>=500)) AS errors
  | eval error_pct = round(errors/total*100, 1)
  | where error_pct > 5
```

### Latency spike

```spl
source=nginx
  | stats perc99(duration_ms) AS p99
  | where p99 > 2000
```

### Missing heartbeat (no events from a source)

```spl
source=health-check
  | stats count
  | where count = 0
```

### High error rate per service

```spl
level=error
  | stats count AS errors by service
  | where errors > 50
```

### Security: failed login brute force

```spl
source=auth type="login_failed"
  | stats count AS failures by src_ip
  | where failures > 20
```

---

## Next steps

- [Create dashboards](/docs/guides/dashboards) -- visualize the same queries your alerts monitor
- [Materialized views](/docs/guides/materialized-views) -- speed up alert evaluation with precomputed aggregations
- [REST API: Alerts](/docs/api/alerts) -- full API reference for alert CRUD
- [CLI: `alerts`](/docs/cli/alerts) -- complete CLI reference for alert management
