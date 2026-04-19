---
title: Set Up Alerts
description: How to define, test, and operate SPL2-powered alerts in LynxDB based on the current server implementation.
---

# Set Up Alerts

LynxDB alerts evaluate an SPL2 query on a schedule and send notifications when the query returns one or more rows.

:::note Current implementation
The built-in notifier registry and alert validation currently support exactly three channel backends: `webhook`, `slack`, and `telegram`.
:::

## How alerts work

1. Define an SPL2 query that returns rows only when the condition is met.
2. Set an evaluation interval such as `1m` or `5m`.
3. Attach one or more notification channels.
4. Use dry-run and channel-test endpoints before relying on the alert in production.

The alert query should usually aggregate first, then filter with `where`.

## Create an alert

Use [`lynxdb alerts create --file alert.json`](/docs/cli/alerts) or [`POST /api/v1/alerts`](/docs/api/alerts).

```json title="alert.json"
{
  "name": "High error rate",
  "query": "level=error | stats count as errors | where errors > 100",
  "interval": "5m",
  "channels": [
    {
      "type": "slack",
      "config": {
        "webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"
      }
    }
  ]
}
```

```bash
lynxdb alerts create --file alert.json
```

```bash
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "High error rate",
  "query": "level=error | stats count as errors | where errors > 100",
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

## Configure notification channels

Each alert can have multiple channels.

### Slack

```json
{
  "type": "slack",
  "config": {
    "webhook_url": "https://hooks.slack.com/services/T00/B00/xxx"
  }
}
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

### Webhook

```json
{
  "type": "webhook",
  "config": {
    "url": "https://hooks.example.com/alerts",
    "method": "POST"
  }
}
```

### Multiple channels on one alert

```bash
curl -X POST localhost:3100/api/v1/alerts -d '{
  "name": "Critical error rate",
  "query": "level=error | stats count as errors | where errors > 500",
  "interval": "5m",
  "channels": [
    {"type": "slack", "config": {"webhook_url": "https://hooks.slack.com/..."}},
    {"type": "telegram", "config": {"bot_token": "123456:ABC-DEF...", "chat_id": "-1001234567890"}},
    {"type": "webhook", "config": {"url": "https://hooks.example.com/alerts"}}
  ]
}'
```

## Test an alert

### Dry-run the query

```bash
lynxdb alerts test <alert_id>
```

This runs the alert query without sending notifications.

### Test notification delivery

```bash
lynxdb alerts test-channels <alert_id>
```

This sends a test notification to every enabled channel on the stored alert.

## Manage alerts

### List alerts

```bash
lynxdb alerts
```

Or via the API:

```bash
curl -s localhost:3100/api/v1/alerts | jq .
```

### View one alert

```bash
lynxdb alerts <alert_id>
```

### Disable or re-enable an alert

Use the CLI or `PATCH /api/v1/alerts/{id}`.

```bash
lynxdb alerts disable <alert_id>
lynxdb alerts enable <alert_id>
```

```bash
curl -X PATCH localhost:3100/api/v1/alerts/alt_xyz789 -d '{
  "enabled": false
}'
```

### Delete an alert

```bash
lynxdb alerts delete <alert_id>
lynxdb alerts delete <alert_id> --force
```

## Alert query patterns

### Error rate above threshold

```spl
level=error | stats count AS errors | where errors > 100
```

### Error percentage

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

### Missing heartbeat

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

### Failed login burst

```spl
source=auth type="login_failed"
  | stats count AS failures by src_ip
  | where failures > 20
```

## Alerts in cluster mode

Alert definitions are stored at the server layer and the docs set includes cluster-mode behavior elsewhere, but large separated-role alerting topologies should still be validated in staging against the version you plan to run.

## Next Steps

- [REST API: Alerts](/docs/api/alerts) for the exact payloads
- [CLI: `alerts`](/docs/cli/alerts) for current CLI coverage
- [Materialized views](/docs/guides/materialized-views) to speed up repeated alert queries
- [Dashboards](/docs/guides/dashboards) to visualize the same conditions your alerts watch
