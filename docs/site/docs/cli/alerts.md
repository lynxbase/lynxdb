---
sidebar_position: 8
title: alerts
description: Create, test, enable, disable, and delete SPL2-powered alerts with multi-channel notifications.
---

# alerts

Manage alerts -- SPL2-powered threshold monitoring with multi-channel notifications.

```
lynxdb alerts [id]
```

Without a subcommand, lists all alerts. With an ID argument, shows alert details.

## alerts create

Create a new alert with an SPL2 query and check interval.

```
lynxdb alerts create --name <name> --query <query> [--interval <duration>]
```

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | (required) | Alert name |
| `--query` | (required) | SPL2 query that triggers the alert when results are returned |
| `--interval` | `5m` | Check interval |

### Examples

```bash
# Create an alert for high error rate
lynxdb alerts create --name "High errors" \
  --query 'level=error | stats count as errors | where errors > 100' \
  --interval 5m

# Alert on 5xx spike
lynxdb alerts create --name "5xx spike" \
  --query 'source=nginx status>=500 | stats count as c | where c > 50' \
  --interval 1m

# Alert on slow queries
lynxdb alerts create --name "Slow queries" \
  --query 'duration_ms > 5000 | stats count as slow | where slow > 10' \
  --interval 10m
```

Notification channels (Slack, Telegram, PagerDuty, webhook, etc.) are configured via the REST API. See the [REST API documentation](/docs/api/alerts) for the full alert configuration schema.

---

## alerts test

Test alert evaluation without sending notifications. Runs the alert query and shows whether it would trigger.

```
lynxdb alerts test <id>
```

```bash
lynxdb alerts test alert_abc123
```

---

## alerts test-channels

Send a test notification to all configured channels for an alert. Useful for verifying Slack webhooks, PagerDuty routing keys, etc.

```
lynxdb alerts test-channels <id>
```

```bash
lynxdb alerts test-channels alert_abc123
```

---

## alerts enable

Enable a disabled alert.

```
lynxdb alerts enable <id>
```

```bash
lynxdb alerts enable alert_abc123
```

---

## alerts disable

Disable an alert without deleting it. The alert retains its configuration and history.

```
lynxdb alerts disable <id>
```

```bash
lynxdb alerts disable alert_abc123
```

---

## alerts delete

Delete an alert permanently.

```
lynxdb alerts delete <id> [--force]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--force` | `false` | Skip confirmation prompt |

```bash
# Delete with confirmation prompt
lynxdb alerts delete alert_abc123

# Skip confirmation
lynxdb alerts delete alert_abc123 --force
```

## Supported Notification Channels

Channels are configured via the REST API when creating or updating an alert:

| Channel | Configuration |
|---------|--------------|
| **Slack** | `webhook_url` |
| **Telegram** | `bot_token`, `chat_id` |
| **PagerDuty** | `routing_key`, `severity` |
| **OpsGenie** | `api_key` |
| **Email** | `to`, `from`, SMTP settings |
| **incident.io** | API key and configuration |
| **Webhook** | Any HTTP endpoint |

## See Also

- [query](/docs/cli/query) for testing alert queries interactively
- [mv](/docs/cli/mv) for materialized views that can accelerate alert queries
- [Server](/docs/cli/server) for running the server that evaluates alerts
