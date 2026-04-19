---
sidebar_position: 8
title: alerts
description: Create, inspect, test, and manage alerts stored on a LynxDB server.
---

# alerts

Create, inspect, and test server-side alerts.

```
lynxdb alerts [id]
```

Without a subcommand, lists all alerts. With an ID argument, shows alert details.

## alerts create

Create an alert from a JSON file.

```
lynxdb alerts create --file alert.json
```

| Flag | Default | Description |
|------|---------|-------------|
| `--file` | (required) | Path to an alert JSON payload matching the Alerts API |

Example `alert.json`:

```json
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

## alerts test

Test alert evaluation without sending notifications.

```
lynxdb alerts test <id>
```

```bash
lynxdb alerts test alert_abc123
```

---

## alerts test-channels

Send a test notification to all enabled channels on an alert.

```
lynxdb alerts test-channels <id>
```

```bash
lynxdb alerts test-channels alert_abc123
```

---

## alerts enable

Enable a stored alert.

```
lynxdb alerts enable <id>
```

```bash
lynxdb alerts enable alert_abc123
```

---

## alerts disable

Disable a stored alert.

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
lynxdb alerts delete alert_abc123
lynxdb alerts delete alert_abc123 --force
```

## Implemented Notification Channels

Channels are configured through the alert JSON file or the REST API. The current server build wires these channel backends:

| Channel | Required configuration |
|---------|------------------------|
| **Slack** | `webhook_url` |
| **Telegram** | `bot_token`, `chat_id` |
| **Webhook** | `url` |

## See Also

- [Alerts API](/docs/api/alerts) for payloads and channel definitions
- [Set Up Alerts](/docs/guides/alerts) for end-to-end examples
- [query](/docs/cli/query) for testing alert queries interactively
