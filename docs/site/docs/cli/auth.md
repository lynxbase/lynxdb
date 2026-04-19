---
title: auth
description: Create, list, revoke, rotate, and inspect API keys for auth-enabled LynxDB servers.
---

# auth

Manage API keys for a server started with authentication enabled.

```bash
lynxdb auth <subcommand>
```

See [Server Settings](/docs/configuration/server) and [TLS & Authentication](/docs/deployment/tls-auth) for server-side setup.

## Prerequisites

- start the server with auth enabled, for example `lynxdb server --auth`
- authenticate the CLI with a root key before creating, revoking, or rotating keys

## auth create

Create a new API key.

```bash
lynxdb auth create --name <name> [--scope full|ingest|query|admin] [--expires never|<duration>] [--description <text>]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--name` | required | Human-readable key name |
| `--scope` | `full` | Key scope: `full`, `ingest`, `query`, `admin` |
| `--expires` | `never` | Expiration such as `30d`, `90d`, `1y`, or a Go duration |
| `--description` | empty | Optional description |

### Examples

```bash
# Create an ingest-only key
lynxdb auth create --name filebeat --scope ingest

# Create a query key that expires in 90 days
lynxdb auth create --name grafana --scope query --expires 90d

# Create an admin key with a description
lynxdb auth create --name ci-admin --scope admin --description "CI automation"
```

The token is shown once. Save it immediately.

## auth list

List configured API keys.

```bash
lynxdb auth list
```

This shows key IDs, names, scopes, creation time, expiration, and last-use timestamps. Root keys are marked as `[root]`.

## auth revoke

Revoke an API key by ID.

```bash
lynxdb auth revoke <id> [--yes]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--yes`, `-y` | `false` | Skip the confirmation prompt |

### Example

```bash
lynxdb auth revoke key_002 --yes
```

## auth rotate-root

Create a new root key and revoke the current one in a single step.

```bash
lynxdb auth rotate-root [--yes]
```

### Flags

| Flag | Default | Description |
|---|---|---|
| `--yes`, `-y` | `false` | Skip the confirmation prompt |

### Example

```bash
lynxdb auth rotate-root --yes
```

The new root token is returned once. Any automation using the old root key must be updated immediately.

## auth status

Show the current authentication state for the configured server.

```bash
lynxdb auth status
```

Use this to confirm whether auth is enabled, whether your current key is valid, and whether it has root privileges.

## Common Workflows

```bash
# Create an ingest key for a shipper
lynxdb auth create --name edge-agent --scope ingest

# List current keys
lynxdb auth list

# Revoke an old key
lynxdb auth revoke key_00abc --yes

# Rotate the root key during maintenance
lynxdb auth rotate-root --yes
```

## Related

- [TLS & Authentication](/docs/deployment/tls-auth)
- [Server API](/docs/api/server)
- [REST API Overview](/docs/api/overview)
