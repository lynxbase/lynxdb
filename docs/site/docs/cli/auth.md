---
title: login, logout & auth
description: Save credentials for a LynxDB server and manage its API keys.
---

# login, logout & auth

A server started with `--auth` requires an API key on every request under `/api/`. `login` and `logout` manage the keys the CLI stores; `auth` manages the keys the server accepts.

See [TLS & Authentication](../deployment/tls-auth.md) for server setup.

## login

```
lynxdb login [--token <key>]
```

Checks the key against the server named by `--server` and saves it, keyed by the server URL, in `lynxdb/credentials.yaml` under the user config directory (`~/.config` on Linux, `~/Library/Application Support` on macOS). Later commands to that server send the saved key unless `--token` or `LYNXDB_TOKEN` is set.

Without `--token`, `login` prompts for the key when stdin is a terminal.

```bash
lynxdb login
lynxdb login --server https://logs.example.com
lynxdb login --token "$LYNXDB_TOKEN"
```

For an `https://` server whose certificate no system CA signed, such as the server's self-signed one, `login` shows the certificate's SHA-256 fingerprint and asks whether to trust it. The fingerprint is saved with the key, and later connections fail when the server presents a different certificate. In a non-interactive shell the prompt cannot appear; pass `--tls-skip-verify` or run `login` once in a terminal.

## logout

```
lynxdb logout [--all]
```

Removes the saved key and fingerprint for the server named by `--server`. `--all` removes the credentials of every server.

```bash
lynxdb logout --server https://logs.example.com
lynxdb logout --all
```

## auth

```
lynxdb auth <subcommand>
```

`create`, `list`, `revoke` and `rotate-root` call the server's key API, which takes only the root key, the one the server prints at its first start. Any other key gets `403 FORBIDDEN` and the command exits with 7.

### auth create

```
lynxdb auth create --name <name> [--scope <scope>] [--expires <duration>] [--description <text>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--name` | required | Key name |
| `--scope` | `full` | `ingest`, `query`, `admin` or `full` |
| `--expires` | `never` | Lifetime: `30d`, `12w`, `1y`, or a Go duration such as `720h` |
| `--description` | | Free text |

| Scope | Allows |
|-------|--------|
| `ingest` | Ingest endpoints: `/api/v1/ingest`, `/api/v1/ingest/raw`, Elasticsearch `_bulk` and `_doc`, Splunk HEC, the OTLP listeners |
| `query` | Queries, query streams and job status |
| `admin` | Canceling jobs, reading the server config |
| `full` | Everything except the key API and config changes, which take the root key |

Endpoints that only read state, such as `/api/v1/stats`, `/api/v1/status`, `/api/v1/fields`, `/api/v1/histogram` and `/api/v1/query/explain`, take a key of any scope.

The token is printed once on stderr. An `ingest` key also prints a Filebeat output snippet for the Elasticsearch-compatible endpoint, and a `query` key prints CLI and HTTP header usage. With `--format json` the key is printed to stdout as JSON: `token` is the bearer token and `api_key` the Elasticsearch-style API key.

```bash
lynxdb auth create --name shipper --scope ingest
lynxdb auth create --name dashboards --scope query --expires 90d
lynxdb auth create --name ci --scope admin --description "CI automation"
lynxdb auth create --name web-01 --scope ingest --format json | jq -r .token
```

```
  ✔ API key created

  Name           shipper
  ID             ak_apc9c7MczIzS
  Scope          ingest
  Expires        never

  Token (shown once — save it now):
    lynx_ak_apc9c7MczIzS_hGAkWvj7FAc9QeWJKY40uvSwMFLZDe1f

  ─── filebeat.yml ─────────────────────────────────────────
  output.elasticsearch:
    hosts: ["https://logs.example.com"]
    api_key: "ak_apc9c7MczIzS:lynx_ak_apc9c7MczIzS_hGAkWvj7FAc9QeWJKY40uvSwMFLZDe1f"
    allow_older_versions: true

  setup.ilm.enabled: false
  setup.template.enabled: false
  ─────────────────────────────────────────────────────────────
```

### auth list

```
lynxdb auth list
```

**Alias:** `ls`

Lists keys with ID, name, scope, expiry, last use and age. The root key shows as `[root]`, and the summary counts keys that expire within 30 days.

```
┌─────────────────┬─────────┬────────┬────────────┬───────────┬──────────┐
│ ID              │ NAME    │ SCOPE  │ EXPIRES    │ LAST USED │ CREATED  │
├─────────────────┼─────────┼────────┼────────────┼───────────┼──────────┤
│ rk_HVyY4Qs5JGXq │ [root]  │ full   │ never      │ just now  │ just now │
│ ak_apc9c7MczIzS │ shipper │ ingest │ never      │ —         │ just now │
│ ak_FEIkzu2kl0N5 │ dash    │ query  │ 2027-02-15 │ —         │ just now │
└─────────────────┴─────────┴────────┴────────────┴───────────┴──────────┘
```

### auth revoke

```
lynxdb auth revoke <id> [--yes]
```

Revokes a key at once; requests that use it fail from then on. The command asks for confirmation when stdin is a terminal; `--yes` (`-y`) skips the prompt.

### auth rotate-root

```
lynxdb auth rotate-root [--yes]
```

Creates a new root key, revokes the old one and prints the new token once. The CLI also replaces the saved credentials for the server with the new key. `--yes` (`-y`) skips the confirmation prompt.

### auth status

```
lynxdb auth status
```

Shows the server URL, the TLS mode (off, CA-verified, pinned fingerprint, or verification skipped) and whether the current key authenticates.

```
  Server    https://logs.example.com
  TLS       TOFU (fingerprint saved)
  Fingerprint  SHA-256:4a:1f:...
  Your key  lynx_ak_7fA3mNpQ9xKv...
  Status    authenticated
```

## Using a key without login

```bash
lynxdb query 'source=main | stats count()' --token "$KEY"
LYNXDB_TOKEN="$KEY" lynxdb status
curl -H "Authorization: Bearer $KEY" http://localhost:3100/api/v1/stats
```

## See also

- [server](./server.md#authentication)
- [Connection profiles](./config-cmd.md#config-add-profile)
- [Server API](../api/server.md)
