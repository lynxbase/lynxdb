---
title: TLS and Authentication
description: Serve LynxDB over HTTPS, turn on API key authentication, manage keys and scopes, and connect clients and shippers.
---

# TLS and Authentication

TLS and API key authentication are independent; production servers should use both. Authentication needs a data directory, because keys are stored in `<data_dir>/auth/`.

## TLS

### Self-signed certificate

```bash
lynxdb server --data-dir /var/lib/lynxdb --tls
```

The server generates a certificate under `<data_dir>/tls/` on first start, reuses it afterwards, and prints its SHA-256 fingerprint at startup.

### Your own certificate

```bash
lynxdb server --config /etc/lynxdb/config.yaml \
  --tls-cert /etc/lynxdb/tls/server.crt \
  --tls-key /etc/lynxdb/tls/server.key
```

```yaml
tls:
  cert_file: "/etc/lynxdb/tls/server.crt"
  key_file: "/etc/lynxdb/tls/server.key"
```

The certificate and key are PEM files and must be set together. The minimum protocol version is TLS 1.2. The certificate is loaded at startup, so a renewed certificate takes effect after a restart:

```bash
sudo certbot certonly --standalone -d lynxdb.example.com
# /etc/letsencrypt/renewal-hooks/deploy/lynxdb.sh
systemctl restart lynxdb
```

The server's user needs read access to the key file.

### Clients

```bash
lynxdb query --server https://lynxdb.example.com 'source=main level=ERROR | stats count() by host'
```

A CA-signed certificate is verified normally. For a self-signed certificate, `lynxdb login` shows the fingerprint and asks whether to trust it, then saves it with the credentials; later commands verify the server against the saved fingerprint:

```text
$ lynxdb login --server https://lynxdb.internal:3100

Warning: Unknown TLS certificate for https://lynxdb.internal:3100:
  Fingerprint: SHA-256:4f:2a:...

  Trust this certificate? [y/N]: y
```

If the certificate changes, the CLI refuses to connect until you run `lynxdb logout --server <url>` and log in again. Non-interactive scripts can pass `--tls-skip-verify` (or set `LYNXDB_TLS_SKIP_VERIFY=true`), which encrypts without authenticating the server.

## Authentication

### Turn it on

```bash
lynxdb server --data-dir /var/lib/lynxdb --auth
```

```yaml
auth:
  enabled: true
```

On the first start with no keys, the server generates a root key and prints it once to stderr:

```text
  ⚠  Auth enabled — no API keys exist. Generated root key:

    lynx_rk_7fA3mNpQ9xKvL2wB8cD4eF6gH1jR5tY

  Save this key now. It will NOT be shown again.
  Use it to authenticate:  lynxdb login
  Or generate new keys:    lynxdb auth create
```

Root keys start with `lynx_rk_`, other keys with `lynx_ak_`.

### Log in

```bash
lynxdb login                                    # prompts for the key
lynxdb login --server https://lynxdb.example.com --token lynx_rk_7fA3mNpQ9xKvL2wB8cD4eF6gH1jR5tY
lynxdb auth status
lynxdb logout                                   # this server
lynxdb logout --all                             # every saved server
```

Credentials are saved per server URL in `credentials.yaml` under the user config directory (`~/.config/lynxdb/` on Linux, `~/Library/Application Support/lynxdb/` on macOS). The CLI picks a key in this order: `--token`, `LYNXDB_TOKEN`, saved credentials.

### Create and manage keys

Key management needs the root key.

```bash
lynxdb auth create --name filebeat-prod --scope ingest
lynxdb auth create --name dashboards --scope query --expires 90d --description "read-only dashboards"
lynxdb auth list
lynxdb auth revoke <id>
lynxdb auth rotate-root
```

`create` prints the token once; for an `ingest` key it also prints a ready-to-paste Filebeat `output.elasticsearch` block with the key in `api_key` form. `--expires` takes `30d`, `90d`, `1y` or `never` (default). `rotate-root` creates a root key, revokes the current one, and stores the replacement in the local credentials.

### Scopes

| Scope | Allows |
|-------|--------|
| `ingest` | `/api/v1/ingest`, `/api/v1/ingest/raw`, `/api/v1/ingest/hec`, and the `/api/v1/es/` bulk and document routes |
| `query` | `GET` and `POST /api/v1/query`, `/api/v1/query/stream`, listing, reading and streaming jobs |
| `admin` | Cancelling jobs, `GET /api/v1/config` |
| `full` | Everything a scoped key can do |
| root key | Everything, plus key management and `PATCH /api/v1/config` |

The other API routes accept any valid key regardless of scope: query explain, the PPL catalog, OTLP on `/api/v1/otlp/v1/logs`, index, field and source listings, status, stats, histogram and the pprof endpoints.

### Sending keys over HTTP

```bash
curl -s https://lynxdb.example.com/api/v1/query \
  -H "Authorization: Bearer lynx_ak_Q2wE4rT6yU8iO0pA1sD3fG5hJ7kL9zX" \
  -H "Content-Type: application/json" \
  -d '{"q": "source=main level=ERROR | stats count() by host"}'
```

| Scheme | Header | Typical client |
|--------|--------|----------------|
| Bearer | `Authorization: Bearer <token>` | CLI, scripts |
| ApiKey | `Authorization: ApiKey <base64(id:token)>` | Filebeat, Elasticsearch clients |
| Basic | `Authorization: Basic <base64(user:token)>`, user name ignored | Fluent Bit, Vector |
| Query parameter | `?_token=<token>` | Browser `EventSource` for job progress streams |

The server strips the `_token` parameter from the URL before logging, but it can still appear in proxy logs; prefer headers.

### Routes outside API key authentication

API key checks apply to paths under `/api/`. The other routes behave as follows when auth is on:

| Route | Behavior |
|-------|----------|
| `/health`, `/metrics` | Open, for probes and Prometheus. |
| `/_bulk`, `/{index}/_bulk` and other root-level Elasticsearch routes | Bulk requests are rejected with `401`; point shippers at `/api/v1/es/_bulk` with a key. |
| `/services/collector*` (Splunk HEC) | Not checked against API keys: any non-empty `Authorization: Splunk <token>` is accepted. Use `/api/v1/ingest/hec` for authenticated HEC ingest, or block these paths at a proxy. |
| OTLP listener (`ingest.otlp.http_listen`, `grpc_listen`) | Separate plain listeners without auth or TLS. Disable them or keep them on a private network; `/api/v1/otlp/v1/logs` on the API port is authenticated. |

## TLS and Auth Together

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"

tls:
  cert_file: "/etc/lynxdb/tls/server.crt"
  key_file: "/etc/lynxdb/tls/server.key"

auth:
  enabled: true

ingest:
  otlp:
    http_listen: ""
```

```bash
lynxdb login --server https://lynxdb.example.com --token lynx_rk_7fA3mNpQ9xKvL2wB8cD4eF6gH1jR5tY
lynxdb query 'source=main level=ERROR | stats count() as errors by host | sort - errors'
```

## Connection Profiles

```bash
lynxdb config add-profile prod --url https://lynxdb.example.com --token lynx_ak_prod...
lynxdb config add-profile dev --url https://dev.example.com --token lynx_ak_dev...
lynxdb query 'source=main level=ERROR | stats count() by host' --profile prod
export LYNXDB_PROFILE=prod
```

## Checklist

- [ ] TLS with a CA-signed certificate, and a restart after each renewal
- [ ] `auth.enabled: true` with a persistent `data_dir`
- [ ] Root key stored in a secrets manager; rotated with `lynxdb auth rotate-root` after setup
- [ ] One key per shipper and user, with the narrowest scope
- [ ] OTLP listeners disabled or on a private network; `/services/collector` blocked if HEC is not used
- [ ] Firewall limited to the ports in use

## Next Steps

- [Single Node Deployment](./single-node.md)
- [Docker Deployment](./docker.md)
- [Kubernetes Deployment](./kubernetes.md)
- [auth command](../cli/auth.md)
