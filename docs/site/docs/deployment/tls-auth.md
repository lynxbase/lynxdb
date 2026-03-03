---
title: TLS and Authentication
description: Secure LynxDB with TLS encryption and API key authentication -- setup, certificate management, and key rotation.
---

# TLS and Authentication

LynxDB supports TLS encryption for all HTTP traffic and API key authentication for access control. Both are optional and independent -- you can use TLS without auth, auth without TLS, or both together.

## TLS

### Auto-Generated Self-Signed Certificate

The simplest way to enable TLS. LynxDB generates a self-signed certificate at startup:

```bash
lynxdb server --tls
```

The CLI implements Trust-On-First-Use (TOFU) for self-signed certificates:

```bash
$ lynxdb login --server https://localhost:3100
  TLS certificate is self-signed.
  Fingerprint: SHA256:a1b2c3d4e5f6...
  Trust this certificate? [y/N] y
  Certificate fingerprint saved.
```

After trusting, subsequent connections to the same server work without prompts.

### Your Own Certificates

For production, use certificates from your organization's CA or Let's Encrypt:

```bash
lynxdb server \
  --tls-cert /etc/ssl/certs/lynxdb.crt \
  --tls-key /etc/ssl/private/lynxdb.key
```

#### With Let's Encrypt (certbot)

```bash
# Obtain certificate
sudo certbot certonly --standalone -d lynxdb.company.com

# Start LynxDB with the certificate
lynxdb server \
  --tls-cert /etc/letsencrypt/live/lynxdb.company.com/fullchain.pem \
  --tls-key /etc/letsencrypt/live/lynxdb.company.com/privkey.pem
```

Set up auto-renewal:

```bash
# /etc/cron.d/certbot-renew
0 0 * * * root certbot renew --quiet --post-hook "systemctl reload lynxdb"
```

### systemd Service with TLS

```ini
# /etc/systemd/system/lynxdb.service
[Service]
ExecStart=/usr/local/bin/lynxdb server \
  --config /etc/lynxdb/config.yaml \
  --tls-cert /etc/ssl/certs/lynxdb.crt \
  --tls-key /etc/ssl/private/lynxdb.key
```

### Client Configuration

```bash
# Connect to TLS-enabled server
lynxdb query --server https://lynxdb.company.com 'level=error | stats count'

# Skip TLS verification (development only)
lynxdb query --server https://localhost:3100 --tls-skip-verify 'level=error'

# Or via environment variable
export LYNXDB_SERVER=https://lynxdb.company.com
export LYNXDB_TLS_SKIP_VERIFY=true  # Development only
```

## Authentication

### Enabling Auth

Enable API key authentication with the `--auth` flag:

```bash
lynxdb server --auth
```

When auth is enabled and no keys exist, LynxDB generates a root key at startup:

```
Auth enabled -- no API keys exist. Generated root key:

  lxk_a1b2c3d4e5f6...

Save this key now. It will NOT be shown again.
```

### Logging In

```bash
# Interactive (prompts for key with hidden input)
lynxdb login

# Non-interactive
lynxdb login --token lxk_a1b2c3d4e5f6...

# Or set the environment variable
export LYNXDB_TOKEN=lxk_a1b2c3d4e5f6...
```

Credentials are stored in `~/.config/lynxdb/credentials` and are scoped to the server URL.

### Creating API Keys

Create additional keys for different services and team members:

```bash
# Create a key for your CI pipeline
lynxdb auth create-key --name ci-pipeline
# Created API key "ci-pipeline":
#   lxk_9f8e7d6c5b4a...
# Save this key now. It will NOT be shown again.

# Create a key for Grafana
lynxdb auth create-key --name grafana-dashboard

# Create a key for the ingest pipeline
lynxdb auth create-key --name filebeat-ingest
```

### Managing Keys

```bash
# List all keys
lynxdb auth list-keys
# ID          NAME              PREFIX        CREATED              LAST USED
# key_001     root              lxk_a1b2...   2026-01-15T10:00Z    2026-03-01T14:30Z
# key_002     ci-pipeline       lxk_9f8e...   2026-02-01T09:00Z    2026-03-01T12:00Z
# key_003     grafana-dashboard lxk_3c4d...   2026-02-15T11:00Z    2026-03-01T14:25Z

# Revoke a key
lynxdb auth revoke-key key_002

# Check auth status
lynxdb auth status
# Server:     https://lynxdb.company.com
# TLS:        verified (CA-signed)
# Auth:       authenticated as "root" (key_001)
```

### Rotating the Root Key

```bash
lynxdb auth rotate-root
# This will:
#   1. Generate a new root key
#   2. Revoke the current root key
#   3. Update your local credentials
#
# Continue? [y/N] y
#
# New root key:
#   lxk_new_root_key...
# Save this key now. It will NOT be shown again.
# Local credentials updated.
```

### Logging Out

```bash
# Remove credentials for current server
lynxdb logout

# Remove credentials for a specific server
lynxdb logout --server https://lynxdb.company.com

# Remove all saved credentials
lynxdb logout --all
```

## TLS + Auth Together

The recommended production setup uses both TLS and authentication:

```bash
lynxdb server \
  --tls-cert /etc/ssl/certs/lynxdb.crt \
  --tls-key /etc/ssl/private/lynxdb.key \
  --auth \
  --config /etc/lynxdb/config.yaml
```

Client setup:

```bash
# Login once
lynxdb login --server https://lynxdb.company.com --token lxk_a1b2c3d4e5f6...

# All subsequent commands use saved credentials
lynxdb query 'level=error | stats count'
```

### API Access with Auth

```bash
# Include token in API requests
curl -s https://lynxdb.company.com/api/v1/query \
  -H "Authorization: Bearer lxk_a1b2c3d4e5f6..." \
  -d '{"q": "level=error | stats count"}'

# Or use the X-LynxDB-Token header
curl -s https://lynxdb.company.com/api/v1/query \
  -H "X-LynxDB-Token: lxk_a1b2c3d4e5f6..." \
  -d '{"q": "level=error | stats count"}'
```

## Connection Profiles

Manage multiple server connections with profiles:

```bash
# Add a production profile
lynxdb config add-profile prod \
  --url https://lynxdb.company.com \
  --token lxk_production_key...

# Add a staging profile
lynxdb config add-profile staging \
  --url https://staging-lynxdb.company.com \
  --token lxk_staging_key...

# Use a specific profile
lynxdb query 'level=error | stats count' --profile prod

# Set default profile
export LYNXDB_PROFILE=prod
```

## Security Checklist

For production deployments:

- [ ] Enable TLS with CA-signed certificates (not self-signed)
- [ ] Enable authentication (`--auth`)
- [ ] Save the root key securely (password manager, secrets vault)
- [ ] Create separate API keys for each service/user
- [ ] Rotate the root key after initial setup
- [ ] Set up certificate auto-renewal
- [ ] Use firewall rules to restrict port access
- [ ] Monitor auth failures in logs

## Next Steps

- [Single Node Deployment](/docs/deployment/single-node) -- systemd service setup
- [Docker Deployment](/docs/deployment/docker) -- container-based TLS
- [Kubernetes Deployment](/docs/deployment/kubernetes) -- K8s secrets for TLS and auth
- [Monitoring](/docs/operations/monitoring) -- monitor security events
