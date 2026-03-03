---
title: Single Node Deployment
description: Deploy LynxDB as a single-node server with systemd, persistent storage, and production defaults.
---

# Single Node Deployment

The simplest production deployment. A single LynxDB process handles ingestion, storage, compaction, and queries. Handles 50-100K events/sec on commodity hardware with no configuration needed.

## Quick Start

```bash
# Install LynxDB
curl -fsSL https://lynxdb.org/install.sh | sh

# Start with defaults (localhost:3100, ~/.local/share/lynxdb)
lynxdb server
```

## Production Setup

### Recommended: `lynxdb install`

A single command performs all production setup steps:

```bash
sudo lynxdb install
```

This creates:

- A dedicated `lynxdb` system user and group
- Data directory at `/var/lib/lynxdb` with correct ownership
- Default config at `/etc/lynxdb/config.yaml`
- File descriptor limits (`nofile=65536`)
- `CAP_NET_BIND_SERVICE` for binding to privileged ports
- A hardened systemd service with `ProtectSystem=strict`, `NoNewPrivileges=true`, and more
- A post-install self-test to verify everything works

Start and verify:

```bash
sudo systemctl enable lynxdb
sudo systemctl start lynxdb

lynxdb health
lynxdb status
```

Customize with flags:

```bash
# Custom data directory
sudo lynxdb install --data-dir /data/lynxdb

# Skip the systemd unit (e.g., for container environments)
sudo lynxdb install --skip-service

# Non-interactive (CI/CD)
sudo lynxdb install --yes
```

See the full [`install` reference](/docs/cli/install) for all flags.

### Manual Setup

If you need full control over each step, perform the setup manually:

#### 1. Create a Dedicated User

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin lynxdb
```

#### 2. Create Directories

```bash
sudo mkdir -p /var/lib/lynxdb
sudo mkdir -p /etc/lynxdb
sudo chown lynxdb:lynxdb /var/lib/lynxdb
```

#### 3. Create Config File

```bash
sudo lynxdb config init --system
sudo chown lynxdb:lynxdb /etc/lynxdb/config.yaml
```

Edit `/etc/lynxdb/config.yaml`:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

storage:
  compression: "lz4"
  flush_threshold: "512mb"
  compaction_interval: "30s"
  compaction_workers: 2
  cache_max_bytes: "2gb"

query:
  max_concurrent: 20
  max_query_runtime: "10m"
  default_result_limit: 1000

ingest:
  max_body_size: "50mb"
```

#### 4. Create systemd Service

Create `/etc/systemd/system/lynxdb.service`:

```ini
[Unit]
Description=LynxDB Log Analytics Database
Documentation=https://lynxdb.org/docs
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=lynxdb
Group=lynxdb
ExecStart=/usr/local/bin/lynxdb server --config /etc/lynxdb/config.yaml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5s
LimitNOFILE=65536
LimitMEMLOCK=infinity

# Security hardening
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/lynxdb
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

#### 5. Start the Service

```bash
sudo systemctl daemon-reload
sudo systemctl enable lynxdb
sudo systemctl start lynxdb

# Check status
sudo systemctl status lynxdb

# View logs
sudo journalctl -u lynxdb -f
```

#### 6. Verify

```bash
# Health check
lynxdb health

# Server status
lynxdb status

# Run diagnostics
lynxdb doctor
```

## Configuration via Environment Variables

An alternative to the config file, especially useful for container deployments:

```ini
# /etc/systemd/system/lynxdb.service.d/override.conf
[Service]
Environment=LYNXDB_LISTEN=0.0.0.0:3100
Environment=LYNXDB_DATA_DIR=/var/lib/lynxdb
Environment=LYNXDB_RETENTION=30d
Environment=LYNXDB_LOG_LEVEL=info
```

```bash
sudo systemctl daemon-reload
sudo systemctl restart lynxdb
```

## Enabling TLS and Authentication

For servers exposed to the network:

```bash
# With auto-generated self-signed certificate
lynxdb server --tls --auth --config /etc/lynxdb/config.yaml

# With your own certificates
lynxdb server \
  --tls-cert /etc/ssl/certs/lynxdb.crt \
  --tls-key /etc/ssl/private/lynxdb.key \
  --auth \
  --config /etc/lynxdb/config.yaml
```

Update the systemd `ExecStart` line accordingly. See [TLS and Authentication](/docs/deployment/tls-auth) for details.

## Firewall

Open port 3100 (or your custom port):

```bash
# ufw
sudo ufw allow 3100/tcp

# firewalld
sudo firewall-cmd --permanent --add-port=3100/tcp
sudo firewall-cmd --reload

# iptables
sudo iptables -A INPUT -p tcp --dport 3100 -j ACCEPT
```

## Hot-Reload Configuration

Change settings without restarting:

```bash
# Edit config
sudo vim /etc/lynxdb/config.yaml

# Reload (sends SIGHUP)
lynxdb config reload
# or
sudo systemctl reload lynxdb
```

Hot-reloadable settings: `log_level`, `retention`, `query.max_concurrent`, `query.default_result_limit`, `query.max_result_limit`, `query.max_query_runtime`.

## Hardware Recommendations

| Workload | CPU | Memory | Disk | Throughput |
|----------|-----|--------|------|------------|
| Light (up to 10K events/sec) | 2 cores | 2 GB | 50 GB SSD | Small team |
| Medium (up to 50K events/sec) | 4 cores | 8 GB | 200 GB SSD | Startup |
| Heavy (up to 100K events/sec) | 8 cores | 16 GB | 500 GB SSD | Mid-size company |

For higher throughput, consider a [small cluster](/docs/deployment/small-cluster) or [S3 tiering](/docs/deployment/s3-setup) for cost-effective long-term storage.

## Log Rotation

LynxDB writes logs to stderr, which systemd captures in the journal. Configure journal log rotation in `/etc/systemd/journald.conf`:

```ini
[Journal]
SystemMaxUse=1G
MaxRetentionSec=30d
```

## Backup

For single-node deployments, the simplest backup strategy is periodic snapshots of the data directory:

```bash
# Stop writes briefly for a consistent snapshot
sudo systemctl stop lynxdb
tar czf /backups/lynxdb-$(date +%Y%m%d).tar.gz /var/lib/lynxdb
sudo systemctl start lynxdb
```

For zero-downtime backups, see [Backup and Restore](/docs/operations/backup-restore).

## Next Steps

- [Docker Deployment](/docs/deployment/docker) -- container-based deployment
- [S3 Storage Setup](/docs/deployment/s3-setup) -- add S3 tiering for long-term storage
- [TLS and Authentication](/docs/deployment/tls-auth) -- secure the server
- [Monitoring](/docs/operations/monitoring) -- observe server health
- [Small Cluster](/docs/deployment/small-cluster) -- scale beyond a single node
