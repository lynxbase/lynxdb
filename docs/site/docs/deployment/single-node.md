---
title: Single Node Deployment
description: Run LynxDB as a single-node server under systemd with persistent storage, a config file, and reloadable settings.
---

# Single Node Deployment

One `lynxdb server` process handles ingest, storage, compaction and queries. This is the recommended deployment.

## Install

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
lynxdb version
```

The script installs the binary only. Archives for Linux, macOS and Windows are also attached to each [GitHub release](https://github.com/lynxbase/lynxdb/releases).

To try it in the foreground with the defaults (`localhost:3100`, data in `~/.local/share/lynxdb`):

```bash
lynxdb server
```

## Production Setup

### 1. Create a user and directories

```bash
sudo useradd --system --no-create-home --shell /usr/sbin/nologin lynxdb
sudo mkdir -p /var/lib/lynxdb /etc/lynxdb
sudo chown lynxdb:lynxdb /var/lib/lynxdb
```

### 2. Write the config file

```bash
sudo lynxdb config init --system
```

This writes the commented template with every default to `/etc/lynxdb/config.yaml`. Edit the values you need:

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

storage:
  compaction_workers: 2

query:
  max_concurrent: 32
  max_query_runtime: "10m"
  spill_dir: "/var/lib/lynxdb/spill"

ingest:
  otlp:
    http_listen: ""          # set an address only if you ship OTLP; see Firewall

auth:
  enabled: true
```

Check it:

```bash
sudo -u lynxdb lynxdb config validate --config /etc/lynxdb/config.yaml
```

### 3. Create the systemd unit

`/etc/systemd/system/lynxdb.service`:

```ini
[Unit]
Description=LynxDB log analytics database
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

ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/lynxdb
NoNewPrivileges=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

Adjust the `ExecStart` path to where the binary is installed. If the syslog receiver listens on a port below 1024, add `AmbientCapabilities=CAP_NET_BIND_SERVICE`.

### 4. Start and verify

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now lynxdb
sudo journalctl -u lynxdb -f
```

With `auth.enabled: true`, the first start prints a root key to the journal once. Save it, then:

```bash
lynxdb login --server http://localhost:3100
lynxdb health
lynxdb status
lynxdb query 'source=main | stats count() by host'
```

## Settings by Environment Variable

Instead of editing the file, a systemd drop-in can set variables:

```ini
# /etc/systemd/system/lynxdb.service.d/env.conf
[Service]
Environment=LYNXDB_RETENTION=30d
Environment=LYNXDB_LOG_LEVEL=info
```

```bash
sudo systemctl daemon-reload
sudo systemctl restart lynxdb
```

See [Environment Variables](../configuration/environment-variables.md) for the full list.

## TLS and Authentication

```bash
# Self-signed certificate stored under <data_dir>/tls/
lynxdb server --config /etc/lynxdb/config.yaml --tls --auth

# Your own certificate
lynxdb server --config /etc/lynxdb/config.yaml \
  --tls-cert /etc/lynxdb/tls/server.crt \
  --tls-key /etc/lynxdb/tls/server.key \
  --auth
```

Put the same settings in the `tls` and `auth` sections of the config file to keep `ExecStart` short. See [TLS and Authentication](./tls-auth.md).

## Firewall

| Port | Purpose |
|------|---------|
| 3100/tcp | HTTP API (`listen`) |
| 4318/tcp | OTLP/HTTP listener (`ingest.otlp.http_listen`, on all interfaces by default, without auth) |
| 5514/udp, 5514/tcp, 6514/tcp | Syslog, when configured |

```bash
sudo ufw allow 3100/tcp
# or
sudo firewall-cmd --permanent --add-port=3100/tcp && sudo firewall-cmd --reload
```

## Reloading Configuration

```bash
sudo systemctl reload lynxdb
# or
sudo -u lynxdb lynxdb config reload --config /etc/lynxdb/config.yaml
```

A reload re-reads the config file and environment and applies `log_level`, `retention`, most `query.*` settings, `tail.*`, and parts of `ingest.*` and `syslog.*`. Listen addresses, `data_dir`, TLS, auth and storage layout need a restart. The full list is in [Configuration Overview](../configuration/overview.md#reloading-a-running-server).

## Sizing

- Memory: queries and the query cache share `server.total_memory_pool_bytes`, 40% of RAM by default. Each query is limited by `query.max_query_memory_bytes` (1 GB) and spills sorts and aggregations to `query.spill_dir` beyond that.
- Disk: parts are compressed columnar files. Put `data_dir` and `spill_dir` on SSD. Retention deletes whole time partitions (`storage.partition_by`).
- CPU: compaction uses `storage.compaction_workers` cores; queries use the rest.

For long retention on cheaper storage, add [S3 tiering](./s3-setup.md).

## Logs

The server writes its log to stdout (text format, level from `log_level`); under systemd it goes to the journal. Queries slower than `query.slow_query_threshold_ms` are logged at `WARN`.

## Backup

The simplest backup is a copy of the data directory while the server is stopped:

```bash
sudo systemctl stop lynxdb
sudo tar czf /backups/lynxdb-$(date +%Y%m%d).tar.gz -C /var/lib lynxdb
sudo systemctl start lynxdb
```

See [Backup and Restore](../operations/backup-restore.md) for other strategies.

## Next Steps

- [Docker Deployment](./docker.md)
- [S3/MinIO Storage Backend](./s3-setup.md)
- [TLS and Authentication](./tls-auth.md)
- [Monitoring](../operations/monitoring.md)
- [PPL overview](../ppl/overview.md)
