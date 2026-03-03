---
sidebar_position: 4
title: Server Mode
description: Run LynxDB as a persistent server with storage, API, and web UI.
---

# Server Mode

Server mode runs LynxDB as a persistent HTTP server with on-disk storage, full REST API, materialized views, alerts, and dashboards.

## Start the Server

```bash
# Start with defaults (localhost:3100, data in ~/.local/share/lynxdb)
lynxdb server

# Specify a data directory
lynxdb server --data-dir /var/lib/lynxdb

# Custom listen address
lynxdb server --addr 0.0.0.0:3100

# In-memory mode (no persistence, useful for testing)
lynxdb server --data-dir ""
```

On startup, you'll see:

```
  Config:  /home/user/.config/lynxdb/config.yaml
  Data:    /var/lib/lynxdb
  Listen:  localhost:3100

time=2026-01-15T10:00:00Z level=INFO msg="starting LynxDB" version=0.5.0 addr=localhost:3100
```

## Ingest Data

### From the CLI

```bash
# Ingest a log file
lynxdb ingest access.log --source web-01

# Ingest with metadata
lynxdb ingest access.log --source web-01 --sourcetype nginx

# Pipe data
cat events.json | lynxdb ingest
```

### From the API

```bash
# Single JSON event
curl -X POST localhost:3100/api/v1/ingest \
  -d '{"message": "user login", "user_id": 42, "level": "info"}'

# NDJSON batch
curl -X POST localhost:3100/api/v1/ingest \
  -H 'Content-Type: application/x-ndjson' \
  -d '{"level":"info","msg":"event 1"}
{"level":"error","msg":"event 2"}'

# Raw text
echo '192.168.1.1 - - [14/Feb/2026:14:23:01] "GET /api HTTP/1.1" 200' | \
  curl -X POST localhost:3100/api/v1/ingest/raw --data-binary @-
```

No schema needed. Fields are discovered and indexed automatically.

## Query Data

### CLI

```bash
# Basic search
lynxdb query 'level=error'

# Aggregation
lynxdb query 'level=error | stats count by source | sort -count'

# Time range
lynxdb query 'level=error | stats count' --since 1h

# Output formats
lynxdb query 'level=error | stats count by source' --format table
lynxdb query 'level=error | stats count by source' --format csv > report.csv
```

### API

```bash
# Synchronous query
curl -s localhost:3100/api/v1/query \
  -d '{"q": "level=error | stats count by source", "from": "-1h"}' | jq .

# Streaming results (NDJSON)
curl -s localhost:3100/api/v1/query/stream \
  -d '{"q": "level=error", "from": "-1h"}'
```

## Monitor with Live Tail

Stream log events in real-time:

```bash
# Tail all events
lynxdb tail

# Tail with filter
lynxdb tail 'level=error'

# Tail with pipeline
lynxdb tail 'source=nginx status>=500 | fields _time, uri, status'
```

## Check Server Status

```bash
# Server overview
lynxdb status

# Health check (for load balancers)
lynxdb health

# Field catalog
lynxdb fields

# Cache statistics
lynxdb cache stats
```

## Run as a Service

### Recommended: `lynxdb install`

The easiest way to set up LynxDB as a system service. One command creates a dedicated user, directories, config file, and a hardened systemd/launchd unit:

```bash
sudo lynxdb install
sudo systemctl start lynxdb
```

See the full [`install` reference](/docs/cli/install) for all flags and customization options.

### Manual systemd

If you prefer to manage the service unit yourself:

```ini
# /etc/systemd/system/lynxdb.service
[Unit]
Description=LynxDB Log Analytics
After=network.target

[Service]
Type=simple
User=lynxdb
ExecStart=/usr/local/bin/lynxdb server --data-dir /var/lib/lynxdb
Restart=on-failure
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl enable lynxdb
sudo systemctl start lynxdb
```

### Docker

```bash
docker run -d --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  OrlovEvgeny/Lynxdb server --data-dir /data
```

## Configuration

Create a config file for persistent settings:

```bash
lynxdb config init
```

This creates `~/.config/lynxdb/config.yaml`:

```yaml
listen: "localhost:3100"
data_dir: "/var/lib/lynxdb"
retention: "7d"
log_level: "info"

storage:
  compression: "lz4"
  flush_threshold: "512mb"
```

See the full [Configuration Reference](/docs/configuration/overview) for all options.

## Next Steps

- **[Your First SPL2 Query](/docs/getting-started/first-query)** -- Learn the query language
- **[Ingesting Data](/docs/guides/ingest-data)** -- All ingestion methods
- **[Configuration](/docs/configuration/overview)** -- Tune your server
- **[Deployment](/docs/deployment/single-node)** -- Production deployment guide
