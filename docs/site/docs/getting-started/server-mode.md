---
sidebar_position: 4
title: Server Mode
description: Run LynxDB as a persistent server with on-disk storage and a REST API.
---

# Server Mode

`lynxdb server` runs LynxDB as an HTTP server. It stores events on disk,
accepts data from the CLI, the REST API and log shippers, and answers
[PPL](../ppl/overview.md) queries.

## Start the server

```bash
# Defaults: listen on localhost:3100, data in ~/.local/share/lynxdb
lynxdb server

# Another data directory
lynxdb server --data-dir /var/lib/lynxdb

# Listen on all interfaces
lynxdb server --addr 0.0.0.0:3100

# In-memory: nothing is persisted (for tests)
lynxdb server --data-dir ""
```

On startup the server prints where its settings come from:

```text
  Config  /home/user/.config/lynxdb/config.yaml
  Data    /var/lib/lynxdb
  Listen  localhost:3100

time=2026-01-15T10:00:00.000Z level=INFO msg="starting LynxDB" version=0.5.0 addr=localhost:3100 ...
```

It also starts an OTLP/HTTP log receiver on port 4318. See
[Server configuration](../configuration/server.md) for every flag and setting.

:::caution
`--auth` needs a persistent data directory: API keys are stored there and
are not created for an in-memory server.
:::

## Ingest data

### From the CLI

```bash
# A log file
lynxdb ingest access.log --source web-01

# With a source type and a target index
lynxdb ingest access.log --source web-01 --sourcetype nginx --index web

# Standard input
kubectl logs deploy/api --since=1h | lynxdb ingest --source k8s-api
```

`lynxdb ingest` sends lines in batches of 5000 (`--batch-size`) to
`POST /api/v1/ingest/raw` and prints the event count and rate when done.

### From the REST API

```bash
# Structured events: a JSON array of objects with an "event" text
curl -X POST localhost:3100/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '[
    {"event":"user login","source":"auth-api","fields":{"user_id":42,"level":"info"}},
    {"event":"connection refused","source":"api","fields":{"service":"redis","level":"error"}}
  ]'

# Raw lines, one event per line
echo '192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234' | \
  curl -X POST localhost:3100/api/v1/ingest/raw \
    -H 'X-Source: nginx' \
    --data-binary @-
```

```json
{"data":{"accepted":2,"failed":0}}
```

Log shippers connect through the Elasticsearch `_bulk` API, OTLP, Splunk HEC
or syslog. See [Ingest Data](../guides/ingest-data.md).

## Query data

### CLI

`lynxdb query` sends the query to the server at `http://localhost:3100`
(`--server` or `LYNXDB_SERVER` changes it):

```bash
# Search
lynxdb query 'source=main level=ERROR'

# Aggregate
lynxdb query 'source=main level=ERROR | stats count() as errors by host | sort - errors'

# Time range
lynxdb query 'source=main level=ERROR | stats count()' --since 1h

# Output formats
lynxdb query 'source=main level=ERROR | stats count() by host' --format table
lynxdb query 'source=main level=ERROR | stats count() by host' --format csv > report.csv
```

### REST API

```bash
curl -s -X POST localhost:3100/api/v1/query \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main level=ERROR | stats count() as errors by host | sort - errors | head 3", "from": "-1h"}'
```

```json
{"data":{"columns":["host","errors"],"rows":[["api-02",67],["web-02",58],["web-03",57]],"total_rows":3,"has_more":false,"type":"aggregate"},"meta":{"took_ms":0,"scanned":294,"query_id":"qry_f2a8494fc0969fd9", ...}}
```

`POST /api/v1/query/stream` returns one JSON object per row, then a
`__meta` line:

```bash
curl -s -X POST localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main level=ERROR | fields _time, host, status | head 2"}'
```

```json
{"_time":"2025-01-15T08:00:00.105Z","host":"web-03","status":502}
{"_time":"2025-01-15T08:00:00.543Z","host":"api-02","status":502}
{"__meta":{"scanned":294,"took_ms":0,"total":2}}
```

See the [Query API](../api/query.md) for jobs, errors and profiling.

## Live tail

`lynxdb tail` takes a full PPL query, prints recent matching events, then
streams new ones as they arrive:

```bash
# Every event
lynxdb tail

# Errors only
lynxdb tail 'source=main level=ERROR'

# Server errors with selected fields
lynxdb tail 'source=main status>=500 | fields _time, host, path, status'
```

See [Live Tail](../guides/live-tail.md).

## Inspect the server

```bash
lynxdb status        # uptime, storage, event counts
lynxdb health        # health check, for scripts and load balancers
lynxdb indexes       # indexes and their retention
lynxdb fields        # field catalog: name, type, coverage
lynxdb cache stats   # query cache statistics
```

```text
  LynxDB 0.5.0 — uptime 6m — healthy

  Storage   258.0 KB
  Events    1,043 total  38 today
  Segments  7  buffer: 0 events
  Indexes   4
  Oldest    2025-01-15T08:00:00Z
```

## Run as a service

### systemd

```ini
# /etc/systemd/system/lynxdb.service
[Unit]
Description=LynxDB log analytics
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
sudo useradd --system --home /var/lib/lynxdb --shell /usr/sbin/nologin lynxdb
sudo mkdir -p /var/lib/lynxdb && sudo chown lynxdb:lynxdb /var/lib/lynxdb
sudo systemctl daemon-reload
sudo systemctl enable --now lynxdb
```

### Docker

```bash
docker run -d --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  ghcr.io/lynxbase/lynxdb server --addr 0.0.0.0:3100 --data-dir /data
```

See [Single-node deployment](../deployment/single-node.md) and
[Docker deployment](../deployment/docker.md).

## Configuration

Create a config file with every setting commented out:

```bash
lynxdb config init
```

It is written to `~/.config/lynxdb/config.yaml`. Uncomment the lines to
change, for example:

```yaml
listen: "localhost:3100"
data_dir: "~/.local/share/lynxdb"
retention: "7d"
log_level: "info"

storage:
  compression: "lz4"
```

`lynxdb config validate` checks the file and `lynxdb config reload` applies
it to a running server. See the
[Configuration Reference](../configuration/overview.md).

## Next steps

- [Your First Query](./first-query.md): PPL basics
- [Ingest Data](../guides/ingest-data.md): every ingestion path
- [Configuration](../configuration/overview.md): tune the server
- [Single-node deployment](../deployment/single-node.md): production setup
