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

# A temporary data directory, deleted when the server exits (for tests)
lynxdb server --data-dir ""
```

On startup the server prints where its settings come from:

```text
  Config  /home/user/.config/lynxdb/config.yaml
  Data    /var/lib/lynxdb
  Listen  localhost:3100

time=2026-11-17T16:41:03.285+01:00 level=INFO msg="starting LynxDB" version=dev addr=localhost:3100 config=/home/user/.config/lynxdb/config.yaml
time=2026-11-17T16:41:03.287+01:00 level=INFO msg="API server started" addr=127.0.0.1:3100
```

An `Overrides` line lists the environment variables and flags that changed
settings. The server also starts an OTLP/HTTP log receiver on port 4318. See
[Server configuration](../configuration/server.md) for every flag and setting.

Each index is a directory under `indexes/` in the data directory, created with the
index's first events. See [Storage](../architecture/storage.md) for what the
files hold.

:::caution
`--auth` needs a data directory: API keys are stored in it. With
`--data-dir ""` the server runs without authentication.
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
# Structured events: NDJSON, one JSON object per line, stored as they are
printf '%s\n' \
  '{"message":"user login","source":"auth-api","user_id":42,"level":"info"}' \
  '{"message":"connection refused","source":"api","service":"redis","level":"error"}' |
  curl -X POST localhost:3100/api/v1/ingest --data-binary @-

# Raw lines, one event per line
echo '192.168.1.1 - - [14/Feb/2026:14:23:01 +0000] "GET /api HTTP/1.1" 200 1234' | \
  curl -X POST localhost:3100/api/v1/ingest/raw \
    -H 'X-Source: nginx' \
    --data-binary @-
```

```json
{"data":{"accepted":2,"failed":0,"expired":0}}
```

`/api/v1/ingest` writes to the index in `?index=` or the `X-Index` header,
`main` by default. `/api/v1/ingest/raw` extracts the leading timestamp and
`key=value` pairs of text lines and keeps the text in `_raw`. The server
writes ingested events to disk within a second (`storage.flush_interval`),
and before every query, so a query sees every event ingested before it.

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
  -d '{"q": "source=main level=ERROR | stats count() as errors by host | sort - errors | head 3"}'
```

```json
{"data":{"type":"aggregate","columns":["errors","host"],"rows":[[67,"api-02"],[58,"web-02"],[57,"web-03"]],"total_rows":3,"has_more":false},"meta":{"took_ms":0.1,"query_id":"qry_a3938b4846474ae2","rows":3}}
```

`POST /api/v1/query/stream` returns one JSON object per row, then a
`__meta` line:

```bash
curl -s -X POST localhost:3100/api/v1/query/stream \
  -H 'Content-Type: application/json' \
  -d '{"q": "source=main level=ERROR | sort _time | fields _time, host, status | head 2"}'
```

```json
{"_time":"2025-01-15T08:00:00.105Z","host":"web-03","status":502}
{"_time":"2025-01-15T08:00:00.543Z","host":"api-02","status":502}
{"__meta":{"total":2,"took_ms":0}}
```

See the [Query API](../api/query.md) for jobs and errors.

## Inspect the server

```bash
lynxdb status        # uptime, health, storage, events, parts, indexes; non-zero exit when unhealthy
lynxdb indexes       # indexes with their events, bytes, parts and time span
lynxdb fields        # field catalog: each field and the indexes that hold it
```

```text
  LynxDB dev — uptime 0m — healthy

  Storage  48.6 KB
  Events  1,036 total
  Parts  3
  Indexes  2
  Indexes main (97%), web (3%)
  Oldest  2025-01-15T08:00:00Z
  Newest  2026-11-17T15:54:45Z
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
  partition: "24h"
```

`lynxdb config validate` checks the file and `lynxdb config reload` applies
it to a running server. See the
[Configuration Reference](../configuration/overview.md).

## Next steps

- [Your First Query](./first-query.md): PPL basics
- [Ingest Data](../guides/ingest-data.md): every ingestion path
- [Configuration](../configuration/overview.md): tune the server
- [Single-node deployment](../deployment/single-node.md): production setup
