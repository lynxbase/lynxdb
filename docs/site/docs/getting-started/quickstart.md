---
sidebar_position: 1
title: Quick Start
description: Install LynxDB and run your first PPL query in a few minutes.
---

# Quick Start

LynxDB is a log analytics database in a single binary. You query it with
[OpenSearch PPL](../ppl/overview.md), either on local files with no server or
on a running LynxDB server.

## Install

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="curl" label="Install script (Linux/macOS)" default>

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

</TabItem>
<TabItem value="brew" label="Homebrew">

```bash
brew install lynxbase/tap/lynxdb
```

</TabItem>
<TabItem value="docker" label="Docker">

```bash
docker run -p 3100:3100 ghcr.io/lynxbase/lynxdb server --addr 0.0.0.0:3100
```

</TabItem>
<TabItem value="source" label="Go">

```bash
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest
```

</TabItem>
</Tabs>

Check the binary:

```bash
lynxdb version
```

See [Installation](./install.md) for all options.

## Query a local file

`lynxdb query --file` loads a file into an in-process index named `main`,
runs the query and exits. Nothing is written to disk.

Create a small NDJSON file:

```bash
cat > events.ndjson <<'EOF'
{"_time":"2026-01-15T00:00:00Z","level":"error","status":500,"duration_ms":120,"message":"failed"}
{"_time":"2026-01-15T00:01:00Z","level":"info","status":200,"duration_ms":15,"message":"ok"}
{"_time":"2026-01-15T00:02:00Z","level":"error","status":503,"duration_ms":250,"message":"timeout"}
EOF
```

Count events by level:

```bash
lynxdb query --file events.ndjson 'source=main | stats count() as n by level | sort level' --format ndjson --no-stats
```

```json
{"level":"error","n":2}
{"level":"info","n":1}
```

Filter and pick columns:

```bash
lynxdb query --file events.ndjson 'source=main | where status >= 500 | fields level, status, message' --format csv --no-stats
```

```text
level,message,status
error,failed,500
error,timeout,503
```

Any log file or command output works the same way:

```bash
# A local nginx access log; combined-format fields are extracted on load
lynxdb query --file /var/log/nginx/access.log 'source=main status>=500 | top 10 uri'

# Standard input
kubectl logs deploy/api | lynxdb query 'source=main level=error | stats count() by msg'
```

See [Pipe Mode](./pipe-mode.md) for formats, globs and output options.

## Start a server

A server stores events on disk and answers queries over HTTP on port 3100.

```bash
lynxdb server
```

In another terminal, send some events and query them:

```bash
# A structured event through the REST API
curl -X POST localhost:3100/api/v1/ingest \
  -H 'Content-Type: application/json' \
  -d '[{"event":"hello from lynxdb","source":"quickstart","fields":{"level":"info","service":"demo"}}]'

# A log file through the CLI
lynxdb ingest access.log --source web-01

# Query the server
lynxdb query 'source=main level=info | stats count() by service'
```

See [Server Mode](./server-mode.md) for storage, ingestion and service setup.

## Read a query

```
source=main status>=500
| stats count() as errors, avg(duration_ms) as avg_ms by uri
| sort - errors
| head 10
```

- `source=main status>=500` reads the index `main` and keeps events whose
  `status` is 500 or more.
- `stats` counts the events and averages `duration_ms` for each `uri`.
- `sort - errors` orders by the `errors` column, largest first.
- `head 10` keeps the first ten rows.

[Your First Query](./first-query.md) walks through the basic commands.

## Next steps

- [Pipe Mode](./pipe-mode.md): query files and standard input without a server
- [Server Mode](./server-mode.md): persistent storage and the REST API
- [Your First Query](./first-query.md): PPL basics
- [PPL overview](../ppl/overview.md): the query language reference
