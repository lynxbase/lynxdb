---
sidebar_position: 1
title: Quick Start
description: Install LynxDB and run your first query in 5 minutes.
---

# Quick Start

Get from zero to your first log analytics query in under 5 minutes.

## Install

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<Tabs>
<TabItem value="curl" label="curl (Linux/macOS)" default>

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
docker run -p 3100:3100 ghcr.io/lynxbase/lynxdb server
```

</TabItem>
<TabItem value="source" label="From Source">

```bash
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest
```

</TabItem>
</Tabs>

Verify the installation:

```bash
lynxdb version
```

## Option 1: Query Local Files (No Server)

The fastest way to try LynxDB -- query any log file without starting a server:

```bash
# Query a local log file
lynxdb query --file /var/log/syslog '| stats count by level'

# Pipe from any command
kubectl logs deploy/api | lynxdb query '| stats avg(duration_ms) by endpoint'

# Query nginx access logs
lynxdb query --file '/var/log/nginx/*.log' '| where status>=500 | top 10 uri'
```

## Option 2: Run the Built-in Demo

Start the demo to generate realistic log data from 4 sources:

```bash
# Terminal 1: Start the demo (generates 200 events/sec)
lynxdb demo
```

```bash
# Terminal 2: Query the demo data
lynxdb query 'source=nginx status>=500
  | stats count, avg(duration_ms) as avg_lat by uri
  | sort -count
  | head 5'
```

```bash
# Live tail errors
lynxdb tail 'level=error'
```

## Option 3: Start a Server

For persistent storage and the full API:

```bash
# Start the server
lynxdb server &

# Ingest some data
echo '{"message": "hello from lynxdb", "level": "info"}' | \
  curl -X POST localhost:3100/api/v1/ingest -d @-

# Or ingest a log file
lynxdb ingest access.log --source web-01

# Query it
lynxdb query 'level=info | stats count'
```

## Your First SPL2 Query

SPL2 is a pipeline language. Data flows left to right through `|` (pipe) operators:

```spl
source=nginx status>=500
  | stats count, avg(duration_ms) by uri
  | sort -count
  | head 10
```

This reads as: "From nginx logs where status is 500+, count events and average duration by URI, sort by count descending, take top 10."

:::tip
If your query starts with `|`, LynxDB automatically prepends `FROM main` -- so `| stats count` is the same as `FROM main | stats count`.
:::

## Next Steps

- **[Pipe Mode](/docs/getting-started/pipe-mode)** -- Master serverless querying
- **[Server Mode](/docs/getting-started/server-mode)** -- Set up persistent storage
- **[Your First SPL2 Query](/docs/getting-started/first-query)** -- SPL2 crash course
- **[SPL2 Reference](/docs/spl2/overview)** -- Full language reference
