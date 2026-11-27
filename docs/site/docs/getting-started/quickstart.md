---
sidebar_position: 1
title: LynxDB quick start
sidebar_label: Quick start
description: Count HTTP errors in a local file, then run the same PPL query against a LynxDB server.
---

# Quick start

Count HTTP 5xx responses by service in a sample log file. Then store the
same events on a server and query them again.

## Install

These examples use the PPL syntax in the current source tree. Build from
the [LynxDB repository](https://github.com/lynxbase/lynxdb) with Go 1.26 or later.

```bash
go build -o lynxdb ./cmd/lynxdb
./lynxdb version
```

See [Installation](./install.md) for release binaries, Homebrew, and Docker.
Use `lynxdb` in the examples if you installed it on your `PATH`.

## Query a local file

Create `access.ndjson` with one JSON object per line.

```bash
cat > access.ndjson <<'NDJSON'
{"status":200,"service":"api"}
{"status":503,"service":"api"}
{"status":502,"service":"api"}
NDJSON
```

Run this query in the same directory.

```bash
./lynxdb query --file access.ndjson '
  source=main
  | where status >= 500
  | stats count() as errors by service' \
  --format csv --no-stats
```

```csv
errors,service
2,api
```

`--file` loads the input into a temporary index named `main`. The CLI deletes
it on exit. See [pipe mode](./pipe-mode.md) to query stdin or a set of files.

## Start a server

Start LynxDB in one terminal. `--data-dir` keeps the tutorial's data in a
separate directory. The default HTTP address is `localhost:3100`.

```bash
./lynxdb server --data-dir ./quickstart-data
```

In a second terminal, send the sample file to the `web` index.

```bash
curl -fsS 'http://localhost:3100/api/v1/ingest?index=web' \
  -H 'Content-Type: application/x-ndjson' \
  --data-binary @access.ndjson
```

Run the same aggregation against `web`. With no file or pipe input, the
CLI sends the query to the server.

```bash
./lynxdb query '
  source=web
  | where status >= 500
  | stats count() as errors by service' \
  --format csv --no-stats
```

The result is again `2,api` after a single upload. Stop the server with
`Ctrl+C` when you're done. The events stay in `quickstart-data`.

## Read a query

`source=web` selects an index. `where status >= 500` keeps matching events.
`stats count() as errors by service` groups them by service and names the
count column `errors`.

## Next steps

- [Your first query](./first-query.md) covers search, sorting, and field extraction.
- [Server mode](./server-mode.md) covers storage and service setup.
- [Ingest data](../guides/ingest-data.md) lists protocols and shipper settings.
