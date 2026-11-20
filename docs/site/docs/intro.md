---
sidebar_position: 0
slug: /
title: What is LynxDB?
description: Query log files and standard input with OpenSearch PPL, or store logs on a LynxDB server.
---

# What is LynxDB?

LynxDB is an open-source log analytics database written in Go. The same
binary queries local files and standard input or runs as a standalone server.
Both modes use [OpenSearch Piped Processing Language (PPL)](./ppl/overview.md).

## One binary, two ways to run it

| To do this | Use |
| --- | --- |
| Inspect a log file or aggregate the output of `kubectl logs` | [Pipe mode](./getting-started/pipe-mode.md) |
| Ingest logs continuously and query them over HTTP | [Server mode](./getting-started/server-mode.md) |

### Files and pipes

```bash
lynxdb query --file access.log 'source=main status>=500 | top 10 uri'
```

The CLI loads the input into a temporary index named `main` and removes it
when the command exits. It keeps JSON fields and extracts timestamps,
`key=value` pairs, and fields from formats such as combined access logs
and syslog.

### Server

Start `lynxdb server` in one terminal. In another terminal, ingest a file and
query the stored events.

```bash
lynxdb ingest access.log
lynxdb query 'source=main status>=500 | top 10 uri'
```

Each index is a columnar store partitioned by time. Token filters let the
server skip blocks that cannot match a text search. Besides the CLI, the
server accepts logs through [NDJSON, Elasticsearch `_bulk`, OTLP, Splunk HEC,
and syslog](./guides/ingest-data.md).

## The query language

```text
source=main level=ERROR earliest=-1h
| stats count() as errors, p95(response_time) as p95 by host
| sort - errors
```

A query names an index, then pipes events through commands such as
`where`, `stats`, and `sort`. LynxDB compiles PPL to an intermediate
representation, optimizes the plan, and runs it on the columnar engine.
See [supported features](./ppl/feature-matrix.md) for command and function
coverage, or [PPL errors](./ppl/errors.md) for diagnostics.

## Next steps

- [Quick start](./getting-started/quickstart.md) to run a query on sample data.
- [CLI reference](./cli/overview.md) for commands and output formats.
- [REST API](./api/overview.md) for HTTP clients.
- [Architecture](./architecture/overview.md) for storage and execution.
