# LynxDB

<img src="docs/assets/lynxdb-icon.png" alt="" width="64" align="right">

LynxDB is a log analytics database in one binary. Query local files or stdin,
or run a server that stores logs on disk. Both modes use
[OpenSearch Piped Processing Language (PPL)](https://docs.lynxdb.org/docs/ppl/overview).

[Documentation](https://docs.lynxdb.org/) ·
[Releases](https://github.com/lynxbase/lynxdb/releases) ·
[Benchmarks](BENCHMARKS.md) ·
[Discord](https://discord.gg/RgggCFdgWK)

> LynxDB is in active development. APIs, the storage format, and query behavior
> may change between releases.

## Install

Build the CLI from this checkout with Go 1.26 or later.

```bash
go build -o lynxdb ./cmd/lynxdb
```

For downloadable binaries, Homebrew, and Docker, see the
[installation guide](https://docs.lynxdb.org/docs/getting-started/install).

## Query logs locally

Count HTTP 5xx responses by service. `--file` loads files, or standard
input for `-`, into the temporary index `main`, which a local query reads
when it names no source.

```bash
printf '%s\n' \
  '{"status":200,"service":"api"}' \
  '{"status":503,"service":"api"}' \
  '{"status":502,"service":"api"}' |
  ./lynxdb query --file - '
    where status >= 500
    | stats count() as errors by service' \
    --format csv --no-stats
```

```csv
errors,service
2,api
```

The CLI keeps JSON fields and extracts timestamps, `key=value` pairs, and
fields from common text formats such as combined access logs and syslog.
See [pipe mode](https://docs.lynxdb.org/docs/getting-started/pipe-mode)
for input formats and shell examples.

## Store logs on a server

Start the server from the repository root.

```bash
./lynxdb server
```

In a second terminal, ingest a local file and query the `web` index.

```bash
./lynxdb ingest access.log --index web
./lynxdb query 'source=web | stats count() as events'
```

The server listens on `localhost:3100` by default. It stores events in
time-partitioned columnar indexes. Token filters let searches skip blocks
that cannot match. Ingest endpoints accept NDJSON, Elasticsearch `_bulk`,
OpenTelemetry logs, Splunk HEC, and syslog.

## Documentation

| Task | Guide |
| --- | --- |
| Write a query or look up a command | [PPL guide](https://docs.lynxdb.org/docs/ppl/overview) · [Supported features](https://docs.lynxdb.org/docs/ppl/feature-matrix) |
| Send logs from a shipper | [Ingest data](https://docs.lynxdb.org/docs/guides/ingest-data) |
| Set up a persistent server | [Single-node deployment](https://docs.lynxdb.org/docs/deployment/single-node) · [Configuration](https://docs.lynxdb.org/docs/configuration/overview) |
| Automate queries and ingestion | [CLI](https://docs.lynxdb.org/docs/cli/overview) · [REST API](https://docs.lynxdb.org/docs/api/overview) |
| Read about the engine and storage | [Architecture](https://docs.lynxdb.org/docs/architecture/overview) |

## Contributing

See the [contributor guide](CONTRIBUTING.md), the [changelog](CHANGELOG.md),
and [GitHub issues](https://github.com/lynxbase/lynxdb/issues).

LynxDB draws on [OpenSearch](https://opensearch.org/) for PPL,
[ClickHouse](https://clickhouse.com/) for analytical execution,
[VictoriaLogs](https://docs.victoriametrics.com/victorialogs/) for log storage,
and Unix tools such as `grep`, `awk`, and `jq` for shell composition.

[Apache License 2.0](LICENSE)
