# LynxDB

<div align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="docs/assets/lynxdb-logo-transparent-w.png">
    <source media="(prefers-color-scheme: light)" srcset="docs/assets/lynxdb-logo-transparent.png">
    <img alt="LynxDB logo" src="docs/assets/lynxdb-logo-transparent.png" height="300">
  </picture>
</div>

<p align="center">
  <a href="https://docs.lynxdb.org/"><img src="https://img.shields.io/badge/Docs-blue.svg?style=for-the-badge&logo=gitbook&logoColor=white" alt="Documentation"></a>
  <a href="https://github.com/lynxbase/lynxdb/releases"><img src="https://img.shields.io/github/v/release/lynxbase/lynxdb?style=for-the-badge&color=brightgreen&display_name=tag" alt="Latest Release"></a>
  <a href="https://github.com/lynxbase/lynxdb/actions/workflows/ci.yaml"><img src="https://img.shields.io/github/actions/workflow/status/lynxbase/lynxdb/ci.yaml?branch=main&style=for-the-badge&label=build" alt="Build Status"></a>
  <a href="https://discord.gg/RgggCFdgWK"><img src="https://img.shields.io/badge/discord-5865F2.svg?style=for-the-badge&logo=discord&logoColor=white" alt="Join Discord"></a>
</p>

LynxDB is a single-binary log analytics database. It works as a command-line
tool over files and pipes and as a persistent server. Queries are written in
OpenSearch Piped Processing Language (PPL).

> LynxDB is in active development. APIs, storage format, and query behavior may
> change between releases.

## Why LynxDB

- **No server needed:** run PPL over stdin or local files with no daemon.
- **Server mode:** ingest logs once into columnar storage, one store per
  index partitioned by time, and query them repeatedly.
- **OpenSearch PPL:** search terms, `where`, `eval`, `stats`, `timechart`,
  `rex`, `spath`, `join`, `lookup` and the PPL function library, compiled
  to a language-independent IR and optimized before it runs.
- **Explicit support:** every PPL command and function is either supported,
  supported with listed limits, or rejected with an error that says why.
- **Drop-in ingest paths:** Elasticsearch `_bulk`, OpenTelemetry OTLP, Splunk
  HEC, syslog, and raw HTTP ingest.

## Install

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

Other options:

```bash
brew install lynxbase/tap/lynxdb
go install github.com/lynxbase/lynxdb/cmd/lynxdb@latest
docker run -p 3100:3100 ghcr.io/lynxbase/lynxdb server
```

## Query without a server

Pipe logs into `lynxdb query`. The input is loaded into an index named `main`
in a temporary directory, deleted on exit. JSON lines keep their fields; text
lines get their timestamp, `key=value` pairs and the fields of combined and
syslog lines extracted while loading.

```bash
kubectl logs deploy/api | lynxdb query '
  source=main
  | where status >= 500
  | stats count() as errors, p95(duration_ms) as p95_ms by service
  | sort - errors
  | head 10'
```

Query a local file directly:

```bash
lynxdb query --file access.log '
  source=main status>=500
  | stats count() as hits, dc(client_ip) as unique_ips by uri
  | sort - hits
  | head 20'
```

See which fields a file has:

```bash
lynxdb query --file app.log 'describe main'
```

## Run a server

```bash
lynxdb server
lynxdb ingest access.log --index web

lynxdb query '
  source=web status>=500 earliest=-1h
  | timechart span=5m count() by path'
```

Search terms after `source=` filter events: free-text words and quoted
phrases match the words of the event's text, and `field=value` terms compare
fields. Every block of stored events carries token filters, so a search reads
only the blocks that can hold its words.

## PPL in 60 seconds

```
source=main "timeout" status>=500 earliest=-24h
| rex field=uri "(?<route>/api/v[0-9]+/[a-z]+)"
| eval slow = response_time > 500
| stats count() as n,
        count(eval(slow)) as slow_requests,
        p95(response_time) as p95_ms
  by service, route
| sort - n
| head 20
```

Conditional aggregation and ratios:

```
source=main earliest=-1h
| stats count(eval(status >= 500)) as errors, count() as total by service
| eval error_rate = round(errors * 100.0 / total, 1)
| sort - error_rate
```

Joins and subsearches:

```
source=auth action=login_failed earliest=-24h
| stats count() as failures by src_ip
| join left=f right=t on f.src_ip = t.client_ip [ source=threat_feed | fields client_ip, threat_type ]
| sort - failures
```

```
source=main
| where user IN [ source=main | where status = 503 | fields user ]
| stats count() as requests by user
```

Write spaces around `-` and `*` in arithmetic: as in the official PPL grammar,
`a-b` and `a*b` are field names.

## Features

- **OpenSearch PPL** with structured errors (`SYNTAX_ERROR`,
  `SEMANTIC_ERROR`, `TYPE_ERROR`, `UNSUPPORTED_COMMAND`,
  `UNSUPPORTED_FUNCTION`, `UNSUPPORTED_FEATURE`, ...) that carry a line,
  a column and a suggestion.
- **Canonical IR** between the language and the engine, with a rule-based
  optimizer whose time ranges, search terms, column pruning and row limits
  reach the scans.
- **Columnar execution:** filters, computed columns, aggregates, sorts and
  limits run over the column blocks of the store on every core; row
  operators run the rest of PPL.
- **Operational modes:** stdin/file mode, local server, REST API, syslog,
  and shipper-compatible ingest.

## Comparison

| | LynxDB | Elasticsearch | Loki | ClickHouse |
|---|---|---|---|---|
| Deployment | Single binary, single node | Single node or cluster | Single binary or microservices | Single binary or cluster |
| Dependencies | None | JVM | Object storage in production | Keeper for replication |
| Query files without a server | Yes | No | No | Yes |
| Schema | Schema-on-read | Schema-on-write | Labels + line | Schema-on-write |
| Full-text index | Token filters per block | Lucene | Label index only | Data-skipping indexes |
| License | Apache 2.0 | ELv2 / AGPL | AGPL | Apache 2.0 |

## CLI map

```text
lynxdb query <query>         run a PPL query (server, --file, or stdin)
lynxdb explain <query>       show the logical and optimized plans
lynxdb fields [name]         inspect the field catalog
lynxdb ingest <file>         ingest local files into a server
lynxdb import <file>         import NDJSON, CSV or Elasticsearch bulk files
lynxdb server                start the server
lynxdb status | health | indexes | jobs
lynxdb config                inspect and edit configuration
lynxdb auth | login | logout manage API keys and credentials
```

Run `lynxdb <command> --help` or see [docs/site/docs/cli/overview.md](docs/site/docs/cli/overview.md)
for every command and flag.

## Configuration

No configuration is required for file and pipe queries or local use. Server
defaults are documented in [docs/site/docs/configuration](docs/site/docs/configuration/overview.md).

Common overrides:

```bash
lynxdb server --data-dir /var/lib/lynxdb --addr 0.0.0.0:3100
LYNXDB_SERVER=http://localhost:3100 lynxdb query 'source=main | stats count()'
lynxdb config init
```

## Documentation

- [What is LynxDB?](docs/site/docs/intro.md)
- [Quick Start](docs/site/docs/getting-started/quickstart.md)
- [PPL overview](docs/site/docs/ppl/overview.md)
- [PPL commands](docs/site/docs/ppl/commands.md)
- [PPL functions](docs/site/docs/ppl/functions.md)
- [Supported features](docs/site/docs/ppl/feature-matrix.md)

## Contributing

Contributor workflow and PR guidelines live in [CONTRIBUTING.md](CONTRIBUTING.md).

## Feedback

- [Issues](https://github.com/lynxbase/lynxdb/issues)
- [Discord](https://discord.gg/RgggCFdgWK)

---

LynxDB would not exist without the projects that inspired it:

- **[OpenSearch](https://opensearch.org/)**: for the Piped Processing Language.
- **[ClickHouse](https://clickhouse.com/)**: for showing how much analytical
  performance a focused engine can deliver.
- **[VictoriaLogs](https://docs.victoriametrics.com/victorialogs/)**: for
  proving that operational log storage can be simple and efficient.
- **`grep`, `awk`, `sed`, `jq`**: for the Unix style of composable data tools.

## Star History

<a href="https://www.star-history.com/?repos=lynxbase%2Flynxdb&type=date&legend=top-left">
 <picture>
   <source media="(prefers-color-scheme: dark)" srcset="https://api.star-history.com/chart?repos=lynxbase/lynxdb&type=date&theme=dark&legend=top-left" />
   <source media="(prefers-color-scheme: light)" srcset="https://api.star-history.com/chart?repos=lynxbase/lynxdb&type=date&legend=top-left" />
   <img alt="Star History Chart" src="https://api.star-history.com/chart?repos=lynxbase/lynxdb&type=date&legend=top-left" />
 </picture>
</a>

## License

[Apache 2.0](LICENSE)
