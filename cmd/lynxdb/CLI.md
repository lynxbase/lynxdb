# LynxDB CLI Reference

`lynxdb` is one binary: the LynxDB server and its command-line client. Queries are written in OpenSearch PPL (Piped Processing Language) and compiled by `pkg/query`.

```
lynxdb [command] [flags]
```

User documentation lives in `docs/site/docs/cli/`; the query language is described in `docs/site/docs/ppl/`.

## Contents

- [Global flags](#global-flags)
- [Connection settings](#connection-settings)
- [Root command](#root-command)
- Query: [query](#query), [explain](#explain), [tail](#tail), [fields](#fields)
- Load data: [ingest](#ingest), [import](#import)
- Server: [server](#server)
- Inspect: [status](#status), [health](#health), [indexes](#indexes), [cache](#cache), [jobs](#jobs)
- Authentication: [login](#login), [logout](#logout), [auth](#auth)
- Configuration: [config](#config)
- Utilities: [version](#version), [completion](#completion)
- [Output formats](#output-formats)
- [Exit codes](#exit-codes)
- [Signals](#signals)

## Global flags

Persistent flags on the root command (`root.go`):

| Flag | Short | Default | Env var | Description |
|------|-------|---------|---------|-------------|
| `--server` | | `http://localhost:3100` | `LYNXDB_SERVER` | Server address |
| `--token` | | | `LYNXDB_TOKEN` | API key for authentication |
| `--profile` | `-p` | | `LYNXDB_PROFILE` | Connection profile name |
| `--config` | | | | Config file path (`LYNXDB_CONFIG` when unset) |
| `--format` | `-F` | `auto` | | `auto`, `json`, `ndjson`, `table`, `box`, `ascii`, `markdown`, `vertical`, `line`, `G`, `csv`, `tsv`, `raw` |
| `--quiet` | `-q` | `false` | | Suppress non-data output, including error messages |
| `--verbose` | `-v` | `false` | | Show extra detail |
| `--no-stats` | | `false` | | Suppress query statistics |
| `--no-color` | | `false` | | Disable colored output |
| `--theme` | | `auto` | | Human output theme: `auto`, `dark`, `light`, `plain` |
| `--compact` | | `false` | | Compact human output |
| `--max-rows` | | `0` | | Maximum rows in human table output (`0` = all) |
| `--max-width` | | `0` | | Maximum width of human table output (`0` = terminal width) |
| `--null-value` | | | | Placeholder for null or empty values in human table output |
| `--debug` | | `false` | | Debug logging to stderr |
| `--tls-skip-verify` | | `false` | `LYNXDB_TLS_SKIP_VERIFY` | Skip TLS certificate verification (`true`, `1`, `yes`) |

`import` defines its own `--format` (input format) and `login` its own `--token`; they shadow the global flags on those commands.

## Connection settings

Server address, first match wins: `--server`, the URL of the selected profile, `server` in `.lynxdbrc`, `LYNXDB_SERVER`, `http://localhost:3100`.

API key: `--token`, then `LYNXDB_TOKEN`, then the credentials file written by `lynxdb login` (`<user config dir>/lynxdb/credentials.yaml`, keyed by server URL).

`.lynxdbrc` (nearest one in the current directory or a parent) sets client defaults that apply when the flag is absent:

```yaml
server: https://logs.example.com
profile: staging
default_format: table
default_since: 6h         # lynxdb query --since
default_source: web       # --source on query, ingest, import
```

## Root command

| Invocation | Behavior |
|------------|----------|
| `lynxdb` | Welcome screen with quick-start hints |
| `cat app.log \| lynxdb` | Loads stdin into index `main` and runs `source=main \| head 10` |
| `cat app.log \| lynxdb '<PPL query>'` | Loads stdin and runs the query, like `lynxdb query` in stdin mode |
| `lynxdb '<PPL query>'` without piped stdin | Error: a bare query needs piped stdin |

---

## query

```
lynxdb query '<PPL query>' [flags]
```

Alias `q`. Runs a PPL query. Arguments are joined with spaces. A trailing `\G` switches `--format auto` to vertical output.

**Modes**

- **Server** (default): `POST /api/v1/query` to `--server`. A query the server promotes to an asynchronous job (HTTP 202) is polled until it finishes or `--timeout` expires (default 6 minutes).
- **Local** (`--file`, or stdin is a pipe): the query is compiled first, then the input is loaded into an in-memory index `main` and the same compiled plan runs in process. Each line is one event. JSON objects and `key=value` pairs become fields, and a recognized timestamp becomes `_time`. The first 20 lines are checked for combined (Apache/Nginx), syslog and Docker JSON log formats, whose fields are extracted while loading; `--raw` disables that detection. `_source` is the file path, `stdin` or `--source`. `--file` and piped stdin together are an error.
- **Plan only** (`--explain`): compiles in the CLI and prints the logical plan, optimized plan and optimizer rules. Reads no input, needs no server.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--since` | `-s` | | Relative time range (`15m`, `1h`, `7d`); server mode |
| `--from` | | | Absolute start time (ISO 8601); needs `--to`; server mode |
| `--to` | | | Absolute end time (ISO 8601); needs `--from`; server mode |
| `--file` | `-f` | | Query a local file or glob instead of the server |
| `--sourcetype` | | | `_sourcetype` of loaded events; local mode |
| `--source` | | | `_source` of loaded events; local mode |
| `--output` | `-o` | | Write results to a file |
| `--timeout` | | `6m` | Query timeout (`10s`, `5m`); server mode |
| `--analyze` | | | Execution profile: `basic` (bare flag), `full`, `trace` |
| `--max-memory` | | | Memory limit of a local query (`512mb`, `1gb`) |
| `--fail-on-empty` | | `false` | Exit with code 6 when there are no rows |
| `--copy` | | `false` | Copy results to the clipboard as TSV; server mode |
| `--explain` | | `false` | Print the compiled plans without running the query |
| `--raw` | | `false` | Disable log format detection; local mode |
| `--no-lint` | | `false` | Disable advisory query lints; server mode |
| `--no-suggestions` | | `false` | Disable advisory query suggestions; server mode |

`--profile-query` is a hidden alias of `--analyze`.

```bash
# Server
lynxdb query 'source=main level=ERROR | stats count() as errors by host | sort - errors'
lynxdb query 'source=main status>=500 | top 10 path' --since 24h
lynxdb query 'source=main | stats count() by host' --from 2026-01-15T00:00:00Z --to 2026-01-16T00:00:00Z
lynxdb q 'source=main | timechart span=5m count() by level' --since 1h
lynxdb query 'source=main | stats p95(response_time) as p95, dc(user) as users by host' --format table

# Local
lynxdb query --file access.log 'source=main | stats count() by status'
lynxdb query --file '/var/log/app/*.log' 'source=main level=ERROR | stats count() by _source'
cat app.log | lynxdb query 'source=main | where response_time > 500 | stats count() by path'
lynxdb query --raw --file app.log 'source=main "connection refused" | head 20'

# Plans and profiles
lynxdb query --explain 'source=main level=ERROR | stats count() as n by host | sort - n | head 3'
lynxdb query 'source=main level=ERROR | stats count() by host' --analyze full

# Scripting
lynxdb query 'source=main level=FATAL' --since 15m --fail-on-empty --quiet < /dev/null > /dev/null
lynxdb query 'source=main | stats count() by host' --format csv > hosts.csv
lynxdb query 'source=main status>=500 | fields _raw' --since 1h --format raw \
  | lynxdb query 'source=main | stats count() by path'
```

Output: rows go to stdout, statistics and lints to stderr. On a terminal the statistics are a summary footer (rows, time, scanned, skipped segments, bytes read); otherwise one JSON line. A query that does not compile prints the query with a caret under the error position and exits with 4.

## explain

```
lynxdb explain '<PPL query>' [flags]
```

Compiles the query on the server (`GET /api/v1/query/explain`) and prints the logical plan, the optimized plan, the optimizer rules that fired, and scan hints: result type, cost class, search terms, fields read and source scope.

| Flag | Default | Description |
|------|---------|-------------|
| `--analyze` | `false` | Run the query with full profiling and print the profile after the plan |

```bash
lynxdb explain 'source=main level=ERROR | stats count() by host'
lynxdb explain 'source=main status>=500 | top 10 path' --format json
lynxdb explain --analyze 'source=main level=ERROR | stats count()'
```

## tail

```
lynxdb tail ['<PPL query>'] [flags]
```

Alias `t`. Streams matching events over Server-Sent Events (`GET /api/v1/tail`). The argument is a full PPL query; without one the CLI tails `source=*`. The query is compiled on the server first. The server sends up to `--count` historical events, a catch-up marker, then live events.

Only row-by-row commands are allowed: search terms and comparisons in the source command, `where`, `eval`, `fields`, `rename`, `parse`, `rex`, `grok`, `spath`, `dedup`, `head`. Commands that need all input rows (`stats`, `timechart`, `chart`, `top`, `rare`, `sort`, `reverse`, `eventstats`, `streamstats`, `trendline`, `join`, `lookup`, `append`, `union`, `multisearch`, `appendcol`, `transpose`, `addtotals`, `addcoltotals`, `mvcombine`, `patterns`, `describe`) are rejected with HTTP 422.

| Flag | Short | Default | Description |
|------|-------|---------|-------------|
| `--count` | `-n` | `100` | Historical events before live streaming (server cap 10000) |
| `--from` | | `-1h` | Historical lookback |

```bash
lynxdb tail
lynxdb tail 'source=main level=ERROR'
lynxdb tail 'source=main host=web-* status>=500 | fields _time, host, path, status'
lynxdb tail 'source=main | where response_time > 500' --count 50 --from -15m
lynxdb tail 'source=main level=ERROR' --format json
```

## fields

```
lynxdb fields [name] [flags]
```

Alias `f`. Field catalog from the server: name, type, coverage and top values. With a name, details for that field; with `--values`, its top 50 values.

| Flag | Short | Description |
|------|-------|-------------|
| `--values` | | Top values for the named field |
| `--since` | `-s` | Relative time range |
| `--from` | | Absolute start time (ISO 8601) |
| `--to` | | Absolute end time (ISO 8601) |
| `--source` | | Only fields seen from this source |
| `--prefix` | | Only fields whose names start with this prefix |

```bash
lynxdb fields
lynxdb fields status --values
lynxdb fields --prefix st --since 1h
```

---

## ingest

```
lynxdb ingest [file] [flags]
```

Alias `i`. Sends raw lines from a file, or stdin, to `POST /api/v1/ingest/raw` in batches. Empty lines are skipped.

| Flag | Default | Description |
|------|---------|-------------|
| `--source` | | Source of the events (server default `http`) |
| `--sourcetype` | | Sourcetype of the events (server default `raw`) |
| `--index` | | Target index (server default `main`) |
| `--batch-size` | `5000` | Lines per batch, 1 to 1,000,000 |

```bash
lynxdb ingest access.log --source web-01 --sourcetype nginx
cat app.log | lynxdb ingest --index production
```

## import

```
lynxdb import <file> [flags]
```

Imports structured data. `-` reads stdin and needs `--format`. With `auto`, the extension decides (`.csv`/`.tsv` CSV, `.ndjson`/`.jsonl` NDJSON); for `.json`, `.log` and no extension, the first line decides.

- `ndjson`: one object per line; an `event` envelope (`event`, `time`, `source`, `sourcetype`, `host`, `index`, `fields`) or a flat object (`_raw` or the line as raw text; timestamp from `time`, `_time`, `timestamp`, `_timestamp`, `@timestamp`, `ts` or `datetime`; scalar values as fields). Sent to `POST /api/v1/ingest`.
- `csv`: header row, then one event per row; numbers are typed, `_time` and `_raw` columns are recognized. Sent to `POST /api/v1/ingest`.
- `esbulk`: Elasticsearch `_bulk` action and document pairs, sent to `POST /api/v1/es/_bulk`. `--index` is rejected; `--source` replaces each action's `_index`.

| Flag | Default | Description |
|------|---------|-------------|
| `--format` | `auto` | `auto`, `ndjson`, `csv`, `esbulk` |
| `--source` | | Source for events that have none |
| `--index` | | Index for events that have none |
| `--batch-size` | `5000` | Events per batch |
| `--dry-run` | `false` | Validate and count without sending |
| `--delimiter` | `,` | CSV delimiter (`'\t'` for tab) |

```bash
lynxdb import events.ndjson
lynxdb import export.csv --source web-01 --index nginx
lynxdb import es_dump.json --format esbulk
cat events.ndjson | lynxdb import - --format ndjson
```

---

## server

```
lynxdb server [flags]
```

Starts the server. Settings come from the config file, then `LYNXDB_*` environment variables, then these flags.

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | `localhost:3100` | Listen address |
| `--data-dir` | `~/.local/share/lynxdb` | Data directory; `""` for in-memory |
| `--s3-bucket` | | S3 bucket for warm/cold storage |
| `--s3-region` | | AWS region |
| `--s3-prefix` | | Key prefix in S3 |
| `--compaction-interval` | | Compaction check interval |
| `--tiering-interval` | | Tier evaluation interval |
| `--cache-max-mb` | | Query cache size (`512mb`, `1gb`) |
| `--log-level` | | `debug`, `info`, `warn`, `error` |
| `--auth` | `false` | Require API keys (needs a data directory) |
| `--tls` | `false` | Serve HTTPS; self-signed certificate in `<data-dir>/tls/` without `--tls-cert` |
| `--tls-cert` | | Certificate PEM file |
| `--tls-key` | | Private key PEM file |
| `--max-query-pool` | | Global query memory pool (`2gb`, `4gb`) |
| `--spill-dir` | OS temp dir | Directory for spill files |
| `--profile-runtime` | `false` | Mutex and block profiling (about 2-5% overhead) |
| `--syslog` | | UDP and TCP syslog address (port 5514 when omitted) |
| `--syslog-udp` | | UDP syslog address |
| `--syslog-tcp` | | TCP syslog address |
| `--syslog-tls` | `false` | TLS for TCP syslog (port 6514 when omitted) |
| `--syslog-parser` | | `auto`, `rfc5424`, `rfc3164`, `raw` |
| `--syslog-index` | | Index for syslog events |
| `--ingest-es-enabled` | `true` | Elasticsearch-compatible `_bulk` endpoint |
| `--ingest-es-version` | | Elasticsearch version advertised to clients |
| `--otlp-http-listen` | `0.0.0.0:4318` | OTLP/HTTP address; empty disables |
| `--otlp-grpc-listen` | | OTLP/gRPC address; empty disables |
| `--otlp-grpc-max-recv-bytes` | | Largest OTLP/gRPC message |
| `--ingest-max-compressed-body-bytes` | | Largest compressed shipper body |
| `--ingest-max-decompressed-body-bytes` | | Largest decompressed shipper body |
| `--ingest-staging-enabled` | `true` | Server-side shipper staging buffer |
| `--ingest-staging-max-bytes` | | Staging buffer size limit |
| `--ingest-staging-max-age` | | Staging buffer max age |
| `--cluster.enabled` | `false` | Cluster mode |
| `--cluster.node-id` | | Node ID |
| `--cluster.roles` | | `meta`, `ingest`, `query` (comma-separated) |
| `--cluster.seeds` | | Seed addresses (`host:port`, comma-separated) |
| `--cluster.grpc-port` | | Node-to-node gRPC port |

With `--auth` and no keys, the server generates a root key and prints it once. The PID is written to `<data-dir>/lynxdb.pid` (in-memory mode: `$XDG_RUNTIME_DIR` or the temp directory).

```bash
lynxdb server
lynxdb server --addr 0.0.0.0:3100 --data-dir /var/lib/lynxdb
lynxdb server --data-dir ""
lynxdb server --data-dir /var/lib/lynxdb --tls --auth
lynxdb server --syslog :514 --otlp-grpc-listen :4317
```

---

## status

```
lynxdb status
```

Alias `st`. Version, uptime, health, storage, event counts, segments, buffered events, indexes, top sources, oldest event and the query memory pool. `--format json` prints `uptime_seconds`, `storage_bytes`, `total_events`, `events_today`, `index_count`, `segment_count`, `buffered_events`, `oldest_event`, `health` and `sources`.

## health

```
lynxdb health
```

Calls `GET /health` and prints the status (`healthy`, `degraded`, `unhealthy`).

## indexes

```
lynxdb indexes
```

Lists indexes with retention and replication factor.

## cache

```
lynxdb cache stats
lynxdb cache clear [--force]
```

`stats` prints hits, misses, hit rate, entries, size and evictions of the query cache. `clear` empties it after confirmation; `--force` skips the prompt.

## jobs

```
lynxdb jobs [job_id] [flags]
```

Lists asynchronous query jobs, shows one, or cancels one.

| Flag | Default | Description |
|------|---------|-------------|
| `--status` | | Filter: `running`, `done`, `error` |
| `--cancel` | `false` | Cancel the given job |

```bash
lynxdb jobs --status running
lynxdb jobs qry_8c5ca731a36c88d7 --cancel
```

---

## login

```
lynxdb login [--token <key>]
```

Verifies the key against `--server` and saves it in the credentials file. Prompts for the key when stdin is a terminal and `--token` is absent. For an `https://` server with a certificate no system CA signed, shows its SHA-256 fingerprint, asks for trust and saves the fingerprint (trust on first use).

```bash
lynxdb login --server https://logs.example.com
lynxdb login --token "$LYNXDB_TOKEN"
```

## logout

```
lynxdb logout [--all]
```

Removes saved credentials for `--server`; `--all` removes them for every server.

## auth

Manage API keys on the server; needs an `admin` or `full` key.

| Subcommand | Flags | Description |
|------------|-------|-------------|
| `auth create` | `--name` (required), `--scope` (`full`; `ingest`, `query`, `admin`), `--expires` (`never`; `30d`, `90d`, `1y`, Go durations), `--description` | Create a key; the token is shown once |
| `auth list` (alias `ls`) | | List keys: ID, name, scope, expiry, last use, age |
| `auth revoke <id>` | `--yes`/`-y` | Revoke a key |
| `auth rotate-root` | `--yes`/`-y` | Replace the root key and update saved credentials |
| `auth status` | | Server, TLS mode and whether the current key authenticates |

```bash
lynxdb auth create --name shipper --scope ingest
lynxdb auth create --name dashboards --scope query --expires 90d --format json
lynxdb auth revoke ak_7fA3mNpQ --yes
```

---

## config

```
lynxdb config [subcommand] [flags]
```

Without a subcommand, prints every setting with its value and origin. `config` and `config validate` accept the override flags `--addr`, `--data-dir`, `--s3-bucket`, `--s3-region`, `--s3-prefix`, `--compaction-interval`, `--tiering-interval`, `--cache-max-mb`, `--log-level`.

Config file search, when `--config` is unset: `LYNXDB_CONFIG`, `./lynxdb.yaml`, `$XDG_CONFIG_HOME/lynxdb/config.yaml` (or `~/.config/lynxdb/config.yaml`), `~/.lynxdb/config.yaml`, `/etc/lynxdb/config.yaml`.

| Subcommand | Flags | Description |
|------------|-------|-------------|
| `config init` | `--system` | Write the default template (`/etc/lynxdb/config.yaml` with `--system`) |
| `config validate` | | Validate, warn on unknown keys, list non-default values |
| `config reload` | | Send `SIGHUP` to the server found through its PID file |
| `config get <key>` | | Print one value and its origin |
| `config set <key> <value>` | `--dry-run` | Write one value to the config file |
| `config edit` | | Open the config file in `$EDITOR` or `$VISUAL` |
| `config path` | | Print the config file path that `init`, `set`, `edit` and `reset` use |
| `config reset` | `--dry-run` | Overwrite the config file with defaults after typed confirmation |
| `config add-profile <name>` | `--url` (required), `--token` | Add or update a connection profile |
| `config list-profiles` | | List profiles |
| `config remove-profile <name>` | | Remove a profile |

```bash
lynxdb config get retention
lynxdb config set retention 30d
lynxdb config reload
lynxdb config add-profile prod --url https://logs.example.com --token "$PROD_KEY"
lynxdb query 'source=main level=ERROR | stats count() by host' --profile prod
```

A reload applies `log_level`, `retention`, `query.*` and `tail.*` (except `query.global_query_pool_bytes`, `query.spill_dir`, `query.max_temp_dir_size_bytes`), `storage.compaction_rate_limit_mb`, `ingest.mode`, `ingest.max_batch_size`, `ingest.max_line_bytes`, `ingest.es_compat.*`, `ingest.limits.*`, `ingest.staging.*`, `ingest.splunk_hec.enabled`, `ingest.splunk_hec.require_token`, `http.shutdown_timeout` and the `syslog` index, sourcetype, timezone, hostname and batch settings. Other changes need a restart.

---

## version

```
lynxdb version [--short | --json]
```

`--short` prints the version string; `--json` (also the default when stdout is not a terminal) prints name, version, commit, build date, Go version, OS and architecture.

## completion

```
lynxdb completion bash|zsh|fish|powershell
```

```bash
lynxdb completion bash >> ~/.bashrc
lynxdb completion zsh > "${fpath[1]}/_lynxdb"
lynxdb completion fish > ~/.config/fish/completions/lynxdb.fish
lynxdb completion powershell | Out-String | Invoke-Expression
```

Completes commands, flags, config keys, and field names and job IDs fetched from the server.

---

## Output formats

| Format | Output |
|--------|--------|
| `auto` | Terminal: table, or a bare value for a 1x1 result. Not a terminal: `json` |
| `json`, `ndjson` | One JSON object per row |
| `table`, `box` | Box-drawn table |
| `ascii` | ASCII table |
| `markdown` | Markdown table |
| `vertical`, `line`, `G` | One block per row, one line per field |
| `csv` | CSV with header |
| `tsv` | TSV with header |
| `raw` | `_raw` per row, or tab-separated `field=value` pairs |

Data goes to stdout; statistics, hints, lints and errors go to stderr. `NO_COLOR` disables color.

## Exit codes

Defined in `exitcodes.go` and mapped in `root.go`:

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | Other failure |
| 2 | Required flag missing |
| 3 | Cannot connect to the server |
| 4 | Query does not compile (PPL syntax, semantic, type, planning or unsupported-feature error) or another HTTP 400 rejection |
| 5 | Query timeout |
| 6 | No rows with `--fail-on-empty` |
| 7 | Missing, invalid or insufficient API key |
| 78 | Data directory on-disk format incompatible with this binary |
| 130 | Interrupted (SIGINT) |

## Signals

| Process | Signal | Action |
|---------|--------|--------|
| `lynxdb server` | `SIGINT`, `SIGTERM` | Graceful shutdown |
| `lynxdb server` | `SIGHUP` | Reload the config file |
| Client commands | `SIGINT` | Stop and exit with 130 |
