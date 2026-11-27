---
sidebar_position: 1
title: LynxDB command-line reference
sidebar_label: CLI Overview
description: How the lynxdb binary runs queries, global flags, connection settings, the command map and exit codes.
---

# CLI Overview

LynxDB ships as one binary, `lynxdb`. It runs the server and is also the client for it. Queries are written in [PPL](../ppl/overview.md), the OpenSearch Piped Processing Language.

```
lynxdb [command] [flags]
```

## Where a query runs

`lynxdb query` runs a query in one of two places, chosen by its flags and never by what standard input is.

| Mode | Trigger | What happens |
|------|---------|--------------|
| Server | Default | The CLI sends the query to `POST /api/v1/query` on the server named by `--server` and prints the rows it returns. |
| Local | `--file <path or glob>`, or `--file -` for standard input | The CLI opens a database of its own in a temporary directory, loads the input into the index `main`, runs the query in process and removes the directory on exit. No server is involved. |

Both modes compile the same PPL text with the same compiler and run it on the same engine. A local query may leave out the source and then reads `main`; a server query names its source.

```bash
# Server
lynxdb query 'source=main level=ERROR | stats count() as errors by host'

# Local file
lynxdb query --file access.log 'stats count() by status'

# Standard input
kubectl logs deploy/api | lynxdb query --file - 'where response_time > 500 | stats count() by path'
```

The [query page](./query.md) covers flags, time ranges and how local input is loaded.

### Bare queries

A single quoted argument that is not a command name is a query over standard input, like `lynxdb query --file -`:

```bash
cat app.log | lynxdb 'stats count() by level'
```

A lone word is always read as a command name, so `lynxdb staus` fails with `unknown command` instead of searching standard input for the word; search for one word with `lynxdb 'search staus'`. With standard input on a terminal, a bare query fails with a hint to pipe input. Plain `lynxdb` prints a short welcome screen and does not read standard input.

## Global flags

These flags work with every command.

| Flag | Short | Default | Env var | Description |
|------|-------|---------|---------|-------------|
| `--server` | | `http://localhost:3100` | `LYNXDB_SERVER` | Server address |
| `--token` | | | `LYNXDB_TOKEN` | API key sent as a bearer token |
| `--profile` | `-p` | | `LYNXDB_PROFILE` | Connection profile from the config file |
| `--config` | | auto | `LYNXDB_CONFIG` | Config file path (server, `config` commands, profiles) |
| `--format` | `-F` | `auto` | | Output format: `auto`, `json`, `ndjson`, `table`, `markdown`, `csv`, `tsv`, `raw` |
| `--quiet` | `-q` | `false` | | Print data only: no hints, progress, statistics or error messages |
| `--verbose` | `-v` | `false` | | Show extra detail, such as the full statistics block after a query on a terminal |
| `--no-stats` | | `false` | | Do not print query statistics |
| `--no-color` | | `false` | | Disable color (`NO_COLOR` with any value does the same) |
| `--theme` | | `auto` | | Human output theme: `auto`, `dark`, `light`, `plain` |
| `--compact` | | `false` | | Tighter human output |
| `--max-rows` | | `0` | | Rows shown in human table output (`0` = all) |
| `--max-width` | | `0` | | Width of human table output (`0` = terminal width) |
| `--null-value` | | | | Placeholder for null or empty values in human table output |
| `--debug` | | `false` | | Debug logging to stderr |
| `--tls-skip-verify` | | `false` | `LYNXDB_TLS_SKIP_VERIFY` | Skip TLS certificate verification (`true`, `1` or `yes` in the env var) |

`lynxdb ingest` uses `--format` for its input format, and `lynxdb login` uses `--token` for the key to save; on those two commands the global flag of the same name is not available.

See [Output Formats](./output-formats.md) for what each format prints.

## Connection settings

The CLI resolves the server address in this order, first match wins:

1. `--server`
2. The URL of the selected profile (`--profile`, then `profile` in `.lynxdbrc`, then `LYNXDB_PROFILE`)
3. `LYNXDB_SERVER`
4. `http://localhost:3100`

The API key comes from `--token`, then `LYNXDB_TOKEN`, then the credentials file that `lynxdb login` writes (`lynxdb/credentials.yaml` under the user config directory, keyed by server URL).

### Project defaults: `.lynxdbrc`

The CLI looks for a `.lynxdbrc` YAML file in the current directory and each parent. Values apply only when the matching flag is not given:

```yaml
profile: prod
default_format: table
default_since: 6h
default_source: web
```

| Key | Applies to |
|-----|-----------|
| `profile` | `--profile` |
| `default_format` | `--format` |
| `default_since` | `--since` of `lynxdb query` against a server, when none of `--since`, `--from` and `--to` is given |
| `default_source` | `--source` on `query` and `ingest` |

`.lynxdbrc` holds client defaults only. Server settings live in the config file; see [config](./config-cmd.md).

A `.lynxdbrc` cannot set the server address: the file can come with any directory you work in, and the API key goes to that server. A `server` key in it is ignored with a warning on stderr:

```
  ⚠  /home/me/repo/.lynxdbrc: server is ignored; choose the server with --server, LYNXDB_SERVER or a profile
```

## Commands

| Area | Commands |
|------|----------|
| Query | [`query`](./query.md) (`q`), [`explain`](./explain.md), [`fields`](./status.md#fields) (`f`) |
| Load data | [`ingest`](./ingest.md) (`i`) |
| Run the server | [`server`](./server.md) |
| Inspect the server | [`status`](./status.md) (`st`), [`indexes`](./status.md#indexes), [`jobs`](./status.md#jobs) |
| Manage partitions | [`partitions list`, `drop`, `detach`, `attach`](./status.md#partitions) |
| Authentication | [`login`](./auth.md#login), [`logout`](./auth.md#logout), [`auth`](./auth.md) |
| Configuration | [`config`](./config-cmd.md) |
| Utilities | [`version`](./status.md#version), [`completion`](./completion.md) |

Every command prints its flags and examples with `lynxdb <command> --help`.

## Exit codes

| Code | Meaning |
|------|---------|
| 0 | Success, including a confirmation prompt answered with no |
| 1 | Any other failure: an unknown flag, a missing argument, an execution error on the server, a bad file |
| 2 | A required flag is missing, for example `lynxdb auth create` without `--name`; or the time flags of `query` or `fields --values` are refused: `--since` with `--from` or `--to`; a time flag or `default_since` whose value is not a time; a start after its end or after now |
| 3 | The CLI cannot connect to the server |
| 4 | The query does not compile: a PPL syntax, semantic, type, planning or unsupported-feature error, reported by the server or by local compilation. Any other HTTP 400 answer, such as `QUERY_TOO_LARGE` or `VALIDATION_ERROR`, also exits with 4 |
| 5 | The query timed out (`--timeout`, the CLI's 6-minute default, or a server timeout) |
| 6 | The query returned no rows and `--fail-on-empty` was set |
| 7 | The API key is missing, invalid or lacks the scope for the request |
| 130 | Interrupted with Ctrl+C (the shell reports a process stopped by `SIGINT` as 130) |

[PPL errors](../ppl/errors.md) lists the compile error codes and their messages.

```bash
lynxdb query 'source=main level=FATAL' --since 1h --fail-on-empty --quiet > /dev/null
case $? in
  0) echo "fatal errors found" ;;
  6) echo "no fatal errors" ;;
  3) echo "server unreachable" ;;
  4) echo "query does not compile" ;;
  *) echo "query failed" ;;
esac
```

## Related

- [query](./query.md)
- [explain](./explain.md)
- [PPL overview](../ppl/overview.md)
- [REST API](../api/overview.md)
