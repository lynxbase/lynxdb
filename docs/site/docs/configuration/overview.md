---
title: Configuration Overview
description: How LynxDB resolves server configuration, where it finds config files, what .lynxdbrc does, and which settings a reload applies.
---

# Configuration Overview

LynxDB has two configuration layers:

- **Server configuration** for `lynxdb server`: CLI flags, `LYNXDB_*` environment variables, a YAML config file, and compiled defaults.
- **Client defaults** for the CLI: connection profiles in the config file and per-project `.lynxdbrc` files.

A `.lynxdbrc` file never configures the server process.

## Precedence

| Priority | Source | Example |
|----------|--------|---------|
| 1 (highest) | CLI flags | `--addr 0.0.0.0:3100` |
| 2 | Environment variables | `LYNXDB_LISTEN=0.0.0.0:3100` |
| 3 | Config file | `listen: "0.0.0.0:3100"` |
| 4 (lowest) | Compiled defaults | `localhost:3100` |

An environment variable set to an empty string is ignored. To keep the indexes in a temporary directory that is removed at shutdown, pass `--data-dir ""` or set `data_dir: ""` in the file; `LYNXDB_DATA_DIR=""` has no effect.

Not every key has a flag or an environment variable. [Environment Variables](./environment-variables.md) lists the variables, and [Server Settings](./server.md#server-flags) lists the server flags.

## Config File Discovery

With no `--config` flag and no `LYNXDB_CONFIG` variable, LynxDB uses the first file that exists:

1. `./lynxdb.yaml`
2. `$XDG_CONFIG_HOME/lynxdb/config.yaml` when `XDG_CONFIG_HOME` is set, otherwise `~/.config/lynxdb/config.yaml`
3. `~/.lynxdb/config.yaml`
4. `/etc/lynxdb/config.yaml`

If none exists, compiled defaults apply. An explicit path that cannot be read is an error.

Unknown keys produce a warning with the closest known key:

```text
Warning: unknown config key "storage.partiton" in /etc/lynxdb/config.yaml
  Did you mean "storage.partition"?
```

## Value Formats

| Type | Format | Examples |
|------|--------|----------|
| Duration | Go duration syntax. `retention`, `storage.partition`, `storage.flush_interval` and the `syslog.*` durations also accept a `d` suffix for days. | `200ms`, `30s`, `5m`, `24h`, `7d` |
| Byte size | Integer bytes or a `kb`, `mb`, `gb`, `tb` suffix (case-insensitive, 1kb = 1024 bytes) | `1048576`, `512mb`, `10gb` |
| List | YAML list in the file, comma-separated in environment variables | `[_time, timestamp]`, `_time,timestamp` |

## Settings

| Section | Keys | Page |
|---------|------|------|
| Top level | `listen`, `data_dir`, `retention`, `log_level` | [Server Settings](./server.md) |
| `storage` | `partition`, `flush_interval`, `time_fields` | [Storage Settings](./storage.md) |
| `query` | `sync_timeout`, `job_ttl`, `max_concurrent`, `default_result_limit`, `max_result_limit`, `max_query_length` | [Query Settings](./query.md) |
| `ingest` | `max_body_size`, `max_batch_size`, `max_line_bytes`, `mode`, `es_compat.*`, `otlp.*`, `splunk_hec.*`, `limits.*` | [Ingest Settings](./ingest.md) |
| `http` | `idle_timeout`, `shutdown_timeout`, `read_header_timeout`, `rate_limit` | [Server Settings](./server.md#http) |
| `syslog` | listeners, parser, framing, routing, limits | [Syslog Receiver](./syslog.md) |
| `tls`, `auth` | `tls.enabled`, `tls.cert_file`, `tls.key_file`, `auth.enabled` | [Server Settings](./server.md#tls) |
| `profiles` | connection profiles for the CLI | [Connection Profiles](#connection-profiles) |

## The `config` Command

```bash
lynxdb config                          # effective config and the source of each value
lynxdb config init                     # write the commented template to the user config path
lynxdb config init --system            # write it to /etc/lynxdb/config.yaml
lynxdb config validate                 # load, apply env and flag overrides, validate
lynxdb config get storage.partition
lynxdb config set retention 30d
lynxdb config set retention 1d --dry-run
lynxdb config edit                     # open the file in $EDITOR (falls back to $VISUAL)
lynxdb config reset                    # reset the file to defaults, with confirmation
lynxdb config path
lynxdb config reload                   # send SIGHUP to the running server
```

`init`, `set`, `edit`, `reset` and `path` act on `--config` when given, otherwise on the user config path (`$XDG_CONFIG_HOME/lynxdb/config.yaml` or `~/.config/lynxdb/config.yaml`). That is not necessarily the file the search order above finds. `set` and `reset` change the file only; a running server picks the change up on reload or restart.

`lynxdb config` and `lynxdb config validate` accept the same override flags as the server for the shared keys: `--addr`, `--data-dir`, `--log-level`.

See [config command](../cli/config-cmd.md) for the full CLI reference.

## Minimal Config File

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

storage:
  partition: "24h"

query:
  max_concurrent: 32
  sync_timeout: "30s"

ingest:
  max_body_size: "100mb"

auth:
  enabled: true
```

`lynxdb config init` writes the full commented template with every default.

## Reloading a Running Server

```bash
lynxdb config reload
# or
kill -HUP "$(cat /var/lib/lynxdb/lynxdb.pid)"
```

`lynxdb config reload` reads the PID file `lynxdb.pid` in the configured `data_dir` and sends `SIGHUP`. On `SIGHUP` the server re-reads the config file and the environment, validates the result, applies the settings below, and logs every changed setting that needs a restart.

A reload does not re-apply server flags. A setting passed as a flag (for example `--log-level debug`) falls back to its file, environment, or default value on reload, so keep reloadable settings in the config file.

Applied on reload:

- `log_level`
- `query.sync_timeout`, `query.default_result_limit`, `query.max_result_limit`, `query.max_query_length`
- `ingest.mode`, `ingest.max_batch_size`, `ingest.max_line_bytes`
- `ingest.es_compat.enabled`
- `ingest.splunk_hec.enabled`, `ingest.splunk_hec.require_token`
- `http.shutdown_timeout`
- `syslog.index`, `syslog.sourcetype`, `syslog.default_timezone`, `syslog.default_hostname`, `syslog.batch_size`, `syslog.batch_timeout`

Every other setting needs a restart, including `listen`, `data_dir`, `retention`, `storage.*`, `query.max_concurrent`, `query.job_ttl`, `tls.*`, `auth.*`, `http.idle_timeout`, `http.read_header_timeout`, `http.rate_limit`, `ingest.max_body_size`, `ingest.limits.*`, `ingest.es_compat.advertised_version`, `ingest.es_compat.cluster_name`, `ingest.otlp.*`, and the syslog listeners, parser and connection limits.

`PATCH /api/v1/config` applies a JSON patch to the running configuration with the same rules and returns the keys that still need a restart in `restart_required`. When auth is on, it needs the root key.

## Connection Profiles

Profiles live under `profiles:` in the config file:

```bash
lynxdb config add-profile prod --url https://lynxdb.example.com --token lynx_ak_abc123
lynxdb config add-profile dev --url http://dev-logs.example.com:3100
lynxdb config list-profiles
lynxdb config remove-profile dev

lynxdb query 'source=main level=ERROR | stats count() by host' --profile prod
```

```yaml
profiles:
  prod:
    url: https://lynxdb.example.com
    token: lynx_ak_abc123
```

`LYNXDB_PROFILE` sets the default profile.

## Project Defaults with `.lynxdbrc`

The CLI walks up from the current directory to the nearest `.lynxdbrc` and applies it as defaults for flags that were not set explicitly:

```yaml
# .lynxdbrc
server: https://dev-logs.example.com
profile: dev
default_format: table
default_since: 6h
default_source: web
```

| Key | Default for |
|-----|-------------|
| `server` | `--server` |
| `profile` | `--profile` |
| `default_format` | `--format` |
| `default_since` | `--since` on `lynxdb query` |
| `default_source` | `--source` on `lynxdb query`, `lynxdb ingest` and `lynxdb import` |

## Related

- [Server Settings](./server.md)
- [Storage Settings](./storage.md)
- [Query Settings](./query.md)
- [Ingest Settings](./ingest.md)
- [Syslog Receiver](./syslog.md)
- [Environment Variables](./environment-variables.md)
