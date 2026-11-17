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

An environment variable set to an empty string is ignored. To run in-memory, pass `--data-dir ""` or set `data_dir: ""` in the file; `LYNXDB_DATA_DIR=""` has no effect.

Not every key has a flag or an environment variable. [Environment Variables](./environment-variables.md) lists the bound variables, and [Server Settings](./server.md#server-flags) lists the server flags.

## Config File Discovery

With no `--config` flag and no `LYNXDB_CONFIG` variable, LynxDB uses the first file that exists:

1. `./lynxdb.yaml`
2. `$XDG_CONFIG_HOME/lynxdb/config.yaml` when `XDG_CONFIG_HOME` is set, otherwise `~/.config/lynxdb/config.yaml`
3. `~/.lynxdb/config.yaml`
4. `/etc/lynxdb/config.yaml`

If none exists, compiled defaults apply. An explicit path that cannot be read is an error.

Unknown keys produce a warning with the closest known key:

```text
Warning: unknown config key "storage.compresion" in /etc/lynxdb/config.yaml
  Did you mean "storage.compression"?
```

## Value Formats

| Type | Format | Examples |
|------|--------|----------|
| Duration | Go duration syntax. `retention`, `ingest.staging.*`, `syslog.*` and `cluster.*` durations also accept a `d` suffix for days. | `200ms`, `30s`, `5m`, `24h`, `7d` |
| Byte size | Integer bytes or a `kb`, `mb`, `gb`, `tb` suffix (case-insensitive, 1kb = 1024 bytes) | `1048576`, `512mb`, `10gb` |
| List | YAML list in the file, comma-separated in environment variables and flags | `[meta, ingest, query]`, `meta,ingest,query` |

## The `config` Command

```bash
lynxdb config                          # effective config and the source of each value
lynxdb config init                     # write the commented template to the user config path
lynxdb config init --system            # write it to /etc/lynxdb/config.yaml
lynxdb config validate                 # load, apply env and flag overrides, validate
lynxdb config get storage.flush_threshold
lynxdb config set retention 30d
lynxdb config set retention 1d --dry-run
lynxdb config edit                     # open the file in $EDITOR (falls back to $VISUAL)
lynxdb config reset                    # reset the file to defaults, with confirmation
lynxdb config path
lynxdb config reload                   # send SIGHUP to the running server
```

`init`, `set`, `edit`, `reset` and `path` act on `--config` when given, otherwise on the user config path (`$XDG_CONFIG_HOME/lynxdb/config.yaml` or `~/.config/lynxdb/config.yaml`). That is not necessarily the file the search order above finds. `set` and `reset` change the file only; a running server picks the change up on reload or restart.

`lynxdb config` and `lynxdb config validate` accept the same override flags as the server for the shared keys: `--addr`, `--data-dir`, `--s3-bucket`, `--s3-region`, `--s3-prefix`, `--compaction-interval`, `--tiering-interval`, `--cache-max-mb`, `--log-level`.

See [config command](../cli/config-cmd.md) for the full CLI reference.

## Minimal Config File

```yaml
listen: "0.0.0.0:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

storage:
  flush_threshold: "512mb"
  compaction_workers: 2

query:
  max_concurrent: 32
  max_query_runtime: "5m"
  max_query_memory_bytes: "1gb"

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

- `log_level`, `retention`
- `query.*`, except `query.global_query_pool_bytes`, `query.spill_dir` and `query.max_temp_dir_size_bytes`
- `tail.*`
- `storage.compaction_rate_limit_mb`
- `ingest.mode`, `ingest.max_batch_size`, `ingest.max_line_bytes`
- `ingest.es_compat.enabled`, `ingest.es_compat.advertised_version`, `ingest.es_compat.strip_logstash_date_suffix`
- `ingest.limits.*`
- `ingest.staging.enabled`, `ingest.staging.max_bytes`, `ingest.staging.max_age`, `ingest.staging.max_inflight_events`
- `ingest.splunk_hec.enabled`, `ingest.splunk_hec.require_token`
- `http.shutdown_timeout`
- `syslog.index`, `syslog.sourcetype`, `syslog.default_timezone`, `syslog.default_hostname`, `syslog.batch_size`, `syslog.batch_timeout`

Every other setting needs a restart, including `listen`, `data_dir`, `tls.*`, `auth.*`, all other `storage.*` keys, `http.idle_timeout`, `http.read_header_timeout`, `http.rate_limit`, `ingest.max_body_size`, `ingest.fsync`, `ingest.dedup_*`, `ingest.otlp.*`, syslog listeners and connection limits, `server.*`, `buffer_manager.*` and `cluster.*`.

`PATCH /api/v1/config` applies a JSON patch to the running configuration with the same rules and returns the keys that still need a restart in `restart_required`. When auth is on, it needs the root key.

## Connection Profiles

Profiles live under `profiles:` in the config file:

```bash
lynxdb config add-profile prod --url https://lynxdb.example.com --token lxk_abc123
lynxdb config add-profile staging --url https://staging.example.com
lynxdb config list-profiles
lynxdb config remove-profile staging

lynxdb query 'source=main level=ERROR | stats count() by host' --profile prod
```

```yaml
profiles:
  prod:
    url: https://lynxdb.example.com
    token: lxk_abc123
```

`LYNXDB_PROFILE` sets the default profile.

## Project Defaults with `.lynxdbrc`

The CLI walks up from the current directory to the nearest `.lynxdbrc` and applies it as defaults for flags that were not set explicitly:

```yaml
# .lynxdbrc
server: https://staging.example.com
profile: staging
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
- [S3 Tiering](./s3-tiering.md)
- [Cluster Settings](./cluster.md)
- [Environment Variables](./environment-variables.md)
