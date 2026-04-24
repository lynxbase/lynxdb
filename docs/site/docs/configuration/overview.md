---
title: Configuration Overview
description: How LynxDB resolves server config, where it looks for config files, what .lynxdbrc does, and what reload actually changes.
---

# Configuration Overview

LynxDB has two different configuration mechanisms:

- Server configuration for `lynxdb server`, loaded from CLI flags, environment variables, YAML config files, and compiled defaults.
- Client-side defaults for the CLI, including connection profiles and per-project `.lynxdbrc` files.

Keep those layers separate. A `.lynxdbrc` file does not configure the server process.

## Server Configuration Precedence

For server settings, LynxDB resolves values in this order:

| Priority | Source | Example |
|----------|--------|---------|
| 1 (highest) | CLI flags | `--addr 0.0.0.0:3100` |
| 2 | Environment variables | `LYNXDB_LISTEN=0.0.0.0:3100` |
| 3 | Config file (YAML) | `listen: "0.0.0.0:3100"` |
| 4 (lowest) | Compiled defaults | `localhost:3100` |

`.lynxdbrc` is not part of this cascade.

## Config File Discovery

When an explicit config path is not provided with `--config` or `LYNXDB_CONFIG`, LynxDB looks for a server config file in this order:

1. `./lynxdb.yaml`
2. `$XDG_CONFIG_HOME/lynxdb/config.yaml` if `XDG_CONFIG_HOME` is set
3. `~/.config/lynxdb/config.yaml` otherwise
4. `~/.lynxdb/config.yaml`
5. `/etc/lynxdb/config.yaml`

The first file found is used. If none exist, compiled defaults apply.

## The `config` Command

`lynxdb config` is the operational entry point for inspecting and editing config files.

### Show the effective config

```bash
lynxdb config
```

Running `lynxdb config` with no subcommand prints the resolved configuration and the source of each value.

### Create a config file

```bash
# Create a user-scoped config file
lynxdb config init

# Create /etc/lynxdb/config.yaml
lynxdb config init --system
```

`lynxdb config init` writes the built-in commented template. It is a starting point for editing, not a runtime dump of the currently effective config.

### Validate and inspect values

```bash
lynxdb config validate
lynxdb config get retention
lynxdb config get storage.compression
```

`lynxdb config validate` loads the file, applies env and CLI overrides, warns about unknown keys, and validates the final config.

### Update config files

```bash
lynxdb config set retention 30d
lynxdb config set storage.compression zstd
lynxdb config edit
lynxdb config reset
```

`lynxdb config set` and `lynxdb config reset` modify the config file on disk. They do not change a running server until you reload or restart it.

### Config file path behavior

```bash
lynxdb config path
```

`lynxdb config path` prints the path LynxDB will use for file-writing operations such as `init`, `edit`, `set`, and `reset`. By default, that is the user config path returned by `DefaultConfigFilePath()`, not necessarily the file discovered by the search order above.

## Minimal Config Example

```yaml
listen: "localhost:3100"
data_dir: "/var/lib/lynxdb"
retention: "30d"
log_level: "info"

storage:
  compression: "lz4"
  flush_threshold: "512mb"
  compaction_workers: 2

query:
  max_query_runtime: "5m"
  max_query_memory_bytes: "1gb"
  global_query_pool_bytes: "0"
  spill_dir: ""

ingest:
  max_body_size: "100mb"
  max_batch_size: 1000

http:
  idle_timeout: "120s"
  shutdown_timeout: "30s"

tls:
  enabled: false

auth:
  enabled: false
```

For a complete commented template, run `lynxdb config init`.

## Project Defaults with `.lynxdbrc`

LynxDB walks up from the current working directory to find the nearest `.lynxdbrc`.

These files are for CLI defaults only. They do not affect the server process.

```yaml
# .lynxdbrc
server: https://staging.company.com
default_format: table
default_since: 6h
default_source: web
profile: staging
```

Current behavior is narrower than the file schema suggests:

- `server` is applied as the default `--server`
- `default_format` is applied as the default `--format`
- `profile` is applied as the default `--profile`
- `default_since` is applied as the default `--since` for `lynxdb query`
- `default_source` is applied as the default `--source` for `lynxdb query`, `lynxdb ingest`, and `lynxdb import`

That makes `.lynxdbrc` useful for per-repository server targeting and for lightweight query/ingest defaults, while still allowing explicit flags to override project settings.

## Hot-Reload

To reload a running server from its config file:

```bash
lynxdb config reload
```

`lynxdb config reload` finds the server PID file under the current `data_dir` and sends `SIGHUP`.

In the current implementation:

- `log_level` is re-applied
- `retention` is re-applied
- most query execution settings are reloaded for future work
- `ingest.mode`, `ingest.max_batch_size`, and `ingest.max_line_bytes` are re-applied
- `tail.*` limits are re-applied
- `http.idle_timeout`, `http.shutdown_timeout`, and `http.read_header_timeout` are re-applied
- `storage.compaction_rate_limit_mb` is explicitly re-applied

Treat these changes as restart-required:

- `listen`
- `data_dir`
- `tls.*`
- `auth.*`
- `no_ui`
- `http.rate_limit`
- `ingest.max_body_size` and ingest-engine settings such as dedup/fsync
- `query.global_query_pool_bytes`, `query.spill_dir`, and `query.max_temp_dir_size_bytes`
- storage scheduler settings such as `storage.compaction_workers`

## Connection Profiles

Connection profiles live in the main config file under `profiles:` and are managed with `lynxdb config add-profile`, `list-profiles`, and `remove-profile`.

```bash
lynxdb config add-profile prod --url https://lynxdb.company.com --token lxk_abc123
lynxdb config add-profile staging --url https://staging.company.com

lynxdb config list-profiles

lynxdb query 'level=error | stats count' --profile prod
lynxdb query 'level=error | stats count' -p staging
```

## Related

- [Server Settings](/docs/configuration/server)
- [Storage Settings](/docs/configuration/storage)
- [Query Settings](/docs/configuration/query)
- [Ingest Settings](/docs/configuration/ingest)
- [Cluster Settings](/docs/configuration/cluster)
- [config & doctor](/docs/cli/config-cmd)
