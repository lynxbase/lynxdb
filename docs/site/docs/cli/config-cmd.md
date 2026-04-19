---
sidebar_position: 9
title: config & doctor
description: Manage LynxDB configuration -- init, validate, reload, get/set values, profiles, and environment diagnostics.
---

# config & doctor

Show and manage LynxDB configuration. The `config` command tree covers initialization, validation, hot-reload, individual settings, and connection profiles. The `doctor` command checks your environment.

## config

When run without a subcommand, displays the resolved configuration with all overrides applied.

```
lynxdb config [subcommand] [flags]
```

### Persistent Flags

Available on `config` and all its subcommands:

| Flag | Default | Description |
|------|---------|-------------|
| `--addr` | | Listen address override |
| `--data-dir` | | Data directory override |
| `--s3-bucket` | | S3 bucket override |
| `--s3-region` | | AWS region override |
| `--s3-prefix` | | S3 prefix override |
| `--compaction-interval` | | Compaction interval override |
| `--tiering-interval` | | Tiering interval override |
| `--cache-max-mb` | | Cache max size override |
| `--log-level` | | Log level override |

---

## config init

Create a default config file with all documented settings.

```
lynxdb config init [--system]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--system` | `false` | Write to `/etc/lynxdb/config.yaml` instead of user directory |

```bash
# Create user config
lynxdb config init

# Create system-wide config
lynxdb config init --system
```

---

## config validate

Validate config file and show non-default values. Warns on unknown keys.

```
lynxdb config validate
```

---

## config reload

Send SIGHUP to a running server to reload its configuration. The server writes a PID file so this command can find it automatically.

```
lynxdb config reload
```

**Hot-reloadable settings** (no restart required):

- `log_level`
- `retention`
- `query.max_concurrent`
- `query.default_result_limit`
- `query.max_result_limit`
- `query.max_query_runtime`

**Settings that require a restart** (server warns on reload):

- `listen`
- `data_dir`

---

## config get

Show a single config value. Tab-completes known config keys.

```
lynxdb config get <key>
```

```bash
lynxdb config get retention
lynxdb config get storage.compression
```

---

## config set

Set a config value in the config file.

```
lynxdb config set <key> <value> [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | `false` | Show what would change without applying |

```bash
lynxdb config set retention 30d
lynxdb config set storage.compression zstd
lynxdb config set retention 1d --dry-run
```

---

## config edit

Open config file in `$EDITOR`. Creates the file with defaults if it does not exist.

```
lynxdb config edit
```

---

## config path

Print the resolved config file path.

```
lynxdb config path
```

---

## config reset

Reset config file to defaults (with confirmation).

```
lynxdb config reset [--dry-run]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--dry-run` | `false` | Show what would be reset without applying |

---

## config add-profile

Add or update a connection profile for quick server switching.

```
lynxdb config add-profile <name> --url <url> [--token <token>]
```

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | (required) | Server URL for the profile |
| `--token` | | Authentication token (optional) |

```bash
# Add a production profile
lynxdb config add-profile prod --url https://lynxdb.company.com --token xxx

# Add a staging profile without auth
lynxdb config add-profile staging --url https://staging.company.com
```

Use profiles with the global `--profile` / `-p` flag:

```bash
lynxdb query 'level=error | stats count' --profile prod
lynxdb query 'level=error | stats count' -p staging
```

Or set a default profile via environment variable:

```bash
export LYNXDB_PROFILE=prod
lynxdb query 'level=error | stats count'
```

---

## config list-profiles

List all connection profiles.

```
lynxdb config list-profiles
```

---

## config remove-profile

Remove a connection profile.

```
lynxdb config remove-profile <name>
```

---

## Configuration Precedence

Settings are resolved in this order (highest to lowest priority):

1. CLI flags (`--addr`, `--data-dir`, etc.)
2. Environment variables (`LYNXDB_LISTEN`, `LYNXDB_DATA_DIR`, etc.)
3. Project file (`.lynxdbrc` in current or parent directory)
4. Config file (YAML)
5. Compiled defaults

### Config File Search Order

When `--config` is not specified:

1. `LYNXDB_CONFIG` environment variable
2. `./lynxdb.yaml` (current directory)
3. `$XDG_CONFIG_HOME/lynxdb/config.yaml` (or `~/.config/lynxdb/config.yaml`)
4. `~/.lynxdb/config.yaml`
5. `/etc/lynxdb/config.yaml`

The first file found is used. If none exist, compiled defaults apply.

### Project File (`.lynxdbrc`)

A `.lynxdbrc` YAML file in the current or any parent directory can set per-project defaults:

```yaml
server: https://staging.company.com
default_format: table
default_since: 6h
default_source: web
profile: staging
```

These are applied as defaults and are overridden by explicit CLI flags.

- `server`, `default_format`, and `profile` affect root CLI defaults
- `default_since` is applied to `lynxdb query` when `--since` is omitted
- `default_source` is applied to `lynxdb query`, `lynxdb ingest`, and `lynxdb import` when `--source` is omitted

### Example Config File

```yaml
listen: "localhost:3100"
data_dir: "/var/lib/lynxdb"
retention: "7d"
log_level: "info"

storage:
  compression: "lz4"
  row_group_size: 65536
  flush_threshold: "512mb"
  compaction_interval: "30s"
  compaction_workers: 2
  s3_bucket: ""
  s3_region: "us-east-1"
  cache_max_bytes: "1gb"
  cache_ttl: "5m"

query:
  sync_timeout: "30s"
  max_query_runtime: "5m"
  max_concurrent: 10
  default_result_limit: 1000
  max_result_limit: 50000

ingest:
  max_body_size: "100mb"
  max_batch_size: 1000

http:
  idle_timeout: "2m"
  shutdown_timeout: "30s"
```

---

## doctor

Run environment diagnostics. Checks binary, config, data directory, server connectivity, disk space, retention, and shell completion.

```
lynxdb doctor
```

Supports `--format json`.

### Console Output

```
  ok Binary        v0.1.0 (linux/amd64, go1.25.4)
  ok Config        /home/user/.config/lynxdb/config.yaml (valid)
  ok Data dir      /var/lib/lynxdb (42 GB free)
  ok Server        localhost:3100 (healthy, uptime 2d 5h)
  ok Events        3.4M total
  ok Storage       1.2 GB
  ok Retention     7d
  ok Completion    zsh detected

  All checks passed.
```

## See Also

- [Server](/docs/cli/server) for running the server and signal handling
- [CLI Overview](/docs/cli/overview) for global flags and environment variables
- [Shell Completion](/docs/cli/completion) for setting up tab completion
