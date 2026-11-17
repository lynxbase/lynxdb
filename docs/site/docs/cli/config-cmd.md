---
sidebar_position: 9
title: config
description: Show, validate, edit and reload LynxDB configuration, and manage connection profiles.
---

# config

Show and manage the server's configuration file and the CLI's connection profiles.

```
lynxdb config [subcommand] [flags]
```

Without a subcommand, `config` prints every setting with its value and where the value came from: `default`, the config file, an environment variable or a flag.

## Config file

When `--config` is not given, the first file found wins:

1. `LYNXDB_CONFIG`
2. `./lynxdb.yaml`
3. `$XDG_CONFIG_HOME/lynxdb/config.yaml`, or `~/.config/lynxdb/config.yaml`
4. `~/.lynxdb/config.yaml`
5. `/etc/lynxdb/config.yaml`

With no file, built-in defaults apply. The commands that write a file (`init`, `set`, `edit`, `reset`) use `--config`, or else `$XDG_CONFIG_HOME/lynxdb/config.yaml` (`~/.config/lynxdb/config.yaml`).

Settings resolve in this order, highest first: `lynxdb server` flags, `LYNXDB_*` environment variables, the config file, defaults. [Configuration](../configuration/overview.md) lists every key and [Environment variables](../configuration/environment-variables.md) their variable names.

## Override flags

`config` and `config validate` accept these flags to preview what the same `lynxdb server` flags would change:

| Flag | Overrides |
|------|-----------|
| `--addr` | `listen` |
| `--data-dir` | `data_dir` |
| `--compaction-interval` | `storage.compaction_interval` |
| `--cache-max-mb` | `storage.cache_max_bytes` |
| `--log-level` | `log_level` |

```bash
lynxdb config --data-dir /var/lib/lynxdb --log-level debug
```

## config init

```
lynxdb config init [--system]
```

Writes a commented template with every setting and its default. `--system` writes `/etc/lynxdb/config.yaml`. The command refuses to overwrite an existing file.

## config validate

```
lynxdb config validate
```

Loads and validates the config file, warns about unknown keys, and lists the values that differ from the defaults.

## config get

```
lynxdb config get <key>
```

Prints one value and its origin. Keys tab-complete.

```bash
lynxdb config get retention
lynxdb config get storage.compression
lynxdb config get listen --format json
```

## config set

```
lynxdb config set <key> <value> [--dry-run]
```

Writes one value to the config file. `--dry-run` shows the current and the new value without writing.

```bash
lynxdb config set retention 30d
lynxdb config set storage.compression zstd
lynxdb config set log_level debug --dry-run
```

A running server picks the change up on [`config reload`](#config-reload).

## config edit

```
lynxdb config edit
```

Opens the config file in `$EDITOR` (or `$VISUAL`), creating it from the template first when it does not exist.

## config path

```
lynxdb config path
```

Prints the path `init`, `set`, `edit` and `reset` write to.

## config reset

```
lynxdb config reset [--dry-run]
```

Overwrites the config file with the default template after you type `reset` to confirm. `--dry-run` shows what would be reset.

## config reload

```
lynxdb config reload
```

Sends `SIGHUP` to the running server. The CLI finds the server's PID in `<data_dir>/lynxdb.pid`, so it reads the same config to locate the data directory. [server](./server.md#what-a-reload-changes) lists the settings a reload applies.

## config add-profile

```
lynxdb config add-profile <name> --url <url> [--token <token>]
```

A connection profile stores a server URL and an optional API key in the config file, under `profiles`. Running it again with the same name updates the profile.

| Flag | Default | Description |
|------|---------|-------------|
| `--url` | required | Server URL |
| `--token` | | API key |

```bash
lynxdb config add-profile prod --url https://logs.example.com --token "$PROD_KEY"
lynxdb config add-profile staging --url https://staging-logs.example.com
```

Select a profile with `--profile` (`-p`), `LYNXDB_PROFILE` or `profile:` in `.lynxdbrc`. `--server` takes precedence over the profile's URL.

```bash
lynxdb query 'source=main level=ERROR | stats count() by host' --profile prod
lynxdb status -p staging
LYNXDB_PROFILE=prod lynxdb tail 'source=main level=ERROR'
```

## config list-profiles

```
lynxdb config list-profiles
```

Lists profiles with their URL and whether they hold a key.

## config remove-profile

```
lynxdb config remove-profile <name>
```

## See also

- [CLI overview](./overview.md#connection-settings) for `.lynxdbrc` and connection precedence
- [Configuration](../configuration/overview.md)
- [server](./server.md)
