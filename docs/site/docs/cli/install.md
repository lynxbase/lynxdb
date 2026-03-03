---
sidebar_position: 14
title: install & uninstall
description: Set up LynxDB as a system service with a single command -- dedicated user, directories, config, hardened systemd/launchd unit.
---

# install & uninstall

Set up (or remove) LynxDB as a production service. One command replaces the manual steps of creating a user, directories, config file, and systemd/launchd unit.

## install

```
lynxdb install [flags]
```

### System mode (root)

When run as root (or with `sudo`), `lynxdb install` performs these steps:

1. **Copies the binary** to `/usr/local/bin/lynxdb` (or `--prefix/bin`)
2. **Creates a system user and group** (`lynxdb:lynxdb`, no login shell)
3. **Creates directories** -- `/var/lib/lynxdb` (data), `/etc/lynxdb` (config)
4. **Writes a default config** to `/etc/lynxdb/config.yaml` (skipped if file exists)
5. **Sets file descriptor limits** -- writes a `/etc/security/limits.d/lynxdb.conf` with `nofile=65536`
6. **Grants `CAP_NET_BIND_SERVICE`** so the service can bind to ports below 1024 without running as root
7. **Installs a hardened systemd service** (`lynxdb.service`) with `ProtectSystem=strict`, `ProtectHome=true`, `NoNewPrivileges=true`, `PrivateTmp=true`, and `ReadWritePaths=/var/lib/lynxdb`
8. **Runs a self-test** -- starts the server briefly and verifies the health endpoint responds

All steps are idempotent. Re-running `lynxdb install` after an upgrade refreshes the binary and service file without touching your config or data.

### User mode (non-root)

When run without root, LynxDB installs into the current user's home directory:

| Resource | Path |
|----------|------|
| Binary | `~/.local/bin/lynxdb` |
| Config | `~/.config/lynxdb/config.yaml` |
| Data | `~/.local/share/lynxdb` |
| Service | `~/.config/systemd/user/lynxdb.service` (Linux) or `~/Library/LaunchAgents/org.lynxdb.plist` (macOS) |

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--yes` | `false` | Skip confirmation prompt (non-interactive) |
| `--user` | `lynxdb` | System user to create (system mode only) |
| `--group` | `lynxdb` | System group to create (system mode only) |
| `--data-dir` | `/var/lib/lynxdb` | Override the data directory |
| `--prefix` | `/usr/local` | Installation prefix (binary goes to `PREFIX/bin`) |
| `--skip-service` | `false` | Do not create a systemd/launchd service unit |
| `--skip-config` | `false` | Do not write a default config file |
| `--skip-capabilities` | `false` | Do not set `CAP_NET_BIND_SERVICE` |
| `--skip-ulimits` | `false` | Do not write file descriptor limits config |
| `--skip-self-test` | `false` | Do not run the post-install health check |

### Examples

```bash
# Standard system install (interactive -- prompts for confirmation)
sudo lynxdb install

# Non-interactive (CI/CD, automation scripts)
sudo lynxdb install --yes

# Custom data directory
sudo lynxdb install --data-dir /data/lynxdb

# User-local install (no root required)
lynxdb install

# Install binary and config only, no systemd unit
sudo lynxdb install --skip-service

# Custom prefix (e.g., /opt/lynxdb)
sudo lynxdb install --prefix /opt/lynxdb
```

### After installation

```bash
# Start the service
sudo systemctl start lynxdb

# Enable on boot
sudo systemctl enable lynxdb

# Verify
lynxdb health
lynxdb status
```

---

## uninstall {#uninstall}

```
lynxdb uninstall [flags]
```

Removes the systemd/launchd service, binary, config, and related system files. **Data is never removed** -- your `/var/lib/lynxdb` (or custom data directory) is always preserved.

### What gets removed

| Resource | Standard | With `--purge` |
|----------|----------|----------------|
| systemd / launchd service | Stopped and removed | Stopped and removed |
| Binary (`/usr/local/bin/lynxdb`) | Removed | Removed |
| Limits config (`/etc/security/limits.d/lynxdb.conf`) | Removed | Removed |
| Config directory (`/etc/lynxdb/`) | Preserved | Removed |
| System user (`lynxdb`) | Preserved | Removed |
| **Data directory (`/var/lib/lynxdb/`)** | **Preserved** | **Preserved** |

### Flags

| Flag | Default | Description |
|------|---------|-------------|
| `--yes` | `false` | Skip confirmation prompt |
| `--purge` | `false` | Also remove config directory and system user (data is still preserved) |

### Examples

```bash
# Standard uninstall (prompts for confirmation)
sudo lynxdb uninstall

# Non-interactive
sudo lynxdb uninstall --yes

# Remove everything except data
sudo lynxdb uninstall --purge
```

After uninstalling, your data directory remains intact. To remove data manually:

```bash
sudo rm -rf /var/lib/lynxdb
```

## See Also

- [server](/docs/cli/server) -- server flags and signals
- [config](/docs/cli/config-cmd) -- configuration management
- [Single Node Deployment](/docs/deployment/single-node) -- production deployment guide
- [Upgrading](/docs/operations/upgrading) -- upgrade procedures
