---
title: Upgrading LynxDB
description: Upgrade LynxDB by replacing the binary and restarting -- install channels, upgrade procedures, storage format compatibility, and rollback.
---

# Upgrading LynxDB

LynxDB is a single binary. Upgrading means replacing that binary with a newer release and restarting the server. On startup the server checks the data directory's format marker and segment headers, rebuilds its part registry from disk, and continues where the old process stopped. There is no separate migration step.

Back up the data directory before upgrading a production server (see [Backup and Restore](/docs/operations/backup-restore)).

## Getting a New Binary

### Install Script

Re-run the install script. It reads the stable manifest at `https://dl.lynxdb.org/manifest.json`, downloads the release for your platform, verifies its checksum, and replaces the installed binary:

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

Install a specific version:

```bash
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version v0.5.0
# or
LYNXDB_VERSION=v0.5.0 curl -fsSL https://lynxdb.org/install.sh | sh
```

Install into a custom directory with `--dir /opt/lynxdb/bin` or `LYNXDB_INSTALL_DIR`.

### Homebrew

```bash
brew update
brew upgrade lynxbase/tap/lynxdb
```

### Docker

```bash
# Latest stable release
docker pull ghcr.io/lynxbase/lynxdb:latest

# A specific version
docker pull ghcr.io/lynxbase/lynxdb:0.5.0
```

With Docker Compose, update the image tag and recreate the container:

```bash
docker compose pull
docker compose up -d
```

### GitHub Releases

Every release publishes archives and checksums on the [GitHub releases page](https://github.com/lynxbase/lynxdb/releases).

## Nightly Builds

Nightly builds are prereleases built from `main` for testing. They may contain regressions and are not meant for production. Their versions look like `v0.2.6-nightly.20261116.g403d8ad`.

```bash
# Install script
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --channel nightly --allow-prerelease

# Homebrew (a separate formula that installs the same lynxdb binary name)
brew install lynxbase/tap/lynxdb-nightly
brew upgrade lynxbase/tap/lynxdb-nightly

# Docker
docker pull ghcr.io/lynxbase/lynxdb:nightly
```

The install script reads the nightly manifest (`https://dl.lynxdb.org/nightly/manifest.json`) only when both `--channel nightly` and `--allow-prerelease` are given. To return to stable, re-run the install script without those flags, or `brew uninstall lynxdb-nightly && brew install lynxbase/tap/lynxdb`.

## Single-Node Upgrade

### With systemd

```bash
# 1. Stop the server
sudo systemctl stop lynxdb

# 2. Replace the binary
curl -fsSL https://lynxdb.org/install.sh | sudo sh

# 3. Check the version
lynxdb version --short

# 4. Start the server
sudo systemctl start lynxdb

# 5. Check health
lynxdb health
lynxdb status
```

Stopping the server flushes buffered events to parts. Downtime is the time between stop and start, usually a few seconds plus the startup part scan.

### Behind a Load Balancer

1. Remove the node from the load balancer, or let its health check fail.
2. Wait for in-flight requests to finish.
3. Stop the server, replace the binary, start the server.
4. Check `lynxdb health`.
5. Add the node back.

### Kubernetes

```bash
kubectl -n lynxdb set image statefulset/lynxdb lynxdb=ghcr.io/lynxbase/lynxdb:0.5.0
kubectl -n lynxdb rollout status statefulset/lynxdb
```

## Several Servers

`lynxdb server` runs as a single node: the `cluster` settings are validated but start no cluster runtime (see [Distributed Architecture](/docs/architecture/distributed)). Servers that each own a data directory are upgraded independently, one at a time, with the single-node procedure above:

```bash
for host in lynxdb-1 lynxdb-2 lynxdb-3; do
  ssh "$host" "sudo systemctl stop lynxdb"
  ssh "$host" "curl -fsSL https://lynxdb.org/install.sh | sudo sh"
  ssh "$host" "sudo systemctl start lynxdb"
  ssh "$host" "lynxdb health"
done
```

A Kubernetes StatefulSet with `updateStrategy: RollingUpdate` replaces its pods one at a time after an image change:

```bash
kubectl -n lynxdb rollout status statefulset/lynxdb
```

## Storage Format

Segments use the `.lsg` format, versioned by a format major. The data directory holds a `FORMAT` marker file (`LSGFMT v<N>`). On startup the server:

1. Writes the marker for the newest format it supports when the data directory has no segments and no marker.
2. Refuses to start when the directory holds segments but no marker, when the marker is newer than the binary supports, or when any segment has an unknown format major or required capability bit. The CLI exits with code 78.
3. Logs a warning, without refusing to start, for a segment with physical damage such as a checksum failure.

Format support by release:

| LynxDB release | Reads | Writes |
|---|---|---|
| v0.1.8 and later | format 1 and 2 | format 2 |
| v0.1.6, v0.1.7 | format 1 | format 1 |
| before v0.1.6 | older layouts | older layouts |

Upgrading from v0.1.6 or later needs no action: the new binary reads the existing segments, writes new parts in format 2, and compaction rewrites merged parts in format 2. The marker is not rewritten. Data directories from releases before v0.1.6 have no `FORMAT` marker and use a layout current releases cannot read; re-ingest that data into a fresh data directory.

See `docs/storage-format.md` in the repository for the format registry and capability bits.

## Config Compatibility

Config keys keep their meaning across releases, and new keys come with defaults, so an existing config file keeps working. `lynxdb config validate` warns about unknown keys: keys the running release does not read.

## Rollback

To roll back, stop the server, install the older version, and start it again:

```bash
sudo systemctl stop lynxdb
curl -fsSL https://lynxdb.org/install.sh | sudo sh -s -- --version v0.4.0
sudo systemctl start lynxdb
```

With Docker, pin the older tag:

```bash
docker pull ghcr.io/lynxbase/lynxdb:0.4.0
docker compose up -d
```

A rollback works as long as the older release reads every segment in the data directory. Releases from v0.1.8 on read formats 1 and 2. Rolling back to v0.1.6 or v0.1.7 after a newer release wrote format 2 parts fails at startup; restore a backup taken before the upgrade instead.

## Checking the Version

```bash
lynxdb version            # version, commit, build date, Go runtime
lynxdb version --short    # version string only
lynxdb version --json     # structured output
```

## Next Steps

- [Backup and Restore](/docs/operations/backup-restore) -- back up before upgrading
- [Monitoring](/docs/operations/monitoring) -- check health after an upgrade
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose startup errors
- [Configuration Overview](/docs/configuration/overview) -- configuration options
