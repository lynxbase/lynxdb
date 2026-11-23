---
title: Upgrading LynxDB
description: Upgrade LynxDB by replacing the binary and restarting -- install channels, upgrade procedures, config compatibility, and rollback.
---

# Upgrading LynxDB

LynxDB is a single binary. Upgrading means replacing that binary with a newer release and restarting the server. On startup the server opens every index directory under `data_dir` from its manifest and continues from the state the stopped process left. There is no separate migration step.

Back up the data directory before upgrading a production server (see [Backup and Restore](/docs/operations/backup-restore)).

## Getting a Release

### Install Script

Re-run the install script. It reads the newest version of the stable channel from `https://dl.lynxdb.org/channels/stable`, downloads the release for your platform, verifies its checksum, and replaces the installed binary:

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

Install a specific version:

```bash
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version 26.11.1.80
# or
curl -fsSL https://lynxdb.org/install.sh | LYNXDB_VERSION=26.11.1.80 sh
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
docker pull ghcr.io/lynxbase/lynxdb:26.11.1.80

# The newest patch of one line
docker pull ghcr.io/lynxbase/lynxdb:26.11
```

With Docker Compose, update the image tag and recreate the container:

```bash
docker compose pull
docker compose up -d
```

### GitHub Releases

Every release publishes archives and `SHA256SUMS` on the [GitHub releases page](https://github.com/lynxbase/lynxdb/releases), with a build attestation for each file:

```bash
gh attestation verify lynxdb-26.11.1.80-linux-amd64.tar.gz --repo lynxbase/lynxdb
```

## Versions and Lines

A version reads `year.month.patch.build`. `26.11` is a line, named after the month it was cut from `main`. `26.11.1.80` is its first release and `26.11.2.12` its first patch. The build number counts the commits since the release before.

The `stable` channel of the install script, the `lynxdb` Homebrew formula and the `latest` image follow the newest release, whatever its line. To stay on one line and take only its patches, use the formula or the image tag of the line:

```bash
brew install lynxbase/tap/lynxdb@26.11
docker pull ghcr.io/lynxbase/lynxdb:26.11
```

A line marked lts keeps receiving patches after newer lines open. `--channel lts` of the install script follows the newest lts release.

## Release Candidates

A candidate is published as a pre-release for testing before the release it precedes. It may contain regressions and is not meant for production. No channel, formula or `latest` tag follows it. Install one by its version:

```bash
# Install script
curl -fsSL https://lynxdb.org/install.sh | sh -s -- --version 26.12.1.45

# Docker
docker pull ghcr.io/lynxbase/lynxdb:26.12.1.45
```

To return to stable, re-run the install script without `--version`.

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

Stopping the server writes the events it holds in memory and closes every index. Downtime is the time between stop and start.

### Behind a Load Balancer

1. Remove the node from the load balancer, or let its health check fail.
2. Wait for in-flight requests to finish.
3. Stop the server, replace the binary, start the server.
4. Check `lynxdb health`.
5. Add the node back.

### Kubernetes

```bash
kubectl -n lynxdb set image statefulset/lynxdb lynxdb=ghcr.io/lynxbase/lynxdb:26.11.2.12
kubectl -n lynxdb rollout status statefulset/lynxdb
```

## Several Servers

Servers that each own a data directory are upgraded independently, one at a time, with the single-node procedure above:

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

## Config Compatibility

A config key that the running release does not read is ignored, and the server prints a warning about it to stderr at startup. Check a config file against the installed release before the restart:

```bash
lynxdb config validate --config /etc/lynxdb/config.yaml
```

```text
  ⚠  unknown config key "query.max_rows" in /etc/lynxdb/config.yaml
  ✔ Config file /etc/lynxdb/config.yaml is valid.
```

## Rollback

To roll back, stop the server, install the older version, and start it again:

```bash
sudo systemctl stop lynxdb
curl -fsSL https://lynxdb.org/install.sh | sudo sh -s -- --version 26.11.1.80
sudo systemctl start lynxdb
```

With Docker, pin the older tag:

```bash
docker pull ghcr.io/lynxbase/lynxdb:26.11.1.80
docker compose up -d
```

A rollback works when the older release opens every index the newer one wrote. If the older server stops at startup with an error about an index, restore the backup taken before the upgrade.

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
