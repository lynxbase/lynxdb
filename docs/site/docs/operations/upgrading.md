---
title: Upgrading LynxDB
description: Upgrade LynxDB safely -- self-update command, manual upgrade, rolling cluster upgrade, and version management.
---

# Upgrading LynxDB

LynxDB includes a built-in self-update mechanism. The binary can upgrade itself in place, or you can update through your package manager or container registry.

## Check for Updates

```bash
lynxdb upgrade --check
```

```
Update available: v0.4.0 -> v0.5.0
Changelog: https://github.com/lynxbase/lynxdb/releases/tag/v0.5.0
```

If you are already on the latest version:

```
LynxDB v0.5.0 is up to date.
```

## Self-Update (Single Binary)

The simplest way to upgrade. The binary downloads the new version, verifies the checksum, and performs an atomic swap:

```bash
lynxdb upgrade
```

```
Checking for updates...
Downloading lynxdb v0.5.0 for linux/amd64...
100%  15.0MB
Checksum verified
Upgraded: v0.4.0 -> v0.5.0
```

### Upgrade to a Specific Version

```bash
lynxdb upgrade --version v0.5.0-rc.1
```

### How Self-Update Works

1. Fetches `manifest.json` from `dl.lynxdb.org` (fallback: GitHub Releases)
2. Compares the installed version with the latest version
3. Downloads the new binary to a temporary file while computing SHA256
4. Verifies the checksum matches the manifest
5. Performs an atomic rename: `.new` -> current binary, old binary -> `.old`
6. Cleans up `.old` file

The update is atomic -- if anything fails, the old binary remains in place.

## Install Script

Re-run the install script to upgrade:

```bash
curl -fsSL https://lynxdb.org/install.sh | sh
```

The script detects the existing installation and shows the version change:

```
LynxDB v0.4.0 is installed. Upgrading to v0.5.0...
```

### Install a Specific Version

```bash
LYNXDB_VERSION=v0.5.0 curl -fsSL https://lynxdb.org/install.sh | sh
```

## Homebrew

```bash
brew update
brew upgrade lynxbase/tap/lynxdb
```

## Docker

```bash
# Pull the latest image
docker pull ghcr.io/lynxbase/lynxdb:latest

# Or a specific version
docker pull ghcr.io/lynxbase/lynxdb:0.5.0
```

### Docker Compose

```bash
docker compose pull
docker compose up -d
```

### Kubernetes

```bash
# Update the image tag in your manifest
kubectl -n lynxdb set image statefulset/lynxdb lynxdb=ghcr.io/lynxbase/lynxdb:0.5.0

# Watch the rollout
kubectl -n lynxdb rollout status statefulset/lynxdb
```

## Single-Node Upgrade Procedure

### With systemd

```bash
# 1. Check for updates
lynxdb upgrade --check

# 2. Stop the server
sudo systemctl stop lynxdb

# 3. Upgrade the binary
sudo lynxdb upgrade
# or: curl -fsSL https://lynxdb.org/install.sh | sudo sh
# or: sudo lynxdb install --yes   (re-running install upgrades the binary in place)

# 4. Verify the new version
lynxdb version

# 5. Start the server
sudo systemctl start lynxdb

# 6. Verify health
lynxdb health
lynxdb status
```

**Downtime:** A few seconds (stop -> upgrade -> start).

### Zero-Downtime with Load Balancer

If you have a load balancer in front of LynxDB:

```bash
# 1. Remove from load balancer
# (configure health check to fail, or manually remove)

# 2. Wait for in-flight requests to complete
sleep 10

# 3. Stop, upgrade, start
sudo systemctl stop lynxdb
sudo lynxdb upgrade
sudo systemctl start lynxdb

# 4. Verify health
lynxdb health

# 5. Re-add to load balancer
```

## Cluster Rolling Upgrade

For cluster deployments, upgrade one node at a time to maintain availability:

### Small Cluster (All Roles)

```bash
# Upgrade nodes one at a time
for node in node-1 node-2 node-3; do
  echo "Upgrading $node..."

  # Stop the node
  ssh $node "sudo systemctl stop lynxdb"

  # Upgrade
  ssh $node "sudo lynxdb upgrade"

  # Start
  ssh $node "sudo systemctl start lynxdb"

  # Wait for the node to rejoin the cluster
  sleep 30

  # Verify health
  ssh $node "lynxdb health"

  echo "$node upgraded successfully."
done
```

### Protocol Version Compatibility

LynxDB cluster nodes check protocol version compatibility during the `Handshake` RPC. Nodes with different protocol versions cannot join the same cluster. During a rolling upgrade:

- Nodes running the **old** version continue to communicate normally.
- The **new** binary must support the same protocol version as the old binary (backward-compatible within a minor version).
- Major version upgrades that bump the protocol version require upgrading all nodes within a maintenance window.

Check the release notes for protocol version changes before upgrading.

### Large Cluster (Role-Separated)

Upgrade in this order to minimize disruption:

1. **Query nodes first** (stateless, easiest to replace)
2. **Ingest nodes second** (stateless after flush)
3. **Meta nodes last** (maintain Raft quorum)

```bash
# 1. Query nodes (can upgrade in parallel, but rolling is safer)
for node in query-1 query-2 query-3; do
  ssh $node "sudo systemctl stop lynxdb && sudo lynxdb upgrade && sudo systemctl start lynxdb"
  sleep 15
  ssh $node "lynxdb health"
done

# 2. Ingest nodes (one at a time, wait for shard reassignment ~25s)
for node in ingest-1 ingest-2 ingest-3; do
  ssh $node "sudo systemctl stop lynxdb && sudo lynxdb upgrade && sudo systemctl start lynxdb"
  sleep 35  # Wait for shard reassignment (~25s) + rejoin
  ssh $node "lynxdb health"
done

# 3. Meta nodes (one at a time, maintain quorum)
for node in meta-1 meta-2 meta-3; do
  ssh $node "sudo systemctl stop lynxdb && sudo lynxdb upgrade && sudo systemctl start lynxdb"
  sleep 35  # Wait for Raft leader election + lease renewal
  ssh $node "lynxdb health"
done
```

### Kubernetes Rolling Update

```yaml
# Update the image in the StatefulSet
spec:
  template:
    spec:
      containers:
        - name: lynxdb
          image: ghcr.io/lynxbase/lynxdb:0.5.0  # New version
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      partition: 0  # Update all pods
```

```bash
kubectl -n lynxdb rollout status statefulset/lynxdb
```

## Version Compatibility

### Data Format Compatibility

LynxDB maintains backward compatibility for the `.lsg` segment format. Newer versions can read segments written by older versions. In rare cases, a major version upgrade may require a one-time migration:

```bash
# If needed (check release notes), run the migration
lynxdb migrate
```

### Config Compatibility

New config keys are added with sensible defaults. Existing config files continue to work without changes. Unknown keys produce a warning during `lynxdb config validate`.

## Rollback

If an upgrade causes issues:

### Single Node

```bash
# The old binary is saved as lynxdb.old after self-update
sudo systemctl stop lynxdb
sudo mv /usr/local/bin/lynxdb.old /usr/local/bin/lynxdb
sudo systemctl start lynxdb
```

### Install a Specific Older Version

```bash
LYNXDB_VERSION=v0.4.0 curl -fsSL https://lynxdb.org/install.sh | sh
```

### Docker

```bash
docker pull ghcr.io/lynxbase/lynxdb:0.4.0
docker compose up -d
```

## Release Channels

| Channel | URL | Description |
|---------|-----|-------------|
| Stable | `dl.lynxdb.org/manifest.json` | Production-ready releases |
| Pre-release | `lynxdb upgrade --version v0.5.0-rc.1` | Release candidates for testing |

## Checking Version

```bash
lynxdb version

# Output:
# LynxDB v0.5.0 (abc1234) built 2026-03-01T10:00:00Z
# Go: go1.25.4 linux/amd64
```

## Next Steps

- [Backup and Restore](/docs/operations/backup-restore) -- back up before upgrading
- [Monitoring](/docs/operations/monitoring) -- verify health after upgrade
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose post-upgrade issues
- [Configuration Overview](/docs/configuration/overview) -- check for new config options
