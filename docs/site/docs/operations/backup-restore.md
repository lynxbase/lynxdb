---
title: Backup and Restore
description: Back up and restore LynxDB -- the data directory layout, consistent copies of a stopped server or a filesystem snapshot, and restoring whole indexes.
---

# Backup and Restore

LynxDB keeps everything it stores in the `data_dir` directory, one subdirectory of `indexes/` per index. A backup is a copy of that directory taken at one moment: from a stopped server, or from an atomic filesystem snapshot.

## What to Back Up

| Component | Location | Contains |
|-----------|----------|----------|
| Indexes | `<data_dir>/indexes/<index>/` | The index's `MANIFEST`, `MANIFEST.end` and `.part` files |
| API keys | `<data_dir>/auth/keys.json` | Keys, when authentication is on |
| TLS certificate | `<data_dir>/tls/` | The self-signed certificate and key, when `--tls` generated them |
| Config file | `/etc/lynxdb/config.yaml` or `~/.config/lynxdb/config.yaml` | Server configuration |
| CLI credentials | `credentials.yaml` in the user config directory of each client | Keys saved by `lynxdb login` |

`<data_dir>/lynxdb.pid` holds the PID of the running server and needs no backup.

### Inside an index directory

```text
/var/lib/lynxdb/main/
├── 0000000000000000.part
├── 0000000000000001.part
├── MANIFEST
└── MANIFEST.end
```

- A `.part` file is immutable once written. New events and merges add part files; a merge then deletes the parts it replaced.
- `MANIFEST` is an append-only log of the changes to the set of live parts. A change is visible only after its record is on disk.
- `MANIFEST.end` records how far the manifest reached, so that a manifest that lost records is detected.

When a store opens, it reads the manifest, opens every part it names, and deletes the files no record names: temporary files of writes that did not finish, and parts a merge replaced. A part file that no record names while the manifest may have lost records is moved to `lost+found/` in the index directory instead of being deleted. A manifest that names a missing or damaged part, or that is shorter than its end mark, stops the server at startup and leaves every file as it was.

Because merges delete part files while the server runs, a file-by-file copy of a running server can hold a manifest that names parts the copy lacks. Copy a stopped server, or a snapshot.

## Single-Node Backup

### Stop and Copy

Stopping the server writes the events it holds in memory and closes every index:

```bash
sudo systemctl stop lynxdb

tar czf /backups/lynxdb-$(date +%Y%m%d-%H%M%S).tar.gz \
  /var/lib/lynxdb \
  /etc/lynxdb/config.yaml

sudo systemctl start lynxdb
```

The server is down for the time of the copy.

### Filesystem Snapshot

A snapshot that captures the whole data directory at one instant (LVM, ZFS, Btrfs, cloud volume snapshots) is a consistent copy of a running server. It is what the server would find after a power cut: the events of the last flush interval may be missing (see [Durability](#durability)).

```bash
# LVM snapshot
sudo lvcreate --snapshot --name lynxdb-snap --size 10G /dev/vg0/lynxdb
sudo mount /dev/vg0/lynxdb-snap /mnt/lynxdb-snap
tar czf /backups/lynxdb-$(date +%Y%m%d-%H%M%S).tar.gz -C /mnt/lynxdb-snap .
sudo umount /mnt/lynxdb-snap
sudo lvremove -f /dev/vg0/lynxdb-snap
```

```bash
# ZFS snapshot
sudo zfs snapshot tank/lynxdb@backup-$(date +%Y%m%d)
sudo zfs send tank/lynxdb@backup-$(date +%Y%m%d) > /backups/lynxdb-snapshot.zfs
```

The snapshot must cover the whole data directory; snapshots of separate volumes are not taken at the same instant.

### Incremental Copies with rsync

Part files never change once written, so `rsync` copies only the parts added since the last run. Run it against a stopped server or a mounted snapshot, not the live directory:

```bash
rsync -a --delete /mnt/lynxdb-snap/ /backups/lynxdb-latest/
```

### Backup Script

```bash
#!/bin/bash
# lynxdb-backup.sh -- stop, copy, start
set -euo pipefail

BACKUP_DIR="/backups/lynxdb"
DATA_DIR="/var/lib/lynxdb"
CONFIG="/etc/lynxdb/config.yaml"
BACKUP_FILE="${BACKUP_DIR}/lynxdb-$(date +%Y%m%d-%H%M%S).tar.gz"

mkdir -p "$BACKUP_DIR"

systemctl stop lynxdb
trap 'systemctl start lynxdb' EXIT
tar czf "$BACKUP_FILE" "$DATA_DIR" "$CONFIG"

echo "Backup complete: $BACKUP_FILE ($(du -h "$BACKUP_FILE" | cut -f1))"

# Keep the last 7 backups
ls -t "${BACKUP_DIR}"/lynxdb-*.tar.gz | tail -n +8 | xargs -r rm
```

## Durability

An ingest request is answered once its events are in the server's buffers. The store writes buffered events to part files within `storage.flush_interval` (one second by default) and syncs them before a query, a stats request or a Splunk HEC acknowledgement. Events that were answered but not yet written are lost if the process or the machine stops abruptly; shippers that retry unacknowledged batches send them again. A clean stop writes every buffered event.

## Restoring from Backup

### Whole Server

```bash
sudo systemctl stop lynxdb
sudo rm -rf /var/lib/lynxdb/*
sudo tar xzf /backups/lynxdb-20260301-020000.tar.gz -C /
sudo chown -R lynxdb:lynxdb /var/lib/lynxdb
sudo systemctl start lynxdb

lynxdb health
lynxdb status
```

### One Index

Every index lives in its own directory, so one index can be restored on its own. Copy the whole directory, from one backup:

```bash
sudo systemctl stop lynxdb
sudo rm -rf /var/lib/lynxdb/web
sudo tar xzf /backups/lynxdb-20260301-020000.tar.gz -C / var/lib/lynxdb/web
sudo chown -R lynxdb:lynxdb /var/lib/lynxdb/web
sudo systemctl start lynxdb
```

The server opens every index directory it finds at startup, so an index directory copied from another server appears under its directory name.

### Restore to a Different Server

```bash
scp /backups/lynxdb-20260301-020000.tar.gz new-server:/tmp/

ssh new-server
sudo tar xzf /tmp/lynxdb-20260301-020000.tar.gz -C /
sudo chown -R lynxdb:lynxdb /var/lib/lynxdb
sudo systemctl start lynxdb
```

## Testing Backups

Restore a backup into a scratch container and compare its counts with the source:

```bash
mkdir -p /tmp/lynxdb-restore
tar xzf /backups/lynxdb-20260301-020000.tar.gz -C /tmp/lynxdb-restore

docker run -d --name lynxdb-restore-test \
  -p 3101:3100 \
  -v /tmp/lynxdb-restore/var/lib/lynxdb:/data \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  -e LYNXDB_RETENTION=0 \
  ghcr.io/lynxbase/lynxdb:latest

curl -s localhost:3101/api/v1/stats | jq .data.total_events
docker rm -f lynxdb-restore-test
```

`LYNXDB_RETENTION=0` keeps the test server from deleting partitions that have expired since the backup.

## Next Steps

- [Retention Policies](/docs/operations/retention) -- manage data lifecycle
- [Upgrading](/docs/operations/upgrading) -- safe upgrade procedures
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose issues
