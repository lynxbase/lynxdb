---
title: Backup and Restore
description: Backup and restore strategies for LynxDB -- filesystem snapshots, S3-based backups, and disaster recovery.
---

# Backup and Restore

LynxDB stores all data in the `data_dir` directory. Backups are straightforward because the storage format is self-contained -- there are no external dependencies or distributed state to coordinate (except in cluster mode where S3 is the source of truth).

## What to Back Up

| Component | Location | Contains |
|-----------|----------|----------|
| Data directory | `data_dir` (e.g., `/var/lib/lynxdb`) | WAL, segments, indexes, metadata |
| Config file | `/etc/lynxdb/config.yaml` or `~/.config/lynxdb/config.yaml` | Server configuration |
| Credentials | `~/.config/lynxdb/credentials` | API keys (if auth is enabled) |

## Single-Node Backup

### Method 1: Stop-and-Copy (Simplest)

Stop the server, copy the data directory, restart:

```bash
# Stop the server
sudo systemctl stop lynxdb

# Create backup
tar czf /backups/lynxdb-$(date +%Y%m%d-%H%M%S).tar.gz \
  /var/lib/lynxdb \
  /etc/lynxdb/config.yaml

# Restart
sudo systemctl start lynxdb
```

Downtime: seconds to minutes depending on data size. This is the safest method because it guarantees a consistent snapshot.

### Method 2: Filesystem Snapshot (Zero Downtime)

If your filesystem supports snapshots (LVM, ZFS, Btrfs, EBS snapshots):

```bash
# LVM snapshot
sudo lvcreate --snapshot --name lynxdb-snap --size 10G /dev/vg0/lynxdb
sudo mount /dev/vg0/lynxdb-snap /mnt/lynxdb-snap
tar czf /backups/lynxdb-$(date +%Y%m%d-%H%M%S).tar.gz /mnt/lynxdb-snap
sudo umount /mnt/lynxdb-snap
sudo lvremove -f /dev/vg0/lynxdb-snap
```

```bash
# ZFS snapshot
sudo zfs snapshot tank/lynxdb@backup-$(date +%Y%m%d)
sudo zfs send tank/lynxdb@backup-$(date +%Y%m%d) > /backups/lynxdb-snapshot.zfs
```

```bash
# AWS EBS snapshot
VOLUME_ID=$(aws ec2 describe-volumes \
  --filters "Name=attachment.instance-id,Values=$(ec2-metadata -i | cut -d' ' -f2)" \
  --query 'Volumes[?Attachments[?Device==`/dev/xvdf`]].VolumeId' \
  --output text)
aws ec2 create-snapshot --volume-id $VOLUME_ID --description "lynxdb-backup-$(date +%Y%m%d)"
```

### Method 3: rsync (Incremental)

For incremental backups of a running server. Note: this may capture the WAL mid-write, so a WAL replay is needed on restore:

```bash
rsync -avz --delete \
  /var/lib/lynxdb/ \
  /backups/lynxdb-latest/
```

Set up as a cron job:

```bash
# /etc/cron.d/lynxdb-backup
0 2 * * * root rsync -avz --delete /var/lib/lynxdb/ /backups/lynxdb-latest/ 2>&1 | logger -t lynxdb-backup
```

## S3-Based Backup

When S3 tiering is enabled, S3 already contains all compacted segments. For a complete backup strategy:

### Segments (Already in S3)

If you use S3 tiering, compacted segments are already in S3. Your backup strategy only needs to cover:
- In-flight data (WAL + memtable) on ingest nodes
- Metadata (config, API keys, materialized view definitions)

### Full S3 Backup

Enable S3 cross-region replication for disaster recovery:

```bash
# Enable versioning (required for replication)
aws s3api put-bucket-versioning \
  --bucket my-lynxdb-logs \
  --versioning-configuration Status=Enabled

# Set up cross-region replication
aws s3api put-bucket-replication \
  --bucket my-lynxdb-logs \
  --replication-configuration '{
    "Role": "arn:aws:iam::123456789012:role/replication-role",
    "Rules": [{
      "Status": "Enabled",
      "Destination": {
        "Bucket": "arn:aws:s3:::my-lynxdb-logs-dr",
        "StorageClass": "STANDARD_IA"
      }
    }]
  }'
```

### Backup Script

```bash
#!/bin/bash
# lynxdb-backup.sh -- full backup for single-node LynxDB

set -euo pipefail

BACKUP_DIR="/backups/lynxdb"
DATA_DIR="/var/lib/lynxdb"
CONFIG="/etc/lynxdb/config.yaml"
DATE=$(date +%Y%m%d-%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/lynxdb-${DATE}.tar.gz"

mkdir -p "$BACKUP_DIR"

echo "Starting LynxDB backup..."

# Create backup (server continues running)
tar czf "$BACKUP_FILE" \
  "$DATA_DIR" \
  "$CONFIG" \
  2>/dev/null

BACKUP_SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo "Backup complete: $BACKUP_FILE ($BACKUP_SIZE)"

# Rotate old backups (keep last 7)
ls -t "${BACKUP_DIR}"/lynxdb-*.tar.gz | tail -n +8 | xargs -r rm
echo "Old backups rotated."
```

## Restoring from Backup

### Single-Node Restore

```bash
# Stop the server
sudo systemctl stop lynxdb

# Remove existing data
sudo rm -rf /var/lib/lynxdb/*

# Extract backup
sudo tar xzf /backups/lynxdb-20260301-020000.tar.gz -C /

# Fix ownership
sudo chown -R lynxdb:lynxdb /var/lib/lynxdb

# Start the server (WAL replay happens automatically)
sudo systemctl start lynxdb

# Verify
lynxdb health
lynxdb status
```

### Restore to a Different Server

```bash
# Copy backup to new server
scp /backups/lynxdb-20260301-020000.tar.gz new-server:/tmp/

# On new server
ssh new-server
sudo tar xzf /tmp/lynxdb-20260301-020000.tar.gz -C /
sudo chown -R lynxdb:lynxdb /var/lib/lynxdb
sudo systemctl start lynxdb
```

### Restore from S3

If S3 was the source of truth (cluster mode), a new node will automatically download segments from S3 on startup. You only need to configure the new node with the same S3 bucket:

```yaml
storage:
  s3_bucket: "my-lynxdb-logs"
  s3_region: "us-east-1"
```

```bash
lynxdb server --config /etc/lynxdb/config.yaml
# Segments are fetched from S3 on demand
```

## Cluster Backup

In cluster mode with S3 shared storage:

- **Segments**: Already in S3 (source of truth). Back up using S3 versioning and cross-region replication.
- **Metadata**: Back up the meta node data directories (Raft state). Only one meta node's state is needed for restore.
- **Config**: Back up config files for all node roles.

```bash
# Back up meta node state
tar czf /backups/lynxdb-meta-$(date +%Y%m%d).tar.gz /var/lib/lynxdb
```

## Testing Backups

Periodically verify that backups can be restored:

```bash
# Spin up a test instance
docker run -d \
  --name lynxdb-restore-test \
  -p 3101:3100 \
  -v /backups/lynxdb-latest:/data \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  OrlovEvgeny/Lynxdb:latest

# Verify data is intact
curl -s localhost:3101/api/v1/stats | jq .events_total
docker rm -f lynxdb-restore-test
```

## Next Steps

- [Retention Policies](/docs/operations/retention) -- manage data lifecycle
- [S3 Storage Setup](/docs/deployment/s3-setup) -- S3-based storage
- [Upgrading](/docs/operations/upgrading) -- safe upgrade procedures
- [Troubleshooting](/docs/operations/troubleshooting) -- diagnose issues
