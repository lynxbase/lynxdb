---
title: S3 Tiering
description: Configure LynxDB S3 storage tiering -- S3 bucket, region, tiering intervals, endpoint overrides, and segment cache.
---

# S3 Tiering

LynxDB supports automatic tiered storage with S3-compatible object stores. Segments are promoted from hot (local SSD) to warm (S3) to cold (Glacier) based on age policies. S3 acts as the source of truth for segments in cluster deployments.

## How Tiering Works

```
Hot (local SSD)  -->  Warm (S3)  -->  Cold (Glacier)
  < 7 days            < 30 days       < 90 days
```

1. Fresh data lives on local SSD for fast query access
2. Segments older than the hot tier threshold are uploaded to S3
3. Segments older than the warm tier threshold are transitioned to Glacier (via S3 lifecycle rules)
4. A local segment cache keeps frequently queried warm-tier segments on disk for performance

## S3 Bucket Configuration

### Bucket Name

| Config Key | `storage.s3_bucket` |
|---|---|
| **CLI Flag** | `--s3-bucket` |
| **Env Var** | `LYNXDB_STORAGE_S3_BUCKET` |
| **Default** | `""` (tiering disabled) |

```yaml
storage:
  s3_bucket: "my-lynxdb-logs"
```

When `s3_bucket` is empty, tiering is disabled and all data stays on local disk.

### Region

| Config Key | `storage.s3_region` |
|---|---|
| **CLI Flag** | `--s3-region` |
| **Env Var** | `LYNXDB_STORAGE_S3_REGION` |
| **Default** | `us-east-1` |

```yaml
storage:
  s3_region: "eu-west-1"
```

### Key Prefix

Optional prefix for all keys in the S3 bucket. Useful for sharing a bucket across environments.

| Config Key | `storage.s3_prefix` |
|---|---|
| **CLI Flag** | `--s3-prefix` |
| **Env Var** | `LYNXDB_STORAGE_S3_PREFIX` |
| **Default** | `""` |

```yaml
storage:
  s3_prefix: "production/"
```

### Custom Endpoint (MinIO)

Override the S3 endpoint URL for S3-compatible stores like MinIO, Ceph, or R2.

| Config Key | `storage.s3_endpoint` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_S3_ENDPOINT` |
| **Default** | `""` (AWS S3) |

```yaml
storage:
  s3_endpoint: "http://minio.local:9000"
  s3_force_path_style: true
```

### Force Path Style

Use path-style S3 URLs (`http://host/bucket/key`) instead of virtual-hosted style (`http://bucket.host/key`). Required for MinIO and some S3-compatible stores.

| Config Key | `storage.s3_force_path_style` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_S3_FORCE_PATH_STYLE` |
| **Default** | `false` |

```yaml
storage:
  s3_force_path_style: true
```

## Tiering Interval

How often LynxDB checks for segments eligible for tier promotion.

| Config Key | `storage.tiering_interval` |
|---|---|
| **CLI Flag** | `--tiering-interval` |
| **Env Var** | `LYNXDB_STORAGE_TIERING_INTERVAL` |
| **Default** | `5m` |

```yaml
storage:
  tiering_interval: "5m"
```

## Tiering Parallelism

Number of concurrent segment uploads to S3.

| Config Key | `storage.tiering_parallelism` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_TIERING_PARALLELISM` |
| **Default** | `2` |

```yaml
storage:
  tiering_parallelism: 4
```

Increase for faster uploads when moving large volumes of data to S3. Be aware this increases network bandwidth usage.

## Segment Cache

Local disk cache for warm-tier segments fetched from S3. This avoids repeated S3 downloads for frequently queried data.

| Config Key | `storage.segment_cache_size` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_SEGMENT_CACHE_SIZE` |
| **Default** | `1gb` |

```yaml
storage:
  segment_cache_size: "10gb"
```

The cache uses LRU eviction. Set this to a size that fits your most frequently queried warm-tier data.

## AWS Credentials

LynxDB uses the standard AWS SDK credential chain:

1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. Shared credentials file (`~/.aws/credentials`)
3. IAM instance profile (EC2, ECS, EKS)
4. IAM role via STS (IRSA for EKS)

```bash
# Option 1: Environment variables
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
lynxdb server --s3-bucket my-logs --s3-region us-east-1

# Option 2: IAM instance profile (no credentials needed on EC2)
lynxdb server --s3-bucket my-logs --s3-region us-east-1
```

## MinIO Setup

For self-hosted S3-compatible storage:

```yaml
storage:
  s3_bucket: "lynxdb"
  s3_region: "us-east-1"
  s3_endpoint: "http://minio.local:9000"
  s3_force_path_style: true
```

```bash
# MinIO credentials via environment
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

lynxdb server \
  --s3-bucket lynxdb \
  --s3-region us-east-1 \
  --data-dir /var/lib/lynxdb
```

See [S3/MinIO Storage Backend Setup](/docs/deployment/s3-setup) for a detailed setup guide.

## Complete Example

```yaml
storage:
  s3_bucket: "company-lynxdb-logs"
  s3_region: "us-west-2"
  s3_prefix: "production/"
  tiering_interval: "5m"
  tiering_parallelism: 4
  segment_cache_size: "20gb"
  cache_max_bytes: "4gb"
```

```bash
lynxdb server \
  --data-dir /var/lib/lynxdb \
  --s3-bucket company-lynxdb-logs \
  --s3-region us-west-2 \
  --cache-max-mb 4gb
```

## Next Steps

- [S3/MinIO Storage Backend Setup](/docs/deployment/s3-setup) -- complete setup guide with IAM policies
- [Small Cluster](/docs/deployment/small-cluster) -- S3 as shared storage for cluster deployments
- [Large Cluster](/docs/deployment/large-cluster) -- role splitting with S3 source of truth
- [Storage Settings](/docs/configuration/storage) -- compression and compaction
