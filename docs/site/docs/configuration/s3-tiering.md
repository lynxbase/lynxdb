---
title: S3 Tiering
description: Configure LynxDB object-store tiering -- S3 bucket, region, tiering interval, endpoint overrides, and the local segment cache.
---

# S3 Tiering

LynxDB can offload older segments to S3-compatible object storage while keeping them queryable through a local cache.

:::note Status
The built-in tiering loop evaluates segment age on `storage.tiering_interval`, uploads eligible hot segments to object storage, and later moves warm segments into the cold namespace. If you want the cold tier to use Glacier or Deep Archive underneath, add bucket lifecycle rules at the S3 layer.
:::

## How Tiering Works

```
Hot (local disk)  -->  Warm (object store)  -->  Cold (object store / archive policy)
```

1. Fresh parts stay on local disk for fast reads.
2. The background tiering loop checks segment age against the index config.
3. Eligible hot segments are uploaded to object storage and marked warm.
4. Warm segments can later be promoted to cold object keys.
5. Queries fetch remote segments into the local segment cache on demand.

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

| Config Key | `storage.s3_prefix` |
|---|---|
| **CLI Flag** | `--s3-prefix` |
| **Env Var** | `LYNXDB_STORAGE_S3_PREFIX` |
| **Default** | `lynxdb/` |

```yaml
storage:
  s3_prefix: "production/"
```

### Custom Endpoint

Use this for MinIO, Ceph, or other S3-compatible stores.

| Config Key | `storage.s3_endpoint` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_S3_ENDPOINT` |
| **Default** | `""` |

```yaml
storage:
  s3_endpoint: "http://minio.local:9000"
  s3_force_path_style: true
```

### Force Path Style

| Config Key | `storage.s3_force_path_style` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_S3_FORCE_PATH_STYLE` |
| **Default** | `false` |

## Tiering Interval

How often LynxDB evaluates segments for hot-to-warm or warm-to-cold movement.

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

Maximum concurrent tier-upload workers.

| Config Key | `storage.tiering_parallelism` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_TIERING_PARALLELISM` |
| **Default** | `3` |

```yaml
storage:
  tiering_parallelism: 3
```

## Segment Cache

Local cache for remote segments fetched from object storage.

| Config Key | `storage.segment_cache_size` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_SEGMENT_CACHE_SIZE` |
| **Default** | `10gb` |

```yaml
storage:
  segment_cache_size: "20gb"
```

The cache survives restarts and reduces repeated object-store downloads for commonly queried warm or cold segments.

## Remote Fetch Timeout

| Config Key | `storage.remote_fetch_timeout` |
|---|---|
| **Env Var** | `LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT` |
| **Default** | `30s` |

```yaml
storage:
  remote_fetch_timeout: "30s"
```

## AWS Credentials

LynxDB uses the standard AWS SDK credential chain:

1. Environment variables
2. Shared credentials file
3. Instance profile or task role
4. Web identity / IRSA

## MinIO Example

```yaml
storage:
  s3_bucket: "lynxdb"
  s3_region: "us-east-1"
  s3_endpoint: "http://minio.local:9000"
  s3_force_path_style: true
  tiering_interval: "5m"
  segment_cache_size: "20gb"
```

## Complete Example

```yaml
storage:
  s3_bucket: "company-lynxdb-logs"
  s3_region: "us-west-2"
  s3_prefix: "production/"
  tiering_interval: "5m"
  tiering_parallelism: 3
  segment_cache_size: "20gb"
  remote_fetch_timeout: "30s"
```

## Next Steps

- [Storage Engine](/docs/architecture/storage-engine)
- [Storage Settings](/docs/configuration/storage)
- [S3/MinIO Storage Backend Setup](/docs/deployment/s3-setup)
- [Large Cluster](/docs/deployment/large-cluster)
