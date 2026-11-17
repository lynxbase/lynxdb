---
title: S3 Tiering
description: Configure the object store that LynxDB moves older segments to, the tiering interval, and remote segment fetches.
---

# S3 Tiering

LynxDB can move older segments from local disk to an S3-compatible bucket and still query them: a query that needs a remote segment downloads it into a local cache first.

## How Tiering Works

```text
hot (local disk)  ->  warm (<prefix>warm/<index>/<id>.lsg)  ->  cold (<prefix>cold/<index>/<id>.lsg)
```

1. Every `storage.tiering_interval`, the server checks the segments produced by compaction.
2. A segment older than 7 days (counted from when it was written) is uploaded to the warm key. After the upload is confirmed with a HEAD request, the local file is deleted.
3. After 30 days the segment is copied to the cold key.
4. When a query needs a warm or cold segment, the server downloads it to `<data_dir>/segment-cache/<id>.lsg` and reads it from there on later queries. Concurrent requests for the same segment share one download.
5. Failed uploads are retried on later cycles.

The 7-day and 30-day ages are fixed per index. To move cold objects to Glacier or Deep Archive, add a bucket lifecycle rule on the `<prefix>cold/` prefix; LynxDB does not set storage classes.

:::caution Tiering without a bucket
The tiering loop runs on every disk-backed server. Without `storage.s3_bucket`, the server uses an in-process object store, so segments moved to the warm tier live only in memory and are lost on restart. If `retention` is longer than 7 days, configure a bucket.
:::

## Bucket Settings

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `storage.s3_bucket` | `--s3-bucket` | `LYNXDB_STORAGE_S3_BUCKET` | empty | Bucket name. |
| `storage.s3_region` | `--s3-region` | `LYNXDB_STORAGE_S3_REGION` | `us-east-1` | Bucket region. It takes precedence over `AWS_REGION`. |
| `storage.s3_prefix` | `--s3-prefix` | `LYNXDB_STORAGE_S3_PREFIX` | `lynxdb/` | Key prefix inside the bucket. |
| `storage.s3_endpoint` | none | `LYNXDB_STORAGE_S3_ENDPOINT` | empty | Endpoint URL for S3-compatible stores (MinIO, Ceph, R2). |
| `storage.s3_force_path_style` | none | `LYNXDB_STORAGE_S3_FORCE_PATH_STYLE` | `false` | Path-style addressing (`endpoint/bucket/key`), needed by MinIO and most self-hosted stores. |

All bucket settings need a restart. The server does not contact the bucket at startup; access errors show up in the log as `tiering: move to warm failed` on the first upload.

## Tiering and Fetch Settings

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `storage.tiering_interval` | `--tiering-interval` | `LYNXDB_STORAGE_TIERING_INTERVAL` | `5m` | How often tier moves run. Each cycle is bounded by the interval minus 5 seconds (at least 30 seconds). |
| `storage.remote_fetch_timeout` | none | `LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT` | `30s` | Timeout for downloading one remote segment during a query. |
| `storage.tiering_parallelism` | none | `LYNXDB_STORAGE_TIERING_PARALLELISM` | `3` | Validated (at least 1) but not read: uploads run one at a time per cycle. |
| `storage.segment_cache_size` | none | `LYNXDB_STORAGE_SEGMENT_CACHE_SIZE` | `10gb` | Not read: the segment cache directory is not size-bounded. Cached files are deleted when their segment expires. |

## Credentials

LynxDB uses the AWS SDK default credential chain:

1. `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_SESSION_TOKEN`
2. Shared config and credentials files (`~/.aws/config`, `~/.aws/credentials`, `AWS_PROFILE`)
3. Web identity tokens (EKS IAM Roles for Service Accounts)
4. ECS task roles and EC2 instance profiles

The identity needs `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject` and `s3:ListBucket` on the bucket and prefix. See [S3/MinIO Storage Backend](../deployment/s3-setup.md) for a policy and setup steps.

## Examples

AWS S3:

```yaml
storage:
  s3_bucket: "example-lynxdb-logs"
  s3_region: "eu-west-1"
  s3_prefix: "production/"
  tiering_interval: "5m"
  remote_fetch_timeout: "60s"
```

MinIO:

```yaml
storage:
  s3_bucket: "lynxdb"
  s3_region: "us-east-1"
  s3_endpoint: "http://minio:9000"
  s3_force_path_style: true
```

## Related

- [S3/MinIO Storage Backend](../deployment/s3-setup.md)
- [Storage Settings](./storage.md)
- [Storage Engine](../architecture/storage-engine.md)
- [Retention](../operations/retention.md)
