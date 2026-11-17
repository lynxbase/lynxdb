---
title: S3/MinIO Storage Backend
description: Set up AWS S3, MinIO or another S3-compatible store for LynxDB tiered storage.
---

# S3/MinIO Storage Backend

LynxDB moves segments older than 7 days from local disk to an S3-compatible bucket (the warm tier, under `<prefix>warm/`) and copies them to `<prefix>cold/` after 30 days. Queries download remote segments into `<data_dir>/segment-cache/` on demand. [S3 Tiering](../configuration/s3-tiering.md) describes the settings and the tiering cycle.

## AWS S3

### 1. Create a bucket

```bash
aws s3 mb s3://example-lynxdb-logs --region eu-west-1
```

### 2. Grant access

Tiering issues `PutObject`, `GetObject` (including ranged reads), `HeadObject`, `CopyObject` and `DeleteObject`. With `s3:ListBucket`, a `HeadObject` on a missing key answers 404 instead of 403:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::example-lynxdb-logs/production/*"
    },
    {
      "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::example-lynxdb-logs",
      "Condition": {"StringLike": {"s3:prefix": "production/*"}}
    }
  ]
}
```

### 3. Provide credentials

LynxDB uses the AWS SDK default credential chain, so any of these works:

- `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` in the server environment
- A shared credentials file or `AWS_PROFILE` for the server's user
- An EC2 instance profile or ECS task role
- EKS IAM Roles for Service Accounts (see [Kubernetes](./kubernetes.md#s3-tiering))

### 4. Configure LynxDB

```yaml
storage:
  s3_bucket: "example-lynxdb-logs"
  s3_region: "eu-west-1"
  s3_prefix: "production/"
  tiering_interval: "5m"
```

or with flags:

```bash
lynxdb server --data-dir /var/lib/lynxdb \
  --s3-bucket example-lynxdb-logs --s3-region eu-west-1 --s3-prefix production/
```

Restart the server after changing bucket settings.

### 5. Optional: archive the cold tier

LynxDB does not set storage classes. A lifecycle rule on the cold prefix moves those objects to cheaper storage:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket example-lynxdb-logs \
  --lifecycle-configuration '{
    "Rules": [{
      "ID": "lynxdb-cold",
      "Status": "Enabled",
      "Filter": {"Prefix": "production/cold/"},
      "Transitions": [{"Days": 0, "StorageClass": "GLACIER_IR"}]
    }]
  }'
```

Use a class with millisecond retrieval (such as Glacier Instant Retrieval): a query that touches a cold segment reads it directly, and classes that need a restore request fail those reads.

## MinIO

```bash
docker run -d --name minio -p 9000:9000 -p 9001:9001 \
  -v minio-data:/data \
  -e MINIO_ROOT_USER=minioadmin -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/lynxdb
```

```yaml
storage:
  s3_bucket: "lynxdb"
  s3_region: "us-east-1"
  s3_endpoint: "http://localhost:9000"
  s3_force_path_style: true
```

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin
lynxdb server --config /etc/lynxdb/config.yaml
```

A Docker Compose file with LynxDB, MinIO and a bucket-creation job is in [Docker Deployment](./docker.md#with-minio-for-s3-tiering).

## Other S3-Compatible Stores

| Store | `s3_endpoint` | `s3_force_path_style` |
|-------|---------------|-----------------------|
| AWS S3 | empty | `false` |
| MinIO | `http://minio:9000` | `true` |
| Ceph RadosGW | gateway URL | `true` |
| Cloudflare R2 | `https://<account-id>.r2.cloudflarestorage.com` | `true` |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com` | `false` |
| Backblaze B2 | `https://s3.<region>.backblazeb2.com` | `false` |

Set `s3_region` to the region name the store expects (`auto` for R2).

## Verifying

The server does not contact the bucket at startup. To confirm that tiering works:

- Watch the log for `tiering: move to warm failed`, which carries the S3 error (credentials, region, endpoint).
- Check the tiering counters on `/metrics`:

```bash
curl -s http://localhost:3100/metrics | grep -E '^lynxdb_tiering_(uploads|downloads)'
```

`lynxdb_tiering_uploads_total` grows when segments move to the bucket, and `lynxdb_tiering_downloads_total` when queries fetch them back. Uploads start once compacted segments are older than 7 days.

## Next Steps

- [S3 Tiering settings](../configuration/s3-tiering.md)
- [Docker Deployment](./docker.md)
- [Kubernetes Deployment](./kubernetes.md)
- [Backup and Restore](../operations/backup-restore.md)
