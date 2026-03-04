---
title: S3/MinIO Storage Backend
description: Set up S3 or MinIO as the shared storage backend for LynxDB tiered storage and cluster deployments.
---

# S3/MinIO Storage Backend

S3-compatible object storage is used for two purposes in LynxDB:

1. **Tiered storage** -- automatically move older segments from local SSD to S3 (warm tier) and Glacier (cold tier)
2. **Shared storage** -- in cluster mode, S3 is the source of truth for segments, enabling stateless ingest and query nodes

## AWS S3 Setup

### 1. Create an S3 Bucket

```bash
aws s3 mb s3://my-lynxdb-logs --region us-east-1
```

### 2. Create an IAM Policy

Create a policy with the minimum required permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::my-lynxdb-logs",
        "arn:aws:s3:::my-lynxdb-logs/*"
      ]
    }
  ]
}
```

```bash
aws iam create-policy \
  --policy-name LynxDBS3Access \
  --policy-document file://lynxdb-s3-policy.json
```

### 3. Configure Credentials

**Option A: Environment variables**

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

lynxdb server --s3-bucket my-lynxdb-logs --s3-region us-east-1
```

**Option B: AWS credentials file**

```ini
# ~/.aws/credentials
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

**Option C: IAM instance profile (EC2)**

No credentials needed. Attach the IAM role to the EC2 instance.

**Option D: IRSA (EKS)**

```yaml
# Kubernetes ServiceAccount
apiVersion: v1
kind: ServiceAccount
metadata:
  name: lynxdb
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/lynxdb-s3-role
```

### 4. Configure LynxDB

```yaml
# config.yaml
storage:
  s3_bucket: "my-lynxdb-logs"
  s3_region: "us-east-1"
  s3_prefix: "production/"           # Optional: namespace within bucket
  tiering_interval: "5m"
  tiering_parallelism: 4
  segment_cache_size: "20gb"
```

```bash
lynxdb server \
  --data-dir /var/lib/lynxdb \
  --s3-bucket my-lynxdb-logs \
  --s3-region us-east-1
```

### 5. Configure S3 Lifecycle Rules (Optional)

Set up automatic transitions to Glacier for cold storage:

```bash
aws s3api put-bucket-lifecycle-configuration \
  --bucket my-lynxdb-logs \
  --lifecycle-configuration '{
    "Rules": [
      {
        "ID": "ColdTier",
        "Status": "Enabled",
        "Filter": {"Prefix": "production/"},
        "Transitions": [
          {
            "Days": 90,
            "StorageClass": "GLACIER"
          }
        ]
      }
    ]
  }'
```

## MinIO Setup

MinIO provides S3-compatible object storage that you can self-host.

### 1. Deploy MinIO

```bash
# Docker
docker run -d \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -v minio-data:/data \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"
```

### 2. Create a Bucket

```bash
# Install mc (MinIO Client)
# Then create a bucket:
mc alias set local http://localhost:9000 minioadmin minioadmin
mc mb local/lynxdb
```

### 3. Configure LynxDB

```yaml
# config.yaml
storage:
  s3_bucket: "lynxdb"
  s3_region: "us-east-1"
  s3_endpoint: "http://minio.local:9000"
  s3_force_path_style: true
  tiering_interval: "5m"
  segment_cache_size: "10gb"
```

```bash
export AWS_ACCESS_KEY_ID=minioadmin
export AWS_SECRET_ACCESS_KEY=minioadmin

lynxdb server \
  --data-dir /var/lib/lynxdb \
  --s3-bucket lynxdb \
  --s3-region us-east-1
```

### Docker Compose with MinIO

```yaml
services:
  lynxdb:
    image: ghcr.io/lynxbase/lynxdb:latest
    ports:
      - "3100:3100"
    volumes:
      - lynxdb-data:/data
    environment:
      LYNXDB_LISTEN: "0.0.0.0:3100"
      LYNXDB_DATA_DIR: "/data"
      LYNXDB_STORAGE_S3_BUCKET: "lynxdb"
      LYNXDB_STORAGE_S3_REGION: "us-east-1"
      LYNXDB_STORAGE_S3_ENDPOINT: "http://minio:9000"
      LYNXDB_STORAGE_S3_FORCE_PATH_STYLE: "true"
      AWS_ACCESS_KEY_ID: "minioadmin"
      AWS_SECRET_ACCESS_KEY: "minioadmin"
    depends_on:
      minio-init:
        condition: service_completed_successfully

  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    ports:
      - "9000:9000"
      - "9001:9001"
    volumes:
      - minio-data:/data
    environment:
      MINIO_ROOT_USER: "minioadmin"
      MINIO_ROOT_PASSWORD: "minioadmin"
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 10s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin;
      mc mb --ignore-existing local/lynxdb;
      "

volumes:
  lynxdb-data:
  minio-data:
```

## Other S3-Compatible Stores

LynxDB works with any S3-compatible object store by setting `s3_endpoint` and `s3_force_path_style`:

| Provider | Endpoint | Notes |
|----------|----------|-------|
| AWS S3 | (default) | Standard AWS SDK credential chain |
| MinIO | `http://minio:9000` | Set `s3_force_path_style: true` |
| Cloudflare R2 | `https://<account-id>.r2.cloudflarestorage.com` | Set `s3_force_path_style: true` |
| DigitalOcean Spaces | `https://<region>.digitaloceanspaces.com` | |
| Backblaze B2 | `https://s3.<region>.backblazeb2.com` | |
| Ceph (RadosGW) | Your Ceph gateway URL | Set `s3_force_path_style: true` |

## Segment Cache

Query nodes maintain a local cache of frequently accessed warm-tier segments. Configure the cache size based on available disk space:

```yaml
storage:
  segment_cache_size: "50gb"    # Local cache for warm-tier segments
```

The cache uses LRU eviction. Size it to hold the segments most frequently queried. A good starting point is 2-4x the hot tier size.

## Verifying S3 Connectivity

After starting the server with S3 configured:

```bash
# Check server status (should show S3 configuration)
lynxdb status

# Run diagnostics
lynxdb doctor

# Ingest some data and wait for tiering
lynxdb demo &
sleep 300  # Wait for tiering interval
lynxdb status  # Check segment distribution
```

## Next Steps

- [S3 Tiering Configuration](/docs/configuration/s3-tiering) -- detailed tiering settings
- [Small Cluster](/docs/deployment/small-cluster) -- cluster with S3 shared storage
- [Large Cluster](/docs/deployment/large-cluster) -- role-separated cluster
- [Backup and Restore](/docs/operations/backup-restore) -- S3-based backup strategies
