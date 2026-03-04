---
title: Docker Deployment
description: Deploy LynxDB with Docker and Docker Compose -- images, volumes, environment variables, and multi-service examples.
---

# Docker Deployment

LynxDB provides official Docker images built from scratch (no OS, just the static binary). The image is small (~15MB) and runs with zero dependencies.

## Quick Start

```bash
# Run LynxDB with persistent storage
docker run -d \
  --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  ghcr.io/lynxbase/lynxdb:latest

# Verify
curl http://localhost:3100/health
```

## Docker Images

| Image | Description |
|-------|-------------|
| `ghcr.io/lynxbase/lynxdb:latest` | Latest stable release |
| `ghcr.io/lynxbase/lynxdb:0.5.0` | Specific version |
| `ghcr.io/lynxbase/lynxdb:0.5` | Latest patch for minor version |

Images are available for `linux/amd64` and `linux/arm64`.

## Docker Run

### Minimal (In-Memory)

```bash
docker run -d \
  --name lynxdb \
  -p 3100:3100 \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  ghcr.io/lynxbase/lynxdb:latest
```

### Persistent Storage

```bash
docker run -d \
  --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  -e LYNXDB_RETENTION=30d \
  ghcr.io/lynxbase/lynxdb:latest
```

### With S3 Tiering

```bash
docker run -d \
  --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  -e LYNXDB_STORAGE_S3_BUCKET=my-logs \
  -e LYNXDB_STORAGE_S3_REGION=us-east-1 \
  -e AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE \
  -e AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY \
  ghcr.io/lynxbase/lynxdb:latest
```

### With Config File

```bash
docker run -d \
  --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  -v /path/to/config.yaml:/etc/lynxdb/config.yaml:ro \
  ghcr.io/lynxbase/lynxdb:latest server --config /etc/lynxdb/config.yaml
```

### With TLS and Auth

```bash
docker run -d \
  --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  -v /path/to/certs:/certs:ro \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  ghcr.io/lynxbase/lynxdb:latest server \
    --tls-cert /certs/lynxdb.crt \
    --tls-key /certs/lynxdb.key \
    --auth
```

## Docker Compose

### Single Node

```yaml
# docker-compose.yaml
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
      LYNXDB_RETENTION: "30d"
      LYNXDB_LOG_LEVEL: "info"
      LYNXDB_STORAGE_COMPRESSION: "lz4"
      LYNXDB_STORAGE_CACHE_MAX_BYTES: "2gb"
      LYNXDB_QUERY_MAX_CONCURRENT: "20"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "lynxdb", "health", "--server", "http://localhost:3100"]
      interval: 30s
      timeout: 5s
      retries: 3

volumes:
  lynxdb-data:
```

### With MinIO for S3 Tiering

```yaml
# docker-compose.yaml
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
      LYNXDB_RETENTION: "90d"
      LYNXDB_STORAGE_S3_BUCKET: "lynxdb"
      LYNXDB_STORAGE_S3_REGION: "us-east-1"
      LYNXDB_STORAGE_S3_ENDPOINT: "http://minio:9000"
      LYNXDB_STORAGE_S3_FORCE_PATH_STYLE: "true"
      LYNXDB_STORAGE_SEGMENT_CACHE_SIZE: "5gb"
      AWS_ACCESS_KEY_ID: "minioadmin"
      AWS_SECRET_ACCESS_KEY: "minioadmin"
    depends_on:
      minio:
        condition: service_healthy
    restart: unless-stopped

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

### Full Stack with Log Shipping

```yaml
# docker-compose.yaml
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
      LYNXDB_RETENTION: "30d"
    restart: unless-stopped

  # Example app generating logs
  app:
    image: your-app:latest
    logging:
      driver: "fluentd"
      options:
        fluentd-address: "localhost:24224"
        tag: "app.{{.Name}}"

  # Fluentd ships logs to LynxDB via Elasticsearch _bulk API
  fluentd:
    image: fluent/fluentd:latest
    ports:
      - "24224:24224"
    volumes:
      - ./fluentd.conf:/fluentd/etc/fluent.conf:ro
    depends_on:
      - lynxdb

volumes:
  lynxdb-data:
```

## Resource Limits

Set memory and CPU limits appropriate for your workload:

```yaml
services:
  lynxdb:
    image: ghcr.io/lynxbase/lynxdb:latest
    deploy:
      resources:
        limits:
          memory: 4g
          cpus: "4"
        reservations:
          memory: 1g
          cpus: "1"
```

## Dockerfile for Custom Image

If you need to customize the image (e.g., add a config file):

```dockerfile
FROM ghcr.io/lynxbase/lynxdb:latest

COPY config.yaml /etc/lynxdb/config.yaml

ENTRYPOINT ["lynxdb"]
CMD ["server", "--config", "/etc/lynxdb/config.yaml"]
```

## Health Checks

The `/health` endpoint returns `200 OK` when the server is ready:

```bash
# From outside the container
curl http://localhost:3100/health

# Docker health check
docker inspect --format='{{.State.Health.Status}}' lynxdb
```

## Viewing Logs

```bash
docker logs lynxdb
docker logs -f lynxdb          # Follow
docker logs --since 1h lynxdb  # Last hour
```

## Upgrading

```bash
# Pull new image
docker pull ghcr.io/lynxbase/lynxdb:latest

# Recreate container (data is on the volume, so nothing is lost)
docker stop lynxdb
docker rm lynxdb
docker run -d --name lynxdb ... ghcr.io/lynxbase/lynxdb:latest

# Or with Docker Compose
docker compose pull
docker compose up -d
```

## Next Steps

- [Kubernetes Deployment](/docs/deployment/kubernetes) -- deploy on Kubernetes
- [S3 Storage Setup](/docs/deployment/s3-setup) -- MinIO and AWS S3 integration
- [Environment Variables](/docs/configuration/environment-variables) -- full variable reference
- [TLS and Authentication](/docs/deployment/tls-auth) -- secure the deployment
