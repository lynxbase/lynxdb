---
title: Docker Deployment
description: Run LynxDB with Docker and Docker Compose - image tags, volumes, environment variables and health checks.
---

# Docker Deployment

## Quick Start

```bash
docker run -d --name lynxdb \
  -p 3100:3100 \
  -v lynxdb-data:/data \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  ghcr.io/lynxbase/lynxdb:latest

curl http://localhost:3100/health
```

Set `LYNXDB_LISTEN=0.0.0.0:3100`: the default `localhost:3100` is not reachable through a published port. Set `LYNXDB_DATA_DIR` to a mounted volume, or data is written inside the container's filesystem.

## Image

The image is built `FROM scratch`: it holds the static `lynxdb` binary at `/usr/local/bin/lynxdb` and the CA certificate bundle, nothing else. There is no shell, `curl` or `wget` in the image. The entrypoint is `lynxdb` and the default command is `server`, so arguments after the image name replace `server`:

```bash
docker run --rm ghcr.io/lynxbase/lynxdb:latest version
docker run -d ... ghcr.io/lynxbase/lynxdb:latest server --addr 0.0.0.0:3100 --data-dir /data
```

| Tag | Contents |
|-----|----------|
| `latest` | Latest stable release |
| `<version>`, for example `0.5.0` | A specific release |
| `nightly` | Latest nightly prerelease |
| `<version>-nightly.<date>.g<sha>`, `sha-<commit>` | A specific nightly build |

Images are published for `linux/amd64` and `linux/arm64`. Nightly builds come from `main` and are not meant for production.

## docker run Examples

### In-memory

```bash
docker run -d --name lynxdb -p 3100:3100 \
  ghcr.io/lynxbase/lynxdb:latest server --addr 0.0.0.0:3100 --data-dir ""
```

Nothing is persisted, and authentication is unavailable in this mode.

### Persistent, with TLS and auth

```bash
docker run -d --name lynxdb -p 3100:3100 \
  -v lynxdb-data:/data \
  -v /srv/lynxdb/tls:/tls:ro \
  -e LYNXDB_LISTEN=0.0.0.0:3100 \
  -e LYNXDB_DATA_DIR=/data \
  -e LYNXDB_RETENTION=30d \
  ghcr.io/lynxbase/lynxdb:latest server \
    --tls-cert /tls/server.crt --tls-key /tls/server.key --auth

docker logs lynxdb 2>&1 | grep -A2 'Generated root key'
```

The root key is printed once, on the first start with an empty key store; the keys are stored in the volume under `/data/auth`.

### With a config file

`/etc/lynxdb/config.yaml` is in the config search path, so mounting a file there is enough:

```bash
docker run -d --name lynxdb -p 3100:3100 \
  -v lynxdb-data:/data \
  -v /srv/lynxdb/config.yaml:/etc/lynxdb/config.yaml:ro \
  ghcr.io/lynxbase/lynxdb:latest
```

The file must set `listen: "0.0.0.0:3100"` and `data_dir: "/data"`.

### Other ports

Publish only the listeners you use:

| Port | Listener | Setting |
|------|----------|---------|
| 3100 | HTTP API | `listen` |
| 4318 | OTLP/HTTP, on by default inside the container | `ingest.otlp.http_listen` |
| 5514, 6514 | Syslog | `syslog.udp`, `syslog.tcp` |

## Docker Compose

### Single node

```yaml
# compose.yaml
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
      LYNXDB_QUERY_MAX_CONCURRENT: "32"
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "/usr/local/bin/lynxdb", "health", "--server", "http://localhost:3100"]
      interval: 30s
      timeout: 5s
      retries: 3
    deploy:
      resources:
        limits:
          memory: 4g

volumes:
  lynxdb-data:
```

The health check runs the `lynxdb` binary itself, since the image has no shell or `curl`. With TLS on, use `--server https://localhost:3100 --tls-skip-verify`. On Linux the automatic memory pool is 40% of the cgroup memory limit when one is set, so a container limit is enough to size it; set `server.total_memory_pool_bytes` to choose a different share.

### Log shippers

Shippers such as Filebeat, Fluent Bit and Vector send to the Elasticsearch bulk API (`http://lynxdb:3100/api/v1/es/_bulk`), Splunk HEC or OTLP. See the [Ingest Data guides](../guides/ingest-data.md) for shipper configs.

## Custom Image

```dockerfile
FROM ghcr.io/lynxbase/lynxdb:latest
COPY config.yaml /etc/lynxdb/config.yaml
```

The base image's entrypoint and command still apply.

## Querying from the Host

```bash
lynxdb query --server http://localhost:3100 'source=main level=ERROR | stats count() by host'
docker exec lynxdb lynxdb query 'source=main | stats count() by host'
```

## Updating the Image

Data lives in the volume, so replacing the container keeps it:

```bash
docker pull ghcr.io/lynxbase/lynxdb:latest
docker stop lynxdb && docker rm lynxdb
docker run -d --name lynxdb ... ghcr.io/lynxbase/lynxdb:latest

# Compose
docker compose pull && docker compose up -d
```

## Next Steps

- [Kubernetes Deployment](./kubernetes.md)
- [Environment Variables](../configuration/environment-variables.md)
- [TLS and Authentication](./tls-auth.md)
