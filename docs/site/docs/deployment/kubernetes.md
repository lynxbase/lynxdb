---
title: Kubernetes Deployment
description: Run LynxDB on Kubernetes as a StatefulSet with a persistent volume, a ConfigMap, probes, S3 credentials and Prometheus scraping.
---

# Kubernetes Deployment

LynxDB runs as one container from `ghcr.io/lynxbase/lynxdb`. A StatefulSet with a PersistentVolumeClaim gives it stable storage for parts, API keys and the PID file.

## Single-Node StatefulSet

### ConfigMap

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: lynxdb-config
  namespace: lynxdb
data:
  config.yaml: |
    listen: "0.0.0.0:3100"
    data_dir: "/data"
    retention: "30d"
    log_level: "info"

    query:
      max_concurrent: 32
      max_query_runtime: "10m"
      spill_dir: "/data/spill"

    ingest:
      otlp:
        http_listen: "0.0.0.0:4318"

    auth:
      enabled: true
```

### StatefulSet and Services

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: lynxdb
  namespace: lynxdb
spec:
  serviceName: lynxdb
  replicas: 1
  selector:
    matchLabels:
      app: lynxdb
  template:
    metadata:
      labels:
        app: lynxdb
    spec:
      containers:
        - name: lynxdb
          image: ghcr.io/lynxbase/lynxdb:latest
          args: ["server", "--config", "/etc/lynxdb/config.yaml"]
          ports:
            - name: http
              containerPort: 3100
            - name: otlp-http
              containerPort: 4318
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/lynxdb
              readOnly: true
          resources:
            requests:
              memory: "2Gi"
              cpu: "500m"
            limits:
              memory: "8Gi"
          readinessProbe:
            httpGet:
              path: /health
              port: http
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 10
            periodSeconds: 30
      volumes:
        - name: config
          configMap:
            name: lynxdb-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 100Gi
---
apiVersion: v1
kind: Service
metadata:
  name: lynxdb
  namespace: lynxdb
  labels:
    app: lynxdb
spec:
  selector:
    app: lynxdb
  ports:
    - name: http
      port: 3100
      targetPort: http
    - name: otlp-http
      port: 4318
      targetPort: otlp-http
```

```bash
kubectl create namespace lynxdb
kubectl apply -f configmap.yaml -f statefulset.yaml
kubectl -n lynxdb rollout status statefulset/lynxdb
kubectl -n lynxdb logs lynxdb-0 | grep -A2 'Generated root key'
```

Notes:

- `/health` needs no API key, so the probes work with auth on. With TLS on, set `scheme: HTTPS` on both probes.
- The automatic memory pool is 40% of the container's memory limit. Set `server.total_memory_pool_bytes` to change it.
- On the first start with auth on, the root key is printed once to the pod log. Keys live in the volume under `/data/auth`.
- The image has no shell. Use `kubectl -n lynxdb exec lynxdb-0 -- lynxdb health` or `kubectl -n lynxdb port-forward svc/lynxdb 3100` to reach the server.
- The OTLP listener has no auth; drop the `otlp-http` port, or keep it on an internal Service only, if the cluster network is shared.

### Ingress

```yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: lynxdb
  namespace: lynxdb
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "32m"
    nginx.ingress.kubernetes.io/proxy-buffering: "off"
    nginx.ingress.kubernetes.io/proxy-read-timeout: "3600"
spec:
  rules:
    - host: lynxdb.example.com
      http:
        paths:
          - path: /
            pathType: Prefix
            backend:
              service:
                name: lynxdb
                port:
                  number: 3100
```

Buffering off and a long read timeout keep live tail (SSE) and NDJSON streaming responses flowing. The body size matches `ingest.limits.max_compressed_body_bytes`.

## S3 Tiering

Static credentials from a Secret:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: lynxdb-s3-credentials
  namespace: lynxdb
type: Opaque
stringData:
  access-key-id: "AKIAIOSFODNN7EXAMPLE"
  secret-access-key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
```

```yaml
# container spec
env:
  - name: LYNXDB_STORAGE_S3_BUCKET
    value: "example-lynxdb-logs"
  - name: LYNXDB_STORAGE_S3_REGION
    value: "us-east-1"
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: lynxdb-s3-credentials
        key: access-key-id
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: lynxdb-s3-credentials
        key: secret-access-key
```

On EKS, use IAM Roles for Service Accounts instead; the AWS SDK picks up the web identity token:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: lynxdb
  namespace: lynxdb
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/lynxdb-s3
```

Add `serviceAccountName: lynxdb` to the pod spec. See [S3/MinIO Storage Backend](./s3-setup.md) for the IAM policy.

## Prometheus

`/metrics` serves Prometheus metrics without authentication:

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: lynxdb
  namespace: lynxdb
spec:
  selector:
    matchLabels:
      app: lynxdb
  endpoints:
    - port: http
      path: /metrics
      interval: 30s
```

## Next Steps

- [Docker Deployment](./docker.md)
- [S3/MinIO Storage Backend](./s3-setup.md)
- [TLS and Authentication](./tls-auth.md)
- [Monitoring](../operations/monitoring.md)
