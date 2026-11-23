---
title: Kubernetes Deployment
description: Run LynxDB on Kubernetes as a StatefulSet with a persistent volume, a ConfigMap, probes and Prometheus scraping.
---

# Kubernetes Deployment

LynxDB runs as one container from `ghcr.io/lynxbase/lynxdb`. A StatefulSet with a PersistentVolumeClaim gives it stable storage for its indexes, API keys and the PID file.

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
- Every index lives in its own directory on the volume; the server opens them all at startup.
- On the first start with auth on, the root key is printed once to the pod log. Keys live in the volume under `/data/auth`.
- The image has no shell. Use `kubectl -n lynxdb exec lynxdb-0 -- lynxdb status` or `kubectl -n lynxdb port-forward svc/lynxdb 3100` to reach the server.
- With auth on, the OTLP listener needs a key with the `ingest` scope, sent as `Authorization: Bearer <key>`. It has no TLS; keep the `otlp-http` port on an internal Service if the cluster network is shared, or drop it with `http_listen: ""`.

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

Buffering off and a long read timeout keep NDJSON exports from `/api/v1/query/stream` flowing. The body size matches `ingest.limits.max_compressed_body_bytes`.

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
- [TLS and Authentication](./tls-auth.md)
- [Monitoring](../operations/monitoring.md)
