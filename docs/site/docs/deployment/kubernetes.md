---
title: Kubernetes Deployment
description: Deploy LynxDB on Kubernetes -- single-node StatefulSet, persistent volumes, ConfigMap, and cluster examples.
---

# Kubernetes Deployment

LynxDB runs as a single static binary with no dependencies, making it straightforward to deploy on Kubernetes. This guide covers single-node and cluster deployments.

## Single-Node StatefulSet

A StatefulSet with a PersistentVolumeClaim provides stable storage for LynxDB data.

### Namespace

```yaml
# namespace.yaml
apiVersion: v1
kind: Namespace
metadata:
  name: lynxdb
```

### ConfigMap

```yaml
# configmap.yaml
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

    storage:
      compression: "lz4"
      flush_threshold: "512mb"
      cache_max_bytes: "2gb"

    query:
      max_concurrent: 20
      max_query_runtime: "10m"

    ingest:
      max_body_size: "50mb"
```

### StatefulSet

```yaml
# statefulset.yaml
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
              protocol: TCP
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/lynxdb
              readOnly: true
          resources:
            requests:
              memory: "1Gi"
              cpu: "500m"
            limits:
              memory: "4Gi"
              cpu: "4"
          livenessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 5
            periodSeconds: 30
          readinessProbe:
            httpGet:
              path: /health
              port: http
            initialDelaySeconds: 3
            periodSeconds: 10
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
```

### Service

```yaml
# service.yaml
apiVersion: v1
kind: Service
metadata:
  name: lynxdb
  namespace: lynxdb
spec:
  type: ClusterIP
  selector:
    app: lynxdb
  ports:
    - name: http
      port: 3100
      targetPort: http
      protocol: TCP
```

### Ingress (Optional)

```yaml
# ingress.yaml
apiVersion: networking.k8s.io/v1
kind: Ingress
metadata:
  name: lynxdb
  namespace: lynxdb
  annotations:
    nginx.ingress.kubernetes.io/proxy-body-size: "50m"
spec:
  rules:
    - host: lynxdb.company.com
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

### Apply All

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f statefulset.yaml
kubectl apply -f service.yaml
kubectl apply -f ingress.yaml

# Verify
kubectl -n lynxdb get pods
kubectl -n lynxdb logs lynxdb-0
```

## With S3 Tiering

Add S3 credentials via a Secret and update the ConfigMap:

```yaml
# s3-secret.yaml
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

Update the StatefulSet container to include S3 environment variables:

```yaml
env:
  - name: LYNXDB_STORAGE_S3_BUCKET
    value: "my-lynxdb-logs"
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

For EKS, use IAM Roles for Service Accounts (IRSA) instead of static credentials:

```yaml
# service-account.yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: lynxdb
  namespace: lynxdb
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::123456789012:role/lynxdb-s3-role
```

Add `serviceAccountName: lynxdb` to the StatefulSet pod spec.

## With Authentication

```yaml
# auth-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: lynxdb-auth
  namespace: lynxdb
type: Opaque
stringData:
  root-token: "lxk_your_root_token_here"
```

Update the StatefulSet args:

```yaml
args: ["server", "--config", "/etc/lynxdb/config.yaml", "--auth"]
```

## Cluster Deployment (3 Nodes)

For a small HA cluster where every node runs all roles:

```yaml
# cluster-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: lynxdb
  namespace: lynxdb
spec:
  serviceName: lynxdb-headless
  replicas: 3
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
          args:
            - server
            - --config
            - /etc/lynxdb/config.yaml
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: LYNXDB_STORAGE_S3_BUCKET
              value: "my-lynxdb-logs"
          ports:
            - name: http
              containerPort: 3100
            - name: cluster
              containerPort: 9400
          volumeMounts:
            - name: data
              mountPath: /data
            - name: config
              mountPath: /etc/lynxdb
              readOnly: true
          resources:
            requests:
              memory: "2Gi"
              cpu: "1"
            limits:
              memory: "8Gi"
              cpu: "4"
      volumes:
        - name: config
          configMap:
            name: lynxdb-cluster-config
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 200Gi
---
# Headless service for cluster discovery
apiVersion: v1
kind: Service
metadata:
  name: lynxdb-headless
  namespace: lynxdb
spec:
  type: ClusterIP
  clusterIP: None
  selector:
    app: lynxdb
  ports:
    - name: http
      port: 3100
    - name: cluster
      port: 9400
---
# Client-facing service (load balanced)
apiVersion: v1
kind: Service
metadata:
  name: lynxdb
  namespace: lynxdb
spec:
  type: ClusterIP
  selector:
    app: lynxdb
  ports:
    - name: http
      port: 3100
```

Cluster ConfigMap:

```yaml
# cluster-configmap.yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: lynxdb-cluster-config
  namespace: lynxdb
data:
  config.yaml: |
    listen: "0.0.0.0:3100"
    data_dir: "/data"
    retention: "30d"

    cluster:
      roles: [meta, ingest, query]
      seeds:
        - "lynxdb-0.lynxdb-headless.lynxdb.svc.cluster.local:9400"
        - "lynxdb-1.lynxdb-headless.lynxdb.svc.cluster.local:9400"
        - "lynxdb-2.lynxdb-headless.lynxdb.svc.cluster.local:9400"

    storage:
      s3_bucket: "my-lynxdb-logs"
      s3_region: "us-east-1"
```

## Monitoring

Integrate with Prometheus using a ServiceMonitor:

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
      path: /api/v1/stats
      interval: 30s
```

## Next Steps

- [Small Cluster](/docs/deployment/small-cluster) -- 3-10 node cluster architecture
- [Large Cluster](/docs/deployment/large-cluster) -- role splitting for 10-1000+ nodes
- [S3 Storage Setup](/docs/deployment/s3-setup) -- S3 configuration for Kubernetes
- [Monitoring](/docs/operations/monitoring) -- observability setup
