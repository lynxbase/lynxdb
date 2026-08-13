---
title: Analyze Kubernetes Audit Logs
description: Query Kubernetes API audit JSON Lines with LynxDB to find anonymous requests, source IPs, and targeted resources.
---

# Analyze Kubernetes Audit Logs

The Kubernetes API server's log audit backend writes one JSON event per line.
When the backend uses its common `/var/log/kubernetes/audit/audit.log` path,
LynxDB can analyze the file directly in pipe mode without starting a server or
ingesting the data first.

See the Kubernetes documentation for [configuring the audit log
backend](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/#log-backend)
and the [audit Event field
reference](https://kubernetes.io/docs/reference/config-api/apiserver-audit.v1/#audit-k8s-io-v1-Event).
Kubernetes records an [anonymous
request](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#anonymous-requests)
with the username `system:anonymous` and membership in the
`system:unauthenticated` group.

## Find anonymous API requests

This query groups anonymous requests by user agent and summarizes their source
IPs and targeted resource types:

```bash
sudo cat /var/log/kubernetes/audit/audit.log | lynxdb query '
  | parse json
  | where user.username == "system:anonymous"
  | extend source_ips_array = from_json(sourceIPs)
  | explode source_ips_array as source_ip
  | stats dc(auditID) as requests,
          values(source_ip, 20) as source_ips,
          values(objectRef.resource, 20) as resources
    by userAgent
  | sort -requests'
```

The pipeline does the following:

1. `parse json` exposes nested fields such as `user.username` and
   `objectRef.resource`.
2. `from_json(sourceIPs)` restores the JSON-encoded IP list, and `explode`
   emits one row per address.
3. `dc(auditID)` counts unique API requests after that expansion, so requests
   with multiple proxy addresses are not counted more than once.
4. `values(..., 20)` keeps each summary bounded to 20 distinct IPs and resource
   types per user agent.

:::caution
Kubernetes documents `auditID` as a unique ID for each request. It also warns
that proxy entries in `sourceIPs` can be supplied by the client and that
`userAgent` is untrusted client input. Treat both fields as investigation clues,
not identity proof.
:::

## Summarize failed requests

To see which API operations and resources produce error responses, aggregate
the response code instead:

```bash
sudo cat /var/log/kubernetes/audit/audit.log | lynxdb query '
  | parse json
  | where responseStatus.code >= 400
  | stats count() as requests
    by responseStatus.code, verb, objectRef.resource
  | sort -requests'
```

`objectRef` is absent from some non-resource and list requests, so those groups
may have a null resource value.
