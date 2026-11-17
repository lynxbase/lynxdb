---
title: Analyze Kubernetes Audit Logs
description: Query Kubernetes API audit JSON Lines with LynxDB to find anonymous requests, source IPs, and targeted resources.
---

# Analyze Kubernetes Audit Logs

The Kubernetes API server's log audit backend writes one JSON event per line.
When the backend writes to its common `/var/log/kubernetes/audit/audit.log`
path, LynxDB can analyze the file in pipe mode, without starting a server or
ingesting the data first.

See the Kubernetes documentation for [configuring the audit log
backend](https://kubernetes.io/docs/tasks/debug/debug-cluster/audit/#log-backend)
and the [audit Event field
reference](https://kubernetes.io/docs/reference/config-api/apiserver-audit.v1/#audit-k8s-io-v1-Event).
Kubernetes records an [anonymous
request](https://kubernetes.io/docs/reference/access-authn-authz/authentication/#anonymous-requests)
with the username `system:anonymous` and membership in the
`system:unauthenticated` group.

Top-level keys such as `auditID`, `verb`, `userAgent` and `requestURI`
become fields when the file is loaded. The nested `user`, `objectRef` and
`responseStatus` objects and the `sourceIPs` array stay in `_raw`; the
queries below unpack them with `spath` and `flatten`.

## Find anonymous API requests

Count anonymous requests per user agent and source IP:

```bash
sudo cat /var/log/kubernetes/audit/audit.log | lynxdb query '
  source=main
  | spath output=doc
  | flatten doc
  | where user.username = "system:anonymous"
  | expand sourceIPs
  | stats dc(auditID) as requests by userAgent, sourceIPs
  | sort - requests'
```

```text
requests,sourceIPs,userAgent
2,203.0.113.7,curl/8.4.0
1,10.0.0.4,curl/8.4.0
1,198.51.100.23,python-requests/2.31
```

The pipeline does the following:

1. `spath output=doc` parses each event into an object, and `flatten doc`
   turns its members into columns such as `user.username`,
   `objectRef.resource`, `responseStatus.code` and `sourceIPs`.
2. `where` keeps the anonymous requests.
3. `expand sourceIPs` emits one row per address in the `sourceIPs` array.
4. `dc(auditID)` counts distinct API requests for each user agent and
   address. A request that lists several addresses appears under each of
   them.

To see which resource types anonymous clients asked for:

```bash
sudo cat /var/log/kubernetes/audit/audit.log | lynxdb query '
  source=main
  | spath output=doc
  | flatten doc
  | where user.username = "system:anonymous"
  | stats count() as requests by userAgent, objectRef.resource'
```

Requests to non-resource URLs such as `/version` have no `objectRef`, so
their `objectRef.resource` is empty.

:::caution
Kubernetes documents `auditID` as a unique ID for each request. It also warns
that proxy entries in `sourceIPs` can be supplied by the client and that
`userAgent` is untrusted client input. Treat both fields as investigation
clues, not identity proof.
:::

## Summarize failed requests

To see which API operations and resources produce error responses, group by
the response code:

```bash
sudo cat /var/log/kubernetes/audit/audit.log | lynxdb query '
  source=main
  | spath output=doc
  | flatten doc
  | where responseStatus.code >= 400
  | stats count() as requests by responseStatus.code, verb, objectRef.resource
  | sort - requests'
```

```text
objectRef.resource,requests,responseStatus.code,verb
deployments,1,409,patch
pods,1,403,list
secrets,1,403,list
```

When you need only a few nested values, `spath path=` extracts them without
flattening the whole event:

```bash
sudo cat /var/log/kubernetes/audit/audit.log | lynxdb query '
  source=main
  | spath path=responseStatus.code output=code
  | where cast(code as int) >= 400
  | spath path=objectRef.resource output=resource
  | stats count() as requests by code, verb, resource
  | sort - requests'
```

See [Working with JSON Logs](./json-processing.md) for more on `spath`,
`flatten` and `expand`.
