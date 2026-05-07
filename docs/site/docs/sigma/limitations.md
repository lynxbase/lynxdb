# Sigma limitations

[Back to Sigma docs](index.md)

This page lists known limits of using rsigma-generated LynxDB SPL2. LynxDB's
contract starts after rsigma emits a query; rules that rsigma rejects are not
LynxDB query compatibility failures.

## Current limits

| Area | Limit | Where to track |
|---|---|---|
| Unsupported rsigma rules | Some Sigma constructs may not convert for the LynxDB backend. | [rsigma issues](https://github.com/timescale/rsigma/issues) |
| Rare correlation forms | Correlation rules only work when rsigma lowers them to LynxDB SPL2. | [rsigma issues](https://github.com/timescale/rsigma/issues) |
| IPv6 CIDR edge cases | IPv4 CIDR is covered by `cidrmatch`; IPv6 edge cases need rule-specific validation before being called supported. | LynxDB issue tracker and rsigma issue tracker |
| Field naming | Sigma packs assume a schema such as ECS, OCSF, or Windows event fields. LynxDB does not rename fields unless the query tells it to. | [Pipelines](pipelines.md) |
| Helper commands | `lynxdb query --queries-file` and `lynxdb saved import` consume SPL2 files only; they do not convert Sigma YAML. | Use rsigma before calling LynxDB helpers. |

## What LynxDB does not provide

LynxDB does not provide a Sigma rule editor, rule scheduler, or alerting
system. Use cron, GitHub Actions, Airflow, or another runner to execute the
SPL2 queries. See [tutorial 06](tutorials/06-scheduled-detection.md).

LynxDB does not vendor or run rsigma. Install rsigma separately and pass the
generated SPL2 to LynxDB.
