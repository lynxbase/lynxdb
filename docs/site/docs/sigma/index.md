# Sigma rules on LynxDB

Sigma is a rule format for security detections. rsigma is an external CLI that
can compile Sigma YAML into LynxDB SPL2. LynxDB does not ship rsigma or parse
Sigma YAML itself; it executes the SPL2 query that rsigma emits.

The basic flow is:

```bash
cargo install rsigma
rsigma convert -t lynxdb rule.yml > rule.spl2
lynxdb query "$(cat rule.spl2)"
```

Against a running server, the unassisted REST path is:

```bash
QUERY="$(rsigma convert -t lynxdb rule.yml)"
curl -sS http://localhost:3100/api/v1/query \
  -H 'content-type: application/json' \
  -d "{\"query\":$(printf '%s' "$QUERY" | jq -Rs .)}"
```

For larger rule sets, pass a query file with
`lynxdb query --queries-file rules.spl2` or import it as saved queries with
`lynxdb saved import rules.spl2`.

Reference:

- [Compatibility contract](compat.md)
- [Sigma to SPL2 mapping](spl2-mapping.md)
- [Pipelines](pipelines.md)
- [Cookbook](cookbook.md)
- [Troubleshooting](troubleshooting.md)
- [Limitations](limitations.md)
- [Drift runbook](drift-runbook.md)

Tutorials:

- [01: Detect Whoami in 60 seconds](tutorials/01-quickstart.md)
- [02: Bulk conversion](tutorials/02-bulk-conversion.md)
- [03: Windows EVTX](tutorials/03-windows-evtx.md)
- [04: CloudTrail](tutorials/04-cloudtrail.md)
- [05: Pipelines](tutorials/05-pipelines.md)
- [06: Scheduled detection](tutorials/06-scheduled-detection.md)

Next: [tutorial 01](tutorials/01-quickstart.md).
