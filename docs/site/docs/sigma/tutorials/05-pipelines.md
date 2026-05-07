# Pipelines

[Back to Sigma docs](../index.md)

Use rsigma pipelines to select a LynxDB index and map rule fields to ingested
event fields.

Create a concrete event:

```bash
printf '%s\n' '{"process.command_line":"cmd.exe /c whoami","user.name":"alice"}' > ecs.ndjson
lynxdb ingest ecs.ndjson --source ecs --sourcetype json --index security
```

Create a rule that uses ECS field names:

```bash
cat > ecs-whoami.yml <<'YAML'
title: ECS Whoami
logsource:
  product: windows
detection:
  selection:
    process.command_line|contains: whoami
  condition: selection
YAML
```

Create a pipeline that targets the `security` index:

```bash
cat > ecs-lynxdb.yml <<'YAML'
transformations:
  - type: set_state
    key: index
    value: security
YAML
```

Convert and run:

```bash
rsigma convert -t lynxdb -p ecs-lynxdb.yml ecs-whoami.yml > ecs-whoami.spl2
lynxdb query "$(cat ecs-whoami.spl2)" --since 24h
```

If your ingested fields differ from the rule fields, add rsigma field-mapping
transformations to the same pipeline. Keep those mappings near the rule pack so
the generated SPL2 is reproducible.
