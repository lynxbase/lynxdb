# Windows EVTX

[Back to Sigma docs](../index.md)

This flow uses Windows Security events exported as NDJSON. In a real deployment
the same event shape can arrive through the existing OpenTelemetry pipeline and
LynxDB OTLP receiver.

Start LynxDB with the OTLP receiver configured for your environment:

```bash
lynxdb server
```

Create one Windows Security event and ingest it into a concrete index:

```bash
printf '%s\n' '{"EventID":4688,"CommandLine":"C:\\Windows\\System32\\whoami.exe","Image":"C:\\Windows\\System32\\whoami.exe","User":"alice"}' > windows-security.ndjson
lynxdb ingest windows-security.ndjson --source windows --sourcetype json --index security
```

Create a small Windows process rule:

```bash
cat > windows-whoami.yml <<'YAML'
title: Windows Whoami Process
logsource:
  product: windows
detection:
  selection:
    EventID: 4688
    CommandLine|contains: whoami
  condition: selection
YAML
```

Convert the rule with an rsigma pipeline that targets the same index:

```bash
cat > windows-lynxdb.yml <<'YAML'
transformations:
  - type: set_state
    key: index
    value: security
YAML

rsigma convert -t lynxdb -p windows-lynxdb.yml windows-whoami.yml > windows.spl2
```

Run the generated query:

```bash
lynxdb query "$(cat windows.spl2)" --since 24h --format ndjson
```

If your event fields use ECS or OCSF names instead of Sigma's Windows field
names, add field mappings to the rsigma pipeline. See
[tutorial 05](05-pipelines.md).

To convert a checked-out SigmaHQ Windows pack after validating the single-rule
flow:

```bash
git clone https://github.com/SigmaHQ/sigma.git sigma
rsigma convert -t lynxdb -p windows-lynxdb.yml sigma/rules/windows > windows-pack.spl2
```
