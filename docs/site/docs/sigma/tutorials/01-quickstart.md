# Detect Whoami in 60 seconds

[Back to Sigma docs](../index.md)

This tutorial uses rsigma as an external converter and LynxDB as the SPL2
execution target.

Install rsigma:

```bash
cargo install rsigma
```

Create a small Sigma rule:

```bash
cat > whoami.yml <<'YAML'
title: Whoami Process
logsource:
  product: windows
detection:
  selection:
    CommandLine|contains: whoami
  condition: selection
YAML
```

Create one matching event:

```bash
printf '%s\n' '{"CommandLine":"cmd.exe /c whoami","Image":"C:\\Windows\\System32\\cmd.exe"}' > events.ndjson
```

Convert the rule:

```bash
rsigma convert -t lynxdb whoami.yml > whoami.spl2
cat whoami.spl2
```

Expected query shape:

```spl
FROM main | search CommandLine=*"whoami"*
```

Run the query against the event file:

```bash
lynxdb query --file events.ndjson "$(cat whoami.spl2)" --format ndjson
```

The output should contain the event with `cmd.exe /c whoami`.

The same query can be sent to a running server without helper commands:

```bash
lynxdb server
```

In another terminal:

```bash
lynxdb ingest events.ndjson --source windows --sourcetype json
QUERY="$(cat whoami.spl2)"
curl -sS http://localhost:3100/api/v1/query \
  -H 'content-type: application/json' \
  -d "{\"query\":$(printf '%s' "$QUERY" | jq -Rs .)}"
```
