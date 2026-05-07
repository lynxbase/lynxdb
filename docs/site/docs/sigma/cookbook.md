# Sigma cookbook

[Back to Sigma docs](index.md)

## Send matches to syslog

Convert a rule and run it on a schedule:

```bash
rsigma convert -t lynxdb whoami.yml > whoami.spl2
lynxdb query "$(cat whoami.spl2)" --since 5m --format ndjson \
  | xargs -I{} logger -t lynxdb-sigma '{}'
```

## Send matches to Slack

This example posts each match to a Slack incoming webhook endpoint:

```bash
lynxdb query "$(cat whoami.spl2)" --since 5m --format ndjson \
  | xargs -I{} curl -sS -X POST "$SLACK_WEBHOOK_URL" \
      -H 'content-type: application/json' \
      -d '{"text":"LynxDB Sigma match: {}"}'
```

## Send matches to PagerDuty

This example posts each match to the PagerDuty Events API:

```bash
lynxdb query "$(cat whoami.spl2)" --since 5m --format ndjson \
  | xargs -I{} curl -sS -X POST https://events.pagerduty.com/v2/enqueue \
      -H 'content-type: application/json' \
      -d '{"routing_key":"00000000000000000000000000000000","event_action":"trigger","payload":{"summary":"LynxDB Sigma match","source":"lynxdb","severity":"warning","custom_details":{}}}'
```

## Embed minimal output

rsigma's `format=minimal` output omits `FROM main | search`, which lets you
place the predicate inside a larger pipeline:

```bash
predicate="$(rsigma convert -t lynxdb -f minimal whoami.yml)"
lynxdb query "FROM security | search $predicate | stats count by user"
```

Measure this query against your own data before adding alerts or budgets.

## GitHub Action for rule conversion

This workflow converts rules in a repository and runs them against a LynxDB
server.

```yaml
name: sigma-to-lynxdb
on:
  push:
    paths:
      - "rules/**/*.yml"

jobs:
  convert:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: dtolnay/rust-toolchain@stable
      - run: cargo install rsigma
      - run: rsigma convert -t lynxdb -r rules > rules.spl2
      - run: lynxdb query --queries-file rules.spl2 --since 15m --format ndjson
        env:
          LYNXDB_SERVER: http://localhost:3100
```
