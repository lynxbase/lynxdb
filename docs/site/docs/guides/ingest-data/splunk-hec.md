---
title: Splunk HEC to LynxDB
description: Send Splunk HTTP Event Collector events to LynxDB.
---

# Splunk HEC to LynxDB

## If you already post to Splunk HEC

Change the HEC URL to LynxDB and keep the `Authorization: Splunk <token>` header.

```diff
-https://splunk.example.com:8088/services/collector/event
+http://lynxdb:3100/services/collector/event
```

By default, LynxDB accepts any non-empty Splunk token. Tight token validation can be added later without changing the HEC path.

## Full annotated request

```bash
# Splunk HEC payload example for LynxDB.
# LynxDB P1 accepts any non-empty Splunk token by default.
curl -sS "http://lynxdb:3100/services/collector/event" \
  -H "Authorization: Splunk changeme" \
  -H "Content-Type: application/json" \
  --data-binary '{"event":"hello from splunk hec","source":"manual","sourcetype":"text"}'
```

Generate the same shape locally with:

```bash
lynxdb shippers config splunk-hec --remote http://lynxdb:3100
```

## Migrating from Splunk

HEC envelope fields map directly:

| HEC field | LynxDB field |
|---|---|
| `event` | `_raw` |
| `time` | event timestamp |
| `host` | `host` |
| `source` | `_source` |
| `sourcetype` | `sourcetype` |
| `fields` | structured fields |

If `source` is empty and `index` is present, LynxDB uses `index` as `_source`. Events are still written to the LynxDB `main` index.

## Troubleshooting

Check `/services/collector/health` for the HEC health response:

```bash
curl -sS http://lynxdb:3100/services/collector/health
```

Run `lynxdb doctor shippers` to confirm HEC traffic has been observed.

## Supported behavior

Supported: `/services/collector/event`, `/services/collector`, `/services/collector/raw`, `/services/collector/health`, gzip/zstd request decoding, and Splunk token header parsing.

Not supported yet: Splunk HEC indexer ack mode and Splunk SPL1 query compatibility.
