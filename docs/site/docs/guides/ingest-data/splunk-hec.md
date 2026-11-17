---
title: Splunk HEC to LynxDB
description: Send Splunk HTTP Event Collector events to LynxDB.
---

# Splunk HEC to LynxDB

## If you already post to Splunk HEC

Change the HEC URL to LynxDB and keep the `Authorization: Splunk <token>`
header.

```diff
-https://splunk.example.com:8088/services/collector/event
+http://lynxdb:3100/services/collector/event
```

LynxDB accepts any non-empty Splunk token. A request without the header is
rejected.

## Full annotated request

```bash
# Splunk HEC event for LynxDB.
curl -sS "http://lynxdb:3100/services/collector/event" \
  -H "Authorization: Splunk changeme" \
  -H "Content-Type: application/json" \
  --data-binary '{"event":"hello from splunk hec","source":"manual","sourcetype":"text"}'
```

```json
{"code":0,"text":"Success"}
```

Query it:

```bash
lynxdb query 'source=main | where source = "manual" | fields _time, sourcetype, _raw'
```

```json
{"_raw":"hello from splunk hec","_time":"2026-11-17T15:46:47.15592Z","sourcetype":"text"}
```

## Field mapping

| HEC field | LynxDB field |
|---|---|
| `event` | `_raw` |
| `time` | `_time` |
| `host` | `host` |
| `source` | `source` |
| `sourcetype` | `sourcetype` |
| `fields` | fields of the same names |
| `index` | `source` when `source` is empty, otherwise the `source_tag` field |

Events are written to the LynxDB `main` index whatever their HEC `index`.
The ingest pipeline then extracts `key=value` pairs and JSON keys from the
event text as for any other event; see
[Field Extraction](../field-extraction.md).

## Acknowledgements

Senders that use indexer acknowledgement pass a channel in the
`X-Splunk-Request-Channel` header. The event response then carries an
`ackId`, and `POST /services/collector/ack` with `{"acks":[...]}` reports
which IDs are committed.

## Troubleshooting

`/services/collector/health` answers the HEC health check:

```bash
curl -sS http://lynxdb:3100/services/collector/health
```

A query syncs the database before it runs, so an event is searchable once
its request was answered. An event whose ID `/services/collector/ack`
reports as committed is written to disk. HEC routes can be turned off with
`ingest.splunk_hec.enabled: false`.

## Supported behavior

Supported: `/services/collector/event`, `/services/collector`,
`/services/collector/raw`, `/services/collector/ack`,
`/services/collector/health`, gzip and zstd request bodies, and the
`Authorization: Splunk <token>` header. `/api/v1/ingest/hec` accepts the same
events under LynxDB's own authentication.
