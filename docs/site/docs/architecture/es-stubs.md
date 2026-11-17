---
title: Elasticsearch Compatibility Stubs
description: Compatibility endpoints LynxDB exposes for log shippers that expect Elasticsearch management APIs.
---

# Elasticsearch Compatibility Stubs

LynxDB implements enough Elasticsearch-compatible surface area for log shippers to start, health check, and send bulk data. These endpoints are not a general Elasticsearch API.

## Ingest routes

| Route | Behavior |
|---|---|
| `POST /_bulk` | Ingest NDJSON bulk events into the LynxDB index named by each action's `_index`. |
| `POST /{index}/_bulk` | Same, with `{index}` as the index when the action metadata omits `_index`. |
| `POST /_data_stream/{name}/_bulk` | Ingest Vector data stream bulk events into the index `{name}`. |
| `POST /api/v1/es/_bulk` | Prefixed bulk alias. |
| `POST /api/v1/es/_data_stream/{name}/_bulk` | Prefixed data stream bulk alias. |
| `POST /api/v1/es/{index}/_doc` | Single-document ingest into `{index}`. |

The target index of a bulk document is, in order: the data stream name; the action's `_index`, with `${field}` placeholders filled from the document; the `{index}` path segment; the document's `target_index` field (or `attributes.target_index`, `resource.attributes.target_index`, `fields.target_index`); otherwise `default`. Query the events with `source=<index>`.

Each document becomes one event:

- `_time` from `@timestamp` (or `timestamp`), or the field named by the `_time_field` query parameter
- `_source` from `log.file.path` (also under `attributes.`, `resource.attributes.`, `fields.`, or a nested `log.file.path` object), else the document's `source` field
- `host` from `host`, `host.name` or `agent.hostname`
- `_raw` from the field named by the `_msg_field` query parameter, else the remaining document serialized to JSON
- every remaining top-level key as a field

`update` and `delete` actions are rejected per item; `index` and `create` are accepted.

## Probe and setup stubs

| Route | Response shape |
|---|---|
| `GET /` | Elasticsearch-like cluster handshake. |
| `HEAD /` | Empty `200 OK` handshake. |
| `GET /_xpack` | Basic license feature summary. |
| `GET /_xpack/license` | Active basic license. |
| `GET /_license` | Active basic license. |
| `GET /_cat/templates` | Empty JSON array. |
| `GET /_cat/indices` | Empty JSON array. |
| `GET /_cluster/health` | Green single-node health response. |
| `GET /_index_template/{name}` | Empty index template list. |
| `PUT /_index_template/{name}` | `{"acknowledged": true}`. |
| `GET /_ilm/policy/{name}` | Empty `404` policy response. |
| `GET /_ingest/pipeline/{name}` | Empty `404` pipeline response. |
| `GET /_nodes/{path}` | Minimal node HTTP info. |
| `GET /_alias` | Empty aliases object. |
| `GET /_data_stream/{name}` | Empty data stream list. |
| `PUT /_data_stream/{name}` | `{"acknowledged": true}`. |
| `GET /_search` | Empty hits response. |
| `POST /_security/user/_authenticate` | Authenticated user stub. |
| `HEAD /{index}` | Empty `200 OK` index probe for shipper health checks. |

Every unprefixed probe above has a matching `/api/v1/es/...` alias. Drop-in shipper configs should use the unprefixed routes.

Under the `/api/v1/es/` prefix, `PUT /api/v1/es/_ilm/policy/{name}`, `PUT /api/v1/es/_ingest/pipeline/{name}` and `PUT /api/v1/es/{index}` answer `501` with `{"error":"unsupported Elasticsearch management endpoint","status":501}` and log a warning with the method and path.

## Explicit non-goals

These stubs do not store Elasticsearch templates, ILM policies, ingest pipelines, aliases, users, roles, or data streams. They only keep setup probes from blocking log ingest. LynxDB is append-only: `_id` values are echoed in responses but are not used for deduplication or updates.
