---
title: Elasticsearch Compatibility Stubs
description: Compatibility endpoints LynxDB exposes for log shippers that expect Elasticsearch management APIs.
---

# Elasticsearch Compatibility Stubs

LynxDB implements enough Elasticsearch-compatible surface area for log shippers to start, health check, and send bulk data. These endpoints are not a general Elasticsearch API.

## Ingest routes

| Route | Behavior |
|---|---|
| `POST /_bulk` | Ingest NDJSON bulk events. |
| `POST /{index}/_bulk` | Ingest NDJSON bulk events using `{index}` as `_source` when action metadata omits `_index`. |
| `POST /_data_stream/{name}/_bulk` | Ingest Vector data stream bulk events using `{name}` as `_source`. |
| `POST /api/v1/es/_bulk` | Legacy prefixed bulk alias. |
| `POST /api/v1/es/_data_stream/{name}/_bulk` | Legacy prefixed data stream bulk alias. |
| `POST /api/v1/es/{index}/_doc` | Single-document ingest alias. |

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
| `PUT /_ilm/policy/{name}` | Unsupported management endpoint response. |
| `GET /_ingest/pipeline/{name}` | Empty `404` pipeline response. |
| `PUT /_ingest/pipeline/{name}` | Unsupported management endpoint response. |
| `GET /_nodes/{path}` | Minimal node HTTP info. |
| `GET /_alias` | Empty aliases object. |
| `GET /_data_stream/{name}` | Empty data stream list. |
| `PUT /_data_stream/{name}` | `{"acknowledged": true}`. |
| `GET /_search` | Empty hits response. |
| `POST /_security/user/_authenticate` | Authenticated user stub. |
| `HEAD /{index}` | Empty `200 OK` index probe for shipper health checks. |

Every unprefixed probe above has a matching `/api/v1/es/...` alias where that legacy prefix is useful. New drop-in shipper configs should prefer unprefixed routes.

## Explicit non-goals

These stubs do not store Elasticsearch templates, ILM policies, ingest pipelines, aliases, users, roles, or data streams. They only prevent setup probes from blocking log ingest. LynxDB remains append-only: `_id` values are echoed in responses but are not used for deduplication or updates.

Unsupported management endpoint calls are logged by path so future high-frequency misses can be promoted to explicit stubs.
