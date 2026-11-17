---
title: Elasticsearch Compatibility Stubs
description: Compatibility endpoints LynxDB exposes for log shippers that expect Elasticsearch management APIs.
---

# Elasticsearch Compatibility Stubs

LynxDB implements enough Elasticsearch-compatible surface area for log shippers to start, health check, and send bulk data. These endpoints are not a general Elasticsearch API.

## Ingest routes

The ingest routes take documents while `ingest.es_compat.enabled` is on (the default); otherwise they answer `503` with `"type": "unavailable"`.

| Route | Behavior |
|---|---|
| `POST /_bulk` | Bulk ingest into the index each action names |
| `POST /{index}/_bulk` | Same, with `{index}` for actions that name no `_index` |
| `POST /_data_stream/{name}/_bulk` | Same, with the data stream `{name}` for actions that name no `_index` |
| `POST /api/v1/es/_bulk` | Prefixed bulk alias |
| `POST /api/v1/es/_data_stream/{name}/_bulk` | Prefixed data stream bulk alias |
| `POST /api/v1/es/{index}/_doc` | Single-document ingest into `{index}` |

A bulk body is NDJSON with `application/x-ndjson` or `application/json` as its content type, optionally gzip-compressed. Each document line is appended, as it is, to the index named by the action's `_index`, else by the `{index}` or data stream of the URL, else to `main`:

```bash
printf '{"index":{"_index":"web"}}\n{"@timestamp":"2026-02-10T08:00:00Z","message":"hello","host":{"name":"h1"}}\n' |
  curl -s -X POST localhost:3100/_bulk -H 'Content-Type: application/x-ndjson' --data-binary @-
curl -s localhost:3100/api/v1/query -d '{"q":"source=web"}'
```

```json
{"data":{"type":"events","events":[{"@timestamp":"2026-02-10T08:00:00Z","_time":"2026-02-10T08:00:00Z","host.name":"h1","message":"hello"}],"total":1,"has_more":false},"meta":{"took_ms":0.096,"query_id":"qry_a0579f645c0c50f0","rows":1}}
```

The document keeps its fields; nested objects are read as paths such as `host.name`. `_time` comes from `@timestamp` or another field of `storage.time_fields`, else from the time of arrival.

- `index` and `create` actions are accepted. `update` and `delete` actions are refused per item with `400 action_request_validation_exception`.
- An index name holds lowercase letters, digits, `.`, `_` and `-`, does not start with `-`, `_`, `+` or `.`, and is at most 255 bytes long; any other name is refused per item with `400 invalid_index_name_exception`.
- A data line that is not a JSON object is refused per item with `400 mapper_parsing_exception`.
- The server makes the accepted documents durable before it answers, because shippers count a `200` as stored.
- `_id` values are echoed in the response, or generated when the action has none. LynxDB is append-only: they are not used for deduplication or updates.

## Probe and setup stubs

| Route | Response shape |
|---|---|
| `GET /` | Elasticsearch-like cluster handshake |
| `HEAD /` | Empty `200 OK` handshake |
| `GET /_xpack` | Basic license feature summary |
| `GET /_xpack/license` | Active basic license |
| `GET /_license` | Active basic license |
| `GET /_cat/templates` | Empty JSON array |
| `GET /_cat/indices` | Empty JSON array |
| `GET /_cluster/health` | Green single-node health response |
| `GET /_index_template/{name}` | Empty index template list |
| `PUT /_index_template/{name}` | `{"acknowledged": true}` |
| `GET /_ilm/policy/{name}` | Empty `404` policy response |
| `GET /_ingest/pipeline/{name}` | Empty `404` pipeline response |
| `GET /_nodes/{path}` | Minimal node HTTP info |
| `GET /_alias` | Empty aliases object |
| `GET /_data_stream/{name}` | Empty data stream list |
| `PUT /_data_stream/{name}` | `{"acknowledged": true}` |
| `GET /_search` | Empty hits response |
| `POST /_security/user/_authenticate` | Authenticated user stub |
| `HEAD /{index}` | Empty `200 OK` index probe for shipper health checks |

Every unprefixed probe above has a matching `/api/v1/es/...` alias. Drop-in shipper configs should use the unprefixed routes.

Under the `/api/v1/es/` prefix, `PUT /api/v1/es/_ilm/policy/{name}`, `PUT /api/v1/es/_ingest/pipeline/{name}` and `PUT /api/v1/es/{index}` answer `501` with `{"error":"unsupported Elasticsearch management endpoint","status":501}` and log a warning with the method and path.

## Explicit non-goals

These stubs do not store Elasticsearch templates, ILM policies, ingest pipelines, aliases, users, roles, or data streams. They only keep setup probes from blocking log ingest.
