---
title: Shipper Conformance Matrix
description: Nightly compatibility coverage and quarantine reasons for drop-in log shippers.
---

# Shipper Conformance Matrix

The nightly conformance workflow runs the `e2e_matrix` test driver against pinned shipper cells. The pull-request compatibility suite remains smaller; nightly runs cover the broader matrix and report each cell separately.

## Enforced cells

| Cell | Coverage |
|---|---|
| `filebeat/8.15/none` | Elasticsearch bulk ingest, Filebeat 8 handshake. |
| `fluent-bit/3.x/gzip` | Elasticsearch output with gzip compression. |
| `fluent-bit/3.x/logstash_format` | `Logstash_Format On`, date suffix stripped from `_source`. |
| `vector/0.40/zstd_bulk` | Vector bulk mode with zstd compression. |
| `vector/0.40/zstd_data_stream` | Vector data stream mode through `/_data_stream/{name}/_bulk`. |
| `otelcol/0.105/http_proto_gzip` | OTLP HTTP protobuf with gzip compression. |
| `otelcol/0.105/grpc_gzip` | OTLP gRPC with gzip compression. |
| `splunk-hec/curl/ack` | HEC event ingest plus `/services/collector/ack` polling. |

## Quarantine list

These cells are listed in the test driver with `t.Skip` and a reason. Promoting a quarantined cell means removing the skip and proving it passes in the nightly workflow.

| Cell | Reason |
|---|---|
| `filebeat/7.17/none` | Legacy version cell not yet promoted to nightly enforcement. |
| `filebeat/8.10/none` | Intermediate version cell not yet promoted to nightly enforcement. |
| `filebeat/9.0/none` | Image availability varies by registry state. |
| `fluent-bit/2.x/gzip` | Legacy version cell not yet promoted to nightly enforcement. |
| `vector/0.30/zstd_bulk` | Legacy sink config shape differs from the current fixture. |
| `vector/0.45/zstd_bulk` | Future-version cell pending registry pin. |
| `vector/latest/zstd_bulk` | `latest` is intentionally tracked outside PR gates. |
| `otelcol/0.95/http_proto_gzip` | Legacy collector config validation pending. |
| `otelcol/latest/http_proto_gzip` | `latest` is intentionally tracked outside PR gates. |
| `splunk-uf/9.x/hec` | Splunk Universal Forwarder image requires license/bootstrap secrets in CI. |

## Local run

```bash
make test-conformance
```
