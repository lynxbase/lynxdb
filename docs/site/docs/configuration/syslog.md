---
title: Syslog Receiver
description: Configure the native syslog receiver that ingests RFC 5424, RFC 3164 and raw messages over UDP, TCP and TLS.
---

# Syslog Receiver

The `syslog` section configures a native receiver for syslog over UDP (RFC 5426), TCP (RFC 6587) and TCP with TLS. Messages are parsed as RFC 5424, RFC 3164 or raw text and written to the index `syslog.index`, one flat JSON object per message.

The receiver is off until `syslog.udp` or `syslog.tcp` has an address.

## Quick Start

```bash
# UDP and TCP on port 5514
lynxdb server --syslog :5514

# UDP only
lynxdb server --syslog-udp :5514

# TCP with TLS on port 6514 (needs server TLS)
lynxdb server --tls --syslog-tcp :6514 --syslog-tls
```

```yaml
syslog:
  udp: ":5514"
  tcp: ":5514"
```

Point a sender at the receiver:

```text
# rsyslog: @@ forwards over TCP, a single @ over UDP
*.* @@lynxdb-host:5514

# syslog-ng
destination d_lynxdb { network("lynxdb-host" port(5514) transport("tcp")); };
```

Ports below 1024 (such as 514) need root or the `CAP_NET_BIND_SERVICE` capability.

Check what arrived:

```bash
lynxdb query 'source=main sourcetype="syslog:rfc5424" | stats count() by host, app_name'
lynxdb query 'source=main severity <= 3 | stats count() by host, severity_label'
```

## Listeners

| Key | Flag | Env var | Default | Description |
|-----|------|---------|---------|-------------|
| `syslog.udp` | `--syslog-udp`, `--syslog` | `LYNXDB_SYSLOG_UDP` | empty | UDP address (`host:port`). One message per datagram. |
| `syslog.tcp` | `--syslog-tcp`, `--syslog` | `LYNXDB_SYSLOG_TCP` | empty | TCP address (`host:port`). |
| `syslog.tls` | `--syslog-tls` | `LYNXDB_SYSLOG_TLS` | `false` | Wrap the TCP listener with the server's TLS configuration. |

The flags accept `:port`, `host:port`, a bare port or a bare host; a missing port becomes `5514`, or `6514` for TCP when `--syslog-tls` is set. `syslog.tls` needs server TLS (`--tls`, `tls.enabled`, or a certificate and key); without it the receiver fails to start.

## Parser

| Key | Flag | Env var | Default |
|-----|------|---------|---------|
| `syslog.parser` | `--syslog-parser` | `LYNXDB_SYSLOG_PARSER` | `auto` |

| Value | Behavior |
|-------|----------|
| `auto` | Per message: RFC 5424 when a version digit follows the `<PRI>` header, RFC 3164 for other `<PRI>` messages, raw otherwise. |
| `rfc5424` | Parse every message as RFC 5424. |
| `rfc3164` | Parse every message as RFC 3164 (BSD syslog). |
| `raw` | No parsing; the message is stored in `_raw`. |

A message that fails to parse is stored as a raw event and counted in `lynxdb_syslog_parse_errors_total`.

### Extracted Fields

| Field | RFC 5424 | RFC 3164 | Description |
|-------|----------|----------|-------------|
| `facility`, `facility_label` | yes | yes | Facility code (0 to 23) and name (`kern`, `user`, `daemon`, `local0` to `local7`, ...) |
| `severity`, `severity_label` | yes | yes | Severity (0 to 7) and name (`emerg`, `alert`, `crit`, `err`, `warning`, `notice`, `info`, `debug`) |
| `level` | yes | yes | Same as `severity_label` |
| `host` | yes | yes | Hostname from the header |
| `app_name` | yes | yes | APP-NAME (RFC 5424) or tag (RFC 3164) |
| `procid` | yes | yes | Process ID |
| `msgid` | yes | no | MSGID |
| `message` | yes | yes | Message text |
| `sd_<SD-ID>_<param>` | yes | no | Structured data parameters |

Each event also keeps the message in `_raw`, and the `key=value` pairs of the message text become fields too. The event time is the header timestamp, or the receive time when the header has none. The `sourcetype` field is `<syslog.sourcetype>:<dialect>`: `syslog:rfc5424`, `syslog:rfc3164` or `syslog:raw`.

## TCP Framing

| Key | Env var | Default | Values |
|-----|---------|---------|--------|
| `syslog.framing` | `LYNXDB_SYSLOG_FRAMING` | `auto` | `auto`, `octet-counting`, `non-transparent` |
| `syslog.trailer` | `LYNXDB_SYSLOG_TRAILER` | `auto` | `auto`, `lf`, `nul`, `crlf` |

With `auto` framing, a connection whose first byte is a digit uses octet counting (`<length> <message>`); any other connection uses non-transparent framing. With `auto` trailer, the first LF or NUL on the connection sets the trailer; a CR before LF is stripped. Framing does not apply to UDP.

## Timestamps, Host and Routing

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `syslog.default_timezone` | `LYNXDB_SYSLOG_DEFAULT_TIMEZONE` | `Local` | Time zone for RFC 3164 timestamps, which carry no zone or year. `Local` or an IANA name such as `UTC` or `Europe/Berlin`. |
| `syslog.default_hostname` | `LYNXDB_SYSLOG_DEFAULT_HOSTNAME` | empty | Host for messages whose hostname is missing or `-`. |
| `syslog.index` | `LYNXDB_SYSLOG_INDEX` (flag `--syslog-index`) | `main` | Target index. |
| `syslog.sourcetype` | `LYNXDB_SYSLOG_SOURCETYPE` | `syslog` | Base sourcetype; the dialect is appended. |
| `syslog.use_peer_as_source` | `LYNXDB_SYSLOG_USE_PEER_AS_SOURCE` | `true` | Set the `source` field to `udp://<peer>` or `tcp://<peer>`. |

## Limits and Batching

| Key | Env var | Default | Description |
|-----|---------|---------|-------------|
| `syslog.max_message_bytes` | `LYNXDB_SYSLOG_MAX_MESSAGE_BYTES` | `65536` | Larger messages are dropped. At least 1024. |
| `syslog.udp_read_buffer` | `LYNXDB_SYSLOG_UDP_READ_BUFFER` | `2mb` | UDP socket receive buffer. Raise it on busy UDP receivers to avoid kernel drops. |
| `syslog.tcp_idle_timeout` | `LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT` | `5m` | Close TCP connections idle this long. |
| `syslog.tcp_max_connections` | `LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS` | `1000` | Further connections are closed immediately. |
| `syslog.batch_size` | `LYNXDB_SYSLOG_BATCH_SIZE` | `1000` | Events per batch handed to ingest. |
| `syslog.batch_timeout` | `LYNXDB_SYSLOG_BATCH_TIMEOUT` | `200ms` | Hand over a partial batch after this long. |

## Reload

`syslog.index`, `syslog.sourcetype`, `syslog.default_timezone`, `syslog.default_hostname`, `syslog.batch_size` and `syslog.batch_timeout` take effect on reload. Listener addresses, TLS, parser, framing, trailer, buffer and connection limits need a restart.

## Metrics

| Metric | Type | Labels |
|--------|------|--------|
| `lynxdb_syslog_messages_received_total` | counter | `transport`, `dialect` |
| `lynxdb_syslog_messages_dropped_total` | counter | `transport`, `reason` (`toolarge`, `conn_limit`, `backpressure`) |
| `lynxdb_syslog_active_connections` | gauge | `transport="tcp"` |
| `lynxdb_syslog_parse_errors_total` | counter | `dialect` |

## Complete Example

```yaml
tls:
  cert_file: /etc/lynxdb/tls/server.crt
  key_file: /etc/lynxdb/tls/server.key

syslog:
  udp: ":5514"
  tcp: ":6514"
  tls: true
  parser: auto
  default_timezone: "UTC"
  index: "main"
  sourcetype: "syslog"
  max_message_bytes: 131072
  udp_read_buffer: "8mb"
  tcp_max_connections: 5000
  batch_size: 1000
  batch_timeout: "200ms"
```

## Related

- [Ingest Settings](./ingest.md)
- [Ingest Data Guide](../guides/ingest-data.md)
- [TLS and Authentication](../deployment/tls-auth.md)
- [Monitoring](../operations/monitoring.md)
