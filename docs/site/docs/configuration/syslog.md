---
title: Syslog Receiver
description: Configure the native syslog TCP/UDP receiver to ingest logs directly from syslog-enabled devices and agents.
---

# Syslog Receiver

The `syslog` section configures a native syslog receiver that listens for log messages over UDP (RFC 5426) and TCP (RFC 6587). LynxDB parses incoming messages using RFC 5424, RFC 3164, or a raw pass-through dialect and writes them to the storage engine through the same ingest pipeline used by the HTTP API.

The syslog receiver is disabled by default. Set at least one listen address (`udp` or `tcp`) to enable it.

## Quick Start

```bash
# Enable syslog on UDP and TCP port 514
lynxdb server --syslog :514

# UDP only on port 5514
lynxdb server --syslog-udp :5514

# TCP with TLS on port 6514
lynxdb server --syslog-tcp :6514 --syslog-tls
```

Or in a config file:

```yaml
syslog:
  udp: ":514"
  tcp: ":514"
```

Point any syslog-capable device or agent at the configured address:

```bash
# rsyslog
*.* @@lynxdb-host:514

# syslog-ng
destination d_lynxdb { tcp("lynxdb-host" port(514)); };
```

## Listen Addresses

### `syslog.udp`

| Config Key | `syslog.udp` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_UDP` |
| **Default** | _(empty, disabled)_ |

UDP listen address in `host:port` format. When empty, the UDP receiver is disabled.

```yaml
syslog:
  udp: ":514"
```

UDP is connectionless and suited for high-throughput fire-and-forget logging. Each datagram carries one syslog message.

### `syslog.tcp`

| Config Key | `syslog.tcp` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_TCP` |
| **Default** | _(empty, disabled)_ |

TCP listen address in `host:port` format. When empty, the TCP receiver is disabled.

```yaml
syslog:
  tcp: ":514"
```

TCP provides reliable, ordered delivery and supports connection-level batching. Most production syslog deployments use TCP.

### `syslog.tls`

| Config Key | `syslog.tls` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_TLS` |
| **Default** | `false` |

Wrap the TCP listener with the server TLS configuration. Requires TLS to be configured at the server level (`tls.enabled: true` or `--tls`). When `--syslog-tcp` is used without an explicit port and TLS is enabled, the default port becomes `6514` (the IANA-assigned port for syslog over TLS).

```yaml
tls:
  enabled: true
  cert_file: /etc/ssl/lynxdb.crt
  key_file: /etc/ssl/lynxdb.key

syslog:
  tcp: ":6514"
  tls: true
```

## Parser

### `syslog.parser`

| Config Key | `syslog.parser` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_PARSER` |
| **Default** | `auto` |

Controls how incoming syslog messages are parsed:

| Value | Description |
|---|---|
| `auto` | Detect dialect per message: RFC 5424 if the version digit is present, RFC 3164 for BSD-style pri headers, `raw` otherwise. Default. |
| `rfc5424` | Parse all messages as [RFC 5424](https://tools.ietf.org/html/rfc5424). Unparseable messages are stored as raw with `_parse_error=true`. |
| `rfc3164` | Parse all messages as [RFC 3164](https://tools.ietf.org/html/rfc3164) (BSD syslog). Unparseable messages are stored as raw with `_parse_error=true`. |
| `raw` | No parsing. The full message is stored in `_raw` as-is. |

### Extracted fields

Successful parsing extracts the following fields:

| Field | RFC 5424 | RFC 3164 | Description |
|---|---|---|---|
| `facility` | yes | yes | Numeric facility code (0--23) |
| `facility_label` | yes | yes | Human-readable label: `kern`, `user`, `daemon`, `local0`--`local7`, etc. |
| `severity` | yes | yes | Numeric severity (0--7) |
| `severity_label` | yes | yes | Human-readable label: `emerg`, `alert`, `crit`, `err`, `warning`, `notice`, `info`, `debug` |
| `level` | yes | yes | Alias for `severity_label` |
| `host` | yes | yes | Hostname from the syslog header |
| `app_name` | yes | yes | Application name (RFC 5424) or tag (RFC 3164) |
| `procid` | yes | yes | Process ID |
| `msgid` | yes | no | Message ID |
| `message` | yes | yes | Free-form message text |
| `sd_*` | yes | no | Structured data parameters, flattened as `sd_<SD-ID>_<param-name>` |

The `sourcetype` is set to `<base>:<dialect>` (e.g., `syslog:rfc5424`, `syslog:rfc3164`, `syslog:raw`).

## Framing

### `syslog.framing`

| Config Key | `syslog.framing` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_FRAMING` |
| **Default** | `auto` |

Controls how TCP message boundaries are detected:

| Value | Description |
|---|---|
| `auto` | Detect per connection: if the first byte is a digit, use octet-counting; otherwise use non-transparent framing. Default. |
| `octet-counting` | RFC 6587 octet-counting framing. Each frame is `<length> <message>`. |
| `non-transparent` | RFC 6587 non-transparent framing. Messages are delimited by a trailer character. |

Framing applies only to TCP. UDP datagrams carry one message each and do not use framing.

### `syslog.trailer`

| Config Key | `syslog.trailer` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_TRAILER` |
| **Default** | `auto` |

Trailer character for non-transparent framing. Only used when framing is (or auto-detects as) `non-transparent`.

| Value | Description |
|---|---|
| `auto` | Detect the trailer from the first message: `\n` (LF), `\0` (NUL), or `\r\n` (CRLF). Default. |
| `lf` | Line feed (`\n`) |
| `nul` | Null byte (`\0`) |
| `crlf` | Carriage return + line feed (`\r\n`) |

## Timestamps and Hostname

### `syslog.default_timezone`

| Config Key | `syslog.default_timezone` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_DEFAULT_TIMEZONE` |
| **Default** | `Local` |

Timezone used to interpret RFC 3164 timestamps, which do not include timezone or year information. Accepts `Local` (server timezone) or any IANA timezone name such as `UTC`, `America/New_York`, `Europe/Berlin`.

RFC 5424 timestamps include full timezone information and are not affected by this setting.

```yaml
syslog:
  default_timezone: "UTC"
```

### `syslog.default_hostname`

| Config Key | `syslog.default_hostname` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_DEFAULT_HOSTNAME` |
| **Default** | _(empty)_ |

Hostname to assign when the wire value is missing or `-`. When empty, the event host is left unset for such messages.

```yaml
syslog:
  default_hostname: "unknown"
```

## Routing

### `syslog.index`

| Config Key | `syslog.index` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_INDEX` |
| **Default** | `main` |

Target index for all syslog events.

```yaml
syslog:
  index: "syslog"
```

### `syslog.sourcetype`

| Config Key | `syslog.sourcetype` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_SOURCETYPE` |
| **Default** | `syslog` |

Base sourcetype for syslog events. The parsed dialect is appended as a suffix: `syslog:rfc5424`, `syslog:rfc3164`, or `syslog:raw`.

```yaml
syslog:
  sourcetype: "syslog"
```

### `syslog.use_peer_as_source`

| Config Key | `syslog.use_peer_as_source` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_USE_PEER_AS_SOURCE` |
| **Default** | `true` |

Set the event `_source` to the peer address (`udp://host:port` or `tcp://host:port`). When `false`, the source field is left empty.

## Size and Connection Limits

### `syslog.max_message_bytes`

| Config Key | `syslog.max_message_bytes` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_MAX_MESSAGE_BYTES` |
| **Default** | `65536` (64 KB) |

Maximum size of a single syslog message in bytes. Messages exceeding this limit are dropped. Must be at least 1024.

```yaml
syslog:
  max_message_bytes: 131072
```

### `syslog.udp_read_buffer`

| Config Key | `syslog.udp_read_buffer` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_UDP_READ_BUFFER` |
| **Default** | `2mb` |

UDP socket receive buffer size. Increase on high-throughput UDP receivers to reduce kernel-level drops.

```yaml
syslog:
  udp_read_buffer: "4mb"
```

### `syslog.tcp_idle_timeout`

| Config Key | `syslog.tcp_idle_timeout` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT` |
| **Default** | `5m` |

Idle timeout for TCP connections. Connections with no data within this period are closed.

```yaml
syslog:
  tcp_idle_timeout: "10m"
```

### `syslog.tcp_max_connections`

| Config Key | `syslog.tcp_max_connections` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS` |
| **Default** | `1000` |

Maximum number of concurrent TCP syslog connections. New connections exceeding this limit are immediately closed.

```yaml
syslog:
  tcp_max_connections: 5000
```

## Batching

Syslog events are batched in memory before being flushed to the storage engine.

### `syslog.batch_size`

| Config Key | `syslog.batch_size` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_BATCH_SIZE` |
| **Default** | `1000` |

Number of events to accumulate before flushing a batch.

### `syslog.batch_timeout`

| Config Key | `syslog.batch_timeout` |
|---|---|
| **Env Var** | `LYNXDB_SYSLOG_BATCH_TIMEOUT` |
| **Default** | `200ms` |

Maximum time to wait before flushing an incomplete batch. Triggers regardless of batch size when the timeout expires.

## CLI Flags

The `lynxdb server` command supports these syslog-related flags:

| Flag | Description |
|------|-------------|
| `--syslog <addr>` | Enable both UDP and TCP syslog on the given address (default port `5514` when omitted) |
| `--syslog-udp <addr>` | Enable UDP syslog only |
| `--syslog-tcp <addr>` | Enable TCP syslog only (default port `6514` when `--syslog-tls` is set) |
| `--syslog-tls` | Wrap TCP syslog with server TLS |
| `--syslog-parser <dialect>` | Parser dialect: `auto`, `rfc5424`, `rfc3164`, `raw` |
| `--syslog-index <name>` | Target index for syslog events |

## Hot-Reloadable Settings

The following syslog settings can be reloaded without restarting the server (`SIGHUP` or `lynxdb config reload`):

- `syslog.index`
- `syslog.sourcetype`
- `syslog.default_timezone`
- `syslog.default_hostname`
- `syslog.batch_size`
- `syslog.batch_timeout`

Listen addresses (`udp`, `tcp`), TLS, parser, framing, trailer, and connection limits require a server restart.

## Prometheus Metrics

The syslog receiver exposes these Prometheus metrics:

| Metric | Type | Labels | Description |
|---|---|---|---|
| `lynxdb_syslog_messages_received_total` | counter | `transport`, `dialect` | Total messages received |
| `lynxdb_syslog_messages_dropped_total` | counter | `transport`, `reason` | Total messages dropped (`toolarge`, `conn_limit`, `backpressure`) |
| `lynxdb_syslog_active_connections` | gauge | `transport=tcp` | Current active TCP connections |
| `lynxdb_syslog_parse_errors_total` | counter | `dialect` | Total parse errors |

## Complete Example

```yaml
syslog:
  udp: ":514"
  tcp: ":514"
  tls: false
  parser: auto
  framing: auto
  trailer: auto
  default_timezone: "UTC"
  default_hostname: ""
  index: "main"
  sourcetype: "syslog"
  use_peer_as_source: true
  max_message_bytes: 65536
  udp_read_buffer: "2mb"
  tcp_idle_timeout: "5m"
  tcp_max_connections: 1000
  batch_size: 1000
  batch_timeout: "200ms"
```

## Tuning Guidelines

| Scenario | Recommendation |
|---|---|
| High-throughput network devices | Use UDP, increase `udp_read_buffer` to `4mb` or higher |
| Reliable delivery required | Use TCP with `octet-counting` framing |
| Security / compliance | Enable TLS on TCP port 6514 |
| Many senders | Increase `tcp_max_connections` |
| Low-latency flush | Decrease `batch_timeout` to `50ms` |
| Large structured messages | Increase `max_message_bytes` |
| RFC 3164 devices in non-local timezone | Set `default_timezone` explicitly |

## Next Steps

- [Ingest Settings](/docs/configuration/ingest)
- [Ingest Data Guide](/docs/guides/ingest-data)
- [TLS and Auth](/docs/deployment/tls-auth)
- [Monitoring](/docs/operations/monitoring)
