package config

// Environment variable names for LynxDB configuration.
// Defined here so they can be referenced in envBindings, Entries, and elsewhere.
const (
	// LYNXDB_AUTH_ENABLED is the enable or disable API key authentication.
	// YAML key: auth.enabled. Default: false.
	LYNXDB_AUTH_ENABLED = "LYNXDB_AUTH_ENABLED"
	// LYNXDB_CONFIG is the path to the YAML config file (overrides auto-discovery).
	LYNXDB_CONFIG = "LYNXDB_CONFIG"
	// LYNXDB_DATA_DIR is the root data directory for persistent storage.
	// YAML key: data_dir. Default: ~/.local/share/lynxdb.
	LYNXDB_DATA_DIR = "LYNXDB_DATA_DIR"
	// LYNXDB_HTTP_IDLE_TIMEOUT is the HTTP keep-alive idle timeout.
	// YAML key: http.idle_timeout. Default: 120s.
	LYNXDB_HTTP_IDLE_TIMEOUT = "LYNXDB_HTTP_IDLE_TIMEOUT"
	// LYNXDB_HTTP_RATE_LIMIT is the requests per second per IP (0 = unlimited).
	// YAML key: http.rate_limit. Default: 0.
	LYNXDB_HTTP_RATE_LIMIT = "LYNXDB_HTTP_RATE_LIMIT"
	// LYNXDB_HTTP_READ_HEADER_TIMEOUT is the max time allowed to read request headers.
	// YAML key: http.read_header_timeout. Default: 10s.
	LYNXDB_HTTP_READ_HEADER_TIMEOUT = "LYNXDB_HTTP_READ_HEADER_TIMEOUT"
	// LYNXDB_HTTP_SHUTDOWN_TIMEOUT is the graceful shutdown timeout.
	// YAML key: http.shutdown_timeout. Default: 30s.
	LYNXDB_HTTP_SHUTDOWN_TIMEOUT = "LYNXDB_HTTP_SHUTDOWN_TIMEOUT"
	// LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION is the version shown to Elasticsearch shippers.
	// YAML key: ingest.es_compat.advertised_version. Default: 8.15.0.
	LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION = "LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION"
	// LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME is the cluster name in Elasticsearch handshake.
	// YAML key: ingest.es_compat.cluster_name. Default: lynxdb.
	LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME = "LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME"
	// LYNXDB_INGEST_ES_COMPAT_ENABLED is the enable drop-in Elasticsearch shipper endpoints.
	// YAML key: ingest.es_compat.enabled. Default: true.
	LYNXDB_INGEST_ES_COMPAT_ENABLED = "LYNXDB_INGEST_ES_COMPAT_ENABLED"
	// LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES is the max compressed shipper request body.
	// YAML key: ingest.limits.max_compressed_body_bytes. Default: 32mb.
	LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES = "LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES"
	// LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES is the max decoded shipper request body.
	// YAML key: ingest.limits.max_decompressed_body_bytes. Default: 256mb.
	LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES = "LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES"
	// LYNXDB_INGEST_MAX_BATCH_SIZE is the max events per streaming batch.
	// YAML key: ingest.max_batch_size. Default: 1000.
	LYNXDB_INGEST_MAX_BATCH_SIZE = "LYNXDB_INGEST_MAX_BATCH_SIZE"
	// LYNXDB_INGEST_MAX_BODY_SIZE is the max HTTP request body size.
	// YAML key: ingest.max_body_size. Default: 100mb.
	LYNXDB_INGEST_MAX_BODY_SIZE = "LYNXDB_INGEST_MAX_BODY_SIZE"
	// LYNXDB_INGEST_MAX_LINE_BYTES is the max bytes per single line in streaming ingest.
	// YAML key: ingest.max_line_bytes. Default: 1048576.
	LYNXDB_INGEST_MAX_LINE_BYTES = "LYNXDB_INGEST_MAX_LINE_BYTES"
	// LYNXDB_INGEST_MODE is the ingest mode (empty = auto).
	// YAML key: ingest.mode. Default: "".
	LYNXDB_INGEST_MODE = "LYNXDB_INGEST_MODE"
	// LYNXDB_INGEST_OTLP_GRPC_LISTEN is the OTLP/gRPC listen address (empty = disabled).
	// YAML key: ingest.otlp.grpc_listen. Default: "".
	LYNXDB_INGEST_OTLP_GRPC_LISTEN = "LYNXDB_INGEST_OTLP_GRPC_LISTEN"
	// LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES is the OTLP/gRPC max receive message size.
	// YAML key: ingest.otlp.grpc_max_recv_bytes. Default: 16mb.
	LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES = "LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES"
	// LYNXDB_INGEST_OTLP_HTTP_LISTEN is the canonical OTLP/HTTP listen address (empty = disabled).
	// YAML key: ingest.otlp.http_listen. Default: 0.0.0.0:4318.
	LYNXDB_INGEST_OTLP_HTTP_LISTEN = "LYNXDB_INGEST_OTLP_HTTP_LISTEN"
	// LYNXDB_INGEST_SPLUNK_HEC_ENABLED is the enable Splunk HEC-compatible routes.
	// YAML key: ingest.splunk_hec.enabled. Default: true.
	LYNXDB_INGEST_SPLUNK_HEC_ENABLED = "LYNXDB_INGEST_SPLUNK_HEC_ENABLED"
	// LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN is the require valid Splunk token for HEC requests.
	// YAML key: ingest.splunk_hec.require_token. Default: false.
	LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN = "LYNXDB_INGEST_SPLUNK_HEC_REQUIRE_TOKEN"
	// LYNXDB_LISTEN is the listen address for the HTTP API server.
	// YAML key: listen. Default: localhost:3100.
	LYNXDB_LISTEN = "LYNXDB_LISTEN"
	// LYNXDB_LOG_LEVEL is the log level: debug, info, warn, error.
	// YAML key: log_level. Default: info.
	LYNXDB_LOG_LEVEL = "LYNXDB_LOG_LEVEL"
	// LYNXDB_QUERY_DEFAULT_RESULT_LIMIT is the default result row limit.
	// YAML key: query.default_result_limit. Default: 1000.
	LYNXDB_QUERY_DEFAULT_RESULT_LIMIT = "LYNXDB_QUERY_DEFAULT_RESULT_LIMIT"
	// LYNXDB_QUERY_JOB_TTL is the completed job retention time.
	// YAML key: query.job_ttl. Default: 5m.
	LYNXDB_QUERY_JOB_TTL = "LYNXDB_QUERY_JOB_TTL"
	// LYNXDB_QUERY_MAX_CONCURRENT is the max concurrent queries.
	// YAML key: query.max_concurrent. Default: 32.
	LYNXDB_QUERY_MAX_CONCURRENT = "LYNXDB_QUERY_MAX_CONCURRENT"
	// LYNXDB_QUERY_MAX_QUERY_LENGTH is the max query source length in bytes.
	// YAML key: query.max_query_length. Default: 1048576.
	LYNXDB_QUERY_MAX_QUERY_LENGTH = "LYNXDB_QUERY_MAX_QUERY_LENGTH"
	// LYNXDB_QUERY_MAX_RESULT_LIMIT is the maximum allowed result row limit.
	// YAML key: query.max_result_limit. Default: 50000.
	LYNXDB_QUERY_MAX_RESULT_LIMIT = "LYNXDB_QUERY_MAX_RESULT_LIMIT"
	// LYNXDB_QUERY_SYNC_TIMEOUT is the max wait for synchronous query completion.
	// YAML key: query.sync_timeout. Default: 30s.
	LYNXDB_QUERY_SYNC_TIMEOUT = "LYNXDB_QUERY_SYNC_TIMEOUT"
	// LYNXDB_RETENTION is the global data retention period; events older than this are deleted.
	// YAML key: retention. Default: 7d.
	LYNXDB_RETENTION = "LYNXDB_RETENTION"
	// LYNXDB_STORAGE_FLUSH_INTERVAL is the longest wait before ingested events are written (0 = store default).
	// YAML key: storage.flush_interval. Default: 0s.
	LYNXDB_STORAGE_FLUSH_INTERVAL = "LYNXDB_STORAGE_FLUSH_INTERVAL"
	// LYNXDB_STORAGE_PARTITION is the span of time whose events are kept apart.
	// YAML key: storage.partition. Default: 24h.
	LYNXDB_STORAGE_PARTITION = "LYNXDB_STORAGE_PARTITION"
	// LYNXDB_STORAGE_TIME_FIELDS is the comma-separated list of fields that set the event time, in order.
	// YAML key: storage.time_fields. Default: _time,timestamp,@timestamp,time,ts.
	LYNXDB_STORAGE_TIME_FIELDS = "LYNXDB_STORAGE_TIME_FIELDS"
	// LYNXDB_SYSLOG_BATCH_SIZE is the flush after this many events.
	// YAML key: syslog.batch_size. Default: 1000.
	LYNXDB_SYSLOG_BATCH_SIZE = "LYNXDB_SYSLOG_BATCH_SIZE"
	// LYNXDB_SYSLOG_BATCH_TIMEOUT is the flush after this much idle time.
	// YAML key: syslog.batch_timeout. Default: 200ms.
	LYNXDB_SYSLOG_BATCH_TIMEOUT = "LYNXDB_SYSLOG_BATCH_TIMEOUT"
	// LYNXDB_SYSLOG_DEFAULT_HOSTNAME is the hostname used when wire value is missing.
	// YAML key: syslog.default_hostname. Default: "".
	LYNXDB_SYSLOG_DEFAULT_HOSTNAME = "LYNXDB_SYSLOG_DEFAULT_HOSTNAME"
	// LYNXDB_SYSLOG_DEFAULT_TIMEZONE is the timezone for RFC 3164 timestamps.
	// YAML key: syslog.default_timezone. Default: Local.
	LYNXDB_SYSLOG_DEFAULT_TIMEZONE = "LYNXDB_SYSLOG_DEFAULT_TIMEZONE"
	// LYNXDB_SYSLOG_FRAMING is the syslog framing: auto, octet-counting, non-transparent.
	// YAML key: syslog.framing. Default: auto.
	LYNXDB_SYSLOG_FRAMING = "LYNXDB_SYSLOG_FRAMING"
	// LYNXDB_SYSLOG_INDEX is the target index for syslog events.
	// YAML key: syslog.index. Default: main.
	LYNXDB_SYSLOG_INDEX = "LYNXDB_SYSLOG_INDEX"
	// LYNXDB_SYSLOG_MAX_MESSAGE_BYTES is the hard cap for a single syslog message.
	// YAML key: syslog.max_message_bytes. Default: 65536.
	LYNXDB_SYSLOG_MAX_MESSAGE_BYTES = "LYNXDB_SYSLOG_MAX_MESSAGE_BYTES"
	// LYNXDB_SYSLOG_PARSER is the syslog parser: auto, rfc5424, rfc3164, raw.
	// YAML key: syslog.parser. Default: auto.
	LYNXDB_SYSLOG_PARSER = "LYNXDB_SYSLOG_PARSER"
	// LYNXDB_SYSLOG_SOURCETYPE is the base sourcetype, suffixed by parsed dialect.
	// YAML key: syslog.sourcetype. Default: syslog.
	LYNXDB_SYSLOG_SOURCETYPE = "LYNXDB_SYSLOG_SOURCETYPE"
	// LYNXDB_SYSLOG_TCP is the TCP syslog listen address (empty = disabled).
	// YAML key: syslog.tcp. Default: "".
	LYNXDB_SYSLOG_TCP = "LYNXDB_SYSLOG_TCP"
	// LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT is the TCP connection idle timeout.
	// YAML key: syslog.tcp_idle_timeout. Default: 5m.
	LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT = "LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT"
	// LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS is the max concurrent TCP syslog connections.
	// YAML key: syslog.tcp_max_connections. Default: 1000.
	LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS = "LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS"
	// LYNXDB_SYSLOG_TLS is the wrap TCP syslog with server TLS config.
	// YAML key: syslog.tls. Default: false.
	LYNXDB_SYSLOG_TLS = "LYNXDB_SYSLOG_TLS"
	// LYNXDB_SYSLOG_TRAILER is the syslog trailer: auto, lf, nul, crlf.
	// YAML key: syslog.trailer. Default: auto.
	LYNXDB_SYSLOG_TRAILER = "LYNXDB_SYSLOG_TRAILER"
	// LYNXDB_SYSLOG_UDP is the UDP syslog listen address (empty = disabled).
	// YAML key: syslog.udp. Default: "".
	LYNXDB_SYSLOG_UDP = "LYNXDB_SYSLOG_UDP"
	// LYNXDB_SYSLOG_UDP_READ_BUFFER is the UDP socket receive buffer.
	// YAML key: syslog.udp_read_buffer. Default: 2mb.
	LYNXDB_SYSLOG_UDP_READ_BUFFER = "LYNXDB_SYSLOG_UDP_READ_BUFFER"
	// LYNXDB_SYSLOG_USE_PEER_AS_SOURCE is the set source to udp://peer or tcp://peer.
	// YAML key: syslog.use_peer_as_source. Default: true.
	LYNXDB_SYSLOG_USE_PEER_AS_SOURCE = "LYNXDB_SYSLOG_USE_PEER_AS_SOURCE"
	// LYNXDB_TLS_CERT_FILE is the path to PEM certificate file.
	// YAML key: tls.cert_file. Default: "".
	LYNXDB_TLS_CERT_FILE = "LYNXDB_TLS_CERT_FILE"
	// LYNXDB_TLS_ENABLED is the enable TLS (auto self-signed if no cert/key).
	// YAML key: tls.enabled. Default: false.
	LYNXDB_TLS_ENABLED = "LYNXDB_TLS_ENABLED"
	// LYNXDB_TLS_KEY_FILE is the path to PEM private key file.
	// YAML key: tls.key_file. Default: "".
	LYNXDB_TLS_KEY_FILE = "LYNXDB_TLS_KEY_FILE"
)
