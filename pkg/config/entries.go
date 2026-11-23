package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Entry describes a single config value with its source.
type Entry struct {
	Key    string
	Value  string
	Source string // "default", "config file", "env var", or a CLI flag like "--addr"
}

// CLIOverride represents a CLI flag that was explicitly set.
type CLIOverride struct {
	Key   string // config key, e.g. "listen", "data_dir"
	Value string // display value
	Flag  string // the flag name, e.g. "--addr", "--data-dir"
}

// Entries returns all config entries with their sources for display.
// ConfigPath is the explicit --config flag value (empty = auto-discover).
func Entries(configPath string) []Entry {
	return EntriesWithCLI(configPath, nil)
}

// EntriesWithCLI returns all config entries with sources, including CLI flag overrides.
// CLI overrides take highest precedence: CLI > env > config file > default.
func EntriesWithCLI(configPath string, cli []CLIOverride) []Entry {
	dflt := DefaultConfig()

	cliMap := make(map[string]CLIOverride, len(cli))
	for _, o := range cli {
		cliMap[o.Key] = o
	}

	// Load file-only config (without env overlays) to detect file sources.
	path, _ := resolveConfigPath(configPath)
	var fileCfg Config
	hasFile := false
	if path != "" {
		if data, err := os.ReadFile(path); err == nil {
			if err := yaml.Unmarshal(data, &fileCfg); err == nil {
				hasFile = true
			}
		}
	}

	// Final config with full precedence (file + env, no CLI).
	cfg, _, _ := Load(configPath)

	var entries []Entry
	add := func(key, value, dfltValue, fileValue, envVar string) {
		// CLI flag takes highest precedence.
		if o, ok := cliMap[key]; ok {
			entries = append(entries, Entry{Key: key, Value: o.Value, Source: o.Flag})

			return
		}
		source := "default"
		if envVar != "" && os.Getenv(envVar) != "" {
			source = "env var"
		} else if hasFile && fileValue != "" && fileValue != dfltValue {
			source = "config file"
		}
		entries = append(entries, Entry{Key: key, Value: value, Source: source})
	}

	// Top-level.
	add("listen", cfg.Listen, dflt.Listen, fileCfg.Listen, LYNXDB_LISTEN)
	add("data_dir", cfg.DataDir, dflt.DataDir, fileCfg.DataDir, LYNXDB_DATA_DIR)
	add("retention", cfg.Retention.String(), dflt.Retention.String(), fileCfg.Retention.String(), LYNXDB_RETENTION)
	add("log_level", cfg.LogLevel, dflt.LogLevel, fileCfg.LogLevel, LYNXDB_LOG_LEVEL)

	// Storage.
	add("storage.partition", cfg.Storage.Partition.String(), dflt.Storage.Partition.String(), fileCfg.Storage.Partition.String(), LYNXDB_STORAGE_PARTITION)
	add("storage.flush_interval", cfg.Storage.FlushInterval.String(), dflt.Storage.FlushInterval.String(), fileCfg.Storage.FlushInterval.String(), LYNXDB_STORAGE_FLUSH_INTERVAL)
	add("storage.time_fields", strings.Join(cfg.Storage.TimeFields, ","), strings.Join(dflt.Storage.TimeFields, ","), strings.Join(fileCfg.Storage.TimeFields, ","), LYNXDB_STORAGE_TIME_FIELDS)
	add("storage.sort_key", strings.Join(cfg.Storage.SortKey, ","), strings.Join(dflt.Storage.SortKey, ","), strings.Join(fileCfg.Storage.SortKey, ","), LYNXDB_STORAGE_SORT_KEY)

	// Helper for duration fields.
	durationEntry := func(key string, val, dfltVal, fileVal time.Duration, envVar string) {
		add(key, val.String(), dfltVal.String(), fileVal.String(), envVar)
	}

	// Query.
	durationEntry("query.sync_timeout", cfg.Query.SyncTimeout, dflt.Query.SyncTimeout, fileCfg.Query.SyncTimeout, LYNXDB_QUERY_SYNC_TIMEOUT)
	durationEntry("query.job_ttl", cfg.Query.JobTTL, dflt.Query.JobTTL, fileCfg.Query.JobTTL, LYNXDB_QUERY_JOB_TTL)
	add("query.max_concurrent", fmt.Sprintf("%d", cfg.Query.MaxConcurrent), fmt.Sprintf("%d", dflt.Query.MaxConcurrent), fmt.Sprintf("%d", fileCfg.Query.MaxConcurrent), LYNXDB_QUERY_MAX_CONCURRENT)
	add("query.default_result_limit", fmt.Sprintf("%d", cfg.Query.DefaultResultLimit), fmt.Sprintf("%d", dflt.Query.DefaultResultLimit), fmt.Sprintf("%d", fileCfg.Query.DefaultResultLimit), LYNXDB_QUERY_DEFAULT_RESULT_LIMIT)
	add("query.max_result_limit", fmt.Sprintf("%d", cfg.Query.MaxResultLimit), fmt.Sprintf("%d", dflt.Query.MaxResultLimit), fmt.Sprintf("%d", fileCfg.Query.MaxResultLimit), LYNXDB_QUERY_MAX_RESULT_LIMIT)
	add("query.max_query_length", strconv.Itoa(cfg.Query.MaxQueryLength), strconv.Itoa(dflt.Query.MaxQueryLength), strconv.Itoa(fileCfg.Query.MaxQueryLength), "")

	// Ingest.
	add("ingest.max_body_size", cfg.Ingest.MaxBodySize.String(), dflt.Ingest.MaxBodySize.String(), fileCfg.Ingest.MaxBodySize.String(), LYNXDB_INGEST_MAX_BODY_SIZE)
	add("ingest.max_batch_size", fmt.Sprintf("%d", cfg.Ingest.MaxBatchSize), fmt.Sprintf("%d", dflt.Ingest.MaxBatchSize), fmt.Sprintf("%d", fileCfg.Ingest.MaxBatchSize), LYNXDB_INGEST_MAX_BATCH_SIZE)
	add("ingest.max_line_bytes", fmt.Sprintf("%d", cfg.Ingest.MaxLineBytes), fmt.Sprintf("%d", dflt.Ingest.MaxLineBytes), fmt.Sprintf("%d", fileCfg.Ingest.MaxLineBytes), LYNXDB_INGEST_MAX_LINE_BYTES)
	add("ingest.es_compat.enabled", strconv.FormatBool(cfg.Ingest.ESCompat.Enabled), strconv.FormatBool(dflt.Ingest.ESCompat.Enabled), strconv.FormatBool(fileCfg.Ingest.ESCompat.Enabled), LYNXDB_INGEST_ES_COMPAT_ENABLED)
	add("ingest.es_compat.advertised_version", cfg.Ingest.ESCompat.AdvertisedVersion, dflt.Ingest.ESCompat.AdvertisedVersion, fileCfg.Ingest.ESCompat.AdvertisedVersion, LYNXDB_INGEST_ES_COMPAT_ADVERTISED_VERSION)
	add("ingest.es_compat.cluster_name", cfg.Ingest.ESCompat.ClusterName, dflt.Ingest.ESCompat.ClusterName, fileCfg.Ingest.ESCompat.ClusterName, LYNXDB_INGEST_ES_COMPAT_CLUSTER_NAME)
	add("ingest.otlp.http_listen", cfg.Ingest.OTLP.HTTPListen, dflt.Ingest.OTLP.HTTPListen, fileCfg.Ingest.OTLP.HTTPListen, LYNXDB_INGEST_OTLP_HTTP_LISTEN)
	add("ingest.otlp.grpc_listen", cfg.Ingest.OTLP.GRPCListen, dflt.Ingest.OTLP.GRPCListen, fileCfg.Ingest.OTLP.GRPCListen, LYNXDB_INGEST_OTLP_GRPC_LISTEN)
	add("ingest.otlp.grpc_max_recv_bytes", cfg.Ingest.OTLP.GRPCMaxRecvBytes.String(), dflt.Ingest.OTLP.GRPCMaxRecvBytes.String(), fileCfg.Ingest.OTLP.GRPCMaxRecvBytes.String(), LYNXDB_INGEST_OTLP_GRPC_MAX_RECV_BYTES)
	add("ingest.splunk_hec.enabled", strconv.FormatBool(cfg.Ingest.SplunkHEC.Enabled), strconv.FormatBool(dflt.Ingest.SplunkHEC.Enabled), strconv.FormatBool(fileCfg.Ingest.SplunkHEC.Enabled), LYNXDB_INGEST_SPLUNK_HEC_ENABLED)
	add("ingest.limits.max_compressed_body_bytes", cfg.Ingest.Limits.MaxCompressedBodyBytes.String(), dflt.Ingest.Limits.MaxCompressedBodyBytes.String(), fileCfg.Ingest.Limits.MaxCompressedBodyBytes.String(), LYNXDB_INGEST_LIMITS_MAX_COMPRESSED_BODY_BYTES)
	add("ingest.limits.max_decompressed_body_bytes", cfg.Ingest.Limits.MaxDecompressedBodyBytes.String(), dflt.Ingest.Limits.MaxDecompressedBodyBytes.String(), fileCfg.Ingest.Limits.MaxDecompressedBodyBytes.String(), LYNXDB_INGEST_LIMITS_MAX_DECOMPRESSED_BODY_BYTES)
	add("ingest.mode", cfg.Ingest.Mode, dflt.Ingest.Mode, fileCfg.Ingest.Mode, LYNXDB_INGEST_MODE)

	// HTTP.
	durationEntry("http.idle_timeout", cfg.HTTP.IdleTimeout, dflt.HTTP.IdleTimeout, fileCfg.HTTP.IdleTimeout, LYNXDB_HTTP_IDLE_TIMEOUT)
	durationEntry("http.shutdown_timeout", cfg.HTTP.ShutdownTimeout, dflt.HTTP.ShutdownTimeout, fileCfg.HTTP.ShutdownTimeout, LYNXDB_HTTP_SHUTDOWN_TIMEOUT)
	durationEntry("http.read_header_timeout", cfg.HTTP.ReadHeaderTimeout, dflt.HTTP.ReadHeaderTimeout, fileCfg.HTTP.ReadHeaderTimeout, "")
	add("http.rate_limit", fmt.Sprintf("%.2f", cfg.HTTP.RateLimit), fmt.Sprintf("%.2f", dflt.HTTP.RateLimit), fmt.Sprintf("%.2f", fileCfg.HTTP.RateLimit), LYNXDB_HTTP_RATE_LIMIT)

	// Syslog.
	add("syslog.udp", cfg.Syslog.UDP, dflt.Syslog.UDP, fileCfg.Syslog.UDP, LYNXDB_SYSLOG_UDP)
	add("syslog.tcp", cfg.Syslog.TCP, dflt.Syslog.TCP, fileCfg.Syslog.TCP, LYNXDB_SYSLOG_TCP)
	add("syslog.tls", strconv.FormatBool(cfg.Syslog.TLS), strconv.FormatBool(dflt.Syslog.TLS), strconv.FormatBool(fileCfg.Syslog.TLS), LYNXDB_SYSLOG_TLS)
	add("syslog.parser", cfg.Syslog.Parser, dflt.Syslog.Parser, fileCfg.Syslog.Parser, LYNXDB_SYSLOG_PARSER)
	add("syslog.framing", cfg.Syslog.Framing, dflt.Syslog.Framing, fileCfg.Syslog.Framing, LYNXDB_SYSLOG_FRAMING)
	add("syslog.trailer", cfg.Syslog.Trailer, dflt.Syslog.Trailer, fileCfg.Syslog.Trailer, LYNXDB_SYSLOG_TRAILER)
	add("syslog.default_timezone", cfg.Syslog.DefaultTimezone, dflt.Syslog.DefaultTimezone, fileCfg.Syslog.DefaultTimezone, LYNXDB_SYSLOG_DEFAULT_TIMEZONE)
	add("syslog.default_hostname", cfg.Syslog.DefaultHostname, dflt.Syslog.DefaultHostname, fileCfg.Syslog.DefaultHostname, LYNXDB_SYSLOG_DEFAULT_HOSTNAME)
	add("syslog.index", cfg.Syslog.Index, dflt.Syslog.Index, fileCfg.Syslog.Index, LYNXDB_SYSLOG_INDEX)
	add("syslog.sourcetype", cfg.Syslog.SourceType, dflt.Syslog.SourceType, fileCfg.Syslog.SourceType, LYNXDB_SYSLOG_SOURCETYPE)
	add("syslog.use_peer_as_source", strconv.FormatBool(cfg.Syslog.UsePeerAsSource), strconv.FormatBool(dflt.Syslog.UsePeerAsSource), strconv.FormatBool(fileCfg.Syslog.UsePeerAsSource), LYNXDB_SYSLOG_USE_PEER_AS_SOURCE)
	add("syslog.max_message_bytes", strconv.Itoa(cfg.Syslog.MaxMessageBytes), strconv.Itoa(dflt.Syslog.MaxMessageBytes), strconv.Itoa(fileCfg.Syslog.MaxMessageBytes), LYNXDB_SYSLOG_MAX_MESSAGE_BYTES)
	add("syslog.udp_read_buffer", cfg.Syslog.UDPReadBuffer.String(), dflt.Syslog.UDPReadBuffer.String(), fileCfg.Syslog.UDPReadBuffer.String(), LYNXDB_SYSLOG_UDP_READ_BUFFER)
	add("syslog.tcp_idle_timeout", cfg.Syslog.TCPIdleTimeout.String(), dflt.Syslog.TCPIdleTimeout.String(), fileCfg.Syslog.TCPIdleTimeout.String(), LYNXDB_SYSLOG_TCP_IDLE_TIMEOUT)
	add("syslog.tcp_max_connections", strconv.Itoa(cfg.Syslog.TCPMaxConns), strconv.Itoa(dflt.Syslog.TCPMaxConns), strconv.Itoa(fileCfg.Syslog.TCPMaxConns), LYNXDB_SYSLOG_TCP_MAX_CONNECTIONS)
	add("syslog.batch_size", strconv.Itoa(cfg.Syslog.BatchSize), strconv.Itoa(dflt.Syslog.BatchSize), strconv.Itoa(fileCfg.Syslog.BatchSize), LYNXDB_SYSLOG_BATCH_SIZE)
	add("syslog.batch_timeout", cfg.Syslog.BatchTimeout.String(), dflt.Syslog.BatchTimeout.String(), fileCfg.Syslog.BatchTimeout.String(), LYNXDB_SYSLOG_BATCH_TIMEOUT)

	// TLS.
	add("tls.enabled", strconv.FormatBool(cfg.TLS.Enabled), strconv.FormatBool(dflt.TLS.Enabled), strconv.FormatBool(fileCfg.TLS.Enabled), LYNXDB_TLS_ENABLED)
	add("tls.cert_file", cfg.TLS.CertFile, dflt.TLS.CertFile, fileCfg.TLS.CertFile, LYNXDB_TLS_CERT_FILE)
	add("tls.key_file", cfg.TLS.KeyFile, dflt.TLS.KeyFile, fileCfg.TLS.KeyFile, LYNXDB_TLS_KEY_FILE)

	// Auth.
	add("auth.enabled", strconv.FormatBool(cfg.Auth.Enabled), strconv.FormatBool(dflt.Auth.Enabled), strconv.FormatBool(fileCfg.Auth.Enabled), LYNXDB_AUTH_ENABLED)

	return entries
}
