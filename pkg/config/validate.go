package config

import (
	"fmt"
	"net"
	"regexp"
	"strings"
	"time"
)

// ValidationError describes a single config validation failure with context.
type ValidationError struct {
	Section string // "storage", "query", "ingest", "http", or "" for top-level
	Field   string // "partition", "log_level", etc.
	Value   string // the actual invalid value
	Message string // "must not be negative"
}

func (e *ValidationError) Error() string {
	if e.Section != "" {
		return fmt.Sprintf("%s.%s: %s", e.Section, e.Field, e.Message)
	}

	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

func validationErr(section, field, value, message string) *ValidationError {
	return &ValidationError{
		Section: section,
		Field:   field,
		Value:   value,
		Message: message,
	}
}

// Validate checks the configuration for invalid values.
func (c *Config) Validate() error {
	if c.Listen == "" {
		return validationErr("", "listen", c.Listen, "must not be empty")
	}
	if _, _, err := net.SplitHostPort(c.Listen); err != nil {
		return validationErr("", "listen", c.Listen, "must be a valid host:port address")
	}

	if c.Retention.Duration() < 0 {
		return validationErr("", "retention", c.Retention.String(), "must not be negative")
	}

	switch strings.ToLower(c.LogLevel) {
	case "debug", "info", "warn", "error":
		// ok
	default:
		return validationErr("", "log_level", c.LogLevel, "must be debug, info, warn, or error")
	}

	if err := c.Storage.validate(); err != nil {
		return err
	}
	if err := c.Query.validate(); err != nil {
		return err
	}
	if err := c.Ingest.validate(); err != nil {
		return err
	}

	if err := c.HTTP.validate(); err != nil {
		return err
	}
	if err := c.Syslog.validate(); err != nil {
		return err
	}

	return c.TLS.validate()
}

func (s *StorageConfig) validate() error {
	if s.Partition < 0 {
		return validationErr("storage", "partition", s.Partition.String(), "must not be negative")
	}
	if s.FlushInterval < 0 {
		return validationErr("storage", "flush_interval", s.FlushInterval.String(), "must not be negative")
	}
	for _, f := range s.TimeFields {
		if f == "" {
			return validationErr("storage", "time_fields", strings.Join(s.TimeFields, ","), "must not name an empty field")
		}
	}

	return nil
}

func (q *QueryConfig) validate() error {
	if q.SyncTimeout < time.Second {
		return validationErr("query", "sync_timeout", q.SyncTimeout.String(), "must be at least 1s")
	}
	if q.JobTTL < time.Second {
		return validationErr("query", "job_ttl", q.JobTTL.String(), "must be at least 1s")
	}
	if q.MaxConcurrent < 1 {
		return validationErr("query", "max_concurrent", fmt.Sprintf("%d", q.MaxConcurrent), "must be at least 1")
	}
	if q.DefaultResultLimit < 1 {
		return validationErr("query", "default_result_limit", fmt.Sprintf("%d", q.DefaultResultLimit), "must be at least 1")
	}
	if q.MaxResultLimit < q.DefaultResultLimit {
		return validationErr("query", "max_result_limit", fmt.Sprintf("%d", q.MaxResultLimit), "must be >= default_result_limit")
	}
	if q.MaxQueryLength < 0 {
		return validationErr("query", "max_query_length", fmt.Sprintf("%d", q.MaxQueryLength), "must not be negative (0 = unlimited)")
	}

	return nil
}

func (i *IngestConfig) validate() error {
	if i.MaxBodySize < 1*KB {
		return validationErr("ingest", "max_body_size", i.MaxBodySize.String(), "must be at least 1kb")
	}
	if i.MaxBatchSize < 1 {
		return validationErr("ingest", "max_batch_size", fmt.Sprintf("%d", i.MaxBatchSize), "must be at least 1")
	}
	if i.MaxLineBytes < 1024 {
		return validationErr("ingest", "max_line_bytes", fmt.Sprintf("%d", i.MaxLineBytes), "must be at least 1024 bytes")
	}
	if i.Limits.MaxCompressedBodyBytes < 1*KB {
		return validationErr("ingest", "limits.max_compressed_body_bytes", i.Limits.MaxCompressedBodyBytes.String(), "must be at least 1kb")
	}
	if i.Limits.MaxDecompressedBodyBytes < 1*KB {
		return validationErr("ingest", "limits.max_decompressed_body_bytes", i.Limits.MaxDecompressedBodyBytes.String(), "must be at least 1kb")
	}
	if i.Limits.MaxDecompressedBodyBytes < i.Limits.MaxCompressedBodyBytes {
		return validationErr("ingest", "limits.max_decompressed_body_bytes", i.Limits.MaxDecompressedBodyBytes.String(),
			fmt.Sprintf("must be >= limits.max_compressed_body_bytes (%s)", i.Limits.MaxCompressedBodyBytes.String()))
	}
	if i.ESCompat.AdvertisedVersion != "" && !esVersionRE.MatchString(i.ESCompat.AdvertisedVersion) {
		return validationErr("ingest", "es_compat.advertised_version", i.ESCompat.AdvertisedVersion, "must match X.Y.Z")
	}
	if i.ESCompat.Enabled && i.ESCompat.ClusterName == "" {
		return validationErr("ingest", "es_compat.cluster_name", "", "must not be empty when es_compat is enabled")
	}
	if i.OTLP.HTTPListen != "" {
		if _, _, err := net.SplitHostPort(i.OTLP.HTTPListen); err != nil {
			return validationErr("ingest", "otlp.http_listen", i.OTLP.HTTPListen, "must be a valid host:port address")
		}
	}
	if i.OTLP.GRPCListen != "" {
		if _, _, err := net.SplitHostPort(i.OTLP.GRPCListen); err != nil {
			return validationErr("ingest", "otlp.grpc_listen", i.OTLP.GRPCListen, "must be a valid host:port address")
		}
	}
	if i.OTLP.GRPCMaxRecvBytes < 1*MB {
		return validationErr("ingest", "otlp.grpc_max_recv_bytes", i.OTLP.GRPCMaxRecvBytes.String(), "must be at least 1mb")
	}

	switch i.Mode {
	case "", "full", "lightweight":
		// ok ("" uses default)
	default:
		return validationErr("ingest", "mode", i.Mode, "must be full or lightweight")
	}

	return nil
}

var esVersionRE = regexp.MustCompile(`^\d+\.\d+\.\d+$`)

func (s *SyslogConfig) validate() error {
	if s.UDP != "" {
		if _, _, err := net.SplitHostPort(s.UDP); err != nil {
			return validationErr("syslog", "udp", s.UDP, "must be a valid host:port address")
		}
	}
	if s.TCP != "" {
		if _, _, err := net.SplitHostPort(s.TCP); err != nil {
			return validationErr("syslog", "tcp", s.TCP, "must be a valid host:port address")
		}
	}

	switch strings.ToLower(s.Parser) {
	case "", "auto", "rfc5424", "rfc3164", "raw":
	default:
		return validationErr("syslog", "parser", s.Parser, "must be auto, rfc5424, rfc3164, or raw")
	}
	switch strings.ToLower(s.Framing) {
	case "", "auto", "octet-counting", "non-transparent":
	default:
		return validationErr("syslog", "framing", s.Framing, "must be auto, octet-counting, or non-transparent")
	}
	switch strings.ToLower(s.Trailer) {
	case "", "auto", "lf", "nul", "crlf":
	default:
		return validationErr("syslog", "trailer", s.Trailer, "must be auto, lf, nul, or crlf")
	}
	if s.DefaultTimezone != "" && s.DefaultTimezone != "Local" {
		if _, err := time.LoadLocation(s.DefaultTimezone); err != nil {
			return validationErr("syslog", "default_timezone", s.DefaultTimezone, "must be Local or a valid IANA timezone")
		}
	}
	if s.MaxMessageBytes != 0 && s.MaxMessageBytes < 1024 {
		return validationErr("syslog", "max_message_bytes", fmt.Sprintf("%d", s.MaxMessageBytes), "must be at least 1024 bytes")
	}
	if s.UDPReadBuffer < 0 {
		return validationErr("syslog", "udp_read_buffer", s.UDPReadBuffer.String(), "must not be negative")
	}
	if s.TCPIdleTimeout.Duration() < 0 {
		return validationErr("syslog", "tcp_idle_timeout", s.TCPIdleTimeout.String(), "must not be negative")
	}
	if s.TCPMaxConns < 0 {
		return validationErr("syslog", "tcp_max_connections", fmt.Sprintf("%d", s.TCPMaxConns), "must not be negative")
	}
	if s.BatchSize < 0 {
		return validationErr("syslog", "batch_size", fmt.Sprintf("%d", s.BatchSize), "must not be negative")
	}
	if s.BatchTimeout.Duration() < 0 {
		return validationErr("syslog", "batch_timeout", s.BatchTimeout.String(), "must not be negative")
	}

	return nil
}

func (t *TLSConfig) validate() error {
	// If cert_file is set, key_file must also be set (and vice versa).
	if t.CertFile != "" && t.KeyFile == "" {
		return validationErr("tls", "key_file", "", "must be set when cert_file is provided")
	}
	if t.KeyFile != "" && t.CertFile == "" {
		return validationErr("tls", "cert_file", "", "must be set when key_file is provided")
	}

	return nil
}

func (h *HTTPConfig) validate() error {
	if h.IdleTimeout < time.Second {
		return validationErr("http", "idle_timeout", h.IdleTimeout.String(), "must be at least 1s")
	}
	if h.ShutdownTimeout < time.Second {
		return validationErr("http", "shutdown_timeout", h.ShutdownTimeout.String(), "must be at least 1s")
	}
	if h.ReadHeaderTimeout < 0 {
		return validationErr("http", "read_header_timeout", h.ReadHeaderTimeout.String(), "must not be negative")
	}
	if h.RateLimit < 0 {
		return validationErr("http", "rate_limit", fmt.Sprintf("%.2f", h.RateLimit), "must not be negative")
	}

	return nil
}
