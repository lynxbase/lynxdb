// Package config manages LynxDB configuration with a precedence cascade:
// CLI flags > env vars > config file > defaults.
package config

import (
	"os"
	"path/filepath"
	"time"
)

// defaultMaxConcurrent is the default ceiling on concurrent queries. It is a
// protective limit, not a target — most queries are short and I/O-bound, so a
// generous ceiling avoids spurious 429s under normal interactive/dashboard load
// while still bounding worst-case resource use. Kept a fixed value so
// DefaultConfig() stays deterministic and mirrors defaults.yaml.
const defaultMaxConcurrent = 32

// Config holds all LynxDB configuration values.
type Config struct {
	Listen    string   `yaml:"listen"    json:"listen"`
	DataDir   string   `yaml:"data_dir"  json:"data_dir"`
	Retention Duration `yaml:"retention" json:"retention"`
	LogLevel  string   `yaml:"log_level" json:"log_level"`

	Storage  StorageConfig      `yaml:"storage"  json:"storage"`
	Query    QueryConfig        `yaml:"query"    json:"query"`
	Ingest   IngestConfig       `yaml:"ingest"   json:"ingest"`
	HTTP     HTTPConfig         `yaml:"http"     json:"http"`
	Syslog   SyslogConfig       `yaml:"syslog"   json:"syslog"`
	TLS      TLSConfig          `yaml:"tls"      json:"tls"`
	Auth     AuthConfig         `yaml:"auth"     json:"auth"`
	Profiles map[string]Profile `yaml:"profiles" json:"profiles"`
}

// TLSConfig holds TLS/HTTPS parameters for the server.
type TLSConfig struct {
	// Enabled turns on TLS. When true without CertFile/KeyFile, a self-signed cert is auto-generated.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// CertFile is the path to a PEM-encoded TLS certificate. Implies Enabled=true.
	CertFile string `yaml:"cert_file" json:"cert_file,omitempty"`
	// KeyFile is the path to a PEM-encoded TLS private key. Implies Enabled=true.
	KeyFile string `yaml:"key_file" json:"key_file,omitempty"`
}

// AuthConfig holds authentication parameters for the server.
type AuthConfig struct {
	// Enabled turns on API key authentication.
	Enabled bool `yaml:"enabled" json:"enabled"`
}

// Profile holds connection parameters for a named server.
type Profile struct {
	URL   string `yaml:"url"   json:"url"`
	Token string `yaml:"token" json:"token,omitempty"`
}

// StorageConfig holds storage engine parameters.
type StorageConfig struct {
	// Partition is the span of time whose events are kept apart; retention drops whole spans.
	Partition Duration `yaml:"partition" json:"partition"`
	// FlushInterval bounds how long ingested events wait in memory; zero means the store's default.
	FlushInterval Duration `yaml:"flush_interval" json:"flush_interval"`
	// TimeFields name the fields whose value becomes the event time, tried in order.
	TimeFields []string `yaml:"time_fields" json:"time_fields"`
	// SortKey names the fields that order the rows of each part written
	// after it is set; empty keeps the order of time.
	SortKey []string `yaml:"sort_key" json:"sort_key"`
	// PartDictionary gives the parts of an index created after it is set a
	// dictionary of their own for the string columns whose values repeat
	// across their blocks.
	PartDictionary bool `yaml:"part_dictionary" json:"part_dictionary"`
}

// QueryConfig holds query execution parameters.
type QueryConfig struct {
	SyncTimeout        time.Duration `yaml:"sync_timeout"         json:"sync_timeout"`
	JobTTL             time.Duration `yaml:"job_ttl"              json:"job_ttl"`
	MaxConcurrent      int           `yaml:"max_concurrent"       json:"max_concurrent"`
	DefaultResultLimit int           `yaml:"default_result_limit" json:"default_result_limit"`
	MaxResultLimit     int           `yaml:"max_result_limit"     json:"max_result_limit"`

	// MaxQueryLength is the maximum allowed length of a query string in bytes.
	// Requests exceeding this limit are rejected with HTTP 400.
	// Default: 1048576 (1MB). Set to 0 to disable the limit.
	MaxQueryLength int `yaml:"max_query_length" json:"max_query_length"`
}

// IngestConfig holds ingestion parameters.
type IngestConfig struct {
	MaxBodySize  ByteSize           `yaml:"max_body_size"  json:"max_body_size"`
	MaxBatchSize int                `yaml:"max_batch_size" json:"max_batch_size"`
	MaxLineBytes int                `yaml:"max_line_bytes" json:"max_line_bytes"`
	ESCompat     ESCompatConfig     `yaml:"es_compat" json:"es_compat"`
	OTLP         OTLPConfig         `yaml:"otlp" json:"otlp"`
	SplunkHEC    SplunkHECConfig    `yaml:"splunk_hec" json:"splunk_hec"`
	Limits       IngestLimitsConfig `yaml:"limits" json:"limits"`

	// Mode controls how much parsing happens at ingest time.
	// "full" (default): extract all JSON fields into columns.
	// "lightweight": extract only well-known metadata fields (_time, host, source,
	// sourcetype, level, index), leaving all other data in _raw for query-time
	// extraction via rex or spath. Reduces ingest CPU by ~30-40%.
	Mode string `yaml:"mode" json:"mode"`
}

type IngestLimitsConfig struct {
	MaxCompressedBodyBytes   ByteSize `yaml:"max_compressed_body_bytes" json:"max_compressed_body_bytes"`
	MaxDecompressedBodyBytes ByteSize `yaml:"max_decompressed_body_bytes" json:"max_decompressed_body_bytes"`
}

type ESCompatConfig struct {
	Enabled           bool   `yaml:"enabled" json:"enabled"`
	AdvertisedVersion string `yaml:"advertised_version" json:"advertised_version"`
	ClusterName       string `yaml:"cluster_name" json:"cluster_name"`
}

type OTLPConfig struct {
	HTTPListen       string   `yaml:"http_listen" json:"http_listen"`
	GRPCListen       string   `yaml:"grpc_listen" json:"grpc_listen"`
	GRPCMaxRecvBytes ByteSize `yaml:"grpc_max_recv_bytes" json:"grpc_max_recv_bytes"`
}

type SplunkHECConfig struct {
	Enabled bool `yaml:"enabled" json:"enabled"`
}

// SyslogConfig configures the native syslog TCP/UDP receiver.
type SyslogConfig struct {
	UDP             string   `yaml:"udp"                 json:"udp"`
	TCP             string   `yaml:"tcp"                 json:"tcp"`
	TLS             bool     `yaml:"tls"                 json:"tls"`
	Parser          string   `yaml:"parser"              json:"parser"`
	Framing         string   `yaml:"framing"             json:"framing"`
	Trailer         string   `yaml:"trailer"             json:"trailer"`
	DefaultTimezone string   `yaml:"default_timezone"    json:"default_timezone"`
	DefaultHostname string   `yaml:"default_hostname"    json:"default_hostname"`
	Index           string   `yaml:"index"               json:"index"`
	SourceType      string   `yaml:"sourcetype"          json:"sourcetype"`
	UsePeerAsSource bool     `yaml:"use_peer_as_source"  json:"use_peer_as_source"`
	MaxMessageBytes int      `yaml:"max_message_bytes"   json:"max_message_bytes"`
	UDPReadBuffer   ByteSize `yaml:"udp_read_buffer"     json:"udp_read_buffer"`
	TCPIdleTimeout  Duration `yaml:"tcp_idle_timeout"    json:"tcp_idle_timeout"`
	TCPMaxConns     int      `yaml:"tcp_max_connections" json:"tcp_max_connections"`
	BatchSize       int      `yaml:"batch_size"          json:"batch_size"`
	BatchTimeout    Duration `yaml:"batch_timeout"       json:"batch_timeout"`
}

// Enabled reports whether any syslog listener is configured.
func (s SyslogConfig) Enabled() bool {
	return s.UDP != "" || s.TCP != ""
}

// HTTPConfig holds HTTP server parameters.
type HTTPConfig struct {
	IdleTimeout       time.Duration `yaml:"idle_timeout"              json:"idle_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"          json:"shutdown_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"       json:"read_header_timeout"`
	RateLimit         float64       `yaml:"rate_limit"                json:"rate_limit"` // requests per second; 0 = unlimited
}

// DefaultConfig returns a Config populated with all compiled defaults.
func DefaultConfig() *Config {
	return &Config{
		Listen:    "localhost:3100",
		DataDir:   defaultDataDir(),
		Retention: Duration(7 * 24 * time.Hour),
		LogLevel:  "info",

		Storage: StorageConfig{
			Partition:  Duration(24 * time.Hour),
			TimeFields: []string{"_time", "timestamp", "@timestamp", "time", "ts"},
			SortKey:    []string{},
		},

		Query: QueryConfig{
			SyncTimeout:        30 * time.Second,
			JobTTL:             5 * time.Minute,
			MaxConcurrent:      defaultMaxConcurrent,
			DefaultResultLimit: 1000,
			MaxResultLimit:     50000,
			MaxQueryLength:     1 << 20, // 1 MB
		},

		Ingest: IngestConfig{
			MaxBodySize:  100 * MB,
			MaxBatchSize: 1000,
			MaxLineBytes: 1 << 20, // 1 MB
			ESCompat: ESCompatConfig{
				Enabled:           true,
				AdvertisedVersion: "8.15.0",
				ClusterName:       "lynxdb",
			},
			OTLP: OTLPConfig{
				HTTPListen:       "localhost:4318",
				GRPCListen:       "",
				GRPCMaxRecvBytes: 16 * MB,
			},
			SplunkHEC: SplunkHECConfig{
				Enabled: true,
			},
			Limits: IngestLimitsConfig{
				MaxCompressedBodyBytes:   32 * MB,
				MaxDecompressedBodyBytes: 256 * MB,
			},
		},

		HTTP: HTTPConfig{
			IdleTimeout:       120 * time.Second,
			ShutdownTimeout:   30 * time.Second,
			ReadHeaderTimeout: 10 * time.Second,
			RateLimit:         0, // unlimited by default; opt-in per deployment
		},

		Syslog: SyslogConfig{
			Parser:          "auto",
			Framing:         "auto",
			Trailer:         "auto",
			DefaultTimezone: "Local",
			Index:           "main",
			SourceType:      "syslog",
			UsePeerAsSource: true,
			MaxMessageBytes: 65536,
			UDPReadBuffer:   2 * MB,
			TCPIdleTimeout:  Duration(5 * time.Minute),
			TCPMaxConns:     1000,
			BatchSize:       1000,
			BatchTimeout:    Duration(200 * time.Millisecond),
		},
	}
}

func defaultDataDir() string {
	// XDG-compliant default.
	if dataHome := os.Getenv("XDG_DATA_HOME"); dataHome != "" {
		return filepath.Join(dataHome, "lynxdb")
	}
	home, _ := os.UserHomeDir()
	if home != "" {
		return filepath.Join(home, ".local", "share", "lynxdb")
	}

	return ".lynxdb/data"
}
