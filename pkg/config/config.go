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

	Storage       StorageConfig       `yaml:"storage"        json:"storage"`
	Query         QueryConfig         `yaml:"query"          json:"query"`
	Ingest        IngestConfig        `yaml:"ingest"         json:"ingest"`
	HTTP          HTTPConfig          `yaml:"http"           json:"http"`
	Syslog        SyslogConfig        `yaml:"syslog"         json:"syslog"`
	TLS           TLSConfig           `yaml:"tls"            json:"tls"`
	Auth          AuthConfig          `yaml:"auth"           json:"auth"`
	Server        ServerConfig        `yaml:"server"         json:"server"`
	BufferManager BufferManagerConfig `yaml:"buffer_manager" json:"buffer_manager"`
	Cluster       ClusterConfig       `yaml:"cluster"        json:"cluster"`
	Profiles      map[string]Profile  `yaml:"profiles"       json:"profiles"`
}

// ClusterConfig holds distributed clustering parameters.
type ClusterConfig struct {
	// Enabled turns on cluster mode. When false (default), LynxDB runs as a single node.
	Enabled bool `yaml:"enabled" json:"enabled"`
	// NodeID is a unique identifier for this node within the cluster.
	NodeID string `yaml:"node_id" json:"node_id"`
	// Roles specifies which roles this node performs: "meta", "ingest", "query".
	// In small clusters (<10 nodes), every node typically runs all three roles.
	Roles []string `yaml:"roles" json:"roles"`
	// Seeds is the list of seed node addresses for cluster discovery (host:grpc_port).
	Seeds []string `yaml:"seeds" json:"seeds"`
	// GRPCPort is the port for inter-node gRPC communication.
	GRPCPort int `yaml:"grpc_port" json:"grpc_port"`
	// HeartbeatInterval is how often nodes send heartbeats to the meta leader.
	HeartbeatInterval Duration `yaml:"heartbeat_interval" json:"heartbeat_interval"`
	// LeaseDuration is the validity period of a shard lease.
	LeaseDuration Duration `yaml:"lease_duration" json:"lease_duration"`
	// MaxClockSkew is the maximum tolerated clock skew between cluster nodes.
	MaxClockSkew Duration `yaml:"max_clock_skew" json:"max_clock_skew"`
	// VirtualPartitionCount is the total number of virtual partitions for sharding.
	VirtualPartitionCount int `yaml:"virtual_partition_count" json:"virtual_partition_count"`
	// TimeBucketSize is the time granularity for shard time bucketing.
	TimeBucketSize Duration `yaml:"time_bucket_size" json:"time_bucket_size"`
	// AckLevel controls the durability guarantee for distributed ingest.
	// "none" = fire-and-forget, "one" = after local commit, "all" = after all ISR ACK.
	AckLevel string `yaml:"ack_level" json:"ack_level"`
	// ReplicationFactor is the number of replicas (including primary) for each shard.
	ReplicationFactor int `yaml:"replication_factor" json:"replication_factor"`
	// MetaLossTimeout is the duration after the last lease renewal before
	// entering degraded meta-loss mode (writes accepted without validation).
	MetaLossTimeout Duration `yaml:"meta_loss_timeout" json:"meta_loss_timeout"`

	// MaxConcurrentShardQueries limits the number of concurrent shard RPCs
	// during a single scatter-gather query. Default: 50.
	MaxConcurrentShardQueries int `yaml:"max_concurrent_shard_queries" json:"max_concurrent_shard_queries"`
	// ShardQueryTimeout is the per-shard query timeout. Default: 30s.
	ShardQueryTimeout Duration `yaml:"shard_query_timeout" json:"shard_query_timeout"`
	// PartialResultsEnabled controls whether queries return partial results
	// when some shards fail. Default: true.
	PartialResultsEnabled *bool `yaml:"partial_results" json:"partial_results"`
	// PartialFailureThreshold is the minimum fraction of successful shards
	// required to return results. Below this, the query fails. Default: 0.5.
	PartialFailureThreshold float64 `yaml:"partial_failure_threshold" json:"partial_failure_threshold"`
	// DCHLLThreshold is the cardinality at which dc() promotes from exact
	DCHLLThreshold int `yaml:"dc_hll_threshold" json:"dc_hll_threshold"`
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

	S3Bucket         string `yaml:"s3_bucket"           json:"s3_bucket"`
	S3Region         string `yaml:"s3_region"           json:"s3_region"`
	S3Prefix         string `yaml:"s3_prefix"           json:"s3_prefix"`
	S3Endpoint       string `yaml:"s3_endpoint"         json:"s3_endpoint"`
	S3ForcePathStyle bool   `yaml:"s3_force_path_style" json:"s3_force_path_style"`

	TieringInterval    time.Duration `yaml:"tiering_interval"    json:"tiering_interval"`
	TieringParallelism int           `yaml:"tiering_parallelism" json:"tiering_parallelism"`
	SegmentCacheSize   ByteSize      `yaml:"segment_cache_size"  json:"segment_cache_size"`
	RemoteFetchTimeout time.Duration `yaml:"remote_fetch_timeout" json:"remote_fetch_timeout"`
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

	// FSync controls whether part files are fsynced before atomic rename.
	// When true (default), data is guaranteed durable after each flush.
	// When false, the OS page cache buffers writes — lower latency for
	// small batches but data may be lost on power failure. Safe for async
	// ingest pipelines where lost data can be re-sent.
	FSync *bool `yaml:"fsync" json:"fsync"`

	// DedupEnabled enables xxhash64-based event deduplication at ingest time.
	// Catches duplicate events from at-least-once delivery, retried HTTP
	// requests, or replayed log files. Disabled by default to avoid false
	// positives on legitimately repeated log lines.
	DedupEnabled bool `yaml:"dedup_enabled" json:"dedup_enabled"`

	// DedupCapacity is the number of recent event hashes to remember across
	// batches for cross-batch dedup. Higher values catch duplicates over a
	// longer window but use more memory (~16 bytes per entry).
	// Default: 100,000.
	DedupCapacity int `yaml:"dedup_capacity" json:"dedup_capacity"`
}

type IngestLimitsConfig struct {
	MaxCompressedBodyBytes   ByteSize `yaml:"max_compressed_body_bytes" json:"max_compressed_body_bytes"`
	MaxDecompressedBodyBytes ByteSize `yaml:"max_decompressed_body_bytes" json:"max_decompressed_body_bytes"`
}

type ESCompatConfig struct {
	Enabled                 bool   `yaml:"enabled" json:"enabled"`
	AdvertisedVersion       string `yaml:"advertised_version" json:"advertised_version"`
	ClusterName             string `yaml:"cluster_name" json:"cluster_name"`
	StripLogstashDateSuffix bool   `yaml:"strip_logstash_date_suffix" json:"strip_logstash_date_suffix"`
}

type OTLPConfig struct {
	HTTPListen       string   `yaml:"http_listen" json:"http_listen"`
	GRPCListen       string   `yaml:"grpc_listen" json:"grpc_listen"`
	GRPCMaxRecvBytes ByteSize `yaml:"grpc_max_recv_bytes" json:"grpc_max_recv_bytes"`
}

type SplunkHECConfig struct {
	Enabled      bool `yaml:"enabled" json:"enabled"`
	RequireToken bool `yaml:"require_token" json:"require_token"`
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

// ServerConfig holds unified memory pool parameters.
type ServerConfig struct {
	// TotalMemoryPoolBytes is the total memory for queries + cache combined.
	// When set, it enables elastic sharing: queries can evict cache entries
	// down to a reserve floor, and cache grows to fill free space when query
	// load is low.
	// 0 means auto-detect at startup (40% of system RAM, clamped [512MB, 128GB]).
	// When set, it replaces both GlobalQueryPoolBytes and CacheMaxBytes.
	TotalMemoryPoolBytes ByteSize `yaml:"total_memory_pool_bytes" json:"total_memory_pool_bytes"`

	// CacheReservePercent is the minimum cache floor as a percentage of
	// TotalMemoryPoolBytes. Queries cannot evict cache below this floor.
	// Valid range: 0-50. Default: 20.
	CacheReservePercent int `yaml:"cache_reserve_percent" json:"cache_reserve_percent"`
}

// BufferManagerConfig configures the unified buffer manager.
// When Enabled is false (default), the system uses streaming scan with
// per-operator spill at fixed thresholds.
type BufferManagerConfig struct {
	// Enabled activates the unified buffer manager. Default: false.
	Enabled bool `yaml:"enabled" json:"enabled"`

	// MaxMemoryBytes is the total memory for the buffer pool.
	// 0 = auto (80% of total RAM, clamped to sensible bounds).
	MaxMemoryBytes ByteSize `yaml:"max_memory_bytes" json:"max_memory_bytes"`

	// PageSize is the page size in bytes. Default: 65536 (64KB).
	PageSize int `yaml:"page_size" json:"page_size"`

	// CacheTargetPercent is the advisory fraction (0-100) of the pool for segment cache.
	// Default: 60.
	CacheTargetPercent int `yaml:"cache_target_percent" json:"cache_target_percent"`

	// QueryTargetPercent is the advisory fraction (0-100) for query operators.
	// Default: 30.
	QueryTargetPercent int `yaml:"query_target_percent" json:"query_target_percent"`

	// BatcherTargetPercent is the advisory fraction (0-100) for batcher.
	// Default: 10.
	BatcherTargetPercent int `yaml:"batcher_target_percent" json:"batcher_target_percent"`

	// EnableOffHeap uses mmap for page data (recommended for production).
	// When false or on unsupported platforms, falls back to Go heap allocation.
	// Default: true.
	EnableOffHeap bool `yaml:"enable_off_heap" json:"enable_off_heap"`

	// MaxPinnedPagesPerQuery is a safety limit to prevent pin leaks.
	// 0 = no limit. Default: 1024.
	MaxPinnedPagesPerQuery int `yaml:"max_pinned_pages_per_query" json:"max_pinned_pages_per_query"`
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

			S3Region: "us-east-1",
			S3Prefix: "lynxdb/",

			TieringInterval:    5 * time.Minute,
			TieringParallelism: 3,
			SegmentCacheSize:   10 * GB,
			RemoteFetchTimeout: 30 * time.Second,
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
			MaxBodySize:   100 * MB,
			MaxBatchSize:  1000,
			MaxLineBytes:  1 << 20, // 1 MB
			DedupCapacity: 100_000,
			ESCompat: ESCompatConfig{
				Enabled:                 true,
				AdvertisedVersion:       "8.15.0",
				ClusterName:             "lynxdb",
				StripLogstashDateSuffix: true,
			},
			OTLP: OTLPConfig{
				HTTPListen:       "0.0.0.0:4318",
				GRPCListen:       "",
				GRPCMaxRecvBytes: 16 * MB,
			},
			SplunkHEC: SplunkHECConfig{
				Enabled:      true,
				RequireToken: false,
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

		Server: ServerConfig{
			CacheReservePercent: 20,
		},

		Cluster: ClusterConfig{
			Enabled:                   false,
			GRPCPort:                  9400,
			HeartbeatInterval:         Duration(5 * time.Second),
			LeaseDuration:             Duration(10 * time.Second),
			MaxClockSkew:              Duration(50 * time.Millisecond),
			VirtualPartitionCount:     1024,
			TimeBucketSize:            Duration(24 * time.Hour),
			AckLevel:                  "one",
			ReplicationFactor:         1,
			MetaLossTimeout:           Duration(30 * time.Second),
			MaxConcurrentShardQueries: 50,
			ShardQueryTimeout:         Duration(30 * time.Second),
			PartialFailureThreshold:   0.5,
			DCHLLThreshold:            10000,
		},

		BufferManager: BufferManagerConfig{
			Enabled:                false, // opt-in; streaming scan + fixed-threshold spill work without it
			PageSize:               65536, // 64KB
			CacheTargetPercent:     60,
			QueryTargetPercent:     30,
			BatcherTargetPercent:   10,
			EnableOffHeap:          true,
			MaxPinnedPagesPerQuery: 1024,
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
