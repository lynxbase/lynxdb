// Package config manages LynxDB configuration with a precedence cascade:
// CLI flags > env vars > config file > defaults.
package config

import (
	"os"
	"path/filepath"
	"time"
)

// Config holds all LynxDB configuration values.
type Config struct {
	Listen    string   `yaml:"listen"    json:"listen"`
	DataDir   string   `yaml:"data_dir"  json:"data_dir"`
	Retention Duration `yaml:"retention" json:"retention"`
	LogLevel  string   `yaml:"log_level" json:"log_level"`
	NoUI      bool     `yaml:"no_ui"     json:"no_ui"`

	Storage       StorageConfig       `yaml:"storage"        json:"storage"`
	Query         QueryConfig         `yaml:"query"          json:"query"`
	Ingest        IngestConfig        `yaml:"ingest"         json:"ingest"`
	HTTP          HTTPConfig          `yaml:"http"           json:"http"`
	Tail          TailConfig          `yaml:"tail"           json:"tail"`
	TLS           TLSConfig           `yaml:"tls"            json:"tls"`
	Auth          AuthConfig          `yaml:"auth"           json:"auth"`
	Views         ViewsConfig         `yaml:"views"          json:"views"`
	Server        ServerConfig        `yaml:"server"         json:"server"`
	BufferManager BufferManagerConfig `yaml:"buffer_manager" json:"buffer_manager"`
	Cluster       ClusterConfig      `yaml:"cluster"        json:"cluster"`
	Profiles      map[string]Profile `yaml:"profiles"       json:"profiles"`
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
	// set tracking to HyperLogLog approximation. Default: 10000.
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
	Compression      string        `yaml:"compression"          json:"compression"`
	RowGroupSize     int           `yaml:"row_group_size"       json:"row_group_size"`
	FlushThreshold   ByteSize      `yaml:"flush_threshold"      json:"flush_threshold"`
	FlushIdleTimeout time.Duration `yaml:"flush_idle_timeout"   json:"flush_idle_timeout"`

	// MaxColumnsPerPart limits the number of user-defined columns extracted
	// per segment. When the number of unique fields across events in a batch
	// exceeds this limit, only the top-N most frequent fields are stored as
	// columns; the rest remain searchable via _raw full-text search.
	// This prevents column explosion from high-cardinality JSON keys.
	// Default: 256. Set to 0 to disable the cap (not recommended).
	MaxColumnsPerPart int `yaml:"max_columns_per_part" json:"max_columns_per_part"`

	// PartitionBy controls the time-based partitioning of data on disk.
	// Valid values: "daily" (default), "hourly", "weekly", "monthly", "none".
	// Finer granularity enables faster retention (rm -rf per partition) and
	// more targeted time-range queries. Coarser reduces directory count.
	PartitionBy string `yaml:"partition_by" json:"partition_by"`

	CompactionInterval    time.Duration `yaml:"compaction_interval"      json:"compaction_interval"`
	CompactionWorkers     int           `yaml:"compaction_workers"       json:"compaction_workers"`
	CompactionRateLimitMB int           `yaml:"compaction_rate_limit_mb" json:"compaction_rate_limit_mb"`
	L0Threshold           int           `yaml:"l0_threshold"             json:"l0_threshold"`
	L1Threshold           int           `yaml:"l1_threshold"             json:"l1_threshold"`
	L2TargetSize          ByteSize      `yaml:"l2_target_size"           json:"l2_target_size"`

	S3Bucket         string `yaml:"s3_bucket"           json:"s3_bucket"`
	S3Region         string `yaml:"s3_region"           json:"s3_region"`
	S3Prefix         string `yaml:"s3_prefix"           json:"s3_prefix"`
	S3Endpoint       string `yaml:"s3_endpoint"         json:"s3_endpoint"`
	S3ForcePathStyle bool   `yaml:"s3_force_path_style" json:"s3_force_path_style"`

	TieringInterval    time.Duration `yaml:"tiering_interval"    json:"tiering_interval"`
	TieringParallelism int           `yaml:"tiering_parallelism" json:"tiering_parallelism"`
	SegmentCacheSize   ByteSize      `yaml:"segment_cache_size"  json:"segment_cache_size"`
	RemoteFetchTimeout time.Duration `yaml:"remote_fetch_timeout" json:"remote_fetch_timeout"`

	CacheMaxBytes ByteSize      `yaml:"cache_max_bytes" json:"cache_max_bytes"`
	CacheTTL      time.Duration `yaml:"cache_ttl"       json:"cache_ttl"`
}

// QueryConfig holds query execution parameters.
type QueryConfig struct {
	SyncTimeout        time.Duration `yaml:"sync_timeout"           json:"sync_timeout"`
	MaxQueryRuntime    time.Duration `yaml:"max_query_runtime"      json:"max_query_runtime"`
	JobTTL             time.Duration `yaml:"job_ttl"                json:"job_ttl"`
	JobGCInterval      time.Duration `yaml:"job_gc_interval"        json:"job_gc_interval"`
	MaxConcurrent      int           `yaml:"max_concurrent"         json:"max_concurrent"`
	DefaultResultLimit int           `yaml:"default_result_limit"   json:"default_result_limit"`
	MaxResultLimit     int           `yaml:"max_result_limit"       json:"max_result_limit"`
	MaxQueryMemory     ByteSize      `yaml:"max_query_memory_bytes" json:"max_query_memory_bytes"`

	// BitmapSelectivityThreshold is the maximum fraction of rows a bitmap may
	// cover before the engine falls back to sequential scan with projection.
	// When the inverted index bitmap covers more than this fraction of rows,
	// the bitmap is discarded to avoid per-row roaring bitmap lookup overhead.
	// Default: 0.9 (90%). Set to 1.0 to always use bitmap path.
	BitmapSelectivityThreshold float64 `yaml:"bitmap_selectivity_threshold" json:"bitmap_selectivity_threshold"`

	// GlobalQueryPoolBytes limits the total memory available for all concurrent
	// queries combined. 0 means auto-detect at startup (25% of system RAM).
	// This prevents pathological concurrent queries from OOMing the process.
	GlobalQueryPoolBytes ByteSize `yaml:"global_query_pool_bytes" json:"global_query_pool_bytes"`

	// SpillDir is the directory for temporary spill files created during sort
	// and aggregate operations that exceed the memory budget. Empty means use
	// the system temp directory (os.TempDir()).
	SpillDir string `yaml:"spill_dir" json:"spill_dir"`

	// MaxTempDirSizeBytes limits the total disk space used by spill files across
	// all concurrent queries. When the limit is reached, new spill file creation
	// fails with ErrTempSpaceFull. 0 means unlimited (no quota enforcement).
	// Default: 10GB.
	MaxTempDirSizeBytes ByteSize `yaml:"max_temp_dir_size_bytes" json:"max_temp_dir_size_bytes"`

	// DedupExact forces the dedup operator to use exact string matching
	// instead of xxhash64. Use this when dedup correctness is critical
	// for very high cardinality datasets (>1M unique values), where the
	// xxhash64 collision probability becomes non-negligible.
	// Default: false (use hash-based dedup for ~7x lower memory usage).
	DedupExact bool `yaml:"dedup_exact" json:"dedup_exact"`

	// SlowQueryThresholdMs is the duration in milliseconds above which a
	// query is logged at WARN level with full execution statistics.
	// Set to 0 to disable slow query logging. Default: 1000 (1 second).
	// Hot-reloadable: takes effect without server restart.
	SlowQueryThresholdMs int64 `yaml:"slow_query_threshold_ms" json:"slow_query_threshold_ms"`

	// MaxBranchParallelism limits the number of concurrent goroutines for
	// branch-level parallelism in APPEND, MULTISEARCH, and multi-source FROM.
	// 0 means auto (GOMAXPROCS). This is a soft limit on goroutines, not I/O.
	MaxBranchParallelism int `yaml:"max_branch_parallelism" json:"max_branch_parallelism"`

	// MaxQueryLength is the maximum allowed length of a query string in bytes.
	// Requests exceeding this limit are rejected with HTTP 400.
	// Default: 1048576 (1MB). Set to 0 to disable the limit.
	MaxQueryLength int `yaml:"max_query_length" json:"max_query_length"`
}

// IngestConfig holds ingestion parameters.
type IngestConfig struct {
	MaxBodySize  ByteSize `yaml:"max_body_size"  json:"max_body_size"`
	MaxBatchSize int      `yaml:"max_batch_size" json:"max_batch_size"`

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

// HTTPConfig holds HTTP server parameters.
type HTTPConfig struct {
	IdleTimeout       time.Duration `yaml:"idle_timeout"          json:"idle_timeout"`
	ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"      json:"shutdown_timeout"`
	ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"   json:"read_header_timeout"`
	RateLimit         float64       `yaml:"rate_limit"            json:"rate_limit"` // requests per second; 0 = unlimited
}

// TailConfig holds live tail session parameters.
type TailConfig struct {
	MaxConcurrentSessions int           `yaml:"max_concurrent_sessions" json:"max_concurrent_sessions"` // 0 = unlimited
	MaxSessionDuration    time.Duration `yaml:"max_session_duration"    json:"max_session_duration"`    // 0 = unlimited
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

// ViewsConfig holds materialized view parameters.
type ViewsConfig struct {
	// MaxBackfillMemoryBytes is the dedicated memory budget for a single MV backfill.
	// Default: min(10% of global pool, 2 * max_query_memory_bytes).
	// 0 means auto-compute from the global pool.
	MaxBackfillMemoryBytes ByteSize `yaml:"max_backfill_memory_bytes" json:"max_backfill_memory_bytes"`

	// BackfillBackpressureWait is how long a backfill waits when the global pool
	// is under pressure from interactive queries before retrying.
	// Default: 5s.
	BackfillBackpressureWait Duration `yaml:"backfill_backpressure_wait" json:"backfill_backpressure_wait"`

	// BackfillMaxRetries is the maximum number of backpressure retries before
	// failing the backfill. Default: 60 (5 minutes at 5s intervals).
	BackfillMaxRetries int `yaml:"backfill_max_retries" json:"backfill_max_retries"`

	// DispatchBatchSize is the maximum number of events buffered per view
	// before flushing to the events slice. Default: 1000.
	DispatchBatchSize int `yaml:"dispatch_batch_size" json:"dispatch_batch_size"`

	// DispatchBatchDelay is the maximum time to buffer events before flushing.
	// Default: 100ms.
	DispatchBatchDelay Duration `yaml:"dispatch_batch_delay" json:"dispatch_batch_delay"`
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
			Compression:       "lz4",
			RowGroupSize:      65536,
			FlushThreshold:    512 * MB,
			FlushIdleTimeout:  30 * time.Second,
			MaxColumnsPerPart: 256,
			PartitionBy:       "daily",

			CompactionInterval:    30 * time.Second,
			CompactionWorkers:     2,
			CompactionRateLimitMB: 100,
			L0Threshold:           4,
			L1Threshold:           4,
			L2TargetSize:          1 * GB,

			S3Region: "us-east-1",
			S3Prefix: "lynxdb/",

			TieringInterval:    5 * time.Minute,
			TieringParallelism: 3,
			SegmentCacheSize:   10 * GB,
			RemoteFetchTimeout: 30 * time.Second,

			CacheMaxBytes: 1 * GB,
			CacheTTL:      5 * time.Minute,
		},

		Query: QueryConfig{
			SyncTimeout:                30 * time.Second,
			MaxQueryRuntime:            5 * time.Minute,
			JobTTL:                     5 * time.Minute,
			JobGCInterval:              30 * time.Second,
			MaxConcurrent:              10,
			DefaultResultLimit:         1000,
			MaxResultLimit:             50000,
			MaxQueryMemory:             1 << 30, // 1 GB
			MaxTempDirSizeBytes:        10 * GB, // 10 GB
			BitmapSelectivityThreshold: 0.9,
			SlowQueryThresholdMs:       1000, // 1 second
			MaxQueryLength:             1 << 20, // 1 MB
		},

		Ingest: IngestConfig{
			MaxBodySize:   100 * MB,
			MaxBatchSize:  1000,
			DedupCapacity: 100_000,
		},

		HTTP: HTTPConfig{
			IdleTimeout:       120 * time.Second,
			ShutdownTimeout:   30 * time.Second,
			ReadHeaderTimeout: 10 * time.Second,
		},

		Tail: TailConfig{
			MaxConcurrentSessions: 100,
			MaxSessionDuration:    24 * time.Hour,
		},

		Server: ServerConfig{
			CacheReservePercent: 20,
		},

		Views: ViewsConfig{
			BackfillBackpressureWait: Duration(5 * time.Second),
			BackfillMaxRetries:       60,
			DispatchBatchSize:        1000,
			DispatchBatchDelay:       Duration(100 * time.Millisecond),
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
