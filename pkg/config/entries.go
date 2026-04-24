package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// formatBoolPtr formats a *bool as "true", "false", or "" (unset).
func formatBoolPtr(b *bool) string {
	if b == nil {
		return ""
	}

	return strconv.FormatBool(*b)
}

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
		if os.Getenv(envVar) != "" {
			source = "env var"
		} else if hasFile && fileValue != "" && fileValue != dfltValue {
			source = "config file"
		}
		entries = append(entries, Entry{Key: key, Value: value, Source: source})
	}

	// Top-level.
	add("listen", cfg.Listen, dflt.Listen, fileCfg.Listen, "LYNXDB_LISTEN")
	add("data_dir", cfg.DataDir, dflt.DataDir, fileCfg.DataDir, "LYNXDB_DATA_DIR")
	add("retention", cfg.Retention.String(), dflt.Retention.String(), fileCfg.Retention.String(), "LYNXDB_RETENTION")
	add("log_level", cfg.LogLevel, dflt.LogLevel, fileCfg.LogLevel, "LYNXDB_LOG_LEVEL")
	add("no_ui", strconv.FormatBool(cfg.NoUI), strconv.FormatBool(dflt.NoUI), strconv.FormatBool(fileCfg.NoUI), "LYNXDB_NO_UI")

	// Storage.
	add("storage.compression", cfg.Storage.Compression, dflt.Storage.Compression, fileCfg.Storage.Compression, "LYNXDB_STORAGE_COMPRESSION")
	add("storage.row_group_size", fmt.Sprintf("%d", cfg.Storage.RowGroupSize), fmt.Sprintf("%d", dflt.Storage.RowGroupSize), fmt.Sprintf("%d", fileCfg.Storage.RowGroupSize), "LYNXDB_STORAGE_ROW_GROUP_SIZE")
	add("storage.flush_threshold", cfg.Storage.FlushThreshold.String(), dflt.Storage.FlushThreshold.String(), fileCfg.Storage.FlushThreshold.String(), "LYNXDB_STORAGE_FLUSH_THRESHOLD")
	add("storage.flush_idle_timeout", cfg.Storage.FlushIdleTimeout.String(), dflt.Storage.FlushIdleTimeout.String(), fileCfg.Storage.FlushIdleTimeout.String(), "LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT")
	add("storage.max_columns_per_part", fmt.Sprintf("%d", cfg.Storage.MaxColumnsPerPart), fmt.Sprintf("%d", dflt.Storage.MaxColumnsPerPart), fmt.Sprintf("%d", fileCfg.Storage.MaxColumnsPerPart), "LYNXDB_STORAGE_MAX_COLUMNS_PER_PART")
	add("storage.partition_by", cfg.Storage.PartitionBy, dflt.Storage.PartitionBy, fileCfg.Storage.PartitionBy, "LYNXDB_STORAGE_PARTITION_BY")
	add("storage.compaction_interval", cfg.Storage.CompactionInterval.String(), dflt.Storage.CompactionInterval.String(), fileCfg.Storage.CompactionInterval.String(), "LYNXDB_STORAGE_COMPACTION_INTERVAL")
	add("storage.compaction_workers", fmt.Sprintf("%d", cfg.Storage.CompactionWorkers), fmt.Sprintf("%d", dflt.Storage.CompactionWorkers), fmt.Sprintf("%d", fileCfg.Storage.CompactionWorkers), "LYNXDB_STORAGE_COMPACTION_WORKERS")
	add("storage.compaction_rate_limit_mb", fmt.Sprintf("%d", cfg.Storage.CompactionRateLimitMB), fmt.Sprintf("%d", dflt.Storage.CompactionRateLimitMB), fmt.Sprintf("%d", fileCfg.Storage.CompactionRateLimitMB), "LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB")
	add("storage.l0_threshold", fmt.Sprintf("%d", cfg.Storage.L0Threshold), fmt.Sprintf("%d", dflt.Storage.L0Threshold), fmt.Sprintf("%d", fileCfg.Storage.L0Threshold), "LYNXDB_STORAGE_L0_THRESHOLD")
	add("storage.l1_threshold", fmt.Sprintf("%d", cfg.Storage.L1Threshold), fmt.Sprintf("%d", dflt.Storage.L1Threshold), fmt.Sprintf("%d", fileCfg.Storage.L1Threshold), "LYNXDB_STORAGE_L1_THRESHOLD")
	add("storage.l2_target_size", cfg.Storage.L2TargetSize.String(), dflt.Storage.L2TargetSize.String(), fileCfg.Storage.L2TargetSize.String(), "LYNXDB_STORAGE_L2_TARGET_SIZE")
	add("storage.s3_bucket", cfg.Storage.S3Bucket, dflt.Storage.S3Bucket, fileCfg.Storage.S3Bucket, "LYNXDB_STORAGE_S3_BUCKET")
	add("storage.s3_region", cfg.Storage.S3Region, dflt.Storage.S3Region, fileCfg.Storage.S3Region, "LYNXDB_STORAGE_S3_REGION")
	add("storage.s3_prefix", cfg.Storage.S3Prefix, dflt.Storage.S3Prefix, fileCfg.Storage.S3Prefix, "LYNXDB_STORAGE_S3_PREFIX")
	add("storage.s3_endpoint", cfg.Storage.S3Endpoint, dflt.Storage.S3Endpoint, fileCfg.Storage.S3Endpoint, "LYNXDB_STORAGE_S3_ENDPOINT")
	add("storage.s3_force_path_style", strconv.FormatBool(cfg.Storage.S3ForcePathStyle), strconv.FormatBool(dflt.Storage.S3ForcePathStyle), strconv.FormatBool(fileCfg.Storage.S3ForcePathStyle), "LYNXDB_STORAGE_S3_FORCE_PATH_STYLE")
	add("storage.tiering_interval", cfg.Storage.TieringInterval.String(), dflt.Storage.TieringInterval.String(), fileCfg.Storage.TieringInterval.String(), "LYNXDB_STORAGE_TIERING_INTERVAL")
	add("storage.tiering_parallelism", fmt.Sprintf("%d", cfg.Storage.TieringParallelism), fmt.Sprintf("%d", dflt.Storage.TieringParallelism), fmt.Sprintf("%d", fileCfg.Storage.TieringParallelism), "LYNXDB_STORAGE_TIERING_PARALLELISM")
	add("storage.segment_cache_size", cfg.Storage.SegmentCacheSize.String(), dflt.Storage.SegmentCacheSize.String(), fileCfg.Storage.SegmentCacheSize.String(), "LYNXDB_STORAGE_SEGMENT_CACHE_SIZE")
	add("storage.cache_max_bytes", cfg.Storage.CacheMaxBytes.String(), dflt.Storage.CacheMaxBytes.String(), fileCfg.Storage.CacheMaxBytes.String(), "LYNXDB_STORAGE_CACHE_MAX_BYTES")
	add("storage.cache_ttl", cfg.Storage.CacheTTL.String(), dflt.Storage.CacheTTL.String(), fileCfg.Storage.CacheTTL.String(), "LYNXDB_STORAGE_CACHE_TTL")

	// Helper for duration fields.
	durationEntry := func(key string, val, dfltVal, fileVal time.Duration, envVar string) {
		add(key, val.String(), dfltVal.String(), fileVal.String(), envVar)
	}

	// Storage — additional fields.
	durationEntry("storage.remote_fetch_timeout", cfg.Storage.RemoteFetchTimeout, dflt.Storage.RemoteFetchTimeout, fileCfg.Storage.RemoteFetchTimeout, "LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT")

	// Query.
	durationEntry("query.sync_timeout", cfg.Query.SyncTimeout, dflt.Query.SyncTimeout, fileCfg.Query.SyncTimeout, "LYNXDB_QUERY_SYNC_TIMEOUT")
	durationEntry("query.max_query_runtime", cfg.Query.MaxQueryRuntime, dflt.Query.MaxQueryRuntime, fileCfg.Query.MaxQueryRuntime, "LYNXDB_QUERY_MAX_QUERY_RUNTIME")
	durationEntry("query.job_ttl", cfg.Query.JobTTL, dflt.Query.JobTTL, fileCfg.Query.JobTTL, "LYNXDB_QUERY_JOB_TTL")
	durationEntry("query.job_gc_interval", cfg.Query.JobGCInterval, dflt.Query.JobGCInterval, fileCfg.Query.JobGCInterval, "LYNXDB_QUERY_JOB_GC_INTERVAL")
	add("query.max_concurrent", fmt.Sprintf("%d", cfg.Query.MaxConcurrent), fmt.Sprintf("%d", dflt.Query.MaxConcurrent), fmt.Sprintf("%d", fileCfg.Query.MaxConcurrent), "LYNXDB_QUERY_MAX_CONCURRENT")
	add("query.default_result_limit", fmt.Sprintf("%d", cfg.Query.DefaultResultLimit), fmt.Sprintf("%d", dflt.Query.DefaultResultLimit), fmt.Sprintf("%d", fileCfg.Query.DefaultResultLimit), "LYNXDB_QUERY_DEFAULT_RESULT_LIMIT")
	add("query.max_result_limit", fmt.Sprintf("%d", cfg.Query.MaxResultLimit), fmt.Sprintf("%d", dflt.Query.MaxResultLimit), fmt.Sprintf("%d", fileCfg.Query.MaxResultLimit), "LYNXDB_QUERY_MAX_RESULT_LIMIT")
	add("query.max_query_memory_bytes", cfg.Query.MaxQueryMemory.String(), dflt.Query.MaxQueryMemory.String(), fileCfg.Query.MaxQueryMemory.String(), "LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES")
	add("query.bitmap_selectivity_threshold", fmt.Sprintf("%.2f", cfg.Query.BitmapSelectivityThreshold), fmt.Sprintf("%.2f", dflt.Query.BitmapSelectivityThreshold), fmt.Sprintf("%.2f", fileCfg.Query.BitmapSelectivityThreshold), "LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD")
	add("query.global_query_pool_bytes", cfg.Query.GlobalQueryPoolBytes.String(), dflt.Query.GlobalQueryPoolBytes.String(), fileCfg.Query.GlobalQueryPoolBytes.String(), "LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES")
	add("query.spill_dir", cfg.Query.SpillDir, dflt.Query.SpillDir, fileCfg.Query.SpillDir, "LYNXDB_QUERY_SPILL_DIR")
	add("query.max_temp_dir_size_bytes", cfg.Query.MaxTempDirSizeBytes.String(), dflt.Query.MaxTempDirSizeBytes.String(), fileCfg.Query.MaxTempDirSizeBytes.String(), "LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES")
	add("query.dedup_exact", strconv.FormatBool(cfg.Query.DedupExact), strconv.FormatBool(dflt.Query.DedupExact), strconv.FormatBool(fileCfg.Query.DedupExact), "LYNXDB_QUERY_DEDUP_EXACT")
	add("query.slow_query_threshold_ms", strconv.FormatInt(cfg.Query.SlowQueryThresholdMs, 10), strconv.FormatInt(dflt.Query.SlowQueryThresholdMs, 10), strconv.FormatInt(fileCfg.Query.SlowQueryThresholdMs, 10), "LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS")
	add("query.max_branch_parallelism", strconv.Itoa(cfg.Query.MaxBranchParallelism), strconv.Itoa(dflt.Query.MaxBranchParallelism), strconv.Itoa(fileCfg.Query.MaxBranchParallelism), "LYNXDB_QUERY_MAX_BRANCH_PARALLELISM")
	add("query.max_query_length", strconv.Itoa(cfg.Query.MaxQueryLength), strconv.Itoa(dflt.Query.MaxQueryLength), strconv.Itoa(fileCfg.Query.MaxQueryLength), "LYNXDB_QUERY_MAX_QUERY_LENGTH")
	add("query.preview_size", strconv.Itoa(cfg.Query.PreviewSize), strconv.Itoa(dflt.Query.PreviewSize), strconv.Itoa(fileCfg.Query.PreviewSize), "LYNXDB_QUERY_PREVIEW_SIZE")

	// Ingest.
	add("ingest.max_body_size", cfg.Ingest.MaxBodySize.String(), dflt.Ingest.MaxBodySize.String(), fileCfg.Ingest.MaxBodySize.String(), "LYNXDB_INGEST_MAX_BODY_SIZE")
	add("ingest.max_batch_size", fmt.Sprintf("%d", cfg.Ingest.MaxBatchSize), fmt.Sprintf("%d", dflt.Ingest.MaxBatchSize), fmt.Sprintf("%d", fileCfg.Ingest.MaxBatchSize), "LYNXDB_INGEST_MAX_BATCH_SIZE")
	add("ingest.max_line_bytes", fmt.Sprintf("%d", cfg.Ingest.MaxLineBytes), fmt.Sprintf("%d", dflt.Ingest.MaxLineBytes), fmt.Sprintf("%d", fileCfg.Ingest.MaxLineBytes), "LYNXDB_INGEST_MAX_LINE_BYTES")
	add("ingest.mode", cfg.Ingest.Mode, dflt.Ingest.Mode, fileCfg.Ingest.Mode, "LYNXDB_INGEST_MODE")
	add("ingest.fsync", formatBoolPtr(cfg.Ingest.FSync), formatBoolPtr(dflt.Ingest.FSync), formatBoolPtr(fileCfg.Ingest.FSync), "LYNXDB_INGEST_FSYNC")
	add("ingest.dedup_enabled", strconv.FormatBool(cfg.Ingest.DedupEnabled), strconv.FormatBool(dflt.Ingest.DedupEnabled), strconv.FormatBool(fileCfg.Ingest.DedupEnabled), "LYNXDB_INGEST_DEDUP_ENABLED")
	add("ingest.dedup_capacity", strconv.Itoa(cfg.Ingest.DedupCapacity), strconv.Itoa(dflt.Ingest.DedupCapacity), strconv.Itoa(fileCfg.Ingest.DedupCapacity), "LYNXDB_INGEST_DEDUP_CAPACITY")

	// HTTP.
	durationEntry("http.idle_timeout", cfg.HTTP.IdleTimeout, dflt.HTTP.IdleTimeout, fileCfg.HTTP.IdleTimeout, "LYNXDB_HTTP_IDLE_TIMEOUT")
	durationEntry("http.shutdown_timeout", cfg.HTTP.ShutdownTimeout, dflt.HTTP.ShutdownTimeout, fileCfg.HTTP.ShutdownTimeout, "LYNXDB_HTTP_SHUTDOWN_TIMEOUT")
	durationEntry("http.read_header_timeout", cfg.HTTP.ReadHeaderTimeout, dflt.HTTP.ReadHeaderTimeout, fileCfg.HTTP.ReadHeaderTimeout, "LYNXDB_HTTP_READ_HEADER_TIMEOUT")
	add("http.rate_limit", fmt.Sprintf("%.2f", cfg.HTTP.RateLimit), fmt.Sprintf("%.2f", dflt.HTTP.RateLimit), fmt.Sprintf("%.2f", fileCfg.HTTP.RateLimit), "LYNXDB_HTTP_RATE_LIMIT")

	// Tail.
	add("tail.max_concurrent_sessions", strconv.Itoa(cfg.Tail.MaxConcurrentSessions), strconv.Itoa(dflt.Tail.MaxConcurrentSessions), strconv.Itoa(fileCfg.Tail.MaxConcurrentSessions), "LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS")
	durationEntry("tail.max_session_duration", cfg.Tail.MaxSessionDuration, dflt.Tail.MaxSessionDuration, fileCfg.Tail.MaxSessionDuration, "LYNXDB_TAIL_MAX_SESSION_DURATION")

	// TLS.
	add("tls.enabled", strconv.FormatBool(cfg.TLS.Enabled), strconv.FormatBool(dflt.TLS.Enabled), strconv.FormatBool(fileCfg.TLS.Enabled), "LYNXDB_TLS_ENABLED")
	add("tls.cert_file", cfg.TLS.CertFile, dflt.TLS.CertFile, fileCfg.TLS.CertFile, "LYNXDB_TLS_CERT_FILE")
	add("tls.key_file", cfg.TLS.KeyFile, dflt.TLS.KeyFile, fileCfg.TLS.KeyFile, "LYNXDB_TLS_KEY_FILE")

	// Auth.
	add("auth.enabled", strconv.FormatBool(cfg.Auth.Enabled), strconv.FormatBool(dflt.Auth.Enabled), strconv.FormatBool(fileCfg.Auth.Enabled), "LYNXDB_AUTH_ENABLED")

	// Server.
	add("server.total_memory_pool_bytes", cfg.Server.TotalMemoryPoolBytes.String(), dflt.Server.TotalMemoryPoolBytes.String(), fileCfg.Server.TotalMemoryPoolBytes.String(), "LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES")
	add("server.cache_reserve_percent", strconv.Itoa(cfg.Server.CacheReservePercent), strconv.Itoa(dflt.Server.CacheReservePercent), strconv.Itoa(fileCfg.Server.CacheReservePercent), "LYNXDB_SERVER_CACHE_RESERVE_PERCENT")

	// Views.
	add("views.max_backfill_memory_bytes", cfg.Views.MaxBackfillMemoryBytes.String(), dflt.Views.MaxBackfillMemoryBytes.String(), fileCfg.Views.MaxBackfillMemoryBytes.String(), "LYNXDB_VIEWS_MAX_BACKFILL_MEMORY_BYTES")
	add("views.backfill_backpressure_wait", cfg.Views.BackfillBackpressureWait.String(), dflt.Views.BackfillBackpressureWait.String(), fileCfg.Views.BackfillBackpressureWait.String(), "LYNXDB_VIEWS_BACKFILL_BACKPRESSURE_WAIT")
	add("views.backfill_max_retries", strconv.Itoa(cfg.Views.BackfillMaxRetries), strconv.Itoa(dflt.Views.BackfillMaxRetries), strconv.Itoa(fileCfg.Views.BackfillMaxRetries), "LYNXDB_VIEWS_BACKFILL_MAX_RETRIES")
	add("views.dispatch_batch_size", strconv.Itoa(cfg.Views.DispatchBatchSize), strconv.Itoa(dflt.Views.DispatchBatchSize), strconv.Itoa(fileCfg.Views.DispatchBatchSize), "LYNXDB_VIEWS_DISPATCH_BATCH_SIZE")
	add("views.dispatch_batch_delay", cfg.Views.DispatchBatchDelay.String(), dflt.Views.DispatchBatchDelay.String(), fileCfg.Views.DispatchBatchDelay.String(), "LYNXDB_VIEWS_DISPATCH_BATCH_DELAY")
	add("views.backfill_timeout", cfg.Views.BackfillTimeout.String(), dflt.Views.BackfillTimeout.String(), fileCfg.Views.BackfillTimeout.String(), "LYNXDB_VIEWS_BACKFILL_TIMEOUT")

	// Buffer Manager.
	add("buffer_manager.enabled", strconv.FormatBool(cfg.BufferManager.Enabled), strconv.FormatBool(dflt.BufferManager.Enabled), strconv.FormatBool(fileCfg.BufferManager.Enabled), "LYNXDB_BUFFER_MANAGER_ENABLED")
	add("buffer_manager.max_memory_bytes", cfg.BufferManager.MaxMemoryBytes.String(), dflt.BufferManager.MaxMemoryBytes.String(), fileCfg.BufferManager.MaxMemoryBytes.String(), "LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES")
	add("buffer_manager.page_size", strconv.Itoa(cfg.BufferManager.PageSize), strconv.Itoa(dflt.BufferManager.PageSize), strconv.Itoa(fileCfg.BufferManager.PageSize), "LYNXDB_BUFFER_MANAGER_PAGE_SIZE")
	add("buffer_manager.cache_target_percent", strconv.Itoa(cfg.BufferManager.CacheTargetPercent), strconv.Itoa(dflt.BufferManager.CacheTargetPercent), strconv.Itoa(fileCfg.BufferManager.CacheTargetPercent), "LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT")
	add("buffer_manager.query_target_percent", strconv.Itoa(cfg.BufferManager.QueryTargetPercent), strconv.Itoa(dflt.BufferManager.QueryTargetPercent), strconv.Itoa(fileCfg.BufferManager.QueryTargetPercent), "LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT")
	add("buffer_manager.batcher_target_percent", strconv.Itoa(cfg.BufferManager.BatcherTargetPercent), strconv.Itoa(dflt.BufferManager.BatcherTargetPercent), strconv.Itoa(fileCfg.BufferManager.BatcherTargetPercent), "LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT")
	add("buffer_manager.enable_off_heap", strconv.FormatBool(cfg.BufferManager.EnableOffHeap), strconv.FormatBool(dflt.BufferManager.EnableOffHeap), strconv.FormatBool(fileCfg.BufferManager.EnableOffHeap), "LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP")
	add("buffer_manager.max_pinned_pages_per_query", strconv.Itoa(cfg.BufferManager.MaxPinnedPagesPerQuery), strconv.Itoa(dflt.BufferManager.MaxPinnedPagesPerQuery), strconv.Itoa(fileCfg.BufferManager.MaxPinnedPagesPerQuery), "LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY")

	// Cluster.
	add("cluster.enabled", strconv.FormatBool(cfg.Cluster.Enabled), strconv.FormatBool(dflt.Cluster.Enabled), strconv.FormatBool(fileCfg.Cluster.Enabled), "LYNXDB_CLUSTER_ENABLED")
	add("cluster.node_id", cfg.Cluster.NodeID, dflt.Cluster.NodeID, fileCfg.Cluster.NodeID, "LYNXDB_CLUSTER_NODE_ID")
	add("cluster.roles", strings.Join(cfg.Cluster.Roles, ","), strings.Join(dflt.Cluster.Roles, ","), strings.Join(fileCfg.Cluster.Roles, ","), "LYNXDB_CLUSTER_ROLES")
	add("cluster.seeds", strings.Join(cfg.Cluster.Seeds, ","), strings.Join(dflt.Cluster.Seeds, ","), strings.Join(fileCfg.Cluster.Seeds, ","), "LYNXDB_CLUSTER_SEEDS")
	add("cluster.grpc_port", strconv.Itoa(cfg.Cluster.GRPCPort), strconv.Itoa(dflt.Cluster.GRPCPort), strconv.Itoa(fileCfg.Cluster.GRPCPort), "LYNXDB_CLUSTER_GRPC_PORT")
	add("cluster.heartbeat_interval", cfg.Cluster.HeartbeatInterval.String(), dflt.Cluster.HeartbeatInterval.String(), fileCfg.Cluster.HeartbeatInterval.String(), "LYNXDB_CLUSTER_HEARTBEAT_INTERVAL")
	add("cluster.lease_duration", cfg.Cluster.LeaseDuration.String(), dflt.Cluster.LeaseDuration.String(), fileCfg.Cluster.LeaseDuration.String(), "LYNXDB_CLUSTER_LEASE_DURATION")
	add("cluster.max_clock_skew", cfg.Cluster.MaxClockSkew.String(), dflt.Cluster.MaxClockSkew.String(), fileCfg.Cluster.MaxClockSkew.String(), "LYNXDB_CLUSTER_MAX_CLOCK_SKEW")
	add("cluster.virtual_partition_count", strconv.Itoa(cfg.Cluster.VirtualPartitionCount), strconv.Itoa(dflt.Cluster.VirtualPartitionCount), strconv.Itoa(fileCfg.Cluster.VirtualPartitionCount), "LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT")
	add("cluster.time_bucket_size", cfg.Cluster.TimeBucketSize.String(), dflt.Cluster.TimeBucketSize.String(), fileCfg.Cluster.TimeBucketSize.String(), "LYNXDB_CLUSTER_TIME_BUCKET_SIZE")
	add("cluster.ack_level", cfg.Cluster.AckLevel, dflt.Cluster.AckLevel, fileCfg.Cluster.AckLevel, "LYNXDB_CLUSTER_ACK_LEVEL")
	add("cluster.replication_factor", strconv.Itoa(cfg.Cluster.ReplicationFactor), strconv.Itoa(dflt.Cluster.ReplicationFactor), strconv.Itoa(fileCfg.Cluster.ReplicationFactor), "LYNXDB_CLUSTER_REPLICATION_FACTOR")
	add("cluster.meta_loss_timeout", cfg.Cluster.MetaLossTimeout.String(), dflt.Cluster.MetaLossTimeout.String(), fileCfg.Cluster.MetaLossTimeout.String(), "LYNXDB_CLUSTER_META_LOSS_TIMEOUT")
	add("cluster.max_concurrent_shard_queries", fmt.Sprintf("%d", cfg.Cluster.MaxConcurrentShardQueries), fmt.Sprintf("%d", dflt.Cluster.MaxConcurrentShardQueries), fmt.Sprintf("%d", fileCfg.Cluster.MaxConcurrentShardQueries), "LYNXDB_CLUSTER_MAX_CONCURRENT_SHARD_QUERIES")
	durationEntry("cluster.shard_query_timeout", cfg.Cluster.ShardQueryTimeout.Duration(), dflt.Cluster.ShardQueryTimeout.Duration(), fileCfg.Cluster.ShardQueryTimeout.Duration(), "LYNXDB_CLUSTER_SHARD_QUERY_TIMEOUT")
	add("cluster.partial_failure_threshold", fmt.Sprintf("%.2f", cfg.Cluster.PartialFailureThreshold), fmt.Sprintf("%.2f", dflt.Cluster.PartialFailureThreshold), fmt.Sprintf("%.2f", fileCfg.Cluster.PartialFailureThreshold), "LYNXDB_CLUSTER_PARTIAL_FAILURE_THRESHOLD")
	add("cluster.partial_results", formatBoolPtr(cfg.Cluster.PartialResultsEnabled), formatBoolPtr(dflt.Cluster.PartialResultsEnabled), formatBoolPtr(fileCfg.Cluster.PartialResultsEnabled), "LYNXDB_CLUSTER_PARTIAL_RESULTS")
	add("cluster.dc_hll_threshold", fmt.Sprintf("%d", cfg.Cluster.DCHLLThreshold), fmt.Sprintf("%d", dflt.Cluster.DCHLLThreshold), fmt.Sprintf("%d", fileCfg.Cluster.DCHLLThreshold), "LYNXDB_CLUSTER_DC_HLL_THRESHOLD")

	return entries
}
