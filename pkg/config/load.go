package config

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gopkg.in/yaml.v3"
)

//go:embed defaults.yaml
var DefaultsTemplate []byte

// Override describes a single env var or config file override that was applied.
type Override struct {
	Key    string // e.g. "listen", "storage.compression"
	Source string // "LYNXDB_LISTEN" or "config file"
	Value  string
}

// Load resolves the config path, reads the file, applies env vars, and returns
// the config along with the path that was used (empty if no file found).
// If configPath is non-empty, it is used directly.
func Load(configPath string) (*Config, string, error) {
	cfg, path, _, _, err := LoadWithOverrides(configPath)

	return cfg, path, err
}

// LoadWithOverrides is like Load but also returns the list of overrides applied
// and any warnings (e.g. unknown config keys).
func LoadWithOverrides(configPath string) (*Config, string, []Override, []string, error) {
	cfg := DefaultConfig()
	var overrides []Override
	var warnings []string

	path, explicit := resolveConfigPath(configPath)
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			if explicit {
				return nil, "", nil, nil, fmt.Errorf("read config %s: %w", path, err)
			}
			path = ""
		} else {
			if err := yaml.Unmarshal(data, cfg); err != nil {
				return nil, path, nil, nil, fmt.Errorf("parse config %s: %w", path, err)
			}
			warnings = detectUnknownKeys(data, path)
		}
	}

	envOverrides, envWarnings := applyEnvOverrides(cfg)
	overrides = append(overrides, envOverrides...)
	warnings = append(warnings, envWarnings...)

	return cfg, path, overrides, warnings, nil
}

// resolveConfigPath finds the config file using the search hierarchy.
// Returns the path and whether it's an explicit path (flag or env var).
func resolveConfigPath(explicit string) (string, bool) {
	if explicit != "" {
		return explicit, true
	}

	if p := os.Getenv("LYNXDB_CONFIG"); p != "" {
		return p, true
	}

	candidates := []string{
		"lynxdb.yaml",
	}

	home, _ := os.UserHomeDir()
	if home != "" {
		if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
			candidates = append(candidates, filepath.Join(xdg, "lynxdb", "config.yaml"))
		} else {
			candidates = append(candidates, filepath.Join(home, ".config", "lynxdb", "config.yaml"))
		}
		candidates = append(candidates, filepath.Join(home, ".lynxdb", "config.yaml"))
	}

	candidates = append(candidates, "/etc/lynxdb/config.yaml")

	for _, c := range candidates {
		if _, err := os.Stat(c); err == nil {
			return c, false
		}
	}

	return "", false
}

// envBinding maps an environment variable to a Config setter.
type envBinding struct {
	envVar   string
	key      string // config key, e.g. "listen", "storage.compression"
	apply    func(c *Config, v string) error
	valueStr func(c *Config) string // extract current value after apply
}

var envBindings = []envBinding{
	{"LYNXDB_LISTEN", "listen",
		func(c *Config, v string) error {
			c.Listen = v

			return nil
		},
		func(c *Config) string { return c.Listen }},
	{"LYNXDB_DATA_DIR", "data_dir",
		func(c *Config, v string) error {
			c.DataDir = v

			return nil
		},
		func(c *Config) string { return c.DataDir }},
	{"LYNXDB_RETENTION", "retention",
		func(c *Config, v string) error {
			d, err := ParseDuration(v)
			if err != nil {
				return err
			}
			c.Retention = d

			return nil
		},
		func(c *Config) string { return c.Retention.String() }},
	{"LYNXDB_LOG_LEVEL", "log_level",
		func(c *Config, v string) error {
			c.LogLevel = v

			return nil
		},
		func(c *Config) string { return c.LogLevel }},

	// Storage
	{"LYNXDB_STORAGE_COMPRESSION", "storage.compression",
		func(c *Config, v string) error {
			c.Storage.Compression = v

			return nil
		},
		func(c *Config) string { return c.Storage.Compression }},
	{"LYNXDB_STORAGE_ROW_GROUP_SIZE", "storage.row_group_size",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Storage.RowGroupSize = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Storage.RowGroupSize) }},
	{"LYNXDB_STORAGE_FLUSH_THRESHOLD", "storage.flush_threshold",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Storage.FlushThreshold = b

			return nil
		},
		func(c *Config) string { return c.Storage.FlushThreshold.String() }},
	{"LYNXDB_STORAGE_FLUSH_IDLE_TIMEOUT", "storage.flush_idle_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Storage.FlushIdleTimeout = d

			return nil
		},
		func(c *Config) string { return c.Storage.FlushIdleTimeout.String() }},
	{"LYNXDB_STORAGE_COMPACTION_INTERVAL", "storage.compaction_interval",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Storage.CompactionInterval = d

			return nil
		},
		func(c *Config) string { return c.Storage.CompactionInterval.String() }},
	{"LYNXDB_STORAGE_COMPACTION_WORKERS", "storage.compaction_workers",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Storage.CompactionWorkers = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Storage.CompactionWorkers) }},
	{"LYNXDB_STORAGE_COMPACTION_RATE_LIMIT_MB", "storage.compaction_rate_limit_mb",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Storage.CompactionRateLimitMB = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Storage.CompactionRateLimitMB) }},
	{"LYNXDB_STORAGE_L0_THRESHOLD", "storage.l0_threshold",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Storage.L0Threshold = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Storage.L0Threshold) }},
	{"LYNXDB_STORAGE_L1_THRESHOLD", "storage.l1_threshold",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Storage.L1Threshold = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Storage.L1Threshold) }},
	{"LYNXDB_STORAGE_L2_TARGET_SIZE", "storage.l2_target_size",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Storage.L2TargetSize = b

			return nil
		},
		func(c *Config) string { return c.Storage.L2TargetSize.String() }},
	{"LYNXDB_STORAGE_S3_BUCKET", "storage.s3_bucket",
		func(c *Config, v string) error {
			c.Storage.S3Bucket = v

			return nil
		},
		func(c *Config) string { return c.Storage.S3Bucket }},
	{"LYNXDB_STORAGE_S3_REGION", "storage.s3_region",
		func(c *Config, v string) error {
			c.Storage.S3Region = v

			return nil
		},
		func(c *Config) string { return c.Storage.S3Region }},
	{"LYNXDB_STORAGE_S3_PREFIX", "storage.s3_prefix",
		func(c *Config, v string) error {
			c.Storage.S3Prefix = v

			return nil
		},
		func(c *Config) string { return c.Storage.S3Prefix }},
	{"LYNXDB_STORAGE_S3_ENDPOINT", "storage.s3_endpoint",
		func(c *Config, v string) error {
			c.Storage.S3Endpoint = v

			return nil
		},
		func(c *Config) string { return c.Storage.S3Endpoint }},
	{"LYNXDB_STORAGE_S3_FORCE_PATH_STYLE", "storage.s3_force_path_style",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.Storage.S3ForcePathStyle = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.Storage.S3ForcePathStyle) }},
	{"LYNXDB_STORAGE_TIERING_INTERVAL", "storage.tiering_interval",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Storage.TieringInterval = d

			return nil
		},
		func(c *Config) string { return c.Storage.TieringInterval.String() }},
	{"LYNXDB_STORAGE_TIERING_PARALLELISM", "storage.tiering_parallelism",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Storage.TieringParallelism = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Storage.TieringParallelism) }},
	{"LYNXDB_STORAGE_SEGMENT_CACHE_SIZE", "storage.segment_cache_size",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Storage.SegmentCacheSize = b

			return nil
		},
		func(c *Config) string { return c.Storage.SegmentCacheSize.String() }},
	{"LYNXDB_STORAGE_CACHE_MAX_BYTES", "storage.cache_max_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Storage.CacheMaxBytes = b

			return nil
		},
		func(c *Config) string { return c.Storage.CacheMaxBytes.String() }},
	{"LYNXDB_STORAGE_CACHE_TTL", "storage.cache_ttl",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Storage.CacheTTL = d

			return nil
		},
		func(c *Config) string { return c.Storage.CacheTTL.String() }},

	// Query
	{"LYNXDB_QUERY_SYNC_TIMEOUT", "query.sync_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Query.SyncTimeout = d

			return nil
		},
		func(c *Config) string { return c.Query.SyncTimeout.String() }},
	{"LYNXDB_QUERY_JOB_TTL", "query.job_ttl",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Query.JobTTL = d

			return nil
		},
		func(c *Config) string { return c.Query.JobTTL.String() }},
	{"LYNXDB_QUERY_JOB_GC_INTERVAL", "query.job_gc_interval",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Query.JobGCInterval = d

			return nil
		},
		func(c *Config) string { return c.Query.JobGCInterval.String() }},
	{"LYNXDB_QUERY_MAX_CONCURRENT", "query.max_concurrent",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Query.MaxConcurrent = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Query.MaxConcurrent) }},
	{"LYNXDB_QUERY_DEFAULT_RESULT_LIMIT", "query.default_result_limit",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Query.DefaultResultLimit = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Query.DefaultResultLimit) }},
	{"LYNXDB_QUERY_MAX_RESULT_LIMIT", "query.max_result_limit",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Query.MaxResultLimit = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Query.MaxResultLimit) }},
	{"LYNXDB_QUERY_BITMAP_SELECTIVITY_THRESHOLD", "query.bitmap_selectivity_threshold",
		func(c *Config, v string) error {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return err
			}
			c.Query.BitmapSelectivityThreshold = f

			return nil
		},
		func(c *Config) string {
			return fmt.Sprintf("%.2f", c.Query.BitmapSelectivityThreshold)
		}},

	// Ingest
	{"LYNXDB_INGEST_MAX_BODY_SIZE", "ingest.max_body_size",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Ingest.MaxBodySize = b

			return nil
		},
		func(c *Config) string { return c.Ingest.MaxBodySize.String() }},
	{"LYNXDB_INGEST_MAX_BATCH_SIZE", "ingest.max_batch_size",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Ingest.MaxBatchSize = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Ingest.MaxBatchSize) }},
	{"LYNXDB_INGEST_MAX_LINE_BYTES", "ingest.max_line_bytes",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Ingest.MaxLineBytes = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Ingest.MaxLineBytes) }},
	{"LYNXDB_INGEST_MODE", "ingest.mode",
		func(c *Config, v string) error {
			c.Ingest.Mode = v

			return nil
		},
		func(c *Config) string { return c.Ingest.Mode }},

	// HTTP
	{"LYNXDB_HTTP_IDLE_TIMEOUT", "http.idle_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.HTTP.IdleTimeout = d

			return nil
		},
		func(c *Config) string { return c.HTTP.IdleTimeout.String() }},
	{"LYNXDB_HTTP_SHUTDOWN_TIMEOUT", "http.shutdown_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.HTTP.ShutdownTimeout = d

			return nil
		},
		func(c *Config) string { return c.HTTP.ShutdownTimeout.String() }},
	{"LYNXDB_HTTP_ALERT_SHUTDOWN_TIMEOUT", "http.alert_shutdown_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.HTTP.AlertShutdownTimeout = d

			return nil
		},
		func(c *Config) string { return c.HTTP.AlertShutdownTimeout.String() }},
	{"LYNXDB_HTTP_RATE_LIMIT", "http.rate_limit",
		func(c *Config, v string) error {
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				return err
			}
			c.HTTP.RateLimit = f

			return nil
		},
		func(c *Config) string { return fmt.Sprintf("%.2f", c.HTTP.RateLimit) }},

	// Query — additional bindings
	{"LYNXDB_QUERY_MAX_QUERY_RUNTIME", "query.max_query_runtime",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Query.MaxQueryRuntime = d

			return nil
		},
		func(c *Config) string { return c.Query.MaxQueryRuntime.String() }},
	{"LYNXDB_QUERY_MAX_QUERY_MEMORY_BYTES", "query.max_query_memory_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Query.MaxQueryMemory = b

			return nil
		},
		func(c *Config) string { return c.Query.MaxQueryMemory.String() }},
	{"LYNXDB_QUERY_GLOBAL_QUERY_POOL_BYTES", "query.global_query_pool_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Query.GlobalQueryPoolBytes = b

			return nil
		},
		func(c *Config) string { return c.Query.GlobalQueryPoolBytes.String() }},
	{"LYNXDB_QUERY_SPILL_DIR", "query.spill_dir",
		func(c *Config, v string) error {
			c.Query.SpillDir = v

			return nil
		},
		func(c *Config) string { return c.Query.SpillDir }},
	{"LYNXDB_QUERY_MAX_TEMP_DIR_SIZE_BYTES", "query.max_temp_dir_size_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Query.MaxTempDirSizeBytes = b

			return nil
		},
		func(c *Config) string { return c.Query.MaxTempDirSizeBytes.String() }},
	{"LYNXDB_QUERY_DEDUP_EXACT", "query.dedup_exact",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.Query.DedupExact = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.Query.DedupExact) }},
	{"LYNXDB_QUERY_SLOW_QUERY_THRESHOLD_MS", "query.slow_query_threshold_ms",
		func(c *Config, v string) error {
			n, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				return err
			}
			c.Query.SlowQueryThresholdMs = n

			return nil
		},
		func(c *Config) string { return strconv.FormatInt(c.Query.SlowQueryThresholdMs, 10) }},
	{"LYNXDB_QUERY_MAX_BRANCH_PARALLELISM", "query.max_branch_parallelism",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Query.MaxBranchParallelism = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Query.MaxBranchParallelism) }},

	// Storage — additional bindings
	{"LYNXDB_STORAGE_REMOTE_FETCH_TIMEOUT", "storage.remote_fetch_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Storage.RemoteFetchTimeout = d

			return nil
		},
		func(c *Config) string { return c.Storage.RemoteFetchTimeout.String() }},

	// Tail
	{"LYNXDB_TAIL_MAX_CONCURRENT_SESSIONS", "tail.max_concurrent_sessions",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Tail.MaxConcurrentSessions = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Tail.MaxConcurrentSessions) }},
	{"LYNXDB_TAIL_MAX_SESSION_DURATION", "tail.max_session_duration",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Tail.MaxSessionDuration = d

			return nil
		},
		func(c *Config) string { return c.Tail.MaxSessionDuration.String() }},

	// TLS
	{"LYNXDB_TLS_ENABLED", "tls.enabled",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.TLS.Enabled = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.TLS.Enabled) }},
	{"LYNXDB_TLS_CERT_FILE", "tls.cert_file",
		func(c *Config, v string) error {
			c.TLS.CertFile = v

			return nil
		},
		func(c *Config) string { return c.TLS.CertFile }},
	{"LYNXDB_TLS_KEY_FILE", "tls.key_file",
		func(c *Config, v string) error {
			c.TLS.KeyFile = v

			return nil
		},
		func(c *Config) string { return c.TLS.KeyFile }},

	// Auth
	{"LYNXDB_AUTH_ENABLED", "auth.enabled",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.Auth.Enabled = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.Auth.Enabled) }},

	// Server
	{"LYNXDB_SERVER_TOTAL_MEMORY_POOL_BYTES", "server.total_memory_pool_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Server.TotalMemoryPoolBytes = b

			return nil
		},
		func(c *Config) string { return c.Server.TotalMemoryPoolBytes.String() }},
	{"LYNXDB_SERVER_CACHE_RESERVE_PERCENT", "server.cache_reserve_percent",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Server.CacheReservePercent = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Server.CacheReservePercent) }},

	// Views
	{"LYNXDB_VIEWS_MAX_BACKFILL_MEMORY_BYTES", "views.max_backfill_memory_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.Views.MaxBackfillMemoryBytes = b

			return nil
		},
		func(c *Config) string { return c.Views.MaxBackfillMemoryBytes.String() }},
	{"LYNXDB_VIEWS_BACKFILL_BACKPRESSURE_WAIT", "views.backfill_backpressure_wait",
		func(c *Config, v string) error {
			d, err := ParseDuration(v)
			if err != nil {
				return err
			}
			c.Views.BackfillBackpressureWait = d

			return nil
		},
		func(c *Config) string { return c.Views.BackfillBackpressureWait.String() }},
	{"LYNXDB_VIEWS_BACKFILL_MAX_RETRIES", "views.backfill_max_retries",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Views.BackfillMaxRetries = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Views.BackfillMaxRetries) }},

	{"LYNXDB_VIEWS_BACKFILL_TIMEOUT", "views.backfill_timeout",
		func(c *Config, v string) error {
			d, err := time.ParseDuration(v)
			if err != nil {
				return err
			}
			c.Views.BackfillTimeout = Duration(d)

			return nil
		},
		func(c *Config) string { return c.Views.BackfillTimeout.String() }},

	// Buffer Manager
	{"LYNXDB_BUFFER_MANAGER_ENABLED", "buffer_manager.enabled",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.BufferManager.Enabled = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.BufferManager.Enabled) }},
	{"LYNXDB_BUFFER_MANAGER_MAX_MEMORY_BYTES", "buffer_manager.max_memory_bytes",
		func(c *Config, v string) error {
			b, err := ParseByteSize(v)
			if err != nil {
				return err
			}
			c.BufferManager.MaxMemoryBytes = b

			return nil
		},
		func(c *Config) string { return c.BufferManager.MaxMemoryBytes.String() }},
	{"LYNXDB_BUFFER_MANAGER_PAGE_SIZE", "buffer_manager.page_size",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.BufferManager.PageSize = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.BufferManager.PageSize) }},
	{"LYNXDB_BUFFER_MANAGER_CACHE_TARGET_PERCENT", "buffer_manager.cache_target_percent",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.BufferManager.CacheTargetPercent = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.BufferManager.CacheTargetPercent) }},
	{"LYNXDB_BUFFER_MANAGER_QUERY_TARGET_PERCENT", "buffer_manager.query_target_percent",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.BufferManager.QueryTargetPercent = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.BufferManager.QueryTargetPercent) }},
	{"LYNXDB_BUFFER_MANAGER_BATCHER_TARGET_PERCENT", "buffer_manager.batcher_target_percent",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.BufferManager.BatcherTargetPercent = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.BufferManager.BatcherTargetPercent) }},
	{"LYNXDB_BUFFER_MANAGER_ENABLE_OFF_HEAP", "buffer_manager.enable_off_heap",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.BufferManager.EnableOffHeap = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.BufferManager.EnableOffHeap) }},
	{"LYNXDB_BUFFER_MANAGER_MAX_PINNED_PAGES_PER_QUERY", "buffer_manager.max_pinned_pages_per_query",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.BufferManager.MaxPinnedPagesPerQuery = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.BufferManager.MaxPinnedPagesPerQuery) }},

	// Cluster
	{"LYNXDB_CLUSTER_ENABLED", "cluster.enabled",
		func(c *Config, v string) error {
			b, err := strconv.ParseBool(v)
			if err != nil {
				return err
			}
			c.Cluster.Enabled = b

			return nil
		},
		func(c *Config) string { return strconv.FormatBool(c.Cluster.Enabled) }},
	{"LYNXDB_CLUSTER_NODE_ID", "cluster.node_id",
		func(c *Config, v string) error {
			c.Cluster.NodeID = v

			return nil
		},
		func(c *Config) string { return c.Cluster.NodeID }},
	{"LYNXDB_CLUSTER_ROLES", "cluster.roles",
		func(c *Config, v string) error {
			c.Cluster.Roles = strings.Split(v, ",")

			return nil
		},
		func(c *Config) string { return strings.Join(c.Cluster.Roles, ",") }},
	{"LYNXDB_CLUSTER_SEEDS", "cluster.seeds",
		func(c *Config, v string) error {
			c.Cluster.Seeds = strings.Split(v, ",")

			return nil
		},
		func(c *Config) string { return strings.Join(c.Cluster.Seeds, ",") }},
	{"LYNXDB_CLUSTER_GRPC_PORT", "cluster.grpc_port",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Cluster.GRPCPort = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Cluster.GRPCPort) }},
	{"LYNXDB_CLUSTER_VIRTUAL_PARTITION_COUNT", "cluster.virtual_partition_count",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Cluster.VirtualPartitionCount = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Cluster.VirtualPartitionCount) }},
	{"LYNXDB_CLUSTER_ACK_LEVEL", "cluster.ack_level",
		func(c *Config, v string) error {
			c.Cluster.AckLevel = v

			return nil
		},
		func(c *Config) string { return c.Cluster.AckLevel }},
	{"LYNXDB_CLUSTER_REPLICATION_FACTOR", "cluster.replication_factor",
		func(c *Config, v string) error {
			n, err := strconv.Atoi(v)
			if err != nil {
				return err
			}
			c.Cluster.ReplicationFactor = n

			return nil
		},
		func(c *Config) string { return strconv.Itoa(c.Cluster.ReplicationFactor) }},
	{"LYNXDB_CLUSTER_META_LOSS_TIMEOUT", "cluster.meta_loss_timeout",
		func(c *Config, v string) error {
			d, err := ParseDuration(v)
			if err != nil {
				return err
			}
			c.Cluster.MetaLossTimeout = d

			return nil
		},
		func(c *Config) string { return c.Cluster.MetaLossTimeout.String() }},
}

func applyEnvOverrides(cfg *Config) ([]Override, []string) {
	var overrides []Override
	var warnings []string
	for _, b := range envBindings {
		if v := os.Getenv(b.envVar); v != "" {
			if err := b.apply(cfg, v); err != nil {
				warnings = append(warnings, fmt.Sprintf("env var %s=%q: %v (using default)", b.envVar, v, err))
			} else {
				overrides = append(overrides, Override{
					Key:    b.key,
					Source: b.envVar,
					Value:  b.valueStr(cfg),
				})
			}
		}
	}

	return overrides, warnings
}

var (
	knownKeysOnce sync.Once
	knownKeysMap  map[string]bool
)

// knownKeys returns the set of valid YAML keys (dotted paths) derived from struct tags.
func knownKeys() map[string]bool {
	knownKeysOnce.Do(func() {
		knownKeysMap = make(map[string]bool)
		collectYAMLKeys(reflect.TypeOf(Config{}), "", knownKeysMap)
	})

	return knownKeysMap
}

// mapPrefixes tracks YAML keys backed by map[string]T fields.
// Sub-keys under these prefixes are dynamic and should not trigger unknown-key warnings.
var mapPrefixes []string

func collectYAMLKeys(t reflect.Type, prefix string, keys map[string]bool) {
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		tag := f.Tag.Get("yaml")
		if tag == "" || tag == "-" {
			continue
		}
		// Strip options after comma.
		if idx := strings.IndexByte(tag, ','); idx >= 0 {
			tag = tag[:idx]
		}
		key := tag
		if prefix != "" {
			key = prefix + "." + tag
		}
		if f.Type.Kind() == reflect.Map {
			// Register the key but mark it as a dynamic-children prefix.
			keys[key] = true
			mapPrefixes = append(mapPrefixes, key+".")
		} else if f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(Duration(0)) && f.Type != reflect.TypeOf(ByteSize(0)) {
			// Recurse into nested struct, but also register the section key.
			keys[key] = true
			collectYAMLKeys(f.Type, key, keys)
		} else {
			keys[key] = true
		}
	}
}

// detectUnknownKeys decodes YAML into a raw map and reports keys not present in Config struct tags.
func detectUnknownKeys(data []byte, path string) []string {
	var raw map[string]interface{}
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return nil
	}

	known := knownKeys()
	var warnings []string

	flatKeys := flattenMap(raw, "")
	for _, key := range flatKeys {
		if known[key] {
			continue
		}
		// Skip keys under map-typed parents (e.g. profiles.staging.url).
		underMap := false
		for _, mp := range mapPrefixes {
			if strings.HasPrefix(key, mp) {
				underMap = true

				break
			}
		}
		if underMap {
			continue
		}
		w := fmt.Sprintf("unknown config key %q in %s", key, path)
		if suggestion := closestKey(key, known); suggestion != "" {
			w += fmt.Sprintf("\n  Did you mean %q?", suggestion)
		}
		warnings = append(warnings, w)
	}

	return warnings
}

// flattenMap returns all leaf-level dotted keys from a nested map.
func flattenMap(m map[string]interface{}, prefix string) []string {
	var keys []string
	for k, v := range m {
		full := k
		if prefix != "" {
			full = prefix + "." + k
		}
		if sub, ok := v.(map[string]interface{}); ok {
			keys = append(keys, flattenMap(sub, full)...)
		} else {
			keys = append(keys, full)
		}
	}
	sort.Strings(keys)

	return keys
}

// closestKey finds the closest known key by edit distance (Levenshtein).
// Returns empty string if no key is close enough (distance > 3).
func closestKey(input string, known map[string]bool) string {
	best := ""
	bestDist := 4 // threshold
	for k := range known {
		d := levenshtein(input, k)
		if d < bestDist {
			bestDist = d
			best = k
		}
	}

	return best
}

func levenshtein(a, b string) int {
	la, lb := len(a), len(b)
	if la == 0 {
		return lb
	}
	if lb == 0 {
		return la
	}

	prev := make([]int, lb+1)
	curr := make([]int, lb+1)
	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= la; i++ {
		curr[0] = i
		for j := 1; j <= lb; j++ {
			cost := 1
			if a[i-1] == b[j-1] {
				cost = 0
			}
			curr[j] = min(curr[j-1]+1, min(prev[j]+1, prev[j-1]+cost))
		}
		prev, curr = curr, prev
	}

	return prev[lb]
}

// Save writes the config to the given path (or the default config path).
func Save(cfg *Config, path string) error {
	if path == "" {
		path = DefaultConfigFilePath()
	}
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}
	data, err := yaml.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return os.WriteFile(path, data, 0o600)
}

// GetValue looks up a config key (dotted path like "storage.compression")
// and returns the effective value and its source.
// The configPath is the explicit --config flag value (empty = auto-discover).
func GetValue(configPath, key string) (value, source string, err error) {
	if !knownKeys()[key] {
		suggestion := closestKey(key, knownKeys())
		if suggestion != "" {
			return "", "", fmt.Errorf("unknown config key %q (did you mean %q?)", key, suggestion)
		}

		return "", "", fmt.Errorf("unknown config key %q", key)
	}

	entries := Entries(configPath)
	for _, e := range entries {
		if e.Key == key {
			return e.Value, e.Source, nil
		}
	}

	return "", "", fmt.Errorf("config key %q not found", key)
}

// SetValueInFile reads a YAML config file, sets one key, and writes it back.
// Uses yaml.Node to preserve existing comments and formatting.
// If the file doesn't exist, a new file is created with the single key.
func SetValueInFile(path, key, value string) error {
	if !knownKeys()[key] {
		suggestion := closestKey(key, knownKeys())
		if suggestion != "" {
			return fmt.Errorf("unknown config key %q (did you mean %q?)", key, suggestion)
		}

		return fmt.Errorf("unknown config key %q", key)
	}

	// Split dotted key: "storage.compression" -> section="storage", field="compression".
	var section, field string
	if idx := strings.IndexByte(key, '.'); idx >= 0 {
		section = key[:idx]
		field = key[idx+1:]
	} else {
		field = key
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create config dir: %w", err)
	}

	// Load existing YAML as a document node tree (preserves comments).
	var doc yaml.Node
	data, err := os.ReadFile(path)
	if err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("read config: %w", err)
		}
		// File doesn't exist — start with empty document.
		doc = yaml.Node{
			Kind: yaml.DocumentNode,
			Content: []*yaml.Node{
				{Kind: yaml.MappingNode},
			},
		}
	} else {
		if err := yaml.Unmarshal(data, &doc); err != nil {
			return fmt.Errorf("parse config: %w", err)
		}
	}

	root := doc.Content[0] // root mapping node

	if section == "" {
		// Top-level key.
		setYAMLKey(root, field, value)
	} else {
		// Nested key — find or create the section mapping.
		sectionNode := findOrCreateMapping(root, section)
		setYAMLKey(sectionNode, field, value)
	}

	// Marshal back and write.
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	return os.WriteFile(path, out, 0o600)
}

// KnownKeyNames returns all valid config key names (dotted paths).
func KnownKeyNames() []string {
	known := knownKeys()
	keys := make([]string, 0, len(known))
	for k := range known {
		// Skip section-only keys (e.g. "storage", "query") — only leaf keys.
		if isLeafKey(k, known) {
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)

	return keys
}

// isLeafKey returns true if no other key starts with key+".".
func isLeafKey(key string, known map[string]bool) bool {
	prefix := key + "."
	for k := range known {
		if strings.HasPrefix(k, prefix) {
			return false
		}
	}

	return true
}

// setYAMLKey sets a scalar value on a YAML mapping node, adding the key if absent.
func setYAMLKey(mapping *yaml.Node, key, value string) {
	for i := 0; i+1 < len(mapping.Content); i += 2 {
		if mapping.Content[i].Value == key {
			mapping.Content[i+1].Value = value
			mapping.Content[i+1].Tag = "" // clear explicit tag so yaml.v3 infers type
			mapping.Content[i+1].Style = 0

			return
		}
	}

	// Key not found — append new key-value pair.
	mapping.Content = append(mapping.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		&yaml.Node{Kind: yaml.ScalarNode, Value: value},
	)
}

// findOrCreateMapping finds a sub-mapping under the given key, creating it if absent.
func findOrCreateMapping(parent *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(parent.Content); i += 2 {
		if parent.Content[i].Value == key {
			return parent.Content[i+1]
		}
	}

	// Not found — create.
	child := &yaml.Node{Kind: yaml.MappingNode}
	parent.Content = append(parent.Content,
		&yaml.Node{Kind: yaml.ScalarNode, Value: key},
		child,
	)

	return child
}

// AddProfile adds or updates a connection profile in the config file.
func AddProfile(configPath, name, profileURL, token string) error {
	path := configPath
	if path == "" {
		path = DefaultConfigFilePath()
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		// If config doesn't exist yet, start fresh.
		cfg = DefaultConfig()
	}

	if cfg.Profiles == nil {
		cfg.Profiles = make(map[string]Profile)
	}

	cfg.Profiles[name] = Profile{URL: profileURL, Token: token}

	return Save(cfg, path)
}

// RemoveProfile removes a connection profile from the config file.
func RemoveProfile(configPath, name string) error {
	path := configPath
	if path == "" {
		path = DefaultConfigFilePath()
	}

	cfg, _, err := Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	if _, ok := cfg.Profiles[name]; !ok {
		return fmt.Errorf("profile %q not found", name)
	}

	delete(cfg.Profiles, name)

	return Save(cfg, path)
}

// GetProfile returns the profile with the given name, or an error if not found.
func GetProfile(configPath, name string) (*Profile, error) {
	cfg, _, err := Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	p, ok := cfg.Profiles[name]
	if !ok {
		var names []string
		for k := range cfg.Profiles {
			names = append(names, k)
		}
		if len(names) > 0 {
			sort.Strings(names)

			return nil, fmt.Errorf("profile %q not found. Available: %s", name, strings.Join(names, ", "))
		}

		return nil, fmt.Errorf("profile %q not found. Add one with: lynxdb config add-profile %s --url <url>", name, name)
	}

	return &p, nil
}

// ListProfiles returns all configured profiles.
func ListProfiles(configPath string) (map[string]Profile, error) {
	cfg, _, err := Load(configPath)
	if err != nil {
		return nil, fmt.Errorf("load config: %w", err)
	}

	if cfg.Profiles == nil {
		return map[string]Profile{}, nil
	}

	return cfg.Profiles, nil
}

// DefaultConfigFilePath returns the default config file path using XDG conventions.
func DefaultConfigFilePath() string {
	home, _ := os.UserHomeDir()
	if xdg := os.Getenv("XDG_CONFIG_HOME"); xdg != "" {
		return filepath.Join(xdg, "lynxdb", "config.yaml")
	}
	if home != "" {
		return filepath.Join(home, ".config", "lynxdb", "config.yaml")
	}

	return "lynxdb.yaml"
}

// PIDFilePath returns the path for the server PID file.
// If dataDir is set, the PID file lives alongside the data.
// Otherwise it falls back to XDG_RUNTIME_DIR, then os.TempDir().
func PIDFilePath(dataDir string) string {
	const name = "lynxdb.pid"
	if dataDir != "" {
		return filepath.Join(dataDir, name)
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		return filepath.Join(xdg, name)
	}

	return filepath.Join(os.TempDir(), name)
}
