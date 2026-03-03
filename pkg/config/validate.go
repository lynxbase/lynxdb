package config

import (
	"fmt"
	"strings"
	"time"
)

// ValidationError describes a single config validation failure with context.
type ValidationError struct {
	Section string // "storage", "query", "ingest", "http", or "" for top-level
	Field   string // "compression", "log_level", etc.
	Value   string // the actual invalid value
	Message string // "must be lz4 or zstd"
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
	if err := c.Tail.validate(); err != nil {
		return err
	}
	if err := c.Server.validate(); err != nil {
		return err
	}
	if err := c.Views.validate(); err != nil {
		return err
	}
	if err := c.BufferManager.validate(); err != nil {
		return err
	}

	return c.TLS.validate()
}

func (s *StorageConfig) validate() error {
	switch s.Compression {
	case "lz4", "zstd":
		// ok
	default:
		return validationErr("storage", "compression", s.Compression, "must be lz4 or zstd")
	}

	if s.RowGroupSize < 1 {
		return validationErr("storage", "row_group_size", fmt.Sprintf("%d", s.RowGroupSize), "must be at least 1")
	}
	if s.FlushThreshold < 1*MB {
		return validationErr("storage", "flush_threshold", s.FlushThreshold.String(), "must be at least 1mb")
	}
	if s.FlushIdleTimeout < 0 {
		return validationErr("storage", "flush_idle_timeout", s.FlushIdleTimeout.String(), "must not be negative")
	}
	if s.CompactionInterval < time.Second {
		return validationErr("storage", "compaction_interval", s.CompactionInterval.String(), "must be at least 1s")
	}
	if s.CompactionWorkers < 1 {
		return validationErr("storage", "compaction_workers", fmt.Sprintf("%d", s.CompactionWorkers), "must be at least 1")
	}
	if s.CompactionRateLimitMB < 0 {
		return validationErr("storage", "compaction_rate_limit_mb", fmt.Sprintf("%d", s.CompactionRateLimitMB), "must not be negative")
	}
	if s.L0Threshold < 1 {
		return validationErr("storage", "l0_threshold", fmt.Sprintf("%d", s.L0Threshold), "must be at least 1")
	}
	if s.L1Threshold < 1 {
		return validationErr("storage", "l1_threshold", fmt.Sprintf("%d", s.L1Threshold), "must be at least 1")
	}
	if s.L2TargetSize < 1*MB {
		return validationErr("storage", "l2_target_size", s.L2TargetSize.String(), "must be at least 1mb")
	}
	if s.TieringParallelism < 1 {
		return validationErr("storage", "tiering_parallelism", fmt.Sprintf("%d", s.TieringParallelism), "must be at least 1")
	}
	if s.CacheMaxBytes < 0 {
		return validationErr("storage", "cache_max_bytes", s.CacheMaxBytes.String(), "must not be negative")
	}
	if s.CacheTTL < 0 {
		return validationErr("storage", "cache_ttl", s.CacheTTL.String(), "must not be negative")
	}
	if s.RemoteFetchTimeout < 0 {
		return validationErr("storage", "remote_fetch_timeout", s.RemoteFetchTimeout.String(), "must not be negative")
	}

	return nil
}

func (q *QueryConfig) validate() error {
	if q.SyncTimeout < time.Second {
		return validationErr("query", "sync_timeout", q.SyncTimeout.String(), "must be at least 1s")
	}
	if q.MaxQueryRuntime > 0 && q.MaxQueryRuntime < time.Second {
		return validationErr("query", "max_query_runtime", q.MaxQueryRuntime.String(), "must be at least 1s or 0 (unlimited)")
	}
	if q.JobTTL < time.Second {
		return validationErr("query", "job_ttl", q.JobTTL.String(), "must be at least 1s")
	}
	if q.JobGCInterval < time.Second {
		return validationErr("query", "job_gc_interval", q.JobGCInterval.String(), "must be at least 1s")
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
	if q.BitmapSelectivityThreshold < 0 || q.BitmapSelectivityThreshold > 1.0 {
		return validationErr("query", "bitmap_selectivity_threshold",
			fmt.Sprintf("%.2f", q.BitmapSelectivityThreshold),
			"must be between 0.0 and 1.0")
	}
	if q.MaxQueryMemory < 0 {
		return validationErr("query", "max_query_memory_bytes", q.MaxQueryMemory.String(), "must not be negative")
	}
	if q.GlobalQueryPoolBytes < 0 {
		return validationErr("query", "global_query_pool_bytes", q.GlobalQueryPoolBytes.String(), "must not be negative")
	}
	// Pool must fit at least one query — otherwise no query can ever run.
	if q.GlobalQueryPoolBytes > 0 && q.MaxQueryMemory > 0 && q.GlobalQueryPoolBytes < q.MaxQueryMemory {
		return validationErr("query", "global_query_pool_bytes", q.GlobalQueryPoolBytes.String(),
			fmt.Sprintf("must be >= max_query_memory_bytes (%s); pool must fit at least one query", q.MaxQueryMemory.String()))
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

func (s *ServerConfig) validate() error {
	if s.TotalMemoryPoolBytes < 0 {
		return validationErr("server", "total_memory_pool_bytes", s.TotalMemoryPoolBytes.String(), "must not be negative")
	}
	if s.CacheReservePercent < 0 || s.CacheReservePercent > 50 {
		return validationErr("server", "cache_reserve_percent",
			fmt.Sprintf("%d", s.CacheReservePercent), "must be between 0 and 50")
	}

	return nil
}

func (v *ViewsConfig) validate() error {
	if v.MaxBackfillMemoryBytes < 0 {
		return validationErr("views", "max_backfill_memory_bytes", v.MaxBackfillMemoryBytes.String(), "must not be negative")
	}
	if v.BackfillBackpressureWait.Duration() < 0 {
		return validationErr("views", "backfill_backpressure_wait", v.BackfillBackpressureWait.String(), "must not be negative")
	}
	if v.BackfillMaxRetries < 0 {
		return validationErr("views", "backfill_max_retries",
			fmt.Sprintf("%d", v.BackfillMaxRetries), "must not be negative")
	}

	return nil
}

func (t *TailConfig) validate() error {
	if t.MaxConcurrentSessions < 0 {
		return validationErr("tail", "max_concurrent_sessions",
			fmt.Sprintf("%d", t.MaxConcurrentSessions), "must not be negative (0 = unlimited)")
	}
	if t.MaxSessionDuration < 0 {
		return validationErr("tail", "max_session_duration",
			t.MaxSessionDuration.String(), "must not be negative (0 = unlimited)")
	}

	return nil
}

func (b *BufferManagerConfig) validate() error {
	if b.Enabled {
		if b.PageSize < 4096 || b.PageSize&(b.PageSize-1) != 0 {
			return validationErr("buffer_manager", "page_size",
				fmt.Sprintf("%d", b.PageSize), "must be a power of 2 and at least 4096")
		}
	}
	if b.CacheTargetPercent < 0 || b.CacheTargetPercent > 100 {
		return validationErr("buffer_manager", "cache_target_percent",
			fmt.Sprintf("%d", b.CacheTargetPercent), "must be between 0 and 100")
	}
	if b.QueryTargetPercent < 0 || b.QueryTargetPercent > 100 {
		return validationErr("buffer_manager", "query_target_percent",
			fmt.Sprintf("%d", b.QueryTargetPercent), "must be between 0 and 100")
	}
	if b.BatcherTargetPercent < 0 || b.BatcherTargetPercent > 100 {
		return validationErr("buffer_manager", "batcher_target_percent",
			fmt.Sprintf("%d", b.BatcherTargetPercent), "must be between 0 and 100")
	}
	total := b.CacheTargetPercent + b.QueryTargetPercent + b.BatcherTargetPercent
	if total > 100 {
		return validationErr("buffer_manager", "target_percents",
			fmt.Sprintf("cache(%d) + query(%d) + batcher(%d) = %d",
				b.CacheTargetPercent, b.QueryTargetPercent, b.BatcherTargetPercent, total),
			"sum of target percentages must not exceed 100")
	}
	if b.MaxPinnedPagesPerQuery < 0 {
		return validationErr("buffer_manager", "max_pinned_pages_per_query",
			fmt.Sprintf("%d", b.MaxPinnedPagesPerQuery), "must not be negative (0 = no limit)")
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
	if h.RateLimit < 0 {
		return validationErr("http", "rate_limit", fmt.Sprintf("%.2f", h.RateLimit), "must not be negative")
	}

	return nil
}
