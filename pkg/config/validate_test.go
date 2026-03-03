package config

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDefaultConfigValidates(t *testing.T) {
	cfg := DefaultConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestValidateTopLevel(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*Config)
		wantErr string
	}{
		{
			"empty listen",
			func(c *Config) { c.Listen = "" },
			"listen: must not be empty",
		},
		{
			"negative retention",
			func(c *Config) { c.Retention = Duration(-time.Hour) },
			"retention: must not be negative",
		},
		{
			"invalid log level",
			func(c *Config) { c.LogLevel = "verbose" },
			"log_level: must be debug, info, warn, or error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(cfg)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateStorage(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*StorageConfig)
		wantErr string
	}{
		{"bad compression", func(s *StorageConfig) { s.Compression = "snappy" }, "storage.compression"},
		{"zero row group", func(s *StorageConfig) { s.RowGroupSize = 0 }, "storage.row_group_size"},
		{"small flush", func(s *StorageConfig) { s.FlushThreshold = 100 }, "storage.flush_threshold"},
		{"zero compaction workers", func(s *StorageConfig) { s.CompactionWorkers = 0 }, "storage.compaction_workers"},
		{"zero l0 threshold", func(s *StorageConfig) { s.L0Threshold = 0 }, "storage.l0_threshold"},
		{"small l2 target", func(s *StorageConfig) { s.L2TargetSize = 100 }, "storage.l2_target_size"},
		// F10: RemoteFetchTimeout validation
		{"negative remote_fetch_timeout", func(s *StorageConfig) { s.RemoteFetchTimeout = -time.Second }, "storage.remote_fetch_timeout"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.Storage)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateQuery(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*QueryConfig)
		wantErr string
	}{
		{"zero sync timeout", func(q *QueryConfig) { q.SyncTimeout = 0 }, "query.sync_timeout"},
		{"zero max concurrent", func(q *QueryConfig) { q.MaxConcurrent = 0 }, "query.max_concurrent"},
		{"result limit inversion", func(q *QueryConfig) { q.MaxResultLimit = 10; q.DefaultResultLimit = 100 }, "query.max_result_limit"},
		{"negative pool bytes", func(q *QueryConfig) { q.GlobalQueryPoolBytes = -1 }, "query.global_query_pool_bytes"},
		{"pool smaller than per-query", func(q *QueryConfig) {
			q.MaxQueryMemory = 1 * GB
			q.GlobalQueryPoolBytes = 512 * MB
		}, "must be >= max_query_memory_bytes"},
		// F11: MaxQueryMemory validation
		{"negative max_query_memory", func(q *QueryConfig) { q.MaxQueryMemory = -1 }, "query.max_query_memory_bytes"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.Query)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateIngest(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*IngestConfig)
		wantErr string
	}{
		{"small body size", func(i *IngestConfig) { i.MaxBodySize = 100 }, "ingest.max_body_size"},
		{"zero batch size", func(i *IngestConfig) { i.MaxBatchSize = 0 }, "ingest.max_batch_size"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.Ingest)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateHTTP(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*HTTPConfig)
		wantErr string
	}{
		{"zero idle timeout", func(h *HTTPConfig) { h.IdleTimeout = 0 }, "http.idle_timeout"},
		{"zero shutdown timeout", func(h *HTTPConfig) { h.ShutdownTimeout = 0 }, "http.shutdown_timeout"},
		// F9: RateLimit validation
		{"negative rate_limit", func(h *HTTPConfig) { h.RateLimit = -1.0 }, "http.rate_limit"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.HTTP)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

// F14: Tests for Tail, Server, Views, TLS, BufferManager validation.

func TestValidateTail(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*TailConfig)
		wantErr string
	}{
		{"negative max_concurrent_sessions", func(tc *TailConfig) { tc.MaxConcurrentSessions = -5 }, "tail.max_concurrent_sessions"},
		{"negative max_session_duration", func(tc *TailConfig) { tc.MaxSessionDuration = -time.Hour }, "tail.max_session_duration"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.Tail)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateTail_ZeroIsUnlimited(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Tail.MaxConcurrentSessions = 0
	cfg.Tail.MaxSessionDuration = 0
	if err := cfg.Validate(); err != nil {
		t.Fatalf("zero (unlimited) should be valid: %v", err)
	}
}

func TestValidateServer(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*ServerConfig)
		wantErr string
	}{
		{"negative total_memory_pool_bytes", func(s *ServerConfig) { s.TotalMemoryPoolBytes = -1 }, "server.total_memory_pool_bytes"},
		{"cache_reserve_percent too high", func(s *ServerConfig) { s.CacheReservePercent = 51 }, "server.cache_reserve_percent"},
		{"cache_reserve_percent negative", func(s *ServerConfig) { s.CacheReservePercent = -1 }, "server.cache_reserve_percent"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.Server)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateViews(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*ViewsConfig)
		wantErr string
	}{
		{"negative max_backfill_memory", func(v *ViewsConfig) { v.MaxBackfillMemoryBytes = -1 }, "views.max_backfill_memory_bytes"},
		{"negative backfill_backpressure_wait", func(v *ViewsConfig) { v.BackfillBackpressureWait = Duration(-time.Second) }, "views.backfill_backpressure_wait"},
		{"negative backfill_max_retries", func(v *ViewsConfig) { v.BackfillMaxRetries = -1 }, "views.backfill_max_retries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.Views)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateTLS(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*TLSConfig)
		wantErr string
	}{
		{"cert without key", func(tc *TLSConfig) { tc.CertFile = "/path/cert.pem" }, "tls.key_file"},
		{"key without cert", func(tc *TLSConfig) { tc.KeyFile = "/path/key.pem" }, "tls.cert_file"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.TLS)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateTLS_BothSetIsValid(t *testing.T) {
	cfg := DefaultConfig()
	cfg.TLS.CertFile = "/path/cert.pem"
	cfg.TLS.KeyFile = "/path/key.pem"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("both cert and key should be valid: %v", err)
	}
}

func TestValidateBufferManager(t *testing.T) {
	tests := []struct {
		name    string
		modify  func(*BufferManagerConfig)
		wantErr string
	}{
		{"non-power-of-2 page_size when enabled", func(b *BufferManagerConfig) {
			b.Enabled = true
			b.PageSize = 5000
		}, "buffer_manager.page_size"},
		{"page_size too small when enabled", func(b *BufferManagerConfig) {
			b.Enabled = true
			b.PageSize = 2048
		}, "buffer_manager.page_size"},
		{"cache_target_percent over 100", func(b *BufferManagerConfig) { b.CacheTargetPercent = 101 }, "buffer_manager.cache_target_percent"},
		{"cache_target_percent negative", func(b *BufferManagerConfig) { b.CacheTargetPercent = -1 }, "buffer_manager.cache_target_percent"},
		{"query_target_percent over 100", func(b *BufferManagerConfig) { b.QueryTargetPercent = 101 }, "buffer_manager.query_target_percent"},
		{"batcher_target_percent negative", func(b *BufferManagerConfig) { b.BatcherTargetPercent = -1 }, "buffer_manager.batcher_target_percent"},
		// F8: cross-field sum check
		{"target percents sum > 100", func(b *BufferManagerConfig) {
			b.CacheTargetPercent = 60
			b.QueryTargetPercent = 30
			b.BatcherTargetPercent = 20
		}, "sum of target percentages must not exceed 100"},
		{"negative max_pinned_pages_per_query", func(b *BufferManagerConfig) { b.MaxPinnedPagesPerQuery = -1 }, "buffer_manager.max_pinned_pages_per_query"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tt.modify(&cfg.BufferManager)
			err := cfg.Validate()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error %q should contain %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestValidateBufferManager_DisabledSkipsPageSizeCheck(t *testing.T) {
	// When disabled, non-power-of-2 page sizes should still pass (the field is unused).
	cfg := DefaultConfig()
	cfg.BufferManager.Enabled = false
	cfg.BufferManager.PageSize = 5000
	if err := cfg.Validate(); err != nil {
		t.Fatalf("disabled buffer manager should skip page_size check: %v", err)
	}
}

func TestValidateBufferManager_PercentsSumExactly100(t *testing.T) {
	cfg := DefaultConfig()
	cfg.BufferManager.CacheTargetPercent = 50
	cfg.BufferManager.QueryTargetPercent = 30
	cfg.BufferManager.BatcherTargetPercent = 20
	if err := cfg.Validate(); err != nil {
		t.Fatalf("percents summing to exactly 100 should be valid: %v", err)
	}
}

func TestValidationErrorType(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.Compression = "snappy"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error")
	}

	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T: %v", err, err)
	}
	if ve.Section != "storage" {
		t.Errorf("Section: got %q, want storage", ve.Section)
	}
	if ve.Field != "compression" {
		t.Errorf("Field: got %q, want compression", ve.Field)
	}
	if ve.Value != "snappy" {
		t.Errorf("Value: got %q, want snappy", ve.Value)
	}
	if ve.Message != "must be lz4 or zstd" {
		t.Errorf("Message: got %q", ve.Message)
	}
}

func TestValidationErrorTopLevel(t *testing.T) {
	cfg := DefaultConfig()
	cfg.LogLevel = "verbose"
	err := cfg.Validate()

	var ve *ValidationError
	if !errors.As(err, &ve) {
		t.Fatalf("expected *ValidationError, got %T", err)
	}
	if ve.Section != "" {
		t.Errorf("Section: got %q, want empty for top-level", ve.Section)
	}
	if ve.Field != "log_level" {
		t.Errorf("Field: got %q, want log_level", ve.Field)
	}
	if ve.Value != "verbose" {
		t.Errorf("Value: got %q, want verbose", ve.Value)
	}
}
