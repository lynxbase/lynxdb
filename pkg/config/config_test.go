package config

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()
	if cfg.Listen != "localhost:3100" {
		t.Errorf("expected localhost:3100, got %s", cfg.Listen)
	}
	if cfg.Retention != Duration(7*24*time.Hour) {
		t.Errorf("expected 7d, got %v", cfg.Retention)
	}
	if cfg.LogLevel != "info" {
		t.Errorf("expected info, got %s", cfg.LogLevel)
	}
	if cfg.Storage.Compression != "lz4" {
		t.Errorf("expected lz4, got %s", cfg.Storage.Compression)
	}
	if cfg.Storage.RowGroupSize != 65536 {
		t.Errorf("expected 65536, got %d", cfg.Storage.RowGroupSize)
	}
	if cfg.Query.MaxConcurrent != 10 {
		t.Errorf("expected 10, got %d", cfg.Query.MaxConcurrent)
	}
	if cfg.Ingest.MaxBatchSize != 1000 {
		t.Errorf("expected 1000, got %d", cfg.Ingest.MaxBatchSize)
	}
	if cfg.HTTP.IdleTimeout != 120*time.Second {
		t.Errorf("expected 120s, got %v", cfg.HTTP.IdleTimeout)
	}
}
