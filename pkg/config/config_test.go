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
	if !cfg.Ingest.ESCompat.Enabled {
		t.Error("expected ES compatibility enabled by default")
	}
	if cfg.Ingest.ESCompat.AdvertisedVersion != "8.15.0" {
		t.Errorf("expected advertised ES version 8.15.0, got %q", cfg.Ingest.ESCompat.AdvertisedVersion)
	}
	if cfg.Ingest.ESCompat.ClusterName != "lynxdb" {
		t.Errorf("expected ES cluster name lynxdb, got %q", cfg.Ingest.ESCompat.ClusterName)
	}
	if !cfg.Ingest.ESCompat.StripLogstashDateSuffix {
		t.Error("expected ES Logstash date suffix stripping enabled by default")
	}
	if cfg.Ingest.OTLP.HTTPListen != "0.0.0.0:4318" {
		t.Errorf("expected OTLP HTTP listen 0.0.0.0:4318, got %q", cfg.Ingest.OTLP.HTTPListen)
	}
	if cfg.Ingest.OTLP.GRPCMaxRecvBytes != 16*MB {
		t.Errorf("expected OTLP gRPC max recv 16mb, got %s", cfg.Ingest.OTLP.GRPCMaxRecvBytes)
	}
	if !cfg.Ingest.SplunkHEC.Enabled {
		t.Error("expected Splunk HEC enabled by default")
	}
	if cfg.Ingest.SplunkHEC.RequireToken {
		t.Error("expected Splunk HEC require_token false by default")
	}
	if cfg.Ingest.Limits.MaxCompressedBodyBytes != 32*MB {
		t.Errorf("expected compressed body limit 32mb, got %s", cfg.Ingest.Limits.MaxCompressedBodyBytes)
	}
	if cfg.Ingest.Limits.MaxDecompressedBodyBytes != 256*MB {
		t.Errorf("expected decompressed body limit 256mb, got %s", cfg.Ingest.Limits.MaxDecompressedBodyBytes)
	}
	if !cfg.Ingest.Staging.Enabled {
		t.Error("expected staging enabled by default")
	}
	if cfg.Ingest.Staging.MaxBytes != 64*MB {
		t.Errorf("expected staging max_bytes 64mb, got %s", cfg.Ingest.Staging.MaxBytes)
	}
	if cfg.Ingest.Staging.MaxAge != Duration(5*time.Second) {
		t.Errorf("expected staging max_age 5s, got %s", cfg.Ingest.Staging.MaxAge)
	}
	if cfg.HTTP.IdleTimeout != 120*time.Second {
		t.Errorf("expected 120s, got %v", cfg.HTTP.IdleTimeout)
	}
}
