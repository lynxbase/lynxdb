package config

import (
	"slices"
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
	if cfg.Storage.Partition != Duration(24*time.Hour) {
		t.Errorf("expected 24h partition, got %v", cfg.Storage.Partition)
	}
	if cfg.Storage.FlushInterval != 0 {
		t.Errorf("expected zero flush interval, got %v", cfg.Storage.FlushInterval)
	}
	if want := []string{"_time", "timestamp", "@timestamp", "time", "ts"}; !slices.Equal(cfg.Storage.TimeFields, want) {
		t.Errorf("expected time fields %v, got %v", want, cfg.Storage.TimeFields)
	}
	if cfg.Query.MaxConcurrent != defaultMaxConcurrent {
		t.Errorf("expected %d, got %d", defaultMaxConcurrent, cfg.Query.MaxConcurrent)
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
	if cfg.HTTP.IdleTimeout != 120*time.Second {
		t.Errorf("expected 120s, got %v", cfg.HTTP.IdleTimeout)
	}
}
