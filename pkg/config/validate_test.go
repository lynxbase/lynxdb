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
			"invalid listen address",
			func(c *Config) { c.Listen = "not-a-valid-address" },
			"listen: must be a valid host:port address",
		},
		{
			"invalid log level",
			func(c *Config) { c.LogLevel = "verbose" },
			"log_level: must be debug, info, warn, or error",
		},
		{
			"auth without a data directory",
			func(c *Config) { c.Auth.Enabled = true; c.DataDir = "" },
			"auth.enabled: requires data_dir",
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
		{"negative partition", func(s *StorageConfig) { s.Partition = Duration(-time.Hour) }, "storage.partition"},
		{"negative flush_interval", func(s *StorageConfig) { s.FlushInterval = Duration(-time.Second) }, "storage.flush_interval"},
		{"empty time field", func(s *StorageConfig) { s.TimeFields = []string{"ts", ""} }, "storage.time_fields"},
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
		{"zero job ttl", func(q *QueryConfig) { q.JobTTL = 0 }, "query.job_ttl"},
		{"negative max query length", func(q *QueryConfig) { q.MaxQueryLength = -1 }, "query.max_query_length"},
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
		{"bad advertised version", func(i *IngestConfig) {
			i.ESCompat.AdvertisedVersion = "8.x"
		}, "ingest.es_compat.advertised_version"},
		{"empty enabled cluster name", func(i *IngestConfig) {
			i.ESCompat.ClusterName = ""
		}, "ingest.es_compat.cluster_name"},
		{"bad otlp http listen", func(i *IngestConfig) {
			i.OTLP.HTTPListen = "not-an-addr"
		}, "ingest.otlp.http_listen"},
		{"bad otlp grpc listen", func(i *IngestConfig) {
			i.OTLP.GRPCListen = "not-an-addr"
		}, "ingest.otlp.grpc_listen"},
		{"small otlp grpc max recv", func(i *IngestConfig) {
			i.OTLP.GRPCMaxRecvBytes = 512 * KB
		}, "ingest.otlp.grpc_max_recv_bytes"},
		{"small compressed body limit", func(i *IngestConfig) {
			i.Limits.MaxCompressedBodyBytes = 100
		}, "ingest.limits.max_compressed_body_bytes"},
		{"decompressed below compressed", func(i *IngestConfig) {
			i.Limits.MaxCompressedBodyBytes = 2 * MB
			i.Limits.MaxDecompressedBodyBytes = 1 * MB
		}, "must be >= limits.max_compressed_body_bytes"},
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
		{"negative read_header_timeout", func(h *HTTPConfig) { h.ReadHeaderTimeout = -time.Second }, "http.read_header_timeout"},
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

func TestValidationErrorType(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.Partition = Duration(-time.Hour)
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
	if ve.Field != "partition" {
		t.Errorf("Field: got %q, want partition", ve.Field)
	}
	if ve.Value != "-1h0m0s" {
		t.Errorf("Value: got %q, want -1h0m0s", ve.Value)
	}
	if ve.Message != "must not be negative" {
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
