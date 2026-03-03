package model

import (
	"errors"
	"testing"
	"time"
)

func TestDefaultIndexConfig(t *testing.T) {
	cfg := DefaultIndexConfig("main")
	if cfg.Name != "main" {
		t.Fatalf("expected name 'main', got %q", cfg.Name)
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config should be valid: %v", err)
	}
}

func TestIndexConfigValidation(t *testing.T) {
	base := DefaultIndexConfig("test")

	tests := []struct {
		name    string
		modify  func(*IndexConfig)
		wantErr error
	}{
		{
			"valid",
			func(c *IndexConfig) {},
			nil,
		},
		{
			"empty name",
			func(c *IndexConfig) { c.Name = "" },
			ErrEmptyIndexName,
		},
		{
			"zero retention",
			func(c *IndexConfig) { c.RetentionPeriod = 0 },
			ErrInvalidRetention,
		},
		{
			"negative retention",
			func(c *IndexConfig) { c.RetentionPeriod = -time.Hour },
			ErrInvalidRetention,
		},
		{
			"zero replication",
			func(c *IndexConfig) { c.ReplicationFactor = 0 },
			ErrInvalidReplication,
		},
		{
			"zero partitions",
			func(c *IndexConfig) { c.PartitionCount = 0 },
			ErrInvalidPartitionCount,
		},
		{
			"invalid compression",
			func(c *IndexConfig) { c.Compression = "snappy" },
			ErrInvalidCompression,
		},
		{
			"valid compression lz4",
			func(c *IndexConfig) { c.Compression = "lz4" },
			nil,
		},
		{
			"valid compression zstd",
			func(c *IndexConfig) { c.Compression = "zstd" },
			nil,
		},
		{
			"valid compression none",
			func(c *IndexConfig) { c.Compression = "none" },
			nil,
		},
		{
			"valid compression empty",
			func(c *IndexConfig) { c.Compression = "" },
			nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := base
			tt.modify(&cfg)
			err := cfg.Validate()
			if tt.wantErr == nil {
				if err != nil {
					t.Fatalf("expected no error, got %v", err)
				}

				return
			}
			if !errors.Is(err, tt.wantErr) {
				t.Fatalf("expected error %v, got %v", tt.wantErr, err)
			}
		})
	}
}
