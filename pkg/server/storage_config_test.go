package server

import (
	"testing"
	"time"

	"github.com/lynxbase/lynxdb/pkg/config"
)

func TestBatcherConfigFromStorageConfigUsesConfiguredFlushSettings(t *testing.T) {
	storageCfg := config.DefaultConfig().Storage
	storageCfg.FlushThreshold = 256 * config.MB
	storageCfg.FlushIdleTimeout = 15 * time.Second

	batcherCfg := batcherConfigFromStorageConfig(storageCfg)
	if batcherCfg.MaxBytes != int64(256*config.MB) {
		t.Fatalf("MaxBytes: got %d, want %d", batcherCfg.MaxBytes, int64(256*config.MB))
	}
	if batcherCfg.MaxWait != 15*time.Second {
		t.Fatalf("MaxWait: got %s, want 15s", batcherCfg.MaxWait)
	}
}

func TestCompactionWorkersFromStorageConfigUsesConfiguredWorkers(t *testing.T) {
	storageCfg := config.DefaultConfig().Storage
	storageCfg.CompactionWorkers = 6

	if got := compactionWorkersFromStorageConfig(storageCfg); got != 6 {
		t.Fatalf("workers: got %d, want 6", got)
	}
}
