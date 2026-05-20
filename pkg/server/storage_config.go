package server

import (
	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/storage/part"
)

func batcherConfigFromStorageConfig(storageCfg config.StorageConfig) part.BatcherConfig {
	cfg := part.DefaultBatcherConfig()
	if storageCfg.FlushThreshold > 0 {
		cfg.MaxBytes = storageCfg.FlushThreshold.Int64()
	}
	if storageCfg.FlushIdleTimeout > 0 {
		cfg.MaxWait = storageCfg.FlushIdleTimeout
	}

	return cfg
}

func compactionWorkersFromStorageConfig(storageCfg config.StorageConfig) int {
	if storageCfg.CompactionWorkers > 0 {
		return storageCfg.CompactionWorkers
	}

	return config.DefaultConfig().Storage.CompactionWorkers
}
