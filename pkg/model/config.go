package model

import "time"

// IndexConfig defines the configuration for a logical index.
type IndexConfig struct {
	// Name is the unique name of the index.
	Name string
	// MaxHotAge is how long segments stay on hot (SSD) storage.
	MaxHotAge time.Duration
	// MaxWarmAge is how long segments stay on warm (S3) storage before moving to cold.
	MaxWarmAge time.Duration
	// RetentionPeriod is the maximum age of events before deletion.
	RetentionPeriod time.Duration
	// ReplicationFactor is the number of replicas for each segment.
	ReplicationFactor int
	// PartitionCount is the number of partitions (for hashing events to shards).
	PartitionCount int
	// Compression is the layer 2 block compression algorithm: "lz4" (default) or "zstd".
	Compression string
}

// DefaultIndexConfig returns a sensible default IndexConfig.
func DefaultIndexConfig(name string) IndexConfig {
	return IndexConfig{
		Name:              name,
		MaxHotAge:         7 * 24 * time.Hour,  // 7 days
		MaxWarmAge:        30 * 24 * time.Hour, // 30 days
		RetentionPeriod:   90 * 24 * time.Hour, // 90 days
		ReplicationFactor: 1,
		PartitionCount:    4,
		Compression:       "lz4",
	}
}

// Validate returns an error if the config is invalid.
func (c IndexConfig) Validate() error {
	if c.Name == "" {
		return ErrEmptyIndexName
	}
	if c.RetentionPeriod <= 0 {
		return ErrInvalidRetention
	}
	if c.ReplicationFactor < 1 {
		return ErrInvalidReplication
	}
	if c.PartitionCount < 1 {
		return ErrInvalidPartitionCount
	}
	switch c.Compression {
	case "", "lz4", "zstd", "none":
		// valid
	default:
		return ErrInvalidCompression
	}

	return nil
}
