package model

import "errors"

var (
	ErrEmptyIndexName        = errors.New("index name must not be empty")
	ErrInvalidRetention      = errors.New("retention period must be positive")
	ErrInvalidReplication    = errors.New("replication factor must be at least 1")
	ErrInvalidPartitionCount = errors.New("partition count must be at least 1")
	ErrInvalidCompression    = errors.New("unsupported compression algorithm (valid: lz4, zstd, none)")
)
