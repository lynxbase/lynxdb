package database

import (
	"context"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
)

// ListPartitions describes the live partitions of an index in time order.
func (db *DB) ListPartitions(ctx context.Context, index string) ([]colstore.PartitionStats, error) {
	return db.core.ListPartitions(ctx, index)
}

// DropPartition removes a partition and reports the events and bytes removed.
func (db *DB) DropPartition(ctx context.Context, index string, start time.Time) (colstore.PartitionStats, error) {
	return db.core.DropPartition(ctx, index, start)
}

// DetachPartition archives a partition before removing it from index.
func (db *DB) DetachPartition(ctx context.Context, index string, start time.Time, dest string) (colstore.PartitionStats, error) {
	return db.core.DetachPartition(ctx, index, start, dest)
}

// AttachPartition imports a partition subject to the index's usual retention.
func (db *DB) AttachPartition(ctx context.Context, index, src string) (colstore.PartitionStats, error) {
	return db.core.AttachPartition(ctx, index, src)
}
