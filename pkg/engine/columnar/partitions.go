package columnar

import (
	"context"
	"fmt"
	"time"

	"github.com/lynxbase/lynxdb/pkg/colstore"
)

func (db *DB) partitionStore(index string) (*colstore.Store, error) {
	s, err := db.store(index, false)
	if s == nil && err == nil {
		return nil, fmt.Errorf("%w: %s", ErrNoIndex, index)
	}
	return s, err
}

// ListPartitions returns the live partitions of index in time order.
func (db *DB) ListPartitions(ctx context.Context, index string) ([]colstore.PartitionStats, error) {
	s, err := db.partitionStore(index)
	if err != nil {
		return nil, err
	}
	return s.ListPartitions(ctx)
}

// DropPartition removes the partition beginning at start.
func (db *DB) DropPartition(ctx context.Context, index string, start time.Time) (colstore.PartitionStats, error) {
	s, err := db.partitionStore(index)
	if err != nil {
		return colstore.PartitionStats{}, err
	}
	return s.DropPartition(ctx, start)
}

// DetachPartition writes the partition to dest and removes it from index.
func (db *DB) DetachPartition(ctx context.Context, index string, start time.Time, dest string) (colstore.PartitionStats, error) {
	s, err := db.partitionStore(index)
	if err != nil {
		return colstore.PartitionStats{}, err
	}
	return s.DetachPartition(ctx, start, dest)
}

// AttachPartition imports an archive into an existing compatible index.
func (db *DB) AttachPartition(ctx context.Context, index, src string) (colstore.PartitionStats, error) {
	s, err := db.partitionStore(index)
	if err != nil {
		return colstore.PartitionStats{}, err
	}
	return s.AttachPartition(ctx, src)
}
