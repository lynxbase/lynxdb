package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"

	"github.com/lynxbase/lynxdb/internal/objstore"
	"github.com/vmihailenco/msgpack/v5"
)

// PartEntry describes a single part file stored in S3.
// It carries enough metadata for query planning (time range, event count,
// columns) and failover dedup (batch sequence, writer epoch).
type PartEntry struct {
	PartID      string   `msgpack:"part_id"`
	Index       string   `msgpack:"index"`
	MinTimeNs   int64    `msgpack:"min_time_ns"`
	MaxTimeNs   int64    `msgpack:"max_time_ns"`
	EventCount  int64    `msgpack:"event_count"`
	SizeBytes   int64    `msgpack:"size_bytes"`
	Level       int      `msgpack:"level"`
	S3Key       string   `msgpack:"s3_key"`
	Columns     []string `msgpack:"columns"`
	BatchSeq    uint64   `msgpack:"batch_seq"`
	WriterEpoch uint64   `msgpack:"writer_epoch"`
	CreatedAtNs int64    `msgpack:"created_at_ns"`
}

// Catalog is the on-disk representation of all parts for a single partition.
// Stored as a single msgpack blob in object storage.
type Catalog struct {
	Version uint64      `msgpack:"version"`
	Parts   []PartEntry `msgpack:"parts"`
}

// CatalogPartition identifies a catalog shard in object storage.
type CatalogPartition struct {
	Index     string
	Partition uint32
}

// PartCatalog manages the S3-backed part catalog for cluster mode.
// Each (index, partition) pair has an independent catalog file stored at:
//
//	catalog/<index>/p<partition>/catalog.msgpack
//
// The catalog is the source of truth for which parts exist and what batch
// sequences have been committed. Query nodes use it to discover parts,
// and failover logic uses LastBatchSeq to resume from the correct point.
type PartCatalog struct {
	objStore objstore.ObjectStore
	logger   *slog.Logger
	locks    sync.Map // catalog/<index>/p<partition>/catalog.msgpack -> *sync.Mutex
}

// NewPartCatalog creates a new PartCatalog backed by the given object store.
func NewPartCatalog(store objstore.ObjectStore, logger *slog.Logger) *PartCatalog {
	return &PartCatalog{
		objStore: store,
		logger:   logger,
	}
}

// catalogKey returns the S3 key for a catalog file.
func catalogKey(index string, partition uint32) string {
	return fmt.Sprintf("catalog/%s/p%d/catalog.msgpack", index, partition)
}

// Load reads the catalog for the given (index, partition) from S3.
// Returns an empty catalog (Version 0) if the key does not exist.
func (c *PartCatalog) Load(ctx context.Context, index string, partition uint32) (*Catalog, error) {
	key := catalogKey(index, partition)

	data, err := c.objStore.Get(ctx, key)
	if err != nil {
		if objstore.IsNotFound(err) {
			return &Catalog{Version: 0}, nil
		}

		return nil, fmt.Errorf("ingest.PartCatalog.Load: get %s: %w", key, err)
	}

	var cat Catalog
	if err := msgpack.Unmarshal(data, &cat); err != nil {
		return nil, fmt.Errorf("ingest.PartCatalog.Load: unmarshal %s: %w", key, err)
	}

	return &cat, nil
}

// save writes the catalog back to S3. Callers must have incremented Version.
func (c *PartCatalog) save(ctx context.Context, index string, partition uint32, cat *Catalog) error {
	key := catalogKey(index, partition)

	data, err := msgpack.Marshal(cat)
	if err != nil {
		return fmt.Errorf("ingest.PartCatalog.save: marshal: %w", err)
	}

	if err := c.objStore.Put(ctx, key, data); err != nil {
		return fmt.Errorf("ingest.PartCatalog.save: put %s: %w", key, err)
	}

	return nil
}

func (c *PartCatalog) catalogLock(index string, partition uint32) *sync.Mutex {
	key := catalogKey(index, partition)
	mu, _ := c.locks.LoadOrStore(key, &sync.Mutex{})

	return mu.(*sync.Mutex)
}

func (c *PartCatalog) mutate(ctx context.Context, index string, partition uint32, fn func(*Catalog) (bool, error)) error {
	mu := c.catalogLock(index, partition)
	mu.Lock()
	defer mu.Unlock()

	cat, err := c.Load(ctx, index, partition)
	if err != nil {
		return err
	}

	changed, err := fn(cat)
	if err != nil {
		return err
	}
	if !changed {
		return nil
	}

	cat.Version++
	if err := c.save(ctx, index, partition, cat); err != nil {
		return err
	}

	return nil
}

// AddPart appends a new part entry to the catalog.
func (c *PartCatalog) AddPart(ctx context.Context, index string, partition uint32, entry PartEntry) error {
	return c.mutate(ctx, index, partition, func(cat *Catalog) (bool, error) {
		for _, part := range cat.Parts {
			if part.PartID == entry.PartID {
				c.logger.Debug("catalog already contains part",
					"index", index,
					"partition", partition,
					"part_id", entry.PartID)

				return false, nil
			}
		}

		cat.Parts = append(cat.Parts, entry)
		return true, nil
	})
}

// RemoveParts removes parts by ID from the catalog.
func (c *PartCatalog) RemoveParts(ctx context.Context, index string, partition uint32, partIDs []string) error {
	if len(partIDs) == 0 {
		return nil
	}

	removeSet := make(map[string]bool, len(partIDs))
	for _, id := range partIDs {
		removeSet[id] = true
	}

	return c.mutate(ctx, index, partition, func(cat *Catalog) (bool, error) {
		filtered := make([]PartEntry, 0, len(cat.Parts))
		for _, p := range cat.Parts {
			if !removeSet[p.PartID] {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) == len(cat.Parts) {
			return false, nil
		}

		cat.Parts = filtered
		return true, nil
	})
}

// LastBatchSeq returns the highest BatchSeq committed in the catalog for
// the given (index, partition). Returns 0 if the catalog is empty.
// Used during failover to determine the resume point for shadow batcher flush.
func (c *PartCatalog) LastBatchSeq(ctx context.Context, index string, partition uint32) (uint64, error) {
	cat, err := c.Load(ctx, index, partition)
	if err != nil {
		return 0, fmt.Errorf("ingest.PartCatalog.LastBatchSeq: %w", err)
	}

	var maxSeq uint64
	for _, p := range cat.Parts {
		if p.BatchSeq > maxSeq {
			maxSeq = p.BatchSeq
		}
	}

	return maxSeq, nil
}

// ListPartitions returns all catalog partitions currently present in object storage.
func (c *PartCatalog) ListPartitions(ctx context.Context) ([]CatalogPartition, error) {
	keys, err := c.objStore.List(ctx, "catalog/")
	if err != nil {
		return nil, fmt.Errorf("ingest.PartCatalog.ListPartitions: %w", err)
	}

	partitions := make([]CatalogPartition, 0, len(keys))
	for _, key := range keys {
		part, ok := parseCatalogKey(key)
		if !ok {
			continue
		}
		partitions = append(partitions, part)
	}

	return partitions, nil
}

func parseCatalogKey(key string) (CatalogPartition, bool) {
	if !strings.HasPrefix(key, "catalog/") || !strings.HasSuffix(key, "/catalog.msgpack") {
		return CatalogPartition{}, false
	}

	trimmed := strings.TrimSuffix(strings.TrimPrefix(key, "catalog/"), "/catalog.msgpack")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 || !strings.HasPrefix(parts[1], "p") || len(parts[1]) == 1 {
		return CatalogPartition{}, false
	}

	partition, err := strconv.ParseUint(parts[1][1:], 10, 32)
	if err != nil {
		return CatalogPartition{}, false
	}

	return CatalogPartition{
		Index:     parts[0],
		Partition: uint32(partition),
	}, true
}
