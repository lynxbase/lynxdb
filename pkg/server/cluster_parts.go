package server

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	ingestcluster "github.com/lynxbase/lynxdb/pkg/cluster/ingest"
	clusterpb "github.com/lynxbase/lynxdb/pkg/cluster/rpc/proto"
	"github.com/lynxbase/lynxdb/pkg/model"
)

func (e *Engine) handleClusterPartCommitted(ctx context.Context, n *clusterpb.PartCommittedNotification) error {
	if n == nil {
		return nil
	}

	if e.clusterCatalog == nil {
		e.registerRemoteCatalogPart(ingestcluster.PartEntry{
			PartID:      n.PartId,
			Index:       parseShardIndex(n.ShardId),
			MinTimeNs:   n.MinTimeUnixNs,
			MaxTimeNs:   n.MaxTimeUnixNs,
			EventCount:  n.EventCount,
			S3Key:       n.S3Key,
			BatchSeq:    n.BatchSeq,
			CreatedAtNs: time.Now().UnixNano(),
		}, 0)
		return nil
	}

	index, partition, err := parseCatalogShardID(n.ShardId)
	if err != nil {
		return fmt.Errorf("handleClusterPartCommitted: parse shard id %q: %w", n.ShardId, err)
	}

	if err := e.reconcileClusterCatalogPartition(ctx, index, partition); err != nil {
		return fmt.Errorf("handleClusterPartCommitted: reconcile %s/p%d: %w", index, partition, err)
	}

	return nil
}

func (e *Engine) reconcileClusterCatalogs(ctx context.Context) error {
	if e.clusterCatalog == nil {
		return nil
	}

	partitions, err := e.clusterCatalog.ListPartitions(ctx)
	if err != nil {
		return err
	}

	for _, part := range partitions {
		if err := e.reconcileClusterCatalogPartition(ctx, part.Index, part.Partition); err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) reconcileClusterCatalogPartition(ctx context.Context, index string, partition uint32) error {
	cat, err := e.clusterCatalog.Load(ctx, index, partition)
	if err != nil {
		return err
	}

	for _, entry := range cat.Parts {
		e.registerRemoteCatalogPart(entry, partition)
	}

	return nil
}

func (e *Engine) registerRemoteCatalogPart(entry ingestcluster.PartEntry, partition uint32) bool {
	if entry.PartID == "" || entry.Index == "" || entry.S3Key == "" {
		return false
	}

	meta := model.SegmentMeta{
		ID:         entry.PartID,
		Index:      entry.Index,
		Partition:  fmt.Sprintf("p%d", partition),
		MinTime:    time.Unix(0, entry.MinTimeNs),
		MaxTime:    time.Unix(0, entry.MaxTimeNs),
		EventCount: entry.EventCount,
		SizeBytes:  entry.SizeBytes,
		Level:      entry.Level,
		Columns:    append([]string(nil), entry.Columns...),
		Tier:       "warm",
		ObjectKey:  entry.S3Key,
	}
	if entry.CreatedAtNs > 0 {
		meta.CreatedAt = time.Unix(0, entry.CreatedAtNs)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	cur := e.currentEpoch.Load()
	for _, seg := range cur.segments {
		if seg.meta.ID == entry.PartID {
			return false
		}
	}

	sh := &segmentHandle{
		meta:  meta,
		index: entry.Index,
	}

	if e.tierMgr != nil {
		e.tierMgr.AddSegment(meta)
	}

	combined := make([]*segmentHandle, len(cur.segments)+1)
	copy(combined, cur.segments)
	combined[len(cur.segments)] = sh
	e.advanceEpoch(combined, nil)

	if _, ok := e.indexes[entry.Index]; !ok {
		e.indexes[entry.Index] = model.DefaultIndexConfig(entry.Index)
	}
	e.sourceRegistry.Register(entry.Index)
	e.ingestGen.Add(1)

	e.logger.Debug("registered remote cluster part",
		"part_id", entry.PartID,
		"index", entry.Index,
		"partition", partition,
		"s3_key", entry.S3Key)

	return true
}

func parseCatalogShardID(shardID string) (string, uint32, error) {
	parts := strings.Split(shardID, "/")
	if len(parts) < 2 {
		return "", 0, fmt.Errorf("invalid shard id")
	}

	index := parts[0]
	last := parts[len(parts)-1]
	if !strings.HasPrefix(last, "p") || len(last) == 1 {
		return "", 0, fmt.Errorf("missing partition in shard id")
	}

	partition, err := strconv.ParseUint(last[1:], 10, 32)
	if err != nil {
		return "", 0, fmt.Errorf("invalid partition: %w", err)
	}

	return index, uint32(partition), nil
}

func parseShardIndex(shardID string) string {
	if shardID == "" {
		return DefaultIndexName
	}
	if idx := strings.IndexByte(shardID, '/'); idx > 0 {
		return shardID[:idx]
	}
	return shardID
}
