package pipeline

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/lynxbase/lynxdb/pkg/event"
	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// defaultGracePartitions is the number of partitions used in grace hash join.
// 64 provides a good balance between partition granularity and file handle count.
const defaultGracePartitions = 64

// joinPrefetchBuffer is the number of batches to buffer during left-side
// prefetch. 4 batches × 1024 rows × ~200B ≈ 800KB fixed overhead.
const joinPrefetchBuffer = 4

// JoinIterator implements hash join (build small side, probe large side).
// When memory budget is exceeded and a SpillManager is configured, it
// transparently falls back to grace hash join: partition both sides to
// disk, then process one partition at a time.
//
// When prefetch is enabled, the left side is read into a buffered channel
// concurrently with the right-side hash table build, overlapping I/O with
// computation for better throughput.
type JoinIterator struct {
	left      Iterator
	right     Iterator
	field     string
	joinType  string // "inner" or "left"
	strategy  string // "hash" (default), "in_list", "bloom_semi"
	hashMap   map[string][]map[string]event.Value
	bloomKeys map[uint64]bool // for bloom_semi pre-filter
	built     bool
	acct      memgov.MemoryAccount // per-operator memory tracking

	// Grace hash join state (populated only when in-memory build exceeds budget).
	spillMgr         *SpillManager
	graceMode        bool
	leftPartPaths    []string                            // paths to left partition spill files
	rightPartPaths   []string                            // paths to right partition spill files
	currentPartition int                                 // current partition being probed
	partHashMap      map[string][]map[string]event.Value // temp hash table for current partition
	partLeftReader   *SpillReader                        // reader for current left partition
	partBuffer       []map[string]event.Value            // buffered output rows for current partition
	partBufferOffset int                                 // offset into partBuffer
	spilledRows      int64                               // total rows spilled (for ResourceReporter)

	// Prefetch state: when enabled, left-side batches are read into a
	// buffered channel concurrently with the right-side hash table build.
	prefetch       bool
	prefetchCh     chan batchResult // buffered channel for prefetched left batches
	prefetchWg     sync.WaitGroup
	prefetchCancel context.CancelFunc // cancels the prefetch goroutine's context
}

// NewJoinIterator creates a hash join operator.
func NewJoinIterator(left, right Iterator, field, joinType string) *JoinIterator {
	return &JoinIterator{
		left:     left,
		right:    right,
		field:    field,
		joinType: joinType,
		strategy: "hash",
		hashMap:  make(map[string][]map[string]event.Value),
		acct:     memgov.NopAccount(),
	}
}

// SetPrefetch enables left-side prefetch during hash table build.
// When enabled, a goroutine reads left-side batches into a buffered
// channel concurrently with the right-side hash table build, overlapping
// I/O with computation.
func (j *JoinIterator) SetPrefetch(enabled bool) {
	j.prefetch = enabled
}

// NewJoinIteratorWithStrategy creates a hash join with a specific strategy,
// memory budget tracking, and optional grace hash join fallback via SpillManager.
func NewJoinIteratorWithStrategy(left, right Iterator, field, joinType, strategy string,
	acct memgov.MemoryAccount, mgr *SpillManager) *JoinIterator {
	j := NewJoinIteratorWithSpill(left, right, field, joinType, acct, mgr)
	j.strategy = strategy

	return j
}

// NewJoinIteratorWithBudget creates a hash join with memory budget tracking.
func NewJoinIteratorWithBudget(left, right Iterator, field, joinType string, acct memgov.MemoryAccount) *JoinIterator {
	j := NewJoinIterator(left, right, field, joinType)
	j.acct = memgov.EnsureAccount(acct)

	return j
}

// NewJoinIteratorWithSpill creates a hash join with memory budget tracking
// and grace hash join fallback. When the in-memory build side exceeds the
// budget, both sides are partitioned to disk and joined one partition at a time.
func NewJoinIteratorWithSpill(left, right Iterator, field, joinType string,
	acct memgov.MemoryAccount, mgr *SpillManager) *JoinIterator {
	j := NewJoinIteratorWithBudget(left, right, field, joinType, acct)
	j.spillMgr = mgr

	return j
}

func (j *JoinIterator) Init(ctx context.Context) error {
	if err := j.left.Init(ctx); err != nil {
		return err
	}

	return j.right.Init(ctx)
}

func (j *JoinIterator) Next(ctx context.Context) (*Batch, error) {
	if !j.built {
		if err := j.buildHashTable(ctx); err != nil {
			return nil, err
		}
	}

	if j.graceMode {
		return j.nextGrace(ctx)
	}

	// Read from prefetch channel if active, otherwise directly from left.
	var batch *Batch
	var err error
	if j.prefetchCh != nil {
		batch, err = j.nextPrefetched(ctx)
	} else {
		batch, err = j.left.Next(ctx)
	}
	if batch == nil || err != nil {
		return nil, err
	}

	result := NewBatch(batch.Len)
	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		key := ""
		if v, ok := row[j.field]; ok {
			key = v.String()
		}

		// Bloom pre-filter: skip rows whose key hash isn't in the build side.
		if j.bloomKeys != nil {
			h := fnv.New64a()
			h.Write([]byte(key))
			if !j.bloomKeys[h.Sum64()] {
				if strings.EqualFold(j.joinType, "left") {
					result.AddRow(row)
				}

				continue
			}
		}

		matches := j.hashMap[key]
		if len(matches) > 0 {
			for _, match := range matches {
				merged := mergeRows(row, match)
				result.AddRow(merged)
			}
		} else if strings.EqualFold(j.joinType, "left") {
			result.AddRow(row)
		}
	}
	if result.Len > 0 {
		return result, nil
	}

	return j.Next(ctx) // pull next batch from left
}

func (j *JoinIterator) Close() error {
	// Cancel the prefetch context first — this unblocks a left.Next() call
	// that may be stuck on slow I/O, allowing the goroutine to exit promptly.
	if j.prefetchCancel != nil {
		j.prefetchCancel()
	}

	// Wait for prefetch goroutine to exit (if running).
	// Drain the channel to unblock the goroutine if it's stuck on send.
	if j.prefetchCh != nil {
		go func() {
			for range j.prefetchCh {
			}
		}()
		// Use a generous timeout as a last resort — the cancel above should
		// cause a prompt exit in most cases.
		done := make(chan struct{})
		go func() {
			j.prefetchWg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			// Prefetch goroutine is stuck — proceed with cleanup anyway.
			// The goroutine will eventually exit when the left iterator
			// returns an error or EOF.
		}
	}

	if j.partLeftReader != nil {
		j.partLeftReader.Close()
		j.partLeftReader = nil
	}
	// Release all partition spill files.
	if j.spillMgr != nil {
		for _, p := range j.leftPartPaths {
			j.spillMgr.Release(p)
		}
		for _, p := range j.rightPartPaths {
			j.spillMgr.Release(p)
		}
	}
	j.leftPartPaths = nil
	j.rightPartPaths = nil

	j.acct.Close()
	j.left.Close()

	return j.right.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (j *JoinIterator) MemoryUsed() int64 {
	return j.acct.Used()
}

// ResourceStats implements ResourceReporter for per-operator spill metrics.
func (j *JoinIterator) ResourceStats() OperatorResourceStats {
	return OperatorResourceStats{
		PeakBytes:   j.acct.MaxUsed(),
		SpilledRows: j.spilledRows,
	}
}

func (j *JoinIterator) Schema() []FieldInfo { return nil }

func (j *JoinIterator) buildHashTable(ctx context.Context) error {
	// Start left-side prefetch if enabled. The goroutine reads left batches
	// into a buffered channel while we build the hash table from the right side.
	if j.prefetch && !j.graceMode {
		j.startPrefetch(ctx)
	}

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := j.right.Next(ctx)
		if err != nil {
			return err
		}
		if batch == nil {
			break
		}
		if err := j.acct.Grow(int64(batch.Len) * estimatedRowBytes); err != nil {
			// Budget exceeded — fall back to grace hash join if SpillManager is configured.
			if j.spillMgr != nil {
				return j.graceHashJoin(ctx, batch)
			}

			return fmt.Errorf("join.buildHashTable: %w", err)
		}
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			key := ""
			if v, ok := row[j.field]; ok {
				key = v.String()
			}
			j.hashMap[key] = append(j.hashMap[key], row)
		}
	}

	// For bloom_semi strategy, build a hash set of keys for pre-filtering.
	if j.strategy == "bloom_semi" && len(j.hashMap) > 0 {
		j.bloomKeys = make(map[uint64]bool, len(j.hashMap))
		for key := range j.hashMap {
			h := fnv.New64a()
			h.Write([]byte(key))
			j.bloomKeys[h.Sum64()] = true
		}
	}

	j.built = true

	return nil
}

// startPrefetch spawns a goroutine that reads left-side batches into a
// buffered channel. This overlaps left-side I/O with right-side hash table
// construction.
//
// A derived context with cancel is used so that Close() can immediately
// unblock a left.Next() call that may be stuck on slow I/O, rather than
// relying on a wall-clock timeout.
func (j *JoinIterator) startPrefetch(ctx context.Context) {
	prefetchCtx, cancel := context.WithCancel(ctx)
	j.prefetchCancel = cancel
	j.prefetchCh = make(chan batchResult, joinPrefetchBuffer)
	j.prefetchWg.Add(1)

	go func() {
		defer j.prefetchWg.Done()
		defer close(j.prefetchCh)

		for {
			batch, err := j.left.Next(prefetchCtx)
			if err != nil {
				select {
				case j.prefetchCh <- batchResult{err: err}:
				case <-prefetchCtx.Done():
				}

				return
			}
			if batch == nil {
				return // left side exhausted
			}
			select {
			case j.prefetchCh <- batchResult{batch: batch}:
			case <-prefetchCtx.Done():
				return
			}
		}
	}()
}

// nextPrefetched reads the next left batch from the prefetch channel.
// Returns (nil, nil) when the left side is exhausted.
func (j *JoinIterator) nextPrefetched(_ context.Context) (*Batch, error) {
	res, ok := <-j.prefetchCh
	if !ok {
		return nil, nil
	}
	if res.err != nil {
		return nil, res.err
	}

	return res.batch, nil
}

// hashPartition computes a partition index for a join key using FNV-32a.
func hashPartition(key string, numPartitions int) int {
	h := fnv.New32a()
	h.Write([]byte(key))

	return int(h.Sum32() % uint32(numPartitions))
}

// graceHashJoin switches to grace hash join when the in-memory build side
// exceeds the memory budget. It partitions both sides into N spill files
// using hash partitioning on the join key, then processes one partition at a time.
func (j *JoinIterator) graceHashJoin(ctx context.Context, overflowBatch *Batch) error {
	numParts := defaultGracePartitions

	rightWriters := make([]*SpillWriter, numParts)
	for i := range rightWriters {
		sw, err := NewManagedSpillWriter(j.spillMgr, fmt.Sprintf("join-R-%02d", i))
		if err != nil {
			return fmt.Errorf("join.graceHashJoin: create right partition: %w", err)
		}
		rightWriters[i] = sw
	}

	// Partition existing hashMap rows into right partitions.
	for key, rows := range j.hashMap {
		p := hashPartition(key, numParts)
		for _, row := range rows {
			if err := rightWriters[p].WriteRow(row); err != nil {
				return fmt.Errorf("join.graceHashJoin: write existing right row: %w", err)
			}
			j.spilledRows++
		}
	}

	// Release in-memory hash table.
	j.hashMap = nil
	j.acct.Shrink(j.acct.Used())

	// Notify coordinator that this operator has spilled, allowing rebalancing.
	if sn, ok := j.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}

	// Partition the overflow batch.
	for i := 0; i < overflowBatch.Len; i++ {
		row := overflowBatch.Row(i)
		key := ""
		if v, ok := row[j.field]; ok {
			key = v.String()
		}
		p := hashPartition(key, numParts)
		if err := rightWriters[p].WriteRow(row); err != nil {
			return fmt.Errorf("join.graceHashJoin: write overflow right row: %w", err)
		}
		j.spilledRows++
	}

	// Continue reading remaining right batches and partition them.
	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		batch, err := j.right.Next(ctx)
		if err != nil {
			return fmt.Errorf("join.graceHashJoin: read right: %w", err)
		}
		if batch == nil {
			break
		}
		for i := 0; i < batch.Len; i++ {
			row := batch.Row(i)
			key := ""
			if v, ok := row[j.field]; ok {
				key = v.String()
			}
			p := hashPartition(key, numParts)
			if err := rightWriters[p].WriteRow(row); err != nil {
				return fmt.Errorf("join.graceHashJoin: write right row: %w", err)
			}
			j.spilledRows++
		}
	}

	// Close right writers and collect paths.
	j.rightPartPaths = make([]string, numParts)
	for i, sw := range rightWriters {
		j.rightPartPaths[i] = sw.Path()
		if err := sw.CloseFile(); err != nil {
			return fmt.Errorf("join.graceHashJoin: close right partition %d: %w", i, err)
		}
	}

	leftWriters := make([]*SpillWriter, numParts)
	for i := range leftWriters {
		sw, err := NewManagedSpillWriter(j.spillMgr, fmt.Sprintf("join-L-%02d", i))
		if err != nil {
			return fmt.Errorf("join.graceHashJoin: create left partition: %w", err)
		}
		leftWriters[i] = sw
	}

	// Read entire left side and partition. When prefetch is active, the
	// prefetch goroutine owns j.left — we must drain from j.prefetchCh
	// instead to avoid a data race (two goroutines calling j.left.Next).
	if err := j.partitionLeftSide(ctx, leftWriters, numParts); err != nil {
		return err
	}

	// Close left writers and collect paths.
	j.leftPartPaths = make([]string, numParts)
	for i, sw := range leftWriters {
		j.leftPartPaths[i] = sw.Path()
		if err := sw.CloseFile(); err != nil {
			return fmt.Errorf("join.graceHashJoin: close left partition %d: %w", i, err)
		}
	}

	j.graceMode = true
	j.currentPartition = 0
	j.built = true

	return nil
}

// partitionLeftSide reads the entire left side and writes rows to partition
// writers. When prefetch is active, the prefetch goroutine owns j.left, so
// rows are drained from j.prefetchCh instead of calling j.left.Next() directly.
// This prevents a data race between two goroutines calling Next() on the
// same non-thread-safe iterator.
func (j *JoinIterator) partitionLeftSide(ctx context.Context, writers []*SpillWriter, numParts int) error {
	if j.prefetchCh != nil {
		// Prefetch goroutine owns j.left — drain from the channel.
		for res := range j.prefetchCh {
			if res.err != nil {
				return fmt.Errorf("join.graceHashJoin: prefetch read left: %w", res.err)
			}
			if err := j.partitionBatchLeft(res.batch, writers, numParts); err != nil {
				return err
			}
		}
		// Channel closed — prefetch goroutine has finished. Safe to nil out
		// so that Next() doesn't try to read from a closed channel.
		j.prefetchCh = nil

		return nil
	}

	// No prefetch — read directly from j.left.
	for {
		batch, err := j.left.Next(ctx)
		if err != nil {
			return fmt.Errorf("join.graceHashJoin: read left: %w", err)
		}
		if batch == nil {
			return nil
		}
		if err := j.partitionBatchLeft(batch, writers, numParts); err != nil {
			return err
		}
	}
}

// partitionBatchLeft writes a batch of left-side rows to the appropriate
// partition writers based on the join key hash.
func (j *JoinIterator) partitionBatchLeft(batch *Batch, writers []*SpillWriter, numParts int) error {
	for i := 0; i < batch.Len; i++ {
		row := batch.Row(i)
		key := ""
		if v, ok := row[j.field]; ok {
			key = v.String()
		}
		p := hashPartition(key, numParts)
		if err := writers[p].WriteRow(row); err != nil {
			return fmt.Errorf("join.graceHashJoin: write left row: %w", err)
		}
		j.spilledRows++
	}

	return nil
}

// nextGrace produces output batches in grace hash join mode.
// For each partition: loads the right side into a temp hash table,
// probes with the left side, and emits matching rows in batches.
func (j *JoinIterator) nextGrace(ctx context.Context) (*Batch, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// If we have buffered output from the current partition, emit it.
		if j.partBufferOffset < len(j.partBuffer) {
			end := j.partBufferOffset + DefaultBatchSize
			if end > len(j.partBuffer) {
				end = len(j.partBuffer)
			}
			batch := BatchFromRows(j.partBuffer[j.partBufferOffset:end])
			j.partBufferOffset = end

			return batch, nil
		}

		// If we have a left reader open, probe rows against the partition hash table.
		if j.partLeftReader != nil {
			batch, err := j.probePartition()
			if err != nil {
				return nil, err
			}
			if batch != nil {
				return batch, nil
			}
			// Partition exhausted — clean up and advance.
			j.partLeftReader.Close()
			j.partLeftReader = nil
			j.partHashMap = nil
			j.acct.Shrink(j.acct.Used())
			j.currentPartition++
		}

		// Load the next non-empty partition.
		if j.currentPartition >= len(j.rightPartPaths) {
			return nil, nil // all partitions processed
		}

		if err := j.loadPartition(j.currentPartition); err != nil {
			return nil, err
		}

		// If the right partition was empty, skip to the next.
		if j.partHashMap == nil {
			j.currentPartition++

			continue
		}
	}
}

// loadPartition loads the right-side partition into a temp hash table
// and opens the left-side partition reader.
func (j *JoinIterator) loadPartition(idx int) error {
	// Load right partition into hash table.
	rightReader, err := NewSpillReader(j.rightPartPaths[idx])
	if err != nil {
		return fmt.Errorf("join.loadPartition: open right %d: %w", idx, err)
	}
	defer rightReader.Close()

	hashMap := make(map[string][]map[string]event.Value)
	for {
		row, readErr := rightReader.ReadRow()
		if errors.Is(readErr, io.EOF) || row == nil {
			break
		}
		if readErr != nil {
			return fmt.Errorf("join.loadPartition: read right %d: %w", idx, readErr)
		}
		// Track memory for partition hash table.
		if growErr := j.acct.Grow(estimatedRowBytes); growErr != nil {
			return fmt.Errorf("join.loadPartition: partition %d too large for memory: %w", idx, growErr)
		}
		key := ""
		if v, ok := row[j.field]; ok {
			key = v.String()
		}
		hashMap[key] = append(hashMap[key], row)
	}

	if len(hashMap) == 0 {
		// Empty right partition — for left join, we still need to emit left rows.
		if !strings.EqualFold(j.joinType, "left") {
			return nil
		}
	}

	j.partHashMap = hashMap

	// Open left partition reader.
	leftReader, err := NewSpillReader(j.leftPartPaths[idx])
	if err != nil {
		return fmt.Errorf("join.loadPartition: open left %d: %w", idx, err)
	}
	j.partLeftReader = leftReader
	j.partBuffer = nil
	j.partBufferOffset = 0

	return nil
}

// probePartition reads rows from the left partition reader and probes them
// against the partition hash table. Returns a batch of results or nil when
// the partition is exhausted.
func (j *JoinIterator) probePartition() (*Batch, error) {
	result := NewBatch(DefaultBatchSize)

	for result.Len < DefaultBatchSize {
		row, err := j.partLeftReader.ReadRow()
		if errors.Is(err, io.EOF) || row == nil {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("join.probePartition: %w", err)
		}

		key := ""
		if v, ok := row[j.field]; ok {
			key = v.String()
		}

		matches := j.partHashMap[key]
		if len(matches) > 0 {
			for _, match := range matches {
				merged := mergeRows(row, match)
				result.AddRow(merged)
			}
		} else if strings.EqualFold(j.joinType, "left") {
			result.AddRow(row)
		}
	}

	if result.Len > 0 {
		return result, nil
	}

	return nil, nil
}

func mergeRows(left, right map[string]event.Value) map[string]event.Value {
	result := make(map[string]event.Value, len(left)+len(right))
	for k, v := range left {
		result[k] = v
	}
	for k, v := range right {
		if _, exists := result[k]; !exists {
			result[k] = v
		}
	}

	return result
}
