package pipeline

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/cespare/xxhash/v2"

	"github.com/OrlovEvgeny/Lynxdb/pkg/stats"
)

// estimatedDedupHashEntryBytes is the estimated memory per entry in the
// hash-based dedup seen map: uint64 key (8B) + int value (8B) + map bucket overhead (~40B).
const estimatedDedupHashEntryBytes int64 = 56

// estimatedDedupExactKeyBytes is the estimated memory per entry in the exact
// (string-keyed) dedup seen map. String header (16B) + key bytes + int (8B) +
// map bucket overhead (~40B).
const estimatedDedupExactKeyBytes int64 = 96

// dedupWarnThreshold is the number of unique entries at which the hash-based
// dedup emits a warning about potential collision risk.
// At 10M unique values, xxhash64 birthday collision probability ~ 2.7e-8.
const dedupWarnThreshold = 10_000_000

// DedupIterator deduplicates rows based on specified fields.
//
// By default, uses xxhash64 for zero-allocation key hashing. The collision
// probability is negligible for typical cardinalities (<1M uniques):
//   - 37K uniques: p(collision) ~ 3.7e-11
//   - 1M uniques:  p(collision) ~ 2.7e-8
//   - 1B uniques:  p(collision) ~ 2.7e-2
//
// For correctness-critical workloads with very high cardinality, set
// exactMode=true (via config.QueryConfig.DedupExact) to use string keys.
//
// When a SpillManager is configured, the iterator spills its seen set to disk
// when the memory budget is exceeded, using a bloom filter + sorted hash file
// for bounded-memory dedup at the cost of disk I/O.
type DedupIterator struct {
	child       Iterator
	fields      []string
	limit       int
	seenHash    map[uint64]int // hash-based dedup (default)
	seenExact   map[string]int // exact mode (only when exactMode is true)
	exactMode   bool
	singleField bool // true when len(fields)==1, enables zero-copy fast path
	acct        stats.MemoryAccount
	warnedSize  bool // true after warning was emitted for large map

	// Spill-to-disk support.
	spillMgr            *SpillManager     // lifecycle manager for spill files (nil = no spill support)
	externalSet         *externalDedupSet // disk-backed seen set (non-nil after spill)
	spilledEntries      int64             // total entries migrated to disk
	spillBytesTotal     int64             // persisted spill bytes (survives Close)
	bloomAllocBytes     int64             // bloom filter heap allocation (tracked but not budget-bound)
	externalLimitCounts map[uint64]int    // per-hash counts for limit>1 in external mode
	// externalLimitCounts grows unboundedly for limit>1 dedup on high-
	// cardinality fields. Each entry is ~40 bytes (uint64 key + int value
	// + map overhead). For 10M unique keys this is ~400MB. May need capping
	// or an approximate counter for extreme cardinalities.
}

// NewDedupIterator creates a dedup operator with optional limit.
func NewDedupIterator(child Iterator, fields []string, limit int) *DedupIterator {
	if limit <= 0 {
		limit = 1
	}

	return &DedupIterator{
		child:       child,
		fields:      fields,
		limit:       limit,
		seenHash:    make(map[uint64]int),
		singleField: len(fields) == 1,
		acct:        stats.NopAccount(),
	}
}

// NewDedupIteratorWithBudget creates a dedup operator with memory budget tracking.
func NewDedupIteratorWithBudget(child Iterator, fields []string, limit int, acct stats.MemoryAccount) *DedupIterator {
	d := NewDedupIterator(child, fields, limit)
	d.acct = stats.EnsureAccount(acct)

	return d
}

// NewDedupIteratorExact creates a dedup operator using exact string keys.
// Use this when correctness is critical for very high cardinality datasets.
func NewDedupIteratorExact(child Iterator, fields []string, limit int, acct stats.MemoryAccount) *DedupIterator {
	if limit <= 0 {
		limit = 1
	}

	return &DedupIterator{
		child:       child,
		fields:      fields,
		limit:       limit,
		seenExact:   make(map[string]int),
		exactMode:   true,
		singleField: len(fields) == 1,
		acct:        stats.EnsureAccount(acct),
	}
}

// Init delegates to the child iterator.
func (d *DedupIterator) Init(ctx context.Context) error {
	return d.child.Init(ctx)
}

// Next returns the next batch of deduplicated rows. Rows whose dedup key fields
// have already been seen (up to `limit` times) are filtered out.
//
// When an external set is active (after a spill), all dedup checks go through
// the bloom filter + disk hash file path.
func (d *DedupIterator) Next(ctx context.Context) (*Batch, error) {
	for {
		batch, err := d.child.Next(ctx)
		if batch == nil || err != nil {
			return nil, err
		}

		matches := make([]bool, batch.Len)

		var matchCount int
		if d.externalSet != nil {
			// External mode (after spill): use bloom filter + disk hash file.
			matchCount, err = d.dedupExternal(batch, matches)
		} else if d.exactMode {
			matchCount, err = d.dedupExact(batch, matches)
		} else {
			matchCount, err = d.dedupHash(batch, matches)
		}
		if err != nil {
			return nil, err
		}

		if matchCount == 0 {
			continue
		}
		if matchCount == batch.Len {
			return batch, nil
		}

		return compactBatch(batch, matches, matchCount), nil
	}
}

// dedupHash uses xxhash64 for zero-allocation dedup.
// When a budget error occurs and a SpillManager is configured, the in-memory
// seen map is migrated to disk and the remaining rows in the batch are
// processed via the external dedup path.
func (d *DedupIterator) dedupHash(batch *Batch, matches []bool) (int, error) {
	matchCount := 0

	for i := 0; i < batch.Len; i++ {
		h := d.computeHash(batch, i)

		if d.seenHash[h] < d.limit {
			if d.seenHash[h] == 0 {
				if err := d.acct.Grow(estimatedDedupHashEntryBytes); err != nil {
					// Budget exceeded — attempt spill if SpillManager is available.
					if d.spillMgr != nil {
						if spillErr := d.spill(); spillErr != nil {
							return matchCount, fmt.Errorf("dedup.dedupHash: %w", spillErr)
						}
						// Process remaining rows [i, batch.Len) via external path.
						extCount, extErr := d.processRemainingExternal(batch, i, matches)

						return matchCount + extCount, extErr
					}

					return matchCount, err
				}
			}
			d.seenHash[h]++
			matches[i] = true
			matchCount++
		}
	}

	// Warn once when hash map grows large (collision risk increases).
	if !d.warnedSize && len(d.seenHash) > dedupWarnThreshold {
		d.warnedSize = true
		slog.Warn("dedup hash map exceeds 10M entries; consider --dedup-exact for correctness",
			"entries", len(d.seenHash))
	}

	return matchCount, nil
}

// dedupExact uses string keys for correctness-critical dedup.
// When a budget error occurs and a SpillManager is configured, the in-memory
// seen map is migrated to disk (with hash-based dedup) and remaining rows
// are processed via the external dedup path.
func (d *DedupIterator) dedupExact(batch *Batch, matches []bool) (int, error) {
	matchCount := 0
	var sb strings.Builder

	for i := 0; i < batch.Len; i++ {
		sb.Reset()
		for j, f := range d.fields {
			if j > 0 {
				sb.WriteByte('|')
			}
			if col, ok := batch.Columns[f]; ok && i < len(col) {
				sb.WriteString(col[i].String())
			}
		}
		key := sb.String()

		if d.seenExact[key] < d.limit {
			if d.seenExact[key] == 0 {
				if err := d.acct.Grow(estimatedDedupExactKeyBytes + int64(len(key))); err != nil {
					// Budget exceeded — attempt spill if SpillManager is available.
					if d.spillMgr != nil {
						if spillErr := d.spill(); spillErr != nil {
							return matchCount, fmt.Errorf("dedup.dedupExact: %w", spillErr)
						}
						// Process remaining rows [i, batch.Len) via external path.
						extCount, extErr := d.processRemainingExternal(batch, i, matches)

						return matchCount + extCount, extErr
					}

					return matchCount, err
				}
			}
			d.seenExact[key]++
			matches[i] = true
			matchCount++
		}
	}

	return matchCount, nil
}

// computeHash computes an xxhash64 digest of the dedup key fields for row i.
func (d *DedupIterator) computeHash(batch *Batch, row int) uint64 {
	// Fast path: single-field dedup avoids the hasher overhead.
	if d.singleField {
		col, ok := batch.Columns[d.fields[0]]
		if !ok || row >= len(col) {
			return 0
		}

		return xxhash.Sum64String(col[row].String())
	}

	// Multi-field: hash each field value with a separator.
	h := xxhash.New()
	for _, field := range d.fields {
		col, ok := batch.Columns[field]
		if !ok || row >= len(col) {
			_, _ = h.Write([]byte{0}) // null sentinel

			continue
		}
		_, _ = h.WriteString(col[row].String())
		_, _ = h.Write([]byte{0xFF}) // field separator
	}

	return h.Sum64()
}

// Close releases resources: memory budget account and external set (if spilled).
func (d *DedupIterator) Close() error {
	d.acct.Close()
	if d.externalSet != nil {
		// Persist spill metrics before cleanup (ResourceStats may be called after Close).
		d.spillBytesTotal = d.externalSet.spillBytes
		d.externalSet.close()
		d.externalSet = nil
	}

	return d.child.Close()
}

// MemoryUsed returns the current tracked memory for this operator.
func (d *DedupIterator) MemoryUsed() int64 {
	return d.acct.Used()
}

// Schema delegates to the child iterator.
func (d *DedupIterator) Schema() []FieldInfo { return d.child.Schema() }
