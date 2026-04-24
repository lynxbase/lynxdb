package pipeline

import (
	"encoding/binary"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"

	"github.com/cespare/xxhash/v2"

	"github.com/lynxbase/lynxdb/pkg/memgov"
)

// maxExternalLimitEntries caps the number of entries in the external limit
// counts map to prevent OOM. When exceeded, limit>1 dedup degrades to
// limit=1 behavior (bloom-only dedup) and logs a warning.
const maxExternalLimitEntries = 1_000_000

// estimatedLimitCountEntryBytes is the estimated memory per entry in the
// externalLimitCounts map: uint64 key (8B) + int value (8B) + map bucket overhead (~40B).
const estimatedLimitCountEntryBytes int64 = 56

// externalDedupMaxBloomBits is the maximum number of bits in the bloom filter
// used for the external dedup seen set. 64MB = 536M bits, supporting ~56M unique
// values with <1% false positive rate. The actual allocation is sized proportionally
// to the number of entries being migrated (10 bits per entry, minimum 1M bits).
const externalDedupMaxBloomBits = 64 * 1024 * 1024 * 8 // 536_870_912 bits

// externalDedupMinBloomBits is the minimum bloom filter size (128KB = 1M bits).
const externalDedupMinBloomBits = 1 << 20

// externalDedupHashK is the number of hash functions for the bloom filter.
// K = (m/n) * ln(2) ~ 9.6 for 56M items in 536M bits. We use 7 which gives
// ~1% FPR for expected cardinalities.
const externalDedupHashK = 7

// externalDedupBufferSize is the number of hashes buffered in memory before
// flushing to the disk hash file. 64K entries * 8 bytes = 512KB per flush.
const externalDedupBufferSize = 65536

// externalDedupSet implements a two-tier dedup seen set:
//
// Tier 1: In-memory bloom filter for fast negative lookups (definitely not seen).
// Tier 2: On-disk sorted hash file for exact positive verification.
//
// When the in-memory dedup map exceeds the memory budget, all entries are
// migrated to this structure. New values are checked against the bloom filter
// first (O(1)); if the bloom says "maybe present", a binary search on the
// sorted disk file confirms (O(log N) with 1-2 disk reads via sparse index).
//
// Hash collision at 64-bit xxhash: birthday paradox gives 50% collision
// probability at ~4 billion entries — negligible for log analytics workloads.
type externalDedupSet struct {
	bloom     []uint64 // bit array for bloom filter
	bloomBits uint64   // total bits in bloom

	// Disk-backed sorted hash file.
	hashFile    *os.File      // sorted 8-byte uint64 hashes
	hashCount   int64         // total hashes written to disk
	sparseIndex []sparseEntry // sparse index for binary search (every 1024th hash)

	// In-memory buffer for new hashes before flushing to disk.
	buffer    []uint64
	bufferMax int

	// Spill manager for file lifecycle.
	spillMgr *SpillManager

	// Metrics.
	spillBytes int64
}

// sparseEntry maps a position in the hash file to the hash value at that position.
// Used for binary search: find the right region, then scan linearly within it.
type sparseEntry struct {
	offset int64  // byte offset in the hash file
	hash   uint64 // hash value at this offset
}

// newExternalDedupSet creates a new external dedup set from an existing
// in-memory hash map. All entries from the map are added to the bloom filter
// and written to a sorted disk file. The in-memory map can then be cleared.
func newExternalDedupSet(
	seenHash map[uint64]int,
	seenExact map[string]int,
	exactMode bool,
	spillMgr *SpillManager,
) (*externalDedupSet, error) {
	// Size bloom filter proportionally: 10 bits per entry, clamped to
	// [externalDedupMinBloomBits, externalDedupMaxBloomBits]. This avoids
	// allocating 64MB for small cardinalities while still supporting large ones.
	n := len(seenHash)
	if exactMode {
		n = len(seenExact)
	}
	bloomBits := uint64(n) * 10
	if bloomBits < externalDedupMinBloomBits {
		bloomBits = externalDedupMinBloomBits
	}
	if bloomBits > externalDedupMaxBloomBits {
		bloomBits = externalDedupMaxBloomBits
	}
	// Round up to multiple of 64 for word alignment.
	bloomBits = ((bloomBits + 63) / 64) * 64

	eds := &externalDedupSet{
		bloomBits: bloomBits,
		bloom:     make([]uint64, bloomBits/64),
		bufferMax: externalDedupBufferSize,
		buffer:    make([]uint64, 0, externalDedupBufferSize),
		spillMgr:  spillMgr,
	}

	// Collect all hashes from the existing maps.
	var hashes []uint64
	if exactMode && seenExact != nil {
		hashes = make([]uint64, 0, len(seenExact))
		for key := range seenExact {
			h := xxhash.Sum64String(key)
			hashes = append(hashes, h)
		}
	} else if seenHash != nil {
		hashes = make([]uint64, 0, len(seenHash))
		for h := range seenHash {
			hashes = append(hashes, h)
		}
	}

	// Sort hashes for the disk file.
	sort.Slice(hashes, func(i, j int) bool { return hashes[i] < hashes[j] })

	// Add all hashes to bloom filter.
	for _, h := range hashes {
		eds.bloomAdd(h)
	}

	// Write sorted hashes to disk.
	f, err := spillMgr.NewSpillFile("dedup-hashes")
	if err != nil {
		return nil, fmt.Errorf("dedup_external: create hash file: %w", err)
	}
	eds.hashFile = f

	if err := eds.writeHashesToDisk(hashes); err != nil {
		f.Close()

		return nil, fmt.Errorf("dedup_external: write initial hashes: %w", err)
	}

	return eds, nil
}

// writeHashesToDisk writes sorted hashes to the disk file and builds the sparse index.
func (eds *externalDedupSet) writeHashesToDisk(hashes []uint64) error {
	const sparseInterval = 1024
	buf := make([]byte, 8)

	for i, h := range hashes {
		offset := int64(i) * 8
		if i%sparseInterval == 0 {
			eds.sparseIndex = append(eds.sparseIndex, sparseEntry{
				offset: offset,
				hash:   h,
			})
		}
		binary.LittleEndian.PutUint64(buf, h)
		if _, err := eds.hashFile.Write(buf); err != nil {
			return err
		}
	}
	eds.hashCount = int64(len(hashes))
	eds.spillBytes = eds.hashCount * 8

	// Sync to ensure data is on disk before reads.
	return eds.hashFile.Sync()
}

// containsHash checks if a hash value has been seen.
// Checks bloom filter (fast path) → in-memory buffer (linear scan) →
// disk hash file (binary search, only if bloom says present).
func (eds *externalDedupSet) containsHash(h uint64) bool {
	// Fast path: bloom filter says definitely not seen.
	if !eds.bloomTest(h) {
		return false
	}

	for _, bh := range eds.buffer {
		if bh == h {
			return true
		}
	}

	// Bloom says maybe — verify with disk binary search.
	return eds.diskContains(h)
}

// addHash adds a hash value to the seen set.
func (eds *externalDedupSet) addHash(h uint64) error {
	eds.bloomAdd(h)
	eds.buffer = append(eds.buffer, h)

	if len(eds.buffer) >= eds.bufferMax {
		return eds.flushBuffer()
	}

	return nil
}

// flushBuffer sorts the in-memory buffer and merges it with the disk hash file.
func (eds *externalDedupSet) flushBuffer() error {
	if len(eds.buffer) == 0 {
		return nil
	}

	// Sort buffer.
	sort.Slice(eds.buffer, func(i, j int) bool { return eds.buffer[i] < eds.buffer[j] })

	newFile, err := eds.spillMgr.NewSpillFile("dedup-hashes-merged")
	if err != nil {
		return fmt.Errorf("dedup_external: create merged file: %w", err)
	}

	// Merge: read existing disk file + buffer into new file.
	if _, seekErr := eds.hashFile.Seek(0, io.SeekStart); seekErr != nil {
		newFile.Close()

		return fmt.Errorf("dedup_external: seek hash file: %w", seekErr)
	}

	var (
		diskBuf     = make([]byte, 8)
		writeBuf    = make([]byte, 8)
		diskHash    uint64
		diskValid   bool
		bufIdx      int
		newCount    int64
		newSparse   []sparseEntry
		sparseEvery = 1024
	)

	// Read first disk hash.
	if _, readErr := io.ReadFull(eds.hashFile, diskBuf); readErr == nil {
		diskHash = binary.LittleEndian.Uint64(diskBuf)
		diskValid = true
	}

	for diskValid || bufIdx < len(eds.buffer) {
		var writeHash uint64
		switch {
		case !diskValid:
			writeHash = eds.buffer[bufIdx]
			bufIdx++
		case bufIdx >= len(eds.buffer):
			writeHash = diskHash
			if _, readErr := io.ReadFull(eds.hashFile, diskBuf); readErr == nil {
				diskHash = binary.LittleEndian.Uint64(diskBuf)
			} else {
				diskValid = false
			}
		case eds.buffer[bufIdx] < diskHash:
			writeHash = eds.buffer[bufIdx]
			bufIdx++
		case eds.buffer[bufIdx] > diskHash:
			writeHash = diskHash
			if _, readErr := io.ReadFull(eds.hashFile, diskBuf); readErr == nil {
				diskHash = binary.LittleEndian.Uint64(diskBuf)
			} else {
				diskValid = false
			}
		default:
			// Equal — deduplicate.
			writeHash = eds.buffer[bufIdx]
			bufIdx++
			if _, readErr := io.ReadFull(eds.hashFile, diskBuf); readErr == nil {
				diskHash = binary.LittleEndian.Uint64(diskBuf)
			} else {
				diskValid = false
			}
		}

		if newCount%int64(sparseEvery) == 0 {
			newSparse = append(newSparse, sparseEntry{
				offset: newCount * 8,
				hash:   writeHash,
			})
		}

		binary.LittleEndian.PutUint64(writeBuf, writeHash)
		if _, writeErr := newFile.Write(writeBuf); writeErr != nil {
			newFile.Close()

			return fmt.Errorf("dedup_external: write merged hash: %w", writeErr)
		}
		newCount++
	}

	if syncErr := newFile.Sync(); syncErr != nil {
		newFile.Close()

		return fmt.Errorf("dedup_external: sync merged file: %w", syncErr)
	}

	// Swap files.
	oldPath := eds.hashFile.Name()
	eds.hashFile.Close()
	eds.spillMgr.Release(oldPath)

	eds.hashFile = newFile
	eds.hashCount = newCount
	eds.sparseIndex = newSparse
	eds.spillBytes = newCount * 8
	eds.buffer = eds.buffer[:0]

	return nil
}

// diskContains performs binary search on the sorted disk hash file.
func (eds *externalDedupSet) diskContains(target uint64) bool {
	if eds.hashCount == 0 {
		return false
	}

	// Use sparse index to narrow the search range.
	var startOffset, endOffset int64
	endOffset = eds.hashCount * 8

	if len(eds.sparseIndex) > 0 {
		// Binary search the sparse index.
		lo, hi := 0, len(eds.sparseIndex)-1
		for lo <= hi {
			mid := (lo + hi) / 2
			if eds.sparseIndex[mid].hash == target {
				return true
			}
			if eds.sparseIndex[mid].hash < target {
				startOffset = eds.sparseIndex[mid].offset
				lo = mid + 1
			} else {
				if mid < len(eds.sparseIndex) {
					endOffset = eds.sparseIndex[mid].offset + 8
				}
				hi = mid - 1
			}
		}
	}

	// Linear scan within the narrowed range.
	buf := make([]byte, 8)
	if _, seekErr := eds.hashFile.Seek(startOffset, io.SeekStart); seekErr != nil {
		return false
	}

	for offset := startOffset; offset < endOffset; offset += 8 {
		if _, err := io.ReadFull(eds.hashFile, buf); err != nil {
			return false
		}
		h := binary.LittleEndian.Uint64(buf)
		if h == target {
			return true
		}
		if h > target {
			return false // sorted — target is not in the file
		}
	}

	return false
}

// bloomAdd sets the bits for hash h in the bloom filter.
func (eds *externalDedupSet) bloomAdd(h uint64) {
	for i := 0; i < externalDedupHashK; i++ {
		bit := eds.bloomHash(h, i)
		wordIdx := bit / 64
		bitIdx := bit % 64
		if wordIdx < uint64(len(eds.bloom)) {
			eds.bloom[wordIdx] |= 1 << bitIdx
		}
	}
}

// bloomTest checks if all bits for hash h are set in the bloom filter.
func (eds *externalDedupSet) bloomTest(h uint64) bool {
	for i := 0; i < externalDedupHashK; i++ {
		bit := eds.bloomHash(h, i)
		wordIdx := bit / 64
		bitIdx := bit % 64
		if wordIdx >= uint64(len(eds.bloom)) {
			return false
		}
		if eds.bloom[wordIdx]&(1<<bitIdx) == 0 {
			return false
		}
	}

	return true
}

// bloomHash computes the i-th hash function for the bloom filter using
// double hashing: h_i(x) = h1(x) + i*h2(x), where h1 and h2 are derived
// from the 64-bit hash by splitting into upper and lower 32 bits.
func (eds *externalDedupSet) bloomHash(h uint64, i int) uint64 {
	h1 := h & 0xFFFFFFFF
	h2 := h >> 32
	if h2 == 0 {
		h2 = 1
	}

	return (h1 + uint64(i)*h2) % eds.bloomBits
}

// close releases all resources: closes and removes the hash file.
func (eds *externalDedupSet) close() {
	if eds.hashFile != nil {
		path := eds.hashFile.Name()
		eds.hashFile.Close()
		if eds.spillMgr != nil {
			eds.spillMgr.Release(path)
		} else {
			os.Remove(path)
		}
		eds.hashFile = nil
	}
	eds.bloom = nil
	eds.buffer = nil
	eds.sparseIndex = nil
}

// computeDedupHash computes xxhash64 for dedup key fields at row i.
func computeDedupHash(batch *Batch, row int, fields []string, singleField bool) uint64 {
	if singleField {
		col, ok := batch.Columns[fields[0]]
		if !ok || row >= len(col) {
			return 0
		}

		return xxhash.Sum64String(col[row].String())
	}

	h := xxhash.New()
	for _, field := range fields {
		col, ok := batch.Columns[field]
		if !ok || row >= len(col) {
			_, _ = h.Write([]byte{0})

			continue
		}
		_, _ = h.WriteString(col[row].String())
		_, _ = h.Write([]byte{0xFF})
	}

	return h.Sum64()
}

// newDedupIteratorWithSpill creates a dedup operator with memory budget tracking
// and disk spill support. When the in-memory seen map exceeds the budget,
// entries are migrated to a bloom filter + sorted disk hash file, allowing
// dedup to continue with bounded memory at the cost of I/O.
func newDedupIteratorWithSpill(child Iterator, fields []string, limit int, acct memgov.MemoryAccount, mgr *SpillManager) *DedupIterator {
	d := NewDedupIteratorWithBudget(child, fields, limit, acct)
	d.spillMgr = mgr

	return d
}

// newDedupIteratorExactWithSpill creates a dedup operator using exact string keys
// with disk spill support.
func newDedupIteratorExactWithSpill(child Iterator, fields []string, limit int, acct memgov.MemoryAccount, mgr *SpillManager) *DedupIterator {
	d := NewDedupIteratorExact(child, fields, limit, acct)
	d.spillMgr = mgr

	return d
}

// spill transitions the dedup iterator from in-memory to external (disk-backed) mode.
// Existing entries are migrated to a bloom filter + sorted hash file.
// Per-hash limit counts are preserved for limit > 1 dedup queries.
func (d *DedupIterator) spill() error {
	if d.spillMgr == nil {
		return fmt.Errorf("dedup: no SpillManager configured for spill")
	}

	eds, err := newExternalDedupSet(d.seenHash, d.seenExact, d.exactMode, d.spillMgr)
	if err != nil {
		return fmt.Errorf("dedup.spill: %w", err)
	}

	d.externalSet = eds
	d.spilledEntries = int64(len(d.seenHash)) + int64(len(d.seenExact))

	// Track the bloom filter heap allocation for observability. The bloom is
	// not budget-bound (it lives outside the MemoryAccount) but we record the
	// size so ResourceStats can report total memory overhead accurately.
	d.bloomAllocBytes = int64(len(eds.bloom)) * 8 // []uint64 → bytes

	// Preserve per-hash limit counts for limit > 1.
	if d.limit > 1 {
		if d.exactMode && d.seenExact != nil {
			d.externalLimitCounts = make(map[uint64]int, len(d.seenExact))
			for key, count := range d.seenExact {
				h := xxhash.Sum64String(key)
				d.externalLimitCounts[h] = count
			}
		} else if d.seenHash != nil {
			d.externalLimitCounts = make(map[uint64]int, len(d.seenHash))
			for h, count := range d.seenHash {
				d.externalLimitCounts[h] = count
			}
		}
	}

	// Calculate limit map memory before clearing accounts.
	limitMapBytes := int64(len(d.externalLimitCounts)) * estimatedLimitCountEntryBytes

	// Clear in-memory maps.
	d.seenHash = nil
	d.seenExact = nil
	d.acct.Shrink(d.acct.Used())

	// Re-track the limit counts map that persists after spill.
	if limitMapBytes > 0 {
		if err := d.acct.Grow(limitMapBytes); err != nil {
			slog.Warn("dedup: failed to track limit map memory after spill", "bytes", limitMapBytes, "err", err)
		}
	}

	// Notify coordinator that this operator has spilled, allowing rebalancing.
	if sn, ok := d.acct.(SpillNotifier); ok {
		sn.NotifySpilled()
	}

	return nil
}

// processRemainingExternal processes rows [startRow, batch.Len) using the
// external dedup set after a mid-batch spill transition. This handles the
// case where dedupHash or dedupExact triggers a spill partway through a batch.
//
// When externalLimitCounts exceeds maxExternalLimitEntries, limit>1 dedup
// degrades to limit=1 behavior (bloom-only) to prevent unbounded memory growth.
func (d *DedupIterator) processRemainingExternal(batch *Batch, startRow int, matches []bool) (int, error) {
	matchCount := 0
	limitCapped := d.limit > 1 && len(d.externalLimitCounts) > maxExternalLimitEntries

	for i := startRow; i < batch.Len; i++ {
		h := computeDedupHash(batch, i, d.fields, d.singleField)

		if d.limit > 1 && !limitCapped && d.externalLimitCounts[h] >= d.limit {
			continue
		}

		if d.externalSet.containsHash(h) {
			// Already seen — track count for limit > 1 if under cap.
			if d.limit > 1 && !limitCapped {
				d.externalLimitCounts[h]++
				if d.externalLimitCounts[h] <= d.limit {
					matches[i] = true
					matchCount++
				}
			}
			// Cap exceeded: bloom says seen, treat as limit=1 (skip row).

			continue
		}

		// New value — add to external set.
		if err := d.externalSet.addHash(h); err != nil {
			return matchCount, fmt.Errorf("dedup_external: add: %w", err)
		}
		if d.limit > 1 && !limitCapped {
			if err := d.acct.Grow(estimatedLimitCountEntryBytes); err != nil {
				slog.Warn("dedup: failed to track limit count memory", "err", err)
			}
			d.externalLimitCounts[h] = 1
			// Re-check cap after insertion.
			if len(d.externalLimitCounts) > maxExternalLimitEntries {
				limitCapped = true
				slog.Warn("dedup: externalLimitCounts exceeded cap, degrading to limit=1 behavior",
					"cap", maxExternalLimitEntries, "entries", len(d.externalLimitCounts))
			}
		}
		matches[i] = true
		matchCount++
	}

	return matchCount, nil
}

// dedupExternal performs dedup using the external set for a full batch.
func (d *DedupIterator) dedupExternal(batch *Batch, matches []bool) (int, error) {
	return d.processRemainingExternal(batch, 0, matches)
}

// ResourceStats implements ResourceReporter for per-operator spill metrics.
// Safe to call after Close() — spill metrics are persisted before cleanup.
func (d *DedupIterator) ResourceStats() OperatorResourceStats {
	spillBytes := d.spillBytesTotal
	if d.externalSet != nil {
		spillBytes = d.externalSet.spillBytes
	}

	return OperatorResourceStats{
		PeakBytes:       d.acct.MaxUsed(),
		SpilledRows:     d.spilledEntries,
		SpillBytes:      spillBytes,
		BloomAllocBytes: d.bloomAllocBytes,
	}
}
