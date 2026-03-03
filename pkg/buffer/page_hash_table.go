package buffer

import (
	"encoding/binary"
	"fmt"
)

// PageHashTable implements a hash table where entries live in buffer pool pages.
// This is used by aggregation operators (STATS) to store group-by keys and
// aggregation state in managed memory instead of the Go heap.
//
// Entries are stored in a flat layout within pages: each entry is a fixed-size
// record containing a hash, key length, key bytes, and value bytes. The bucket
// directory (hash → page slot) uses a separate set of pages.
//
// When the buffer manager evicts an operator's page (because cache or another
// query needs memory), the affected entries are lost from the in-memory table
// and must be rebuilt from the spill file — same mechanism as external
// aggregation, but triggered by memory pressure instead of a fixed threshold.
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline operators.
type PageHashTable struct {
	allocator *OperatorPageAllocator

	// entryPages holds pages containing serialized entries.
	entryPages []*Page
	curPage    *Page // current page being written to
	curOffset  int   // write position in current page

	// directory maps hash → list of entry locations (for collision chains).
	directory map[uint64][]entryLoc

	// Stats.
	entryCount int
}

// entryLoc identifies an entry within the page-based hash table.
type entryLoc struct {
	pageIdx int // index into entryPages
	offset  int // byte offset within the page
	length  int // total entry length (header + key + value)
}

// entryHeader is the fixed-size prefix of each entry.
// Layout: [hash:8][keyLen:4][valLen:4] = 16 bytes.
const entryHeaderSize = 16

// NewPageHashTable creates a hash table backed by buffer pool pages.
func NewPageHashTable(allocator *OperatorPageAllocator) *PageHashTable {
	return &PageHashTable{
		allocator: allocator,
		directory: make(map[uint64][]entryLoc),
	}
}

// Put inserts or updates an entry in the hash table. The key and value are
// serialized as raw bytes. If an entry with the same hash and key already exists,
// its location is returned (caller can overwrite the value in-place via the page).
//
// Returns the PageRef for the value portion of the entry, allowing the caller
// to read/write the aggregation state directly in the page.
func (ht *PageHashTable) Put(hash uint64, key, value []byte) (PageRef, error) {
	// Check for existing entry with same hash+key.
	if locs, ok := ht.directory[hash]; ok {
		for _, loc := range locs {
			if ht.keyEquals(loc, key) {
				// Update value in-place.
				return ht.updateValue(loc, value)
			}
		}
	}

	// New entry — append to current page.
	return ht.appendEntry(hash, key, value)
}

// Get looks up an entry by hash and key. Returns the PageRef for the value
// portion if found, or an invalid PageRef if not found.
func (ht *PageHashTable) Get(hash uint64, key []byte) (PageRef, bool) {
	locs, ok := ht.directory[hash]
	if !ok {
		return PageRef{}, false
	}
	for _, loc := range locs {
		if ht.keyEquals(loc, key) {
			valOffset := loc.offset + entryHeaderSize + len(key)
			valLen := loc.length - entryHeaderSize - len(key)
			page := ht.entryPages[loc.pageIdx]

			return PageRef{
				PageID: page.ID(),
				Offset: valOffset,
				Length: valLen,
			}, true
		}
	}

	return PageRef{}, false
}

// Len returns the number of entries in the hash table.
func (ht *PageHashTable) Len() int {
	return ht.entryCount
}

// PageCount returns the number of pages used by the hash table.
func (ht *PageHashTable) PageCount() int {
	return len(ht.entryPages)
}

// Clear resets the hash table. Pages are NOT freed — call allocator.ReleaseAll()
// to return pages to the pool.
func (ht *PageHashTable) Clear() {
	ht.directory = make(map[uint64][]entryLoc)
	ht.entryPages = nil
	ht.curPage = nil
	ht.curOffset = 0
	ht.entryCount = 0
}

// ForEach iterates over all entries, calling fn with the hash, key bytes, and
// a PageRef to the value. The page is pinned during iteration. Return false
// from fn to stop iteration early.
func (ht *PageHashTable) ForEach(fn func(hash uint64, key []byte, valRef PageRef) bool) {
	for hash, locs := range ht.directory {
		for _, loc := range locs {
			page := ht.entryPages[loc.pageIdx]
			ds := page.DataSlice()
			if ds == nil {
				continue
			}
			keyLen := int(binary.LittleEndian.Uint32(ds[loc.offset+8 : loc.offset+12]))
			key := ds[loc.offset+entryHeaderSize : loc.offset+entryHeaderSize+keyLen]
			valOffset := loc.offset + entryHeaderSize + keyLen
			valLen := loc.length - entryHeaderSize - keyLen

			ref := PageRef{
				PageID: page.ID(),
				Offset: valOffset,
				Length: valLen,
			}
			if !fn(hash, key, ref) {
				return
			}
		}
	}
}

// appendEntry writes a new entry to the current page (or allocates a new one).
func (ht *PageHashTable) appendEntry(hash uint64, key, value []byte) (PageRef, error) {
	totalLen := entryHeaderSize + len(key) + len(value)

	// Ensure we have a page with enough space.
	if ht.curPage == nil || ht.curOffset+totalLen > ht.curPage.Size() {
		if err := ht.allocEntryPage(); err != nil {
			return PageRef{}, err
		}
		// Verify the new page can hold this entry.
		if totalLen > ht.curPage.Size() {
			return PageRef{}, fmt.Errorf("buffer.PageHashTable: entry size %d exceeds page size %d", totalLen, ht.curPage.Size())
		}
	}

	// Write header: [hash:8][keyLen:4][valLen:4].
	var header [entryHeaderSize]byte
	binary.LittleEndian.PutUint64(header[0:8], hash)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(value)))

	if err := ht.curPage.WriteAt(header[:], ht.curOffset); err != nil {
		return PageRef{}, fmt.Errorf("buffer.PageHashTable: write header: %w", err)
	}
	if err := ht.curPage.WriteAt(key, ht.curOffset+entryHeaderSize); err != nil {
		return PageRef{}, fmt.Errorf("buffer.PageHashTable: write key: %w", err)
	}
	if err := ht.curPage.WriteAt(value, ht.curOffset+entryHeaderSize+len(key)); err != nil {
		return PageRef{}, fmt.Errorf("buffer.PageHashTable: write value: %w", err)
	}

	loc := entryLoc{
		pageIdx: len(ht.entryPages) - 1,
		offset:  ht.curOffset,
		length:  totalLen,
	}
	ht.directory[hash] = append(ht.directory[hash], loc)
	ht.entryCount++

	valRef := PageRef{
		PageID: ht.curPage.ID(),
		Offset: ht.curOffset + entryHeaderSize + len(key),
		Length: len(value),
	}
	ht.curOffset += totalLen

	return valRef, nil
}

// updateValue overwrites the value portion of an existing entry.
func (ht *PageHashTable) updateValue(loc entryLoc, value []byte) (PageRef, error) {
	page := ht.entryPages[loc.pageIdx]
	ds := page.DataSlice()
	if ds == nil {
		return PageRef{}, fmt.Errorf("buffer.PageHashTable: page data is nil (evicted)")
	}

	keyLen := int(binary.LittleEndian.Uint32(ds[loc.offset+8 : loc.offset+12]))
	existingValLen := loc.length - entryHeaderSize - keyLen

	if len(value) != existingValLen {
		return PageRef{}, fmt.Errorf("buffer.PageHashTable: value size mismatch (have %d, want %d); in-place update requires same size", len(value), existingValLen)
	}

	valOffset := loc.offset + entryHeaderSize + keyLen
	if err := page.WriteAt(value, valOffset); err != nil {
		return PageRef{}, fmt.Errorf("buffer.PageHashTable: update value: %w", err)
	}

	return PageRef{
		PageID: page.ID(),
		Offset: valOffset,
		Length: len(value),
	}, nil
}

// keyEquals checks if the key stored at loc matches the given key.
func (ht *PageHashTable) keyEquals(loc entryLoc, key []byte) bool {
	page := ht.entryPages[loc.pageIdx]
	ds := page.DataSlice()
	if ds == nil {
		return false
	}

	keyLen := int(binary.LittleEndian.Uint32(ds[loc.offset+8 : loc.offset+12]))
	if keyLen != len(key) {
		return false
	}

	stored := ds[loc.offset+entryHeaderSize : loc.offset+entryHeaderSize+keyLen]

	// Compare byte-by-byte (avoid allocation from converting to string).
	for i := range key {
		if stored[i] != key[i] {
			return false
		}
	}

	return true
}

// allocEntryPage allocates a new entry page from the operator allocator.
func (ht *PageHashTable) allocEntryPage() error {
	p, err := ht.allocator.AllocPage()
	if err != nil {
		return fmt.Errorf("buffer.PageHashTable.allocEntryPage: %w", err)
	}
	ht.entryPages = append(ht.entryPages, p)
	ht.curPage = p
	ht.curOffset = 0

	return nil
}
