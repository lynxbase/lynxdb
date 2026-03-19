package consumers

import (
	"encoding/binary"
	"fmt"

	"github.com/lynxbase/lynxdb/pkg/bufmgr"
)

// FrameHashTable implements a hash table where entries live in buffer manager frames.
// Used by aggregation operators (STATS) to store group-by keys and aggregation
// state in managed memory instead of the Go heap.
//
// NOT thread-safe. Designed for single-goroutine Volcano pipeline operators.
type FrameHashTable struct {
	allocator *QueryOperatorAllocator

	entryFrames []*bufmgr.Frame
	curFrame    *bufmgr.Frame
	curOffset   int

	directory  map[uint64][]entryLoc
	entryCount int
}

type entryLoc struct {
	frameIdx int
	offset   int
	length   int
}

const entryHeaderSize = 16 // [hash:8][keyLen:4][valLen:4]

// NewFrameHashTable creates a hash table backed by buffer manager frames.
func NewFrameHashTable(allocator *QueryOperatorAllocator) *FrameHashTable {
	return &FrameHashTable{
		allocator: allocator,
		directory: make(map[uint64][]entryLoc),
	}
}

// Put inserts or updates an entry.
func (ht *FrameHashTable) Put(hash uint64, key, value []byte) (bufmgr.FrameRef, error) {
	if locs, ok := ht.directory[hash]; ok {
		for _, loc := range locs {
			if ht.keyEquals(loc, key) {
				return ht.updateValue(loc, value)
			}
		}
	}

	return ht.appendEntry(hash, key, value)
}

// Get looks up an entry by hash and key.
func (ht *FrameHashTable) Get(hash uint64, key []byte) (bufmgr.FrameRef, bool) {
	locs, ok := ht.directory[hash]
	if !ok {
		return bufmgr.FrameRef{}, false
	}
	for _, loc := range locs {
		if ht.keyEquals(loc, key) {
			valOffset := loc.offset + entryHeaderSize + len(key)
			valLen := loc.length - entryHeaderSize - len(key)
			frame := ht.entryFrames[loc.frameIdx]

			return bufmgr.FrameRef{
				FrameID: frame.ID,
				Offset:  valOffset,
				Length:  valLen,
			}, true
		}
	}

	return bufmgr.FrameRef{}, false
}

// Len returns the number of entries.
func (ht *FrameHashTable) Len() int {
	return ht.entryCount
}

// FrameCount returns the number of frames used.
func (ht *FrameHashTable) FrameCount() int {
	return len(ht.entryFrames)
}

// Clear resets the hash table.
func (ht *FrameHashTable) Clear() {
	ht.directory = make(map[uint64][]entryLoc)
	ht.entryFrames = nil
	ht.curFrame = nil
	ht.curOffset = 0
	ht.entryCount = 0
}

// ForEach iterates over all entries.
func (ht *FrameHashTable) ForEach(fn func(hash uint64, key []byte, valRef bufmgr.FrameRef) bool) {
	for hash, locs := range ht.directory {
		for _, loc := range locs {
			frame := ht.entryFrames[loc.frameIdx]
			ds := frame.DataSlice()
			if ds == nil {
				continue
			}
			keyLen := int(binary.LittleEndian.Uint32(ds[loc.offset+8 : loc.offset+12]))
			key := ds[loc.offset+entryHeaderSize : loc.offset+entryHeaderSize+keyLen]
			valOffset := loc.offset + entryHeaderSize + keyLen
			valLen := loc.length - entryHeaderSize - keyLen

			ref := bufmgr.FrameRef{
				FrameID: frame.ID,
				Offset:  valOffset,
				Length:  valLen,
			}
			if !fn(hash, key, ref) {
				return
			}
		}
	}
}

func (ht *FrameHashTable) appendEntry(hash uint64, key, value []byte) (bufmgr.FrameRef, error) {
	totalLen := entryHeaderSize + len(key) + len(value)

	if ht.curFrame == nil || ht.curOffset+totalLen > ht.curFrame.Size() {
		if err := ht.allocEntryFrame(); err != nil {
			return bufmgr.FrameRef{}, err
		}
		if totalLen > ht.curFrame.Size() {
			return bufmgr.FrameRef{}, fmt.Errorf(
				"bufmgr.FrameHashTable: entry size %d exceeds frame size %d",
				totalLen, ht.curFrame.Size())
		}
	}

	var header [entryHeaderSize]byte
	binary.LittleEndian.PutUint64(header[0:8], hash)
	binary.LittleEndian.PutUint32(header[8:12], uint32(len(key)))
	binary.LittleEndian.PutUint32(header[12:16], uint32(len(value)))

	if err := ht.curFrame.WriteAt(header[:], ht.curOffset); err != nil {
		return bufmgr.FrameRef{}, err
	}
	if err := ht.curFrame.WriteAt(key, ht.curOffset+entryHeaderSize); err != nil {
		return bufmgr.FrameRef{}, err
	}
	if err := ht.curFrame.WriteAt(value, ht.curOffset+entryHeaderSize+len(key)); err != nil {
		return bufmgr.FrameRef{}, err
	}

	loc := entryLoc{
		frameIdx: len(ht.entryFrames) - 1,
		offset:   ht.curOffset,
		length:   totalLen,
	}
	ht.directory[hash] = append(ht.directory[hash], loc)
	ht.entryCount++

	valRef := bufmgr.FrameRef{
		FrameID: ht.curFrame.ID,
		Offset:  ht.curOffset + entryHeaderSize + len(key),
		Length:  len(value),
	}
	ht.curOffset += totalLen

	return valRef, nil
}

func (ht *FrameHashTable) updateValue(loc entryLoc, value []byte) (bufmgr.FrameRef, error) {
	frame := ht.entryFrames[loc.frameIdx]
	ds := frame.DataSlice()
	if ds == nil {
		return bufmgr.FrameRef{}, fmt.Errorf("bufmgr.FrameHashTable: frame data is nil (evicted)")
	}

	keyLen := int(binary.LittleEndian.Uint32(ds[loc.offset+8 : loc.offset+12]))
	existingValLen := loc.length - entryHeaderSize - keyLen

	if len(value) != existingValLen {
		return bufmgr.FrameRef{}, fmt.Errorf(
			"bufmgr.FrameHashTable: value size mismatch (have %d, want %d)",
			len(value), existingValLen)
	}

	valOffset := loc.offset + entryHeaderSize + keyLen
	if err := frame.WriteAt(value, valOffset); err != nil {
		return bufmgr.FrameRef{}, err
	}

	return bufmgr.FrameRef{
		FrameID: frame.ID,
		Offset:  valOffset,
		Length:  len(value),
	}, nil
}

func (ht *FrameHashTable) keyEquals(loc entryLoc, key []byte) bool {
	frame := ht.entryFrames[loc.frameIdx]
	ds := frame.DataSlice()
	if ds == nil {
		return false
	}

	keyLen := int(binary.LittleEndian.Uint32(ds[loc.offset+8 : loc.offset+12]))
	if keyLen != len(key) {
		return false
	}

	stored := ds[loc.offset+entryHeaderSize : loc.offset+entryHeaderSize+keyLen]
	for i := range key {
		if stored[i] != key[i] {
			return false
		}
	}

	return true
}

func (ht *FrameHashTable) allocEntryFrame() error {
	f, err := ht.allocator.AllocFrame()
	if err != nil {
		return fmt.Errorf("bufmgr.FrameHashTable.allocEntryFrame: %w", err)
	}
	ht.entryFrames = append(ht.entryFrames, f)
	ht.curFrame = f
	ht.curOffset = 0

	return nil
}
