package vec

import (
	"encoding/binary"

	"github.com/lynxbase/lynxdb/pkg/mem"
)

const (
	minKeyChunk = 1 << 10
	maxKeyChunk = 1 << 16
	// A record is its hash, the length of its payload as a varint and the
	// payload.
	keyHashSize = 8
)

// KeyPartitions is an append-only log of hashed byte records split by bits
// of the hash, so a later pass can take one partition at a time and keep
// its working set in cache. The bits are those that a ByteTable which
// numbers the records of one partition leaves free: it reads the ones below
// to pick a slot and the ones above to tell its keys apart.
type KeyPartitions struct {
	arena *mem.Arena
	mask  uint64
	parts []keyPartition
	// chunks holds the chunks that filled up, of every partition, and
	// next[i] the chunk that follows chunks[i] in its partition, -1 for none;
	// one shared list spares each partition a list of its own on the heap.
	chunks [][]byte
	next   []int32
	// free lists the chunks of drained partitions, through next: the first
	// of them, or -1.
	free int32
	// size is the bytes of the records the log holds.
	size int
}

type keyPartition struct {
	cur []byte
	// curID is the place in chunks that cur had before a drained partition
	// left it, and takes again when it is full; -1 for a chunk without one.
	curID int32
	// first and last index the partition's full chunks, -1 without any.
	first, last int32
	n           int
	held        int // bytes of every chunk
}

// NewKeyPartitions returns a log with 1<<bits partitions that allocates
// from a.
func NewKeyPartitions(a *mem.Arena, bits int) *KeyPartitions {
	p := &KeyPartitions{arena: a, mask: 1<<bits - 1, parts: make([]keyPartition, 1<<bits), free: -1}
	for i := range p.parts {
		p.parts[i].curID, p.parts[i].first, p.parts[i].last = -1, -1, -1
	}

	return p
}

// Partitions returns the number of partitions.
func (p *KeyPartitions) Partitions() int { return len(p.parts) }

// Len returns the number of records in partition i.
func (p *KeyPartitions) Len(i int) int { return p.parts[i].n }

// Size returns the bytes of the records that the log holds.
func (p *KeyPartitions) Size() int { return p.size }

// PartitionOf returns the partition, of 1<<bits, of a record with hash h.
func PartitionOf(h uint64, bits int) int {
	return int(h >> (64 - tableFingerprint - tablePartition) & (1<<bits - 1))
}

// Add appends rec to the partition that h selects.
func (p *KeyPartitions) Add(h uint64, rec []byte) {
	p.AddKey(h, 0, false, rec, nil, nil)
}

// AddKey appends to the partition that h selects a record whose payload is
// the byte tag when tagged is set, the bytes of key, the words in
// little-endian order and tail.
func (p *KeyPartitions) AddKey(h uint64, tag byte, tagged bool, key []byte, words []int64, tail []byte) {
	kp := &p.parts[h>>(64-tableFingerprint-tablePartition)&p.mask]
	size := len(key) + 8*len(words) + len(tail)
	if tagged {
		size++
	}
	need := keyHashSize + binary.MaxVarintLen64 + size
	if cap(kp.cur)-len(kp.cur) < need {
		p.grow(kp, need)
	}
	n := len(kp.cur)
	cur := kp.cur[:n+need]
	binary.LittleEndian.PutUint64(cur[n:], h)
	n += keyHashSize
	if size < 0x80 {
		cur[n] = byte(size)
		n++
	} else {
		n += binary.PutUvarint(cur[n:], uint64(size))
	}
	if tagged {
		cur[n] = tag
		n++
	}
	n += copy(cur[n:], key)
	for _, w := range words {
		binary.LittleEndian.PutUint64(cur[n:], uint64(w))
		n += 8
	}
	n += copy(cur[n:], tail)
	p.size += n - len(kp.cur)
	kp.cur = cur[:n]
	kp.n++
}

// PairSize is the size of a record that AddPair appends.
const PairSize = keyHashSize + 1 + 12

// AddPair appends to the partition that h selects a record whose payload is
// the word a and the four bytes of b, in little-endian order: PairSize bytes
// with its hash and the length of the payload.
func (p *KeyPartitions) AddPair(h, a uint64, b uint32) {
	kp := &p.parts[h>>(64-tableFingerprint-tablePartition)&p.mask]
	if cap(kp.cur)-len(kp.cur) < PairSize {
		p.grow(kp, PairSize)
	}
	n := len(kp.cur)
	kp.cur = kp.cur[:n+PairSize]
	rec := kp.cur[n : n+PairSize]
	binary.LittleEndian.PutUint64(rec, h)
	rec[keyHashSize] = 12
	binary.LittleEndian.PutUint64(rec[keyHashSize+1:], a)
	binary.LittleEndian.PutUint32(rec[keyHashSize+9:], b)
	p.size += PairSize
	kp.n++
}

// grow starts a chunk half the size of what the partition already holds, so
// that the unused tail stays a fraction of the partition: a chunk that a
// drained partition left when one of them has the room, or a new one.
func (p *KeyPartitions) grow(kp *keyPartition, need int) {
	size := max(min(max(kp.held/2, minKeyChunk), maxKeyChunk), need)
	if len(kp.cur) != 0 {
		id := kp.curID
		if id < 0 {
			id = int32(len(p.chunks))
			p.chunks = append(p.chunks, nil)
			p.next = append(p.next, -1)
		}
		p.chunks[id], p.next[id] = kp.cur, -1
		if kp.last >= 0 {
			p.next[kp.last] = id
		} else {
			kp.first = id
		}
		kp.last = id
	}
	if id := p.free; id >= 0 && cap(p.chunks[id]) >= need {
		p.free = p.next[id]
		kp.cur, kp.curID = p.chunks[id][:0], id
	} else {
		kp.cur, kp.curID = mem.Raw[byte](p.arena, 0, size), -1
	}
	kp.held += cap(kp.cur)
}

// Each calls fn with the hash and payload of every record of partition i in
// insertion order; the payload is valid until the arena is reset.
func (p *KeyPartitions) Each(i int, fn func(h uint64, rec []byte)) {
	p.Chunks(i, func(chunk []byte) {
		for at := 0; at < len(chunk); {
			h, start, end := RecordAt(chunk, at)
			fn(h, chunk[start:end:end])
			at = end
		}
	})
}

// Chunks calls fn with every chunk of records of partition i in insertion
// order, for a caller that reads the records with RecordAt; a chunk is valid
// until the arena is reset or the partition drained.
func (p *KeyPartitions) Chunks(i int, fn func(chunk []byte)) {
	kp := &p.parts[i]
	for id := kp.first; id >= 0; id = p.next[id] {
		fn(p.chunks[id])
	}
	if len(kp.cur) != 0 {
		fn(kp.cur)
	}
}

// Drain calls fn as Chunks does and then empties partition i, whose chunks
// serve the records that follow, of any partition.
func (p *KeyPartitions) Drain(i int, fn func(chunk []byte)) {
	kp := &p.parts[i]
	if kp.n == 0 {
		return
	}
	p.Chunks(i, fn)
	for id := kp.first; id >= 0; {
		p.size -= len(p.chunks[id])
		following := p.next[id]
		p.next[id], p.free = p.free, id
		id = following
	}
	p.size -= len(kp.cur)
	// The chunk being filled stays the partition's.
	kp.cur, kp.first, kp.last, kp.n = kp.cur[:0], -1, -1, 0
	kp.held = cap(kp.cur)
}

// RecordAt returns the hash of the record that starts at offset at of a
// chunk, and where its payload starts and ends, which is where the next
// record starts.
func RecordAt(chunk []byte, at int) (h uint64, start, end int) {
	h = binary.LittleEndian.Uint64(chunk[at:])
	if n := int(chunk[at+keyHashSize]); n < 0x80 {
		return h, at + keyHashSize + 1, at + keyHashSize + 1 + n
	}
	size, used := binary.Uvarint(chunk[at+keyHashSize:])
	start = at + keyHashSize + used

	return h, start, start + int(size)
}
